#!/usr/bin/env node
const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
const PuppeteerHar = require('puppeteer-har');
const { parse } = require('csv-parse/sync');
require('dotenv').config();

puppeteer.use(StealthPlugin());

// ======= Global preferred model label (set to null to keep default Sonnet 4) =======
const PREFERRED_MENU_LABEL = "Opus 4.1"; // e.g., "Opus 4.1", "Sonnet 4"
// ==================================================================================

// --------- Utilities ---------
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function ensureDir(p) {
  if (!fs.existsSync(p)) fs.mkdirSync(p, { recursive: true });
}

// --------- Claude ---------
const claude = {
  selectors: {
    // Composer & Send
    editor: 'div.ProseMirror[role="textbox"][aria-label="Write your prompt to Claude"]',
    sendButton: 'button[aria-label="Send message"]',

    // Response detection (latest Claude response block → markdown paragraphs)
    answerBlock: 'div.font-claude-response',
    answerTextNodes: '.standard-markdown p',

    // New chat
    newChatButton: 'a[aria-label="New chat"][href="/new"]',

    // Not needed (Claude browse/web toggle is on by default)
    plusMenuButton: null,
    webToggle: null,
    webMenuItem: null,

    // Optional done indicator
    doneIndicator: null,

    // ===== Model dropdown =====
    modelDropdownBtn: 'button[data-testid="model-selector-dropdown"]',
    // Menu container and items
    menuContent: '[role="menu"][data-state="open"]',
    menuItem: '[role="menu"][data-state="open"] [role="menuitem"]',
  },

  enableWeb: async (_page) => {
    // No-op: Claude web/browse toggle is always on by default
  },

  // ===== Minimal helper to choose a model by its visible label =====
  selectModel: async (page, modelLabel, { timeout = 20000 } = {}) => {
    if (!modelLabel) return; // do nothing if not set
    const s = claude.selectors;

    // Ensure dropdown button is present
    const btn = await page.waitForSelector(s.modelDropdownBtn, { timeout }).catch(() => null);
    if (!btn) return;

    // Open the menu
    await btn.click().catch(() => {});
    await page.waitForSelector(s.menuContent, { timeout: 5000 }).catch(() => {});

    // Click the first menu item whose text contains the requested label
    const index = await page.$$eval(s.menuItem, (els, needle) => {
      const n = String(needle).toLowerCase();
      for (let i = 0; i < els.length; i++) {
        const txt = (els[i].innerText || '').trim().toLowerCase();
        if (txt.includes(n)) return i;
      }
      return -1;
    }, modelLabel);

    if (index >= 0) {
      const items = await page.$$(s.menuItem);
      await items[index].click().catch(() => {});
      await page.waitForTimeout(400);
    } else {
      console.warn(`⚠️ Model menu item not found for: "${modelLabel}"`);
    }
  },

  submitPrompt: async (page, text) => {
    const s = claude.selectors;
    const editor = await page.waitForSelector(s.editor, { timeout: 15000 });
    await editor.click({ delay: 80 });
    // clear and type
    await page.keyboard.down('Control'); await page.keyboard.press('A'); await page.keyboard.up('Control');
    await editor.type(text, { delay: 40 });
    // click send
    await page.click(s.sendButton);
  },

  readVisibleText: async (page) => {
    const s = claude.selectors;
    const text = await page.evaluate(({ answerBlock, answerTextNodes }) => {
      const blocks = Array.from(document.querySelectorAll(answerBlock));
      if (!blocks.length) return '';
      const latest = blocks[blocks.length - 1];
      const ps = Array.from(latest.querySelectorAll(answerTextNodes));
      return ps.map(e => e.innerText).filter(t => t && t.trim() !== '').join('\n\n');
    }, { answerBlock: s.answerBlock, answerTextNodes: s.answerTextNodes }).catch(() => '');
    return text || '';
  },

  newChat: async (page) => {
    const s = claude.selectors;
    const btn = await page.$(s.newChatButton);
    if (btn) await btn.click();
  },

  isSSE: (response) => {
    const ct = response.headers()['content-type'] || '';
    return ct.includes('event-stream');
  }
};

// --------- CSV helpers ---------
function loadCsvPrompts(csvFile) {
  if (!fs.existsSync(csvFile)) {
    throw new Error(`Missing CSV file: ${csvFile}`);
  }
  const content = fs.readFileSync(csvFile, 'utf8');
  let records;
  try {
    records = parse(content, {
      columns: true,
      skip_empty_lines: false,
      relax_column_count: true
    });
  } catch (err) {
    throw new Error(`Failed to parse CSV (${csvFile}): ${err.message}`);
  }
  if (!records.length) throw new Error(`CSV has no data rows: ${csvFile}`);
  if (!('query' in records[0])) throw new Error(`CSV does not have a 'query' header column: ${csvFile}`);
  return records.map(r => (r.query === undefined ? '' : r.query.toString()));
}

function savePromptsCatalog(outDir, csvFile, prompts) {
  ensureDir(outDir);
  const jsonlPath = path.join(outDir, 'prompts.jsonl');
  const txtPath   = path.join(outDir, 'prompts.txt');
  const metaPath  = path.join(outDir, 'source_meta.txt');

  fs.writeFileSync(
    jsonlPath,
    prompts.map((q, i) => JSON.stringify({ idx: i + 1, query: q })).join('\n') + '\n',
    'utf8'
  );
  fs.writeFileSync(
    txtPath,
    prompts.map((q, i) => `#${i + 1}\n${q}\n`).join('\n'),
    'utf8'
  );
  fs.writeFileSync(
    metaPath,
    `CSV Source: ${path.resolve(csvFile)}\nTotal Prompts: ${prompts.length}\nSaved: ${new Date().toISOString()}\n`,
    'utf8'
  );

  console.log(`📦 Saved prompts catalog to:
  - ${jsonlPath}
  - ${txtPath}
  - ${metaPath}`);
}

// --------- Stabilization ---------
async function waitForClaudeStability(page, { stabilityMs = 3000, maxTimeout = 60000, pollInterval = 1000 } = {}) {
  const start = Date.now();
  let last = '';
  let stableSince = Date.now();
  while (Date.now() - start < maxTimeout) {
    const current = await claude.readVisibleText(page);
    if (current === last) {
      if (Date.now() - stableSince >= stabilityMs) return current;
    } else {
      last = current;
      stableSince = Date.now();
    }
    await sleep(pollInterval);
  }
  return last;
}

// --------- Core batch runner ---------
async function processPromptsBatch(page, prompts, outDir, startIdx = 0) {
  ensureDir(outDir);

  // Derive category + model from outDir
  const [category, ...modelParts] = path.basename(outDir).split('_');
  const model = modelParts.join('_') || 'claude';

  const harsDir = path.join(outDir, `${category}_hars_${model}`);
  const responsesDir = path.join(outDir, `${category}_responses_${model}`);
  ensureDir(harsDir);
  ensureDir(responsesDir);

  // Set UA and open Claude
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
  );
  await page.goto('https://claude.ai/', { waitUntil: 'load' });
  await sleep(10000);

  // Wait for Claude composer
  await page.waitForSelector(claude.selectors.editor, { timeout: 0 });

  // ===== Select preferred model before first prompt =====
  await claude.selectModel(page, PREFERRED_MENU_LABEL).catch(() => {});

  for (let idx = 0; idx < prompts.length; idx++) {
    const promptText = "Search the web for: " + prompts[idx];
    // const promptText = prompts[idx];
    console.log(`\n=== [${path.basename(outDir)}] Prompt ${idx + 1}/${prompts.length}: "${promptText}" ===`);

    const safeBase = `prompt-${idx + 1}`;
    const harFilename = path.join(harsDir, `network-logs-${safeBase}.har`);
    const answerFilename = path.join(responsesDir, `response-${safeBase}.txt`);

    const har = new PuppeteerHar(page);
    await har.start({ path: harFilename, omitContent: false });

    let finalAnswer = '';
    let sseText = '';

    try {
      // Web is on by default in Claude
      await claude.enableWeb(page);

      // Prepare SSE listener 
      const sseResponsePromise = page.waitForResponse(
        (response) => claude.isSSE(response),
        { timeout: 120000 }
      );

      // Submit prompt
      await claude.submitPrompt(page, promptText);

      // Capture SSE 
      try {
        const sseResponse = await sseResponsePromise;
        sseText = await sseResponse.text();
      } catch (e) {
        console.warn('⚠️ SSE response not captured:', e.message);
      }

      // Wait for answer stabilization
      finalAnswer = await waitForClaudeStability(page, {
        stabilityMs: 3000,
        maxTimeout: 60000,
        pollInterval: 1000
      });

      if (!finalAnswer || finalAnswer.trim() === '') {
        console.warn(`Prompt ${idx + 1} produced no detectable answer before timeout.`);
      } else {
        console.log(`Captured answer (truncated): ${finalAnswer.slice(0, 250).replace(/\n/g, ' ')}...`);
      }

      await sleep(1000);
    } catch (err) {
      console.error(`Error processing prompt ${idx + 1}:`, err.stack || err.message);
    } finally {
      // Stop HAR
      try {
        await har.stop();
        console.log(`✅ HAR saved to ${harFilename}`);
      } catch (e) {
        console.warn('Failed to stop HAR cleanly:', e.message || e);
      }

      // Inject SSE into HAR
      try {
        const harJson = JSON.parse(fs.readFileSync(harFilename, 'utf-8'));
        for (const entry of harJson.log.entries) {
          if (entry.response?.content?.mimeType === 'text/event-stream') {
            entry.response.content.text = sseText;
            entry.response.content.size = Buffer.byteLength(sseText, 'utf8');
            break;
          }
        }
        fs.writeFileSync(harFilename, JSON.stringify(harJson, null, 2));
        console.log(`✅ HAR updated with SSE at ${harFilename}`);
      } catch (e) {
        console.error('Failed to inject SSE into HAR:', e.message || e);
      }

      // Save final answer
      try {
        const header = `Prompt: ${promptText}\nTimestamp: ${new Date().toISOString()}\n\n`;
        fs.writeFileSync(answerFilename, header + (finalAnswer || '[no answer captured]'), 'utf8');
        console.log(`✅ Answer saved to ${answerFilename}`);
      } catch (e) {
        console.warn('Failed to write answer file:', e.message || e);
      }
    }

    // Start a new chat for the next prompt
    try {
      await claude.newChat(page);
      await sleep(2000);
      // Ensure composer is ready again
      await page.waitForSelector(claude.selectors.editor, { timeout: 15000 });
      // ===== Re-select model because new chats default to Sonnet 4 =====
      await claude.selectModel(page, PREFERRED_MENU_LABEL).catch(() => {});
    } catch (e) {
      console.warn('New chat click failed; attempting fallback reload:', e.message);
      await page.goto('https://claude.ai/new', { waitUntil: 'load' });
      await sleep(5000);
      await page.waitForSelector(claude.selectors.editor, { timeout: 0 });
      // ===== Re-select model after fallback navigation as well =====
      await claude.selectModel(page, PREFERRED_MENU_LABEL).catch(() => {});
    }
  }
}

// --------- Main flow (configure your CSVs here) ---------
(async () => {
  const modelName = "opus-4.1";   // used only in output dir names
  const datasetRunNum = "1";    // mirrors your structure

  // Configure your CSV jobs here (uncomment/extend as needed)
  const csvJobs = [
    // {
    //   file: `./dataset/ORCAS-I-gold_label_Instrumental.csv`,
    //   outDir:  `instrumental_${modelName}_${datasetRunNum}`
    // },
    // {
    //   file: `./dataset/ORCAS-I-gold_label_Navigational.csv`,
    //   outDir: `navigational_${modelName}_${datasetRunNum}`
    // },
    // {
    //   file: `./dataset/ORCAS-I-gold_label_Transactional.csv`,
    //   outDir: `transactional_${modelName}_${datasetRunNum}`
    // },
    {
      file: `./dataset/ORCAS-I-gold_label_Factual.csv`,
      outDir: `factual_${modelName}_${datasetRunNum}`
    },
    // {
    //   file: `./dataset/ORCAS-I-gold_label_Abstain.csv`,
    //   outDir: `abstain_${modelName}_${datasetRunNum}`
    // }
  ];

  // Load prompts and save catalogs before launching browser
  const jobData = csvJobs.map(job => {
    const prompts = loadCsvPrompts(job.file);
    console.log(`Loaded ${prompts.length} prompts from ${job.file}`);
    savePromptsCatalog(job.outDir, job.file, prompts);
    return { ...job, prompts };
  });

  const browser = await puppeteer.launch({
    headless: false,
    userDataDir: './puppeteer-profile-claude',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--start-maximized']
  });

  try {
    const [page] = await browser.pages();

    // Process CSVs sequentially
    for (const { file, outDir, prompts } of jobData) {
      console.log(`\n==============================`);
      console.log(`▶ Processing CSV: ${file}`);
      console.log(`▶ Output Dir:    ${outDir}`);
      console.log(`==============================\n`);
      await processPromptsBatch(page, prompts, outDir, /*startIdx=*/0);
    }

  } catch (e) {
    console.error('Fatal error in main flow:', e.stack || e);
  } finally {
    try {
      await browser.close();
      console.log('Browser closed.');
    } catch (e) {
      console.warn('Error closing browser:', e.message || e);
    }
  }
})();