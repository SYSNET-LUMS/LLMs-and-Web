const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const path = require('path');
const PuppeteerHar = require('puppeteer-har');
const { parse } = require('csv-parse/sync');
require('dotenv').config();

puppeteer.use(StealthPlugin());

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function waitForRoleWithText(page, role, text, timeout = 5000) {
  const handle = await page.waitForFunction(
    (role, text) => {
      const nodes = Array.from(document.querySelectorAll(`[role='${role}']`));
      return nodes.find(n => n.textContent && n.textContent.trim() === text) || null;
    },
    { timeout },
    role,
    text
  );
  const el = await handle.asElement();
  if (!el) throw new Error(`Element [role='${role}'] with text "${text}" not found`);
  return el;
}

async function enableWebSearch(page) {
  // 1) Click the "+" composer button
  await safeClick(page, "button[data-testid='composer-plus-btn']");
  await page.waitForSelector("[role='menu']", { timeout: 10000 });

  // 2) Find "More" and open its submenu (hover first; fallback ArrowRight)
  const moreEl = await waitForRoleWithText(page, 'menuitem', 'More', 8000);

  await moreEl.evaluate(el => el.scrollIntoView({ block: 'center', inline: 'center' }));
  await moreEl.hover();

  await sleep(250)

  // Wait for aria-expanded=true; fallback to ArrowRight if needed
  try {
    await page.waitForFunction(el => el.getAttribute('aria-expanded') === 'true', { timeout: 1200 }, moreEl);
  } catch {
    await moreEl.focus();
    await page.keyboard.press('ArrowRight');
    // After ArrowRight, the menu may re-render; wait again
    await page.waitForFunction(el => el.getAttribute('aria-expanded') === 'true', { timeout: 2000 }, moreEl);
  }

  await sleep(250)

  // 3) Click "Web search" in the submenu
  const webSearchEl = await waitForRoleWithText(page, 'menuitemradio', 'Web search', 8000);
  await webSearchEl.hover();
  await webSearchEl.click();
}

// resilient click helper
async function safeClick(page, selector, options = { attempts: 3, waitFor: { timeout: 10000 } }) {
  for (let i = 0; i < options.attempts; i++) {
    try {
      await page.waitForSelector(selector, options.waitFor);
      await page.click(selector);
      return;
    } catch (e) {
      const msg = e.message || '';
      if (
        msg.includes('detached frame') ||
        msg.includes('Node is detached') ||
        msg.includes('Execution context was destroyed') ||
        msg.includes('TimeoutError')
      ) {
        await sleep(500);
        continue;
      }
      throw e;
    }
  }
  throw new Error(`safeClick failed for selector ${selector}`);
}

async function waitForResponseStability(page, { stabilityMs = 3000, maxTimeout = 60000, pollInterval = 1000 } = {}) {
  const start = Date.now();
  let lastText = '';
  let stableSince = Date.now();

  while (Date.now() - start < maxTimeout) {
    const paras = await page.$$eval('p', els => els.map(e => e.innerText).filter(t => t.trim() !== ''));
    const current = paras.join('\n\n');
    if (current === lastText) {
      if (Date.now() - stableSince >= stabilityMs) {
        return current;
      }
    } else {
      lastText = current;
      stableSince = Date.now();
    }
    await sleep(pollInterval);
  }
  return lastText;
}

/**
 * Load prompts from a CSV path. Requires a 'query' header.
 */
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
  if (!records.length) {
    throw new Error(`CSV has no data rows: ${csvFile}`);
  }
  if (!('query' in records[0])) {
    throw new Error(`CSV does not have a 'query' header column: ${csvFile}`);
  }
  const rawPrompts = records.map(r => r.query === undefined ? '' : r.query.toString());
  return rawPrompts;
}

/**
 * Save prompts for auditing: prompts.jsonl (one per line) and prompts.txt
 */
function savePromptsCatalog(outDir, csvFile, prompts) {
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  const jsonlPath = path.join(outDir, 'prompts.jsonl');
  const txtPath   = path.join(outDir, 'prompts.txt');
  const metaPath  = path.join(outDir, 'source_meta.txt');

  // jsonl: one object per line
  fs.writeFileSync(
    jsonlPath,
    prompts.map((q, i) => JSON.stringify({ idx: i + 1, query: q })).join('\n') + '\n',
    'utf8'
  );

  // plain text list
  fs.writeFileSync(
    txtPath,
    prompts.map((q, i) => `#${i + 1}\n${q}\n`).join('\n'),
    'utf8'
  );

  // source info
  fs.writeFileSync(
    metaPath,
    `CSV Source: ${path.resolve(csvFile)}\nTotal Prompts: ${prompts.length}\nSaved: ${new Date().toISOString()}\n`,
    'utf8'
  );

  console.log(`📦 Saved prompts catalog to:\n  - ${jsonlPath}\n  - ${txtPath}\n  - ${metaPath}`);
}

/**
 * Process a batch of prompts using an already-opened page/browser.
 */
async function processPromptsBatch(page, prompts, outDir) {
  if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });

  // derive category + model from outDir
  const [category, ...modelParts] = path.basename(outDir).split('_');
  const model = modelParts.join('_');

  const harsDir = path.join(outDir, `${category}_hars_${model}`);
  const responsesDir = path.join(outDir, `${category}_responses_${model}`);
  if (!fs.existsSync(harsDir)) fs.mkdirSync(harsDir, { recursive: true });
  if (!fs.existsSync(responsesDir)) fs.mkdirSync(responsesDir, { recursive: true });

  // Make sure user agent and prompt box are ready
  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
  );
  await page.goto('https://chatgpt.com/', { waitUntil: 'load' });
  await sleep(10000);

  await page.waitForSelector('div.ProseMirror#prompt-textarea', { timeout: 0 });

  for (let idx = 0; idx < prompts.length; idx++) {
    const promptText = prompts[idx];
    console.log(`\n=== [${path.basename(outDir)}] Prompt ${idx + 1}/${prompts.length}: "${promptText}" ===`);

    const safeBase = `prompt-${idx + 1}`;
    const harFilename = path.join(harsDir, `network-logs-${safeBase}.har`);
    // const sseFilename = path.join(outDir, `sse-stream-${safeBase}.txt`);
    const answerFilename = path.join(responsesDir, `response-${safeBase}.txt`);

    const har = new PuppeteerHar(page);
    await har.start({ path: harFilename, omitContent: false });

    let finalAnswer = '';
    let sseText = '';

    try {
      // Enable web search 
      await enableWebSearch(page);

      // Prepare SSE listener
      const sseResponsePromise = page.waitForResponse(
        response => response.headers()['content-type']?.includes('event-stream'),
        { timeout: 120000 }
      );

      // Type + submit
      const editor = await page.waitForSelector('div.ProseMirror#prompt-textarea', { timeout: 15000 });
      await editor.click({ delay: 100 });
      await page.keyboard.down('Control');
      await page.keyboard.press('A');
      await page.keyboard.up('Control');
      await editor.type(promptText, { delay: 50 });
      await editor.press('Enter');

      // Capture SSE
      try {
        const sseResponse = await sseResponsePromise;
        sseText = await sseResponse.text();
      } catch (e) {
        console.warn('⚠️ SSE response not captured:', e.message);
      }

      // Wait for answer stabilization
      finalAnswer = await waitForResponseStability(page, {
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

      // // Save SSE stream raw (optional)
      // try {
      //   fs.writeFileSync(sseFilename, sseText);
      //   console.log(`✅ SSE stream saved to ${sseFilename}`);
      // } catch (e) {
      //   console.warn('Failed to write SSE file:', e.message || e);
      // }

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

    // Start a new chat
    await page.keyboard.down('Control');
    await page.keyboard.down('Shift');
    await page.keyboard.press('O');
    await page.keyboard.up('Shift');
    await page.keyboard.up('Control');
    await sleep(3000);
  }
}

(async () => {
  // --- Configure your CSVs and output dirs here ---

  const modelName = "gpt-5"
  const datasetRunNum = "1"

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
    // {
    //   file: `./dataset/ORCAS-I-gold_label_Factual.csv`,
    //   outDir: `factual_${modelName}_${datasetRunNum}`
    // },
    {
      file: `./dataset/ORCAS-I-gold_label_Abstain.csv`,
      outDir: `abstain_${modelName}_${datasetRunNum}`
    }
  ];

  // Load + pre-save all prompts per CSV before launching browser
  const jobData = csvJobs.map(job => {
    const prompts = loadCsvPrompts(job.file);
    console.log(`Loaded ${prompts.length} prompts from ${job.file}`);
    // Save a local catalog of prompts per CSV
    savePromptsCatalog(job.outDir, job.file, prompts);
    return { ...job, prompts };
  });

  const browser = await puppeteer.launch({
    headless: false,
    userDataDir: './puppeteer-profile',
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--start-maximized']
  });

  try {
    const [page] = await browser.pages();

    // Process CSVs sequentially: first job, then next, etc.
    for (const { file, outDir, prompts } of jobData) {
      console.log(`\n==============================`);
      console.log(`▶ Processing CSV: ${file}`);
      console.log(`▶ Output Dir:    ${outDir}`);
      console.log(`==============================\n`);
      await processPromptsBatch(page, prompts, outDir);
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