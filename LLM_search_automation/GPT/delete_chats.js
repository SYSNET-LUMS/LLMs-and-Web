// ⚠️ Permanent deletion.

const puppeteer = require('puppeteer-extra');
const Stealth = require('puppeteer-extra-plugin-stealth');
puppeteer.use(Stealth());

const sleep = ms => new Promise(r => setTimeout(r, ms));

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    userDataDir: './puppeteer-profile',
    args: ['--no-sandbox','--disable-setuid-sandbox','--start-maximized']
  });
  const [page] = await browser.pages();

  await page.goto('https://chatgpt.com/', { waitUntil: 'domcontentloaded' });
  await page.waitForSelector('a[href^="/c/"]', { timeout: 60000 });

  const deleteOnce = async () => {
    // Re-query the first chat row every time to avoid stale handles
    const row = await page.$('a[href^="/c/"]');
    if (!row) return false;

    // Hover via mouse coords
    let box = await row.boundingBox();
    if (!box) {
      try { await row.evaluate(el => el.scrollIntoView({ block:'center' })); } catch {}
      await sleep(50);
      box = await row.boundingBox();
      if (!box) return false;
    }
    await page.mouse.move(box.x + box.width / 2, box.y + box.height / 2);
    await sleep(100);

    // Only the hovered row shows the options button
    await page.waitForSelector('button[data-testid$="-options"]', { visible: true, timeout: 2000 })
      .catch(() => { throw new Error('options-not-visible'); });

    // Click the visible options
    await page.click('button[data-testid$="-options"]');
    await page.waitForSelector('[data-radix-menu-content]', { timeout: 5000 });

    // Click "Delete" in the menu
    await page.evaluate(() => {
      const items = Array.from(document.querySelectorAll('[data-radix-menu-content] [role="menuitem"], [data-radix-menu-content] button'));
      const t = s => (s || '').trim().toLowerCase();
      const del = items.find(i => t(i.textContent) === 'delete' || t(i.textContent) === 'delete conversation');
      if (del) del.click();
    });

    // Confirm "Delete" in dialog
    await page.waitForSelector('[role="dialog"], [data-state="open"]', { timeout: 5000 });
    await page.evaluate(() => {
      const btns = Array.from(document.querySelectorAll('[role="dialog"] button, [data-state="open"] button'));
      const t = s => (s || '').trim().toLowerCase();
      const yes = btns.find(b => ['delete','yes, delete','confirm delete','permanently delete'].includes(t(b.textContent)));
      if (yes) yes.click();
    });

    await sleep(1000);
    return true;
  };

  // Loop until no chats left
  while (true) {
    try {
      const ok = await deleteOnce();
      if (!ok) break;
      console.log('🗑 Deleted one chat');
    } catch (e) {
      if (String(e.message || e).includes('detached') || String(e.message || e).includes('options-not-visible')) {
        // Re-try one time (DOM likely re-rendered mid-action)
        try {
          await sleep(150);
          const ok = await deleteOnce();
          if (!ok) break;
          console.log('🗑 Deleted one chat (after retry)');
        } catch (e2) {
          console.warn('⚠️ Skipping one due to rapid re-render:', e2.message || e2);
        }
      } else {
        console.warn('⚠️ Skipping one:', e.message || e);
      }
    }
    // Let list re-render; do NOT hold handles across iterations
    await sleep(100);
  }

  console.log('✅ Done (no more visible chats).');
  await browser.close();
})();