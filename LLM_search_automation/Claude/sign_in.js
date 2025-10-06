#!/usr/bin/env node
// sign_in.js
//
// What it does:
// - Opens https://claude.ai/login
// - Does NOTHING else. You complete sign-in manually. In this case, automation proved to be an inconvenience, so it was avoided :)
// - Close the browser window yourself when done (session will be saved).

const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteer.use(StealthPlugin());

(async () => {
  const browser = await puppeteer.launch({
    headless: false,
    userDataDir: './puppeteer-profile-claude', // persists your session across runs
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--start-maximized',
    ],
    defaultViewport: null,
  });

  try {
    const [page] = await browser.pages();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
    );

    // Go to Claude login and… that’s it.
    await page.goto('https://claude.ai/login', {
      waitUntil: 'domcontentloaded',
      timeout: 60000,
    });

    console.log('\n👋 Browser is open. Sign in to Claude manually.');
    console.log('💾 Your session will be saved in ./puppeteer-profile-claude.');
    console.log('✅ When you’re done (or already signed in), just close the browser window.\n');

    // Do nothing else; leave it fully under your control.
    // Keep the Node process alive until the browser is closed.
    await browser.waitForTarget(() => false, { timeout: 0 });
  } catch (err) {
    console.error('Launcher error:', err);
    // Leave browser open if possible for inspection
  }
})();