const puppeteer = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');
const fs = require('fs');
const PuppeteerHar = require('puppeteer-har');

require('dotenv').config()

puppeteer.use(StealthPlugin());


(async () => {
  // Read prompt from CLI
  // const promptText = process.argv.slice(2).join(' ');
  // if (!promptText) {
  //   console.error('Usage: node chatgpt_automation.js "Your prompt here"');
  //   process.exit(1);
  // }

  const browser = await puppeteer.launch({
    headless: false,
    userDataDir: './puppeteer-profile',
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--start-maximized'
    ]
  });
  
  const [page] = await browser.pages();

  // Start HAR capture
  const har = new PuppeteerHar(page);
  const harPath = 'network-logs.har';
  await har.start({ path: harPath, omitContent: false });

  await page.setUserAgent(
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'
  );

  await page.goto('https://chatgpt.com/', { waitUntil: 'load' });
  await new Promise(resolve => setTimeout(resolve, 5000));

  await page.waitForSelector('button[data-testid="login-button"]', { timeout: 10000 });
  await page.click('button[data-testid="login-button"]');

  // 2) Wait for the email field, type it, and hit Continue
  await page.waitForSelector('input[name="email"]', { timeout: 10000 });
  await page.type('input[name="email"]', process.env.OPENAI_EMAIL, { delay: 50 });

  // click the “Continue” button (value="email")
  await Promise.all([
    page.click('button[type="submit"][value="email"]'),
    page.waitForNavigation({ waitUntil: 'networkidle2' })
  ]);

  // Wait for the password input (using its name="current-password")
  await page.waitForSelector('input[name="current-password"]', { timeout: 10000 });  
  await page.type('input[name="current-password"]', process.env.OPENAI_PASSWORD, { delay: 50 });

  // Click the “Continue” button under the password field
  // You can target it by its primary‐button classes:
  await Promise.all([
    page.click('button._root_625o4_51._primary_625o4_86'),
    page.waitForNavigation({ waitUntil: 'networkidle2' }),
  ]);

  // 4) PAUSE FOR 2FA CODE ENTRY
  console.log('✋ Please check your email, enter the 2FA code in the browser, and complete login.');
  
  await browser.close();
})();