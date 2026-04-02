const { chromium } = require('playwright');
const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

async function main() {
  const email = process.env.WATERSCOPE_EMAIL;
  const password = process.env.WATERSCOPE_PASSWORD;

  if (!email || !password) {
    throw new Error('Missing WATERSCOPE_EMAIL or WATERSCOPE_PASSWORD env vars');
  }

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // Navigate to waterscope.us — Azure B2C will redirect to login
    await page.goto('https://waterscope.us', { waitUntil: 'networkidle' });

    // Fill Azure B2C login form
    await page.fill('input[type="email"], input[name="email"], #email', email);
    await page.fill('input[type="password"], input[name="password"], #password', password);
    await page.click('button[type="submit"], input[type="submit"], #next');

    // Wait for dashboard to load after login
    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 30000 });

    // Scrape the meter stats from the header bar
    const data = await page.evaluate(() => {
      const bar = document.querySelector('#meterpanetopheaderbar');
      if (!bar) return null;

      const allText = bar.innerText;
      const result = {};

      const lcdMatch = allText.match(/LCD Read[:\s]+([^\n]+)/i);
      const dailyMatch = allText.match(/Daily Average[:\s]+([^\n]+)/i);
      const cycleMatch = allText.match(/So Far This Cycle[:\s]+([^\n]+)/i);
      const budgetMatch = allText.match(/Water Budget[:\s]+([^\n]+)/i);

      if (lcdMatch) result.lcdRead = lcdMatch[1].trim();
      if (dailyMatch) result.dailyAverage = dailyMatch[1].trim();
      if (cycleMatch) result.soFarThisCycle = cycleMatch[1].trim();
      if (budgetMatch) result.waterBudgetStatus = budgetMatch[1].trim();

      result.rawText = allText;
      return result;
    });

    if (!data) {
      throw new Error('Could not find #meterpanetopheaderbar on page');
    }

    const payload = {
      ...data,
      scrapedAt: new Date().toISOString(),
    };

    await redis.set('waterwise:latest', JSON.stringify(payload));
    console.log('Saved:', JSON.stringify(payload, null, 2));
  } finally {
    if (browser) await browser.close();
    await redis.quit();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
