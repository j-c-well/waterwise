const { chromium } = require('playwright');

async function main() {
  const email = process.env.WATERSCOPE_EMAIL;
  const password = process.env.WATERSCOPE_PASSWORD;
  const redisUrl = process.env.REDIS_URL;

  if (!email || !password) {
    throw new Error('Missing WATERSCOPE_EMAIL or WATERSCOPE_PASSWORD env vars');
  }

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // Step 1: WaterScope email lookup, click Continue and wait for Azure B2C
    await page.goto('https://waterscope.us/Home/Main', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.fill('#txtSearchUserName', email);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#searchUserName'),
    ]);

    // Step 2: Azure B2C password entry (email pre-filled)
    await page.waitForSelector('#password', { timeout: 60000 });
    await page.fill('#password', password);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#next'),
    ]);

    // Step 3: Dashboard
    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 60000 });

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

    if (redisUrl) {
      const Redis = require('ioredis');
      const redis = new Redis(redisUrl);
      const dateKey = `waterwise:${new Date().toISOString().slice(0, 10)}`;
      await Promise.all([
        redis.set('waterwise:latest', JSON.stringify(payload)),
        redis.set(dateKey, JSON.stringify(payload), 'EX', 7776000),
      ]);
      await redis.quit();
      console.log(`Saved to Redis: waterwise:latest and ${dateKey}`);
      console.log(JSON.stringify(payload, null, 2));
    } else {
      console.log('No REDIS_URL — scraped data:');
      console.log(JSON.stringify(payload, null, 2));
    }
  } finally {
    if (browser) await browser.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
