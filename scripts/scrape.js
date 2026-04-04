const { chromium } = require('playwright');

// Tier thresholds in gallons
const TIER_THRESHOLDS = [0, 3800, 7600, 11400, 15200];

// Rates per 1000 gallons by drought level (index = tier-1)
const RATES = {
  1: [2.89, 3.85,  7.70, 11.54, 15.39],
  2: [2.89, 3.85, 11.54, 17.33, 23.09],
  3: [3.85, 5.78, 23.09, 34.63, 46.17],
};

function getTier(gallons) {
  for (let i = TIER_THRESHOLDS.length - 1; i >= 0; i--) {
    if (gallons >= TIER_THRESHOLDS[i]) return i + 1;
  }
  return 1;
}

function calcCost(gallons, droughtLevel) {
  const rates = RATES[droughtLevel] || RATES[1];
  const costByTier = [0, 0, 0, 0, 0];
  let remaining = gallons;

  for (let i = 0; i < TIER_THRESHOLDS.length; i++) {
    const low = TIER_THRESHOLDS[i];
    const high = TIER_THRESHOLDS[i + 1] ?? Infinity;
    if (remaining <= 0) break;
    const inThisTier = Math.min(remaining, high - low);
    costByTier[i] = (inThisTier / 1000) * rates[i];
    remaining -= inThisTier;
  }

  const total = costByTier.reduce((a, b) => a + b, 0);
  return { total: Math.round(total * 100) / 100, costByTier: costByTier.map(c => Math.round(c * 100) / 100) };
}

function parseNumber(str) {
  if (str == null) return null;
  const n = parseFloat(String(str).replace(/[^0-9.]/g, ''));
  return isNaN(n) ? null : n;
}

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

    // Scrape raw values from the header bar and irrigation panel
    const raw = await page.evaluate(() => {
      const bar = document.querySelector('#meterpanetopheaderbar');
      if (!bar) return null;

      const allText = bar.innerText;
      const result = {};

      const lcdMatch     = allText.match(/LCD Read[:\s]+([^\n]+)/i);
      const dailyMatch   = allText.match(/Daily Average[:\s]+([\d.]+)/i);
      const cycleMatch   = allText.match(/So Far This Cycle[:\s]+([\d.]+)/i);
      const budgetMatch  = allText.match(/Water Budget[:\s]+([^\n]+)/i);
      const billingMatch = allText.match(/Billing Read[:\s]+([\d.]+)/i);

      if (lcdMatch)     result.lcdRead           = lcdMatch[1].trim();
      if (dailyMatch)   result.dailyAverage       = parseFloat(dailyMatch[1]);
      if (cycleMatch)   result.soFarThisCycle     = parseFloat(cycleMatch[1]);
      if (budgetMatch)  result.waterBudgetStatus  = budgetMatch[1].trim();
      if (billingMatch) result.billingRead        = parseFloat(billingMatch[1]);

      result.rawText = allText;

      // Irrigation consumption — look anywhere on the page for the irrigation panel
      const irrEl = document.querySelector('[id*="irrigation"], [class*="irrigation"]');
      const irrText = irrEl ? irrEl.innerText : document.body.innerText;
      const irrMatch = irrText.match(/Irrigation[^]*?consumption[^]*?([\d.]+)\s*G/i)
                    || irrText.match(/([\d.]+)\s*G[^]*?irrigation/i);
      result.irrigationGallons = irrMatch ? parseFloat(irrMatch[1]) : 0;

      return result;
    });

    if (!raw) {
      throw new Error('Could not find #meterpanetopheaderbar on page');
    }

    // Compute enriched fields
    const now = new Date();
    const billingCycleDay = now.getDate();
    const daysInMonth = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    const daysRemaining = daysInMonth - billingCycleDay;
    const droughtLevel = 1; // hardcoded — update manually when EMD changes

    const dailyAverage   = raw.dailyAverage   ?? 0;
    const soFarThisCycle = raw.soFarThisCycle ?? 0;

    const projectedTotal = Math.round(dailyAverage * daysInMonth);
    const currentTier    = getTier(soFarThisCycle);
    const nextThreshold  = TIER_THRESHOLDS[currentTier] ?? Infinity;
    const galsTilNextTier   = currentTier < 5 ? Math.round(nextThreshold - soFarThisCycle) : 0;
    const daysUntilTierCross = dailyAverage > 0 && currentTier < 5
      ? Math.round((galsTilNextTier / dailyAverage) * 10) / 10
      : null;
    const projectedTier = getTier(projectedTotal);

    const { total: costSoFar,     costByTier } = calcCost(soFarThisCycle, droughtLevel);
    const { total: projectedCost              } = calcCost(projectedTotal,  droughtLevel);

    let nudge = 'none';
    if (currentTier >= 3)        nudge = 'in_tier_3plus';
    else if (currentTier === 2)  nudge = 'in_tier_2';
    else if (galsTilNextTier < 500) nudge = 'approaching';

    // Compare with yesterday for alert flags
    let tierCrossedToday    = false;
    let approachingTierAlert = false;

    if (redisUrl) {
      const Redis = require('ioredis');
      const redis = new Redis(redisUrl);

      const yesterday = new Date(now);
      yesterday.setDate(yesterday.getDate() - 1);
      const yesterdayKey = `waterwise:${yesterday.toISOString().slice(0, 10)}`;
      const yesterdayRaw = await redis.get(yesterdayKey);

      if (yesterdayRaw) {
        const prev = JSON.parse(yesterdayRaw);
        if (prev.currentTier != null && currentTier > prev.currentTier) {
          tierCrossedToday = true;
        }
        if (prev.galsTilNextTier != null && prev.galsTilNextTier >= 500 && galsTilNextTier < 500) {
          approachingTierAlert = true;
        }
      }

      const payload = {
        // Raw
        lcdRead:            raw.lcdRead ?? null,
        soFarThisCycle,
        dailyAverage,
        waterBudgetStatus:  raw.waterBudgetStatus ?? null,
        billingRead:        raw.billingRead ?? null,
        irrigationGallons:  raw.irrigationGallons,
        rawText:            raw.rawText,
        scrapedAt:          now.toISOString(),
        // Computed
        billingCycleDay,
        daysInMonth,
        daysRemaining,
        projectedTotal,
        droughtLevel,
        currentTier,
        galsTilNextTier,
        daysUntilTierCross,
        projectedTier,
        costSoFar,
        projectedCost,
        costByTier,
        nudge,
        hasIrrigation:       raw.irrigationGallons > 0,
        tierCrossedToday,
        approachingTierAlert,
      };

      const dateKey = `waterwise:${now.toISOString().slice(0, 10)}`;
      await Promise.all([
        redis.set('waterwise:latest', JSON.stringify(payload)),
        redis.set(dateKey, JSON.stringify(payload), 'EX', 7776000),
      ]);
      await redis.quit();

      console.log(`Saved to Redis: waterwise:latest and ${dateKey}`);
      console.log(JSON.stringify(payload, null, 2));
    } else {
      // No Redis — just log for local debugging
      const payload = {
        ...raw,
        scrapedAt: now.toISOString(),
        billingCycleDay, daysInMonth, daysRemaining, projectedTotal, droughtLevel,
        currentTier, galsTilNextTier, daysUntilTierCross, projectedTier,
        costSoFar, projectedCost, costByTier,
        nudge, hasIrrigation: raw.irrigationGallons > 0,
        tierCrossedToday, approachingTierAlert,
      };
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
