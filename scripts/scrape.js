const { chromium } = require('playwright');
const https = require('https');

const SNOTEL_URL = 'https://wcc.sc.egov.usda.gov/reportGenerator/view_csv/customSingleStationReport/daily/936:CO:SNTL%7Cid=%22%22%7Cname/-2,0/WTEQ::value,WTEQ::median_1991,PREC::value,PREC::median_1991';

function fetchText(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => resolve(data));
    }).on('error', reject);
  });
}

async function fetchSnowpack() {
  try {
    const text = await fetchText(SNOTEL_URL);
    // Skip comment/header lines starting with '#', find the data rows
    const lines = text.split('\n').filter(l => l.trim() && !l.startsWith('#'));
    // Last non-empty data line is the most recent reading
    const dataLine = lines[lines.length - 1];
    if (!dataLine) return null;

    const cols = dataLine.split(',');
    // Columns: Date, SWE value, SWE median, Precip value, Precip median
    const [date, sweRaw, sweMedianRaw, precRaw, precMedianRaw] = cols;

    const swe        = parseFloat(sweRaw);
    const sweMedian  = parseFloat(sweMedianRaw);
    const prec       = parseFloat(precRaw);
    const precMedian = parseFloat(precMedianRaw);

    return {
      snowpackDate:        date ? date.trim() : null,
      snowpackSWE:         isNaN(swe)        ? null : swe,
      snowpackSWEMedian:   isNaN(sweMedian)  ? null : sweMedian,
      snowpackSWEPct:      (!isNaN(swe) && !isNaN(sweMedian) && sweMedian > 0)
                             ? Math.round((swe / sweMedian) * 100) : null,
      precipSeason:        isNaN(prec)       ? null : prec,
      precipSeasonMedian:  isNaN(precMedian)  ? null : precMedian,
      precipPct:           (!isNaN(prec) && !isNaN(precMedian) && precMedian > 0)
                             ? Math.round((prec / precMedian) * 100) : null,
    };
  } catch (err) {
    console.error('Snowpack fetch failed:', err.message);
    return null;
  }
}

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
  console.log('Scraper starting:', new Date().toISOString());
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

    console.log('Login successful');

    // Step 3: Dashboard
    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 60000 });

    // Verify we actually landed on the dashboard, not a redirect back to login
    const currentUrl = page.url();
    if (!currentUrl.includes('/Consumer/')) {
      throw new Error('Login failed or session expired — not on dashboard: ' + currentUrl);
    }

    // Wait until the dashboard has actually populated numeric values (AJAX)
    await page.waitForFunction(() => {
      const body = document.body.innerText;
      return body.includes('So far this cycle') &&
             body.includes('Daily Average') &&
             /[\d,]+\.?\d*\s*G/.test(body);
    }, { timeout: 60000 });

    // Extra wait for JS rendering
    await page.waitForTimeout(3000);

    // Scrape raw values from the full page body
    const raw = await page.evaluate(() => {
      const bar = document.querySelector('#meterpanetopheaderbar');
      if (!bar) return null;

      const barText  = bar.innerText;
      const bodyText = document.body.innerText;
      const result   = {};

      // LCD read, water budget, billing read — from header bar
      const lcdMatch     = barText.match(/LCD Read[:\s]+([^\n]+)/i);
      const budgetMatch  = barText.match(/Water Budget[:\s]+([^\n]+)/i);
      const billingMatch = barText.match(/Billing Read[:\s]+([\d,]+\.?\d*)/i);

      if (lcdMatch)     result.lcdRead          = lcdMatch[1].trim();
      if (budgetMatch)  result.waterBudgetStatus = budgetMatch[1].trim();
      if (billingMatch) result.billingRead       = parseFloat(billingMatch[1].replace(/,/g, ''));

      // soFarThisCycle and dailyAverage — parse from full body (left panel)
      const cycleMatch = bodyText.match(/So far this cycle[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      const dailyMatch = bodyText.match(/Daily Average[\s\S]*?([\d,]+\.?\d*)\s*G/i);

      result.soFarThisCycle = cycleMatch ? parseFloat(cycleMatch[1].replace(/,/g, '')) : 0;
      result.dailyAverage   = dailyMatch ? parseFloat(dailyMatch[1].replace(/,/g, '')) : 0;

      result.rawText = barText;

      // Irrigation consumption — from Irrigation Analysis section of body
      const irrMatch = bodyText.match(/Irrigation Analysis[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      const irrVal   = irrMatch ? parseFloat(irrMatch[1].replace(/,/g, '')) : 0;
      // Sanity check: anything over 10000 G is clearly a misparse
      result.irrigationGallons = irrVal > 10000 ? 0 : irrVal;

      return result;
    });

    // Capture full body text after main evaluate — includes Residential Analysis panel
    const bodyText = await page.evaluate(() => document.body.innerText);
    raw.rawText = bodyText;

    if (!raw) {
      throw new Error('Could not find #meterpanetopheaderbar on page');
    }
    console.log('Dashboard loaded, soFarThisCycle:', raw.soFarThisCycle);

    // Parse fixture breakdown — line-by-line from Residential Analysis section
    const resStart = bodyText.indexOf('Residential Analysis');
    const resEnd   = bodyText.indexOf('Meter Information', resStart);
    const resText  = resStart === -1 ? '' : (resEnd === -1 ? bodyText.slice(resStart) : bodyText.slice(resStart, resEnd));
    const resLines = resText.split('\n').map(l => l.trim()).filter(Boolean);

    function getFixtureValue(lines, label) {
      const idx = lines.findIndex(l => l.toLowerCase().includes(label.toLowerCase()));
      if (idx < 1) return 0;
      for (let i = idx - 1; i >= 0; i--) {
        const match = lines[i].match(/^([\d.]+)\s*G?$/);
        if (match) return Math.round(parseFloat(match[1]));
      }
      return 0;
    }

    raw.fixtures = {
      toilet:         getFixtureValue(resLines, 'Toilet'),
      sink:           getFixtureValue(resLines, 'Sink'),
      shower:         getFixtureValue(resLines, 'Shower'),
      kitchen:        getFixtureValue(resLines, 'Kitchen'),
      bathtub:        getFixtureValue(resLines, 'Bath Tub'),
      washingMachine: getFixtureValue(resLines, 'Washing Machine'),
      date:           null, // set below once consumptionDate is computed
    };
    console.log('fixtures:', JSON.stringify(raw.fixtures));

    // Fetch snowpack data (non-fatal if it fails)
    const snow = await fetchSnowpack();
    const snowFields = snow ?? {
      snowpackDate: null, snowpackSWE: null, snowpackSWEMedian: null,
      snowpackSWEPct: null, precipSeason: null, precipSeasonMedian: null, precipPct: null,
    };

    // Compute enriched fields
    const now = new Date();
    const yesterday = new Date(now);
    yesterday.setDate(yesterday.getDate() - 1);
    const consumptionDate = yesterday.toISOString().split('T')[0];
    // Patch fixtures date now that consumptionDate is defined
    raw.fixtures.date = consumptionDate;
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

      const yesterdayKey = `waterwise:${consumptionDate}`;
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

      // Parse yesterday's consumption from body text
      const consumptionMatch = bodyText.match(/Water Consumption[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      const waterConsumptionToday = consumptionMatch
        ? parseFloat(consumptionMatch[1].replace(/,/g, ''))
        : 0;

      // Compute 7-day rolling average from daily Redis keys
      const historyVals = [];
      for (let i = 1; i <= 7; i++) {
        const d = new Date(now);
        d.setDate(d.getDate() - i);
        const key = `waterwise:${d.toISOString().slice(0, 10)}`;
        const raw7 = await redis.get(key);
        if (raw7) {
          const entry = JSON.parse(raw7);
          if (entry.waterConsumptionToday > 0) historyVals.push(entry.waterConsumptionToday);
        }
      }
      const sevenDayAvg = historyVals.length > 0
        ? historyVals.reduce((a, b) => a + b, 0) / historyVals.length
        : dailyAverage;

      const spikeAlert = waterConsumptionToday > 200 &&
        sevenDayAvg > 0 &&
        waterConsumptionToday > sevenDayAvg * 2;

      const spikeMultiplier = spikeAlert
        ? (waterConsumptionToday / sevenDayAvg).toFixed(1)
        : null;

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
        waterConsumptionToday,
        sevenDayAvg:         Math.round(sevenDayAvg * 10) / 10,
        spikeAlert,
        spikeMultiplier,
        fixtures:            raw.fixtures,
        consumptionDate,
        ...snowFields,
      };

      // Fetch daily interval data using Playwright's request API (shares browser session cookies)
      try {
        const startLogDate = `${consumptionDate} 12:21:44 AM`;
        const endLogDate   = startLogDate;

        const intervalResp = await page.request.post(
          'https://waterscope.us/Consumer/Consumption/ConsumptionHistoryDataClaculation',
          {
            headers: { 'X-Requested-With': 'XMLHttpRequest' },
            form: {
              numberOfDays: '1',
              AccountId:    '1735',
              MeterId:      '3208158',
              startLogDate,
              endLogDate,
            },
          }
        );

        console.log('Interval response status:', intervalResp.status());
        const body = await intervalResp.text();
        console.log('Interval response body (first 500):', body.slice(0, 500));

        if (intervalResp.ok()) {
          let intervalData;
          try {
            intervalData = JSON.parse(body);
          } catch (parseErr) {
            console.error('Interval JSON parse failed — response was not JSON:', parseErr.message);
          }

          if (intervalData) {
            const intervalKey = `waterwise:intervals:${consumptionDate}`;
            await redis.set(intervalKey, JSON.stringify(intervalData), 'EX', 7776000);
            console.log('Interval data saved to', intervalKey);
          }
        } else {
          console.warn('Interval fetch non-OK status:', intervalResp.status());
        }
      } catch (e) {
        console.error('Interval fetch failed (non-fatal):', e.message);
      }

      const dateKey = `waterwise:${consumptionDate}`;
      console.log('Saving to Redis...');
      await Promise.all([
        redis.set('waterwise:latest', JSON.stringify(payload)),
        redis.set(dateKey, JSON.stringify(payload), 'EX', 7776000),
      ]);
      await redis.quit();

      console.log('SUCCESS: Redis updated', dateKey, 'soFarThisCycle:', payload.soFarThisCycle);
      console.log(JSON.stringify(payload, null, 2));

      const { sendAlerts } = require('./email-alert.js');
      try {
        await sendAlerts(payload);
      } catch (e) {
        console.error('Alert email failed:', e.message);
      }
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
        fixtures: raw.fixtures,
        consumptionDate,
        ...snowFields,
      };
      console.log('No REDIS_URL — scraped data:');
      console.log(JSON.stringify(payload, null, 2));
    }
  } finally {
    if (browser) await browser.close();
  }
}

main().catch((err) => {
  console.error('SCRAPER FAILED at:', new Date().toISOString(), err.message);
  process.exit(1);
});
