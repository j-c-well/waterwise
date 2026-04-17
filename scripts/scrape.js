const { chromium } = require('playwright');
const https = require('https');
const path  = require('path');
const { spawn } = require('child_process');
const { sendAlertEmail } = require('./email-alert');

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

// ── Interval timestamp validation ────────────────────────────────────────────
// Parses the Microsoft /Date(ms)/ format used by Metron.
function parseMsDate(val) {
  if (!val) return null;
  const m = String(val).match(/\/Date\((-?\d+)\)\//);
  if (m) return new Date(parseInt(m[1], 10));
  const d = new Date(val);
  return isNaN(d) ? null : d;
}

// Extract a representative timestamp from the first or last row of captured interval data.
function sampleIntervalDate(data) {
  const rows = Array.isArray(data) ? data
             : Array.isArray(data?.data) ? data.data
             : Array.isArray(data?.Data) ? data.Data
             : (() => { for (const v of Object.values(data ?? {})) if (Array.isArray(v) && v.length) return v; return []; })();
  if (!rows.length) return null;
  const row = rows[0];
  const raw = row.ConsumptionChartDate ?? row.consumptionChartDate ??
              row.StartTime ?? row.startTime ?? row.Timestamp ?? row.timestamp ?? null;
  return parseMsDate(raw);
}

// Returns true if the sampled date falls within the expected YYYY-MM-DD date.
// Uses UTC date comparison because Metron stores local Mountain time as UTC.
function intervalDateMatches(data, expectedDate) {
  const sampled = sampleIntervalDate(data);
  if (!sampled) return true; // can't validate — allow through
  const sampledDate = sampled.toISOString().slice(0, 10);
  return sampledDate === expectedDate;
}

// ── Multi-user helpers ────────────────────────────────────────────────────────

function runCorrectionsForUser(date, userId) {
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [path.join(__dirname, 'corrections.js'), date, '--userId', userId],
      { env: process.env, stdio: 'pipe' }
    );
    const lines = [];
    child.stdout.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.stderr.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.on('close', code => resolve({ code, lines }));
  });
}

async function scrapeUser({ email, password, userId, redis, now, snowFields, consumptionDate }) {
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    const capturedIntervals = { data: null, url: null };
    page.on('response', async (response) => {
      const url = response.url();
      if (!url.includes('waterscope.us')) return;
      const type = response.request().resourceType();
      if (type !== 'xhr' && type !== 'fetch') return;
      if (/Consumption|Interval|History|Usage/i.test(url)) {
        try {
          const text = await response.text();
          const json = JSON.parse(text);
          if (Array.isArray(json) && json.length > 0) {
            capturedIntervals.data = json;
            capturedIntervals.url  = url;
          } else if (json && typeof json === 'object') {
            capturedIntervals.data = json;
            capturedIntervals.url  = url;
          }
        } catch (_) { /* not JSON */ }
      }
    });

    await page.goto('https://waterscope.us/Home/Main', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.fill('#txtSearchUserName', email);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#searchUserName'),
    ]);
    await page.waitForSelector('#password', { timeout: 60000 });
    await page.fill('#password', password);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#next'),
    ]);

    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 60000 });
    const currentUrl = page.url();
    if (!currentUrl.includes('/Consumer/')) {
      throw new Error('Login failed — not on dashboard: ' + currentUrl);
    }

    await page.waitForFunction(() => {
      const body = document.body.innerText;
      return body.includes('So far this cycle') && /[\d,]+\.?\d*\s*G/.test(body);
    }, { timeout: 60000 });
    await page.waitForTimeout(3000);

    const raw = await page.evaluate(() => {
      const bar = document.querySelector('#meterpanetopheaderbar');
      if (!bar) return null;
      const barText  = bar.innerText;
      const bodyText = document.body.innerText;
      const result   = {};
      const lcdMatch     = barText.match(/LCD Read[:\s]+([^\n]+)/i);
      const budgetMatch  = barText.match(/Water Budget[:\s]+([^\n]+)/i);
      const billingMatch = barText.match(/Billing Read[:\s]+([\d,]+\.?\d*)/i);
      if (lcdMatch)     result.lcdRead          = lcdMatch[1].trim();
      if (budgetMatch)  result.waterBudgetStatus = budgetMatch[1].trim();
      if (billingMatch) result.billingRead       = parseFloat(billingMatch[1].replace(/,/g, ''));
      const cycleMatch = bodyText.match(/So far this cycle[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      const dailyMatch = bodyText.match(/Daily Average[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      result.soFarThisCycle = cycleMatch ? parseFloat(cycleMatch[1].replace(/,/g, '')) : 0;
      result.dailyAverage   = dailyMatch ? parseFloat(dailyMatch[1].replace(/,/g, '')) : 0;
      const irrMatch = bodyText.match(/Irrigation Analysis[\s\S]*?([\d,]+\.?\d*)\s*G/i);
      const irrVal   = irrMatch ? parseFloat(irrMatch[1].replace(/,/g, '')) : 0;
      result.irrigationGallons = irrVal > 10000 ? 0 : irrVal;
      result.rawText = bodyText;
      return result;
    });

    if (!raw) throw new Error('Could not scrape dashboard');

    const bodyText = await page.evaluate(() => document.body.innerText);
    raw.rawText = bodyText;

    const waterMatch = bodyText.match(/Water Consumption\s*\n\s*([\d.]+)\s*G/i);
    const waterConsumptionToday = waterMatch ? parseFloat(waterMatch[1]) : null;

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
      date:           consumptionDate,
    };

    const billingCycleDay = now.getDate();
    const daysInMonth     = new Date(now.getFullYear(), now.getMonth() + 1, 0).getDate();
    const daysRemaining   = daysInMonth - billingCycleDay;
    const droughtLevel    = 1;
    const dailyAverage    = raw.dailyAverage   ?? 0;
    const soFarThisCycle  = raw.soFarThisCycle ?? 0;
    const projectedTotal  = Math.round(dailyAverage * daysInMonth);
    const currentTier     = getTier(soFarThisCycle);
    const nextThreshold   = TIER_THRESHOLDS[currentTier] ?? Infinity;
    const galsTilNextTier = currentTier < 5 ? Math.round(nextThreshold - soFarThisCycle) : 0;
    const daysUntilTierCross = dailyAverage > 0 && currentTier < 5
      ? Math.round((galsTilNextTier / dailyAverage) * 10) / 10 : null;
    const projectedTier  = getTier(projectedTotal);
    const { total: costSoFar,    costByTier } = calcCost(soFarThisCycle, droughtLevel);
    const { total: projectedCost             } = calcCost(projectedTotal, droughtLevel);
    let nudge = 'none';
    if (currentTier >= 3)           nudge = 'in_tier_3plus';
    else if (currentTier === 2)     nudge = 'in_tier_2';
    else if (galsTilNextTier < 500) nudge = 'approaching';

    // Compare with previous snapshot for tier-crossing and approaching-tier alerts
    let tierCrossedToday     = false;
    let approachingTierAlert = false;
    const prevKey = `waterwise:${userId}:${consumptionDate}`;
    const prevRaw = await redis.get(prevKey);
    if (prevRaw) {
      const prev = JSON.parse(prevRaw);
      if (prev.currentTier != null && currentTier > prev.currentTier) tierCrossedToday = true;
      if (prev.galsTilNextTier != null && prev.galsTilNextTier >= 500 && galsTilNextTier < 500) approachingTierAlert = true;
    }

    // 7-day rolling average of waterConsumptionToday from per-user daily keys
    const historyVals = [];
    for (let i = 1; i <= 7; i++) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const hKey  = `waterwise:${userId}:${d.toISOString().slice(0, 10)}`;
      const hRaw  = await redis.get(hKey);
      if (hRaw) {
        const entry = JSON.parse(hRaw);
        if (entry.waterConsumptionToday > 0) historyVals.push(entry.waterConsumptionToday);
      }
    }
    const sevenDayAvg = historyVals.length > 0
      ? historyVals.reduce((a, b) => a + b, 0) / historyVals.length
      : dailyAverage;

    const spikeAlert = waterConsumptionToday != null &&
      waterConsumptionToday > 200 &&
      sevenDayAvg > 0 &&
      waterConsumptionToday > sevenDayAvg * 2;
    const spikeMultiplier = spikeAlert
      ? (waterConsumptionToday / sevenDayAvg).toFixed(1)
      : null;

    const payload = {
      lcdRead:            raw.lcdRead ?? null,
      soFarThisCycle,
      dailyAverage,
      waterBudgetStatus:  raw.waterBudgetStatus ?? null,
      billingRead:        raw.billingRead ?? null,
      irrigationGallons:  raw.irrigationGallons,
      scrapedAt:          now.toISOString(),
      billingCycleDay, daysInMonth, daysRemaining, projectedTotal, droughtLevel,
      currentTier, galsTilNextTier, daysUntilTierCross, projectedTier,
      costSoFar, projectedCost, costByTier, nudge,
      hasIrrigation:        raw.irrigationGallons > 0,
      tierCrossedToday,
      approachingTierAlert,
      waterConsumptionToday,
      sevenDayAvg:          Math.round(sevenDayAvg * 10) / 10,
      spikeAlert,
      spikeMultiplier,
      fixtures:             raw.fixtures,
      consumptionDate,
      ...snowFields,
    };

    // Save interval data (with date validation)
    let intervalsValid = false;
    if (capturedIntervals.data) {
      if (intervalDateMatches(capturedIntervals.data, consumptionDate)) {
        const intervalKey = `waterwise:${userId}:intervals:${consumptionDate}`;
        await redis.set(intervalKey, JSON.stringify(capturedIntervals.data), 'EX', 7776000);
        console.log(`  Intervals saved → ${intervalKey}`);
        intervalsValid = true;
      } else {
        const got = sampleIntervalDate(capturedIntervals.data)?.toISOString().slice(0, 10) ?? 'unknown';
        console.warn(`  WARNING: Interval data date mismatch — expected ${consumptionDate}, got ${got}. Skipping save.`);
      }
    }

    // Save latest + dated snapshot
    await Promise.all([
      redis.set(`waterwise:${userId}:latest`, JSON.stringify(payload)),
      redis.set(`waterwise:${userId}:${consumptionDate}`, JSON.stringify(payload), 'EX', 7776000),
    ]);

    // Run corrections
    if (intervalsValid) {
      const { code, lines } = await runCorrectionsForUser(consumptionDate, userId);
      const summary = lines.find(l => /Rule 6|correctedFixtures|complete/.test(l)) ?? '';
      if (code !== 0) console.error(`  Corrections failed (exit ${code}):`, lines.slice(-3).join(' | '));
      else console.log(`  Corrections OK — ${summary}`);
    }

    // Run agent classification in parallel (fire and forget — don't block scrape)
    if (intervalsValid && process.env.ANTHROPIC_API_KEY) {
      const agentClassify = spawn(
        process.execPath,
        [path.join(__dirname, 'agent-classify.js'), consumptionDate, '--userId', userId],
        { env: process.env, stdio: 'pipe' }
      );
      agentClassify.stdout.on('data', d => console.log('  [agent]', d.toString().trim()));
      agentClassify.stderr.on('data', d => console.log('  [agent err]', d.toString().trim()));
      console.log('  Agent classification started (background)');
    }

    // Send alerts to this user's email if thresholds are crossed
    if (payload.tierCrossedToday || payload.approachingTierAlert || payload.spikeAlert) {
      try {
        await sendAlertEmail({ ...payload, userId }, email, redis);
        console.log(`  Alert email sent to ${email}`);
      } catch (e) {
        console.log(`  Alert email failed for ${email}:`, e.message);
      }
    }

    console.log(`✓ Scraped ${userId} (${email}): ${soFarThisCycle}G so far this cycle`);
    return { success: true, soFarThisCycle };
  } catch (err) {
    console.error(`✗ Failed ${userId} (${email}): ${err.message}`);
    return { success: false, error: err.message };
  } finally {
    if (browser) await browser.close();
  }
}

async function scrapeAllUsers(redis, now, snowFields, consumptionDate) {
  let keys;
  try {
    keys = await redis.keys('waterwise:creds:*');
  } catch (err) {
    console.error('Multi-user: could not list creds keys:', err.message);
    return;
  }

  if (!keys.length) {
    console.log('Multi-user: no registered users found');
    return;
  }

  const { decrypt } = require('../lib/crypto');
  let succeeded = 0;
  const userResults = [];

  for (const key of keys) {
    const credsRaw = await redis.get(key);
    if (!credsRaw) continue;
    let creds;
    try { creds = JSON.parse(credsRaw); } catch { continue; }

    // Skip the owner placeholder (scraped separately above)
    if (!creds.userId || creds.userId === 'owner') continue;

    let password;
    try {
      password = decrypt(creds.encryptedPassword, creds.iv, creds.authTag);
    } catch (err) {
      console.error(`Multi-user: could not decrypt password for ${creds.userId}:`, err.message);
      const [lp, dom] = (creds.email ?? '').split('@');
      userResults.push({ userId: creds.userId, name: creds.name ?? null, email: lp ? `${lp[0]}***@${dom}` : creds.email, success: false, error: 'decrypt failed', soFarThisCycle: null, durationMs: 0 });
      continue;
    }

    const startMs = Date.now();
    const result  = await scrapeUser({
      email:    creds.email,
      password,
      userId:   creds.userId,
      redis,
      now,
      snowFields,
      consumptionDate,
    });
    const durationMs = Date.now() - startMs;

    const [lp, dom] = (creds.email ?? '').split('@');
    userResults.push({
      userId:         creds.userId,
      name:           creds.name ?? null,
      email:          lp ? `${lp[0]}***@${dom}` : creds.email,
      success:        result.success,
      error:          result.error ?? null,
      soFarThisCycle: result.soFarThisCycle ?? null,
      durationMs,
    });

    if (result.success) succeeded++;
  }

  console.log(`Multi-user scrape complete: ${succeeded}/${userResults.length} succeeded`);
  return userResults;
}

async function main() {
  const trigger = process.argv.includes('--manual')       ? 'manual'
                : process.argv.includes('--registration') ? 'registration'
                : (process.env.TRIGGER || 'scheduled');

  console.log('Scraper starting:', new Date().toISOString(), `(trigger: ${trigger})`);
  const hour = new Date().getUTCHours(); // Metron stores local MT as UTC
  if (hour >= 6 && hour <= 22) {
    console.log('WARNING: Manual scrape during active hours — interval data may contain partial readings');
  }
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

    // Intercept all XHR/fetch responses from waterscope.us so we can capture
    // interval data the dashboard loads automatically, and log all API URLs.
    const capturedIntervals = { data: null, url: null };
    page.on('response', async (response) => {
      const url = response.url();
      if (!url.includes('waterscope.us')) return;
      const type = response.request().resourceType();
      if (type !== 'xhr' && type !== 'fetch') return;
      console.log('XHR:', response.status(), url);
      // Capture any response that looks like interval/consumption data
      if (/Consumption|Interval|History|Usage/i.test(url)) {
        try {
          const text = await response.text();
          const json = JSON.parse(text);
          if (Array.isArray(json) && json.length > 0) {
            capturedIntervals.data = json;
            capturedIntervals.url  = url;
            console.log('Captured interval array from:', url, '— length:', json.length);
          } else if (json && typeof json === 'object') {
            capturedIntervals.data = json;
            capturedIntervals.url  = url;
            console.log('Captured interval object from:', url);
          }
        } catch (_) { /* not JSON */ }
      }
    });

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
    // consumptionDate is always YESTERDAY in MT time — WaterScope data is 1 day behind.
    // MDT = UTC-6, MST = UTC-7. Using -6 year-round is safe: worst case off by 1hr in winter.
    const now          = new Date();
    const MT_OFFSET    = -6; // MDT (UTC-6)
    const mtNow        = new Date(now.getTime() + MT_OFFSET * 3600000);
    const mtYesterday  = new Date(mtNow.getTime() - 24 * 3600000);
    const consumptionDate = mtYesterday.toISOString().slice(0, 10);
    // Patch fixtures date now that consumptionDate is defined
    raw.fixtures.date = consumptionDate;
    const billingCycleDay = mtNow.getUTCDate();
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

      // Save interval data captured passively from the dashboard's own AJAX calls
      try {
        if (capturedIntervals.data) {
          if (intervalDateMatches(capturedIntervals.data, consumptionDate)) {
            const intervalKey = `waterwise:intervals:${consumptionDate}`;
            await redis.set(intervalKey, JSON.stringify(capturedIntervals.data), 'EX', 7776000);
            console.log('Interval data saved to', intervalKey, '(from:', capturedIntervals.url + ')');
            const sample = Array.isArray(capturedIntervals.data)
              ? capturedIntervals.data.slice(0, 2)
              : capturedIntervals.data;
            console.log('Interval sample:', JSON.stringify(sample));
          } else {
            const got = sampleIntervalDate(capturedIntervals.data)?.toISOString().slice(0, 10) ?? 'unknown';
            console.warn(`WARNING: Interval data date mismatch — expected ${consumptionDate}, got ${got}. Skipping save.`);
          }
        } else {
          console.warn('No interval data was captured from dashboard XHR calls');
          // Log what we DID see for diagnosis
          console.log('Dashboard URL at scrape time:', page.url());
        }
      } catch (e) {
        console.error('Interval save failed (non-fatal):', e.message);
      }

      const dateKey = `waterwise:${consumptionDate}`;
      console.log('Saving to Redis...');
      await Promise.all([
        redis.set('waterwise:latest', JSON.stringify(payload)),
        redis.set(dateKey, JSON.stringify(payload), 'EX', 7776000),
      ]);

      console.log('SUCCESS: Redis updated', dateKey, 'soFarThisCycle:', payload.soFarThisCycle);
      console.log(JSON.stringify(payload, null, 2));

      const { sendAlerts } = require('./email-alert.js');
      try {
        await sendAlerts(payload, redis);
      } catch (e) {
        console.error('Alert email failed:', e.message);
      }

      // ── Multi-user scrape ──────────────────────────────────────────────────
      const scrapeUsersStart = Date.now();
      const userResults      = await scrapeAllUsers(redis, now, snowFields, consumptionDate);
      const totalDurationMs  = Date.now() - scrapeUsersStart;

      // ── Scrape health record (daily key) ──────────────────────────────────
      try {
        const healthKey = 'waterwise:scrape-health:' + new Date().toISOString().slice(0, 10);
        await redis.set(healthKey, JSON.stringify({
          ranAt:        new Date().toISOString(),
          ownerSuccess: true,
          users:        userResults,
          totalDurationMs,
        }), 'EX', 7776000);
        console.log('Scrape health record saved →', healthKey);
      } catch (e) {
        console.error('Health record save failed (non-fatal):', e.message);
      }

      // ── Scrape log (rolling list, last 30 runs) ────────────────────────────
      try {
        const succeeded = userResults.filter(u => u.success).length;
        await redis.lpush('waterwise:scrape-log', JSON.stringify({
          ranAt:          new Date().toISOString(),
          trigger,
          users:          userResults,
          totalUsers:     userResults.length,
          succeeded,
          failed:         userResults.length - succeeded,
          totalDurationMs,
        }));
        await redis.ltrim('waterwise:scrape-log', 0, 29);
        console.log('Scrape log updated (last 30 runs kept)');
      } catch (e) {
        console.error('Scrape log write failed (non-fatal):', e.message);
      }

      // ── Failure alerts ─────────────────────────────────────────────────────
      const failures = userResults.filter(u => !u.success);
      if (failures.length && process.env.RESEND_API_KEY && process.env.REPORT_EMAIL) {
        const { Resend } = require('resend');
        const resend = new Resend(process.env.RESEND_API_KEY);
        for (const f of failures) {
          try {
            await resend.emails.send({
              from:    'onboarding@resend.dev',
              to:      process.env.REPORT_EMAIL,
              subject: `⚠️ WaterWise scrape failed for ${f.name ?? f.userId}`,
              text:    `userId: ${f.userId}\nemail: ${f.email}\nerror: ${f.error ?? 'unknown'}\ntime: ${new Date().toISOString()}`,
            });
            console.log(`Failure alert sent for ${f.userId}`);
          } catch (e) {
            console.error('Failure alert send error:', e.message);
          }
        }
      }

      await redis.quit();
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
