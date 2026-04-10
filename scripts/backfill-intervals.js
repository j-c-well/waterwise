'use strict';

// Load .env.local without requiring the dotenv package
const fs   = require('fs');
const path = require('path');

const envPath = path.join(__dirname, '..', '.env.local');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const { chromium } = require('playwright');
const { spawn }    = require('child_process');
const Redis        = require('ioredis');

// ── config ────────────────────────────────────────────────────────────────────

const DATES = [
  '2026-04-02',
  '2026-04-03',
  '2026-04-04',
  '2026-04-05',
  '2026-04-06',
  '2026-04-07',
  '2026-04-08',
];

const ACCOUNT_ID = 1735;
const METER_ID   = 3208158;

// ── helpers ───────────────────────────────────────────────────────────────────

// "YYYY-MM-DD" → "M/D/YYYY HH:MM:SS AM/PM" (no leading zeros on M and D)
function toMetronDate(iso, timeStr = '12:00:00 AM') {
  const [year, month, day] = iso.split('-');
  return `${parseInt(month)}/${parseInt(day)}/${year} ${timeStr}`;
}

// Run corrections.js for a specific date, resolve with { code, lines }
function runCorrections(date) {
  return new Promise((resolve) => {
    const child = spawn(
      process.execPath,
      [path.join(__dirname, 'corrections.js'), date],
      { env: process.env, stdio: 'pipe' }
    );
    const lines = [];
    child.stdout.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.stderr.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.on('close', code => resolve({ code, lines }));
  });
}

// ── main ──────────────────────────────────────────────────────────────────────

async function main() {
  const { REDIS_URL, WATERSCOPE_EMAIL, WATERSCOPE_PASSWORD } = process.env;
  if (!REDIS_URL)           throw new Error('REDIS_URL not set');
  if (!WATERSCOPE_EMAIL)    throw new Error('WATERSCOPE_EMAIL not set');
  if (!WATERSCOPE_PASSWORD) throw new Error('WATERSCOPE_PASSWORD not set');

  const redis = new Redis(REDIS_URL);

  // Check which dates are already populated
  const checks = await Promise.all(
    DATES.map(async date => ({
      date,
      exists: !!(await redis.get(`waterwise:intervals:${date}`)),
    }))
  );
  const todo = checks.filter(c => !c.exists).map(c => c.date);
  const skip = checks.filter(c =>  c.exists).map(c => c.date);

  if (skip.length) console.log('Already have data for:', skip.join(', '), '— skipping');
  if (!todo.length) {
    console.log('All dates already backfilled.');
    await redis.quit();
    return;
  }
  console.log(`Backfilling ${todo.length} date(s): ${todo.join(', ')}`);

  let browser;
  try {
    browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();

    // ── login (same flow as scrape.js) ────────────────────────────────────────
    console.log('Logging in to WaterScope...');
    await page.goto('https://waterscope.us/Home/Main', { waitUntil: 'domcontentloaded', timeout: 60000 });
    await page.fill('#txtSearchUserName', WATERSCOPE_EMAIL);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#searchUserName'),
    ]);
    await page.waitForSelector('#password', { timeout: 60000 });
    await page.fill('#password', WATERSCOPE_PASSWORD);
    await Promise.all([
      page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 60000 }),
      page.click('#next'),
    ]);
    await page.waitForSelector('#meterpanetopheaderbar', { timeout: 60000 });
    await page.waitForTimeout(2000); // let session cookies settle

    const origin   = new URL(page.url()).origin;
    const ENDPOINT = `${origin}/Consumer/Consumption/ConsumptionHistoryDataClaculation`;
    console.log('Authenticated. Endpoint:', ENDPOINT);

    // ── fetch each missing date ───────────────────────────────────────────────
    for (const date of todo) {
      console.log(`\n── ${date} ──`);

      // Build request args in Node (toMetronDate can't be called inside evaluate sandbox)
      const evalArgs = {
        url:  ENDPOINT,
        body: {
          startLogDate: toMetronDate(date, '12:00:00 AM'),
          endLogDate:   toMetronDate(date, '11:59:59 PM'),
          AccountId:    ACCOUNT_ID,
          MeterId:      METER_ID,
        },
      };

      let result;
      try {
        result = await page.evaluate(
          async ({ url, body }) => {
            const res  = await fetch(url, {
              method:      'POST',
              headers:     { 'Content-Type': 'application/json' },
              credentials: 'include',
              body:        JSON.stringify(body),
            });
            const text = await res.text();
            return { status: res.status, text };
          },
          evalArgs
        );
      } catch (err) {
        console.error(`  fetch threw: ${err.message}`);
        continue;
      }

      console.log(`  HTTP ${result.status}, body ${result.text.length} bytes`);

      if (result.status !== 200) {
        console.warn(`  non-200 — skipping`);
        continue;
      }

      // Detect HTML (login redirect or error page)
      if (result.text.trimStart().startsWith('<')) {
        console.warn(`  received HTML — session may have expired or endpoint URL is wrong`);
        console.warn(`  preview: ${result.text.slice(0, 120).replace(/\s+/g, ' ')}`);
        continue;
      }

      let parsed;
      try {
        parsed = JSON.parse(result.text);
      } catch {
        console.warn(`  non-JSON response: ${result.text.slice(0, 200)}`);
        continue;
      }

      // Accept array or object with a nested array
      const rowArray = Array.isArray(parsed)        ? parsed
                     : Array.isArray(parsed?.data)  ? parsed.data
                     : Array.isArray(parsed?.Data)  ? parsed.Data
                     : null;

      if (!rowArray || rowArray.length === 0) {
        console.warn(`  empty/no rows — response: ${JSON.stringify(parsed).slice(0, 200)}`);
        continue;
      }

      // Save raw intervals
      const intervalKey = `waterwise:intervals:${date}`;
      await redis.set(intervalKey, JSON.stringify(parsed), 'EX', 7776000);
      console.log(`  saved ${rowArray.length} rows → ${intervalKey}`);

      // Run corrections
      const { code, lines } = await runCorrections(date);
      if (code !== 0) {
        console.error(`  corrections.js exited ${code}:`);
        lines.forEach(l => console.error('   ', l));
      } else {
        const ruleLines = lines.filter(l => /Rule [1-5]:|correctedFixtures|complete/.test(l));
        ruleLines.forEach(l => console.log('  ', l));
        console.log(`  ✓ Backfilled ${date}: ${rowArray.length} intervals`);
      }

      // Brief pause between requests
      await page.waitForTimeout(1000);
    }

  } finally {
    if (browser) await browser.close();
    await redis.quit();
  }

  console.log('\nBackfill complete.');
}

main().catch(err => {
  console.error('BACKFILL FAILED:', err.message);
  process.exit(1);
});
