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
const { decrypt }  = require('../lib/crypto');

// ── config ────────────────────────────────────────────────────────────────────

// Last 7 days (excluding today — Metron data lags ~1 day)
function lastNDates(n = 7) {
  const dates = [];
  for (let i = 1; i <= n; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    dates.push(d.toISOString().slice(0, 10));
  }
  return dates.reverse(); // oldest first
}

const ACCOUNT_ID = 1735;
const METER_ID   = 3208158;

// ── helpers ───────────────────────────────────────────────────────────────────

// "YYYY-MM-DD" → "M/D/YYYY HH:MM:SS AM/PM" (no leading zeros on M and D)
function toMetronDate(iso, timeStr = '12:00:00 AM') {
  const [year, month, day] = iso.split('-');
  return `${parseInt(month)}/${parseInt(day)}/${year} ${timeStr}`;
}

// Run corrections.js for a specific date (and optional userId), resolve with { code, lines }
function runCorrections(date, userId) {
  const args = [path.join(__dirname, 'corrections.js'), date];
  if (userId) args.push('--userId', userId);
  return new Promise((resolve) => {
    const child = spawn(process.execPath, args, { env: process.env, stdio: 'pipe' });
    const lines = [];
    child.stdout.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.stderr.on('data', d => lines.push(...d.toString().split('\n').filter(Boolean)));
    child.on('close', code => resolve({ code, lines }));
  });
}

// ── main ──────────────────────────────────────────────────────────────────────

async function main() {
  const { REDIS_URL } = process.env;
  if (!REDIS_URL) throw new Error('REDIS_URL not set');

  // Parse --userId flag
  const args   = process.argv.slice(2);
  const uidIdx = args.indexOf('--userId');
  const userId = uidIdx !== -1 ? args[uidIdx + 1] : null;

  const redis = new Redis(REDIS_URL);

  let email, password;

  if (userId) {
    // Load and decrypt credentials for registered user
    console.log(`Loading credentials for userId: ${userId}`);
    const credsRaw = await redis.get(`waterwise:creds:${userId}`);
    if (!credsRaw) throw new Error(`No credentials found for userId ${userId}`);
    const creds = JSON.parse(credsRaw);
    email    = creds.email;
    password = decrypt(creds.encryptedPassword, creds.iv, creds.authTag);
    console.log(`Loaded credentials for ${email}`);
  } else {
    email    = process.env.WATERSCOPE_EMAIL;
    password = process.env.WATERSCOPE_PASSWORD;
    if (!email)    throw new Error('WATERSCOPE_EMAIL not set');
    if (!password) throw new Error('WATERSCOPE_PASSWORD not set');
  }

  const ns    = userId ? `waterwise:${userId}` : 'waterwise';
  const DATES = lastNDates(7);
  console.log(`Backfill window: ${DATES[0]} → ${DATES[DATES.length - 1]}`);

  // Check which dates are already populated
  const checks = await Promise.all(
    DATES.map(async date => ({
      date,
      exists: !!(await redis.get(`${ns}:intervals:${date}`)),
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

    // Intercept the real ConsumptionHistoryDataClaculation request that the
    // dashboard fires on load — capture its headers + body so we can reuse
    // the exact format (content-type, CSRF token, field names) for manual calls.
    const capturedReq = { headers: null, body: null, contentType: null };
    page.on('request', request => {
      if (!request.url().includes('ConsumptionHistoryDataClaculation')) return;
      if (request.method() !== 'POST') return;
      capturedReq.headers     = request.headers();
      capturedReq.body        = request.postData() ?? '';
      capturedReq.contentType = (request.headers()['content-type'] ?? '').split(';')[0].trim();
      console.log('Intercepted dashboard request — content-type:', capturedReq.contentType);
      console.log('Request body sample:', capturedReq.body.slice(0, 200));
    });

    // ── login (same flow as scrape.js) ────────────────────────────────────────
    console.log('Logging in to WaterScope...');
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
    await page.waitForTimeout(3000); // let XHR fire and be intercepted

    const origin   = new URL(page.url()).origin;
    const ENDPOINT = `${origin}/Consumer/Consumption/ConsumptionHistoryDataClaculation`;
    console.log('Authenticated. Endpoint:', ENDPOINT);

    if (!capturedReq.body) {
      console.warn('Dashboard XHR did not fire during login wait — will try JSON POST anyway');
    }

    // ── fetch each missing date ───────────────────────────────────────────────
    for (const date of todo) {
      console.log(`\n── ${date} ──`);

      // Build the request body, matching the format the dashboard actually used.
      // If we captured a form-encoded body, patch its date fields; otherwise fall
      // back to form-encoded without CSRF (may work if endpoint allows it).
      const startDate = toMetronDate(date, '12:00:00 AM');
      const endDate   = toMetronDate(date, '11:59:59 PM');

      let requestBody;
      let contentType;
      if (capturedReq.body && capturedReq.contentType === 'application/x-www-form-urlencoded') {
        // Patch the captured form body with the new dates, keep all other fields intact
        const params = new URLSearchParams(capturedReq.body);
        params.set('startLogDate', startDate);
        params.set('endLogDate',   endDate);
        requestBody = params.toString();
        contentType = 'application/x-www-form-urlencoded';
        console.log(`  using form-encoded (patched from dashboard request)`);
      } else if (capturedReq.body && capturedReq.contentType === 'application/json') {
        const orig = JSON.parse(capturedReq.body);
        orig.startLogDate = startDate;
        orig.endLogDate   = endDate;
        requestBody = JSON.stringify(orig);
        contentType = 'application/json';
        console.log(`  using JSON (patched from dashboard request)`);
      } else {
        // Fallback: try form-encoded without CSRF token
        const params = new URLSearchParams({
          startLogDate: startDate,
          endLogDate:   endDate,
          AccountId:    String(ACCOUNT_ID),
          MeterId:      String(METER_ID),
        });
        requestBody = params.toString();
        contentType = 'application/x-www-form-urlencoded';
        console.log(`  using form-encoded fallback (no captured request)`);
      }

      const evalArgs = { url: ENDPOINT, body: requestBody, contentType };

      let result;
      try {
        result = await page.evaluate(
          async ({ url, body, contentType }) => {
            const res = await fetch(url, {
              method:      'POST',
              headers:     { 'Content-Type': contentType },
              credentials: 'include',
              body,
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
      const intervalKey = `${ns}:intervals:${date}`;
      await redis.set(intervalKey, JSON.stringify(parsed), 'EX', 7776000);
      console.log(`  saved ${rowArray.length} rows → ${intervalKey}`);

      // Run corrections
      const { code, lines } = await runCorrections(date, userId);
      if (code !== 0) {
        console.error(`  corrections.js exited ${code}:`);
        lines.forEach(l => console.error('   ', l));
      } else {
        const ruleLines = lines.filter(l => /Rule [1-7]:|correctedFixtures|complete/.test(l));
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
