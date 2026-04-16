'use strict';

// Load .env.local without requiring dotenv
const fs   = require('fs');
const path = require('path');
const envPath = path.join(__dirname, '..', '.env.local');
if (fs.existsSync(envPath)) {
  for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const Redis   = require('ioredis');
const { Resend } = require('resend');

// ── helpers ───────────────────────────────────────────────────────────────────

function yesterday() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

function formatDate(iso) {
  const d = new Date(iso + 'T12:00:00Z');
  return d.toLocaleString('en-US', { month: 'short', day: 'numeric', timeZone: 'UTC' });
}

function ago(isoStr) {
  if (!isoStr) return 'never';
  const h = Math.round((Date.now() - new Date(isoStr)) / 3600000);
  if (h < 1)  return '<1h ago';
  if (h < 24) return `${h}h ago`;
  return `${Math.round(h / 24)}d ago`;
}

// ── main ──────────────────────────────────────────────────────────────────────

async function main() {
  const { REDIS_URL, RESEND_API_KEY, REPORT_EMAIL } = process.env;
  if (!REDIS_URL)       throw new Error('Missing REDIS_URL');
  if (!RESEND_API_KEY)  throw new Error('Missing RESEND_API_KEY');
  if (!REPORT_EMAIL)    throw new Error('Missing REPORT_EMAIL');

  const redis  = new Redis(REDIS_URL);
  const resend = new Resend(RESEND_API_KEY);
  const date   = yesterday();

  try {
    // ── 1. Scrape health ─────────────────────────────────────────────────────
    const healthRaw = await redis.get(`waterwise:scrape-health:${date}`);
    const health    = healthRaw ? JSON.parse(healthRaw) : null;

    // ── 2. Analytics ─────────────────────────────────────────────────────────
    const analyticsItems = await redis.lrange(`waterwise:analytics:${date}`, 0, -1);
    const byUser = {};
    for (const item of analyticsItems) {
      try {
        const e = JSON.parse(item);
        if (!byUser[e.userId]) byUser[e.userId] = { dashboardLoads: 0, timelineViews: 0, showerAssignments: 0 };
        if (e.event === 'dashboard_load')   byUser[e.userId].dashboardLoads++;
        if (e.event === 'timeline_view')    byUser[e.userId].timelineViews++;
        if (e.event === 'shower_assigned')  byUser[e.userId].showerAssignments++;
      } catch (_) {}
    }

    // ── 3. Email log ─────────────────────────────────────────────────────────
    const emailItems = await redis.lrange(`waterwise:email-log:${date}`, 0, -1);
    const emailsByType = {};
    for (const item of emailItems) {
      try {
        const e = JSON.parse(item);
        emailsByType[e.type] = (emailsByType[e.type] ?? 0) + 1;
      } catch (_) {}
    }

    // ── 4. Registered users + stale check ────────────────────────────────────
    const credKeys = await redis.keys('waterwise:creds:*');
    const users    = [];
    const stale    = [];
    const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 3600 * 1000).toISOString();
    let newSignups = 0;

    for (const k of credKeys) {
      const raw = await redis.get(k);
      if (!raw) continue;
      let creds;
      try { creds = JSON.parse(raw); } catch { continue; }

      const latestRaw    = await redis.get(`waterwise:${creds.userId}:latest`);
      const latest       = latestRaw ? JSON.parse(latestRaw) : null;
      const lastScraped  = latest?.scrapedAt ?? null;
      const staleHours   = lastScraped ? Math.round((Date.now() - new Date(lastScraped)) / 3600000) : null;
      const isStale      = staleHours === null || staleHours > 48;

      const [lp, dom] = (creds.email ?? '').split('@');
      const maskedEmail = lp ? `${lp[0]}***@${dom}` : creds.email;

      users.push({ userId: creds.userId, name: creds.name, email: maskedEmail, lastScraped, staleHours, isStale, createdAt: creds.createdAt ?? null });
      if (isStale) stale.push({ userId: creds.userId, name: creds.name, lastScraped });
      if (creds.createdAt && creds.createdAt >= sevenDaysAgo) newSignups++;
    }

    users.sort((a, b) => (a.createdAt ?? '').localeCompare(b.createdAt ?? ''));

    // ── 5. Redis key count ────────────────────────────────────────────────────
    const redisKeyCount = await redis.dbsize();

    // ── Build email ───────────────────────────────────────────────────────────
    const today = new Date();
    const dateLabel = today.toLocaleString('en-US', { month: 'short', day: 'numeric' });
    const subject   = `WaterWise Daily · ${dateLabel} · ${users.length} user${users.length !== 1 ? 's' : ''}`;

    // Scrape results section
    let scrapeSection = '';
    if (health) {
      const ownerLine = `${health.ownerSuccess ? '✅' : '❌'} Owner  (${Math.round(health.totalDurationMs / 1000)}s total)`;
      const userLines = (health.users ?? []).map(u =>
        `${u.success ? '✅' : '❌'} ${u.name ?? u.userId}  ${u.success ? `${u.soFarThisCycle ?? '?'}G` : `ERROR: ${u.error ?? '?'}`}  (${Math.round((u.durationMs ?? 0) / 1000)}s)`
      );
      scrapeSection = [ownerLine, ...userLines].join('\n');
    } else {
      scrapeSection = '⚠️  No scrape health record found for ' + date;
    }

    // Activity section
    const activityLines = Object.entries(byUser).map(([uid, ev]) =>
      `  ${uid}: ${ev.dashboardLoads} loads · ${ev.timelineViews} timeline · ${ev.showerAssignments} showers`
    );
    const activitySection = activityLines.length ? activityLines.join('\n') : '  (no activity)';

    // Emails section
    const emailSection = Object.entries(emailsByType).map(([t, n]) => `  ${t}: ${n}`).join('\n') || '  (none sent)';

    // System health section
    const userHealthLines = users.map(u =>
      `  ${u.isStale ? '⚠️ ' : '   '}${u.name ?? u.userId} (${u.email})  last scraped: ${u.lastScraped ? ago(u.lastScraped) : 'never'}`
    );
    const systemSection = [
      ...userHealthLines,
      `  Redis keys: ${redisKeyCount.toLocaleString()}`,
    ].join('\n');

    // Signups section
    const signupSection = `  New in last 7 days: ${newSignups}\n  Total registered: ${users.length}`;

    const body = [
      `SCRAPE RESULTS (${formatDate(date)})`,
      scrapeSection,
      '',
      'USER ACTIVITY',
      activitySection,
      '',
      'EMAILS SENT YESTERDAY',
      emailSection,
      '',
      'SYSTEM HEALTH',
      systemSection,
      '',
      'REGISTERED USERS',
      signupSection,
    ].join('\n');

    const html = `<pre style="font-family:monospace;font-size:13px;line-height:1.6">${body.replace(/</g, '&lt;')}</pre>`;

    const result = await resend.emails.send({
      from:    'onboarding@resend.dev',
      to:      REPORT_EMAIL,
      subject,
      html,
      text:    body,
    });

    console.log('Digest sent:', result.data?.id ?? result);
    console.log(body);
  } finally {
    await redis.quit();
  }
}

main().catch(err => {
  console.error('DIGEST FAILED:', err.message);
  process.exit(1);
});
