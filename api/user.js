'use strict';

// Single serverless function routing /api/user/* to avoid Vercel Hobby
// plan's 12-function limit. Route is determined by the path suffix:
//   POST /api/user/register
//   GET  /api/user/data?userId=
//   GET  /api/user/status?userId=

const crypto  = require('crypto');
const Redis   = require('ioredis');
const { encrypt }   = require('../lib/crypto');
const { logEvent }  = require('../lib/analytics');

const redis = new Redis(process.env.REDIS_URL);

function cors(res, methods = 'GET, OPTIONS') {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', methods);
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
}

// ── helpers ───────────────────────────────────────────────────────────────────

async function triggerScrape() {
  // Delay 30s to ensure the Redis creds write has fully committed before
  // the scrape workflow reads waterwise:creds:* keys.
  setTimeout(async () => {
    try {
      const response = await fetch(
        'https://api.github.com/repos/j-c-well/waterwise/actions/workflows/scrape.yml/dispatches',
        {
          method: 'POST',
          headers: {
            'Authorization': 'token ' + process.env.GITHUB_PAT,
            'Accept': 'application/vnd.github.v3+json',
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ ref: 'main', inputs: { trigger: 'registration' } }),
        }
      );
      if (response.ok) console.log('Scrape triggered for new user');
      else console.log('Scrape trigger failed:', response.status);
    } catch (e) {
      console.log('Scrape trigger error:', e.message);
    }
  }, 30000);
}

// ── POST /api/user/register ───────────────────────────────────────────────────
async function handleRegister(req, res) {
  cors(res, 'POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const { email, password, name } = req.body ?? {};

  if (!email || typeof email !== 'string' || !email.includes('@')) {
    return res.status(400).json({ error: 'Valid email required' });
  }
  if (!password || typeof password !== 'string' || password.length < 6) {
    return res.status(400).json({ error: 'Password must be at least 6 characters' });
  }

  const existingId = await redis.get(`waterwise:email:${email.toLowerCase()}`);
  if (existingId) {
    return res.status(409).json({ error: 'An account with that email already exists' });
  }

  const userId      = crypto.randomBytes(4).toString('hex');
  const displayName = (name && name.trim()) || email.split('@')[0];
  const { encrypted: encryptedPassword, iv, authTag } = encrypt(password);

  const creds = {
    userId,
    email:             email.toLowerCase(),
    name:              displayName,
    encryptedPassword,
    iv,
    authTag,
    createdAt:         new Date().toISOString(),
    status:            'active',
  };

  await Promise.all([
    redis.set(`waterwise:creds:${userId}`, JSON.stringify(creds)),
    redis.set(`waterwise:email:${email.toLowerCase()}`, userId),
  ]);

  console.log(`Registered user ${userId} (${email.toLowerCase()})`);

  // Fire-and-forget — don't block the registration response
  triggerScrape();

  return res.status(201).json({
    userId,
    email:        creds.email,
    name:         creds.name,
    dashboardUrl: `https://water-wise-gauge.lovable.app?user=${userId}`,
  });
}

// ── GET /api/user/data?userId= ────────────────────────────────────────────────
async function handleData(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.query ?? {};
  const latestKey  = userId ? `waterwise:${userId}:latest` : 'waterwise:latest';
  const raw = await redis.get(latestKey);

  if (!raw) {
    if (userId) {
      return res.status(202).json({
        status:  'pending',
        message: 'Data not yet scraped. Check back after 3am MT.',
      });
    }
    return res.status(404).json({ error: 'No data yet — scrape has not run' });
  }

  const data       = JSON.parse(raw);
  const staleHours = Math.round((Date.now() - new Date(data.scrapedAt)) / 3600000);
  const dataStale  = staleHours > 25;

  const correctedRaw = data.consumptionDate
    ? await redis.get(
        userId
          ? `waterwise:${userId}:corrected:${data.consumptionDate}`
          : `waterwise:corrected:${data.consumptionDate}`
      )
    : null;
  const corrected = correctedRaw ? JSON.parse(correctedRaw) : null;

  const profileRaw = await redis.get(
    userId ? `waterwise:household:${userId}` : 'waterwise:household:owner'
  );
  const householdProfile = profileRaw ? JSON.parse(profileRaw) : null;

  let fixtures;
  let fixturesSource;
  let leakAlert = null;

  if (corrected?.correctedFixtures) {
    const cf = corrected.correctedFixtures;
    fixtures = {
      toilet:         cf.toilet,
      sink:           cf.sink,
      shower:         cf.shower,
      bath:           cf.bath,
      dishwasher:     cf.dishwasher,
      washingMachine: cf.washingMachine,
      bidet:          cf.bidet,
      other:          cf.other,
      date:           data.fixtures?.date ?? data.consumptionDate,
    };
    fixturesSource = 'corrected';
    const residualLeak = cf._rawSums?.LEAK ?? 0;
    leakAlert = residualLeak > 5
      ? { gallons: Math.round(residualLeak * 10) / 10, detected: true }
      : null;
  } else {
    const mf = data.fixtures ?? {};
    fixtures = {
      toilet:         mf.toilet,
      sink:           mf.sink,
      shower:         mf.shower,
      dishwasher:     mf.kitchen ?? mf.dishwasher ?? 0,
      bathtub:        mf.bathtub,
      washingMachine: mf.washingMachine,
      date:           mf.date,
    };
    fixturesSource = 'metron';
  }

  const signaturesRaw = await redis.lrange(
    userId ? `waterwise:${userId}:signatures` : 'waterwise:signatures',
    0, 49
  );
  const signatures = signaturesRaw
    .map(s => { try { return JSON.parse(s); } catch { return null; } })
    .filter(Boolean)
    .map(({ category, timeOfDay, totalGallons, durationMin, confirmedAt, confirmedBy }) =>
      ({ category, timeOfDay, totalGallons, durationMin, confirmedAt, confirmedBy }));

  logEvent(redis, { event: 'dashboard_load', userId: userId || 'owner' }, req);
  res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
  return res.status(200).json({
    ...data,
    fixtures,
    fixturesSource,
    dataStale,
    staleHours,
    householdProfile,
    leakAlert,
    signatureCount: signatures.length,
    signatures,
  });
}

// ── GET /api/user/status?userId= ──────────────────────────────────────────────
async function handleStatus(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.query ?? {};
  if (!userId) return res.status(400).json({ error: 'userId query param required' });

  const credsRaw = await redis.get(`waterwise:creds:${userId}`);
  if (!credsRaw) return res.status(404).json({ error: 'User not found' });

  const creds     = JSON.parse(credsRaw);
  const latestRaw = await redis.get(`waterwise:${userId}:latest`);
  const latest    = latestRaw ? JSON.parse(latestRaw) : null;

  const lastScraped = latest?.scrapedAt ?? null;
  const staleHours  = lastScraped
    ? Math.round((Date.now() - new Date(lastScraped)) / 3600000)
    : null;

  return res.status(200).json({
    userId:      creds.userId,
    email:       creds.email,
    name:        creds.name,
    hasData:     !!latest,
    lastScraped,
    dataStale:   staleHours !== null ? staleHours > 25 : null,
    lastError:   creds.lastError   ?? null,
    lastErrorAt: creds.lastErrorAt ?? null,
  });
}

// ── GET /api/user/admin?key= ──────────────────────────────────────────────────
async function handleAdmin(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { key } = req.query ?? {};
  const adminKey = (process.env.ADMIN_KEY || '').trim();
  if (!key || key !== adminKey) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const keys = await redis.keys('waterwise:creds:*');
  const users = [];

  for (const redisKey of keys) {
    const credsRaw = await redis.get(redisKey);
    if (!credsRaw) continue;
    let creds;
    try { creds = JSON.parse(credsRaw); } catch { continue; }

    const latestRaw = await redis.get(`waterwise:${creds.userId}:latest`);
    const latest    = latestRaw ? JSON.parse(latestRaw) : null;

    const lastScraped = latest?.scrapedAt ?? null;
    const staleHours  = lastScraped
      ? Math.round((Date.now() - new Date(lastScraped)) / 3600000)
      : null;

    // Mask email: first char + *** + @domain
    const [localPart, domain] = (creds.email ?? '').split('@');
    const maskedEmail = localPart ? `${localPart[0]}***@${domain}` : creds.email;

    users.push({
      userId:         creds.userId,
      name:           creds.name,
      email:          maskedEmail,
      createdAt:      creds.createdAt,
      lastScraped,
      soFarThisCycle: latest?.soFarThisCycle ?? null,
      dataStale:      staleHours !== null ? staleHours > 25 : null,
      dashboardUrl:   `https://waterwise-six.vercel.app?user=${creds.userId}`,
    });
  }

  users.sort((a, b) => (a.createdAt ?? '').localeCompare(b.createdAt ?? ''));

  return res.status(200).json({ users, totalUsers: users.length });
}

// ── GET /api/analytics?key= ───────────────────────────────────────────────────
async function handleAnalytics(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { key } = req.query ?? {};
  const adminKey = (process.env.ADMIN_KEY || '').trim();
  if (!key || key !== adminKey) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const today = new Date();
  const days = Array.from({ length: 14 }, (_, i) => {
    const d = new Date(today);
    d.setDate(d.getDate() - i);
    return d.toISOString().slice(0, 10);
  });

  const byDay       = {};
  const byUser      = {};
  const byUserAgent = { mobile: 0, desktop: 0, unknown: 0 };
  let totalEvents   = 0;
  const uniqueUsers = new Set();

  for (const day of days) {
    const raw = await redis.lrange(`waterwise:analytics:${day}`, 0, -1);
    if (!raw.length) continue;
    byDay[day] = {};
    for (const item of raw) {
      try {
        const e = JSON.parse(item);
        byDay[day][e.event]       = (byDay[day][e.event]  ?? 0) + 1;
        byUser[e.userId]          = byUser[e.userId] ?? {};
        byUser[e.userId][e.event] = (byUser[e.userId][e.event] ?? 0) + 1;
        uniqueUsers.add(e.userId);
        const uaBucket = e.ua ?? 'unknown';
        byUserAgent[uaBucket]     = (byUserAgent[uaBucket] ?? 0) + 1;
        totalEvents++;
      } catch (_) {}
    }
  }

  const sum = (eventName) =>
    Object.values(byDay).reduce((t, d) => t + (d[eventName] ?? 0), 0);

  return res.status(200).json({
    summary: {
      totalEvents,
      uniqueUsers:       [...uniqueUsers],
      dashboardLoads:    sum('dashboard_load'),
      historyViews:      sum('history_view'),
      timelineViews:     sum('timeline_view'),
      showerAssignments: sum('shower_assigned'),
      profileUpdates:    sum('profile_updated'),
      byUserAgent,
    },
    byDay,
    byUser,
  });
}

// ── GET /api/health?key= ──────────────────────────────────────────────────────
async function handleHealth(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { key } = req.query ?? {};
  const adminKey = (process.env.ADMIN_KEY || '').trim();
  if (!key || key !== adminKey) {
    return res.status(403).json({ error: 'Forbidden' });
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const ymd = yesterday.toISOString().slice(0, 10);

  // Scrape health
  const healthRaw = await redis.get(`waterwise:scrape-health:${ymd}`);
  const health    = healthRaw ? JSON.parse(healthRaw) : null;

  // Email count last 7 days
  let emailsSentLast7Days = 0;
  for (let i = 0; i < 7; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const items = await redis.llen(`waterwise:email-log:${d.toISOString().slice(0, 10)}`);
    emailsSentLast7Days += items;
  }

  // Registered users + stale check
  const credKeys = await redis.keys('waterwise:creds:*');
  const staleUsers = [];

  for (const k of credKeys) {
    const raw = await redis.get(k);
    if (!raw) continue;
    let creds;
    try { creds = JSON.parse(raw); } catch { continue; }
    const latestRaw   = await redis.get(`waterwise:${creds.userId}:latest`);
    const latest      = latestRaw ? JSON.parse(latestRaw) : null;
    const lastScraped = latest?.scrapedAt ?? null;
    const staleHours  = lastScraped ? Math.round((Date.now() - new Date(lastScraped)) / 3600000) : null;
    if (staleHours === null || staleHours > 48) {
      staleUsers.push({ userId: creds.userId, name: creds.name, lastScraped });
    }
  }

  const redisKeyCount = await redis.dbsize();

  // Rolling logs for admin dashboard
  const scrapeLogRaw = await redis.lrange('waterwise:scrape-log', 0, 29);
  const scrapeLog    = scrapeLogRaw.map(r => { try { return JSON.parse(r); } catch { return null; } }).filter(Boolean);

  const emailLogRaw  = await redis.lrange('waterwise:email-log', 0, 99);
  const emailLog     = emailLogRaw.map(r => { try { return JSON.parse(r); } catch { return null; } }).filter(Boolean);

  // Email click-through data last 14 days
  const emailClicks = {};
  for (let i = 0; i < 14; i++) {
    const d = new Date();
    d.setDate(d.getDate() - i);
    const dateStr = d.toISOString().slice(0, 10);
    const clicks  = await redis.hgetall(`waterwise:email-clicks:${dateStr}`);
    if (clicks && Object.keys(clicks).length > 0) {
      emailClicks[dateStr] = Object.keys(clicks);
    }
  }

  const emailEventsRaw = await redis.lrange('waterwise:email-events', 0, 19);
  const emailEvents    = emailEventsRaw.map(r => { try { return JSON.parse(r); } catch { return null; } }).filter(Boolean);
  const emailOpens     = await redis.hgetall('waterwise:email-opens') ?? {};
  const emailClicksResend = await redis.hgetall('waterwise:email-clicks-resend') ?? {};

  return res.status(200).json({
    scrapeHealth: health
      ? { lastRan: health.ranAt, allSucceeded: health.ownerSuccess && (health.users ?? []).every(u => u.success), users: health.users ?? [] }
      : null,
    registeredUsers:     credKeys.length,
    emailsSentLast7Days,
    staleUsers,
    redisKeyCount,
    scrapeLog,
    emailLog,
    emailClicks,
    emailEvents,
    emailOpens,
    emailClicksResend,
  });
}

// ── POST /api/analytics/email-click?userId=&week= ────────────────────────────
async function handleEmailClick(req, res) {
  cors(res, 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId, week } = req.method === 'POST'
    ? { ...(req.query ?? {}), ...(req.body ?? {}) }
    : (req.query ?? {});

  if (!userId) return res.status(400).json({ error: 'userId required' });

  const clickWeek = week ?? new Date().toISOString().slice(0, 10);
  const hashKey   = `waterwise:email-clicks:${clickWeek}`;
  const countKey  = `waterwise:email-click-count:${userId}`;

  await Promise.all([
    redis.hset(hashKey, userId, new Date().toISOString()),
    redis.incr(countKey),
  ]);

  return res.status(200).json({ ok: true, userId, week: clickWeek });
}

// ── POST /api/cron/scrape  (called by QStash) ─────────────────────────────────
async function handleCronScrape(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  const cronSecret = (process.env.CRON_SECRET || '').trim();
  if (!cronSecret || req.headers['authorization'] !== 'Bearer ' + cronSecret) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const response = await fetch(
      'https://api.github.com/repos/j-c-well/waterwise/actions/workflows/scrape.yml/dispatches',
      {
        method: 'POST',
        headers: {
          'Authorization': 'token ' + process.env.GITHUB_PAT,
          'Accept': 'application/vnd.github.v3+json',
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ ref: 'main', inputs: { trigger: 'scheduled' } }),
      }
    );
    if (!response.ok) {
      const text = await response.text();
      return res.status(502).json({ error: 'GitHub dispatch failed', status: response.status, body: text });
    }
    console.log('QStash → scrape dispatch triggered');
    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error('cron/scrape error:', err.message);
    return res.status(500).json({ error: err.message });
  }
}

// ── POST /api/cron/email  (called by QStash) ──────────────────────────────────
async function handleCronEmail(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });
  const cronSecret = (process.env.CRON_SECRET || '').trim();
  if (!cronSecret || req.headers['authorization'] !== 'Bearer ' + cronSecret) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  try {
    const { sendWeeklyEmails } = require('../scripts/email-weekly');
    await sendWeeklyEmails();
    console.log('QStash → weekly emails sent');
    return res.status(200).json({ ok: true });
  } catch (err) {
    console.error('cron/email error:', err.message);
    return res.status(500).json({ error: err.message });
  }
}

// ── GET /api/user/email-preview?key=&userId=&template=&variant= ───────────────
async function handleEmailPreview(req, res) {
  cors(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { key, userId, template = 'weekly', variant } = req.query ?? {};
  const adminKey = (process.env.ADMIN_KEY || '').trim();
  if (!key || key !== adminKey) return res.status(403).json({ error: 'Forbidden' });
  if (!userId) return res.status(400).json({ error: 'userId required' });

  const MT_OFFSET   = -6;
  const mtNow       = new Date(Date.now() + MT_OFFSET * 3600000);
  const mtYesterday = new Date(mtNow.getTime() - 24 * 3600000);
  const consumptionDate = mtYesterday.toISOString().slice(0, 10);

  const [latestRaw, correctedRaw, showerLogRaw, profileRaw, credsRaw] = await Promise.all([
    redis.get(`waterwise:${userId}:latest`),
    redis.get(`waterwise:${userId}:corrected:${consumptionDate}`),
    redis.get(`waterwise:shower-log:${userId}`),
    redis.get(`waterwise:household:${userId}`),
    redis.get(`waterwise:creds:${userId}`),
  ]);

  if (!latestRaw) return res.status(404).json({ error: 'No data for user' });

  const base     = JSON.parse(latestRaw);
  const fixtures = correctedRaw ? JSON.parse(correctedRaw) : null;
  const profile  = profileRaw ? JSON.parse(profileRaw) : null;
  const creds    = credsRaw ? JSON.parse(credsRaw) : null;
  const showerLog = showerLogRaw ? JSON.parse(showerLogRaw) : [];

  let data = { ...base, fixtures, householdProfile: profile };

  // Apply variant overrides
  if (variant === 'spike') {
    data = { ...data, spikeAlert: true, waterConsumptionToday: (data.dailyAverage || 0) * 2.5, spikeMultiplier: 2.5 };
  } else if (variant === 'approaching') {
    data = { ...data, nudge: 'approaching', galsTilNextTier: 380 };
  } else if (variant === 'tier2') {
    data = { ...data, nudge: 'in_tier_2', currentTier: 2, soFarThisCycle: 4200 };
  } else if (variant === 'tier3') {
    data = { ...data, nudge: 'in_tier_3plus', currentTier: 3, soFarThisCycle: 8000 };
  } else if (variant === 'lowsnow') {
    data = { ...data, snowpackSWEPct: 35, precipPct: 40 };
  } else if (variant === 'noleaderboard') {
    data = { ...data, showerLog: [] };
  } else if (variant === 'leaderboard') {
    data = { ...data, showerLog };
  }

  const { weeklySnapshot, tierAlert, spikeAlert: spikeAlertTpl, subjectLine } =
    require('../scripts/email-templates');

  let result;
  if (template === 'weekly') {
    result = weeklySnapshot(data, null, userId);
  } else if (template === 'tier') {
    result = tierAlert(data, userId);
  } else if (template === 'spike') {
    result = spikeAlertTpl(data, data.waterConsumptionToday, data.sevenDayAvg, userId);
  } else {
    return res.status(400).json({ error: `Unknown template: ${template}` });
  }

  const name    = creds?.name ?? null;
  const subject = subjectLine(data, name);
  const { html, text } = result;

  return res.status(200).json({
    html,
    subject,
    text,
    variant: variant ?? null,
    template,
    dataUsed: {
      soFarThisCycle:  data.soFarThisCycle,
      nudge:           data.nudge,
      spikeAlert:      data.spikeAlert,
      currentTier:     data.currentTier,
      snowpackSWEPct:  data.snowpackSWEPct,
    },
  });
}

// ── POST /api/webhooks/resend  (Resend event webhook) ────────────────────────
async function handleResendWebhook(req, res) {
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const event = req.body;
  if (!event?.type || !event?.data) return res.status(400).json({ error: 'Invalid payload' });

  const { type, data } = event;
  const ts     = new Date().toISOString();
  const entry  = JSON.stringify({ type, ts, emailId: data.email_id, to: data.to?.[0] });
  const userId = data.tags?.userId ?? null;

  const ops = [
    redis.lpush('waterwise:email-events', entry),
    redis.ltrim('waterwise:email-events', 0, 199),
  ];

  if (type === 'email.opened' && userId) {
    ops.push(redis.hset('waterwise:email-opens', userId, ts));
  }
  if (type === 'email.clicked' && userId) {
    ops.push(redis.hset('waterwise:email-clicks-resend', userId, ts));
  }

  await Promise.all(ops);
  console.log(`Resend webhook: ${type} for ${data.to?.[0] ?? 'unknown'}`);
  return res.status(200).json({ ok: true });
}

// ── Router ────────────────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  const url  = req.url ?? '';
  const tail = url.split('?')[0].replace(/\/?$/, '');

  try {
    if (tail.endsWith('/register')) return await handleRegister(req, res);
    if (tail.endsWith('/data'))     return await handleData(req, res);
    if (tail.endsWith('/status'))   return await handleStatus(req, res);
    if (tail.endsWith('/admin'))     return await handleAdmin(req, res);
    if (tail.endsWith('/analytics')) return await handleAnalytics(req, res);
    if (tail.endsWith('/email-click'))  return await handleEmailClick(req, res);
    if (tail.endsWith('/cron/scrape')) return await handleCronScrape(req, res);
    if (tail.endsWith('/cron/email'))  return await handleCronEmail(req, res);
    if (tail.endsWith('/health'))         return await handleHealth(req, res);
    if (tail.endsWith('/email-preview'))       return await handleEmailPreview(req, res);
    if (tail.endsWith('/webhooks/resend'))     return await handleResendWebhook(req, res);
    return res.status(404).json({ error: 'Unknown /api/user/* route' });
  } catch (err) {
    console.error('api/user error:', err);
    return res.status(500).json({ error: err.message });
  }
};
