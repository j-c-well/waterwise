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
        body: JSON.stringify({ ref: 'main' }),
      }
    );
    if (response.ok) console.log('Scrape triggered for new user');
    else console.log('Scrape trigger failed:', response.status);
  } catch (e) {
    console.log('Scrape trigger error:', e.message);
  }
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
    dashboardUrl: `https://waterwise-six.vercel.app?user=${userId}`,
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

  logEvent(redis, { event: 'dashboard_load', userId: userId || 'owner' });
  res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
  return res.status(200).json({
    ...data,
    fixtures,
    fixturesSource,
    dataStale,
    staleHours,
    householdProfile,
    leakAlert,
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
    userId:    creds.userId,
    email:     creds.email,
    name:      creds.name,
    hasData:   !!latest,
    lastScraped,
    dataStale: staleHours !== null ? staleHours > 25 : null,
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

  const byDay    = {};
  const byUser   = {};
  let totalEvents = 0;
  const uniqueUsers = new Set();

  for (const day of days) {
    const raw = await redis.lrange(`waterwise:analytics:${day}`, 0, -1);
    if (!raw.length) continue;
    byDay[day] = {};
    for (const item of raw) {
      try {
        const e = JSON.parse(item);
        byDay[day][e.event]  = (byDay[day][e.event]  ?? 0) + 1;
        byUser[e.userId]     = byUser[e.userId] ?? {};
        byUser[e.userId][e.event] = (byUser[e.userId][e.event] ?? 0) + 1;
        uniqueUsers.add(e.userId);
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
    },
    byDay,
    byUser,
  });
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
    return res.status(404).json({ error: 'Unknown /api/user/* route' });
  } catch (err) {
    console.error('api/user error:', err);
    return res.status(500).json({ error: err.message });
  }
};
