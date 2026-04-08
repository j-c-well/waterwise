const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);
const KEY   = 'waterwise:event-log:owner';
const MAX   = 90;

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  try {
    const body = req.body || {};
    const entry = {
      appliance: body.appliance  ?? null,
      startTime: body.startTime  ?? null,
      timezone:  body.timezone   ?? 'America/Denver',
      notes:     body.notes      ?? null,
      loggedAt:  new Date().toISOString(),
    };

    const existing = await redis.get(KEY);
    const log = existing ? JSON.parse(existing) : [];
    log.push(entry);
    if (log.length > MAX) log.splice(0, log.length - MAX);

    await redis.set(KEY, JSON.stringify(log));
    return res.status(200).json({ ok: true, entry, total: log.length });
  } catch (err) {
    console.error('Event log error:', err);
    return res.status(500).json({ error: err.message });
  }
};
