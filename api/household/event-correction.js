const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);
const KEY   = 'waterwise:corrections:owner';
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
      date:                   body.date                   ?? null,
      timeWindow:             body.timeWindow             ?? null,
      metronClassification:   body.metronClassification   ?? null,
      userCorrection:         body.userCorrection         ?? null,
      applianceBrand:         body.applianceBrand         ?? null,
      applianceModel:         body.applianceModel         ?? null,
      submittedAt:            new Date().toISOString(),
    };

    const existing = await redis.get(KEY);
    const corrections = existing ? JSON.parse(existing) : [];
    corrections.push(entry);
    if (corrections.length > MAX) corrections.splice(0, corrections.length - MAX);

    await redis.set(KEY, JSON.stringify(corrections));
    return res.status(200).json({ ok: true, entry, total: corrections.length });
  } catch (err) {
    console.error('Event correction error:', err);
    return res.status(500).json({ error: err.message });
  }
};
