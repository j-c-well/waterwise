const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.query ?? {};
  const KEY = userId ? `waterwise:household:${userId}` : 'waterwise:household:owner';

  try {
    if (req.method === 'GET') {
      const raw = await redis.get(KEY);
      return res.status(200).json(raw ? JSON.parse(raw) : {});
    }

    if (req.method === 'POST') {
      const existing = await redis.get(KEY);
      const profile  = existing ? JSON.parse(existing) : {};
      const body     = req.body || {};

      // Deep merge body into profile
      const updated = { ...profile };
      for (const [k, v] of Object.entries(body)) {
        if (v && typeof v === 'object' && !Array.isArray(v)) {
          updated[k] = { ...(profile[k] || {}), ...v };
        } else {
          updated[k] = v;
        }
      }

      // Derive lastAddedLabel from the first non-meta key in body
      const skipKeys = new Set(['lastAddedLabel', 'lastAddedAt', 'updatedAt']);
      const addedKey = Object.keys(body).find(k => !skipKeys.has(k));
      if (addedKey) {
        const val = body[addedKey];
        const brand = val && typeof val === 'object' ? val.brand : null;
        updated.lastAddedLabel = brand ? `${brand} ${addedKey}` : addedKey;
        updated.lastAddedAt    = new Date().toISOString();
      }
      updated.updatedAt = new Date().toISOString();

      await redis.set(KEY, JSON.stringify(updated));
      return res.status(200).json(updated);
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error('Profile error:', err);
    return res.status(500).json({ error: err.message });
  }
};
