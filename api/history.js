const Redis = require('ioredis');
const { logEvent } = require('../lib/analytics');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  const { userId } = req.query ?? {};
  const ns = userId ? `waterwise:${userId}` : 'waterwise';

  try {
    // Build the last 30 date keys
    const keys = [];
    for (let i = 0; i < 30; i++) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      keys.push(`${ns}:${d.toISOString().slice(0, 10)}`);
    }

    const values = await redis.mget(...keys);

    const history = values
      .map((v, i) => v ? { date: keys[i].replace(`${ns}:`, ''), ...JSON.parse(v) } : null)
      .filter(Boolean)
      .sort((a, b) => a.date.localeCompare(b.date));

    logEvent(redis, { event: 'history_view', userId: userId || 'owner' });
    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(history);
  } catch (err) {
    console.error('History fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
};
