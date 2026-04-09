const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  // Accept ?date=YYYY-MM-DD or default to yesterday
  let date = req.query?.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    date = d.toISOString().slice(0, 10);
  }

  try {
    const raw = await redis.get(`waterwise:corrected:${date}`);
    if (!raw) return res.status(404).json({ error: `No corrected data for ${date}` });

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(JSON.parse(raw));
  } catch (err) {
    console.error('Corrected fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
};
