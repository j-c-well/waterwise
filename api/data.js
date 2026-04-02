const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  try {
    const raw = await redis.get('waterwise:latest');
    const data = raw ? JSON.parse(raw) : null;

    if (!data) {
      return res.status(404).json({ error: 'No data yet — scrape has not run' });
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(data);
  } catch (err) {
    console.error('Data fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
};
