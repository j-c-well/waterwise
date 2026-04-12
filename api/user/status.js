'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.query ?? {};
  if (!userId) {
    return res.status(400).json({ error: 'userId query param required' });
  }

  try {
    const credsRaw = await redis.get(`waterwise:creds:${userId}`);
    if (!credsRaw) {
      return res.status(404).json({ error: 'User not found' });
    }

    const creds  = JSON.parse(credsRaw);
    const latestRaw = await redis.get(`waterwise:${userId}:latest`);
    const latest = latestRaw ? JSON.parse(latestRaw) : null;

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
    });
  } catch (err) {
    console.error('User status error:', err);
    return res.status(500).json({ error: err.message });
  }
};
