'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.query ?? {};

  try {
    // No userId → fall back to owner data (backward compat with /api/data)
    const latestKey = userId ? `waterwise:${userId}:latest` : 'waterwise:latest';
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

    const data = JSON.parse(raw);
    const staleHours = Math.round((Date.now() - new Date(data.scrapedAt)) / 3600000);
    const dataStale  = staleHours > 25;

    // Fetch corrected interval data
    const correctedRaw = data.consumptionDate
      ? await redis.get(
          userId
            ? `waterwise:${userId}:corrected:${data.consumptionDate}`
            : `waterwise:corrected:${data.consumptionDate}`
        )
      : null;
    const corrected = correctedRaw ? JSON.parse(correctedRaw) : null;

    // Fetch household profile
    const profileKey = userId ? `waterwise:household:${userId}` : 'waterwise:household:owner';
    const profileRaw = await redis.get(profileKey);
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
  } catch (err) {
    console.error('User data fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
};
