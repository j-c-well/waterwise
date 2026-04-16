const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') {
    res.status(200).end();
    return;
  }

  try {
    const { userId } = req.query ?? {};
    const ns = userId ? `waterwise:${userId}` : 'waterwise';
    const profileKey = userId ? `waterwise:household:${userId}` : 'waterwise:household:owner';

    const [raw, profileRaw] = await Promise.all([
      redis.get(`${ns}:latest`),
      redis.get(profileKey),
    ]);

    const data = raw ? JSON.parse(raw) : null;
    if (!data) {
      return res.status(404).json({ error: 'No data yet — scrape has not run' });
    }

    const staleHours = Math.round((Date.now() - new Date(data.scrapedAt)) / 3600000);
    const dataStale  = staleHours > 25;
    const householdProfile = profileRaw ? JSON.parse(profileRaw) : null;

    // Fetch corrected interval data for this consumption date (available after corrections.js runs)
    const correctedRaw = data.consumptionDate
      ? await redis.get(`${ns}:corrected:${data.consumptionDate}`)
      : null;
    const corrected = correctedRaw ? JSON.parse(correctedRaw) : null;

    // Build fixtures: prefer corrected classification; fall back to raw Metron data.
    // Metron labels the dishwasher as "Kitchen" — rename it here regardless of source.
    let fixtures;
    let fixturesSource;
    let leakAlert = null;

    if (corrected?.correctedFixtures) {
      const cf = corrected.correctedFixtures;
      fixtures = {
        toilet:         cf.toilet,          // { halfFlush, fullFlush, total }
        sink:           cf.sink,
        shower:         cf.shower,
        bath:           cf.bath,            // tub fills reclassified from shower
        dishwasher:     cf.dishwasher,      // corrected engine already uses "dishwasher"
        washingMachine: cf.washingMachine,
        bidet:          cf.bidet,           // { wash, refill, selfClean, total } — present if bidetSeat in profile
        other:          cf.other,
        date:           data.fixtures?.date ?? data.consumptionDate,
      };
      fixturesSource = 'corrected';

      // Leak alert: only flag if LEAK gallons remain after bath reclassification
      const residualLeak = corrected.correctedFixtures._rawSums?.LEAK ?? 0;
      leakAlert = residualLeak > 5
        ? { gallons: Math.round(residualLeak * 10) / 10, detected: true }
        : null;
    } else {
      const mf = data.fixtures ?? {};
      fixtures = {
        toilet:         mf.toilet,
        sink:           mf.sink,
        shower:         mf.shower,
        dishwasher:     mf.kitchen ?? mf.dishwasher ?? 0, // rename kitchen → dishwasher
        bathtub:        mf.bathtub,
        washingMachine: mf.washingMachine,
        date:           mf.date,
      };
      fixturesSource = 'metron';
    }

    // Anomaly summary
    const anomalyKey2 = `${ns}:anomalies:${data.consumptionDate}`;
    const anomalyRaw2 = data.consumptionDate ? await redis.get(anomalyKey2) : null;
    const anomalies2  = anomalyRaw2 ? JSON.parse(anomalyRaw2) : [];
    const unconfirmedAnomalies = anomalies2.filter(a => !a.confirmedAt).length;

    // Seasonal context
    const now = new Date();
    const month = now.getMonth() + 1; // 1-12
    const day   = now.getDate();
    const irrigationSeason    = (month > 5 || (month === 5 && day >= 1)) && month < 11; // May 1 – Oct 31
    const irrigationApproaching = month === 4 && day >= 15; // Apr 15-30

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json({
      ...data,
      fixtures,
      fixturesSource,
      dataStale,
      staleHours,
      householdProfile,
      leakAlert,
      anomalies: anomalies2,
      unconfirmedAnomalies,
      seasonalContext: {
        irrigationSeason,
        irrigationApproaching,
        emdNoWaterHours: irrigationSeason ? '10am - 6pm' : null,
        droughtLevel: 1,
      },
    });
  } catch (err) {
    console.error('Data fetch error:', err);
    return res.status(500).json({ error: err.message });
  }
};
