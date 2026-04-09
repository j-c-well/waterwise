'use strict';

const Redis = require('ioredis');

// ── helpers ──────────────────────────────────────────────────────────────────

function yesterdayDate() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

// Accept any reasonable field-name variant from Metron's response
function normalizeInterval(item) {
  const timeRaw = item.StartTime ?? item.startTime ?? item.Timestamp ?? item.timestamp ??
                  item.IntervalStart ?? item.intervalStart ?? null;
  const volume  = parseFloat(
    item.Volume ?? item.volume ?? item.GallonsUsed ?? item.gallons ??
    item.Value  ?? item.value  ?? 0
  );
  const cls = String(
    item.Classification ?? item.classification ??
    item.EventType      ?? item.eventType      ??
    item.Category       ?? item.category       ?? 'UNKNOWN'
  ).toUpperCase().replace(/[\s-]/g, '_');

  return { time: timeRaw ? new Date(timeRaw) : null, volume, classification: cls, raw: item };
}

// Extract a flat array of intervals from whatever shape the API returned
function extractIntervals(stored) {
  if (Array.isArray(stored))         return stored.map(normalizeInterval);
  if (Array.isArray(stored?.data))   return stored.data.map(normalizeInterval);
  if (Array.isArray(stored?.Data))   return stored.Data.map(normalizeInterval);
  if (Array.isArray(stored?.intervals)) return stored.intervals.map(normalizeInterval);
  // Fallback: try all top-level array-valued keys
  for (const v of Object.values(stored ?? {})) {
    if (Array.isArray(v) && v.length > 0) return v.map(normalizeInterval);
  }
  return [];
}

// Gap between two intervals in milliseconds
function gapMs(a, b) {
  if (!a.time || !b.time) return Infinity;
  return Math.abs(b.time - a.time);
}

const MIN = 60_000; // ms

// Group sorted intervals into clusters where consecutive gap ≤ maxGapMs
function cluster(intervals, maxGapMs = 5 * MIN) {
  const groups = [];
  let cur = [];
  for (const iv of intervals) {
    if (!cur.length || gapMs(cur[cur.length - 1], iv) <= maxGapMs) {
      cur.push(iv);
    } else {
      groups.push(cur);
      cur = [iv];
    }
  }
  if (cur.length) groups.push(cur);
  return groups;
}

function clusterDurationMs(grp) {
  if (grp.length < 2 || !grp[0].time || !grp[grp.length - 1].time) return 0;
  return grp[grp.length - 1].time - grp[0].time;
}

// ── correction rules ──────────────────────────────────────────────────────────

// Rule 1: Sustained OTHER bursts → DISHWASHER
// Pattern: cluster of OTHER intervals, each 0.05–0.20 G, lasting ≥ 20 min
function applyDishwasherDetection(intervals) {
  const otherIdxs = new Set();

  // Collect indices of OTHER-classified, in-range-volume intervals
  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => {
      const cls = iv.classification;
      return (cls === 'OTHER' || cls === 'UNKNOWN') &&
             iv.volume >= 0.05 && iv.volume <= 0.20;
    });

  if (!candidates.length) return;

  // Group candidate indices by time proximity (≤ 5 min gap)
  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    const prev = candidates[k - 1];
    const next = candidates[k];
    if (gapMs(prev.iv, next.iv) <= 5 * MIN) {
      cur.push(next);
    } else {
      groups.push(cur);
      cur = [next];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    const duration = clusterDurationMs(grp.map(c => c.iv));
    if (duration >= 20 * MIN) {
      for (const { i } of grp) otherIdxs.add(i);
    }
  }

  for (const i of otherIdxs) {
    intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule1' };
  }

  console.log(`Rule 1: reclassified ${otherIdxs.size} intervals → DISHWASHER`);
}

// Rule 2: Event-log matching → DISHWASHER
// For each logged dishwasher event at time T, find the closest interval cluster
// within ±15 min and reclassify
function applyEventLogMatching(intervals, eventLog, targetDate) {
  const dishwasherEvents = (eventLog ?? []).filter(e => {
    const appliance = String(e.appliance ?? '').toLowerCase();
    return appliance.includes('dishwasher');
  });

  if (!dishwasherEvents.length) return;

  let reclassified = 0;

  for (const evt of dishwasherEvents) {
    // Parse event start time; skip if it's not on the target date
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;
    if (evtTime.toISOString().slice(0, 10) !== targetDate) continue;

    const WINDOW = 15 * MIN;

    // Find the cluster whose centroid is nearest to evtTime
    const nearby = intervals.filter(iv => iv.time && Math.abs(iv.time - evtTime) <= WINDOW);

    if (!nearby.length) {
      console.log(`Rule 2: no intervals within ±15 min of logged event at ${evtTime.toISOString()}`);
      continue;
    }

    // Find the cluster containing those nearby intervals
    const nearbyTimes = new Set(nearby.map(iv => iv.time?.toISOString()));
    let count = 0;
    for (let i = 0; i < intervals.length; i++) {
      if (nearbyTimes.has(intervals[i].time?.toISOString())) {
        intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule2' };
        count++;
      }
    }
    reclassified += count;
    console.log(`Rule 2: reclassified ${count} intervals → DISHWASHER (event at ${evtTime.toISOString()})`);
  }

  if (!reclassified) console.log('Rule 2: no matching interval clusters found');
}

// Rule 3: Split TOILET into TOILET_HALF (<0.9G) and TOILET_FULL (≥0.9G)
function applyToiletSplit(intervals) {
  let half = 0, full = 0;
  for (let i = 0; i < intervals.length; i++) {
    if (intervals[i].classification === 'TOILET') {
      if (intervals[i].volume < 0.9) {
        intervals[i] = { ...intervals[i], classification: 'TOILET_HALF', correctedBy: 'rule3' };
        half++;
      } else {
        intervals[i] = { ...intervals[i], classification: 'TOILET_FULL', correctedBy: 'rule3' };
        full++;
      }
    }
  }
  console.log(`Rule 3: toilet split — ${half} half-flush, ${full} full-flush`);
}

// ── fixture summary ───────────────────────────────────────────────────────────

function buildCorrectedFixtures(intervals) {
  const sum = {};
  for (const iv of intervals) {
    sum[iv.classification] = (sum[iv.classification] ?? 0) + iv.volume;
  }

  const round = (v) => Math.round((v ?? 0) * 10) / 10;

  const toiletHalf  = round(sum['TOILET_HALF']       ?? 0);
  const toiletFull  = round(sum['TOILET_FULL']        ?? 0);
  const toiletTotal = round(toiletHalf + toiletFull);

  // Map Metron classification names to our output keys (flexible aliases)
  const sink = round(
    (sum['FAUCET'] ?? 0) + (sum['SINK'] ?? 0) + (sum['HOT_WATER'] ?? 0) +
    (sum['COLD_WATER'] ?? 0)
  );

  const washingMachine = round(
    (sum['WASHING_MACHINE'] ?? 0) + (sum['CLOTHES_WASHER'] ?? 0) +
    (sum['CLOTHESWASHER'] ?? 0)
  );

  const shower = round(
    (sum['SHOWER'] ?? 0) + (sum['BATHTUB'] ?? 0) + (sum['BATH_TUB'] ?? 0)
  );

  const dishwasher = round(sum['DISHWASHER'] ?? 0);

  const other = round(
    (sum['OTHER'] ?? 0) + (sum['UNKNOWN'] ?? 0) + (sum['OUTDOOR'] ?? 0) +
    (sum['IRRIGATION'] ?? 0) + (sum['LEAK'] ?? 0)
  );

  return {
    toilet:        { halfFlush: toiletHalf, fullFlush: toiletFull, total: toiletTotal },
    sink,
    dishwasher,
    shower,
    washingMachine,
    other,
    // Pass-through raw sums for debugging
    _rawSums: Object.fromEntries(
      Object.entries(sum).map(([k, v]) => [k, round(v)])
    ),
  };
}

// ── main ──────────────────────────────────────────────────────────────────────

async function main() {
  const redisUrl    = process.env.REDIS_URL;
  const targetDate  = process.argv[2] ?? yesterdayDate();

  console.log('corrections.js starting for date:', targetDate);

  if (!redisUrl) {
    throw new Error('REDIS_URL env var not set');
  }

  const redis = new Redis(redisUrl);

  try {
    // ── read inputs ──
    const [intervalsRaw, profileRaw, eventLogRaw, latestRaw] = await Promise.all([
      redis.get(`waterwise:intervals:${targetDate}`),
      redis.get('waterwise:household:owner'),
      redis.get('waterwise:event-log:owner'),
      redis.get('waterwise:latest'),
    ]);

    if (!intervalsRaw) {
      console.warn(`No interval data found for ${targetDate} — skipping corrections`);
      return;
    }

    const storedIntervals = JSON.parse(intervalsRaw);
    const profile    = profileRaw  ? JSON.parse(profileRaw)  : null;
    const eventLog   = eventLogRaw ? JSON.parse(eventLogRaw) : [];
    const latest     = latestRaw   ? JSON.parse(latestRaw)   : null;

    // Log raw structure on first pass so we can see Metron's format
    const sampleItems = Array.isArray(storedIntervals)
      ? storedIntervals.slice(0, 2)
      : storedIntervals;
    console.log('Interval data sample:', JSON.stringify(sampleItems, null, 2));

    // ── normalize ──
    const intervals = extractIntervals(storedIntervals);
    console.log(`Loaded ${intervals.length} intervals`);

    if (!intervals.length) {
      console.warn('Could not extract any intervals from stored data — check format above');
      return;
    }

    // Sort chronologically
    intervals.sort((a, b) => (a.time ?? 0) - (b.time ?? 0));

    // Log classification distribution before corrections
    const before = {};
    for (const iv of intervals) before[iv.classification] = (before[iv.classification] ?? 0) + 1;
    console.log('Classifications before corrections:', before);

    // ── apply rules (mutates intervals array) ──
    applyDishwasherDetection(intervals);
    applyEventLogMatching(intervals, eventLog, targetDate);
    applyToiletSplit(intervals);

    const after = {};
    for (const iv of intervals) after[iv.classification] = (after[iv.classification] ?? 0) + 1;
    console.log('Classifications after corrections:', after);

    // ── build outputs ──
    const correctedFixtures = buildCorrectedFixtures(intervals);
    console.log('correctedFixtures:', JSON.stringify(correctedFixtures, null, 2));

    const correctedPayload = {
      date:      targetDate,
      correctedAt: new Date().toISOString(),
      profile:   profile ?? null,
      intervals: intervals.map(iv => ({
        time:           iv.time?.toISOString() ?? null,
        volume:         iv.volume,
        classification: iv.classification,
        correctedBy:    iv.correctedBy ?? null,
      })),
      correctedFixtures,
    };

    // ── save to Redis ──
    const correctedKey = `waterwise:corrected:${targetDate}`;
    await redis.set(correctedKey, JSON.stringify(correctedPayload), 'EX', 7776000); // 90 days
    console.log('Saved', correctedKey);

    // Patch waterwise:latest if it matches this date
    if (latest && latest.consumptionDate === targetDate) {
      const patched = { ...latest, correctedFixtures };
      await redis.set('waterwise:latest', JSON.stringify(patched));
      console.log('Patched waterwise:latest with correctedFixtures');
    }

    console.log('corrections.js complete');
  } finally {
    await redis.quit();
  }
}

main().catch((err) => {
  console.error('CORRECTIONS FAILED:', err.message);
  process.exit(1);
});
