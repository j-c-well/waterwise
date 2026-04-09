'use strict';

const Redis = require('ioredis');

// ── helpers ──────────────────────────────────────────────────────────────────

function yesterdayDate() {
  const d = new Date();
  d.setDate(d.getDate() - 1);
  return d.toISOString().slice(0, 10);
}

// Metron's actual format: one row per minute-slot, all classifications as columns
// {"ConsumptionChartDate":"/Date(ms)/","Toilet":0.12,"Shower":null,...}
const METRON_CLS_FIELDS = [
  ['Toilet',           'TOILET'],
  ['WashingMachine',   'WASHING_MACHINE'],
  ['Shower',           'SHOWER'],
  ['BathTub',          'BATHTUB'],
  ['HouseholdUse',     'SINK'],
  ['Sink',             'SINK'],
  ['Irrigation',       'IRRIGATION'],
  ['Leak',             'LEAK'],
  ['IntermittentLeak', 'LEAK'],
  ['Other',            'OTHER'],
];

function parseMsDate(val) {
  if (!val) return null;
  const m = String(val).match(/\/Date\((-?\d+)\)\//);
  if (m) return new Date(parseInt(m[1], 10));
  const d = new Date(val);
  return isNaN(d) ? null : d;
}

function expandRow(item) {
  const time = parseMsDate(
    item.ConsumptionChartDate ?? item.consumptionChartDate ??
    item.StartTime ?? item.startTime ?? item.Timestamp ?? item.timestamp
  );
  const events = [];
  for (const [field, cls] of METRON_CLS_FIELDS) {
    const vol = parseFloat(item[field] ?? 0);
    if (vol > 0) events.push({ time, volume: Math.round(vol * 1000) / 1000, classification: cls, raw: item });
  }
  if (!events.length) {
    const vol = parseFloat(item.Volume ?? item.volume ?? item.dailyLog ?? 0);
    const cls = String(item.Classification ?? item.classification ?? 'UNKNOWN').toUpperCase().replace(/[\s-]/g, '_');
    if (vol > 0) events.push({ time, volume: Math.round(vol * 1000) / 1000, classification: cls, raw: item });
  }
  return events;
}

// Extract a flat array of intervals from whatever shape the API returned
function extractIntervals(stored) {
  const rows = Array.isArray(stored)            ? stored
             : Array.isArray(stored?.data)      ? stored.data
             : Array.isArray(stored?.Data)       ? stored.Data
             : Array.isArray(stored?.intervals) ? stored.intervals
             : (() => {
                 for (const v of Object.values(stored ?? {})) {
                   if (Array.isArray(v) && v.length > 0) return v;
                 }
                 return [];
               })();

  const first = rows[0];
  const isMultiColumn = first && (
    'ConsumptionChartDate' in first || 'consumptionChartDate' in first ||
    METRON_CLS_FIELDS.some(([f]) => f in first)
  );

  if (isMultiColumn) return rows.flatMap(expandRow);

  return rows.map(item => {
    const t = item.StartTime ?? item.startTime ?? item.Timestamp ?? item.timestamp ?? null;
    const v = parseFloat(item.Volume ?? item.volume ?? item.Value ?? 0);
    const c = String(item.Classification ?? item.EventType ?? 'UNKNOWN').toUpperCase().replace(/[\s-]/g, '_');
    return { time: t ? new Date(t) : null, volume: isNaN(v) ? 0 : Math.round(v * 1000) / 1000, classification: c, raw: item };
  });
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
// Pattern: 0.01–0.20G per interval, ≤ 3 min gap, cluster spans ≥ 15 min, total ≥ 0.9G
function applyDishwasherDetection(intervals) {
  const otherIdxs = new Set();

  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => {
      const cls = iv.classification;
      return (cls === 'OTHER' || cls === 'UNKNOWN') &&
             iv.volume >= 0.01 && iv.volume <= 0.20;
    });

  if (!candidates.length) return;

  // Group by ≤ 3 min gap
  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= 3 * MIN) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur);
      cur = [candidates[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    const duration  = clusterDurationMs(grp.map(c => c.iv));
    const totalVol  = grp.reduce((s, c) => s + c.iv.volume, 0);
    if (duration >= 15 * MIN && totalVol >= 0.9) {
      for (const { i } of grp) otherIdxs.add(i);
    }
  }

  for (const i of otherIdxs) {
    intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule1' };
  }

  console.log(`Rule 1: reclassified ${otherIdxs.size} intervals → DISHWASHER`);
}

// Rule 2: Event-log matching → DISHWASHER
// Uses time-of-day comparison (UTC hours+minutes) to avoid date-offset mismatches
// between event log timestamps (local ISO strings) and Metron timestamps (local-as-UTC).
function applyEventLogMatching(intervals, eventLog) {
  const dishwasherEvents = (eventLog ?? []).filter(e =>
    String(e.appliance ?? '').toLowerCase().includes('dishwasher')
  );

  if (!dishwasherEvents.length) return;

  let reclassified = 0;

  for (const evt of dishwasherEvents) {
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;

    const evtTodMin = evtTime.getUTCHours() * 60 + evtTime.getUTCMinutes();
    const WINDOW_MIN = 15;

    let count = 0;
    for (let i = 0; i < intervals.length; i++) {
      const iv = intervals[i];
      if (!iv.time) continue;
      const ivTodMin = iv.time.getUTCHours() * 60 + iv.time.getUTCMinutes();
      if (Math.abs(ivTodMin - evtTodMin) <= WINDOW_MIN) {
        intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule2' };
        count++;
      }
    }
    reclassified += count;
    console.log(`Rule 2: reclassified ${count} intervals → DISHWASHER (TOD ${Math.floor(evtTodMin/60)}:${String(evtTodMin%60).padStart(2,'0')})`);
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
    applyEventLogMatching(intervals, eventLog);
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
