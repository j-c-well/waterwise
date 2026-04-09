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

// ── appliance-specific thresholds ────────────────────────────────────────────

// Return dishwasher detection thresholds tuned to the household's confirmed appliance.
// Checks profile.dishwasher, profile.appliances.dishwasher, or any appliance array entry.
function dishwasherThresholds(profile) {
  const dw =
    profile?.dishwasher ??
    profile?.appliances?.dishwasher ??
    (Array.isArray(profile?.appliances)
      ? profile.appliances.find(a =>
          String(a.applianceType ?? a.type ?? '').toLowerCase().includes('dishwasher'))
      : null);

  const brand = String(dw?.brand ?? '').toLowerCase();
  const model = String(dw?.model ?? '').toUpperCase();

  // Bosch HE models: SHPM / SHPE / SHEM / SHV / SPE prefix families
  const isBosch = brand.includes('bosch') ||
    /^SH[PEV]|^SPE/.test(model);

  if (isBosch) {
    return {
      label:       `Bosch HE (${dw?.model ?? 'unknown model'})`,
      volumeMin:   0.01,   // G per interval
      volumeMax:   0.25,
      gapMin:      5,      // minutes between draws
      durationMin: 20,     // minimum cluster span (minutes) — Bosch cycles run ≥ 60 min
      totalMin:    2.5,    // minimum cluster total (gallons) — Bosch HE uses 3.5G/cycle; allow partial capture
      lookback:    30,     // Rule 2 match window before logged start (minutes)
      forward:     90,     // Rule 2 match window after logged start (minutes)
    };
  }

  return {
    label:       'generic dishwasher',
    volumeMin:   0.01,
    volumeMax:   0.20,
    gapMin:      3,
    durationMin: 15,
    totalMin:    0.9,
    lookback:    15,
    forward:     15,
  };
}

// ── correction rules ──────────────────────────────────────────────────────────

// Rule 1: Sustained OTHER bursts → DISHWASHER
// Thresholds sourced from confirmed household appliance profile when available.
function applyDishwasherDetection(intervals, profile) {
  const thresh = dishwasherThresholds(profile);
  console.log(`Rule 1: using thresholds for ${thresh.label}`);
  const otherIdxs = new Set();

  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => {
      const cls = iv.classification;
      return (cls === 'OTHER' || cls === 'UNKNOWN') &&
             iv.volume >= thresh.volumeMin && iv.volume <= thresh.volumeMax;
    });

  if (!candidates.length) return;

  const gapLimit = thresh.gapMin * MIN;

  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= gapLimit) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur);
      cur = [candidates[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    const duration = clusterDurationMs(grp.map(c => c.iv));
    const totalVol = grp.reduce((s, c) => s + c.iv.volume, 0);
    if (duration >= thresh.durationMin * MIN && totalVol >= thresh.totalMin) {
      for (const { i } of grp) otherIdxs.add(i);
    }
  }

  for (const i of otherIdxs) {
    intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule1' };
  }

  console.log(`Rule 1: reclassified ${otherIdxs.size} intervals → DISHWASHER`);
}

// Rule 2: Event-log matching → DISHWASHER
// Uses time-of-day comparison (UTC hours+minutes) to avoid date-offset mismatches.
// Asymmetric window: lookback covers pre-wash phase; forward covers full cycle duration.
function applyEventLogMatching(intervals, eventLog, profile) {
  const thresh = dishwasherThresholds(profile);

  const dishwasherEvents = (eventLog ?? []).filter(e =>
    String(e.appliance ?? '').toLowerCase().includes('dishwasher')
  );

  if (!dishwasherEvents.length) return;

  let reclassified = 0;

  for (const evt of dishwasherEvents) {
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;

    const evtTodMin = evtTime.getUTCHours() * 60 + evtTime.getUTCMinutes();
    const winStart  = evtTodMin - thresh.lookback;
    const winEnd    = evtTodMin + thresh.forward;

    let count = 0;
    for (let i = 0; i < intervals.length; i++) {
      const iv = intervals[i];
      if (!iv.time) continue;
      const ivTodMin = iv.time.getUTCHours() * 60 + iv.time.getUTCMinutes();
      if (ivTodMin >= winStart && ivTodMin <= winEnd) {
        intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule2' };
        count++;
      }
    }
    reclassified += count;
    console.log(`Rule 2 (${thresh.label}): reclassified ${count} intervals → DISHWASHER` +
      ` (window ${Math.floor(winStart/60)}:${String(winStart%60).padStart(2,'0')}` +
      `–${Math.floor(winEnd/60)}:${String(winEnd%60).padStart(2,'0')})`);
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

// Rule 4: Sustained high-flow TOILET → SHOWER
// Mirrors daily-timeline Rule 4: 3+ consecutive TOILET/TOILET_FULL intervals all > 1.0G,
// spanning > 3 min. Metron misclassifies long showers as Toilet on this meter.
function applyShowerReclassification(intervals) {
  const toiletIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => iv.classification === 'TOILET' || iv.classification === 'TOILET_FULL');

  if (!toiletIdxs.length) return 0;

  const groups = [];
  let cur = [toiletIdxs[0]];
  for (let k = 1; k < toiletIdxs.length; k++) {
    const gap = toiletIdxs[k].iv.time && toiletIdxs[k - 1].iv.time
      ? toiletIdxs[k].iv.time - toiletIdxs[k - 1].iv.time
      : Infinity;
    if (gap <= 3 * MIN) cur.push(toiletIdxs[k]);
    else { groups.push(cur); cur = [toiletIdxs[k]]; }
  }
  groups.push(cur);

  let count = 0;
  for (const grp of groups) {
    const allHigh = grp.every(({ iv }) => iv.volume > 1.0);
    const span = grp.length >= 2 && grp[0].iv.time && grp[grp.length - 1].iv.time
      ? grp[grp.length - 1].iv.time - grp[0].iv.time
      : 0;
    if (grp.length >= 3 && allHigh && span > 3 * MIN) {
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: 'SHOWER', correctedBy: 'rule4' };
        count++;
      }
    }
  }
  console.log(`Rule 4: reclassified ${count} intervals → SHOWER`);
  return count;
}

// Rule 5: Bidet seat detection — requires profile.bidetSeat (any truthy value)
// Runs after shower reclassification so converted shower intervals don't trigger bidet pairing.
// Three sub-patterns:
//   a) Small OTHER 0–3 min BEFORE a full flush → BIDET_WASH
//   b) Small OTHER/SINK 1–4 min AFTER a full flush → BIDET_REFILL
//   c) Isolated overnight OTHER (11pm–6am) single interval → BIDET_SELFCLEAN
function applyBidetDetection(intervals, profile) {
  if (!profile?.bidetSeat) return 0;
  let count = 0;

  for (let i = 0; i < intervals.length; i++) {
    const flush = intervals[i];
    if (flush.classification !== 'TOILET_FULL' || flush.volume < 1.2 || !flush.time) continue;

    const flushTs = flush.time.getTime();

    // (a) pre-flush wash
    for (let j = 0; j < i; j++) {
      const pre = intervals[j];
      if (!pre.time) continue;
      if (pre.classification === 'BIDET_WASH' || pre.classification === 'BIDET_REFILL') continue;
      const diffMin = (flushTs - pre.time.getTime()) / MIN;
      if (diffMin < 0 || diffMin > 3) continue;
      if (pre.classification !== 'OTHER' && pre.classification !== 'UNKNOWN') continue;
      if (pre.volume < 0.05 || pre.volume > 0.20) continue;
      intervals[j] = { ...pre, classification: 'BIDET_WASH', correctedBy: 'rule5', correctionRule: 'bidet-pre-flush' };
      count++;
    }

    // (b) post-flush tank refill
    for (let j = i + 1; j < intervals.length; j++) {
      const post = intervals[j];
      if (!post.time) continue;
      if (post.classification === 'BIDET_WASH' || post.classification === 'BIDET_REFILL') continue;
      const diffMin = (post.time.getTime() - flushTs) / MIN;
      if (diffMin < 1 || diffMin > 4) continue;
      const cls = post.classification;
      if (cls !== 'OTHER' && cls !== 'UNKNOWN' && cls !== 'SINK') continue;
      if (post.volume < 0.05 || post.volume > 0.25) continue;
      intervals[j] = { ...post, classification: 'BIDET_REFILL', correctedBy: 'rule5', correctionRule: 'bidet-post-flush' };
      count++;
    }
  }

  // (c) overnight self-clean cycles
  for (let i = 0; i < intervals.length; i++) {
    const iv = intervals[i];
    if (iv.classification !== 'OTHER' && iv.classification !== 'UNKNOWN') continue;
    if (!iv.time) continue;
    if (iv.volume < 0.04 || iv.volume > 0.12) continue;

    const h = iv.time.getUTCHours();
    if (h >= 6 && h < 23) continue; // not overnight

    const ts = iv.time.getTime();
    const isolated = intervals.every((other, j) => {
      if (j === i || !other.time || other.volume <= 0) return true;
      return Math.abs(other.time.getTime() - ts) > 10 * MIN;
    });
    if (!isolated) continue;

    intervals[i] = { ...iv, classification: 'BIDET_SELFCLEAN', correctedBy: 'rule5', correctionRule: 'bidet-selfclean' };
    count++;
  }

  console.log(`Rule 4: bidet — ${count} intervals reclassified (BIDET_WASH/REFILL/SELFCLEAN)`);
  return count;
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

  const bidetWash      = round(sum['BIDET_WASH']      ?? 0);
  const bidetRefill    = round(sum['BIDET_REFILL']    ?? 0);
  const bidetSelfClean = round(sum['BIDET_SELFCLEAN'] ?? 0);
  const bidetTotal     = round(bidetWash + bidetRefill + bidetSelfClean);

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
    bidet:         { wash: bidetWash, refill: bidetRefill, selfClean: bidetSelfClean, total: bidetTotal },
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
    applyDishwasherDetection(intervals, profile);
    applyEventLogMatching(intervals, eventLog, profile);
    applyToiletSplit(intervals);
    applyShowerReclassification(intervals);
    applyBidetDetection(intervals, profile);

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
