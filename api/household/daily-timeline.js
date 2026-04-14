'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

// ── interval normalization ────────────────────────────────────────────────────
// Metron's actual format: one row per minute-slot, all classifications as columns
// {"ConsumptionChartDate":"/Date(1775606340000)/","Toilet":0.12,"Shower":null,...}

// Classification columns present in Metron's multi-column row format
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

// Parse Microsoft's /Date(ms)/ JSON date format
function parseMsDate(val) {
  if (!val) return null;
  const m = String(val).match(/\/Date\((-?\d+)\)\//);
  if (m) return new Date(parseInt(m[1], 10));
  const d = new Date(val);
  return isNaN(d) ? null : d;
}

// Expand one multi-column Metron row into 0–N single-classification intervals
function expandRow(item) {
  const time = parseMsDate(
    item.ConsumptionChartDate ?? item.consumptionChartDate ??
    item.StartTime ?? item.startTime ?? item.Timestamp ?? item.timestamp
  );

  const events = [];
  for (const [field, cls] of METRON_CLS_FIELDS) {
    const vol = parseFloat(item[field] ?? 0);
    if (vol > 0) {
      events.push({
        time,
        volume:         Math.round(vol * 1000) / 1000,
        metronRaw:      field,
        classification: cls,
      });
    }
  }

  // Fallback: row has no known classification columns — try generic Volume field
  if (!events.length) {
    const vol = parseFloat(item.Volume ?? item.volume ?? item.dailyLog ?? 0);
    const cls  = String(item.Classification ?? item.classification ?? 'UNKNOWN')
                   .toUpperCase().replace(/[\s-]/g, '_');
    if (vol > 0) {
      events.push({ time, volume: Math.round(vol * 1000) / 1000, metronRaw: cls, classification: cls });
    }
  }

  return events;
}

function extractIntervals(stored) {
  const rows = Array.isArray(stored)             ? stored
             : Array.isArray(stored?.data)       ? stored.data
             : Array.isArray(stored?.Data)        ? stored.Data
             : Array.isArray(stored?.intervals)  ? stored.intervals
             : Array.isArray(stored?.Intervals)  ? stored.Intervals
             : (() => {
                 for (const v of Object.values(stored ?? {})) {
                   if (Array.isArray(v) && v.length > 0) return v;
                 }
                 return [];
               })();

  // Detect format: if first row has ConsumptionChartDate or any METRON_CLS_FIELDS key → multi-column
  const first = rows[0];
  const isMultiColumn = first && (
    'ConsumptionChartDate' in first || 'consumptionChartDate' in first ||
    METRON_CLS_FIELDS.some(([f]) => f in first)
  );

  if (isMultiColumn) {
    return rows.flatMap(expandRow);
  }
  // Legacy single-classification-per-row fallback
  return rows.map(item => {
    const timeRaw = item.StartTime ?? item.startTime ?? item.Timestamp ?? item.timestamp ?? null;
    const vol = parseFloat(item.Volume ?? item.volume ?? item.Value ?? 0);
    const cls = String(item.Classification ?? item.EventType ?? item.Category ?? 'UNKNOWN')
                  .toUpperCase().replace(/[\s-]/g, '_');
    return { time: timeRaw ? new Date(timeRaw) : null, volume: isNaN(vol) ? 0 : Math.round(vol * 1000) / 1000, metronRaw: cls, classification: cls };
  });
}

// ── time helpers ──────────────────────────────────────────────────────────────

const MIN_MS = 60_000;

function gapMs(a, b) {
  if (!a?.time || !b?.time) return Infinity;
  return b.time - a.time;
}

// Format UTC time as "6:18 AM" — read UTC fields, no offset conversion
function formatTime(date) {
  if (!date) return null;
  let h = date.getUTCHours();
  const m = date.getUTCMinutes();
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${h}:${String(m).padStart(2, '0')} ${ampm}`;
}

// Group a sorted array into clusters where consecutive gap ≤ maxGapMs
function cluster(intervals, maxGapMs = 5 * MIN_MS) {
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

function spanMs(grp) {
  if (grp.length < 2 || !grp[0].time || !grp[grp.length - 1].time) return 0;
  return grp[grp.length - 1].time - grp[0].time;
}

// ── appliance-specific thresholds ────────────────────────────────────────────

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

  const isBosch = brand.includes('bosch') || /^SH[PEV]|^SPE/.test(model);

  if (isBosch) {
    return {
      label:       `Bosch HE (${dw?.model ?? 'unknown model'})`,
      volumeMin:   0.01,
      volumeMax:   0.25,
      gapMin:      5,
      durationMin: 20,   // Bosch cycles run ≥ 60 min; 20 min minimum avoids short false positives
      totalMin:    2.5,  // Bosch HE uses 3.5G/cycle; 2.5G allows for partial capture
      lookback:    30,
      forward:     90,
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

// Rule 1: sustained OTHER/UNKNOWN bursts → dishwasher
// Thresholds sourced from confirmed household appliance profile when available.
function applyRule1(intervals, profile) {
  const thresh = dishwasherThresholds(profile);
  let count = 0;
  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      (iv.classification === 'OTHER' || iv.classification === 'UNKNOWN') &&
      iv.volume >= thresh.volumeMin && iv.volume <= thresh.volumeMax
    );

  if (!candidates.length) return count;

  const gapLimit = thresh.gapMin * MIN_MS;

  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= gapLimit) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur); cur = [candidates[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    const totalVol = grp.reduce((s, c) => s + c.iv.volume, 0);
    if (spanMs(grp.map(c => c.iv)) >= thresh.durationMin * MIN_MS && totalVol >= thresh.totalMin) {
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule1', correctionRule: 'sustained-low-flow-pattern' };
        count++;
      }
    }
  }
  return count;
}

// Rule 2: event-log match → reclassify nearest cluster
// Compares TIME-OF-DAY only (UTC hours+minutes) to avoid date-offset issues
// between the event log (local-time ISO strings) and Metron timestamps (local-as-UTC).
// Asymmetric window: lookback covers pre-wash phase; forward covers full cycle duration.
function applyRule2(intervals, eventLog, profile) {
  const thresh = dishwasherThresholds(profile);
  let count = 0;
  const dishEvents = (eventLog ?? []).filter(e =>
    String(e.appliance ?? '').toLowerCase().includes('dishwasher')
  );

  for (const evt of dishEvents) {
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;

    // Time-of-day in minutes (0–1439), read as UTC to match Metron's local-as-UTC storage
    const evtTodMin = evtTime.getUTCHours() * 60 + evtTime.getUTCMinutes();
    const winStart  = evtTodMin - thresh.lookback;
    const winEnd    = evtTodMin + thresh.forward;

    let matched = 0;
    for (let i = 0; i < intervals.length; i++) {
      const iv = intervals[i];
      if (!iv.time) continue;
      const ivTodMin = iv.time.getUTCHours() * 60 + iv.time.getUTCMinutes();
      if (ivTodMin >= winStart && ivTodMin <= winEnd) {
        // Only reclassify unconfidently-classified intervals — don't override SHOWER, TOILET, etc.
        if (iv.classification !== 'OTHER' && iv.classification !== 'SINK') continue;
        intervals[i] = { ...iv, classification: 'DISHWASHER', correctedBy: 'rule2', correctionRule: 'event-log-match', confidence: 'high' };
        matched++;
      }
    }
    count += matched;
    console.log(`Rule 2 (${thresh.label}): matched ${matched} intervals` +
      ` (window ${Math.floor(winStart/60)}:${String(winStart%60).padStart(2,'0')}` +
      `–${Math.floor(winEnd/60)}:${String(winEnd%60).padStart(2,'0')})`);
  }
  return count;
}

// Rule 3: dual-flush toilet split
function applyRule3(intervals, profile) {
  if (profile?.toiletType !== 'dual-flush') return;
  for (let i = 0; i < intervals.length; i++) {
    if (intervals[i].classification === 'TOILET') {
      intervals[i] = {
        ...intervals[i],
        classification: intervals[i].volume < 0.9 ? 'TOILET_HALF' : 'TOILET_FULL',
        correctedBy: 'rule3',
        correctionRule: 'dual-flush-split',
      };
    }
  }
}

// Rule 5: Bidet seat detection — requires profile.bidetSeat (any truthy value)
// Three sub-patterns:
//   a) Small OTHER 0–3 min BEFORE a full flush → BIDET_WASH
//   b) Small OTHER/SINK 1–4 min AFTER a full flush → BIDET_REFILL
//   c) Isolated overnight OTHER (11pm–6am) single interval → BIDET_SELFCLEAN
function applyRule5(intervals, profile) {
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
      const diffMin = (flushTs - pre.time.getTime()) / MIN_MS;
      if (diffMin < 0 || diffMin > 3) continue;
      if (pre.classification !== 'OTHER' && pre.classification !== 'UNKNOWN') continue;
      if (pre.volume < 0.05 || pre.volume > 0.20) continue;
      intervals[j] = { ...pre, classification: 'BIDET_WASH', correctedBy: 'rule5', correctionRule: 'bidet-pre-flush', confidence: 'high' };
      count++;
    }

    // (b) post-flush tank refill
    for (let j = i + 1; j < intervals.length; j++) {
      const post = intervals[j];
      if (!post.time) continue;
      if (post.classification === 'BIDET_WASH' || post.classification === 'BIDET_REFILL') continue;
      const diffMin = (post.time.getTime() - flushTs) / MIN_MS;
      if (diffMin < 1 || diffMin > 4) continue;
      const cls = post.classification;
      if (cls !== 'OTHER' && cls !== 'UNKNOWN' && cls !== 'SINK') continue;
      if (post.volume < 0.05 || post.volume > 0.25) continue;
      intervals[j] = { ...post, classification: 'BIDET_REFILL', correctedBy: 'rule5', correctionRule: 'bidet-post-flush', confidence: 'high' };
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
      return Math.abs(other.time.getTime() - ts) > 10 * MIN_MS;
    });
    if (!isolated) continue;

    intervals[i] = { ...iv, classification: 'BIDET_SELFCLEAN', correctedBy: 'rule5', correctionRule: 'bidet-selfclean', confidence: 'high' };
    count++;
  }

  return count;
}

// Rule 6: High-flow long-duration SHOWER → BATH
// Triggers when: span ≥ 10 min, total ≥ 20G, avg flow ≥ 2.0 GPM,
// and no BIDET_WASH/REFILL within 3 min of either end.
function applyRule6(intervals) {
  // Include LEAK alongside SHOWER: Metron sometimes splits a single bath fill
  // across SHOWER + LEAK buckets, so both must be considered as one contiguous event.
  // Only include high-flow LEAK intervals (≥ 0.5G/min) — intermittent leaks run
  // ~0.03G/min and must not be grouped with bath candidates.
  const candidateIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => iv.classification === 'SHOWER' ||
                        (iv.classification === 'LEAK' && iv.volume >= 0.5));

  if (!candidateIdxs.length) return 0;

  const groups = [];
  let cur = [candidateIdxs[0]];
  for (let k = 1; k < candidateIdxs.length; k++) {
    if (gapMs(candidateIdxs[k - 1].iv, candidateIdxs[k].iv) <= 3 * MIN_MS) {
      cur.push(candidateIdxs[k]);
    } else {
      groups.push(cur); cur = [candidateIdxs[k]];
    }
  }
  groups.push(cur);

  let count = 0;
  for (const grp of groups) {
    const first = grp[0].iv;
    const last  = grp[grp.length - 1].iv;
    if (!first.time || !last.time) continue;

    const spanMin  = (last.time - first.time) / MIN_MS;
    const totalGal = grp.reduce((s, c) => s + c.iv.volume, 0);
    const avgFlow  = spanMin > 0 ? totalGal / spanMin : totalGal;

    if (spanMin  < 10)  continue;
    if (totalGal < 20)  continue;
    if (avgFlow  < 1.8) continue;

    const firstTs = first.time.getTime();
    const lastTs  = last.time.getTime();
    const hasBidet = intervals.some(iv => {
      if (!iv.time) return false;
      if (iv.classification !== 'BIDET_WASH' && iv.classification !== 'BIDET_REFILL') return false;
      const ts = iv.time.getTime();
      return Math.abs(ts - firstTs) <= 3 * MIN_MS || Math.abs(ts - lastTs) <= 3 * MIN_MS;
    });
    if (hasBidet) continue;

    for (const { i } of grp) {
      intervals[i] = { ...intervals[i], classification: 'BATH', correctedBy: 'rule6', correctionRule: 'high-flow-long-duration' };
      count++;
    }
  }
  return count;
}

// Rule 4: sustained "Toilet" > 3 consecutive intervals at > 1.0G → shower
// (handles Metron misclassifying long showers as toilet)
function applyRule4(intervals) {
  let count = 0;
  // Work on clustered groups of TOILET events
  const toiletIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => iv.classification === 'TOILET' || iv.classification === 'TOILET_FULL');

  if (!toiletIdxs.length) return count;

  const groups = [];
  let cur = [toiletIdxs[0]];
  for (let k = 1; k < toiletIdxs.length; k++) {
    if (gapMs(toiletIdxs[k - 1].iv, toiletIdxs[k].iv) <= 3 * MIN_MS) {
      cur.push(toiletIdxs[k]);
    } else {
      groups.push(cur); cur = [toiletIdxs[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    // 3+ consecutive intervals all > 1.0G, spanning > 3 min
    const allHigh = grp.every(({ iv }) => iv.volume > 1.0);
    if (grp.length >= 3 && allHigh && spanMs(grp.map(c => c.iv)) > 3 * MIN_MS) {
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: 'SHOWER', correctedBy: 'rule4', correctionRule: 'sustained-high-flow-toilet' };
        count++;
      }
    }
  }
  return count;
}

// ── output builders ───────────────────────────────────────────────────────────

const CLS_TO_KEY = {
  TOILET:          'toilet',
  TOILET_HALF:     'toilet',
  TOILET_FULL:     'toilet',
  SHOWER:          'shower',
  BATHTUB:         'shower',
  BATH_TUB:        'shower',
  FAUCET:          'sink',
  SINK:            'sink',
  HOT_WATER:       'sink',
  COLD_WATER:      'sink',
  DISHWASHER:      'dishwasher',
  WASHING_MACHINE: 'washingMachine',
  CLOTHES_WASHER:  'washingMachine',
  CLOTHESWASHER:   'washingMachine',
  BIDET_WASH:      'bidet',
  BIDET_REFILL:    'bidet',
  BIDET_SELFCLEAN: 'bidet',
  BATH:            'bath',
};

function buildSummary(intervals) {
  const totals = { toilet: 0, shower: 0, bath: 0, sink: 0, dishwasher: 0, washingMachine: 0, bidet: 0, other: 0 };
  const toiletHalf  = { count: 0, total: 0 };
  const toiletFull  = { count: 0, total: 0 };
  const bidetSub    = { wash: 0, refill: 0, selfClean: 0 };

  for (const iv of intervals) {
    const key = CLS_TO_KEY[iv.classification] ?? 'other';
    totals[key] = (totals[key] ?? 0) + iv.volume;

    if (iv.classification === 'TOILET_HALF')     { toiletHalf.count++; toiletHalf.total += iv.volume; }
    if (iv.classification === 'TOILET_FULL')     { toiletFull.count++; toiletFull.total += iv.volume; }
    if (iv.classification === 'BIDET_WASH')      bidetSub.wash      += iv.volume;
    if (iv.classification === 'BIDET_REFILL')    bidetSub.refill    += iv.volume;
    if (iv.classification === 'BIDET_SELFCLEAN') bidetSub.selfClean += iv.volume;
  }

  const r = (v) => Math.round(v * 10) / 10;

  return {
    toilet:         { total: r(totals.toilet), halfFlush: toiletHalf.count, fullFlush: toiletFull.count },
    shower:         { total: r(totals.shower) },
    bath:           { total: r(totals.bath) },
    sink:           { total: r(totals.sink) },
    dishwasher:     { total: r(totals.dishwasher) },
    washingMachine: { total: r(totals.washingMachine) },
    bidet:          { total: r(totals.bidet), wash: r(bidetSub.wash), refill: r(bidetSub.refill), selfClean: r(bidetSub.selfClean) },
    other:          { total: r(totals.other) },
  };
}

function clsToOutput(cls) {
  const map = {
    TOILET: 'toilet', TOILET_HALF: 'toilet', TOILET_FULL: 'toilet',
    SHOWER: 'shower', BATHTUB: 'shower', BATH_TUB: 'shower',
    FAUCET: 'sink', SINK: 'sink', HOT_WATER: 'sink', COLD_WATER: 'sink',
    DISHWASHER: 'dishwasher',
    WASHING_MACHINE: 'washingMachine', CLOTHES_WASHER: 'washingMachine', CLOTHESWASHER: 'washingMachine',
    OTHER: 'other', UNKNOWN: 'other', OUTDOOR: 'other', IRRIGATION: 'other', LEAK: 'other',
    BIDET_WASH: 'bidet', BIDET_REFILL: 'bidet', BIDET_SELFCLEAN: 'bidet',
    BATH: 'bath',
  };
  return map[cls] ?? 'other';
}

const CONF_RANK = { low: 0, medium: 1, high: 2 };

function confidenceFor(iv) {
  if (iv.confidence) return iv.confidence;
  if (iv.correctedBy === 'rule2') return 'high';
  if (iv.correctedBy === 'rule1') return 'medium';
  if (iv.correctedBy) return 'medium';
  return 'high';
}

// ── grouping ──────────────────────────────────────────────────────────────────

const DEFAULT_GAP = 3 * MIN_MS;

// Phase 1: bucket intervals that share the exact same timestamp.
// Metron can assign multiple classifications to a single minute (e.g. shower + toilet).
// Primary = highest-volume interval; the rest are concurrent.
function bucketByTimestamp(intervals) {
  const map = new Map();
  for (const iv of intervals) {
    if (iv.volume <= 0 || !iv.time) continue;
    const key = iv.time.getTime();
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(iv);
  }
  return [...map.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([ts, ivs]) => {
      ivs.sort((a, b) => b.volume - a.volume); // primary first
      return { ts, ivs };
    });
}

// Phase 2: group buckets into event groups.
//
// Fix 1 — shower lookahead:
//   When the active group is shower and the current bucket is sink/other,
//   look ahead up to 2 minutes for the next shower bucket. If found,
//   absorb the sink/other bucket as a side event and keep the shower group open.
//   This handles temp-adjustment blips at the sink mid-shower.
//
// Fix 2 — concurrent same-minute events:
//   Multiple intervals in the same bucket (different classifications at the same
//   minute) are tracked as concurrentIvs. buildEvent exposes concurrent:true and
//   concurrentWith on the output object.
function groupIntervals(intervals) {
  const buckets = bucketByTimestamp(intervals);
  const groups  = [];
  let cur = null;

  for (let bi = 0; bi < buckets.length; bi++) {
    const { ts, ivs } = buckets[bi];
    const primary   = ivs[0];
    const outCls    = clsToOutput(primary.classification);
    const corrected = !!primary.correctedBy;

    // Fix 1: shower lookahead — absorb intervening sink/other buckets
    if (cur?.outCls === 'shower' && !corrected &&
        (outCls === 'sink' || outCls === 'other')) {
      const nextShowerSoon = buckets.slice(bi + 1).some(b => {
        if (b.ts - cur.lastTs > 2 * MIN_MS) return false;
        return clsToOutput(b.ivs[0].classification) === 'shower';
      });
      if (nextShowerSoon) {
        cur.sideIvs.push(...ivs);  // absorbed, not shown as separate event
        continue;
      }
    }

    if (cur && cur.outCls === outCls && cur.corrected === corrected &&
        ts - cur.lastTs <= DEFAULT_GAP) {
      // Extend the current group
      cur.ivs.push(primary);
      cur.concurrentIvs.push(...ivs.slice(1));
      cur.lastTs = ts;
    } else {
      if (cur) groups.push(cur);
      cur = {
        outCls,
        corrected,
        ivs:           [primary],
        concurrentIvs: ivs.slice(1),  // Fix 2: same-minute concurrent intervals
        sideIvs:       [],             // Fix 1: absorbed shower interruptions
        lastTs:        ts,
      };
    }
  }
  if (cur) groups.push(cur);
  return groups;
}

function formatDisplayTime(start, end) {
  if (!end || start === end) return start;
  const sM = start.match(/^(.+) (AM|PM)$/);
  const eM = end.match(/^(.+) (AM|PM)$/);
  if (sM && eM && sM[2] === eM[2]) return `${sM[1]}–${end}`;
  return `${start}–${end}`;
}

function buildEvent(grp) {
  const { ivs, concurrentIvs, sideIvs, outCls, corrected } = grp;
  const first = ivs[0];
  const last  = ivs[ivs.length - 1];

  // Gallons = all primary + concurrent + absorbed side events
  const allIvs   = [...ivs, ...concurrentIvs];
  const gallons  = Math.round(
    [...allIvs, ...sideIvs].reduce((s, iv) => s + iv.volume, 0) * 100
  ) / 100;
  const duration = (first.time && last.time && ivs.length > 1)
    ? Math.round((last.time - first.time) / MIN_MS)
    : 0;

  const timeStart = formatTime(first.time);
  const timeEnd   = (ivs.length > 1 && timeStart !== formatTime(last.time))
    ? formatTime(last.time) : null;
  const displayTime = formatDisplayTime(timeStart, timeEnd);

  // Most-common Metron raw label from primary intervals
  const rawTally = {};
  for (const iv of ivs) rawTally[iv.metronRaw] = (rawTally[iv.metronRaw] ?? 0) + 1;
  const metronClassification = Object.entries(rawTally).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

  // Lowest confidence wins
  const confidence = allIvs.reduce((worst, iv) => {
    const c = confidenceFor(iv);
    return CONF_RANK[c] < CONF_RANK[worst] ? c : worst;
  }, 'high');

  const correctionRule = allIvs.find(iv => iv.correctionRule)?.correctionRule ?? null;

  // Fix 2: dominant concurrent classification (highest total volume among concurrent)
  let concurrentWith = null;
  if (concurrentIvs.length > 0) {
    const byKey = {};
    for (const iv of concurrentIvs) {
      const k = clsToOutput(iv.classification);
      byKey[k] = (byKey[k] ?? 0) + iv.volume;
    }
    concurrentWith = Object.entries(byKey).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
  }

  const event = {
    timeStart,
    displayTime,
    gallons,
    duration,
    classification:        outCls,
    metronClassification,
    corrected,
    confidence,
    correctionRule,
    intervalCount:         ivs.length + concurrentIvs.length,
  };
  if (timeEnd)       event.timeEnd       = timeEnd;
  if (concurrentWith) {
    event.concurrent     = true;
    event.concurrentWith = concurrentWith;
  }
  return event;
}

// ── noise filter ─────────────────────────────────────────────────────────────

function shouldKeep(grp) {
  const allIvs  = [...grp.ivs, ...grp.concurrentIvs];
  const gallons = allIvs.reduce((s, iv) => s + iv.volume, 0);
  const duration = grp.ivs.length > 1 && grp.ivs[0].time && grp.ivs.at(-1).time
    ? Math.round((grp.ivs.at(-1).time - grp.ivs[0].time) / MIN_MS)
    : 0;

  // Drop ALL uncorrected "other" unless it's a sustained unknown
  if (grp.outCls === 'other') {
    if (grp.corrected) return true;
    if (duration >= 10 && gallons >= 0.5) return true;
    return false;
  }

  // Drop single-interval sink/toilet meter artifacts < 0.08G
  if (grp.ivs.length === 1 && gallons < 0.08 &&
      (grp.outCls === 'sink' || grp.outCls === 'toilet')) {
    return false;
  }

  return true;
}

// ── handler ───────────────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'GET') return res.status(405).json({ error: 'Method not allowed' });

  // Parse date and userId params
  let date = req.query?.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    date = d.toISOString().slice(0, 10);
  }
  const { userId } = req.query ?? {};
  const ns = userId ? `waterwise:${userId}` : 'waterwise';

  try {
    const [intervalsRaw, profileRaw, eventLogRaw] = await Promise.all([
      redis.get(`${ns}:intervals:${date}`),
      redis.get(userId ? `waterwise:household:${userId}` : 'waterwise:household:owner'),
      redis.get(userId ? `waterwise:event-log:${userId}` : 'waterwise:event-log:owner'),
    ]);

    if (!intervalsRaw) {
      return res.status(404).json({ error: `Interval data not yet available for ${date}` });
    }

    const stored   = JSON.parse(intervalsRaw);
    const profile  = profileRaw  ? JSON.parse(profileRaw)  : null;
    const eventLog = eventLogRaw ? JSON.parse(eventLogRaw) : [];

    // Normalize and sort chronologically
    const intervals = extractIntervals(stored);
    intervals.sort((a, b) => (a.time ?? 0) - (b.time ?? 0));

    if (!intervals.length) {
      return res.status(422).json({ error: 'Interval data present but could not be parsed', raw: stored });
    }

    // Apply corrections (mutate intervals in-place)
    let corrections = 0;
    corrections += applyRule1(intervals, profile);
    corrections += applyRule2(intervals, eventLog, profile);
    applyRule3(intervals, profile);   // split only — doesn't add to count
    corrections += applyRule4(intervals);
    corrections += applyRule5(intervals, profile);
    corrections += applyRule6(intervals);

    // Summary runs on ALL corrected intervals before noise filtering
    const summary = buildSummary(intervals);

    // Group consecutive same-classification intervals → one event per use
    const groups = groupIntervals(intervals);

    const filtered = groups.filter(shouldKeep);

    const events = filtered.map(buildEvent);

    const body = {
      date,
      timezone: 'local-as-utc',
      events,
      summary,
      corrections,
    };

    // Raw sample in non-prod for debugging Metron field names
    if (process.env.NODE_ENV !== 'production') {
      const raw = Array.isArray(stored) ? stored : Object.values(stored)[0];
      body._meta = {
        rawSample:      Array.isArray(raw) ? raw.slice(0, 3) : raw,
        totalIntervals: intervals.length,
        totalGroups:    groups.length,
        afterFilter:    filtered.length,
      };
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(body);
  } catch (err) {
    console.error('daily-timeline error:', err);
    return res.status(500).json({ error: err.message });
  }
};
