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
// Disabled in practice — too many false positives. Kept for completeness.
// Only fires when profile.dishwasher.confirmed === true.
function applyDishwasherDetection(intervals, profile) {
  if (!profile?.dishwasher?.confirmed) {
    console.log('Rule 1: skipped — dishwasher not confirmed in profile');
    return;
  }
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

// Rule 2: Dishwasher window scan → DISHWASHER
// Only fires when profile.dishwasher.confirmed === true.
// Scans the configured runWindow (default 6pm–6am) for OTHER clusters that match
// dishwasher thresholds: ≥ 2.5G total, ≥ 45 min span, no internal gap > 5 min.
// Catches nightly cycles without requiring explicit event-log entries.
function inDishwasherWindow(time, runWindow) {
  const [startH, startM] = (runWindow?.start ?? '18:00').split(':').map(Number);
  const [endH,   endM]   = (runWindow?.end   ?? '06:00').split(':').map(Number);
  const todMin   = time.getUTCHours() * 60 + time.getUTCMinutes();
  const startMin = startH * 60 + startM;
  const endMin   = endH   * 60 + endM;
  if (startMin > endMin) return todMin >= startMin || todMin < endMin; // spans midnight
  return todMin >= startMin && todMin < endMin;
}

function applyDishwasherWindowScan(intervals, profile) {
  if (!profile?.dishwasher?.confirmed) {
    console.log('Rule 2: skipped — dishwasher not confirmed in profile');
    return 0;
  }

  const thresh      = dishwasherThresholds(profile);
  const timeWindows = profile.dishwasher.timeWindows ?? null;
  const runWindow   = profile.dishwasher.runWindow   ?? null;

  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => {
      if (iv.classification !== 'OTHER' && iv.classification !== 'UNKNOWN') return false;
      if (!iv.time) return false;
      if (timeWindows?.length) return timeWindows.some(w => inDishwasherWindow(iv.time, w));
      return inDishwasherWindow(iv.time, runWindow);
    });

  if (!candidates.length) { console.log('Rule 2: no OTHER intervals in dishwasher window'); return 0; }

  // Group with gap ≤ 5 min
  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= 5 * MIN) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur);
      cur = [candidates[k]];
    }
  }
  groups.push(cur);

  let count = 0;
  for (const grp of groups) {
    const totalVol = grp.reduce((s, c) => s + c.iv.volume, 0);
    const spanMin  = clusterDurationMs(grp.map(c => c.iv)) / MIN;

    // Sanity floor
    if (totalVol < 2.0 || spanMin < 30) continue;
    // Full threshold
    if (totalVol < thresh.totalMin || spanMin < 45) continue;

    for (const { i } of grp) {
      intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule2', correctionRule: 'window-scan' };
      count++;
    }
    console.log(`Rule 2 (${thresh.label}): window scan — ${grp.length} intervals, ${Math.round(totalVol * 10) / 10}G, ${Math.round(spanMin)}min → DISHWASHER`);
  }

  if (!count) console.log('Rule 2: no dishwasher clusters found in window');
  return count;
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

  console.log(`Rule 5: bidet — ${count} intervals reclassified (BIDET_WASH/REFILL/SELFCLEAN)`);
  return count;
}

// Rule 7: Continuous WASHING_MACHINE → SHOWER
// A real washing machine has burst/pause/burst fills separated by agitation gaps.
// Continuous unbroken flow ≥ 8 min at 0.8–2.5 GPM and 6–30G is a shower misclassified
// by Metron. Groups with gap ≤ 2 min are shower candidates.
// Separate check: groups with ≤ 5-min gaps spanning 45–90 min at 15–55G are confirmed
// real washer cycles and are logged but kept.
function applyWashingMachineReclassification(intervals) {
  const wmIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      iv.classification === 'WASHING_MACHINE' ||
      iv.classification === 'CLOTHES_WASHER'  ||
      iv.classification === 'CLOTHESWASHER'
    );

  if (!wmIdxs.length) return 0;

  // ── shower candidate groups (gap ≤ 2 min) ────────────────────────────────
  const showerGroups = [];
  let cur = [wmIdxs[0]];
  for (let k = 1; k < wmIdxs.length; k++) {
    const gap = wmIdxs[k].iv.time && wmIdxs[k - 1].iv.time
      ? wmIdxs[k].iv.time - wmIdxs[k - 1].iv.time : Infinity;
    if (gap <= 2 * MIN) { cur.push(wmIdxs[k]); }
    else { showerGroups.push(cur); cur = [wmIdxs[k]]; }
  }
  showerGroups.push(cur);

  let count = 0;
  for (const grp of showerGroups) {
    const first = grp[0].iv;
    const last  = grp[grp.length - 1].iv;
    if (!first.time || !last.time) continue;

    const spanMin  = (last.time - first.time) / MIN;
    const totalGal = grp.reduce((s, c) => s + c.iv.volume, 0);
    const avgFlow  = spanMin > 0 ? totalGal / spanMin : totalGal;
    const hourUTC  = first.time.getUTCHours();

    const isShower =
      spanMin  >= 8   &&
      avgFlow  >= 0.8 && avgFlow  <= 2.5 &&
      totalGal >= 6   && totalGal <= 30  &&
      hourUTC  >= 5   && hourUTC  <  22;

    if (isShower) {
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: 'SHOWER', correctedBy: 'rule7' };
        count++;
      }
      console.log(`Rule 7: reclassified continuous WashingMachine as SHOWER — ${Math.round(spanMin)}min, ${Math.round(totalGal * 10) / 10}G, ${Math.round(avgFlow * 10) / 10} GPM`);
    }
  }

  // ── real washer check: ≤ 5-min gap groups spanning 45–90 min, 15–55G ─────
  const remaining = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      iv.classification === 'WASHING_MACHINE' ||
      iv.classification === 'CLOTHES_WASHER'  ||
      iv.classification === 'CLOTHESWASHER'
    );

  if (remaining.length) {
    const washerGroups = [];
    let wcur = [remaining[0]];
    for (let k = 1; k < remaining.length; k++) {
      const gap = remaining[k].iv.time && remaining[k - 1].iv.time
        ? remaining[k].iv.time - remaining[k - 1].iv.time : Infinity;
      if (gap <= 5 * MIN) { wcur.push(remaining[k]); }
      else { washerGroups.push(wcur); wcur = [remaining[k]]; }
    }
    washerGroups.push(wcur);

    for (const grp of washerGroups) {
      const first = grp[0].iv;
      const last  = grp[grp.length - 1].iv;
      if (!first.time || !last.time) continue;
      const spanMin  = (last.time - first.time) / MIN;
      const totalGal = grp.reduce((s, c) => s + c.iv.volume, 0);
      if (spanMin >= 45 && spanMin <= 90 && totalGal >= 15 && totalGal <= 55) {
        console.log(`Rule 7: confirmed real WashingMachine — ${Math.round(spanMin)}min, ${Math.round(totalGal * 10) / 10}G — keeping as WashingMachine`);
      }
    }
  }

  if (!count) console.log('Rule 7: no WashingMachine→SHOWER reclassifications');
  return count;
}

// Rule 6: High-flow long-duration SHOWER → BATH
// Triggers when: span ≥ 10 min, total ≥ 20G, avg flow ≥ 2.0 GPM,
// and no BIDET_WASH/REFILL within 3 min of either end (bidet flanks a flush, not a fill).
function applyBathDetection(intervals) {
  // Include LEAK alongside SHOWER: Metron sometimes splits a single bath fill
  // across SHOWER + LEAK buckets, so both must be considered as one contiguous event.
  // Only include high-flow LEAK intervals (≥ 0.5G/min) — intermittent leaks run
  // ~0.03G/min and must not be grouped with bath candidates.
  const candidateIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) => iv.classification === 'SHOWER' ||
                        (iv.classification === 'LEAK' && iv.volume >= 0.5));

  if (!candidateIdxs.length) return 0;

  // Group consecutive SHOWER/LEAK intervals (gap ≤ 3 min)
  const groups = [];
  let cur = [candidateIdxs[0]];
  for (let k = 1; k < candidateIdxs.length; k++) {
    const a = candidateIdxs[k - 1].iv.time;
    const b = candidateIdxs[k].iv.time;
    const gap = (a && b) ? b - a : Infinity;
    if (gap <= 3 * MIN) cur.push(candidateIdxs[k]);
    else { groups.push(cur); cur = [candidateIdxs[k]]; }
  }
  groups.push(cur);

  let count = 0;
  for (const grp of groups) {
    const first = grp[0].iv;
    const last  = grp[grp.length - 1].iv;
    if (!first.time || !last.time) continue;

    const spanMin  = (last.time - first.time) / MIN;
    const totalGal = grp.reduce((s, c) => s + c.iv.volume, 0);
    const avgFlow  = spanMin > 0 ? totalGal / spanMin : totalGal;

    if (spanMin  < 10)  continue;
    if (totalGal < 20)  continue;
    if (avgFlow  < 1.8) continue;

    // Reject if a bidet event flanks this group within 3 min
    const firstTs = first.time.getTime();
    const lastTs  = last.time.getTime();
    const hasBidet = intervals.some(iv => {
      if (!iv.time) return false;
      if (iv.classification !== 'BIDET_WASH' && iv.classification !== 'BIDET_REFILL') return false;
      const ts = iv.time.getTime();
      return Math.abs(ts - firstTs) <= 3 * MIN || Math.abs(ts - lastTs) <= 3 * MIN;
    });
    if (hasBidet) continue;

    for (const { i } of grp) {
      intervals[i] = { ...intervals[i], classification: 'BATH', correctedBy: 'rule6', correctionRule: 'high-flow-long-duration' };
      count++;
    }
    console.log(`Rule 6: bath detected — ${grp.length} intervals, ${Math.round(totalGal * 10) / 10}G over ${Math.round(spanMin)} min (${Math.round(avgFlow * 10) / 10} GPM)`);
  }

  if (!count) console.log('Rule 6: no bath events detected');
  return count;
}

// Rule 8: Signature matching → reclassify groups matching user-confirmed patterns
// Reads stored signatures from Redis and reclassifies consecutive OTHER/UNKNOWN/
// WASHING_MACHINE groups that match a confirmed signature on ≥ 3 of 4 criteria:
// totalGallons ±40%, durationMin ±40%, avgFlowGPM ±40%, timeOfDay bucket.
const CATEGORY_TO_CLS = {
  shower: 'SHOWER', bath: 'BATH', sink: 'SINK',
  dishwasher: 'DISHWASHER', laundry: 'WASHING_MACHINE',
};

async function applySignatureMatching(intervals, userId, redis) {
  const ns     = userId ? `waterwise:${userId}` : 'waterwise';
  const sigRaw = await redis.lrange(`${ns}:signatures`, 0, 49);
  if (!sigRaw.length) { console.log('Rule 8: no signatures stored'); return 0; }

  const signatures = sigRaw.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
  const usable     = signatures.filter(s => CATEGORY_TO_CLS[s.category?.toLowerCase()]);
  if (!usable.length) return 0;

  const candidateIdxs = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      iv.classification === 'OTHER' ||
      iv.classification === 'UNKNOWN' ||
      iv.classification === 'WASHING_MACHINE'
    );

  if (!candidateIdxs.length) return 0;

  const groups = [];
  let cur = [candidateIdxs[0]];
  for (let k = 1; k < candidateIdxs.length; k++) {
    const gap = candidateIdxs[k].iv.time && candidateIdxs[k - 1].iv.time
      ? candidateIdxs[k].iv.time - candidateIdxs[k - 1].iv.time : Infinity;
    if (gap <= 5 * MIN) { cur.push(candidateIdxs[k]); }
    else { groups.push(cur); cur = [candidateIdxs[k]]; }
  }
  groups.push(cur);

  let count = 0;
  for (const grp of groups) {
    const first = grp[0].iv;
    const last  = grp[grp.length - 1].iv;
    if (!first.time || !last.time) continue;

    const durationMin  = (last.time - first.time) / MIN;
    const totalGallons = grp.reduce((s, c) => s + c.iv.volume, 0);
    const avgFlowGPM   = durationMin > 0 ? totalGallons / durationMin : totalGallons;
    const h = first.time.getUTCHours();
    const timeOfDay =
      h >= 5 && h < 12 ? 'morning' : h >= 12 && h < 17 ? 'afternoon' :
      h >= 17 && h < 22 ? 'evening' : 'overnight';

    let bestMatch = null;
    let bestScore = 0;

    for (const sig of usable) {
      let score = 0;
      if (sig.totalGallons > 0 && Math.abs(totalGallons - sig.totalGallons) / sig.totalGallons <= 0.4) score++;
      if (sig.durationMin  > 0 && Math.abs(durationMin  - sig.durationMin)  / sig.durationMin  <= 0.4) score++;
      if (sig.avgFlowGPM   > 0 && Math.abs(avgFlowGPM   - sig.avgFlowGPM)   / sig.avgFlowGPM   <= 0.4) score++;
      if (timeOfDay === sig.timeOfDay) score++;
      if (score >= 3 && score > bestScore) { bestScore = score; bestMatch = sig; }
    }

    if (bestMatch) {
      const cls        = CATEGORY_TO_CLS[bestMatch.category.toLowerCase()];
      const confidence = bestScore === 4 ? 'high' : 'medium';
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: cls, correctedBy: 'rule8', correctionRule: 'signature-match', confidence };
        count++;
      }
      console.log(`Rule 8: signature match — ${grp.length} intervals → ${cls} (confidence: ${confidence}, score: ${bestScore}/4)`);
    }
  }

  if (!count) console.log('Rule 8: no signature matches found');
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

  const bath = round(sum['BATH'] ?? 0);

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
    bath,
    washingMachine,
    bidet:         { wash: bidetWash, refill: bidetRefill, selfClean: bidetSelfClean, total: bidetTotal },
    other,
    // Pass-through raw sums for debugging
    _rawSums: Object.fromEntries(
      Object.entries(sum).map(([k, v]) => [k, round(v)])
    ),
  };
}

// ── anomaly detection ─────────────────────────────────────────────────────────

function formatUTCTime(dt) {
  if (!dt) return null;
  const h = dt.getUTCHours();
  const m = dt.getUTCMinutes();
  const h12 = h % 12 || 12;
  const ampm = h >= 12 ? 'PM' : 'AM';
  return h12 + ':' + (m < 10 ? '0' : '') + m + ' ' + ampm;
}

function detectAnomalies(intervals) {
  const anomalies = [];
  let group = [];

  for (const iv of [...intervals, null]) {
    if (iv && (iv.classification === 'OTHER' || iv.classification === 'LEAK')) {
      group.push(iv);
    } else {
      if (group.length > 0) {
        const totalGal  = group.reduce((s, i) => s + i.volume, 0);
        const firstTime = group[0].time;
        const lastTime  = group[group.length - 1].time;
        const durationMin = firstTime && lastTime
          ? Math.round((lastTime - firstTime) / 60000)
          : 0;

        if (totalGal >= 10) {
          anomalies.push({
            timeStart:     formatUTCTime(firstTime),
            timeEnd:       formatUTCTime(lastTime),
            gallons:       Math.round(totalGal * 10) / 10,
            duration:      durationMin,
            intervalCount: group.length,
            category:      null,
            confirmedAt:   null,
          });
        }
        group = [];
      }
    }
  }

  return anomalies;
}

// ── main ──────────────────────────────────────────────────────────────────────

async function main() {
  const redisUrl   = process.env.REDIS_URL;
  const args       = process.argv.slice(2);
  const userIdFlag = args.indexOf('--userId');
  const userId     = userIdFlag !== -1 ? args[userIdFlag + 1] : null;
  const targetDate = args.find(a => /^\d{4}-\d{2}-\d{2}$/.test(a)) ?? yesterdayDate();

  // Key prefix: 'waterwise:{userId}:' for registered users, 'waterwise:' for owner
  const ns = userId ? `waterwise:${userId}` : 'waterwise';

  console.log('corrections.js starting for date:', targetDate, userId ? `(userId: ${userId})` : '(owner)');

  if (!redisUrl) {
    throw new Error('REDIS_URL env var not set');
  }

  const redis = new Redis(redisUrl);

  try {
    // ── read inputs ──
    const [intervalsRaw, profileRaw, eventLogRaw, latestRaw] = await Promise.all([
      redis.get(`${ns}:intervals:${targetDate}`),
      redis.get(`${ns === 'waterwise' ? 'waterwise:household:owner' : `waterwise:household:${userId}`}`),
      redis.get(`${ns === 'waterwise' ? 'waterwise:event-log:owner' : `waterwise:event-log:${userId}`}`),
      redis.get(`${ns}:latest`),
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
    applyDishwasherWindowScan(intervals, profile);
    applyWashingMachineReclassification(intervals);
    applyToiletSplit(intervals);
    applyShowerReclassification(intervals);
    applyBidetDetection(intervals, profile);
    applyBathDetection(intervals);
    await applySignatureMatching(intervals, userId, redis);

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
    const correctedKey = `${ns}:corrected:${targetDate}`;
    await redis.set(correctedKey, JSON.stringify(correctedPayload), 'EX', 7776000); // 90 days
    console.log('Saved', correctedKey);

    // Patch :latest if it matches this date
    if (latest && latest.consumptionDate === targetDate) {
      const latestKey = `${ns}:latest`;
      const patched = { ...latest, correctedFixtures };
      await redis.set(latestKey, JSON.stringify(patched));
      console.log(`Patched ${latestKey} with correctedFixtures`);
    }

    // ── anomaly detection ──
    const anomalies = detectAnomalies(intervals);
    const anomalyKey = `${ns}:anomalies:${targetDate}`;
    const existingAnomalyRaw = await redis.get(anomalyKey);
    const existingAnomalies  = existingAnomalyRaw ? JSON.parse(existingAnomalyRaw) : [];

    // Merge: preserve confirmed entries, add new unconfirmed
    const mergedAnomalies = anomalies.map(a => {
      const confirmed = existingAnomalies.find(e =>
        e.timeStart === a.timeStart && Math.abs(e.gallons - a.gallons) < 1
      );
      return confirmed ?? a;
    });

    await redis.set(anomalyKey, JSON.stringify(mergedAnomalies), 'EX', 7776000);
    console.log(`Anomalies detected: ${mergedAnomalies.length} (${mergedAnomalies.filter(a => !a.confirmedAt).length} unconfirmed)`);

    console.log('corrections.js complete');
  } finally {
    await redis.quit();
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error('CORRECTIONS FAILED:', err.message);
    process.exit(1);
  });
}

// Export pure rule functions for unit testing (no Redis dependency)
module.exports = {
  applyDishwasherDetection,
  applyDishwasherWindowScan,
  inDishwasherWindow,
  applyToiletSplit,
  applyShowerReclassification,
  applyBidetDetection,
  applyWashingMachineReclassification,
  applyBathDetection,
  buildCorrectedFixtures,
  extractIntervals,
  CATEGORY_TO_CLS,
};
