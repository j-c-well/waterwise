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

// ── correction rules ──────────────────────────────────────────────────────────

// Rule 1: sustained OTHER/UNKNOWN bursts → dishwasher
// Pattern: 0.01–0.20G per interval, ≤ 3 min gap, cluster spans ≥ 15 min, total ≥ 0.9G
// (0.9G threshold catches the real Bosch HE pattern; user-stated 1.0G rounds up naturally)
function applyRule1(intervals) {
  let count = 0;
  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      (iv.classification === 'OTHER' || iv.classification === 'UNKNOWN') &&
      iv.volume >= 0.01 && iv.volume <= 0.20
    );

  if (!candidates.length) return count;

  // Sub-group by time proximity (≤ 3 min gap)
  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= 3 * MIN_MS) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur); cur = [candidates[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    const totalVol = grp.reduce((s, c) => s + c.iv.volume, 0);
    if (spanMs(grp.map(c => c.iv)) >= 15 * MIN_MS && totalVol >= 0.9) {
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
function applyRule2(intervals, eventLog) {
  let count = 0;
  const dishEvents = (eventLog ?? []).filter(e =>
    String(e.appliance ?? '').toLowerCase().includes('dishwasher')
  );

  for (const evt of dishEvents) {
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;

    // Time-of-day in minutes (0–1439), read as UTC to match Metron's local-as-UTC storage
    const evtTodMin = evtTime.getUTCHours() * 60 + evtTime.getUTCMinutes();
    const WINDOW_MIN = 15;

    let matched = 0;
    for (let i = 0; i < intervals.length; i++) {
      const iv = intervals[i];
      if (!iv.time) continue;
      const ivTodMin = iv.time.getUTCHours() * 60 + iv.time.getUTCMinutes();
      if (Math.abs(ivTodMin - evtTodMin) <= WINDOW_MIN) {
        intervals[i] = { ...iv, classification: 'DISHWASHER', correctedBy: 'rule2', correctionRule: 'event-log-match', confidence: 'high' };
        matched++;
      }
    }
    count += matched;
    console.log(`Rule 2: matched ${matched} intervals to event at TOD ${Math.floor(evtTodMin/60)}:${String(evtTodMin%60).padStart(2,'0')}`);
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
};

function buildSummary(intervals) {
  const totals = { toilet: 0, shower: 0, sink: 0, dishwasher: 0, washingMachine: 0, other: 0 };
  const toiletHalf = { count: 0, total: 0 };
  const toiletFull = { count: 0, total: 0 };

  for (const iv of intervals) {
    const key = CLS_TO_KEY[iv.classification] ?? 'other';
    totals[key] = (totals[key] ?? 0) + iv.volume;

    if (iv.classification === 'TOILET_HALF') { toiletHalf.count++; toiletHalf.total += iv.volume; }
    if (iv.classification === 'TOILET_FULL') { toiletFull.count++; toiletFull.total += iv.volume; }
  }

  const r = (v) => Math.round(v * 10) / 10;

  return {
    toilet:        { total: r(totals.toilet), halfFlush: toiletHalf.count, fullFlush: toiletFull.count },
    shower:        { total: r(totals.shower) },
    sink:          { total: r(totals.sink) },
    dishwasher:    { total: r(totals.dishwasher) },
    washingMachine:{ total: r(totals.washingMachine) },
    other:         { total: r(totals.other) },
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

// Group consecutive same-classification intervals into single events.
// Gap tolerances:
//   shower  — 5 min (concurrent sink adjustments during a shower don't break the group;
//             we allow a slightly wider window so interleaved temp-adjust blips are skipped)
//   default — 3 min
// Groups by output classification key + corrected flag.
const GAP_BY_CLS = { shower: 5 * MIN_MS };
const DEFAULT_GAP = 3 * MIN_MS;

function groupIntervals(intervals) {
  const groups = [];
  let cur = null;

  for (const iv of intervals) {
    if (iv.volume <= 0) continue;
    const outCls    = clsToOutput(iv.classification);
    const corrected = !!iv.correctedBy;
    const maxGap    = GAP_BY_CLS[outCls] ?? DEFAULT_GAP;

    if (
      cur &&
      cur.outCls    === outCls &&
      cur.corrected === corrected &&
      iv.time &&
      gapMs(cur.last, iv) <= maxGap
    ) {
      cur.ivs.push(iv);
      cur.last = iv;
    } else {
      if (cur) groups.push(cur);
      cur = { outCls, corrected, ivs: [iv], last: iv };
    }
  }
  if (cur) groups.push(cur);
  return groups;
}

function formatDisplayTime(start, end) {
  if (!end || start === end) return start;
  // "6:18 AM–6:27 AM" → if same period, compress to "6:18–6:27 AM"
  const sM = start.match(/^(.+) (AM|PM)$/);
  const eM = end.match(/^(.+) (AM|PM)$/);
  if (sM && eM && sM[2] === eM[2]) return `${sM[1]}–${end}`;
  return `${start}–${end}`;
}

function buildEvent(grp) {
  const { ivs, outCls, corrected } = grp;
  const first = ivs[0];
  const last  = ivs[ivs.length - 1];

  const gallons   = Math.round(ivs.reduce((s, iv) => s + iv.volume, 0) * 100) / 100;
  const duration  = (first.time && last.time && ivs.length > 1)
    ? Math.round((last.time - first.time) / MIN_MS)
    : 0;

  const timeStart = formatTime(first.time);
  const timeEnd   = (ivs.length > 1 && timeStart !== formatTime(last.time))
    ? formatTime(last.time) : null;
  const displayTime = formatDisplayTime(timeStart, timeEnd);

  // Most-common original Metron classification label across the group
  const rawTally = {};
  for (const iv of ivs) rawTally[iv.metronRaw] = (rawTally[iv.metronRaw] ?? 0) + 1;
  const metronClassification = Object.entries(rawTally).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

  // Lowest confidence wins (most conservative)
  const confidence = ivs.reduce((worst, iv) => {
    const c = confidenceFor(iv);
    return CONF_RANK[c] < CONF_RANK[worst] ? c : worst;
  }, 'high');

  // First non-null correction rule
  const correctionRule = ivs.find(iv => iv.correctionRule)?.correctionRule ?? null;

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
    intervalCount:         ivs.length,
  };
  if (timeEnd) event.timeEnd = timeEnd;
  return event;
}

// ── noise filter ─────────────────────────────────────────────────────────────

function shouldKeep(grp) {
  const gallons  = grp.ivs.reduce((s, iv) => s + iv.volume, 0);
  const duration = grp.ivs.length > 1 && grp.ivs[0].time && grp.ivs.at(-1).time
    ? Math.round((grp.ivs.at(-1).time - grp.ivs[0].time) / MIN_MS)
    : 0;

  // Rule 1 — drop ALL uncorrected "other" unless sustained unknown use
  if (grp.outCls === 'other') {
    if (grp.corrected) return true;                        // event-log matched
    if (duration >= 10 && gallons >= 0.5) return true;    // sustained unknown
    return false;
  }

  // Rule 2 — drop single-interval sink/toilet artifacts < 0.08G
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

  // Parse date param — default to yesterday
  let date = req.query?.date;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    date = d.toISOString().slice(0, 10);
  }

  try {
    const [intervalsRaw, profileRaw, eventLogRaw] = await Promise.all([
      redis.get(`waterwise:intervals:${date}`),
      redis.get('waterwise:household:owner'),
      redis.get('waterwise:event-log:owner'),
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
    corrections += applyRule1(intervals);
    corrections += applyRule2(intervals, eventLog);
    applyRule3(intervals, profile);   // split only — doesn't add to count
    corrections += applyRule4(intervals);

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
