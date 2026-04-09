'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

// ── interval normalization ────────────────────────────────────────────────────

// Accept any reasonable Metron field-name variant
function normalizeInterval(item) {
  const timeRaw =
    item.StartTime    ?? item.startTime    ?? item.Timestamp  ?? item.timestamp  ??
    item.IntervalTime ?? item.intervalTime ?? item.Time       ?? item.time       ??
    item.IntervalDate ?? item.intervalDate ?? null;

  const volume = parseFloat(
    item.Volume       ?? item.volume       ?? item.GallonsUsed ?? item.gallons   ??
    item.Value        ?? item.value        ?? item.FlowVolume  ?? item.flowVolume ?? 0
  );

  const rawCls = String(
    item.Classification ?? item.classification ??
    item.EventType      ?? item.eventType      ??
    item.Category       ?? item.category       ??
    item.FlowType       ?? item.flowType       ?? 'UNKNOWN'
  );

  return {
    time:          timeRaw ? new Date(timeRaw) : null,
    volume:        isNaN(volume) ? 0 : Math.round(volume * 1000) / 1000,
    // Preserve original exactly; uppercase for rule matching
    metronRaw:     rawCls,
    classification: rawCls.toUpperCase().replace(/[\s\-]/g, '_'),
  };
}

function extractIntervals(stored) {
  if (Array.isArray(stored))              return stored.map(normalizeInterval);
  if (Array.isArray(stored?.data))        return stored.data.map(normalizeInterval);
  if (Array.isArray(stored?.Data))        return stored.Data.map(normalizeInterval);
  if (Array.isArray(stored?.intervals))   return stored.intervals.map(normalizeInterval);
  if (Array.isArray(stored?.Intervals))   return stored.Intervals.map(normalizeInterval);
  for (const v of Object.values(stored ?? {})) {
    if (Array.isArray(v) && v.length > 0) return v.map(normalizeInterval);
  }
  return [];
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
// Each event 0.03–0.25G, spaced ≤ 5 min, cluster spans ≥ 20 min
function applyRule1(intervals) {
  let count = 0;
  const candidates = intervals
    .map((iv, i) => ({ iv, i }))
    .filter(({ iv }) =>
      (iv.classification === 'OTHER' || iv.classification === 'UNKNOWN') &&
      iv.volume >= 0.03 && iv.volume <= 0.25
    );

  if (!candidates.length) return count;

  // Sub-group by time proximity
  const groups = [];
  let cur = [candidates[0]];
  for (let k = 1; k < candidates.length; k++) {
    if (gapMs(candidates[k - 1].iv, candidates[k].iv) <= 5 * MIN_MS) {
      cur.push(candidates[k]);
    } else {
      groups.push(cur); cur = [candidates[k]];
    }
  }
  groups.push(cur);

  for (const grp of groups) {
    if (spanMs(grp.map(c => c.iv)) >= 20 * MIN_MS) {
      for (const { i } of grp) {
        intervals[i] = { ...intervals[i], classification: 'DISHWASHER', correctedBy: 'rule1', correctionRule: 'sustained-low-flow-pattern' };
        count++;
      }
    }
  }
  return count;
}

// Rule 2: event-log match → reclassify nearest cluster
function applyRule2(intervals, eventLog, targetDate) {
  let count = 0;
  const dishEvents = (eventLog ?? []).filter(e =>
    String(e.appliance ?? '').toLowerCase().includes('dishwasher')
  );

  for (const evt of dishEvents) {
    const evtTime = evt.startTime ? new Date(evt.startTime) : null;
    if (!evtTime || isNaN(evtTime)) continue;
    if (evtTime.toISOString().slice(0, 10) !== targetDate) continue;

    const WINDOW = 15 * MIN_MS;
    for (let i = 0; i < intervals.length; i++) {
      const iv = intervals[i];
      if (iv.time && Math.abs(iv.time - evtTime) <= WINDOW) {
        intervals[i] = { ...iv, classification: 'DISHWASHER', correctedBy: 'rule2', correctionRule: 'event-log-match', confidence: 'high' };
        count++;
      }
    }
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

function confidenceFor(iv) {
  if (iv.confidence) return iv.confidence;
  if (iv.correctedBy === 'rule2') return 'high';
  if (iv.correctedBy === 'rule1') return 'medium';
  if (iv.correctedBy) return 'medium';
  return 'high';
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

    // Normalize and sort
    const intervals = extractIntervals(stored);
    intervals.sort((a, b) => (a.time ?? 0) - (b.time ?? 0));

    if (!intervals.length) {
      return res.status(422).json({ error: 'Interval data present but could not be parsed', raw: stored });
    }

    // Apply corrections (mutates intervals)
    let corrections = 0;
    corrections += applyRule1(intervals);
    corrections += applyRule2(intervals, eventLog, date);
    applyRule3(intervals, profile);   // split only — doesn't add to count
    corrections += applyRule4(intervals);

    // Build events list — skip zero-volume intervals
    const events = intervals
      .filter(iv => iv.volume > 0)
      .map(iv => {
        const entry = {
          time:                 formatTime(iv.time),
          gallons:              iv.volume,
          classification:       clsToOutput(iv.classification),
          metronClassification: iv.metronRaw,
          corrected:            !!iv.correctedBy,
          confidence:           confidenceFor(iv),
        };
        if (iv.correctionRule) entry.correctionRule = iv.correctionRule;
        return entry;
      });

    const body = {
      date,
      timezone: 'local-as-utc',
      events,
      summary:     buildSummary(intervals),
      corrections,
    };

    // Attach raw sample in non-prod so we can see Metron's field names
    if (process.env.NODE_ENV !== 'production') {
      const raw = Array.isArray(stored) ? stored : Object.values(stored)[0];
      body._meta = { rawSample: Array.isArray(raw) ? raw.slice(0, 3) : raw, totalIntervals: intervals.length };
    }

    res.setHeader('Cache-Control', 's-maxage=300, stale-while-revalidate');
    return res.status(200).json(body);
  } catch (err) {
    console.error('daily-timeline error:', err);
    return res.status(500).json({ error: err.message });
  }
};
