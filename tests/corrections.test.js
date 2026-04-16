'use strict';

const {
  applyDishwasherWindowScan,
  inDishwasherWindow,
  applyToiletSplit,
  applyShowerReclassification,
  applyBidetDetection,
  applyWashingMachineReclassification,
  applyBathDetection,
  buildCorrectedFixtures,
} = require('../scripts/corrections');

// ── helpers ──────────────────────────────────────────────────────────────────

// Build a single interval. Times are UTC hours/minutes (Metron local-as-UTC).
function iv(hourUTC, minUTC, volume, classification) {
  return {
    time:           new Date(`2026-04-14T${String(hourUTC).padStart(2,'0')}:${String(minUTC).padStart(2,'0')}:00Z`),
    volume,
    classification,
  };
}

// Deep-clone an array of intervals so each test starts with a fresh copy.
function clone(arr) {
  return arr.map(item => ({ ...item }));
}

// ── mock fixtures ─────────────────────────────────────────────────────────────

// 9 consecutive shower intervals, 6:18–6:26 AM, ~1.2G each
// Used to verify fixture summary; no rule modifies clean SHOWER intervals.
const mockShower = Array.from({ length: 9 }, (_, i) => iv(6, 18 + i, 1.2, 'SHOWER'));

// 12 SHOWER intervals, 6:12–6:23 PM (~18:12–18:23 UTC), 2.4G each
// Total: 28.8G, span: 11 min, avg flow: ~2.62 GPM → triggers Rule 6 → BATH
const mockBath = Array.from({ length: 12 }, (_, i) => iv(18, 12 + i, 2.4, 'SHOWER'));

// 3 sustained high-flow TOILET_FULL intervals (simulating Metron shower misclassification)
// 6:00, 6:02, 6:04 AM → span 4 min > 3 min, all > 1.0G → Rule 4 → SHOWER
const mockSustainedToilet = [
  iv(6, 0, 1.5, 'TOILET_FULL'),
  iv(6, 2, 1.5, 'TOILET_FULL'),
  iv(6, 4, 1.5, 'TOILET_FULL'),
];

// Bidet scenario: small OTHER flanking a TOILET_FULL flush
// Pre-flush at 7:08 (2 min before), flush at 7:10, post-flush at 7:12 (2 min after)
const mockBidetIntervals = [
  iv(7,  8, 0.12, 'OTHER'),       // pre-flush wash (should → BIDET_WASH)
  iv(7, 10, 1.5,  'TOILET_FULL'), // flush
  iv(7, 12, 0.15, 'OTHER'),       // post-flush refill (should → BIDET_REFILL)
];

// 11 consecutive WASHING_MACHINE intervals, 6:31–6:41 AM, 1.24G each
// Mirrors the Apr 15 real-world trigger: 13.64G, 10 min, 1.364 GPM → Rule 7 → SHOWER
const mockWashingMachineAsShower = Array.from({ length: 11 }, (_, i) =>
  iv(6, 31 + i, 1.24, 'WASHING_MACHINE')
);

// Burst/pause WashingMachine: two 3-interval clusters separated by 10-min gap
// Each cluster spans 2 min < 8 min → NOT a shower candidate → stays WASHING_MACHINE
const mockBurstPauseWM = [
  iv(8,  0, 1.5, 'WASHING_MACHINE'),
  iv(8,  1, 1.5, 'WASHING_MACHINE'),
  iv(8,  2, 1.5, 'WASHING_MACHINE'),
  // 10-min gap
  iv(8, 12, 1.5, 'WASHING_MACHINE'),
  iv(8, 13, 1.5, 'WASHING_MACHINE'),
  iv(8, 14, 1.5, 'WASHING_MACHINE'),
];

// 72 OTHER intervals, 18:00–19:11 (one per minute), 0.08G each
// Total: 5.76G, span: 71 min → within 6pm–6am window → Rule 2 → DISHWASHER
// Note: must compute hour/minute properly to avoid invalid Date (minutes 60+).
const mockDishwasher = Array.from({ length: 72 }, (_, i) => {
  const h = 18 + Math.floor(i / 60);
  const m = i % 60;
  return iv(h, m, 0.08, 'OTHER');
});

// 4 OTHER intervals, 7:19–7:22 AM, 0.08G each
// Total: 0.32G; 7 AM is outside the 18:00–06:00 window → no reclassification
const mockFalsePositiveDishwasher = [
  iv(7, 19, 0.08, 'OTHER'),
  iv(7, 20, 0.08, 'OTHER'),
  iv(7, 21, 0.08, 'OTHER'),
  iv(7, 22, 0.08, 'OTHER'),
];

// Profile with confirmed dishwasher (Bosch, overnight/evening run)
const dishwasherProfile = {
  dishwasher: {
    confirmed: true,
    brand: 'bosch',
    model: 'SHXM98W5N10',
    runWindow: null, // use default 18:00–06:00
  },
};

// ── Rule 3: Dual flush split ──────────────────────────────────────────────────

describe('Rule 3: Dual flush split', () => {
  test('half flush < 0.9G classified as TOILET_HALF', () => {
    const intervals = [iv(7, 0, 0.8, 'TOILET')];
    applyToiletSplit(intervals);
    expect(intervals[0].classification).toBe('TOILET_HALF');
    expect(intervals[0].correctedBy).toBe('rule3');
  });

  test('full flush >= 0.9G classified as TOILET_FULL', () => {
    const intervals = [iv(7, 5, 1.2, 'TOILET')];
    applyToiletSplit(intervals);
    expect(intervals[0].classification).toBe('TOILET_FULL');
    expect(intervals[0].correctedBy).toBe('rule3');
  });

  test('threshold boundary: 0.9G exactly → TOILET_FULL', () => {
    const intervals = [iv(7, 10, 0.9, 'TOILET')];
    applyToiletSplit(intervals);
    expect(intervals[0].classification).toBe('TOILET_FULL');
  });

  test('non-TOILET intervals not modified', () => {
    const intervals = [iv(7, 0, 1.2, 'SHOWER'), iv(7, 1, 0.5, 'SINK')];
    applyToiletSplit(intervals);
    expect(intervals[0].classification).toBe('SHOWER');
    expect(intervals[1].classification).toBe('SINK');
  });

  test('large toilet interval (>3.5G) classified as TOILET_FULL (no upper sanity bound)', () => {
    const intervals = [iv(7, 0, 3.5, 'TOILET')];
    applyToiletSplit(intervals);
    // Current behavior: split at 0.9G with no upper limit check
    expect(intervals[0].classification).toBe('TOILET_FULL');
  });
});

// ── Rule 4: Sustained toilet → shower ────────────────────────────────────────

describe('Rule 4: Sustained toilet → shower', () => {
  test('3 consecutive TOILET_FULL > 1.0G spanning > 3 min → SHOWER', () => {
    const intervals = clone(mockSustainedToilet);
    applyShowerReclassification(intervals);
    expect(intervals.every(iv => iv.classification === 'SHOWER')).toBe(true);
    expect(intervals[0].correctedBy).toBe('rule4');
  });

  test('2 consecutive TOILET_FULL intervals not reclassified', () => {
    const intervals = [
      iv(6, 0, 1.5, 'TOILET_FULL'),
      iv(6, 2, 1.5, 'TOILET_FULL'),
    ];
    applyShowerReclassification(intervals);
    expect(intervals.every(iv => iv.classification === 'TOILET_FULL')).toBe(true);
  });

  test('3 consecutive but low volume (< 1.0G) not reclassified', () => {
    const intervals = [
      iv(6, 0, 0.8, 'TOILET_FULL'),
      iv(6, 2, 0.8, 'TOILET_FULL'),
      iv(6, 4, 0.8, 'TOILET_FULL'),
    ];
    applyShowerReclassification(intervals);
    expect(intervals.every(iv => iv.classification === 'TOILET_FULL')).toBe(true);
  });

  test('3 high-volume TOILET_FULL within 3 min span not reclassified', () => {
    // Span = 2 min, not > 3 min — should stay as TOILET_FULL
    const intervals = [
      iv(6, 0, 1.5, 'TOILET_FULL'),
      iv(6, 1, 1.5, 'TOILET_FULL'),
      iv(6, 2, 1.5, 'TOILET_FULL'),
    ];
    applyShowerReclassification(intervals);
    expect(intervals.every(iv => iv.classification === 'TOILET_FULL')).toBe(true);
  });
});

// ── Rule 5: Bidet detection ───────────────────────────────────────────────────

describe('Rule 5: Bidet detection', () => {
  test('small OTHER before TOILET_FULL → BIDET_WASH', () => {
    const intervals = clone(mockBidetIntervals);
    applyBidetDetection(intervals, { bidetSeat: true });
    expect(intervals[0].classification).toBe('BIDET_WASH');
    expect(intervals[0].correctedBy).toBe('rule5');
  });

  test('small OTHER after TOILET_FULL → BIDET_REFILL', () => {
    const intervals = clone(mockBidetIntervals);
    applyBidetDetection(intervals, { bidetSeat: true });
    expect(intervals[2].classification).toBe('BIDET_REFILL');
    expect(intervals[2].correctedBy).toBe('rule5');
  });

  test('TOILET_FULL itself not reclassified', () => {
    const intervals = clone(mockBidetIntervals);
    applyBidetDetection(intervals, { bidetSeat: true });
    expect(intervals[1].classification).toBe('TOILET_FULL');
  });

  test('bidet not detected without bidetSeat in profile', () => {
    const intervals = clone(mockBidetIntervals);
    applyBidetDetection(intervals, {});
    expect(intervals[0].classification).toBe('OTHER');
    expect(intervals[2].classification).toBe('OTHER');
  });

  test('bidet not detected with null profile', () => {
    const intervals = clone(mockBidetIntervals);
    applyBidetDetection(intervals, null);
    expect(intervals[0].classification).toBe('OTHER');
  });

  test('OTHER interval too large (>0.20G) not reclassified as BIDET_WASH', () => {
    const intervals = [
      iv(7, 8, 0.5, 'OTHER'),        // too large for bidet wash
      iv(7, 10, 1.5, 'TOILET_FULL'),
    ];
    applyBidetDetection(intervals, { bidetSeat: true });
    expect(intervals[0].classification).toBe('OTHER');
  });
});

// ── Rule 6: Bath detection ────────────────────────────────────────────────────

describe('Rule 6: Bath detection', () => {
  test('28.8G over 11 min at ~2.6 GPM → BATH', () => {
    const intervals = clone(mockBath);
    applyBathDetection(intervals);
    expect(intervals.every(iv => iv.classification === 'BATH')).toBe(true);
    expect(intervals[0].correctedBy).toBe('rule6');
  });

  test('total < 20G → not bath', () => {
    // 5 intervals, 0–4 min, 1.5G each = 7.5G — under the 20G threshold
    const intervals = Array.from({ length: 5 }, (_, i) => iv(18, i, 1.5, 'SHOWER'));
    applyBathDetection(intervals);
    expect(intervals.every(iv => iv.classification === 'SHOWER')).toBe(true);
  });

  test('span < 10 min → not bath', () => {
    // 8 intervals, 0–7 min, 3.0G each = 24G but span only 7 min < 10 min
    const intervals = Array.from({ length: 8 }, (_, i) => iv(18, i, 3.0, 'SHOWER'));
    applyBathDetection(intervals);
    expect(intervals.every(iv => iv.classification === 'SHOWER')).toBe(true);
  });

  test('bath not detected if BIDET_WASH flanks the start within 3 min', () => {
    // Bidet wash 3 min before bath start at 18:12 (at 18:09)
    const intervals = [
      iv(18,  9, 0.12, 'BIDET_WASH'),
      ...Array.from({ length: 12 }, (_, i) => iv(18, 12 + i, 2.4, 'SHOWER')),
    ];
    applyBathDetection(intervals);
    // SHOWER intervals should stay as SHOWER (bidet guard blocked the bath detection)
    const showIntervals = intervals.filter(iv => iv.classification === 'SHOWER');
    expect(showIntervals.length).toBe(12);
  });

  test('bath not blocked by BIDET_WASH more than 3 min from bath boundaries', () => {
    // Bidet wash 10 min before bath — too far away to trigger guard
    const intervals = [
      iv(18,  0, 0.12, 'BIDET_WASH'),
      ...Array.from({ length: 12 }, (_, i) => iv(18, 12 + i, 2.4, 'SHOWER')),
    ];
    applyBathDetection(intervals);
    const bathIntervals = intervals.filter(iv => iv.classification === 'BATH');
    expect(bathIntervals.length).toBe(12);
  });
});

// ── Rule 7: WashingMachine → Shower ──────────────────────────────────────────

describe('Rule 7: WashingMachine → Shower', () => {
  test('11 min continuous WashingMachine at 1.36 GPM → SHOWER', () => {
    const intervals = clone(mockWashingMachineAsShower);
    const count = applyWashingMachineReclassification(intervals);
    expect(count).toBe(11);
    expect(intervals.every(iv => iv.classification === 'SHOWER')).toBe(true);
    expect(intervals[0].correctedBy).toBe('rule7');
  });

  test('burst/pause WashingMachine pattern → stays WashingMachine', () => {
    const intervals = clone(mockBurstPauseWM);
    const count = applyWashingMachineReclassification(intervals);
    expect(count).toBe(0);
    expect(intervals.every(iv => iv.classification === 'WASHING_MACHINE')).toBe(true);
  });

  test('WashingMachine outside 5am–10pm window (e.g. 2am) → stays WashingMachine', () => {
    // 11 consecutive intervals at 2am — outside the 5–22 UTC hour gate
    const intervals = Array.from({ length: 11 }, (_, i) => iv(2, i, 1.24, 'WASHING_MACHINE'));
    const count = applyWashingMachineReclassification(intervals);
    expect(count).toBe(0);
    expect(intervals.every(iv => iv.classification === 'WASHING_MACHINE')).toBe(true);
  });

  test('continuous WashingMachine > 30G → stays WashingMachine (too large for shower)', () => {
    // 20 intervals, 2G each = 40G — over the 30G shower ceiling
    const intervals = Array.from({ length: 20 }, (_, i) => iv(8, i, 2.0, 'WASHING_MACHINE'));
    const count = applyWashingMachineReclassification(intervals);
    expect(count).toBe(0);
  });
});

// ── Dishwasher detection ──────────────────────────────────────────────────────

describe('Dishwasher detection (Rule 2)', () => {
  test('no detection without dishwasher.confirmed in profile', () => {
    const intervals = clone(mockDishwasher);
    applyDishwasherWindowScan(intervals, {});
    expect(intervals.every(iv => iv.classification === 'OTHER')).toBe(true);
  });

  test('no detection with null profile', () => {
    const intervals = clone(mockDishwasher);
    applyDishwasherWindowScan(intervals, null);
    expect(intervals.every(iv => iv.classification === 'OTHER')).toBe(true);
  });

  test('0.32G cluster at 7am not classified as dishwasher (outside window + too small)', () => {
    const intervals = clone(mockFalsePositiveDishwasher);
    applyDishwasherWindowScan(intervals, dishwasherProfile);
    expect(intervals.every(iv => iv.classification === 'OTHER')).toBe(true);
  });

  test('5.76G cluster in 6pm–7:11pm window → DISHWASHER', () => {
    const intervals = clone(mockDishwasher);
    const count = applyDishwasherWindowScan(intervals, dishwasherProfile);
    expect(count).toBeGreaterThan(0);
    expect(intervals.every(iv => iv.classification === 'DISHWASHER')).toBe(true);
    expect(intervals[0].correctedBy).toBe('rule2');
  });

  test('cluster under 2G sanity floor stays OTHER even in window', () => {
    // 20 intervals at 18:00–18:19, 0.05G each = 1.0G (under 2.0G floor)
    const intervals = Array.from({ length: 20 }, (_, i) => iv(18, i, 0.05, 'OTHER'));
    applyDishwasherWindowScan(intervals, dishwasherProfile);
    expect(intervals.every(iv => iv.classification === 'OTHER')).toBe(true);
  });

  test('cluster under 45 min span stays OTHER', () => {
    // 30 intervals at 18:00–18:29, 0.15G each = 4.5G but span only 29 min
    const intervals = Array.from({ length: 30 }, (_, i) => iv(18, i, 0.15, 'OTHER'));
    applyDishwasherWindowScan(intervals, dishwasherProfile);
    expect(intervals.every(iv => iv.classification === 'OTHER')).toBe(true);
  });

  test('inDishwasherWindow: 6pm is inside default window', () => {
    expect(inDishwasherWindow(new Date('2026-04-14T18:00:00Z'), null)).toBe(true);
  });

  test('inDishwasherWindow: midnight is inside default window', () => {
    expect(inDishwasherWindow(new Date('2026-04-14T00:00:00Z'), null)).toBe(true);
  });

  test('inDishwasherWindow: 5am is inside default window', () => {
    expect(inDishwasherWindow(new Date('2026-04-14T05:59:00Z'), null)).toBe(true);
  });

  test('inDishwasherWindow: 7am is outside default window', () => {
    expect(inDishwasherWindow(new Date('2026-04-14T07:00:00Z'), null)).toBe(false);
  });

  test('inDishwasherWindow: custom window respected', () => {
    const customWindow = { start: '20:00', end: '04:00' };
    expect(inDishwasherWindow(new Date('2026-04-14T21:00:00Z'), customWindow)).toBe(true);
    expect(inDishwasherWindow(new Date('2026-04-14T18:00:00Z'), customWindow)).toBe(false);
  });
});

// ── Fixture summary ───────────────────────────────────────────────────────────

describe('Fixture summary (buildCorrectedFixtures)', () => {
  test('shower total matches sum of SHOWER intervals', () => {
    const cf = buildCorrectedFixtures(clone(mockShower));
    const expected = Math.round(9 * 1.2 * 10) / 10;
    expect(cf.shower).toBe(expected);
  });

  test('shower is 0 when no shower intervals present', () => {
    const cf = buildCorrectedFixtures([iv(7, 0, 1.2, 'TOILET_FULL')]);
    expect(cf.shower).toBe(0);
  });

  test('all fixture categories sum to total interval volume', () => {
    const intervals = [
      iv(6, 0, 5.0, 'SHOWER'),
      iv(7, 0, 1.0, 'TOILET_FULL'),
      iv(7, 30, 0.5, 'SINK'),
      iv(18, 0, 3.0, 'DISHWASHER'),
    ];
    const cf = buildCorrectedFixtures(intervals);
    const total = cf.shower + cf.toilet.total + cf.sink + cf.dishwasher +
                  cf.washingMachine + cf.bath + cf.bidet.total + cf.other;
    expect(Math.round(total * 10) / 10).toBe(9.5);
  });

  test('bath appears in summary after Rule 6 fires', () => {
    const intervals = clone(mockBath);
    applyBathDetection(intervals);
    const cf = buildCorrectedFixtures(intervals);
    expect(cf.bath).toBeGreaterThan(20);
    expect(cf.shower).toBe(0); // all shower intervals reclassified to bath
  });

  test('toilet.halfFlush and toilet.fullFlush are volume sums (not counts)', () => {
    // buildCorrectedFixtures sums volumes, not counts.
    // Use daily-timeline.js buildSummary for count-based reporting.
    const intervals = [
      iv(7, 0, 0.8, 'TOILET_HALF'),
      iv(7, 5, 0.8, 'TOILET_HALF'),
      iv(7, 10, 1.2, 'TOILET_FULL'),
    ];
    const cf = buildCorrectedFixtures(intervals);
    expect(cf.toilet.halfFlush).toBe(1.6); // sum of TOILET_HALF volumes
    expect(cf.toilet.fullFlush).toBe(1.2); // sum of TOILET_FULL volumes
    expect(cf.toilet.total).toBe(Math.round((0.8 + 0.8 + 1.2) * 10) / 10);
  });

  test('bidet sub-types appear in summary (rounded to 1 decimal)', () => {
    // buildCorrectedFixtures rounds to 1 decimal: 0.12 → 0.1, 0.15 → 0.2, 0.08 → 0.1
    const intervals = [
      iv(7, 8,  0.12, 'BIDET_WASH'),
      iv(7, 12, 0.15, 'BIDET_REFILL'),
      iv(2, 30, 0.08, 'BIDET_SELFCLEAN'),
    ];
    const cf = buildCorrectedFixtures(intervals);
    expect(cf.bidet.wash).toBe(0.1);      // 0.12 rounded
    expect(cf.bidet.refill).toBe(0.2);    // 0.15 rounded
    expect(cf.bidet.selfClean).toBe(0.1); // 0.08 rounded
    expect(cf.bidet.total).toBe(0.4);     // 0.35 rounded
  });

  test('OTHER and LEAK both land in other bucket', () => {
    const intervals = [
      iv(3, 0, 2.0, 'OTHER'),
      iv(3, 30, 1.0, 'LEAK'),
    ];
    const cf = buildCorrectedFixtures(intervals);
    expect(cf.other).toBe(3.0);
  });

  test('washingMachine total correct after Rule 7 reclassifies some intervals', () => {
    // burstPause stays as WASHING_MACHINE (Rule 7 doesn't touch it)
    const intervals = clone(mockBurstPauseWM);
    applyWashingMachineReclassification(intervals);
    const cf = buildCorrectedFixtures(intervals);
    const expectedTotal = Math.round(6 * 1.5 * 10) / 10;
    expect(cf.washingMachine).toBe(expectedTotal);
    expect(cf.shower).toBe(0);
  });
});
