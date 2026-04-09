'use strict';

/**
 * One-shot utility: append a user correction entry to waterwise:corrections:owner.
 * Run via: node scripts/record-correction.js
 * Requires REDIS_URL env var.
 */

const Redis = require('ioredis');

const ENTRY = {
  date:                  '2026-04-08',
  timeWindow:            '7:19-7:40',
  metronClassification:  'Other',
  appliedRule:           'sustained-low-flow-pattern',
  userCorrection:        'NOT_DISHWASHER',
  confidence:            'confirmed',
  notes:                 'user confirmed dishwasher did not run at this time; 0.93G cluster below 2.5G Bosch minimum',
  recordedAt:            new Date().toISOString(),
};

async function main() {
  if (!process.env.REDIS_URL) throw new Error('REDIS_URL not set');
  const redis = new Redis(process.env.REDIS_URL);
  try {
    const KEY = 'waterwise:corrections:owner';
    const raw  = await redis.get(KEY);
    const list = raw ? JSON.parse(raw) : [];

    // Avoid duplicates: skip if same date + timeWindow + appliedRule already recorded
    const isDup = list.some(e =>
      e.date === ENTRY.date &&
      e.timeWindow === ENTRY.timeWindow &&
      e.appliedRule === ENTRY.appliedRule
    );
    if (isDup) {
      console.log('Correction already recorded — skipping.');
      return;
    }

    list.push(ENTRY);
    await redis.set(KEY, JSON.stringify(list));
    console.log(`Saved correction to ${KEY}. Total entries: ${list.length}`);
    console.log(JSON.stringify(ENTRY, null, 2));
  } finally {
    await redis.quit();
  }
}

main().catch(err => {
  console.error('FAILED:', err.message);
  process.exit(1);
});
