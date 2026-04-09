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
    // ── 1. Store negative training correction ──
    const CORR_KEY = 'waterwise:corrections:owner';
    const raw      = await redis.get(CORR_KEY);
    const list     = raw ? JSON.parse(raw) : [];

    const isDup = list.some(e =>
      e.date === ENTRY.date &&
      e.timeWindow === ENTRY.timeWindow &&
      e.appliedRule === ENTRY.appliedRule
    );
    if (isDup) {
      console.log('Correction already recorded — skipping.');
    } else {
      list.push(ENTRY);
      await redis.set(CORR_KEY, JSON.stringify(list));
      console.log(`Saved correction to ${CORR_KEY}. Total entries: ${list.length}`);
    }

    // ── 2. Ensure bidetSeat is set in household profile ──
    const PROFILE_KEY = 'waterwise:household:owner';
    const profileRaw  = await redis.get(PROFILE_KEY);
    const profile     = profileRaw ? JSON.parse(profileRaw) : {};

    if (!profile.bidetSeat) {
      profile.bidetSeat = { brand: 'TOTO', type: 'heated-tank', confirmed: true };
      profile.updatedAt = new Date().toISOString();
      await redis.set(PROFILE_KEY, JSON.stringify(profile));
      console.log('Added bidetSeat to household profile:', JSON.stringify(profile.bidetSeat));
    } else {
      console.log('bidetSeat already in profile — skipping.');
    }
  } finally {
    await redis.quit();
  }
}

main().catch(err => {
  console.error('FAILED:', err.message);
  process.exit(1);
});
