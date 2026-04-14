'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

const NINETY_DAYS_MS = 90 * 24 * 3600 * 1000;

function ninetyDaysAgo() {
  return new Date(Date.now() - NINETY_DAYS_MS).toISOString().slice(0, 10);
}

function computeLeaderboard(log, profile) {
  const members = profile?.members ?? [];

  const stats = {};
  for (const entry of log) {
    const { memberId, gallons, duration } = entry;
    if (!memberId) continue;
    if (!stats[memberId]) stats[memberId] = { total: 0, gallons: 0, minutes: 0, entries: [] };
    stats[memberId].total++;
    stats[memberId].gallons  += gallons  ?? 0;
    stats[memberId].minutes  += duration ?? 0;
    stats[memberId].entries.push(entry);
  }

  const sevenDaysAgo    = new Date(Date.now() -  7 * 24 * 3600 * 1000).toISOString().slice(0, 10);
  const fourteenDaysAgo = new Date(Date.now() - 14 * 24 * 3600 * 1000).toISOString().slice(0, 10);

  const leaderboard = Object.entries(stats).map(([memberId, s]) => {
    const member = members.find(m => m.id === memberId);

    const recent = s.entries.filter(e => e.date >= sevenDaysAgo);
    const prior  = s.entries.filter(e => e.date >= fourteenDaysAgo && e.date < sevenDaysAgo);

    const avgRecent = recent.length ? recent.reduce((sum, e) => sum + (e.gallons ?? 0), 0) / recent.length : null;
    const avgPrior  = prior.length  ? prior.reduce((sum,  e) => sum + (e.gallons ?? 0), 0) / prior.length  : null;

    let trend = 'stable';
    if (avgRecent !== null && avgPrior !== null) {
      if      (avgRecent < avgPrior * 0.95) trend = 'improving';
      else if (avgRecent > avgPrior * 1.05) trend = 'worsening';
    }

    const r = (v) => Math.round(v * 10) / 10;
    return {
      memberId,
      name:         member?.name   ?? memberId,
      avatar:       member?.avatar ?? null,
      totalShowers: s.total,
      avgGallons:   r(s.gallons  / s.total),
      avgMinutes:   r(s.minutes  / s.total),
      totalGallons: r(s.gallons),
      trend,
    };
  });

  // Rank by avgGallons ascending (least water = best)
  leaderboard.sort((a, b) => a.avgGallons - b.avgGallons);
  leaderboard.forEach((entry, i) => { entry.rank = i + 1; });

  return leaderboard;
}

function computeCycleStats(log, profile) {
  const thirtyDaysAgo = new Date(Date.now() - 30 * 24 * 3600 * 1000).toISOString().slice(0, 10);
  const recent = log.filter(e => e.date >= thirtyDaysAgo);
  if (!recent.length) return null;

  const totalGallons = recent.reduce((sum, e) => sum + (e.gallons ?? 0), 0);
  const r = (v) => Math.round(v * 10) / 10;

  const shortest = recent.reduce((min, e) =>
    (e.gallons ?? Infinity) < (min?.gallons ?? Infinity) ? e : min
  , null);

  return {
    totalShowerGallons: r(totalGallons),
    avgShowerGallons:   r(totalGallons / recent.length),
    shortestShower: shortest ? {
      member:  shortest.memberName ?? shortest.memberId,
      gallons: shortest.gallons,
    } : null,
  };
}

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }

  const { userId } = req.method === 'GET' ? (req.query ?? {}) : (req.body ?? {});
  const KEY        = userId ? `waterwise:shower-log:${userId}` : 'waterwise:shower-log:owner';
  const profileKey = userId ? `waterwise:household:${userId}` : 'waterwise:household:owner';

  try {
    if (req.method === 'GET') {
      const [raw, profileRaw] = await Promise.all([
        redis.get(KEY),
        redis.get(profileKey),
      ]);
      const log     = raw        ? JSON.parse(raw)        : [];
      const profile = profileRaw ? JSON.parse(profileRaw) : null;

      const pruned = log.filter(e => e.date >= ninetyDaysAgo());

      const leaderboard = computeLeaderboard(pruned, profile);
      const cycleStats  = computeCycleStats(pruned, profile);

      res.setHeader('Cache-Control', 'no-store');
      return res.status(200).json({ log: pruned, leaderboard, cycleStats });
    }

    if (req.method === 'POST') {
      const body = req.body ?? {};
      const { date, showerTime, memberId, gallons, duration, confirmed } = body;

      if (!date || !showerTime || !memberId) {
        return res.status(400).json({ error: 'date, showerTime, and memberId are required' });
      }
      if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
        return res.status(400).json({ error: 'date must be YYYY-MM-DD' });
      }

      const profileRaw = await redis.get(profileKey);
      const profile    = profileRaw ? JSON.parse(profileRaw) : null;
      const member     = (profile?.members ?? []).find(m => m.id === memberId);

      const entry = {
        date,
        showerTime,
        memberId,
        memberName: member?.name   ?? memberId,
        avatar:     member?.avatar ?? null,
        gallons:    gallons   !== undefined ? gallons   : null,
        duration:   duration  !== undefined ? duration  : null,
        confirmed:  confirmed !== undefined ? confirmed : false,
        loggedAt:   new Date().toISOString(),
      };

      const raw    = await redis.get(KEY);
      const log    = raw ? JSON.parse(raw) : [];
      log.push(entry);
      const pruned = log.filter(e => e.date >= ninetyDaysAgo());

      await redis.set(KEY, JSON.stringify(pruned));
      return res.status(201).json(entry);
    }

    return res.status(405).json({ error: 'Method not allowed' });
  } catch (err) {
    console.error('shower-log error:', err);
    return res.status(500).json({ error: err.message });
  }
};
