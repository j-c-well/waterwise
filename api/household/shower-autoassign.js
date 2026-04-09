'use strict';

const Redis = require('ioredis');

const redis = new Redis(process.env.REDIS_URL);

const CORS = (res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
};

const MIN_MS = 60_000;

// ── time helpers ──────────────────────────────────────────────────────────────

// "HH:MM" → minutes since midnight
function parseWindowMin(str) {
  if (!str) return null;
  const [h, m] = str.split(':').map(Number);
  return isNaN(h) || isNaN(m) ? null : h * 60 + m;
}

// ISO string → UTC hours*60+min (Metron stores local time as UTC)
function isoToUtcMin(iso) {
  const d = new Date(iso);
  return isNaN(d) ? null : d.getUTCHours() * 60 + d.getUTCMinutes();
}

// ISO string → "6:18 AM" display (UTC hours treated as local)
function formatUtcTime(iso) {
  const d = new Date(iso);
  if (isNaN(d)) return null;
  let h = d.getUTCHours();
  const m = d.getUTCMinutes();
  const ampm = h >= 12 ? 'PM' : 'AM';
  h = h % 12 || 12;
  return `${h}:${String(m).padStart(2, '0')} ${ampm}`;
}

// ── day-of-week matching ──────────────────────────────────────────────────────

const DOW_MAP = {
  sunday: 0, monday: 1, tuesday: 2, wednesday: 3,
  thursday: 4, friday: 5, saturday: 6,
};
const WEEKDAY_SET = new Set([1, 2, 3, 4, 5]);
const WEEKEND_SET = new Set([0, 6]);
const DOW_NAMES   = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

function matchesDay(typicalDays, date) {
  if (!typicalDays || typicalDays.includes('any')) return { match: true, dayName: null };
  const dow = new Date(date + 'T12:00:00Z').getUTCDay();
  for (const d of typicalDays) {
    if (d === 'weekday' && WEEKDAY_SET.has(dow)) return { match: true, dayName: DOW_NAMES[dow] };
    if (d === 'weekend' && WEEKEND_SET.has(dow)) return { match: true, dayName: DOW_NAMES[dow] };
    if (DOW_MAP[d?.toLowerCase()] === dow)       return { match: true, dayName: DOW_NAMES[dow] };
  }
  return { match: false, dayName: DOW_NAMES[dow] };
}

// ── member scoring ────────────────────────────────────────────────────────────

// Returns { score (0–3.5), confidence, reason } for one member + shower time
function scoreMatch(member, showerMin, date) {
  const pat = member.showerPattern;
  if (!pat) return { score: 0, confidence: 'none', reason: 'no shower pattern defined' };

  const windows    = pat.timeWindows ?? [];
  const isSporadic = (pat.typicalTimes ?? []).includes('sporadic');
  const { match: dayMatch, dayName } = matchesDay(pat.typicalDays, date);

  // Find best time-window match
  let windowMatch   = false;
  let matchedWindow = null;
  for (const win of windows) {
    const start = parseWindowMin(win.start);
    const end   = parseWindowMin(win.end);
    if (start === null || end === null) continue;
    if (showerMin >= start && showerMin <= end) {
      windowMatch   = true;
      matchedWindow = win;
      break;
    }
  }

  // Score: window (+2), day (+1), sporadic bonus (+0.5)
  let score = 0;
  if (windowMatch) score += 2;
  if (dayMatch)    score += 1;
  if (isSporadic)  score += 0.5;

  let confidence;
  if      (windowMatch && dayMatch)                confidence = 'high';
  else if (windowMatch && !dayMatch)               confidence = 'medium';
  else if (!windowMatch && dayMatch && isSporadic)  confidence = 'low';
  else                                              confidence = 'none';

  let reason;
  const typicalLabel = pat.typicalTimes?.[0] ?? 'pattern';
  if (windowMatch && dayMatch) {
    reason = `matches ${typicalLabel} window ${matchedWindow.start}–${matchedWindow.end}`;
  } else if (windowMatch) {
    reason = `matches time window ${matchedWindow.start}–${matchedWindow.end} but not typical days` +
             (dayName ? ` (${dayName} not in ${pat.typicalDays?.join(', ')})` : '');
  } else if (isSporadic) {
    reason = `sporadic pattern — no time-window match`;
  } else {
    reason = `no matching time window`;
  }

  return { score, confidence, reason };
}

// ── handler ───────────────────────────────────────────────────────────────────

module.exports = async function handler(req, res) {
  CORS(res);
  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') return res.status(405).json({ error: 'Method not allowed' });

  const body = req.body ?? {};
  let { date } = body;
  if (!date || !/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    date = d.toISOString().slice(0, 10);
  }

  try {
    const [correctedRaw, profileRaw, showerLogRaw] = await Promise.all([
      redis.get(`waterwise:corrected:${date}`),
      redis.get('waterwise:household:owner'),
      redis.get('waterwise:shower-log:owner'),
    ]);

    if (!correctedRaw) {
      return res.status(404).json({ error: `No corrected data for ${date}` });
    }

    const corrected = JSON.parse(correctedRaw);
    const profile   = profileRaw   ? JSON.parse(profileRaw)   : null;
    const showerLog = showerLogRaw ? JSON.parse(showerLogRaw) : [];
    const members   = profile?.members ?? [];

    if (!members.length) {
      return res.status(400).json({ error: 'No household members in profile' });
    }

    // Extract SHOWER intervals from corrected data
    const showerIvs = (corrected.intervals ?? []).filter(iv => iv.classification === 'SHOWER');
    if (!showerIvs.length) {
      return res.status(200).json({ date, suggestions: [], message: 'No shower events found for this date' });
    }

    // Group consecutive SHOWER intervals → one event per shower (≤ 3 min gap)
    const sorted = [...showerIvs].sort((a, b) => new Date(a.time) - new Date(b.time));
    const eventGroups = [];
    let cur = [sorted[0]];
    for (let i = 1; i < sorted.length; i++) {
      const gap = new Date(sorted[i].time) - new Date(sorted[i - 1].time);
      if (gap <= 3 * MIN_MS) { cur.push(sorted[i]); }
      else { eventGroups.push(cur); cur = [sorted[i]]; }
    }
    eventGroups.push(cur);

    // Showers already confirmed in the log for this date
    const assignedTimes = new Set(
      showerLog.filter(e => e.date === date).map(e => e.showerTime)
    );

    const suggestions = [];

    for (const grp of eventGroups) {
      const showerTimeStr = formatUtcTime(grp[0].time);
      const showerMin     = isoToUtcMin(grp[0].time);
      if (showerMin === null) continue;

      // Skip already-assigned showers
      if (assignedTimes.has(showerTimeStr)) continue;

      const gallons  = Math.round(grp.reduce((s, iv) => s + iv.volume, 0) * 100) / 100;
      const duration = grp.length > 1
        ? Math.round((new Date(grp[grp.length - 1].time) - new Date(grp[0].time)) / MIN_MS)
        : 0;

      // Score every member and pick the best
      const scored = members
        .map(m => ({ member: m, ...scoreMatch(m, showerMin, date) }))
        .sort((a, b) => b.score - a.score || a.member.id.localeCompare(b.member.id));

      const best     = scored[0];
      const runnerUp = scored[1];
      // Ambiguous if top two share same score
      const ambiguous = runnerUp && scored[0].score === runnerUp.score;

      const suggestion = {
        showerTime:      showerTimeStr,
        gallons,
        duration,
        suggestedMember: best.member.id,
        suggestedName:   best.member.name,
        avatar:          best.member.avatar ?? null,
        confidence:      ambiguous ? 'low' : best.confidence,
        reason:          best.reason,
      };

      if (ambiguous || (runnerUp && best.confidence !== 'high')) {
        suggestion.altSuggestion = {
          memberId: runnerUp.member.id,
          name:     runnerUp.member.name,
          avatar:   runnerUp.member.avatar ?? null,
          reason:   runnerUp.reason,
        };
      }

      suggestions.push(suggestion);
    }

    return res.status(200).json({ date, suggestions });
  } catch (err) {
    console.error('shower-autoassign error:', err);
    return res.status(500).json({ error: err.message });
  }
};
