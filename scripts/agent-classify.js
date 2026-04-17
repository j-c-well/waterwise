'use strict';

// ── Load .env.local for local dev (same pattern as email-digest.js) ──────────
if (!process.env.REDIS_URL) {
  try {
    const fs   = require('fs');
    const path = require('path');
    const envLines = fs.readFileSync(path.join(__dirname, '../.env.local'), 'utf8').split('\n');
    for (const line of envLines) {
      const m = line.match(/^([A-Z0-9_]+)=(.+)$/);
      if (m) process.env[m[1]] = m[2].replace(/^['"]|['"]$/g, '').trim();
    }
  } catch (_) {}
}

const Redis     = require('ioredis');
const Anthropic = require('@anthropic-ai/sdk');
const { extractIntervals } = require('./corrections');

const MIN_MS = 60_000;
const DAYS   = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];

// ── helpers ───────────────────────────────────────────────────────────────────

function parseMsDate(val) {
  if (!val) return null;
  const m = String(val).match(/\/Date\((-?\d+)\)\//);
  if (m) return new Date(parseInt(m[1], 10));
  const d = new Date(val);
  return isNaN(d) ? null : d;
}

function formatUTCTime(dt) {
  if (!dt) return null;
  const h = dt.getUTCHours();
  const m = dt.getUTCMinutes();
  const h12 = h % 12 || 12;
  const ampm = h >= 12 ? 'PM' : 'AM';
  return `${h12}:${String(m).padStart(2, '0')} ${ampm}`;
}

// Parse "6:18 AM" → minutes since midnight
function timeToMinutes(str) {
  if (!str) return null;
  const m = String(str).match(/(\d+):(\d+)\s*(AM|PM)/i);
  if (!m) return null;
  let h = parseInt(m[1]);
  const min = parseInt(m[2]);
  if (m[3].toUpperCase() === 'PM' && h !== 12) h += 12;
  if (m[3].toUpperCase() === 'AM' && h === 12) h = 0;
  return h * 60 + min;
}

// Map internal CLS names to display names for the prompt
const CLS_TO_DISPLAY = {
  SHOWER: 'Shower', TOILET: 'Toilet', TOILET_HALF: 'Toilet', TOILET_FULL: 'Toilet',
  BATH: 'Bath', DISHWASHER: 'Dishwasher', SINK: 'Sink', WASHING_MACHINE: 'WashingMachine',
  BIDET_WASH: 'Bidet', BIDET_REFILL: 'Bidet', BIDET_SELFCLEAN: 'Bidet',
  OTHER: 'Other', UNKNOWN: 'Other', LEAK: 'Leak', IRRIGATION: 'Irrigation',
};

// Normalize agent or corrections classification to a common lowercase key for comparison
function normalizeClass(cls) {
  if (!cls) return 'other';
  const c = String(cls).toLowerCase();
  if (c.includes('toilet')) return 'toilet';
  if (c.includes('shower')) return 'shower';
  if (c.includes('bath')) return 'bath';
  if (c.includes('dish')) return 'dishwasher';
  if (c.includes('sink') || c.includes('faucet')) return 'sink';
  if (c.includes('wash') || c.includes('laundry')) return 'washing_machine';
  if (c.includes('bidet')) return 'bidet';
  if (c.includes('irrigat')) return 'irrigation';
  if (c.includes('leak')) return 'leak';
  return 'other';
}

// ── Step 1: Group raw intervals into events ───────────────────────────────────

function groupIntervalsIntoEvents(intervals) {
  const events = [];
  let group    = null;

  for (const iv of intervals) {
    if (!iv.time || iv.volume <= 0.01) continue;

    const cls = CLS_TO_DISPLAY[iv.classification] ?? 'Other';

    if (
      group &&
      group.metronClassification === cls &&
      iv.time - group._lastTime <= 2 * MIN_MS
    ) {
      // Extend current group
      group.totalGallons += iv.volume;
      group._lastTime     = iv.time;
      group._lastDate     = iv.time;
      group.intervalCount++;
    } else {
      if (group) events.push(finalizeGroup(group));
      group = {
        _firstTime:           iv.time,
        _lastTime:            iv.time,
        _lastDate:            iv.time,
        totalGallons:         iv.volume,
        metronClassification: cls,
        intervalCount:        1,
      };
    }
  }
  if (group) events.push(finalizeGroup(group));
  return events;
}

function finalizeGroup(g) {
  const durationMin = g._lastTime && g._firstTime
    ? Math.round((g._lastTime - g._firstTime) / MIN_MS)
    : 0;
  const avgFlowGPM  = durationMin > 0
    ? Math.round((g.totalGallons / durationMin) * 100) / 100
    : 0;
  return {
    timeStart:            formatUTCTime(g._firstTime),
    timeEnd:              formatUTCTime(g._lastTime),
    durationMin,
    totalGallons:         Math.round(g.totalGallons * 100) / 100,
    avgFlowGPM,
    metronClassification: g.metronClassification,
    intervalCount:        g.intervalCount,
  };
}

// ── Step 2: Build prompt ──────────────────────────────────────────────────────

function buildPrompt({ date, dayOfWeek, events, profile, signatures, billingCtx, prevDayCtx, dishwasherSigCount, dishwasherSigTimes }) {
  const system = `You are a water use analyst for a residential smart meter monitoring system. You classify household water events based on flow data and household context. Always respond with valid JSON only — no explanation, no markdown, just JSON.`;

  // Household context block
  const profileLines = [];
  if (profile) {
    if (profile.members)     profileLines.push(`Household members: ${profile.members}`);
    if (profile.bidetSeat)   profileLines.push('Has bidet seat: yes');
    if (profile.dishwasher?.confirmed) {
      profileLines.push(`Dishwasher: ${profile.dishwasher.brand ?? ''} ${profile.dishwasher.model ?? ''} (confirmed)`.trim());
      if (profile.dishwasher.runWindow) {
        profileLines.push(`Dishwasher window: ${profile.dishwasher.runWindow.start}–${profile.dishwasher.runWindow.end}`);
      }
    }
    if (profile.showerProfiles) {
      const showerLines = Object.entries(profile.showerProfiles)
        .map(([id, s]) => `  ${id}: avg ${s.avgGallons}G / ${s.avgDuration}min at ${s.typicalTime}`)
        .join('\n');
      profileLines.push('Known showers:\n' + showerLines);
    }
  }

  // Recent signatures block
  const sigLines = signatures.length
    ? signatures.slice(0, 10).map(s =>
        `  ${s.category}: ~${s.totalGallons}G, ~${s.durationMin}min, ${s.avgFlowGPM?.toFixed(2) ?? '?'} GPM (${s.timeOfDay})`
      ).join('\n')
    : '  None yet';

  // Dishwasher signature context block
  const dishwasherLines = [];
  if (profile?.dishwasher?.confirmed) {
    dishwasherLines.push(`Confirmed dishwasher runs on file: ${dishwasherSigCount ?? 0}`);
    if (dishwasherSigTimes?.length) {
      dishwasherLines.push(`Recent dishwasher run times: ${dishwasherSigTimes.slice(0, 7).join(', ')}`);
    }
    if ((dishwasherSigCount ?? 0) < 5) {
      dishwasherLines.push(
        'IMPORTANT: Fewer than 5 confirmed dishwasher runs are on file for this household. ' +
        'Do NOT classify any event as "dishwasher" — instead surface as an anomaly with ' +
        'question "Possible dishwasher?" so the user can confirm. Set confidence to 0.0 for ' +
        'any dishwasher classification.'
      );
    }
  }

  // Billing context block
  const billingLines = billingCtx
    ? [`Billing cycle day: ${billingCtx.billingCycleDay}`,
       `Usage so far this cycle: ${billingCtx.soFarThisCycle}G`,
       `7-day avg: ${billingCtx.sevenDayAvg}G/day`,
       `Current tier: ${billingCtx.currentTier ?? 'unknown'}`]
    : [];

  const userPrompt = [
    `Date: ${date} (${dayOfWeek})`,
    '',
    '--- Household Profile ---',
    ...profileLines,
    '',
    '--- Confirmed Flow Signatures (recent) ---',
    sigLines,
    '',
    dishwasherLines.length ? '--- Dishwasher Context ---\n' + dishwasherLines.join('\n') : '',
    '',
    billingLines.length ? '--- Billing Context ---\n' + billingLines.join('\n') : '',
    '',
    prevDayCtx ? `--- Previous Day Summary ---\n${prevDayCtx}` : '',
    '',
    '--- Water Events to Classify ---',
    JSON.stringify(events, null, 2),
    '',
    'Classify each event. For ambiguous events under 0.5G or under 2 min, you may include in anomalies instead.',
    'For appliance classification, prefer surfacing as a question over auto-classifying. A wrong classification destroys user trust. An honest question builds it.',
    'Return JSON with this exact structure:',
    JSON.stringify({
      classifications: [{
        timeStart: 'string',
        timeEnd: 'string',
        gallons: 0,
        classification: 'shower|bath|toilet|sink|dishwasher|washing_machine|bidet|irrigation|leak|other',
        confidence: 0.0,
        reasoning: 'max 20 words',
        needsConfirmation: false,
        metronAgreement: true,
      }],
      anomalies: [{
        timeStart: 'string',
        gallons: 0,
        urgency: 'low|medium|high',
        question: 'string',
      }],
      insights: ['string'],
    }, null, 2),
  ].filter(l => l !== undefined).join('\n');

  return { system, userPrompt };
}

// ── Step 3.5: Post-process confidence thresholds ──────────────────────────────

function applyConfidenceThresholds(agentResult, dishwasherSigCount) {
  const classified = [];
  const anomalies  = [...(agentResult.anomalies ?? [])];

  for (const c of (agentResult.classifications ?? [])) {
    const conf = c.confidence ?? 0;

    // Dishwasher restriction: fewer than 5 confirmed sigs → anomaly only regardless of confidence
    if (normalizeClass(c.classification) === 'dishwasher' && (dishwasherSigCount ?? 0) < 5) {
      anomalies.push({
        timeStart: c.timeStart,
        gallons:   c.gallons,
        urgency:   'low',
        question:  `Possible dishwasher? (~${c.gallons}G${c.reasoning ? ', ' + c.reasoning : ''})`,
      });
      continue;
    }

    if (conf < 0.75) {
      // Below threshold — surface as anomaly instead of classifying
      anomalies.push({
        timeStart: c.timeStart,
        gallons:   c.gallons,
        urgency:   'low',
        question:  c.reasoning ? `Unclear: ${c.reasoning}` : `Unclear event — ${c.classification}?`,
      });
    } else {
      // 0.75–0.89 → needsConfirmation; >= 0.90 → auto-classify
      classified.push({
        ...c,
        needsConfirmation: conf < 0.90 ? true : (c.needsConfirmation ?? false),
      });
    }
  }

  return { ...agentResult, classifications: classified, anomalies };
}

// ── Step 5: Compare with corrections output ───────────────────────────────────

function groupCorrectionsIntervals(intervals) {
  const events = [];
  let group    = null;

  for (const iv of intervals) {
    if (!iv.time) continue;
    const t   = new Date(iv.time);
    const cls = normalizeClass(iv.classification);

    if (group && group._cls === cls && t - group._lastTime <= 5 * MIN_MS) {
      group._lastTime     = t;
      group.totalGallons += iv.volume;
    } else {
      if (group) events.push({ timeStart: group._start, cls: group._cls, totalGallons: group.totalGallons });
      group = { _start: formatUTCTime(t), _lastTime: t, _cls: cls, totalGallons: iv.volume };
    }
  }
  if (group) events.push({ timeStart: group._start, cls: group._cls, totalGallons: group.totalGallons });
  return events;
}

function compareWithCorrections(agentResult, corrected) {
  if (!corrected?.intervals?.length || !agentResult?.classifications?.length) return;

  const corrGroups = groupCorrectionsIntervals(corrected.intervals);

  let rulesAgentAgreements    = 0;
  let rulesAgentDisagreements = 0;
  let metronAgentAgreements   = 0;
  let metronAgentDisagreements = 0;

  for (const ac of agentResult.classifications) {
    const acMinutes = timeToMinutes(ac.timeStart);
    if (acMinutes === null) continue;

    const agentCls = normalizeClass(ac.classification);

    // Find closest corrections group by timeStart
    let bestCorr = null;
    let bestDiff = Infinity;
    for (const cg of corrGroups) {
      const cgMin = timeToMinutes(cg.timeStart);
      if (cgMin === null) continue;
      const diff = Math.abs(cgMin - acMinutes);
      if (diff < bestDiff && diff <= 10) { bestDiff = diff; bestCorr = cg; }
    }

    if (bestCorr) {
      const rulesCls = bestCorr.cls;
      const metronCls = normalizeClass(ac.metronAgreement ? ac.classification : 'other');

      if (agentCls === rulesCls) {
        rulesAgentAgreements++;
      } else {
        rulesAgentDisagreements++;
        console.log('DISAGREEMENT:', JSON.stringify({
          timeWindow:     `${ac.timeStart}${ac.timeEnd ? '–' + ac.timeEnd : ''}`,
          metron:         ac.metronAgreement ? agentCls : 'different',
          rules:          rulesCls,
          agent:          agentCls,
          agentConfidence: ac.confidence,
          agreement:      agentCls === rulesCls ? 'agent+rules vs metron' :
                          `rules=${rulesCls}, agent=${agentCls}`,
        }));
      }

      if (ac.metronAgreement) metronAgentAgreements++;
      else metronAgentDisagreements++;
    }
  }

  console.log(`Agent vs Rules: ${rulesAgentAgreements} agreements, ${rulesAgentDisagreements} disagreements`);
  console.log(`Agent vs Metron: ${metronAgentAgreements} agreements, ${metronAgentDisagreements} disagreements`);
}

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const args       = process.argv.slice(2);
  const userIdFlag = args.indexOf('--userId');
  const userId     = userIdFlag !== -1 ? args[userIdFlag + 1] : null;
  const targetDate = args.find(a => /^\d{4}-\d{2}-\d{2}$/.test(a)) ?? (() => {
    const d = new Date();
    d.setDate(d.getDate() - 1);
    return d.toISOString().slice(0, 10);
  })();

  const ns = userId ? `waterwise:${userId}` : 'waterwise';
  console.log(`agent-classify: date=${targetDate} userId=${userId ?? 'owner'}`);

  if (!process.env.REDIS_URL) {
    console.error('REDIS_URL not set'); process.exit(1);
  }
  if (!process.env.ANTHROPIC_API_KEY) {
    console.error('ANTHROPIC_API_KEY not set'); process.exit(1);
  }

  const redis    = new Redis(process.env.REDIS_URL);
  const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

  try {
    // ── Read inputs from Redis ──────────────────────────────────────────────
    const profileKey = userId ? `waterwise:household:${userId}` : 'waterwise:household:owner';
    const [
      intervalsRaw, profileRaw, latestRaw, signaturesRaw, correctedRaw,
    ] = await Promise.all([
      redis.get(`${ns}:intervals:${targetDate}`),
      redis.get(profileKey),
      redis.get(`${ns}:latest`),
      redis.lrange(`${ns}:signatures`, 0, 9),
      redis.get(`${ns}:corrected:${targetDate}`),
    ]);

    if (!intervalsRaw) {
      console.log(`No interval data for ${targetDate} — skipping`);
      return;
    }

    const profile    = profileRaw  ? JSON.parse(profileRaw)  : null;
    const latest     = latestRaw   ? JSON.parse(latestRaw)   : null;
    const corrected  = correctedRaw ? JSON.parse(correctedRaw) : null;
    const signatures = signaturesRaw
      .map(s => { try { return JSON.parse(s); } catch { return null; } })
      .filter(Boolean);

    // Dishwasher-specific signature context
    const dishwasherSigs     = signatures.filter(s => s.category === 'dishwasher');
    const dishwasherSigCount = dishwasherSigs.length;
    const dishwasherSigTimes = dishwasherSigs.map(s => s.timeOfDay).filter(Boolean);

    // ── Previous day context ────────────────────────────────────────────────
    const prevDate    = new Date(new Date(targetDate).getTime() - 86400000).toISOString().slice(0, 10);
    const prevAgentRaw = await redis.get(`${ns}:agent-classified:${prevDate}`);
    const prevCorrRaw  = prevAgentRaw ? null : await redis.get(`${ns}:corrected:${prevDate}`);

    let prevDayCtx = null;
    if (prevAgentRaw) {
      const prev = JSON.parse(prevAgentRaw);
      const summary = (prev.classifications ?? []).slice(0, 5)
        .map(c => `${c.timeStart}: ${c.classification} (${c.gallons}G)`)
        .join(', ');
      prevDayCtx = `${prevDate}: ${summary}`;
    } else if (prevCorrRaw) {
      const prev = JSON.parse(prevCorrRaw);
      const cf = prev.correctedFixtures ?? {};
      prevDayCtx = `${prevDate}: shower=${cf.shower ?? 0}G toilet=${cf.toilet?.total ?? 0}G dishwasher=${cf.dishwasher ?? 0}G`;
    }

    // ── Step 1: Normalize + group intervals ─────────────────────────────────
    const intervals = extractIntervals(JSON.parse(intervalsRaw));
    intervals.sort((a, b) => (a.time ?? 0) - (b.time ?? 0));
    const allEvents = groupIntervalsIntoEvents(intervals);
    // Filter out sub-threshold events to keep prompt and response size manageable.
    // Events < 0.1G and < 2 min are likely meter artifacts — skip them for classification.
    const events = allEvents.filter(e => e.totalGallons >= 0.1 || e.durationMin >= 2);
    console.log(`Grouped ${intervals.length} intervals into ${allEvents.length} events (${events.length} after filtering)`);

    // ── Step 2: Build prompt ─────────────────────────────────────────────────
    const dayOfWeek = DAYS[new Date(targetDate + 'T12:00:00Z').getUTCDay()];
    const billingCtx = latest ? {
      billingCycleDay: latest.billingCycleDay,
      soFarThisCycle:  latest.soFarThisCycle,
      sevenDayAvg:     latest.sevenDayAvg,
      currentTier:     latest.currentTier,
    } : null;

    const { system, userPrompt } = buildPrompt({
      date:       targetDate,
      dayOfWeek,
      events,
      profile,
      signatures,
      billingCtx,
      prevDayCtx,
      dishwasherSigCount,
      dishwasherSigTimes,
    });

    // ── Step 3: Call Claude API ──────────────────────────────────────────────
    console.log(`Calling Claude API (${events.length} events)...`);
    let agentResult;
    try {
      const message = await anthropic.messages.create({
        model:      'claude-opus-4-5',
        max_tokens: 4096,
        system,
        messages: [{ role: 'user', content: userPrompt }],
      });
      let text = message.content[0]?.text ?? '';
      // Strip markdown code fences if present (model sometimes ignores "JSON only" instruction)
      text = text.replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/, '').trim();
      agentResult = JSON.parse(text);
      console.log(`Agent returned ${(agentResult.classifications ?? []).length} classifications, ` +
                  `${(agentResult.anomalies ?? []).length} anomalies, ` +
                  `${(agentResult.insights ?? []).length} insights (pre-threshold)`);

      // Apply confidence thresholds and dishwasher restriction
      agentResult = applyConfidenceThresholds(agentResult, dishwasherSigCount);
      console.log(`After thresholds: ${(agentResult.classifications ?? []).length} classifications, ` +
                  `${(agentResult.anomalies ?? []).length} anomalies`);
    } catch (err) {
      console.error('Claude API call failed:', err.message);
      return;
    }

    // ── Step 4: Store results ────────────────────────────────────────────────

    // Enrich classifications with duration + intervalCount before storing.
    // Agent returns timeStart/timeEnd as "6:31 AM" strings; compute duration
    // so downstream consumers (shower leaderboard assign flow) see non-zero values.
    function parseMinutes(timeStr) {
      if (!timeStr) return null;
      const [time, period] = timeStr.split(' ');
      const [h, m] = time.split(':').map(Number);
      const hours = period === 'PM' && h !== 12 ? h + 12
        : (period === 'AM' && h === 12 ? 0 : h);
      return hours * 60 + m;
    }
    for (const c of (agentResult.classifications ?? [])) {
      const startMin = parseMinutes(c.timeStart);
      const endMin   = parseMinutes(c.timeEnd || c.timeStart);
      if (startMin !== null && endMin !== null) {
        c.duration      = Math.max(0, endMin - startMin);
        c.intervalCount = c.duration; // 1 interval per minute approximation
      }
    }

    const payload = {
      date:          targetDate,
      classifiedAt:  new Date().toISOString(),
      userId:        userId ?? 'owner',
      model:         'claude-opus-4-5',
      eventCount:    events.length,
      ...agentResult,
    };
    const agentKey = `${ns}:agent-classified:${targetDate}`;
    await redis.set(agentKey, JSON.stringify(payload), 'EX', 7776000);
    console.log(`Stored agent results → ${agentKey}`);

    // ── Step 5: Compare with corrections ─────────────────────────────────────
    if (corrected) {
      compareWithCorrections(agentResult, corrected);
    } else {
      console.log('No corrections output to compare against');
    }

    // Print insights
    if (agentResult.insights?.length) {
      console.log('Insights:', agentResult.insights.join(' | '));
    }

    console.log('agent-classify complete');
  } finally {
    await redis.quit();
  }
}

main().catch(err => {
  console.error('agent-classify failed:', err.message);
  process.exit(1);
});
