'use strict';

// Rate tables (per 1000 gal) by drought level
const RATES = {
  1: [2.89, 3.85,  7.70, 11.54, 15.39],
  2: [2.89, 3.85, 11.54, 17.33, 23.09],
  3: [3.85, 5.78, 23.09, 34.63, 46.17],
};
const TIER_THRESHOLDS = [0, 3800, 7600, 11400, 15200];
const TIER_LABELS = ['Tier 1', 'Tier 2', 'Tier 3', 'Tier 4', 'Tier 5'];

function fmt(n, dec = 0) {
  if (n == null) return '—';
  return Number(n).toLocaleString('en-US', { minimumFractionDigits: dec, maximumFractionDigits: dec });
}
function fmtDollar(n) {
  if (n == null) return '—';
  return '$' + Number(n).toFixed(2);
}

function weekLabel(day) {
  return `Week ${Math.ceil(day / 7)}`;
}

function monthName(date) {
  return date.toLocaleString('en-US', { month: 'long', year: 'numeric' });
}

// ─── Shared layout wrapper ─────────────────────────────────────────────────

const LOGO_SVG = `
<svg width="36" height="36" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg" style="display:block;margin-bottom:10px;">
  <path d="M18 4C18 4 7 16 7 22a11 11 0 0022 0C29 16 18 4 18 4z" fill="rgba(255,255,255,0.92)"/>
  <path d="M13 24c0 3.5 2.4 5.5 5 5.5" stroke="rgba(37,99,235,0.45)" stroke-width="1.8" stroke-linecap="round"/>
</svg>`;

function wrap(accentColor, headerContent, bodyContent) {
  return `<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f0f4f8;padding:24px 0;">
  <tr><td align="center">
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:480px;">

      <!-- Header -->
      <tr><td style="background:${accentColor};border-radius:12px 12px 0 0;padding:28px 32px;">
        ${LOGO_SVG}
        ${headerContent}
      </td></tr>

      <!-- Body -->
      <tr><td style="background:#ffffff;border-radius:0 0 12px 12px;padding:28px 32px;">
        ${bodyContent}
      </td></tr>

      <!-- Footer -->
      <tr><td style="padding:16px 0;text-align:center;">
        <p style="margin:0;font-size:11px;color:#94a3b8;">
          WaterWise · Boulder County water tracking
        </p>
      </td></tr>

    </table>
  </td></tr>
</table>
</body></html>`;
}

// ─── Stat grid cell ────────────────────────────────────────────────────────

function statCell(label, value, sub) {
  return `
  <td width="33%" style="text-align:center;padding:0 4px;">
    <div style="background:#f8fafc;border-radius:8px;padding:14px 8px;">
      <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;margin-bottom:4px;">${label}</div>
      <div style="font-size:20px;font-weight:700;color:#1e293b;line-height:1.1;">${value}</div>
      ${sub ? `<div style="font-size:11px;color:#94a3b8;margin-top:2px;">${sub}</div>` : ''}
    </div>
  </td>`;
}

// ─── Tier row ──────────────────────────────────────────────────────────────

function tierRow(tierIndex, soFarThisCycle, droughtLevel) {
  const low      = TIER_THRESHOLDS[tierIndex];
  const high     = TIER_THRESHOLDS[tierIndex + 1] ?? Infinity;
  const rate     = (RATES[droughtLevel] || RATES[1])[tierIndex];
  const isActive = soFarThisCycle >= low && (high === Infinity || soFarThisCycle < high);
  const filled   = high === Infinity ? 0
                 : Math.min(100, Math.max(0, ((soFarThisCycle - low) / (high - low)) * 100));
  const bgColor  = isActive ? '#eff6ff' : '#f8fafc';
  const label    = isActive ? `<strong>${TIER_LABELS[tierIndex]}</strong>` : TIER_LABELS[tierIndex];
  const rangeStr = high === Infinity ? `${fmt(low)}+ gal` : `${fmt(low)}–${fmt(high)} gal`;

  return `
  <tr>
    <td style="padding:6px 0;">
      <div style="background:${bgColor};border-radius:8px;padding:10px 14px;${isActive ? 'border-left:3px solid #2563eb;' : ''}">
        <table width="100%" cellpadding="0" cellspacing="0" border="0">
          <tr>
            <td style="font-size:13px;color:#334155;">${label}</td>
            <td style="font-size:11px;color:#94a3b8;text-align:right;">${rangeStr}</td>
            <td style="text-align:right;padding-left:8px;">
              <span style="background:${isActive ? '#dbeafe' : '#e2e8f0'};color:${isActive ? '#1d4ed8' : '#64748b'};font-size:11px;font-weight:600;padding:2px 8px;border-radius:20px;">
                $${rate}/kgal
              </span>
            </td>
          </tr>
          ${high !== Infinity ? `
          <tr><td colspan="3" style="padding-top:6px;">
            <div style="background:#e2e8f0;border-radius:4px;height:4px;">
              <div style="background:${isActive ? '#2563eb' : '#cbd5e1'};border-radius:4px;height:4px;width:${isActive ? filled : (soFarThisCycle >= high ? 100 : 0)}%;"></div>
            </div>
          </td></tr>` : ''}
        </table>
      </div>
    </td>
  </tr>`;
}

// ─── Snowpack bar ──────────────────────────────────────────────────────────

function snowBar(label, pct) {
  if (pct == null) return '';
  const capped  = Math.min(pct, 150);
  const color   = pct >= 90 ? '#2563eb' : pct >= 60 ? '#f59e0b' : '#ef4444';
  return `
  <tr><td style="padding:4px 0;">
    <div style="display:flex;align-items:center;justify-content:space-between;font-size:12px;color:#475569;margin-bottom:3px;">
      <span>${label}</span><span style="font-weight:600;color:${color};">${pct}% of normal</span>
    </div>
    <div style="background:#e2e8f0;border-radius:4px;height:6px;">
      <div style="background:${color};border-radius:4px;height:6px;width:${capped / 1.5}%;"></div>
    </div>
  </td></tr>`;
}

// ─── Contextual tip ────────────────────────────────────────────────────────

function contextualTip(data) {
  const { nudge, hasIrrigation, spikeAlert } = data;
  if (spikeAlert) {
    return hasIrrigation
      ? 'Check your irrigation controller — a stuck valve can run for hours unnoticed. Walk your yard for pooling water or unusually wet areas.'
      : 'Check all toilets for running water and look under sinks for slow drips. If no obvious source, read your meter at night vs morning.';
  }
  if (nudge === 'in_tier_3plus') {
    return hasIrrigation
      ? 'Your irrigation is the biggest lever right now. Consider skipping a cycle or shortening run times — each minute matters in Tier 3.'
      : 'You\'re in Tier 3. Check for leaks (toilet dye test, meter reading at night), and delay any irrigation startup if possible.';
  }
  if (nudge === 'in_tier_2') {
    return hasIrrigation
      ? 'You\'re in Tier 2 — pull back irrigation by 15–20% to stay off the Tier 3 cliff.'
      : 'You\'re in Tier 2. Indoor use adds up fast — check showerheads, faucet aerators, and any slow drips.';
  }
  if (nudge === 'approaching') {
    return 'You\'re close to the next tier threshold. A short pause on irrigation or a load of laundry timing shift could keep you in a lower rate.';
  }
  return null; // no tip warranted
}

// ─── Dynamic subject line ──────────────────────────────────────────────────

function subjectLine(data, name) {
  const {
    spikeAlert, nudge, galsTilNextTier, currentTier = 1,
    soFarThisCycle, billingCycleDay, daysInMonth,
  } = data;
  const prefix = name ? `${name} · ` : '';
  const date   = new Date();
  const month  = date.toLocaleString('en-US', { month: 'long' });

  if (spikeAlert) {
    return `${prefix}Something unusual happened with your water yesterday 💧`;
  }
  if (nudge === 'approaching' || nudge === 'in_tier_2' || nudge === 'in_tier_3plus') {
    return `${prefix}${fmt(galsTilNextTier)} gal from Tier ${currentTier + 1} · act this week`;
  }
  return `${prefix}${month} · ${fmt(soFarThisCycle)} gal · Day ${billingCycleDay} of ${daysInMonth}`;
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. weeklySnapshot
// ═══════════════════════════════════════════════════════════════════════════

function weeklySnapshot(data, _history, userId) {
  const {
    soFarThisCycle = 0, dailyAverage = 0, costSoFar = 0, projectedCost = 0,
    billingCycleDay = 1, daysInMonth = 30, daysRemaining = 0,
    currentTier = 1, droughtLevel = 1,
    galsTilNextTier = 0, spikeAlert = false, waterConsumptionToday,
    nudge = 'none',
    snowpackSWEPct, precipPct, snowpackSWE, snowpackSWEMedian,
  } = data;

  const date    = new Date();
  const week    = new Date().toISOString().slice(0, 10);
  const dashUrl = userId
    ? 'https://water-wise-gauge.lovable.app?user=' + userId + '&ref=email&week=' + week
    : 'https://water-wise-gauge.lovable.app';

  // ── Headline stat ──────────────────────────────────────────────────────
  let headlineValue, headlineLabel, headlineSub;
  if (spikeAlert && waterConsumptionToday != null) {
    headlineValue = fmt(waterConsumptionToday) + ' gal';
    headlineLabel = 'Yesterday\'s usage';
    headlineSub   = 'Spike detected';
  } else if (nudge === 'approaching' || nudge === 'in_tier_2' || nudge === 'in_tier_3plus') {
    headlineValue = fmt(galsTilNextTier) + ' gal';
    headlineLabel = `Until Tier ${currentTier + 1}`;
    headlineSub   = nudge === 'approaching' ? 'Getting close' : `Currently in Tier ${currentTier}`;
  } else {
    headlineValue = fmt(soFarThisCycle) + ' gal';
    headlineLabel = 'Used so far';
    headlineSub   = `Day ${billingCycleDay} of ${daysInMonth}`;
  }

  const header = `
    <h1 style="margin:0 0 4px;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-.3px;">
      WaterWise &middot; ${monthName(date)}
    </h1>
    <p style="margin:0;font-size:14px;color:#bfdbfe;">
      ${weekLabel(billingCycleDay)} &middot; Day ${billingCycleDay} of ${daysInMonth}
    </p>`;

  // ── Two-tier display (current + next only) ─────────────────────────────
  const currTierIdx = currentTier - 1;
  const nextTierIdx = currentTier < 5 ? currentTier : null;
  const tiers = tierRow(currTierIdx, soFarThisCycle, droughtLevel)
    + (nextTierIdx !== null ? tierRow(nextTierIdx, soFarThisCycle, droughtLevel) : '');

  // ── Conditional tip ────────────────────────────────────────────────────
  const tip = contextualTip(data);

  // ── Snowpack (< 60% only) ──────────────────────────────────────────────
  const showSnow = snowpackSWEPct != null && snowpackSWEPct < 60;

  const body = `
    <!-- Headline stat -->
    <div style="text-align:center;margin-bottom:24px;padding:20px;background:#f8fafc;border-radius:10px;">
      <div style="font-size:11px;color:#64748b;text-transform:uppercase;letter-spacing:.06em;margin-bottom:6px;">${headlineLabel}</div>
      <div style="font-size:44px;font-weight:800;color:#1e293b;line-height:1;letter-spacing:-.5px;">${headlineValue}</div>
      <div style="font-size:12px;color:#94a3b8;margin-top:6px;">${headlineSub}</div>
    </div>

    <!-- Supporting stats -->
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
      <tr>
        ${statCell('Daily avg', fmt(dailyAverage, 1) + ' gal', 'this cycle')}
        ${statCell('Cost so far', fmtDollar(costSoFar), 'this cycle')}
        ${statCell('Projected', fmtDollar(projectedCost), `${daysRemaining}d remaining`)}
      </tr>
    </table>

    <!-- Tier rows (current + next only) -->
    <p style="margin:0 0 8px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#64748b;">Water Tiers</p>
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
      ${tiers}
    </table>

    ${tip ? `
    <!-- Conservation tip (only when nudge triggered) -->
    <div style="background:#f0f9ff;border-left:3px solid #2563eb;border-radius:0 8px 8px 0;padding:14px 16px;margin-bottom:24px;">
      <p style="margin:0 0 4px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:#2563eb;">Conservation tip</p>
      <p style="margin:0;font-size:13px;color:#1e40af;line-height:1.5;">${tip}</p>
    </div>` : ''}

    ${showSnow ? `
    <!-- Snowpack (only when < 60%) -->
    <p style="margin:0 0 8px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#64748b;">Snowpack (Station 936)</p>
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
      ${snowBar('Snow Water Equivalent', snowpackSWEPct)}
      ${snowBar('Precip Accumulation', precipPct)}
      <tr><td style="padding-top:4px;">
        <p style="margin:0;font-size:11px;color:#94a3b8;">
          SWE: ${snowpackSWE != null ? snowpackSWE + '"' : '—'} vs ${snowpackSWEMedian != null ? snowpackSWEMedian + '" median' : '—'}
        </p>
      </td></tr>
    </table>` : ''}

    <!-- CTA -->
    <table width="100%" cellpadding="0" cellspacing="0" border="0">
      <tr><td align="center">
        <a href="${dashUrl}" style="display:inline-block;background:#2563eb;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;padding:12px 32px;border-radius:8px;">
          View Dashboard
        </a>
      </td></tr>
    </table>`;

  const html = wrap('#2563eb', header, body);

  const text = [
    `WaterWise · ${monthName(date)}`,
    `${weekLabel(billingCycleDay)} · Day ${billingCycleDay} of ${daysInMonth}`,
    '',
    `${headlineLabel}: ${headlineValue}`,
    '',
    `Daily avg:      ${fmt(dailyAverage, 1)} gal`,
    `Cost so far:    ${fmtDollar(costSoFar)}`,
    `Projected cost: ${fmtDollar(projectedCost)}`,
    '',
    'Tiers (current + next):',
    tierRow(currTierIdx, soFarThisCycle, droughtLevel).replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim(),
    nextTierIdx !== null
      ? tierRow(nextTierIdx, soFarThisCycle, droughtLevel).replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim()
      : '',
    '',
    tip ? tip : '',
    '',
    showSnow ? `Snowpack: ${snowpackSWEPct}% of normal SWE · Precip: ${precipPct}% of normal` : '',
    '',
    `Dashboard: ${dashUrl}`,
  ].filter(l => l !== undefined).join('\n');

  return { html, text };
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. tierAlert
// ═══════════════════════════════════════════════════════════════════════════

function tierAlert(data, userId) {
  const {
    soFarThisCycle = 0, dailyAverage = 0, galsTilNextTier = 0,
    daysRemaining = 0, daysUntilTierCross, currentTier = 1, droughtLevel = 1,
    hasIrrigation = false,
  } = data;

  const dashUrl       = userId
    ? 'https://water-wise-gauge.lovable.app?user=' + userId
    : 'https://water-wise-gauge.lovable.app';
  const rates         = RATES[droughtLevel] || RATES[1];
  const currentRate   = rates[currentTier - 1];
  const nextRate      = rates[currentTier] ?? rates[rates.length - 1];
  const remaining     = daysRemaining * dailyAverage;
  const costAtCurrent = (galsTilNextTier / 1000) * currentRate;
  const overTier      = Math.max(0, remaining - galsTilNextTier);
  const costAtNext    = (overTier / 1000) * nextRate;
  const extraCost     = costAtCurrent + costAtNext - (remaining / 1000) * currentRate;
  const crossDays     = daysUntilTierCross != null ? `${daysUntilTierCross} days` : 'soon';

  const header = `
    <h1 style="margin:0 0 4px;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-.3px;">
      ${fmt(galsTilNextTier)} gal from Tier ${currentTier + 1}
    </h1>
    <p style="margin:0;font-size:14px;color:#fecaca;">
      At current pace, you cross in ${crossDays}
    </p>`;

  const tips = hasIrrigation ? [
    'Cut one irrigation cycle this week — a single zone run is typically 300–600 gal.',
    'Check your irrigation schedule: pre-dawn watering loses less to evaporation.',
    'Inspect heads for misting or overspray onto pavement — that\'s pure waste.',
  ] : [
    'Run only full loads in the dishwasher and washing machine this week.',
    'A 5-minute shower uses ~10 gal — cutting 2 min saves ~30 gal/day per person.',
    'Check toilets for silent leaks: add dye tablets to the tank and watch for 15 min.',
  ];

  const body = `
    <!-- Stat grid -->
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
      <tr>
        ${statCell('Used so far', fmt(soFarThisCycle) + ' gal', 'this cycle')}
        ${statCell('Til next tier', fmt(galsTilNextTier) + ' gal', `Tier ${currentTier} → ${currentTier + 1}`)}
        ${statCell('Days left', fmt(daysRemaining), 'in billing cycle')}
      </tr>
    </table>

    <!-- Financial impact band -->
    <div style="background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;padding:16px;margin-bottom:24px;">
      <p style="margin:0 0 6px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#92400e;">Financial Impact</p>
      <p style="margin:0;font-size:24px;font-weight:700;color:#b45309;">
        Crossing now adds ~${fmtDollar(Math.max(0, extraCost))} to your bill
      </p>
      <p style="margin:6px 0 0;font-size:12px;color:#78350f;">
        ${fmt(galsTilNextTier)} gal @ $${currentRate}/kgal → ${fmt(overTier, 0)} gal @ $${nextRate}/kgal
      </p>
    </div>

    <!-- Tips -->
    <p style="margin:0 0 12px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#64748b;">
      ${hasIrrigation ? 'Outdoor' : 'Indoor'} Conservation Tips
    </p>
    ${tips.map((tip, i) => `
    <div style="display:flex;margin-bottom:12px;">
      <div style="min-width:24px;height:24px;background:#fef3c7;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#d97706;margin-right:12px;">${i + 1}</div>
      <p style="margin:0;font-size:13px;color:#334155;line-height:1.5;padding-top:3px;">${tip}</p>
    </div>`).join('')}

    <!-- CTA -->
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;">
      <tr><td align="center">
        <a href="${dashUrl}" style="display:inline-block;background:#d97706;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;padding:12px 32px;border-radius:8px;">
          View Dashboard
        </a>
      </td></tr>
    </table>`;

  const html = wrap('#dc2626', header, body);

  const text = [
    `WATERWISE ALERT: ${fmt(galsTilNextTier)} gal from Tier ${currentTier + 1}`,
    `At current pace you cross in ${crossDays}.`,
    '',
    `Used so far:      ${fmt(soFarThisCycle)} gal`,
    `Til next tier:    ${fmt(galsTilNextTier)} gal`,
    `Days remaining:   ${fmt(daysRemaining)}`,
    '',
    `Crossing now adds ~${fmtDollar(Math.max(0, extraCost))} to your bill.`,
    '',
    'Tips:',
    ...tips.map((t, i) => `  ${i + 1}. ${t}`),
    '',
    `Dashboard: ${dashUrl}`,
  ].join('\n');

  return { html, text };
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. spikeAlert
// ═══════════════════════════════════════════════════════════════════════════

function spikeAlert(data, consumptionToday, sevenDayAvg, userId) {
  const { spikeMultiplier = '?', hasIrrigation = false } = data;
  const multiplier = spikeMultiplier || (sevenDayAvg > 0 ? (consumptionToday / sevenDayAvg).toFixed(1) : '?');
  const dashUrl    = userId
    ? 'https://water-wise-gauge.lovable.app?user=' + userId
    : 'https://water-wise-gauge.lovable.app';

  const header = `
    <h1 style="margin:0 0 4px;font-size:22px;font-weight:700;color:#ffffff;letter-spacing:-.3px;">
      Unusual water use detected
    </h1>
    <p style="margin:0;font-size:14px;color:#fecaca;">
      ${fmt(consumptionToday)} gal yesterday &mdash; ${multiplier}x your normal
    </p>`;

  const tips = hasIrrigation ? [
    'Check your irrigation controller — a stuck valve can run for hours unnoticed.',
    'Walk your yard and look for pooling water or unusually wet areas.',
    'Review your irrigation schedule — a mis-programmed zone can triple your use.',
  ] : [
    'Check all toilets for running water (listen or use a dye tablet).',
    'Look under sinks and around the water heater for slow drips.',
    'If no obvious leak, check your meter reading at night vs morning.',
  ];

  const body = `
    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-bottom:24px;">
      <tr>
        ${statCell('Yesterday', fmt(consumptionToday) + ' gal', 'consumed')}
        ${statCell('Your normal', fmt(sevenDayAvg) + ' gal', '7-day avg')}
        ${statCell('Multiplier', multiplier + 'x', 'above average')}
      </tr>
    </table>

    <p style="margin:0 0 12px;font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#64748b;">
      What to check
    </p>
    ${tips.map((tip, i) => `
    <div style="display:flex;margin-bottom:12px;">
      <div style="min-width:24px;height:24px;background:#fee2e2;border-radius:50%;text-align:center;line-height:24px;font-size:12px;font-weight:700;color:#dc2626;margin-right:12px;">${i + 1}</div>
      <p style="margin:0;font-size:13px;color:#334155;line-height:1.5;padding-top:3px;">${tip}</p>
    </div>`).join('')}

    <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:24px;">
      <tr><td align="center">
        <a href="${dashUrl}" style="display:inline-block;background:#dc2626;color:#ffffff;font-size:14px;font-weight:600;text-decoration:none;padding:12px 32px;border-radius:8px;">
          View Dashboard
        </a>
      </td></tr>
    </table>`;

  const html = wrap('#dc2626', header, body);

  const text = [
    `WATERWISE SPIKE ALERT: ${fmt(consumptionToday)} gal yesterday (${multiplier}x normal)`,
    '',
    `Yesterday: ${fmt(consumptionToday)} gal`,
    `7-day avg: ${fmt(sevenDayAvg)} gal`,
    '',
    'What to check:',
    ...tips.map((t, i) => `  ${i + 1}. ${t}`),
    '',
    `Dashboard: ${dashUrl}`,
  ].join('\n');

  return { html, text };
}

module.exports = { weeklySnapshot, tierAlert, spikeAlert, subjectLine };
