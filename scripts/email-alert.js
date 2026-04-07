'use strict';

const { Resend } = require('resend');
const { tierAlert, spikeAlert } = require('./email-templates.js');

async function sendAlerts(payload) {
  const resendApiKey = process.env.RESEND_API_KEY;
  const reportEmail  = process.env.REPORT_EMAIL;

  if (!resendApiKey || !reportEmail) {
    console.log('Skipping alerts — RESEND_API_KEY or REPORT_EMAIL not set');
    return;
  }

  const resend = new Resend(resendApiKey);

  // Tier alert takes priority
  if (payload.tierCrossedToday || payload.approachingTierAlert) {
    const subject = payload.tierCrossedToday
      ? `You've crossed into Tier ${payload.currentTier} · WaterWise`
      : `Heads up — ${Math.round(payload.galsTilNextTier).toLocaleString()} gal from Tier ${payload.currentTier + 1} · WaterWise`;

    const { html, text } = tierAlert(payload);

    const result = await resend.emails.send({
      from:    'onboarding@resend.dev',
      to:      reportEmail,
      subject,
      html,
      text,
    });

    console.log('Tier alert sent:', result);
    return;
  }

  // Spike alert (only if no tier alert)
  if (payload.spikeAlert) {
    const subject = `Unusual water use · ${payload.spikeMultiplier}x your normal · WaterWise`;
    const { html, text } = spikeAlert(payload, payload.waterConsumptionToday, payload.sevenDayAvg);

    const result = await resend.emails.send({
      from:    'onboarding@resend.dev',
      to:      reportEmail,
      subject,
      html,
      text,
    });

    console.log('Spike alert sent:', result);
    return;
  }

  console.log('No alerts to send');
}

module.exports = { sendAlerts };
