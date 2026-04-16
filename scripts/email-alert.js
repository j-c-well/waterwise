'use strict';

const { Resend } = require('resend');
const { tierAlert, spikeAlert } = require('./email-templates.js');
const { logEmail } = require('../lib/email-log.js');

// redis is optional — passed in from scrape.js for logging; omit in tests
async function sendAlerts(payload, redis) {
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
    const result = await resend.emails.send({ from: 'onboarding@resend.dev', to: reportEmail, subject, html, text });
    console.log('Tier alert sent:', result);
    await logEmail(redis, { type: 'tier_alert', to: reportEmail, userId: 'owner', subject });
    return;
  }

  // Spike alert (only if no tier alert)
  if (payload.spikeAlert) {
    const subject = `Unusual water use · ${payload.spikeMultiplier}x your normal · WaterWise`;
    const { html, text } = spikeAlert(payload, payload.waterConsumptionToday, payload.sevenDayAvg);
    const result = await resend.emails.send({ from: 'onboarding@resend.dev', to: reportEmail, subject, html, text });
    console.log('Spike alert sent:', result);
    await logEmail(redis, { type: 'spike_alert', to: reportEmail, userId: 'owner', subject });
    return;
  }

  console.log('No alerts to send');
}

// Send tier/spike alerts to a specific email address (for registered users)
// redis is optional — passed in from scrape.js for logging
async function sendAlertEmail(payload, toEmail, redis) {
  const resendApiKey = process.env.RESEND_API_KEY;
  if (!resendApiKey || !toEmail) return;

  const resend  = new Resend(resendApiKey);
  const userId  = payload.userId ?? null;

  if (payload.tierCrossedToday || payload.approachingTierAlert) {
    const subject = payload.tierCrossedToday
      ? `You've crossed into Tier ${payload.currentTier} · WaterWise`
      : `Heads up — ${Math.round(payload.galsTilNextTier).toLocaleString()} gal from Tier ${payload.currentTier + 1} · WaterWise`;
    const { html, text } = tierAlert(payload);
    await resend.emails.send({ from: 'onboarding@resend.dev', to: toEmail, subject, html, text });
    await logEmail(redis, { type: 'tier_alert', to: toEmail, userId, subject });
    return;
  }

  if (payload.spikeAlert) {
    const subject = `Unusual water use · ${payload.spikeMultiplier}x your normal · WaterWise`;
    const { html, text } = spikeAlert(payload, payload.waterConsumptionToday, payload.sevenDayAvg);
    await resend.emails.send({ from: 'onboarding@resend.dev', to: toEmail, subject, html, text });
    await logEmail(redis, { type: 'spike_alert', to: toEmail, userId, subject });
  }
}

module.exports = { sendAlerts, sendAlertEmail };
