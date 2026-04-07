'use strict';

const Redis = require('ioredis');
const { Resend } = require('resend');
const { weeklySnapshot } = require('./email-templates.js');

async function main() {
  const redisUrl      = process.env.REDIS_URL;
  const resendApiKey  = process.env.RESEND_API_KEY;
  const reportEmail   = process.env.REPORT_EMAIL;

  if (!redisUrl)     throw new Error('Missing REDIS_URL');
  if (!resendApiKey) throw new Error('Missing RESEND_API_KEY');
  if (!reportEmail)  throw new Error('Missing REPORT_EMAIL');

  const redis  = new Redis(redisUrl);
  const resend = new Resend(resendApiKey);

  try {
    // Read current data
    const latestRaw = await redis.get('waterwise:latest');
    if (!latestRaw) throw new Error('No data in waterwise:latest — scraper has not run yet');
    const data = JSON.parse(latestRaw);

    // Read last 7 daily keys for history
    const history = [];
    for (let i = 1; i <= 7; i++) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = `waterwise:${d.toISOString().slice(0, 10)}`;
      const raw = await redis.get(key);
      if (raw) history.push(JSON.parse(raw));
    }
    history.sort((a, b) => (a.consumptionDate || '').localeCompare(b.consumptionDate || ''));

    const { html, text } = weeklySnapshot(data, history);

    const date         = new Date();
    const monthName    = date.toLocaleString('en-US', { month: 'long' });
    const year         = date.getFullYear();
    const billingCycleDay = data.billingCycleDay ?? date.getDate();
    const daysInMonth  = data.daysInMonth ?? 30;

    const subject = `WaterWise · ${monthName} ${year} · Day ${billingCycleDay} of ${daysInMonth}`;

    const result = await resend.emails.send({
      from:    'onboarding@resend.dev',
      to:      reportEmail,
      subject,
      html,
      text,
    });

    console.log('Email sent:', result);
  } finally {
    await redis.quit();
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
