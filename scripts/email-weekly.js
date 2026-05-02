'use strict';

const Redis = require('ioredis');
const { Resend } = require('resend');
const { logEmail } = require('../lib/email-log.js');
const { weeklySnapshot, subjectLine } = require('./email-templates.js');

async function sendWeeklyEmails() {
  const redisUrl      = process.env.REDIS_URL;
  const resendApiKey  = process.env.RESEND_API_KEY;
  const reportEmail   = process.env.REPORT_EMAIL;

  if (!redisUrl)     throw new Error('Missing REDIS_URL');
  if (!resendApiKey) throw new Error('Missing RESEND_API_KEY');
  if (!reportEmail)  throw new Error('Missing REPORT_EMAIL');

  const redis  = new Redis(redisUrl);
  const resend = new Resend(resendApiKey);

  try {
    // ── Dedup guard — prevent duplicate sends on the same day ──────────────
    const today       = new Date().toISOString().slice(0, 10);
    const dedupKey    = `waterwise:email-sent:${today}`;
    const alreadySent = await redis.get(dedupKey);
    if (alreadySent) {
      console.log(`Weekly email already sent today (${today}) — skipping.`);
      return;
    }
    await redis.set(dedupKey, '1', 'EX', 90000); // 25-hour TTL
    console.log(`Dedup key set: ${dedupKey}`);

    // ── Owner email ────────────────────────────────────────────────────────
    const latestRaw = await redis.get('waterwise:latest');
    if (!latestRaw) throw new Error('No data in waterwise:latest — scraper has not run yet');
    const data = JSON.parse(latestRaw);

    const { html, text } = weeklySnapshot(data, [], null, 'Joshua');
    const subject = subjectLine(data, 'Joshua');

    const result = await resend.emails.send({
      from:    'WaterWise <hello@popsiclelabs.io>',
      to:      reportEmail,
      subject,
      html,
      text,
      headers: { 'X-Entity-Ref-ID': 'owner-' + Date.now() },
    });

    console.log('Owner email sent:', result);
    await logEmail(redis, { type: 'weekly', to: reportEmail, userId: 'owner', subject });

    // ── Multi-user weekly emails ───────────────────────────────────────────
    const credKeys = await redis.keys('waterwise:creds:*');
    for (const credKey of credKeys) {
      try {
        const creds = JSON.parse(await redis.get(credKey));
        if (!creds?.userId || !creds?.email) continue;

        const userLatestRaw = await redis.get(`waterwise:${creds.userId}:latest`);
        if (!userLatestRaw) {
          console.log(`Skipping ${creds.email} — no data yet`);
          continue;
        }
        const userData = JSON.parse(userLatestRaw);

        // Skip if no meaningful data (prevents sending to users whose scrape failed)
        if (userData.soFarThisCycle == null) {
          console.log(`Skipping ${creds.email} — soFarThisCycle is null`);
          continue;
        }

        const { html: userHtml, text: userText } = weeklySnapshot(userData, [], creds.userId, creds.name);
        const userSubject = subjectLine(userData, creds.name);

        await resend.emails.send({
          from:    'WaterWise <hello@popsiclelabs.io>',
          to:      creds.email,
          subject: userSubject,
          html:    userHtml,
          text:    userText,
          headers: { 'X-Entity-Ref-ID': creds.userId + '-' + Date.now() },
        });
        console.log(`Weekly email sent to ${creds.email} — "${userSubject}"`);
        await logEmail(redis, { type: 'weekly', to: creds.email, userId: creds.userId, subject: userSubject });
      } catch (e) {
        console.log('Weekly email failed for', credKey, ':', e.message);
      }
    }
  } finally {
    await redis.quit();
  }
}

module.exports = { sendWeeklyEmails };

if (require.main === module) {
  sendWeeklyEmails().catch(err => {
    console.error(err);
    process.exit(1);
  });
}
