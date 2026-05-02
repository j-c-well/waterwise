'use strict';

function classifyUA(ua) {
  if (!ua) return 'unknown';
  return /Mobile|Android|iPhone|iPad|iPod/i.test(ua) ? 'mobile' : 'desktop';
}

async function logEvent(redis, event, req) {
  try {
    // Skip admin, scraper, and owner/anonymous calls — they inflate real-user metrics
    if (req?.query?.key) return;
    if (req?.headers?.['x-waterwise-source'] === 'scraper') return;
    if (!event.userId || event.userId === 'owner') return;

    const day = new Date().toISOString().slice(0, 10);
    const key = `waterwise:analytics:${day}`;
    const ua  = req?.headers?.['user-agent'] ?? null;
    const ip  = (req?.headers?.['x-forwarded-for'] ?? '').split(',')[0].trim() || null;
    await redis.lpush(key, JSON.stringify({
      ...event,
      ua:        classifyUA(ua),
      ip,
      timestamp: new Date().toISOString(),
    }));
    await redis.expire(key, 604800); // 7 days
  } catch (_) {}
}

module.exports = { logEvent };
