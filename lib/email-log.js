'use strict';

// Fire-and-forget email send logger.
// Logs to waterwise:email-log:YYYY-MM-DD (list), expires after 90 days.
async function logEmail(redis, { type, to, userId, success = true }) {
  if (!redis) return;
  try {
    const today  = new Date().toISOString().slice(0, 10);
    const logKey = `waterwise:email-log:${today}`;
    await redis.lpush(logKey, JSON.stringify({
      type,
      to,
      userId: userId ?? null,
      sentAt: new Date().toISOString(),
      success,
    }));
    await redis.expire(logKey, 7776000);
  } catch (_) {}
}

module.exports = { logEmail };
