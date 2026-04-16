'use strict';

// Fire-and-forget email send logger.
// Writes to two stores:
//   waterwise:email-log:YYYY-MM-DD  — daily list, 90-day TTL (for digest)
//   waterwise:email-log             — rolling list, last 100 (for /api/health)
async function logEmail(redis, { type, to, userId, subject, success = true, error = null }) {
  if (!redis) return;
  try {
    const entry = JSON.stringify({
      sentAt:  new Date().toISOString(),
      type,
      to,
      userId:  userId ?? null,
      subject: subject ?? null,
      success,
      error:   error ?? null,
    });
    const today  = new Date().toISOString().slice(0, 10);
    const dayKey = `waterwise:email-log:${today}`;
    await Promise.all([
      redis.lpush(dayKey, entry).then(() => redis.expire(dayKey, 7776000)),
      redis.lpush('waterwise:email-log', entry).then(() => redis.ltrim('waterwise:email-log', 0, 99)),
    ]);
  } catch (_) {}
}

module.exports = { logEmail };
