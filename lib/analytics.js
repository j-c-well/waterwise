'use strict';

async function logEvent(redis, event) {
  try {
    const day = new Date().toISOString().slice(0, 10);
    const key = `waterwise:analytics:${day}`;
    await redis.lpush(key, JSON.stringify({
      ...event,
      timestamp: new Date().toISOString(),
    }));
    await redis.expire(key, 7776000); // 90 days
  } catch (_) {}
}

module.exports = { logEvent };
