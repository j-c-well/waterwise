'use strict';

const crypto  = require('crypto');
const Redis   = require('ioredis');
const { encrypt } = require('../../lib/crypto');

const redis = new Redis(process.env.REDIS_URL);

module.exports = async function handler(req, res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') { res.status(200).end(); return; }
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { email, password, name } = req.body ?? {};

  if (!email || typeof email !== 'string' || !email.includes('@')) {
    return res.status(400).json({ error: 'Valid email required' });
  }
  if (!password || typeof password !== 'string' || password.length < 6) {
    return res.status(400).json({ error: 'Password must be at least 6 characters' });
  }

  try {
    // Check for duplicate email via reverse-lookup key
    const existingId = await redis.get(`waterwise:email:${email.toLowerCase()}`);
    if (existingId) {
      return res.status(409).json({ error: 'An account with that email already exists' });
    }

    const userId     = crypto.randomBytes(4).toString('hex');
    const displayName = (name && name.trim()) || email.split('@')[0];
    const { encrypted: encryptedPassword, iv, authTag } = encrypt(password);

    const creds = {
      userId,
      email:             email.toLowerCase(),
      name:              displayName,
      encryptedPassword,
      iv,
      authTag,
      createdAt:         new Date().toISOString(),
      status:            'active',
    };

    await Promise.all([
      redis.set(`waterwise:creds:${userId}`, JSON.stringify(creds)),
      redis.set(`waterwise:email:${email.toLowerCase()}`, userId),
    ]);

    console.log(`Registered user ${userId} (${email.toLowerCase()})`);

    return res.status(201).json({
      userId,
      email:        creds.email,
      name:         creds.name,
      dashboardUrl: `https://waterwise-six.vercel.app?user=${userId}`,
    });
  } catch (err) {
    console.error('Register error:', err);
    return res.status(500).json({ error: err.message });
  }
};
