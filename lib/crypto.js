'use strict';

const crypto = require('crypto');
const ALGO = 'aes-256-gcm';

function encrypt(text) {
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGO, key, iv);
  const encrypted = Buffer.concat([
    cipher.update(text, 'utf8'),
    cipher.final(),
  ]);
  const authTag = cipher.getAuthTag();
  return {
    encrypted: encrypted.toString('hex'),
    iv:        iv.toString('hex'),
    authTag:   authTag.toString('hex'),
  };
}

function decrypt(encrypted, iv, authTag) {
  const key = Buffer.from(process.env.ENCRYPTION_KEY, 'hex');
  const decipher = crypto.createDecipheriv(ALGO, key, Buffer.from(iv, 'hex'));
  decipher.setAuthTag(Buffer.from(authTag, 'hex'));
  const decrypted = Buffer.concat([
    decipher.update(Buffer.from(encrypted, 'hex')),
    decipher.final(),
  ]);
  return decrypted.toString('utf8');
}

module.exports = { encrypt, decrypt };
