'use strict';

const crypto = require('crypto');

function asBuffer(value, expectedLength, name) {
  const buffer = Buffer.isBuffer(value) ? value : Buffer.from(value);
  if (expectedLength != null && buffer.length !== expectedLength) {
    throw new RangeError(`${name} must be ${expectedLength} bytes`);
  }
  return buffer;
}

function sequenceBuffer(sequence) {
  if (Buffer.isBuffer(sequence) || ArrayBuffer.isView(sequence)) {
    return asBuffer(sequence, 8, 'sequence');
  }
  const buffer = Buffer.alloc(8);
  buffer.writeBigUInt64LE(BigInt(sequence));
  return buffer;
}

function cryptBody(body, key, sequence) {
  const keyBytes = asBuffer(key, 32, 'key');
  const sequenceBytes = sequenceBuffer(sequence);
  const iv = Buffer.concat([Buffer.alloc(8), sequenceBytes]);
  const cipher = crypto.createCipheriv('chacha20', keyBytes, iv);
  return Buffer.concat([cipher.update(asBuffer(body, null, 'body')), cipher.final()]);
}

function packetTag(ciphertext, key, sequence) {
  return crypto.createHash('sha256')
    .update(asBuffer(ciphertext, null, 'ciphertext'))
    .update(asBuffer(key, 32, 'key'))
    .update(sequenceBuffer(sequence))
    .digest()
    .subarray(0, 6);
}

function encryptPacket(plaintext, key, sequence) {
  const ciphertext = cryptBody(plaintext, key, sequence);
  return Buffer.concat([ciphertext, packetTag(ciphertext, key, sequence)]);
}

function decryptPacket(packet, key, sequence) {
  const packetBytes = asBuffer(packet, null, 'packet');
  if (packetBytes.length < 6) {
    throw new RangeError('packet must contain a 6-byte tag');
  }
  const ciphertext = packetBytes.subarray(0, -6);
  const receivedTag = packetBytes.subarray(-6);
  const expectedTag = packetTag(ciphertext, key, sequence);
  if (!crypto.timingSafeEqual(receivedTag, expectedTag)) {
    throw new Error('invalid packet tag');
  }
  return cryptBody(ciphertext, key, sequence);
}

module.exports = {
  decryptPacket,
  encryptPacket,
  packetTag,
  sequenceBuffer
};
