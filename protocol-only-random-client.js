'use strict';

console.log('[build] protocol-only updated party-proxy-u-position v2026-07-14');

const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { decryptPacket, encryptPacket } = require('./protocol-crypto');

const X25519_BASE = (() => {
  const bytes = Buffer.alloc(32);
  bytes[0] = 9;
  return bytes;
})();

const SERVER_IDENTITY_KEY_HEX = '98dcbf48d0d78d81d339a2d80bbe85c5d32d7a79eb7223ee9b7c4a54e101d57c';
const DEFAULT_PROTOCOLS = ['arras.io#v1.4+sls+et0', 'arras.io'];
const DEFAULT_HEADERS = {
  'accept-encoding': 'gzip, deflate, br, zstd',
  'accept-language': 'en-US,en;q=0.9',
  'cache-control': 'no-cache',
  'origin': 'https://arras.io',
  'pragma': 'no-cache',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36'
};
const DEFAULT_PROXY_URL = process.env.ARRAS_DEFAULT_PROXY_URL || '';
const DEFAULT_VALIDATION_R_VALUE = null;
const ALLOW_CAPTURED_R_REPLAY = false;
const ALLOW_CAPTURED_E_REPLAY = process.env.ARRAS_ALLOW_CAPTURED_E_REPLAY === '1';
const ALLOW_W_TOKEN_R_FALLBACK = process.env.ARRAS_ALLOW_W_TOKEN_R_FALLBACK === '1';
const ALLOW_EARLY_SPAWN = process.env.ARRAS_ALLOW_EARLY_SPAWN === '1';
const ALLOW_ASCII_SPAWN_TEMPLATE = process.env.ARRAS_ALLOW_ASCII_SPAWN_TEMPLATE === '1';
const USE_CAPTURED_K = process.env.ARRAS_USE_CAPTURED_K !== '0';
const SPAWN_AFTER_MODE_INFO = process.env.ARRAS_SPAWN_AFTER_MODE_INFO === '1';
const STARTUP_TEMPLATE_SPAWN = process.env.ARRAS_STARTUP_TEMPLATE_SPAWN === '1';
const EVAL_ENV_MODE = Number(process.env.ARRAS_E_ENV_MODE || 0);
const DEFAULT_SPAWN_EXTRA_FIELDS = [0, 0];
const DEFAULT_SPAWN_PACKET = Buffer.from([0x73, 0xc0, 0xc0, 0x01]);
const MOVEMENT_ENABLED = process.env.ARRAS_PROTOCOL_MOVEMENT !== '0';
const MOVEMENT_INTERVAL_MS = Number(process.env.ARRAS_MOVEMENT_INTERVAL_MS || 30);
const MOVEMENT_JITTER_MS = Number(process.env.ARRAS_MOVEMENT_JITTER_MS || 8);
const MOVEMENT_PHASE_JITTER_MS = Number(process.env.ARRAS_MOVEMENT_PHASE_JITTER_MS || 30);
const MANUAL_FORMATION_SPREAD = Number(process.env.ARRAS_MANUAL_FORMATION_SPREAD || 0.35);
const MANUAL_TARGET_SPREAD = Number(process.env.ARRAS_MANUAL_TARGET_SPREAD || 0);
const MANUAL_TARGET_SPREAD_WOBBLE = Number(process.env.ARRAS_MANUAL_TARGET_SPREAD_WOBBLE || 0);
const MANUAL_TARGET_SPREAD_REFRESH_MS = Number(process.env.ARRAS_MANUAL_TARGET_SPREAD_REFRESH_MS || 2600);
const MANUAL_AIM_WITH_MOVEMENT = process.env.ARRAS_MANUAL_AIM_WITH_MOVEMENT === '1';
const MANUAL_AIM_AXIS = Number(process.env.ARRAS_MANUAL_AIM_AXIS || 80);
const MANUAL_AIM_SPREAD = Number(process.env.ARRAS_MANUAL_AIM_SPREAD || 3.5);
const MANUAL_AIM_SPREAD_WOBBLE = Number(process.env.ARRAS_MANUAL_AIM_SPREAD_WOBBLE || 1.5);
const MANUAL_AIM_SPREAD_REFRESH_MS = Number(process.env.ARRAS_MANUAL_AIM_SPREAD_REFRESH_MS || 2300);
const CONTROL_AIM_MAX_AXIS = Number(process.env.ARRAS_CONTROL_AIM_MAX_AXIS || 96);
const CONTROL_AIM_DEADZONE = Number(process.env.ARRAS_CONTROL_AIM_DEADZONE || 0.75);
const FIRE_STAGGER_MS = Number(process.env.ARRAS_FIRE_STAGGER_MS || 180);
const FIRE_MICRO_PAUSE_ENABLED = process.env.ARRAS_FIRE_MICRO_PAUSE !== '0';
const FIRE_MICRO_PAUSE_MIN_MS = Number(process.env.ARRAS_FIRE_MICRO_PAUSE_MIN_MS || 45);
const FIRE_MICRO_PAUSE_MAX_MS = Number(process.env.ARRAS_FIRE_MICRO_PAUSE_MAX_MS || 120);
const FIRE_MICRO_PAUSE_INTERVAL_MIN_MS = Number(process.env.ARRAS_FIRE_MICRO_PAUSE_INTERVAL_MIN_MS || 1400);
const FIRE_MICRO_PAUSE_INTERVAL_MAX_MS = Number(process.env.ARRAS_FIRE_MICRO_PAUSE_INTERVAL_MAX_MS || 2600);
const STAT_BUILD_START_JITTER_MS = Number(process.env.ARRAS_STAT_BUILD_START_JITTER_MS || 650);
const STAT_BUILD_STEP_JITTER_MS = Number(process.env.ARRAS_STAT_BUILD_STEP_JITTER_MS || 45);
const MANUAL_CONTROL_INTERVAL_MS = Number(process.env.ARRAS_MANUAL_CONTROL_INTERVAL_MS || 90);
const MANUAL_TARGET_AXIS_SCALE = Number(process.env.ARRAS_MANUAL_TARGET_AXIS_SCALE || 40);
const MANUAL_TARGET_MAX_AXIS = Number(process.env.ARRAS_MANUAL_TARGET_MAX_AXIS || 18);
const MANUAL_TARGET_STOP_DISTANCE = Number(process.env.ARRAS_MANUAL_TARGET_STOP_DISTANCE || 0.75);
const MANUAL_TARGET_SLOW_DISTANCE = Number(process.env.ARRAS_MANUAL_TARGET_SLOW_DISTANCE || 2.5);
const MANUAL_TARGET_OVERSHOOT = Number(process.env.ARRAS_MANUAL_TARGET_OVERSHOOT || 0);
const MANUAL_TARGET_MIN_HOLD_MS = Number(process.env.ARRAS_MANUAL_TARGET_MIN_HOLD_MS || 2500);
const TRUST_ESTIMATED_MANUAL_ARRIVAL = process.env.ARRAS_TRUST_ESTIMATED_MANUAL_ARRIVAL === '1';
const MANUAL_TARGET_INVERT = process.env.ARRAS_MANUAL_TARGET_INVERT === '1';
const MANUAL_TARGET_ESTIMATED_SPEED = Number(process.env.ARRAS_MANUAL_TARGET_ESTIMATED_SPEED || 7);
const U_POSITION_MODE = String(process.env.ARRAS_USE_U_POSITION || 'filtered').toLowerCase();
const USE_U_POSITION = !['0', 'false', 'off', 'none'].includes(U_POSITION_MODE);
const TRUST_RAW_U_POSITION = ['1', 'true', 'raw', 'on'].includes(U_POSITION_MODE);
const U_POSITION_ACCEPT_DISTANCE = Number(process.env.ARRAS_U_POSITION_ACCEPT_DISTANCE || 80);
const U_POSITION_BLEND = Number(process.env.ARRAS_U_POSITION_BLEND || 0.18);
const MANUAL_TARGET_X_SCALE = 30;
const MANUAL_TARGET_Y_SCALE = 30;
const MANUAL_SCALE_AUTO_CALIBRATE = process.env.ARRAS_MANUAL_SCALE_AUTO_CALIBRATE === '1';
const MANUAL_SCALE_CALIBRATE_DISTANCE = Number(process.env.ARRAS_MANUAL_SCALE_CALIBRATE_DISTANCE || 80);
const MANUAL_SCALE_MIN = Number(process.env.ARRAS_MANUAL_SCALE_MIN || 20);
const MANUAL_SCALE_MAX = Number(process.env.ARRAS_MANUAL_SCALE_MAX || 60);
const BOT_POSITION_X_SIGN = Number(process.env.ARRAS_BOT_POSITION_X_SIGN || 1);
const BOT_POSITION_Y_SIGN = Number(process.env.ARRAS_BOT_POSITION_Y_SIGN || 1);
const U_POSITION_LOG_MS = Number(process.env.ARRAS_U_POSITION_LOG_MS || 1000);
const U_SAMPLE_LIMIT = Number(process.env.ARRAS_U_SAMPLE_LIMIT || 500);
const MOVEMENT_KEY_LEFT = Number(process.env.ARRAS_MOVEMENT_KEY_LEFT || 0x04);
const MOVEMENT_KEY_RIGHT = Number(process.env.ARRAS_MOVEMENT_KEY_RIGHT || 0x08);
const MOVEMENT_KEY_UP = Number(process.env.ARRAS_MOVEMENT_KEY_UP || 0x01);
const MOVEMENT_KEY_DOWN = Number(process.env.ARRAS_MOVEMENT_KEY_DOWN || 0x02);
const FIRE_INPUT_FLAG = 0x10;
const AUTOFIRE_TOGGLE_PACKET = Buffer.from([0x74, 0x00]);
const UPGRADE_CLICK_COORDS = {
  1: 50,
  2: 90,
  3: 120,
  4: 180
};
const UPGRADE_CLICK_RELEASE_MS = 80;
const EARLY_RETRY_MAX = Number(process.env.ARRAS_PROTOCOL_EARLY_RETRY_MAX || 4);
const EARLY_RETRY_DELAY_MS = Number(process.env.ARRAS_PROTOCOL_EARLY_RETRY_DELAY_MS || 700);
const AUTO_RESPAWN_ENABLED = process.env.ARRAS_PROTOCOL_AUTO_RESPAWN !== '0';
const AUTO_RESPAWN_MODE = String(process.env.ARRAS_PROTOCOL_AUTO_RESPAWN_MODE || 'reconnect').toLowerCase();
const AUTO_RESPAWN_DELAY_MS = Number(process.env.ARRAS_PROTOCOL_AUTO_RESPAWN_DELAY_MS || 900);
const AUTO_RESPAWN_NO_POSITION_MS = Number(process.env.ARRAS_PROTOCOL_NO_POSITION_RESPAWN_MS || 0);
const POSITION_CHANGE_EPSILON = Number(process.env.ARRAS_PROTOCOL_POSITION_EPSILON || 0.05);
const AUTO_RESPAWN_WATCH_MS = Number(process.env.ARRAS_PROTOCOL_RESPAWN_WATCH_MS || 1000);
const FEED_STAT_BUILD = [0, 0, 12, 0, 0, 0, 0, 8];
const VALIDATION_R_ALPHABET = Array.from({ length: 64 }, (_, i) => String.fromCharCode(0x30 + i));
const VALIDATION_R_HASH_PREFIX = process.env.ARRAS_R_HASH_PREFIX || '0000';
const VALIDATION_R_MAX_ATTEMPTS = Number(process.env.ARRAS_R_MAX_ATTEMPTS || 1_000_000);
let httpsProxyAgentCtorPromise = null;
let nextClientLogId = 1;

const P25519 = (1n << 255n) - 19n;

function modP(value) {
  const reduced = value % P25519;
  return reduced >= 0n ? reduced : reduced + P25519;
}

function powP(base, exponent) {
  let x = modP(base);
  let e = exponent;
  let result = 1n;
  while (e > 0n) {
    if (e & 1n) {
      result = modP(result * x);
    }
    x = modP(x * x);
    e >>= 1n;
  }
  return result;
}

function invP(value) {
  return powP(value, P25519 - 2n);
}

function clampScalar(bytes) {
  const scalar = Buffer.from(bytes);
  scalar[0] &= 248;
  scalar[31] &= 127;
  scalar[31] |= 64;
  return scalar;
}

function bytesToBigIntLE(bytes, clearHighBit = false) {
  const copy = Buffer.from(bytes);
  if (clearHighBit) {
    copy[31] &= 127;
  }
  let value = 0n;
  for (let i = copy.length - 1; i >= 0; i--) {
    value = (value << 8n) | BigInt(copy[i]);
  }
  return value;
}

function bigIntToBytesLE(value, length) {
  let x = value;
  const out = Buffer.alloc(length);
  for (let i = 0; i < length; i++) {
    out[i] = Number(x & 0xffn);
    x >>= 8n;
  }
  return out;
}

function derivePartyFromHash(hash) {
  const normalized = String(hash || '').replace(/^#/, '').trim();
  return normalized.match(/\d+$/)?.[0] || '';
}

function findBalancedObjectSource(source, startIndex) {
  const openIndex = source.indexOf('{', startIndex);
  if (openIndex < 0) {
    return '';
  }
  let depth = 0;
  let quote = '';
  let escaped = false;
  for (let i = openIndex; i < source.length; i++) {
    const ch = source[i];
    if (quote) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === quote) {
        quote = '';
      }
      continue;
    }
    if (ch === '"' || ch === "'" || ch === '`') {
      quote = ch;
      continue;
    }
    if (ch === '{') {
      depth++;
    } else if (ch === '}') {
      depth--;
      if (depth === 0) {
        return source.slice(openIndex, i + 1);
      }
    }
  }
  return '';
}

function loadHeadlessTankDefinitions() {
  const indexPath = path.join(__dirname, 'arras-headless', 'index.js');
  try {
    const source = fs.readFileSync(indexPath, 'utf8');
    const buildsIndex = source.indexOf('const builds =');
    const tanksIndex = source.indexOf('const tanks =');
    const buildsSource = findBalancedObjectSource(source, buildsIndex);
    const tanksSource = findBalancedObjectSource(source, tanksIndex);
    if (!buildsSource || !tanksSource) {
      return {};
    }
    const loader = new Function(`"use strict"; const builds = ${buildsSource}; return ${tanksSource};`);
    return loader() || {};
  } catch {
    return {};
  }
}

const TANK_DEFINITIONS = loadHeadlessTankDefinitions();
const CLASS_UPGRADE_KEY_INDEX = {
  y: 0,
  u: 1,
  i: 2,
  h: 3,
  j: 4,
  k: 5
};

function normalizeTankName(tank) {
  return String(tank || 'basic').trim().toLowerCase().replace(/[^a-z0-9_]/g, '');
}

function tankPathFor(tank) {
  const def = TANK_DEFINITIONS[normalizeTankName(tank)];
  if (!def) {
    return [];
  }
  if (Array.isArray(def.path)) {
    return def.path.slice();
  }
  if (typeof def.path === 'string') {
    return [...def.path];
  }
  return [];
}

function tankBuildFor(tank) {
  const def = TANK_DEFINITIONS[normalizeTankName(tank)];
  if (!def || typeof def.build !== 'string') {
    return [];
  }
  return def.build
    .split('/')
    .map((value) => Number.parseInt(value, 10))
    .map((value) => Number.isFinite(value) ? Math.max(0, value) : 0);
}

function buildStatUpgradeSteps(tank, feedMode = false) {
  const build = feedMode ? FEED_STAT_BUILD : tankBuildFor(tank);
  const steps = [];
  for (let statIndex = 0; statIndex < build.length; statIndex += 1) {
    for (let count = 0; count < build[statIndex]; count += 1) {
      steps.push(statIndex);
    }
  }
  return steps;
}

function summarizeStatUpgradeSteps(steps) {
  const counts = new Map();
  for (const statIndex of steps) {
    counts.set(statIndex, (counts.get(statIndex) || 0) + 1);
  }
  return [...counts.entries()]
    .map(([statIndex, count]) => `${statIndex}x${count}`)
    .join(',');
}

function isDeathMessage(text) {
  return /You have been killed by |You have died(?: a stupid death)?\.?|\bRespawn\b/i.test(String(text || '').trim());
}

function isDeathStateUpdatePacket(plaintext) {
  return deathStateMarkerOffset(plaintext) >= 0;
}

function deathStateMarkerOffset(plaintext) {
  if (!plaintext || plaintext.length < 4 || plaintext[0] !== 0x75) {
    return -1;
  }
  for (let offset = 1; offset + 2 < plaintext.length; offset += 1) {
    if (
      plaintext[offset] === 0xe7 &&
      plaintext[offset + 1] === 0xd0 &&
      plaintext[offset + 2] === 0x00
    ) {
      return offset;
    }
  }
  return -1;
}

function buildClassUpgradePacket(step) {
  const index = CLASS_UPGRADE_KEY_INDEX[String(step || '').toLowerCase()];
  if (!Number.isInteger(index)) {
    return null;
  }
  return Buffer.from([0x55, index & 0xff]);
}

function buildUpgradeClickPackets(step) {
  if (!Array.isArray(step) || step.length < 2) {
    return null;
  }
  const x = UPGRADE_CLICK_COORDS[Number(step[0])];
  const y = UPGRADE_CLICK_COORDS[Number(step[1])];
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    return null;
  }
  const down = buildInputPacket({
    mouseX: x,
    mouseY: y,
    mouseDown: true,
    rMouseDown: false,
    mouse: false,
    autofire: false,
    autospin: false,
    manualMode: false
  });
  const up = buildInputPacket({
    mouseX: x,
    mouseY: y,
    mouseDown: false,
    rMouseDown: false,
    mouse: false,
    autofire: false,
    autospin: false,
    manualMode: false
  });
  return { x, y, down, up };
}

function formatUpgradeStep(step) {
  return Array.isArray(step) ? `[${step.join(',')}]` : String(step);
}

function buildStatUpgradePacket(statIndex) {
  if (!Number.isInteger(statIndex) || statIndex < 0 || statIndex > 9) {
    return null;
  }
  return Buffer.from([0x78, statIndex & 0xff, 0xbf]);
}

function x25519(scalarBytes, uBytes) {
  const a24 = 121665n;
  const k = bytesToBigIntLE(clampScalar(scalarBytes));
  const x1 = modP(bytesToBigIntLE(uBytes, true));
  let x2 = 1n;
  let z2 = 0n;
  let x3 = x1;
  let z3 = 1n;
  let swap = 0n;

  for (let t = 254; t >= 0; t--) {
    const bit = (k >> BigInt(t)) & 1n;
    swap ^= bit;
    if (swap === 1n) {
      [x2, x3] = [x3, x2];
      [z2, z3] = [z3, z2];
    }
    swap = bit;

    const a = modP(x2 + z2);
    const aa = modP(a * a);
    const b = modP(x2 - z2);
    const bb = modP(b * b);
    const e = modP(aa - bb);
    const c = modP(x3 + z3);
    const d = modP(x3 - z3);
    const da = modP(d * a);
    const cb = modP(c * b);

    x3 = modP((da + cb) * (da + cb));
    z3 = modP(x1 * modP((da - cb) * (da - cb)));
    x2 = modP(aa * bb);
    z2 = modP(e * (aa + modP(a24 * e)));
  }

  if (swap === 1n) {
    [x2, x3] = [x3, x2];
    [z2, z3] = [z3, z2];
  }

  return bigIntToBytesLE(modP(x2 * invP(z2)), 32);
}

function x25519Base(scalarBytes) {
  return x25519(scalarBytes, X25519_BASE);
}

function concatBytes(parts) {
  return Buffer.concat(parts.map((part) => Buffer.isBuffer(part) ? part : Buffer.from(part)));
}

function encodeCommand(commandChar, fields) {
  const parts = [Buffer.from([commandChar.charCodeAt(0)])];
  for (const field of fields || []) {
    if (typeof field === 'string') {
      const bytes = Buffer.from(field, 'utf8');
      if (bytes.length <= 15) {
        parts.push(Buffer.from([0xc0 | bytes.length]), bytes);
      } else {
        parts.push(Buffer.from([0xfe, bytes.length & 0xff, (bytes.length >> 8) & 0xff]), bytes);
      }
      continue;
    }
    if (typeof field === 'number') {
      parts.push(Buffer.from([field & 0xff]));
      continue;
    }
    if (field && field.raw) {
      parts.push(Buffer.from(field.raw));
      continue;
    }
    if (Buffer.isBuffer(field) || ArrayBuffer.isView(field)) {
      parts.push(Buffer.from(field));
      continue;
    }
    throw new TypeError(`Unsupported command field: ${field}`);
  }
  return concatBytes(parts);
}

function encodeValidationRNonce(value) {
  let n = value;
  let out = '';
  for (let i = 0; i < 6; i++) {
    out = VALIDATION_R_ALPHABET[n & 63] + out;
    n >>>= 6;
  }
  return out;
}

function computeValidationRValue(token) {
  if (!token) {
    return null;
  }
  for (let attempt = 0; attempt < VALIDATION_R_MAX_ATTEMPTS; attempt++) {
    const value = encodeValidationRNonce(attempt);
    const digest = crypto
      .createHash('sha256')
      .update(value + token, 'latin1')
      .digest('hex');
    if (digest.startsWith(VALIDATION_R_HASH_PREFIX)) {
      return { value, attempts: attempt + 1, digest };
    }
  }
  return null;
}

function buildFingerprintPacket(payload) {
  const body = Buffer.from(
    typeof payload === 'string' ? payload : JSON.stringify(payload),
    'utf8'
  );
  return concatBytes([
    Buffer.from([0x54, 0xfe, body.length & 0xff, (body.length >> 8) & 0xff]),
    body
  ]);
}

function defaultFingerprint() {
  return {
    adblock: false,
    mobile: false,
    storage: {},
    overseer: {
      features: {
        wasm: ['base', 'bigInt', 'bulkMemory', 'multiValue', 'mutableGlobals', 'referenceTypes', 'signExtensions', 'simd', 'streaming'],
        rtc: 'good',
        wt: true,
        sw: true,
        gpu: true,
        credentialless: true,
        ua: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36',
        hc: 16,
        renderer: 'WebKit WebGL\nWebKit\nANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0, D3D11)\nGoogle Inc. (NVIDIA)',
        webgl: 'good',
        'experimental-webgl': 'good',
        webgl2: 'good'
      },
      window: {
        innerWidth: 1280,
        innerHeight: 720
      },
      fingerprints: {
        canvas: 'v0:wP4jk0qKgbokGbFmMdPWwurUW+6kNPmMMCfOWbjN4/M=',
        unicode: 'v0:UmVElDEuBHKmqHEiZPEkdMU6Gfj2grq241pseekLrPQ='
      },
      report: [
        'window.addEventListener = function addEventListener() { [native code] }',
        'canvas.addEventListener = function addEventListener() { [native code] }',
        'WebAssembly.instantiate = function instantiate() { [native code] }',
        'WebAssembly.instantiateStreaming = function instantiateStreaming() { [native code] }',
        'requestAnimationFrame = function requestAnimationFrame() { [native code] }',
        'Function = function Function() { [native code] }'
      ].join('\n')
    }
  };
}

function buildOpenPacket(socketUrl) {
  const url = new URL(socketUrl);
  const token = url.searchParams.get('b');
  if (!token || !/^[0-9a-fA-F]{16,}$/.test(token)) {
    throw new Error('Socket URL is missing a valid ?b= token');
  }
  const packet = Buffer.alloc(12);
  packet.set([0x00, 0x01, 0x00, 0x01], 0);
  let value = BigInt(`0x${token.slice(0, 16)}`);
  for (let i = 0; i < 8; i++) {
    packet[4 + i] = Number(value & 0xffn);
    value >>= 8n;
  }
  return packet;
}

function verifyEd25519(message, signature, publicKey) {
  try {
    const spkiPrefix = Buffer.from('302a300506032b6570032100', 'hex');
    const keyObject = crypto.createPublicKey({
      key: concatBytes([spkiPrefix, Buffer.from(publicKey)]),
      format: 'der',
      type: 'spki'
    });
    return crypto.verify(null, Buffer.from(message), keyObject, Buffer.from(signature));
  } catch {
    return true;
  }
}

function latestSocketUrlFromLog() {
  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson'),
    path.join(__dirname, 'latest-socket-url.txt'),
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      if (filePath.endsWith('latest-socket-url.txt')) {
        const url = fs.readFileSync(filePath, 'utf8').trim();
        if (url.startsWith('wss://')) {
          return url;
        }
        continue;
      }
      const text = fs.readFileSync(filePath, 'utf8').trim();
      if (!text) {
        continue;
      }
      const lines = text.split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        const url = row && row.meta && row.meta.url;
        if (typeof url === 'string' && url.startsWith('wss://')) {
          return url;
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

async function getHttpsProxyAgentCtor() {
  if (!httpsProxyAgentCtorPromise) {
    httpsProxyAgentCtorPromise = import('https-proxy-agent')
      .then((mod) => mod.HttpsProxyAgent);
  }
  return httpsProxyAgentCtorPromise;
}

async function buildProxyAgent() {
  const proxyUrl = process.env.ARRAS_PROXY_URL || DEFAULT_PROXY_URL;
  // Empty / 'none' / 'direct' means connect without an HTTP proxy. The old
  // default decodo proxy is dead (HTTP 407), so direct is the safe default.
  if (!proxyUrl || proxyUrl === 'none' || proxyUrl === 'direct') {
    return null;
  }
  try {
    const HttpsProxyAgent = await getHttpsProxyAgentCtor();
    return {
      url: proxyUrl,
      agent: new HttpsProxyAgent(proxyUrl)
    };
  } catch (error) {
    console.warn(`Proxy setup failed: ${error.message}`);
    return null;
  }
}

function latestSocketMetaFromLog(socketUrl) {
  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson'),
    path.join(__dirname, 'validation-events.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const text = fs.readFileSync(filePath, 'utf8').trim();
      if (!text) {
        continue;
      }
      const lines = text.split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (row && row.meta && row.meta.url === socketUrl) {
          return {
            hash: typeof row.meta.hash === 'string' ? row.meta.hash : null,
            squadId: typeof row.meta.squadId === 'string' ? row.meta.squadId : null,
            url: row.meta.url
          };
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function latestValidationRValueFromLog(expectedHash = null) {
  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson'),
    path.join(__dirname, 'validation-events.ndjson')
  ].filter(Boolean);

  const valuePattern = /00[!-~]{4}/g;
  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (!row) {
          continue;
        }
        const rowHash =
          (row.meta && typeof row.meta.hash === 'string' && row.meta.hash) ||
          (typeof row.hash === 'string' && row.hash) ||
          null;
        if (expectedHash && rowHash !== expectedHash) {
          continue;
        }
        if (row.direction && row.direction !== 'OUT') {
          continue;
        }
        const plainHex =
          (row.plain && (!row.plain.command || row.plain.command === 'R') && typeof row.plain.hex === 'string' && row.plain.hex) ||
          (row.meta && row.meta.wasmPlain && typeof row.meta.wasmPlain.hex === 'string' && row.meta.wasmPlain.hex) ||
          null;
        if (plainHex) {
          const decoded = decodeValidationTemplate(plainHex);
          if (decoded && decoded.bytes && decoded.bytes[0] === 0x52 && typeof decoded.value === 'string') {
            return decoded.value;
          }
        }
        if (row.bytes !== 49) {
          continue;
        }
        const memoryWindow = row.meta && row.meta.wasmSend && row.meta.wasmSend.memoryWindow;
        const windowHex = memoryWindow && memoryWindow.hex;
        if (!windowHex || !Number.isInteger(memoryWindow.before)) {
          continue;
        }
        const runs = extractAsciiRunsWithOffsets(Buffer.from(windowHex, 'hex'), 5)
          .map((item) => ({ relOffset: item.offset - memoryWindow.before, text: item.text }))
          .filter((item) => item.relOffset < 0 && item.relOffset >= -512);
        for (let j = runs.length - 1; j >= 0; j--) {
          const matches = runs[j].text.match(valuePattern);
          if (matches && matches.length) {
            return matches[matches.length - 1];
          }
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function latestValidationTokenFromLog(packetSizes, expectedHash = null) {
  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (!row || row.direction !== 'OUT' || !packetSizes.includes(row.bytes)) {
          continue;
        }
        if (expectedHash && row.meta && row.meta.hash !== expectedHash) {
          continue;
        }
        const memoryWindow = row.meta && row.meta.wasmSend && row.meta.wasmSend.memoryWindow;
        const windowHex = memoryWindow && memoryWindow.hex;
        if (!windowHex || !Number.isInteger(memoryWindow.before)) {
          continue;
        }
        const runs = extractAsciiRunsWithOffsets(Buffer.from(windowHex, 'hex'), 8)
          .map((item) => ({ relOffset: item.offset - memoryWindow.before, text: item.text }))
          .filter((item) => item.relOffset < 0 && item.relOffset >= -160);
        for (let j = runs.length - 1; j >= 0; j--) {
          const match = runs[j].text.match(/[A-Za-z0-9+\/\\`\[\]\^_=><:;?@!-]{32,}/);
          if (match) {
            return match[0].slice(0, 32);
          }
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function latestValidationEAnswerFromLog(expectedHash = null) {
  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (!row || row.direction !== 'OUT' || (row.bytes !== 52 && row.bytes !== 53 && row.bytes !== 54)) {
          continue;
        }
        if (expectedHash && row.meta && row.meta.hash !== expectedHash) {
          continue;
        }
        const memoryWindow = row.meta && row.meta.wasmSend && row.meta.wasmSend.memoryWindow;
        const windowHex = memoryWindow && memoryWindow.hex;
        if (!windowHex || !Number.isInteger(memoryWindow.before)) {
          continue;
        }
        const asciiRuns = extractAsciiRunsWithOffsets(Buffer.from(windowHex, 'hex'), 6)
          .map((item) => ({ relOffset: item.offset - memoryWindow.before, text: item.text }))
          .filter((item) => item.relOffset < 0 && item.relOffset >= -512);
        const candidates = [];
        for (let j = 0; j < asciiRuns.length; j++) {
          const matches = Array.from(asciiRuns[j].text.matchAll(/-?\d{6,12}/g));
          for (const match of matches) {
            candidates.push({
              value: match[0],
              relOffset: asciiRuns[j].relOffset
            });
          }
        }
        if (candidates.length) {
          candidates.sort((a, b) => b.relOffset - a.relOffset);
          return candidates[0].value;
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function decodeLengthPrefixedString(buffer, offset) {
  const tag = buffer[offset];
  if ((tag & 0xf0) === 0xc0) {
    const length = tag & 0x0f;
    const start = offset + 1;
    const end = start + length;
    return {
      value: buffer.toString('utf8', start, end),
      nextOffset: end
    };
  }
  if (tag === 0xfe) {
    const length = buffer[offset + 1] | (buffer[offset + 2] << 8);
    const start = offset + 3;
    const end = start + length;
    return {
      value: buffer.toString('utf8', start, end),
      nextOffset: end
    };
  }
  return null;
}

function decodeValidationTemplate(hex) {
  const bytes = Buffer.from(hex, 'hex');
  if (bytes.length < 2) {
    return null;
  }
  let offset = 1;
  const tokenField = decodeLengthPrefixedString(bytes, offset);
  if (!tokenField) {
    return null;
  }
  offset = tokenField.nextOffset;
  const valueField = decodeLengthPrefixedString(bytes, offset);
  if (!valueField) {
    return null;
  }
  offset = valueField.nextOffset;
  return {
    token: tokenField.value,
    value: valueField.value,
    tail: bytes.subarray(offset),
    bytes
  };
}

function latestCdpPlainCommand(commandChar) {
  const filePath = path.join(__dirname, 'cdp-browser-decrypt.ndjson');
  try {
    const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
    for (let i = lines.length - 1; i >= 0; i--) {
      const row = JSON.parse(lines[i]);
      if (
        row &&
        row.direction === 'OUT' &&
        row.command === commandChar &&
        typeof row.hex === 'string' &&
        row.hex.startsWith(commandChar.charCodeAt(0).toString(16))
      ) {
        const packet = Buffer.from(row.hex, 'hex');
        if (commandChar === 'T' && isInstrumentedFingerprintPacket(packet)) {
          continue;
        }
        return packet;
      }
    }
  } catch {
    // Ignore missing or malformed CDP decrypt logs.
  }
  return null;
}

function isInstrumentedFingerprintPacket(packet) {
  if (process.env.ARRAS_ALLOW_CAPTURED_T === '1') {
    return false;
  }
  if (!Buffer.isBuffer(packet) || packet[0] !== 0x54) {
    return false;
  }
  const text = packet.subarray(4).toString('utf8');
  return /patchedInstantiate|patchedInstantiateStreaming|__arrasCapture|playwright|protocol-only-random-client/i.test(text);
}

function latestValidationTemplateFromEvents(command, expectedHash = null) {
  const cdpPlain = latestCdpPlainCommand(command);
  if (cdpPlain) {
    return decodeValidationTemplate(cdpPlain.toString('hex'));
  }

  const filePath = path.join(__dirname, 'validation-events.ndjson');
  try {
    const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
    for (let i = lines.length - 1; i >= 0; i--) {
      const row = JSON.parse(lines[i]);
      if (!row || !row.plain || row.plain.command !== command || typeof row.plain.hex !== 'string') {
        continue;
      }
      if (expectedHash && row.hash !== expectedHash) {
        continue;
      }
      return decodeValidationTemplate(row.plain.hex);
    }
  } catch {
    // Ignore missing or malformed capture logs.
  }
  return null;
}

function isPlausibleSpawnTemplate(buffer) {
  if (!Buffer.isBuffer(buffer) || buffer.length < 4 || buffer[0] !== 0x73) {
    return false;
  }
  const asciiRuns = extractAsciiRuns(buffer.subarray(1), 3);
  return !asciiRuns.some((run) => /[A-Za-z]{2,}/.test(run));
}

function latestSpawnTemplateFromLog(expectedHash = null, options = {}) {
  const cdpSpawn = latestCdpPlainCommand('s');
  if (cdpSpawn && (options.allowAsciiPayload || isPlausibleSpawnTemplate(cdpSpawn))) {
    return cdpSpawn;
  }

  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (!row || row.direction !== 'OUT') {
          continue;
        }
        if (expectedHash && row.meta && row.meta.hash !== expectedHash) {
          continue;
        }
        const plainHex = row.meta && row.meta.wasmPlain && row.meta.wasmPlain.hex;
        if (!plainHex || !plainHex.startsWith('73')) {
          continue;
        }
        const candidate = Buffer.from(plainHex, 'hex');
        if (!options.allowAsciiPayload && !isPlausibleSpawnTemplate(candidate)) {
          continue;
        }
        return candidate;
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function bestSpawnTemplateFromLog(expectedHash = null) {
  return (
    (expectedHash ? latestSpawnTemplateFromLog(expectedHash, { allowAsciiPayload: ALLOW_ASCII_SPAWN_TEMPLATE }) : null) ||
    latestSpawnTemplateFromLog(null)
  );
}

function explicitSpawnTemplateFromEnv() {
  const hex = process.env.ARRAS_SPAWN_HEX;
  if (!hex) {
    return null;
  }
  const clean = hex.replace(/[^0-9a-f]/gi, '');
  if (!clean || clean.length % 2 !== 0) {
    throw new Error('ARRAS_SPAWN_HEX must contain an even number of hex digits');
  }
  return Buffer.from(clean, 'hex');
}

// Real handshake packets captured from the live vm game client
// (see server.js 'out_packets' handler -> latest-captured-packets.json).
function capturedVmPackets() {
  try {
    const raw = fs.readFileSync(path.join(__dirname, 'latest-captured-packets.json'), 'utf8');
    const data = JSON.parse(raw);
    if (!data || !Array.isArray(data.packets)) return [];
    return data.packets
      .filter((h) => typeof h === 'string' && h.length >= 2)
      .map((h) => Buffer.from(h, 'hex'))
      .filter((b) => b.length > 0);
  } catch {
    return [];
  }
}

function capturedHelloTemplate() {
  const packets = capturedVmPackets();
  // Hello is the 'k' command (0x6b) or the first non-empty packet.
  const kPacket = packets.find((b) => b[0] === 0x6b);
  return kPacket || packets[0] || null;
}

function capturedFingerprintTemplate() {
  const packets = capturedVmPackets();
  // Fingerprint is the 'T' command (0x54).
  return packets.find((b) => b[0] === 0x54) || null;
}

function buildDefaultSpawnPacket(party = '') {
  const normalizedParty = String(party || '').trim();
  if (!normalizedParty) {
    return DEFAULT_SPAWN_PACKET;
  }
  return encodeCommand('s', ['', normalizedParty, 1]);
}

function latestCommandTemplateFromLog(commandChar, expectedHash = null) {
  const cdpPlain = latestCdpPlainCommand(commandChar);
  if (cdpPlain) {
    return cdpPlain;
  }

  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'wasm-plain-packets.ndjson'),
    path.join(__dirname, 'protocol-packets.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = lines.length - 1; i >= 0; i--) {
        const row = JSON.parse(lines[i]);
        if (!row) {
          continue;
        }
        const rowHash = row.hash || (row.meta && row.meta.hash);
        if (expectedHash && rowHash && rowHash !== expectedHash) {
          continue;
        }
        const plain = row.plain || (row.meta && row.meta.wasmPlain);
        const plainHex = plain && plain.hex;
        const command = plain && plain.command;
        const isOutbound = row.direction === 'OUT' || !!row.plain;
        if (!isOutbound) {
          continue;
        }
        if (command && command !== commandChar) {
          continue;
        }
        if (!plainHex || !plainHex.startsWith(commandChar.charCodeAt(0).toString(16))) {
          continue;
        }
        const packet = Buffer.from(plainHex, 'hex');
        if (commandChar === 'T' && isInstrumentedFingerprintPacket(packet)) {
          continue;
        }
        return packet;
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function latestPingTemplateFromLog(expectedHash = null) {
  const cdpPing = latestCdpPlainCommand('p');
  if (cdpPing) {
    return cdpPing;
  }

  const candidates = [
    process.env.ARRAS_SOCKET_LOG,
    path.join(__dirname, 'protocol-packets.ndjson')
  ].filter(Boolean);

  for (const filePath of candidates) {
    try {
      const lines = fs.readFileSync(filePath, 'utf8').trim().split(/\r?\n/);
      for (let i = 0; i < lines.length; i++) {
        const row = JSON.parse(lines[i]);
        if (!row || row.direction !== 'OUT') {
          continue;
        }
        if (expectedHash && row.meta && row.meta.hash !== expectedHash) {
          continue;
        }
        const plainHex = row.meta && row.meta.wasmPlain && row.meta.wasmPlain.hex;
        if (!plainHex) {
          continue;
        }
        if (plainHex.startsWith('73')) {
          for (let j = i + 1; j < lines.length; j++) {
            const nextRow = JSON.parse(lines[j]);
            if (!nextRow || nextRow.direction !== 'OUT') {
              continue;
            }
            if (expectedHash && nextRow.meta && nextRow.meta.hash !== expectedHash) {
              continue;
            }
            const nextHex = nextRow.meta && nextRow.meta.wasmPlain && nextRow.meta.wasmPlain.hex;
            if (nextHex && nextHex.startsWith('70') && nextHex.length === 14) {
              return Buffer.from(nextHex, 'hex');
            }
            if (nextHex && !nextHex.startsWith('70')) {
              break;
            }
          }
        }
      }
    } catch {
      // Try the next candidate.
    }
  }
  return null;
}

function writeDebugArtifact(name, content) {
  try {
    fs.writeFileSync(path.join(__dirname, name), String(content), 'utf8');
  } catch (error) {
    console.error(`Failed to write ${name}:`, error.message);
  }
}

function inboundSequence(counter) {
  const sequence = Buffer.alloc(8);
  sequence.writeBigUInt64LE(BigInt(counter));
  sequence[7] |= 0x80;
  return sequence;
}

function printableCommand(byte) {
  return byte >= 0x20 && byte <= 0x7e ? String.fromCharCode(byte) : null;
}

function hexPrefix(buffer, bytes = 24) {
  return Buffer.from(buffer).subarray(0, bytes).toString('hex');
}

function extractAsciiRunsWithOffsets(buffer, minLength = 4) {
  const runs = [];
  let start = -1;
  for (let i = 0; i < buffer.length; i++) {
    const byte = buffer[i];
    const printable = byte >= 0x20 && byte <= 0x7e;
    if (printable) {
      if (start === -1) {
        start = i;
      }
      continue;
    }
    if (start !== -1 && i - start >= minLength) {
      runs.push({ offset: start, text: buffer.toString('utf8', start, i) });
    }
    start = -1;
  }
  if (start !== -1 && buffer.length - start >= minLength) {
    runs.push({ offset: start, text: buffer.toString('utf8', start) });
  }
  return runs;
}

function extractAsciiRuns(buffer, minLength = 4) {
  return extractAsciiRunsWithOffsets(buffer, minLength).map((item) => item.text);
}

function computeEvalChallengeAnswer(expressionSource, envMode = 0) {
  let expr = String(expressionSource || '');
  if (expr.startsWith('{')) {
    expr = expr.slice(1);
  }
  const letIndex = expr.indexOf('let ');
  if (letIndex > 0 && letIndex <= 8) {
    expr = expr.slice(letIndex);
  }
  expr = expr.replace(/^[^\w$]*(?:[A-Za-z]\[?)?let\s+/, 'let ');
  const navigatorObject = { webdriver: envMode === 2 };
  const chromeObject = envMode === 3
    ? {
      runtime: {}
    }
    : {};
  if (envMode === 3) {
    Object.defineProperty(chromeObject.runtime, 'id', {
      configurable: true,
      enumerable: true,
      get() {
        return 'shim';
      }
    });
    chromeObject.runtime.__lookupGetter__ = undefined;
  }
  const processValue = envMode === 4 ? { versions: { node: process.versions.node } } : undefined;
  return Function('navigator', 'chrome', 'process', expr)(navigatorObject, chromeObject, processValue);
}

function decodeEvalChallenge(plaintext) {
  const bytes = Buffer.from(plaintext);
  const tokenField = decodeLengthPrefixedString(bytes, 1);
  if (!tokenField) {
    return null;
  }
  const bodyField = decodeLengthPrefixedString(bytes, tokenField.nextOffset);
  if (!bodyField) {
    return null;
  }
  const token = tokenField.value;
  const body = bodyField.value;
  const answers = {};
  for (const envMode of [0, 1, 2, 3, 4]) {
    try {
      answers[envMode] = computeEvalChallengeAnswer(body, envMode);
    } catch (error) {
      answers[envMode] = `error:${error.message}`;
    }
  }
  return {
    token,
    body,
    answers,
    evalEnvMode: EVAL_ENV_MODE,
    browserLikeAnswer: answers[EVAL_ENV_MODE],
    browserLikeString: `${token}${answers[EVAL_ENV_MODE]}`
  };
}

function extractValidationToken(plaintext) {
  const asciiRuns = extractAsciiRuns(Buffer.from(plaintext));
  return asciiRuns.find((entry) => entry.length >= 16) || null;
}

function packetHint(command, plaintext) {
  const asciiRuns = extractAsciiRuns(Buffer.from(plaintext));
  if (command === 'R') {
    const modeText = asciiRuns.find((entry) => entry.includes('mode='));
    return modeText ? `info=${JSON.stringify(modeText)}` : '';
  }
  if (command === 'J') {
    const labels = asciiRuns.filter((entry) => /[A-Za-z]/.test(entry)).slice(0, 3);
    return labels.length ? `labels=${JSON.stringify(labels)}` : '';
  }
  if (command === 'e') {
    const token = asciiRuns[0] || '';
    const body = asciiRuns.slice(1).join('');
    return token ? `token=${JSON.stringify(token)} bodyChars=${body.length}` : '';
  }
  if (command === 'w' || command === 'C') {
    const text = asciiRuns.slice(0, 1);
    return text.length ? `text=${JSON.stringify(text)}` : '';
  }
  if (command === 'K') {
    const text = asciiRuns.slice(0, 2);
    return text.length ? `text=${JSON.stringify(text)}` : '';
  }
  if (command === 'b') {
    if (plaintext.length === 7 && plaintext.subarray(1).every((byte) => byte === 0)) {
      return 'marker=all-zero-body';
    }
    return `hex2=${hexPrefix(plaintext.subarray(1), 18)}`;
  }
  return '';
}

function parseModeInfo(plaintext) {
  const asciiRuns = extractAsciiRuns(Buffer.from(plaintext));
  const modeText = asciiRuns.find((entry) => entry.includes('mode='));
  if (!modeText) {
    return null;
  }
  const info = {};
  for (const piece of modeText.split(',')) {
    const [key, value] = piece.split('=');
    if (!key || value == null) {
      continue;
    }
    info[key.trim()] = value.trim();
  }
  return Object.keys(info).length ? info : null;
}

function summarizePacket(direction, packetIndex, plaintext) {
  const command = printableCommand(plaintext[0]);
  const label = command ? `cmd=${command}` : `byte0=0x${plaintext[0].toString(16).padStart(2, '0')}`;
  const hint = command ? packetHint(command, plaintext) : '';
  return `[${direction} ${packetIndex}] len=${plaintext.length} ${label}${hint ? ` ${hint}` : ''} hex=${hexPrefix(plaintext)}`;
}

function appendNdjson(fileName, record) {
  try {
    fs.appendFileSync(path.join(__dirname, fileName), `${JSON.stringify(record)}\n`);
  } catch (error) {
    console.error(`Failed to append ${fileName}:`, error.message);
  }
}

function packetRecord(direction, packetIndex, plaintext, extra = {}) {
  const command = printableCommand(plaintext[0]);
  return Object.assign({
    ts: new Date().toISOString(),
    direction,
    packetIndex,
    bytes: plaintext.length,
    command,
    hex: Buffer.from(plaintext).toString('hex'),
    asciiRuns: extractAsciiRuns(Buffer.from(plaintext))
  }, extra);
}

function summarizePacketForValidation(bytes, packetIndex, extra = {}) {
  return Object.assign({
    packetIndex,
    bytes: bytes.length,
    hex: Buffer.from(bytes).toString('hex'),
    command: printableCommand(bytes[0]),
    asciiRuns: extractAsciiRuns(Buffer.from(bytes))
  }, extra);
}

function buildValidationPacket(commandChar, token, value, rawTail) {
  const fields = [token, value];
  if (rawTail && rawTail.length) {
    fields.push({ raw: rawTail });
  }
  return encodeCommand(commandChar, fields);
}

function clampByte(value, fallback = 0) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return fallback;
  }
  return Math.max(0, Math.min(255, Math.round(numeric)));
}

function encodeInputNumber(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return Buffer.from([0]);
  }
  const rounded = Math.max(-255, Math.min(255, Math.round(numeric)));
  if (rounded >= -64 && rounded <= 127) {
    return Buffer.from([rounded < 0 ? 0xc0 + rounded : rounded]);
  }
  if (rounded >= 128) {
    return Buffer.from([0xe0, rounded & 0xff]);
  }
  return Buffer.from([0xef, (-rounded) & 0xff]);
}

function clampInputAxis(value, maxAxis = 127) {
  const numeric = Number(value);
  const limit = Math.max(0, Math.min(127, Number(maxAxis) || 0));
  if (!Number.isFinite(numeric)) {
    return 0;
  }
  return Math.max(-limit, Math.min(limit, numeric));
}

function sanitizeControlAimAxis(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || Math.abs(numeric) < CONTROL_AIM_DEADZONE) {
    return 0;
  }
  return clampInputAxis(numeric, CONTROL_AIM_MAX_AXIS);
}

function randomUnit() {
  try {
    return crypto.randomInt(0, 1_000_000) / 1_000_000;
  } catch (err) {
    return Math.random();
  }
}

function randomBetween(min, max) {
  const low = Number(min);
  const high = Number(max);
  if (!Number.isFinite(low) || !Number.isFinite(high)) {
    return 0;
  }
  if (high <= low) {
    return low;
  }
  return low + randomUnit() * (high - low);
}

function randomDiskPoint(radius) {
  const limit = Math.max(0, Number(radius) || 0);
  if (limit <= 0) {
    return { x: 0, y: 0 };
  }
  const angle = randomUnit() * Math.PI * 2;
  const distance = Math.sqrt(randomUnit()) * limit;
  return {
    x: Math.cos(angle) * distance,
    y: Math.sin(angle) * distance
  };
}

function randomAimVector(axis) {
  const limit = Math.max(1, Number(axis) || 1);
  const angle = randomUnit() * Math.PI * 2;
  return {
    x: Math.cos(angle) * limit,
    y: Math.sin(angle) * limit
  };
}

function stableHashNumber(text) {
  const hash = crypto.createHash('sha256').update(String(text || '')).digest();
  return hash.readUInt32LE(0);
}

function stableFormationOffset(id, radius) {
  const limit = Math.max(0, Number(radius) || 0);
  if (limit <= 0) {
    return { x: 0, y: 0 };
  }
  const hash = stableHashNumber(id);
  const angle = (hash % 360) / 360 * Math.PI * 2;
  const ring = 0.65 + ((hash >>> 9) % 35) / 100;
  return {
    x: Math.cos(angle) * limit * ring,
    y: Math.sin(angle) * limit * ring
  };
}

function resolveManualScale(value, fallback) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric) || Math.abs(numeric) < 1) {
    return fallback;
  }
  return numeric;
}

function clampManualScale(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  return Math.max(MANUAL_SCALE_MIN, Math.min(MANUAL_SCALE_MAX, numeric));
}

function usableManualScaleSample(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) {
    return null;
  }
  if (numeric <= MANUAL_SCALE_MIN || numeric >= MANUAL_SCALE_MAX) {
    return null;
  }
  return numeric;
}

function decodeRawFloatPosition(plaintext, commandByte) {
  if (!plaintext || plaintext.length < 11 || plaintext[0] !== commandByte) {
    return null;
  }
  if (plaintext[1] !== 0xff || plaintext[6] !== 0xff) {
    return null;
  }
  const x = BOT_POSITION_X_SIGN * plaintext.readFloatLE(2);
  const y = BOT_POSITION_Y_SIGN * plaintext.readFloatLE(7);
  if (!Number.isFinite(x) || !Number.isFinite(y)) {
    return null;
  }
  return { x, y };
}

function decodePositionPacket(plaintext) {
  return decodeRawFloatPosition(plaintext, 0x63);
}

function decodeUpdatePositionPacket(plaintext) {
  return decodeRawFloatPosition(plaintext, 0x75);
}

function buildInputPacket(controlState, botPosition = null) {
  const manualMode = Boolean(controlState && controlState.manualMode);
  const ignoreControllerAim = Boolean(controlState && controlState.ignoreControllerAim);
  let mouseX = controlState && Number.isFinite(controlState.mouseX) ? controlState.mouseX : 0;
  let mouseY = controlState && Number.isFinite(controlState.mouseY) ? controlState.mouseY : 0;
  if (ignoreControllerAim) {
    mouseX = clampInputAxis(Number(controlState.manualFallbackAimX) || 0);
    mouseY = clampInputAxis(Number(controlState.manualFallbackAimY) || 0);
  }
  let movementFlags = 0;
  let manualDebug = null;
  if (manualMode && ignoreControllerAim && controlState && controlState.manualAimWithMovement !== false) {
    mouseX = clampInputAxis(Number(controlState.manualFallbackAimX) || 0);
    mouseY = clampInputAxis(Number(controlState.manualFallbackAimY) || 0);
  }
  if (
    controlState &&
    manualMode &&
    botPosition &&
    Number.isFinite(controlState.manualX) &&
    Number.isFinite(controlState.manualY)
  ) {
    const manualScaleX = resolveManualScale(controlState.calibratedManualScaleX ?? controlState.manualScaleX, MANUAL_TARGET_X_SCALE);
    const manualScaleY = resolveManualScale(controlState.calibratedManualScaleY ?? controlState.manualScaleY, MANUAL_TARGET_Y_SCALE);
    const targetX = controlState.manualX * manualScaleX;
    const targetY = controlState.manualY * manualScaleY;
    const targetScale = Math.max(Math.abs(manualScaleX), Math.abs(manualScaleY), 1);
    const stopDistance = MANUAL_TARGET_STOP_DISTANCE * targetScale;
    const slowDistance = Math.max(stopDistance + 1, MANUAL_TARGET_SLOW_DISTANCE * targetScale);
    const axisThreshold = Math.max(1, stopDistance * 0.5);
    const rawDx = targetX - botPosition.x;
    const rawDy = targetY - botPosition.y;
    const rawDistance = Math.hypot(rawDx, rawDy);
    const positionIsEstimated = Boolean(botPosition.estimated);
    if (rawDistance > stopDistance) {
      controlState.lastManualUnitX = rawDx / rawDistance;
      controlState.lastManualUnitY = rawDy / rawDistance;
    }
    const overshootUnitX = Number.isFinite(controlState.lastManualUnitX) ? controlState.lastManualUnitX : 0;
    const overshootUnitY = Number.isFinite(controlState.lastManualUnitY) ? controlState.lastManualUnitY : 0;
    const targetDriveX = targetX + overshootUnitX * MANUAL_TARGET_OVERSHOOT;
    const targetDriveY = targetY + overshootUnitY * MANUAL_TARGET_OVERSHOOT;
    const dx = targetDriveX - botPosition.x;
    const dy = targetDriveY - botPosition.y;
    const distance = Math.hypot(dx, dy);
    let aimUnitX = Number.isFinite(controlState.lastManualUnitX) ? controlState.lastManualUnitX : 0;
    let aimUnitY = Number.isFinite(controlState.lastManualUnitY) ? controlState.lastManualUnitY : 0;
    if (distance > 0.001) {
      aimUnitX = dx / distance;
      aimUnitY = dy / distance;
    }
    const closeRatio = distance >= slowDistance ? 1 : Math.max(0, (distance - stopDistance) / (slowDistance - stopDistance));
    const pulseWindow = Math.max(MOVEMENT_INTERVAL_MS, 180);
    const pulsePhase = ((Date.now() + (process.pid % 37)) % pulseWindow) / pulseWindow;
    const pulseDuty = distance >= slowDistance ? 1 : Math.max(0.18, Math.min(1, closeRatio));
    const allowMoveThisTick = positionIsEstimated ? true : pulsePhase < pulseDuty;
    const now = Date.now();
    const targetChanged =
      !Number.isFinite(controlState.lastManualTargetX) ||
      !Number.isFinite(controlState.lastManualTargetY) ||
      Math.hypot(targetX - controlState.lastManualTargetX, targetY - controlState.lastManualTargetY) > targetScale * 0.2;
    if (targetChanged) {
      controlState.lastManualTargetX = targetX;
      controlState.lastManualTargetY = targetY;
      controlState.manualTargetChangedAt = now;
    }
    const holdRecentTarget = now - (controlState.manualTargetChangedAt || 0) < MANUAL_TARGET_MIN_HOLD_MS;
    const shouldMove =
      distance > stopDistance ||
      (positionIsEstimated && !TRUST_ESTIMATED_MANUAL_ARRIVAL && (Math.abs(overshootUnitX) > 0 || Math.abs(overshootUnitY) > 0)) ||
      (holdRecentTarget && (Math.abs(overshootUnitX) > 0 || Math.abs(overshootUnitY) > 0));
    if (shouldMove) {
      const directionSign = MANUAL_TARGET_INVERT ? -1 : 1;
      const estimatedMoveDistance = positionIsEstimated
        ? Math.max(distance, MANUAL_TARGET_AXIS_SCALE * 8)
        : Math.max(distance, MANUAL_TARGET_AXIS_SCALE * 1.2);
      const moveX = distance > stopDistance ? dx / Math.max(distance, 1) * estimatedMoveDistance : overshootUnitX * MANUAL_TARGET_AXIS_SCALE;
      const moveY = distance > stopDistance ? dy / Math.max(distance, 1) * estimatedMoveDistance : overshootUnitY * MANUAL_TARGET_AXIS_SCALE;
      const moveRatio = positionIsEstimated ? 1 : (distance > stopDistance ? Math.max(0.25, closeRatio) : 0.75);
      const movementX = clampInputAxis(directionSign * moveX / MANUAL_TARGET_AXIS_SCALE * moveRatio, MANUAL_TARGET_MAX_AXIS);
      const movementY = clampInputAxis(directionSign * moveY / MANUAL_TARGET_AXIS_SCALE * moveRatio, MANUAL_TARGET_MAX_AXIS);
      if (allowMoveThisTick && Math.abs(moveX) >= axisThreshold) {
        movementFlags |= moveX < 0 ? MOVEMENT_KEY_LEFT : MOVEMENT_KEY_RIGHT;
      }
      if (allowMoveThisTick && Math.abs(moveY) >= axisThreshold) {
        movementFlags |= moveY < 0 ? MOVEMENT_KEY_UP : MOVEMENT_KEY_DOWN;
      }
      manualDebug = {
        movementX,
        movementY
      };
    } else {
      manualDebug = {
        movementX: 0,
        movementY: 0
      };
    }
    if (controlState.manualAimWithMovement !== false) {
      mouseX = clampInputAxis(aimUnitX * MANUAL_AIM_AXIS + (Number(controlState.manualAimOffsetX) || 0));
      mouseY = clampInputAxis(aimUnitY * MANUAL_AIM_AXIS + (Number(controlState.manualAimOffsetY) || 0));
    }
    manualDebug = {
      ...manualDebug,
      targetX: controlState.manualX,
      targetY: controlState.manualY,
      targetOffsetX: Number(controlState.manualTargetOffsetX) || 0,
      targetOffsetY: Number(controlState.manualTargetOffsetY) || 0,
      scaleX: manualScaleX,
      scaleY: manualScaleY,
      targetInternalX: targetX,
      targetInternalY: targetY,
      targetDriveX,
      targetDriveY,
      botX: botPosition.x,
      botY: botPosition.y,
      positionEstimated: positionIsEstimated,
      dx,
      dy,
      distance,
      stopDistance,
      slowDistance,
      pulseDuty,
      aimUnitX,
      aimUnitY,
      aimOffsetX: Number(controlState.manualAimOffsetX) || 0,
      aimOffsetY: Number(controlState.manualAimOffsetY) || 0,
      movementFlags
    };
  }
  let flags = 0;
  if (controlState && controlState.mouseDown) {
    flags |= FIRE_INPUT_FLAG;
  }
  if (controlState && controlState.rMouseDown) {
    flags |= 0x04;
  }
  flags |= movementFlags;
  if (controlState && controlState.autospin) {
    flags |= 0x08;
  }
  const rawMouseX = mouseX;
  const rawMouseY = mouseY;
  mouseX = sanitizeControlAimAxis(mouseX);
  mouseY = sanitizeControlAimAxis(mouseY);
  const packet = concatBytes([
    Buffer.from([0x43]),
    encodeInputNumber(mouseX),
    encodeInputNumber(mouseY),
    encodeInputNumber(flags & 0xff)
  ]);
  packet.manualDebug = manualDebug ? { ...manualDebug, mouseX, mouseY, flags: flags & 0xff } : null;
  if (Math.abs(rawMouseX - mouseX) > 0.5 || Math.abs(rawMouseY - mouseY) > 0.5) {
    packet.aimDebug = { rawMouseX, rawMouseY, mouseX, mouseY };
  }
  return packet;
}

class ProtocolOnlyRandomClient {
  constructor(socketUrl, options = {}) {
    this.clientLogId = process.env.ARRAS_CLIENT_LOG_ID || String(nextClientLogId++);
    this.logPrefix = `[c${this.clientLogId}]`;
    this.socketUrl = socketUrl;
    this.options = options;
    this.state = 'idle';
    this.key = null;
    this.outboundCounter = 0;
    this.inboundCounter = 0;
    this.packetIndex = 0;
    this.pingTimer = null;
    this.movementTimer = null;
    this.respawnWatchTimer = null;
    this.upgradeTimers = new Set();
    this.spawnTimer = null;
    this.respawnTimer = null;
    this.shutdownTimer = null;
    this.socket = null;
    this.connectAttempt = 0;
    this.retryTimer = null;
    this.manualClose = false;
    this.socketMeta = latestSocketMetaFromLog(socketUrl);
    this.defaultSpawnName = this.deriveDefaultSpawnName();
    this.spawnSentAt = null;
    this.seenAfterSpawn = new Set();
    this.commandCounts = new Map();
    this.modeInfo = null;
    this.recentInboundPlainPackets = [];
    this.lastValidationInbound = null;
    this.spawnLikelyAccepted = false;
    this.sentValidationR = new Set();
    this.sentValidationE = new Set();
    this.spawnQueued = false;
    this.validationSeen = false;
    this.modePacketSeen = false;
    this.inboundPingSeen = false;
    this.validationTokenSeen = false;
    this.validationCTokenSeen = false;
    this.validationWTokenSeen = false;
    this.pendingValidationRToken = null;
    this.validationFallbackTimer = null;
    this.spawnFallbackTimer = null;
    this.delayedValidationRTimer = null;
    this.delayedValidationETimer = null;
    this.modeInfoValidationTimer = null;
    this.modeInfoValidationDelayElapsed = false;
    this.modeInfoInboundCount = 0;
    this.rejected = false;
    this.controlState = null;
    this.targetTank = normalizeTankName(options.tank || 'basic');
    this.upgradeStarted = false;
    this.statBuildStarted = false;
    this.deathDetectedAt = 0;
    this.botPosition = null;
    this.loggedBotPosition = false;
    this.uSampleCount = 0;
    this.lastUPositionLogAt = 0;
    this.lastUPositionRejectLogAt = 0;
    this.lastPositionEstimateAt = 0;
    this.lastLivePositionAt = 0;
    this.lastDistinctPosition = null;
    this.lastDistinctPositionAt = 0;
    this.lastMovementHex = '';
    this.lastMovementLogAt = 0;
    this.lastManualControlSentAt = 0;
    this.lastAutofireState = false;
    this.lastAutofireWanted = false;
    this.autofireChangeReadyAt = 0;
    this.lastInputHadActiveControl = false;
    this.movementIntervalMs = Math.max(1, Math.round(MOVEMENT_INTERVAL_MS + (Math.random() * 2 - 1) * Math.max(0, MOVEMENT_JITTER_MS)));
    this.movementPhaseDelayMs = Math.floor(Math.random() * Math.max(0, MOVEMENT_PHASE_JITTER_MS + 1));
    const formationOffset = stableFormationOffset(this.clientLogId, MANUAL_FORMATION_SPREAD);
    this.manualFormationOffsetX = formationOffset.x;
    this.manualFormationOffsetY = formationOffset.y;
    this.manualTargetOffsetX = formationOffset.x;
    this.manualTargetOffsetY = formationOffset.y;
    this.manualAimOffsetX = 0;
    this.manualAimOffsetY = 0;
    this.manualFallbackAimX = 0;
    this.manualFallbackAimY = 0;
    this.nextManualTargetOffsetAt = 0;
    this.nextManualAimOffsetAt = 0;
    this.rawMouseDown = false;
    this.fireHoldStartedAt = 0;
    this.fireStartDelayMs = Math.floor(Math.random() * Math.max(0, FIRE_STAGGER_MS + 1));
    this.firePauseUntil = 0;
    this.nextFirePauseAt = 0;
    this.calibratedManualScaleX = null;
    this.calibratedManualScaleY = null;
    this.lastManualScaleCalibrationAt = 0;
    this.startedAt = Date.now();
    this.openedAt = null;
    this.handshakeAt = null;
    this.lastInboundAt = null;
    this.lastOutboundAt = null;
    this.lastInboundCommand = null;
    this.lastOutboundCommand = null;
    this.captureHash =
      (typeof options.captureHash === 'string' && options.captureHash) ||
      process.env.ARRAS_CAPTURE_HASH ||
      (this.socketMeta && typeof this.socketMeta.hash === 'string' ? this.socketMeta.hash : null) ||
      null;
    this.validationRTemplate = this.captureHash ? latestValidationTemplateFromEvents('R', this.captureHash) : null;
    this.validationETemplate = this.captureHash ? latestValidationTemplateFromEvents('e', this.captureHash) : null;
    this.validationRToken = options.validationRToken || null;
    this.validationEToken = options.validationEToken || null;
    this.validationRValue = null;
    this.validationRValueSource = 'live-missing';
    this.validationEAnswer =
      options.validationEAnswer ||
      (this.validationETemplate && this.validationETemplate.value) ||
      (this.captureHash ? latestValidationEAnswerFromLog(this.captureHash) : null) ||
      null;
    this.validationRTailExplicit = Boolean(options.validationRTail);
    this.validationRTail =
      options.validationRTail ||
      (this.validationRTemplate && this.validationRTemplate.tail) ||
      null;
    this.validationETail =
      options.validationETail ||
      (this.validationETemplate && this.validationETemplate.tail) ||
      null;
    this.helloTemplate = options.helloTemplate || capturedHelloTemplate() ||
      (USE_CAPTURED_K && this.captureHash ? latestCommandTemplateFromLog('k', this.captureHash) : null);
    this.fingerprintTemplate = options.fingerprintTemplate || capturedFingerprintTemplate() ||
      latestCommandTemplateFromLog('T', this.captureHash);
    this.spawnTemplate = options.spawnTemplate || explicitSpawnTemplateFromEnv() || bestSpawnTemplateFromLog(this.captureHash);
    this.pingTemplate = options.pingTemplate || latestPingTemplateFromLog(this.captureHash);
    this.spawnExtraFields = Array.isArray(options.spawnExtraFields) ? options.spawnExtraFields : DEFAULT_SPAWN_EXTRA_FIELDS;
    if (this.captureHash) {
      this.log(`[INFO] validation-R live-only hash=${this.captureHash} source=${this.validationRValueSource} value=${this.validationRValue || '(none)'}`);
    }
  }

  log(message) {
    console.log(`${this.logPrefix} ${message}`);
  }

  error(message, detail = null) {
    if (detail === null || detail === undefined) {
      console.error(`${this.logPrefix} ${message}`);
      return;
    }
    console.error(`${this.logPrefix} ${message}`, detail);
  }

  refreshTemplatesForHash(captureHash) {
    if (!captureHash || captureHash === this.captureHash) {
      return;
    }
    this.captureHash = captureHash;
    this.validationRTemplate = latestValidationTemplateFromEvents('R', captureHash);
    this.validationETemplate = latestValidationTemplateFromEvents('e', captureHash);
    this.validationRValue = null;
    this.validationRValueSource = 'live-missing';
    this.validationEAnswer =
      (this.validationETemplate && this.validationETemplate.value) ||
      latestValidationEAnswerFromLog(captureHash) ||
      null;
    this.validationRTail =
      (this.validationRTemplate && this.validationRTemplate.tail) ||
      null;
    this.validationETail =
      (this.validationETemplate && this.validationETemplate.tail) ||
      null;
    this.fingerprintTemplate = this.fingerprintTemplate || capturedFingerprintTemplate() || latestCommandTemplateFromLog('T', captureHash);
    this.helloTemplate = this.helloTemplate || capturedHelloTemplate() ||
      (USE_CAPTURED_K ? latestCommandTemplateFromLog('k', captureHash) : null);
    this.spawnTemplate = this.spawnTemplate || explicitSpawnTemplateFromEnv() || bestSpawnTemplateFromLog(captureHash);
    this.pingTemplate = this.pingTemplate || latestPingTemplateFromLog(captureHash);
    this.log(`[INFO] validation-R live-only hash=${captureHash} source=${this.validationRValueSource} value=${this.validationRValue || '(none)'}`);
  }

  deriveDefaultSpawnName() {
    const explicit = typeof this.options.name === 'string' ? this.options.name : '';
    if (explicit) {
      return explicit;
    }
    const hash = this.socketMeta && typeof this.socketMeta.hash === 'string'
      ? this.socketMeta.hash.replace(/^#/, '')
      : '';
    if (hash.length >= 4) {
      return hash.slice(-4);
    }
    return '';
  }

  async connect() {
    this.connectAttempt++;
    this.log(`Using socket URL: ${this.socketUrl}${this.connectAttempt > 1 ? ` attempt=${this.connectAttempt}` : ''}`);
    this.state = 'opening';
    const proxy = await buildProxyAgent();
    if (proxy) {
      this.log(`Using proxy: ${proxy.url}`);
    }
    this.socket = new WebSocket(this.socketUrl, DEFAULT_PROTOCOLS, {
      agent: proxy ? proxy.agent : undefined,
      headers: DEFAULT_HEADERS,
      rejectUnauthorized: false
    });
    this.socket.binaryType = 'arraybuffer';
    this.socket.on('open', () => this.onOpen());
    this.socket.on('message', (data) => this.onMessage(data));
    this.socket.on('error', (error) => this.onError(error));
    this.socket.on('close', (code, reason) => this.onClose(code, reason));

    const durationMs = this.options.durationMs;
    if (durationMs > 0) {
      this.shutdownTimer = setTimeout(() => {
        this.log(`Test duration ${durationMs}ms reached; closing.`);
        this.close();
      }, durationMs);
    }
  }

  onOpen() {
    this.log('WebSocket open; sending session open packet.');
    this.openedAt = Date.now();
    this.state = 'awaiting-challenge';
    this.sendRaw(buildOpenPacket(this.socketUrl));
  }

  onMessage(data) {
    const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
    if (this.state === 'awaiting-challenge' && bytes.length === 96) {
      this.completeHandshake(bytes);
      return;
    }

    if (this.state !== 'ready') {
      console.log(`[raw] len=${bytes.length} hex=${hexPrefix(bytes)}`);
      return;
    }

    try {
      const plaintext = decryptPacket(bytes, this.key, inboundSequence(this.inboundCounter));
      this.inboundCounter += 1;
      this.packetIndex += 1;
      const command = printableCommand(plaintext[0]);
      this.lastInboundAt = Date.now();
      this.lastInboundCommand = command || `0x${plaintext[0].toString(16).padStart(2, '0')}`;
      const inboundPlainSummary = summarizePacketForValidation(plaintext, this.packetIndex, {
        inboundCounter: this.inboundCounter - 1
      });
      if (this.modePacketSeen) {
        this.modeInfoInboundCount += 1;
      }
      this.recentInboundPlainPackets.push(inboundPlainSummary);
      if (this.recentInboundPlainPackets.length > 8) {
        this.recentInboundPlainPackets.shift();
      }
      if (command === 'w' || command === 'C' || command === 'e' || command === 'R') {
        this.lastValidationInbound = inboundPlainSummary;
        appendNdjson('validation-inbound.ndjson', {
          ts: new Date().toISOString(),
          hash: this.hashFromUrl(),
          packetIndex: this.packetIndex,
          inboundCounter: this.inboundCounter - 1,
          plain: inboundPlainSummary
        });
      }
      if (command === 'w' || command === 'C' || command === 'e' || command === 'K' || command === 'R') {
        appendNdjson('protocol-only-validation.ndjson', packetRecord('IN', this.packetIndex, plaintext, {
          hash: this.hashFromUrl(),
          inboundCounter: this.inboundCounter - 1,
          pairedInbound: command === 'K' ? this.lastValidationInbound : null,
          recentInboundPlain: this.recentInboundPlainPackets.slice(-6)
        }));
      }
      if (command === 'e') {
        const evalChallenge = decodeEvalChallenge(plaintext);
        if (evalChallenge) {
          writeDebugArtifact('protocol-only-last-e.txt', [evalChallenge.token, evalChallenge.body].join('\n\n'));
          writeDebugArtifact('protocol-only-last-e.json', JSON.stringify(evalChallenge, null, 2));
          console.log(`[IN ${this.packetIndex}] eval mode=${evalChallenge.evalEnvMode} browserLikeAnswer=${evalChallenge.browserLikeAnswer} tokenLen=${evalChallenge.token.length}`);
          if (String(evalChallenge.browserLikeAnswer).startsWith('error:')) {
            console.log('[INFO] validation-e blocked: eval challenge answer errored');
          } else if (!this.sentValidationE.has(evalChallenge.token)) {
            this.sendPacket(buildValidationPacket(
              'e',
              evalChallenge.token,
              String(evalChallenge.browserLikeAnswer),
              this.validationETail
            ));
            this.sentValidationE.add(evalChallenge.token);
            console.log(`[OUT] validation-e tokenLen=${evalChallenge.token.length} answer=${evalChallenge.browserLikeAnswer}`);
          }
          this.clearSpawnFallback();
        }
      } else if (command === 'p' || command === 'P') {
        const earlyBootstrapSpawn =
          ALLOW_EARLY_SPAWN &&
          command === 'P' &&
          !this.validationTokenSeen &&
          this.captureHash &&
          this.spawnTemplate;
        this.inboundPingSeen = true;
        if (earlyBootstrapSpawn) {
          this.spawnQueued = true;
        }
        this.maybeSpawn('inbound-ping');
      } else if (command === 'w' || command === 'C') {
        this.validationSeen = true;
        if (command === 'w') {
          const validationToken = extractValidationToken(plaintext);
          this.validationWTokenSeen = true;
          this.validationTokenSeen = true;
          this.spawnQueued = true;
          console.log(`[INFO] inbound-w token=${JSON.stringify(validationToken)}`);
          if (ALLOW_W_TOKEN_R_FALLBACK && !this.validationCTokenSeen) {
            const outboundRToken = validationToken || this.validationRToken;
            if (outboundRToken && !this.sentValidationR.has(outboundRToken)) {
              this.pendingValidationRToken = outboundRToken;
              this.armValidationWFallback();
            }
          }
          this.armSpawnFallback();
          if (!SPAWN_AFTER_MODE_INFO) {
            this.maybeSpawn('validation-w');
          }
        }
        if (command === 'C') {
          this.validationCTokenSeen = true;
          this.validationTokenSeen = true;
          this.spawnQueued = true;
          this.clearValidationWFallback();
          this.clearSpawnFallback();
          const validationToken = extractValidationToken(plaintext);
          const outboundRToken = validationToken || this.validationRToken;
          if (outboundRToken && !this.sentValidationR.has(outboundRToken)) {
            this.pendingValidationRToken = outboundRToken;
            this.ensureValidationRValue(outboundRToken);
            this.maybeSendValidationR('validation-c');
          }
          this.maybeSpawn('validation-c');
        }
      } else if (command === 'K') {
        if (this.spawnLikelyAccepted && plaintext.length === 2 && plaintext[1] === 0xc0) {
          this.scheduleRespawn('death-k-c0');
        } else {
          this.rejected = true;
          if (this.delayedValidationETimer) {
            clearTimeout(this.delayedValidationETimer);
            this.delayedValidationETimer = null;
          }
          this.clearModeInfoValidationDelay();
          const asciiRuns = extractAsciiRuns(Buffer.from(plaintext));
          writeDebugArtifact('protocol-only-last-k.txt', asciiRuns.join('\n\n'));
        }
      }
      if (command === 'c') {
        const position = decodePositionPacket(plaintext);
        if (position) {
          this.updateLivePosition(position);
          if (!this.loggedBotPosition) {
            this.loggedBotPosition = true;
            this.log(`[INFO] bot-position x=${position.x.toFixed(1)} y=${position.y.toFixed(1)}`);
          }
        }
      }
      if (command === 'u') {
        const deathMarkerOffset = deathStateMarkerOffset(plaintext);
        if (
          deathMarkerOffset >= 0 &&
          this.spawnLikelyAccepted &&
          this.botPosition
        ) {
          this.scheduleRespawn(`death-state-u-marker offset=${deathMarkerOffset} len=${plaintext.length} hexPrefix=${hexPrefix(Buffer.from(plaintext), 48)}`);
        }
        const position = decodeUpdatePositionPacket(plaintext);
        if (this.uSampleCount < U_SAMPLE_LIMIT) {
          this.uSampleCount += 1;
          appendNdjson('protocol-only-u-samples.ndjson', {
            ts: new Date().toISOString(),
            packetIndex: this.packetIndex,
            inboundCounter: this.inboundCounter,
            bytes: plaintext.length,
            hex: Buffer.from(plaintext).toString('hex'),
            decodedPosition: position,
            botPositionBefore: this.botPosition,
            manual: this.controlState
              ? {
                manualMode: Boolean(this.controlState.manualMode),
                manualX: this.controlState.manualX,
                manualY: this.controlState.manualY
              }
              : null
          });
        }
        if (position && this.updateFilteredUPosition(position, plaintext.length)) {
          const now = Date.now();
          if (now - this.lastUPositionLogAt > U_POSITION_LOG_MS) {
            this.lastUPositionLogAt = now;
          this.log(`[INFO] u-position x=${this.botPosition.x.toFixed(1)} y=${this.botPosition.y.toFixed(1)} mode=${U_POSITION_MODE} len=${plaintext.length}`);
          }
        }
      }
      const countKey = command || `0x${plaintext[0].toString(16).padStart(2, '0')}`;
      this.commandCounts.set(countKey, (this.commandCounts.get(countKey) || 0) + 1);
      if (this.spawnSentAt && command) {
        this.seenAfterSpawn.add(command);
      }
      if (command === 'R' && !this.modeInfo) {
        this.modeInfo = parseModeInfo(plaintext);
        if (this.modeInfo) {
          if (this.modeInfo.id) {
            this.refreshTemplatesForHash(`#${this.modeInfo.id}`);
          }
          this.modePacketSeen = true;
          this.modeInfoInboundCount = 0;
          console.log(`[INFO] mode=${this.modeInfo.mode || '?'} provider=${this.modeInfo.provider || '?'} urlHash=${this.hashFromUrl() || '?'}`);
          if (this.validationCTokenSeen || ALLOW_W_TOKEN_R_FALLBACK) {
            this.maybeSendValidationR('mode-info');
          }
          this.maybeSpawn('mode-info');
        }
      }
      if (
        !this.rejected &&
        this.modePacketSeen &&
        this.modeInfoValidationDelayElapsed &&
        this.pendingValidationRToken &&
        !this.validationCTokenSeen &&
        !this.sentValidationR.has(this.pendingValidationRToken)
      ) {
        const minInboundAfterMode = Number(process.env.ARRAS_MODE_INFO_R_MIN_INBOUND || 1);
        if (this.modeInfoInboundCount >= minInboundAfterMode) {
          this.maybeSendValidationR('post-mode-info-delay');
        }
      }
      const messageText = command === 'm'
        ? extractAsciiRuns(Buffer.from(plaintext)).join(' ')
        : '';
      if (this.spawnLikelyAccepted && command === 'm' && isDeathMessage(messageText)) {
        this.scheduleRespawn(messageText);
      }
      if (
        this.spawnSentAt &&
        !this.spawnLikelyAccepted &&
        (
          (command === 'b' && plaintext.length > 7) ||
          /You have spawned!/.test(messageText)
        )
      ) {
        this.spawnLikelyAccepted = true;
        this.lastLivePositionAt = Date.now();
        this.log(`[INFO] post-spawn accept detected via cmd=${command}; real in-game state.`);
        this.ensureRespawnWatchdog();
        this.scheduleClassUpgrade('post-spawn');
      }
      if (!(command === 'u' && this.options.logU === false)) {
        this.log(summarizePacket('IN', this.packetIndex, plaintext));
      }
    } catch (error) {
      if (error && error.message === 'Socket is not open' && (!this.socket || this.socket.readyState >= WebSocket.CLOSING)) {
        return;
      }
      console.error(`Failed to decrypt inbound packet #${this.inboundCounter}:`, error.message);
    }
  }

  completeHandshake(challenge) {
    const challengeKey = challenge.subarray(0, 32);
    const signature = challenge.subarray(32, 96);

    if (this.options.verifySignature !== false) {
      const isValid = verifyEd25519(
        challengeKey,
        signature,
        Buffer.from(SERVER_IDENTITY_KEY_HEX, 'hex')
      );
      if (!isValid) {
        throw new Error('Challenge signature verification failed');
      }
    }

    const scalar = clampScalar(crypto.randomBytes(32));
    const responsePublicKey = x25519Base(scalar);
    this.key = x25519(scalar, challengeKey);
    this.sendRaw(responsePublicKey);

    this.state = 'ready';
    this.handshakeAt = Date.now();
    console.log('Handshake complete; protection state initialized.');

    const helloPacket = this.helloTemplate || encodeCommand('k', ['', '', '']);
    this.sendPacket(helloPacket);
    console.log(`[OUT] hello source=${this.helloTemplate ? 'captured' : 'default'} bytes=${helloPacket.length}`);
    this.sendPacket(this.fingerprintTemplate || buildFingerprintPacket(defaultFingerprint()));
    console.log(`[OUT] fingerprint source=${this.fingerprintTemplate ? 'captured' : 'sanitized-default'}`);
    if (STARTUP_TEMPLATE_SPAWN && this.spawnTemplate) {
      this.spawnQueued = true;
      this.maybeSpawn('startup-template');
    }
    this.startPingLoop();
    this.scheduleSpawn();
  }

  startPingLoop() {
    const pingMs = this.options.pingMs || 5000;
    this.pingTimer = setInterval(() => {
      if (this.state !== 'ready') {
        return;
      }
      this.sendPing();
    }, pingMs);
  }

  sendPing(reason = 'interval') {
    const packet = this.pingTemplate || Buffer.from([0x70]);
    if (this.sendPacket(packet)) {
      console.log(`[OUT] ping bytes=${packet.length} via=${reason}`);
    }
  }

  scheduleSpawn() {
    if (process.env.ARRAS_USE_SPAWN_TIMER !== '1') {
      return;
    }
    const spawnDelayMs = this.options.spawnDelayMs || 750;
    this.spawnTimer = setTimeout(() => {
      if (this.state !== 'ready') {
        return;
      }
      this.spawnQueued = true;
      this.maybeSpawn('spawn-timer');
    }, spawnDelayMs);
  }

  maybeSpawn(reason) {
    if (!this.spawnQueued || this.spawnSentAt || this.state !== 'ready') {
      return;
    }
    if (this.respawnTimer && reason !== 'auto-respawn') {
      return;
    }
    const readyForSpawn =
      (
        this.validationTokenSeen &&
        (
          this.validationCTokenSeen ||
          this.sentValidationE.size > 0 ||
          reason === 'spawn-fallback' ||
          reason === 'auto-respawn' ||
          reason === 'validation-w' ||
          (SPAWN_AFTER_MODE_INFO && reason === 'mode-info')
        )
      ) ||
      (
        !this.validationTokenSeen &&
        !!this.spawnTemplate &&
        (reason === 'inbound-ping' || reason === 'mode-info' || reason === 'startup-template')
      );
    if (!readyForSpawn) {
      return;
    }
    const hasParty = Boolean(this.options.party);
    const usedTemplateSpawn = Boolean(this.spawnTemplate && !hasParty);
    const packet = usedTemplateSpawn ? this.spawnTemplate : buildDefaultSpawnPacket(this.options.party || '');
    this.spawnSentAt = Date.now();
    this.sendPacket(packet);
    console.log(`[OUT] spawn bytes=${packet.length} via=${reason}`);
    this.sendPing('post-spawn');
    if (ALLOW_W_TOKEN_R_FALLBACK && reason === 'validation-w' && !this.validationCTokenSeen) {
      this.armDelayedValidationRAfterSpawn();
    }
    if (!usedTemplateSpawn && !this.options.party) {
      console.log(`[OUT] spawn minimal accepted-shape hex=${packet.toString('hex')} partyHint="${this.options.party || ''}" via=${reason}`);
    } else if (!usedTemplateSpawn) {
      console.log(`[OUT] spawn party-shape hex=${packet.toString('hex')} party="${this.options.party || ''}" via=${reason}`);
    }
  }

  maybeSendValidationR(reason) {
    if (this.rejected) {
      return;
    }
    if (!this.pendingValidationRToken || this.sentValidationR.has(this.pendingValidationRToken)) {
      return;
    }
    if (!this.modePacketSeen) {
      return;
    }
    this.ensureValidationRValue(this.pendingValidationRToken);
    if (!this.validationRValue) {
      console.log('[INFO] validation-R blocked: no fresh same-session R value for current hash');
      return;
    }
    this.sendPacket(buildValidationPacket(
      'R',
      this.pendingValidationRToken,
      this.validationRValue,
      this.validationRTail || Buffer.alloc(0)
    ));
    this.sentValidationR.add(this.pendingValidationRToken);
    console.log(`[OUT] validation-R tokenLen=${this.pendingValidationRToken.length} value=${JSON.stringify(this.validationRValue)} source=${this.validationRValueSource || 'unknown'} via=${reason}`);
    this.pendingValidationRToken = null;
    this.armDelayedValidationEAfterR();
  }

  ensureValidationRValue(token) {
    if (this.validationRValue && this.validationRToken === token) {
      return true;
    }
    const result = computeValidationRValue(token);
    if (!result) {
      console.log(`[INFO] validation-R pow failed tokenLen=${token ? token.length : 0} attempts=${VALIDATION_R_MAX_ATTEMPTS}`);
      return false;
    }
    this.validationRToken = token;
    this.validationRValue = result.value;
    this.validationRValueSource = `live-pow-sha256/${VALIDATION_R_HASH_PREFIX}`;
    if (!this.validationRTailExplicit) {
      this.validationRTail = null;
    }
    console.log(`[INFO] validation-R pow tokenLen=${token.length} value=${JSON.stringify(result.value)} attempts=${result.attempts} digest=${result.digest.slice(0, 16)}`);
    return true;
  }

  maybeSendCapturedValidationE(reason) {
    const token = this.validationEToken || (this.validationETemplate && this.validationETemplate.token) || null;
    const value = this.validationEAnswer;
    if (!token || value == null) {
      return;
    }
    if (!this.validationETail || !this.validationETail.length) {
      return;
    }
    if (this.sentValidationE.has(token)) {
      return;
    }
    this.sendPacket(buildValidationPacket(
      'e',
      token,
      String(value),
      this.validationETail
    ));
    this.sentValidationE.add(token);
    console.log(`[OUT] validation-e tokenLen=${token.length} answer=${value} via=${reason}`);
  }

  armDelayedValidationRAfterSpawn() {
    if (this.delayedValidationRTimer || !this.spawnSentAt) {
      return;
    }
    const delayMs = Number(process.env.ARRAS_POST_SPAWN_R_DELAY_MS || 550);
    this.delayedValidationRTimer = setTimeout(() => {
      this.delayedValidationRTimer = null;
      if (this.pendingValidationRToken && !this.sentValidationR.has(this.pendingValidationRToken)) {
        this.maybeSendValidationR('post-spawn-delay');
      }
    }, delayMs);
  }

  armDelayedValidationEAfterR() {
    if (!ALLOW_CAPTURED_E_REPLAY) {
      console.log('[INFO] validation-e replay disabled: waiting for live same-session inbound e challenge');
      return;
    }
    const delayMs = Number(process.env.ARRAS_POST_R_E_DELAY_MS || 250);
    if (delayMs <= 0) {
      this.maybeSendCapturedValidationE('post-r-inline');
      return;
    }
    if (this.delayedValidationETimer) {
      return;
    }
    this.delayedValidationETimer = setTimeout(() => {
      this.delayedValidationETimer = null;
      this.maybeSendCapturedValidationE('post-r-delay');
    }, delayMs);
  }

  armValidationRAfterModeInfo() {
    if (this.rejected) {
      return;
    }
    if (!this.pendingValidationRToken || this.validationCTokenSeen) {
      this.maybeSendValidationR('mode-info');
      return;
    }
    if (this.modeInfoValidationTimer || this.modeInfoValidationDelayElapsed) {
      return;
    }
    const delayMs = Number(process.env.ARRAS_MODE_INFO_R_DELAY_MS || 90);
    this.modeInfoValidationTimer = setTimeout(() => {
      this.modeInfoValidationTimer = null;
      this.modeInfoValidationDelayElapsed = true;
      const minInboundAfterMode = Number(process.env.ARRAS_MODE_INFO_R_MIN_INBOUND || 1);
      if (
        !this.rejected &&
        this.pendingValidationRToken &&
        !this.validationCTokenSeen &&
        this.modeInfoInboundCount >= minInboundAfterMode
      ) {
        this.maybeSendValidationR('post-mode-info-delay');
      }
    }, delayMs);
  }

  armValidationWFallback() {
    if (this.validationFallbackTimer || !this.validationWTokenSeen || this.validationCTokenSeen) {
      return;
    }
    const delayMs = Number(process.env.ARRAS_VALIDATION_W_FALLBACK_MS || 400);
    this.validationFallbackTimer = setTimeout(() => {
      this.validationFallbackTimer = null;
      if (!this.validationCTokenSeen && this.pendingValidationRToken && !this.spawnSentAt) {
        this.maybeSendValidationR('validation-w-fallback');
      }
    }, delayMs);
  }

  armSpawnFallback() {
    if (this.spawnFallbackTimer || !this.validationWTokenSeen || this.validationCTokenSeen || this.spawnSentAt) {
      return;
    }
    const delayMs = Number(process.env.ARRAS_SPAWN_W_FALLBACK_MS || 1200);
    this.spawnFallbackTimer = setTimeout(() => {
      this.spawnFallbackTimer = null;
      if (!this.rejected && this.state === 'ready' && !this.spawnSentAt) {
        this.maybeSpawn('spawn-fallback');
      }
    }, delayMs);
  }

  clearValidationWFallback() {
    if (!this.validationFallbackTimer) {
      return;
    }
    clearTimeout(this.validationFallbackTimer);
    this.validationFallbackTimer = null;
  }

  clearSpawnFallback() {
    if (!this.spawnFallbackTimer) {
      return;
    }
    clearTimeout(this.spawnFallbackTimer);
    this.spawnFallbackTimer = null;
  }

  clearDelayedValidationE() {
    if (!this.delayedValidationETimer) {
      return;
    }
    clearTimeout(this.delayedValidationETimer);
    this.delayedValidationETimer = null;
  }

  clearDelayedValidationR() {
    if (!this.delayedValidationRTimer) {
      return;
    }
    clearTimeout(this.delayedValidationRTimer);
    this.delayedValidationRTimer = null;
  }

  clearModeInfoValidationDelay() {
    if (!this.modeInfoValidationTimer) {
      return;
    }
    clearTimeout(this.modeInfoValidationTimer);
    this.modeInfoValidationTimer = null;
  }

  updateTankSelection(message) {
    if (!message || message.type !== 'tankselect') {
      return;
    }
    const tank = normalizeTankName(message.tank || 'basic');
    if (!tank) {
      return;
    }
    if (tank === this.targetTank) {
      return;
    }
    this.targetTank = tank;
    this.upgradeStarted = false;
    this.statBuildStarted = false;
    this.clearUpgradeTimers();
    this.log(`[INFO] tank target=${tank}`);
    if (this.spawnLikelyAccepted) {
      this.scheduleClassUpgrade('tankselect');
    }
  }

  scheduleRespawn(reason) {
    if (!AUTO_RESPAWN_ENABLED || this.respawnTimer || this.rejected || this.state !== 'ready') {
      if (process.env.ARRAS_RESPAWN_DEBUG === '1') {
        console.log(`[INFO] respawn ignored reason=${JSON.stringify(reason)} enabled=${AUTO_RESPAWN_ENABLED} timer=${Boolean(this.respawnTimer)} rejected=${this.rejected} state=${this.state}`);
      }
      return;
    }
    const now = Date.now();
    if (now - this.deathDetectedAt < 1000) {
      if (process.env.ARRAS_RESPAWN_DEBUG === '1') {
        console.log(`[INFO] respawn ignored duplicate reason=${JSON.stringify(reason)} ageMs=${now - this.deathDetectedAt}`);
      }
      return;
    }
    this.deathDetectedAt = now;
    this.spawnQueued = false;
    this.spawnSentAt = null;
    this.spawnLikelyAccepted = false;
    this.upgradeStarted = false;
    this.statBuildStarted = false;
    this.botPosition = null;
    this.loggedBotPosition = false;
    this.lastPositionEstimateAt = 0;
    this.lastLivePositionAt = 0;
    this.lastUPositionRejectLogAt = 0;
    this.lastDistinctPosition = null;
    this.lastDistinctPositionAt = 0;
    this.lastAutofireState = false;
    this.pendingValidationRToken = null;
    this.validationRToken = null;
    this.validationRValue = null;
    this.validationRValueSource = 'live-missing';
    this.clearUpgradeTimers();
    this.log(`[INFO] death detected; respawn scheduled mode=${AUTO_RESPAWN_MODE} tank=${this.targetTank} in=${AUTO_RESPAWN_DELAY_MS}ms reason=${JSON.stringify(reason)}`);
    this.respawnTimer = setTimeout(() => {
      this.respawnTimer = null;
      if (AUTO_RESPAWN_MODE !== 'same-socket') {
        this.reconnectForRespawn();
        return;
      }
      this.spawnQueued = true;
      this.maybeSpawn('auto-respawn');
    }, AUTO_RESPAWN_DELAY_MS);
  }

  resetSessionStateForReconnect() {
    this.state = 'idle';
    this.key = null;
    this.outboundCounter = 0;
    this.inboundCounter = 0;
    this.packetIndex = 0;
    this.spawnSentAt = null;
    this.seenAfterSpawn = new Set();
    this.commandCounts = new Map();
    this.modeInfo = null;
    this.recentInboundPlainPackets = [];
    this.lastValidationInbound = null;
    this.spawnLikelyAccepted = false;
    this.sentValidationR = new Set();
    this.sentValidationE = new Set();
    this.spawnQueued = false;
    this.validationSeen = false;
    this.modePacketSeen = false;
    this.inboundPingSeen = false;
    this.validationTokenSeen = false;
    this.validationCTokenSeen = false;
    this.validationWTokenSeen = false;
    this.pendingValidationRToken = null;
    this.validationFallbackTimer = null;
    this.spawnFallbackTimer = null;
    this.delayedValidationRTimer = null;
    this.delayedValidationETimer = null;
    this.modeInfoValidationTimer = null;
    this.modeInfoValidationDelayElapsed = false;
    this.modeInfoInboundCount = 0;
    this.rejected = false;
    this.upgradeStarted = false;
    this.statBuildStarted = false;
    this.deathDetectedAt = 0;
    this.botPosition = null;
    this.loggedBotPosition = false;
    this.uSampleCount = 0;
    this.lastUPositionLogAt = 0;
    this.lastUPositionRejectLogAt = 0;
    this.lastPositionEstimateAt = 0;
    this.lastLivePositionAt = 0;
    this.lastDistinctPosition = null;
    this.lastDistinctPositionAt = 0;
    this.lastMovementHex = '';
    this.lastMovementLogAt = 0;
    this.lastAutofireState = false;
    this.openedAt = null;
    this.handshakeAt = null;
    this.lastInboundAt = null;
    this.lastOutboundAt = null;
    this.lastInboundCommand = null;
    this.lastOutboundCommand = null;
    this.validationRToken = this.options.validationRToken || null;
    this.validationEToken = this.options.validationEToken || null;
    this.validationRValue = null;
    this.validationRValueSource = 'live-missing';
    this.validationEAnswer =
      this.options.validationEAnswer ||
      (this.validationETemplate && this.validationETemplate.value) ||
      (this.captureHash ? latestValidationEAnswerFromLog(this.captureHash) : null) ||
      null;
  }

  reconnectForRespawn() {
    this.log(`[INFO] reconnecting after death tank=${this.targetTank}`);
    this.cleanupTimers();
    const oldSocket = this.socket;
    this.socket = null;
    if (oldSocket) {
      oldSocket.removeAllListeners('close');
      oldSocket.removeAllListeners('error');
      oldSocket.removeAllListeners('message');
      oldSocket.removeAllListeners('open');
      try {
        if (oldSocket.readyState < WebSocket.CLOSING) {
          oldSocket.close();
        } else if (oldSocket.readyState !== WebSocket.CLOSED && typeof oldSocket.terminate === 'function') {
          oldSocket.terminate();
        }
      } catch (error) {
        this.log(`[INFO] old socket close during respawn failed: ${error && error.message ? error.message : error}`);
      }
    }
    this.resetSessionStateForReconnect();
    this.connect().catch((error) => {
      this.error('Respawn reconnect failed:', error && error.stack ? error.stack : error);
      process.exitCode = 1;
    });
  }

  updateLivePosition(position) {
    const now = Date.now();
    const previous = this.lastDistinctPosition;
    this.botPosition = position;
    this.lastPositionEstimateAt = now;
    this.lastLivePositionAt = now;
    this.maybeCalibrateManualScale(position);
    if (
      !previous ||
      Math.abs(position.x - previous.x) > POSITION_CHANGE_EPSILON ||
      Math.abs(position.y - previous.y) > POSITION_CHANGE_EPSILON
    ) {
      this.lastDistinctPosition = position;
      this.lastDistinctPositionAt = now;
    }
  }

  maybeCalibrateManualScale(position) {
    if (!MANUAL_SCALE_AUTO_CALIBRATE || !position || position.estimated || !this.controlState) {
      return;
    }
    const manualX = Number(this.controlState.manualX);
    const manualY = Number(this.controlState.manualY);
    if (!this.controlState.manualMode || !Number.isFinite(manualX) || !Number.isFinite(manualY)) {
      return;
    }
    const currentScaleX = resolveManualScale(this.calibratedManualScaleX ?? this.controlState.manualScaleX, MANUAL_TARGET_X_SCALE);
    const currentScaleY = resolveManualScale(this.calibratedManualScaleY ?? this.controlState.manualScaleY, MANUAL_TARGET_Y_SCALE);
    const targetX = manualX * currentScaleX;
    const targetY = manualY * currentScaleY;
    const distance = Math.hypot(targetX - position.x, targetY - position.y);
    if (distance > MANUAL_SCALE_CALIBRATE_DISTANCE) {
      return;
    }
    const sampleX = Math.abs(manualX) > 1 ? usableManualScaleSample(position.x / manualX) : null;
    const sampleY = Math.abs(manualY) > 1 ? usableManualScaleSample(position.y / manualY) : null;
    if (sampleX === null && sampleY === null) {
      return;
    }
    if (sampleX !== null) {
      this.calibratedManualScaleX = this.calibratedManualScaleX === null ? sampleX : this.calibratedManualScaleX * 0.8 + sampleX * 0.2;
    }
    if (sampleY !== null) {
      this.calibratedManualScaleY = this.calibratedManualScaleY === null ? sampleY : this.calibratedManualScaleY * 0.8 + sampleY * 0.2;
    }
    this.controlState.calibratedManualScaleX = this.calibratedManualScaleX;
    this.controlState.calibratedManualScaleY = this.calibratedManualScaleY;
    const now = Date.now();
    if (now - this.lastManualScaleCalibrationAt > 3000) {
      this.lastManualScaleCalibrationAt = now;
      console.log(
        `[INFO] manual-scale calibrated scale=(${(this.calibratedManualScaleX ?? currentScaleX).toFixed(2)},` +
        `${(this.calibratedManualScaleY ?? currentScaleY).toFixed(2)}) dist=${distance.toFixed(1)}`
      );
    }
  }

  updateFilteredUPosition(position, packetLength) {
    if (!position || !USE_U_POSITION) {
      return false;
    }
    if (!this.botPosition || TRUST_RAW_U_POSITION) {
      this.updateLivePosition(position);
      return true;
    }
    const dx = position.x - this.botPosition.x;
    const dy = position.y - this.botPosition.y;
    const distance = Math.hypot(dx, dy);
    if (distance > U_POSITION_ACCEPT_DISTANCE) {
      const now = Date.now();
      if (now - this.lastUPositionRejectLogAt > U_POSITION_LOG_MS) {
        this.lastUPositionRejectLogAt = now;
        console.log(
          `[INFO] u-position rejected dist=${distance.toFixed(1)} ` +
          `candidate=(${position.x.toFixed(1)},${position.y.toFixed(1)}) ` +
          `bot=(${this.botPosition.x.toFixed(1)},${this.botPosition.y.toFixed(1)}) len=${packetLength}`
        );
      }
      return false;
    }
    const blend = Math.max(0, Math.min(1, U_POSITION_BLEND));
    this.updateLivePosition({
      x: this.botPosition.x + dx * blend,
      y: this.botPosition.y + dy * blend,
      filtered: true
    });
    return true;
  }

  ensureRespawnWatchdog() {
    if (!AUTO_RESPAWN_ENABLED || this.respawnWatchTimer || AUTO_RESPAWN_NO_POSITION_MS <= 0) {
      return;
    }
    this.respawnWatchTimer = setInterval(() => {
      if (
        this.spawnLikelyAccepted &&
        !this.respawnTimer &&
        !this.rejected &&
        this.state === 'ready' &&
        TRUST_RAW_U_POSITION &&
        this.lastLivePositionAt &&
        Date.now() - this.lastLivePositionAt > AUTO_RESPAWN_NO_POSITION_MS
      ) {
        this.scheduleRespawn(`no-position-updates ${Date.now() - this.lastLivePositionAt}ms`);
        return;
      }
    }, Math.max(250, AUTO_RESPAWN_WATCH_MS));
  }

  scheduleClassUpgrade(reason) {
    if (this.upgradeStarted || !this.spawnLikelyAccepted || this.rejected) {
      return;
    }
    const pathSteps = tankPathFor(this.targetTank);
    if (!pathSteps.length) {
      this.upgradeStarted = true;
      this.log(`[OUT] upgrade skipped tank=${this.targetTank} reason=no-path`);
      const statTimer = setTimeout(() => {
        this.upgradeTimers.delete(statTimer);
        this.scheduleStatBuild('no-class-path');
      }, 900);
      this.upgradeTimers.add(statTimer);
      return;
    }
    this.upgradeStarted = true;
    this.log(`[OUT] upgrade schedule tank=${this.targetTank} steps=${pathSteps.map(formatUpgradeStep).join('')} via=${reason}`);
    let offset = 1200;
    for (const step of pathSteps) {
      const timer = setTimeout(() => {
        this.upgradeTimers.delete(timer);
        this.sendClassUpgradeStep(step);
      }, offset);
      this.upgradeTimers.add(timer);
      offset += 250;
    }
    const statTimer = setTimeout(() => {
      this.upgradeTimers.delete(statTimer);
      this.scheduleStatBuild('class-complete');
    }, offset + 900);
    this.upgradeTimers.add(statTimer);
  }

  sendClassUpgradeStep(step) {
    if (!this.spawnLikelyAccepted || this.rejected) {
      return;
    }
    if (Array.isArray(step)) {
      const click = buildUpgradeClickPackets(step);
      if (!click) {
        console.log(`[OUT] upgrade click-step invalid step=[${step.join(',')}] tank=${this.targetTank}`);
        return;
      }
      this.sendPacket(click.down);
      const releaseTimer = setTimeout(() => {
        this.upgradeTimers.delete(releaseTimer);
        this.sendPacket(click.up);
      }, UPGRADE_CLICK_RELEASE_MS);
      this.upgradeTimers.add(releaseTimer);
      console.log(`[OUT] upgrade click-step step=[${step.join(',')}] tank=${this.targetTank} xy=${click.x},${click.y} down=${click.down.toString('hex')} up=${click.up.toString('hex')}`);
      return;
    }
    const packet = buildClassUpgradePacket(step);
    if (!packet) {
      console.log(`[OUT] upgrade invalid-step step=${JSON.stringify(step)} tank=${this.targetTank}`);
      return;
    }
    if (this.sendPacket(packet)) {
      console.log(`[OUT] upgrade class key=${String(step).toLowerCase()} tank=${this.targetTank} hex=${packet.toString('hex')}`);
    }
  }

  scheduleStatBuild(reason) {
    if (this.statBuildStarted || !this.spawnLikelyAccepted || this.rejected) {
      return;
    }
    const feedMode = Boolean(this.controlState && this.controlState.feeding);
    const steps = buildStatUpgradeSteps(this.targetTank, feedMode);
    this.statBuildStarted = true;
    if (!steps.length) {
      this.log(`[OUT] stat-build skipped tank=${this.targetTank} reason=no-build`);
      return;
    }
    this.log(`[OUT] stat-build schedule tank=${this.targetTank} feed=${feedMode} count=${steps.length} summary=${summarizeStatUpgradeSteps(steps)} via=${reason}`);
    let offset = Math.floor(Math.random() * Math.max(0, STAT_BUILD_START_JITTER_MS + 1));
    for (const statIndex of steps) {
      const timer = setTimeout(() => {
        this.upgradeTimers.delete(timer);
        this.sendStatUpgradeStep(statIndex);
      }, offset);
      this.upgradeTimers.add(timer);
      offset += 90 + Math.floor(Math.random() * Math.max(0, STAT_BUILD_STEP_JITTER_MS + 1));
    }
  }

  sendStatUpgradeStep(statIndex) {
    if (!this.spawnLikelyAccepted || this.rejected) {
      return;
    }
    const packet = buildStatUpgradePacket(statIndex);
    if (!packet) {
      console.log(`[OUT] stat-build invalid stat=${statIndex} tank=${this.targetTank}`);
      return;
    }
    this.sendPacket(packet);
  }

  clearUpgradeTimers() {
    for (const timer of this.upgradeTimers) {
      clearTimeout(timer);
    }
    this.upgradeTimers.clear();
  }

  refreshManualSpread(now) {
    if (now >= this.nextManualTargetOffsetAt) {
      const base = randomDiskPoint(MANUAL_TARGET_SPREAD);
      const wobble = randomDiskPoint(MANUAL_TARGET_SPREAD_WOBBLE);
      this.manualTargetOffsetX = this.manualFormationOffsetX + base.x + wobble.x;
      this.manualTargetOffsetY = this.manualFormationOffsetY + base.y + wobble.y;
      this.nextManualTargetOffsetAt = now + randomBetween(
        MANUAL_TARGET_SPREAD_REFRESH_MS * 0.75,
        MANUAL_TARGET_SPREAD_REFRESH_MS * 1.25
      );
    }
    if (now >= this.nextManualAimOffsetAt) {
      const base = randomDiskPoint(MANUAL_AIM_SPREAD);
      const wobble = randomDiskPoint(MANUAL_AIM_SPREAD_WOBBLE);
      const fallback = randomAimVector(MANUAL_AIM_AXIS);
      this.manualAimOffsetX = base.x + wobble.x;
      this.manualAimOffsetY = base.y + wobble.y;
      this.manualFallbackAimX = fallback.x + this.manualAimOffsetX;
      this.manualFallbackAimY = fallback.y + this.manualAimOffsetY;
      this.nextManualAimOffsetAt = now + randomBetween(
        MANUAL_AIM_SPREAD_REFRESH_MS * 0.75,
        MANUAL_AIM_SPREAD_REFRESH_MS * 1.25
      );
    }
  }

  resolveMouseDown(rawMouseDown, now) {
    const pressed = Boolean(rawMouseDown);
    if (!pressed) {
      this.rawMouseDown = false;
      this.fireHoldStartedAt = 0;
      this.firePauseUntil = 0;
      this.nextFirePauseAt = 0;
      return false;
    }
    if (!this.rawMouseDown) {
      this.rawMouseDown = true;
      this.fireHoldStartedAt = now;
      this.fireStartDelayMs = Math.floor(Math.random() * Math.max(0, FIRE_STAGGER_MS + 1));
      this.nextFirePauseAt = now + randomBetween(FIRE_MICRO_PAUSE_INTERVAL_MIN_MS, FIRE_MICRO_PAUSE_INTERVAL_MAX_MS);
    }
    if (now - this.fireHoldStartedAt < this.fireStartDelayMs) {
      return false;
    }
    if (FIRE_MICRO_PAUSE_ENABLED) {
      if (now < this.firePauseUntil) {
        return false;
      }
      if (this.nextFirePauseAt > 0 && now >= this.nextFirePauseAt) {
        this.firePauseUntil = now + randomBetween(FIRE_MICRO_PAUSE_MIN_MS, FIRE_MICRO_PAUSE_MAX_MS);
        this.nextFirePauseAt = this.firePauseUntil + randomBetween(FIRE_MICRO_PAUSE_INTERVAL_MIN_MS, FIRE_MICRO_PAUSE_INTERVAL_MAX_MS);
        return false;
      }
    }
    return true;
  }

  updateControlState(message) {
    if (!message || message.type !== 'position') {
      return;
    }
    const now = Date.now();
    const manualMode = Boolean(message.manualMode);
    this.refreshManualSpread(now);
    const manualX = Number(message.manualX);
    const manualY = Number(message.manualY);
    this.controlState = {
      x: Number(message.x),
      y: Number(message.y),
      mouseX: Number(message.mouseX),
      mouseY: Number(message.mouseY),
      mouseDown: this.resolveMouseDown(message.mouseDown, now),
      rMouseDown: Boolean(message.rMouseDown),
      mouse: Boolean(message.mouse),
      feeding: Boolean(message.feeding),
      shift: Boolean(message.shift),
      autofire: Boolean(message.autofire),
      autospin: Boolean(message.autospin),
      manualMode,
      ignoreControllerAim: manualMode && MANUAL_AIM_WITH_MOVEMENT,
      manualX: manualMode && Number.isFinite(manualX) ? manualX + this.manualTargetOffsetX : manualX,
      manualY: manualMode && Number.isFinite(manualY) ? manualY + this.manualTargetOffsetY : manualY,
      manualTargetOffsetX: manualMode ? this.manualTargetOffsetX : 0,
      manualTargetOffsetY: manualMode ? this.manualTargetOffsetY : 0,
      manualAimOffsetX: this.manualAimOffsetX,
      manualAimOffsetY: this.manualAimOffsetY,
      manualFallbackAimX: this.manualFallbackAimX,
      manualFallbackAimY: this.manualFallbackAimY,
      manualAimWithMovement: MANUAL_AIM_WITH_MOVEMENT,
      manualScaleX: MANUAL_TARGET_X_SCALE,
      manualScaleY: MANUAL_TARGET_Y_SCALE,
      calibratedManualScaleX: MANUAL_SCALE_AUTO_CALIBRATE ? this.calibratedManualScaleX : null,
      calibratedManualScaleY: MANUAL_SCALE_AUTO_CALIBRATE ? this.calibratedManualScaleY : null,
      updatedAt: now
    };
    this.ensureMovementLoop();
  }

  ensureMovementLoop() {
    if (!MOVEMENT_ENABLED || this.movementTimer || MOVEMENT_INTERVAL_MS <= 0) {
      return;
    }
    const startLoop = () => {
      if (this.movementTimer || !MOVEMENT_ENABLED) {
        return;
      }
      this.sendMovementInput();
      this.movementTimer = setInterval(() => {
        this.sendMovementInput();
      }, this.movementIntervalMs);
    };
    if (this.movementPhaseDelayMs > 0) {
      this.movementTimer = setTimeout(() => {
        this.movementTimer = null;
        startLoop();
      }, this.movementPhaseDelayMs);
      return;
    }
    startLoop();
  }

  sendMovementInput() {
    if (!MOVEMENT_ENABLED || this.rejected || this.state !== 'ready' || !this.spawnLikelyAccepted || !this.controlState) {
      return;
    }
    const now = Date.now();
    const hasPersistentManualTarget =
      this.controlState.manualMode &&
      Number.isFinite(this.controlState.manualX) &&
      Number.isFinite(this.controlState.manualY);
    const autofire = Boolean(this.controlState.autofire);
    if (autofire !== this.lastAutofireWanted) {
      this.lastAutofireWanted = autofire;
      this.autofireChangeReadyAt = now + Math.floor(Math.random() * Math.max(0, FIRE_STAGGER_MS + 1));
    }
    if (autofire !== this.lastAutofireState && now >= this.autofireChangeReadyAt) {
      if (this.sendPacket(AUTOFIRE_TOGGLE_PACKET)) {
        this.log(`[OUT] autofire-toggle enabled=${autofire} hex=${AUTOFIRE_TOGGLE_PACKET.toString('hex')}`);
      }
      this.lastAutofireState = autofire;
    }
    const hasActiveControl =
      hasPersistentManualTarget ||
      Boolean(this.controlState.mouseDown) ||
      Boolean(this.controlState.rMouseDown) ||
      Boolean(this.controlState.autospin) ||
      autofire;
    const sendReleaseInput = this.lastInputHadActiveControl && !hasActiveControl;
    if (!hasActiveControl && !sendReleaseInput) {
      return;
    }
    if (!sendReleaseInput && !hasPersistentManualTarget && now - this.controlState.updatedAt > 1000) {
      return;
    }
    const throttleManualControl =
      hasPersistentManualTarget &&
      !sendReleaseInput &&
      MANUAL_CONTROL_INTERVAL_MS > 0 &&
      now - this.lastManualControlSentAt < MANUAL_CONTROL_INTERVAL_MS;
    if (throttleManualControl) {
      return;
    }
    const packet = buildInputPacket(this.controlState, this.botPosition);
    this.sendPacket(packet);
    this.lastInputHadActiveControl = hasActiveControl;
    if (hasPersistentManualTarget) {
      this.lastManualControlSentAt = now;
    }
    const hex = packet.toString('hex');
    if (packet.manualDebug && this.botPosition && packet.manualDebug.distance > 0) {
      const elapsedSeconds = this.lastPositionEstimateAt ? Math.min(0.25, Math.max(0, (now - this.lastPositionEstimateAt) / 1000)) : MOVEMENT_INTERVAL_MS / 1000;
      this.lastPositionEstimateAt = now;
      const step = Math.min(packet.manualDebug.distance * 0.25, MANUAL_TARGET_ESTIMATED_SPEED * elapsedSeconds);
      if (step > 0) {
        const ratio = step / packet.manualDebug.distance;
        this.botPosition = {
          x: this.botPosition.x + packet.manualDebug.dx * ratio,
          y: this.botPosition.y + packet.manualDebug.dy * ratio,
          estimated: true
        };
      }
    }
    if (packet.manualDebug && now - (this.lastManualDebugAt || 0) > 1000) {
      this.lastManualDebugAt = now;
      const d = packet.manualDebug;
      this.log(
        `[INFO] manual-target target=(${d.targetX.toFixed(1)},${d.targetY.toFixed(1)}) ` +
        `scale=(${d.scaleX.toFixed(1)},${d.scaleY.toFixed(1)}) ` +
        `targetInternal=(${d.targetInternalX.toFixed(1)},${d.targetInternalY.toFixed(1)}) ` +
        `drive=(${d.targetDriveX.toFixed(1)},${d.targetDriveY.toFixed(1)}) ` +
        `bot=(${d.botX.toFixed(1)},${d.botY.toFixed(1)}) est=${d.positionEstimated ? 1 : 0} ` +
        `delta=(${d.dx.toFixed(1)},${d.dy.toFixed(1)}) dist=${d.distance.toFixed(1)} stop=${d.stopDistance.toFixed(1)} ` +
        `pulse=${d.pulseDuty.toFixed(2)} move=(${d.movementX.toFixed(1)},${d.movementY.toFixed(1)}) ` +
        `aim=(${d.mouseX.toFixed(1)},${d.mouseY.toFixed(1)}) ` +
        `flags=0x${d.flags.toString(16).padStart(2, '0')} hex=${hex}`
      );
    }
    if (packet.aimDebug && now - (this.lastAimDebugAt || 0) > 1000) {
      this.lastAimDebugAt = now;
      const d = packet.aimDebug;
      this.log(
        `[INFO] aim-clamped raw=(${d.rawMouseX.toFixed(1)},${d.rawMouseY.toFixed(1)}) ` +
        `sent=(${d.mouseX.toFixed(1)},${d.mouseY.toFixed(1)})`
      );
    }
    if (hex !== this.lastMovementHex || now - this.lastMovementLogAt > 3000) {
      this.lastMovementHex = hex;
      this.lastMovementLogAt = now;
      this.log(`[OUT] movement bytes=${packet.length} hex=${hex}`);
    }
  }

  sendPacket(plaintext) {
    const plainBytes = Buffer.from(plaintext);
    const packet = encryptPacket(plainBytes, this.key, this.outboundCounter);
    const plainCommand = printableCommand(plainBytes[0]);
    if (plainCommand === 'R' || plainCommand === 'e' || plainCommand === 's' || plainCommand === 'k' || plainCommand === 'T' || plainCommand === 'p' || plainCommand === 'C' || plainCommand === 't') {
      appendNdjson('protocol-only-validation.ndjson', packetRecord('OUT', this.packetIndex + 1, plainBytes, {
        hash: this.hashFromUrl(),
        outboundCounter: this.outboundCounter
      }));
    }
    this.lastOutboundAt = Date.now();
    this.lastOutboundCommand = plainCommand || `0x${plainBytes[0].toString(16).padStart(2, '0')}`;
    this.outboundCounter += 1;
    return this.sendRaw(packet);
  }

  sendRaw(bytes) {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) {
      return false;
    }
    this.socket.send(bytes);
    return true;
  }

  onError(error) {
    console.error('WebSocket error:', error && error.message ? error.message : error);
  }

  onClose(code, reason) {
    this.cleanupTimers();
    const text = Buffer.isBuffer(reason) ? reason.toString('utf8') : String(reason || '');
    const earlyRetryable = !this.manualClose && !this.key && !this.spawnSentAt && code === 1006 && this.connectAttempt <= EARLY_RETRY_MAX;
    const counts = [...this.commandCounts.entries()]
      .sort((a, b) => a[0].localeCompare(b[0]))
      .map(([key, value]) => `${key}:${value}`)
      .join(', ');
    const urlHash = this.hashFromUrl();
    if (counts) {
      console.log(`Inbound command counts: ${counts}`);
    }
    if (this.modeInfo) {
      console.log(`Mode summary: mode=${this.modeInfo.mode || '?'} provider=${this.modeInfo.provider || '?'} hash=${urlHash || '?'}`);
    } else if (urlHash) {
      console.log(`Mode summary: hash=${urlHash}`);
    }
    if (this.spawnSentAt) {
      const commands = [...this.seenAfterSpawn].sort().join(',');
      const now = Date.now();
      const openAge = this.openedAt ? ((now - this.openedAt) / 1000).toFixed(1) : '?';
      const spawnAge = ((now - this.spawnSentAt) / 1000).toFixed(1);
      const lastInAge = this.lastInboundAt ? ((now - this.lastInboundAt) / 1000).toFixed(1) : '?';
      const lastOutAge = this.lastOutboundAt ? ((now - this.lastOutboundAt) / 1000).toFixed(1) : '?';
      console.log(`[timing] openAge=${openAge}s spawnAge=${spawnAge}s lastIn=${lastInAge}s(${this.lastInboundCommand || '?'}) lastOut=${lastOutAge}s(${this.lastOutboundCommand || '?'})`);
      console.log(`Post-spawn commands seen: ${commands || '(none)'}`);
      if (this.spawnLikelyAccepted) {
        console.log('Post-spawn status: likely spawned into live game-state, not handshake-only camera idle.');
      } else if (this.seenAfterSpawn.has('b') || this.seenAfterSpawn.has('C') || this.seenAfterSpawn.has('e')) {
        console.log('Post-spawn status: deeper than handshake-only; likely entity/game-state flow.');
      } else {
        console.log('Post-spawn status: inconclusive from current packet mix.');
      }
    }
    console.log(`WebSocket closed code=${code} reason=${text}`);
    if (earlyRetryable) {
      const retryDelay = EARLY_RETRY_DELAY_MS + Math.floor(Math.random() * EARLY_RETRY_DELAY_MS);
      console.log(`[retry] early close before handshake; retry ${this.connectAttempt}/${EARLY_RETRY_MAX} in ${retryDelay}ms`);
      this.retryTimer = setTimeout(() => {
        this.retryTimer = null;
        this.state = 'idle';
        this.socket = null;
        this.key = null;
        this.outboundCounter = 0;
        this.inboundCounter = 0;
        this.packetIndex = 0;
        this.connect().catch((error) => {
          console.error('Reconnect failed:', error && error.stack ? error.stack : error);
          process.exitCode = 1;
        });
      }, retryDelay);
    }
  }

  cleanupTimers() {
    if (this.pingTimer) {
      clearInterval(this.pingTimer);
      this.pingTimer = null;
    }
    if (this.movementTimer) {
      clearInterval(this.movementTimer);
      this.movementTimer = null;
    }
    if (this.respawnWatchTimer) {
      clearInterval(this.respawnWatchTimer);
      this.respawnWatchTimer = null;
    }
    this.clearUpgradeTimers();
    if (this.spawnTimer) {
      clearTimeout(this.spawnTimer);
      this.spawnTimer = null;
    }
    if (this.respawnTimer) {
      clearTimeout(this.respawnTimer);
      this.respawnTimer = null;
    }
    if (this.shutdownTimer) {
      clearTimeout(this.shutdownTimer);
      this.shutdownTimer = null;
    }
    this.clearValidationWFallback();
    this.clearSpawnFallback();
    this.clearDelayedValidationR();
    this.clearDelayedValidationE();
    this.clearModeInfoValidationDelay();
  }

  close() {
    this.manualClose = true;
    this.cleanupTimers();
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.socket && this.socket.readyState < WebSocket.CLOSING) {
      this.socket.close();
    }
  }

  hashFromUrl() {
    if (this.captureHash) {
      return this.captureHash;
    }
    if (this.socketMeta && this.socketMeta.hash) {
      return this.socketMeta.hash;
    }
    return null;
  }
}

const socketUrl =
  process.env.ARRAS_SOCKET_URL ||
  process.argv[2] ||
  latestSocketUrlFromLog();

if (!socketUrl) {
  throw new Error('No socket URL available. Set ARRAS_SOCKET_URL or capture a fresh protocol-packets.ndjson first.');
}

const client = new ProtocolOnlyRandomClient(socketUrl, {
  durationMs: process.env.ARRAS_TEST_DURATION_MS === undefined ? 0 : Number(process.env.ARRAS_TEST_DURATION_MS),
  name: process.env.ARRAS_BOT_NAME || '',
  party: process.env.ARRAS_PARTY || derivePartyFromHash(process.env.ARRAS_CAPTURE_HASH),
  pingMs: Number(process.env.ARRAS_PING_MS || 400),
  spawnDelayMs: Number(process.env.ARRAS_SPAWN_DELAY_MS || 750),
  spawnFlag: Number(process.env.ARRAS_SPAWN_FLAG || 1),
  verifySignature: process.env.ARRAS_VERIFY_SIGNATURE !== '0',
  logU: process.env.ARRAS_LOG_U === '1'
});

if (typeof process.on === 'function') {
  process.on('message', (message) => {
    if (message && message.type === 'tankselect') {
      client.updateTankSelection(message);
      return;
    }
    client.updateControlState(message);
  });
}

client.connect().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
