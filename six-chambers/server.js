'use strict';
/*
 * Six Chambers server: serves the game from ./public and relays online rooms over WebSocket (/ws).
 * No dependencies. Needs Node 18 or newer.
 *
 * The relay is deliberately dumb: it groups connections into named rooms and forwards messages.
 * All game rules run in the host player's browser.
 *
 * Environment variables:
 *   PORT              port to listen on (default 3000)
 *   ALLOWED_ORIGINS   comma-separated list of origins allowed to open a WebSocket,
 *                     e.g. https://sixchambers.example.com (default: any origin)
 *   TRUST_PROXY       set to 1 when behind a proxy/load balancer, so per-IP limits use X-Forwarded-For
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = parseInt(process.env.PORT || '3000', 10);
const ALLOWED = (process.env.ALLOWED_ORIGINS || '').split(',').map(function (s) { return s.trim(); }).filter(Boolean);
const TRUST_PROXY = process.env.TRUST_PROXY === '1';
const PUBLIC = path.join(__dirname, 'public');

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const MAX_FRAME = 16 * 1024;          // bytes per incoming frame / message
const MAX_DATA = 4096;                // bytes of JSON per emitted payload
const MAX_ROOMS_PER_CONN = 3;
const MAX_PEERS_PER_ROOM = 8;
const MAX_ROOMS = 5000;
const MAX_CONN_PER_IP = 20;
const RATE_PER_SEC = 40, BURST = 80;  // messages per connection
const ROOM_RE = /^[a-z0-9][a-z0-9_.-]{0,47}$/;
const TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;

const rooms = new Map();              // room name -> Set of connections
const perIp = new Map();              // ip -> open connection count
let nextId = 0;

/* ---------- static files ---------- */
const TYPES = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8', '.svg': 'image/svg+xml', '.png': 'image/png', '.ico': 'image/x-icon', '.json': 'application/json', '.txt': 'text/plain; charset=utf-8' };
const CSP = [
  "default-src 'self'",
  "script-src 'self' 'unsafe-inline' https://cdnjs.cloudflare.com",
  "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
  "font-src https://fonts.gstatic.com",
  "img-src 'self' data:",
  "connect-src 'self' ws: wss:",
  "frame-ancestors 'none'"
].join('; ');

function securityHeaders(extra) {
  return Object.assign({
    'Content-Security-Policy': CSP,
    'X-Content-Type-Options': 'nosniff',
    'Referrer-Policy': 'no-referrer',
    'X-Frame-Options': 'DENY'
  }, extra || {});
}

const server = http.createServer(function (req, res) {
  let url;
  try { url = decodeURIComponent((req.url || '/').split('?')[0]); } catch (e) { res.writeHead(400); return res.end(); }
  if (url === '/healthz') { res.writeHead(200, securityHeaders({ 'Content-Type': 'text/plain' })); return res.end('ok'); }
  if (req.method !== 'GET' && req.method !== 'HEAD') { res.writeHead(405, { Allow: 'GET, HEAD' }); return res.end(); }
  if (url === '/') url = '/index.html';
  const file = path.normalize(path.join(PUBLIC, url));
  if (file !== PUBLIC && file.indexOf(PUBLIC + path.sep) !== 0) { res.writeHead(403); return res.end(); }
  fs.readFile(file, function (err, buf) {
    if (err) { res.writeHead(404, securityHeaders({ 'Content-Type': 'text/plain' })); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, securityHeaders({
      'Content-Type': TYPES[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600'
    }));
    res.end(req.method === 'HEAD' ? undefined : buf);
  });
});

/* ---------- minimal WebSocket (RFC 6455) ---------- */
function frame(op, payload) {
  const len = payload.length;
  let head;
  if (len < 126) { head = Buffer.alloc(2); head[1] = len; }
  else if (len < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(len, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(0, 2); head.writeUInt32BE(len, 6); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
function send(c, obj) {
  if (c.closed) return;
  if (c.socket.writableLength > 1024 * 1024) return close(c, 1013, 'slow consumer');
  try { c.socket.write(frame(0x1, Buffer.from(JSON.stringify(obj)))); } catch (e) { /* socket gone */ }
}
function close(c, code, reason) {
  if (c.closed) return;
  const r = Buffer.from(reason || '');
  const p = Buffer.alloc(2 + r.length); p.writeUInt16BE(code || 1000, 0); r.copy(p, 2);
  try { c.socket.write(frame(0x8, p)); } catch (e) { /* ignore */ }
  try { c.socket.end(); } catch (e) { /* ignore */ }
  setTimeout(function () { try { c.socket.destroy(); } catch (e) { /* ignore */ } }, 1000).unref();
  cleanup(c);
}
function cleanup(c) {
  if (c.closed) return;
  c.closed = true;
  c.rooms.forEach(function (name) { leaveRoom(c, name); });
  c.rooms.clear();
  const n = (perIp.get(c.ip) || 1) - 1;
  if (n <= 0) perIp.delete(c.ip); else perIp.set(c.ip, n);
}
function leaveRoom(c, name) {
  const set = rooms.get(name);
  c.rooms.delete(name);
  if (!set) return;
  set.delete(c);
  if (set.size === 0) rooms.delete(name);
  else set.forEach(function (o) { send(o, { t: 'peer-', room: name, peer: c.id }); });
}
function onData(c, chunk) {
  c.buf = Buffer.concat([c.buf, chunk]);
  if (c.buf.length > MAX_FRAME * 2 + 16) return close(c, 1009, 'too big');
  for (;;) {
    if (c.buf.length < 2) return;
    const b0 = c.buf[0], b1 = c.buf[1];
    const fin = !!(b0 & 0x80), op = b0 & 0x0f, masked = !!(b1 & 0x80);
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; if (c.buf.readUInt32BE(2) !== 0) return close(c, 1009, 'too big'); len = c.buf.readUInt32BE(6); off = 10; }
    if (!masked) return close(c, 1002, 'unmasked frame');
    if (len > MAX_FRAME) return close(c, 1009, 'too big');
    if (c.buf.length < off + 4 + len) return;
    const mask = c.buf.subarray(off, off + 4);
    const payload = Buffer.from(c.buf.subarray(off + 4, off + 4 + len));
    for (let i = 0; i < len; i++) payload[i] ^= mask[i & 3];
    c.buf = c.buf.subarray(off + 4 + len);
    onFrame(c, fin, op, payload);
    if (c.closed) return;
  }
}
function onFrame(c, fin, op, payload) {
  if (op === 0x8) return close(c, 1000, '');
  if (op === 0x9) { try { c.socket.write(frame(0xA, payload)); } catch (e) { /* ignore */ } return; }
  if (op === 0xA) { c.alive = true; return; }
  if (op === 0x1) c.frags = [payload];
  else if (op === 0x0 && c.frags) c.frags.push(payload);
  else return close(c, 1002, 'bad opcode');
  if (!fin) return;
  const text = Buffer.concat(c.frags).toString('utf8'); c.frags = null;
  let msg; try { msg = JSON.parse(text); } catch (e) { return close(c, 1003, 'bad json'); }
  if (!msg || typeof msg !== 'object') return;
  c.alive = true;
  const now = Date.now();
  c.tokens = Math.min(BURST, c.tokens + (now - c.last) / 1000 * RATE_PER_SEC); c.last = now;
  if (c.tokens < 1) { if (++c.drops > 300) close(c, 1008, 'rate limit'); return; }
  c.tokens -= 1;
  handle(c, msg);
}

/* ---------- room protocol ---------- */
function handle(c, m) {
  const room = typeof m.room === 'string' ? m.room : '';
  if (m.t === 'join') {
    if (!ROOM_RE.test(room)) return send(c, { t: 'err', room: room, code: 'invalid_argument' });
    let set = rooms.get(room);
    if (set && set.has(c)) return send(c, { t: 'joined', room: room, you: c.id, peers: Array.from(set).filter(function (o) { return o !== c; }).map(function (o) { return o.id; }) });
    if (c.rooms.size >= MAX_ROOMS_PER_CONN || (set && set.size >= MAX_PEERS_PER_ROOM) || (!set && rooms.size >= MAX_ROOMS)) return send(c, { t: 'err', room: room, code: 'limit_reached' });
    if (!set) { set = new Set(); rooms.set(room, set); }
    const others = Array.from(set);
    set.add(c); c.rooms.add(room);
    send(c, { t: 'joined', room: room, you: c.id, peers: others.map(function (o) { return o.id; }) });
    others.forEach(function (o) { send(o, { t: 'peer+', room: room, peer: c.id }); });
  } else if (m.t === 'emit') {
    const set = rooms.get(room);
    if (!set || !set.has(c)) return;
    if (typeof m.topic !== 'string' || !TOPIC_RE.test(m.topic)) return;
    if (m.data !== undefined && Buffer.byteLength(JSON.stringify(m.data)) > MAX_DATA) return;
    const out = { t: 'msg', room: room, topic: m.topic, data: m.data, peer: c.id };
    set.forEach(function (o) { if (o !== c) send(o, out); });
  } else if (m.t === 'leave') {
    if (c.rooms.has(room)) leaveRoom(c, room);
  }
}

server.on('upgrade', function (req, socket) {
  const urlPath = (req.url || '').split('?')[0];
  const key = req.headers['sec-websocket-key'];
  if (urlPath !== '/ws' || !key || req.headers.upgrade === undefined || String(req.headers.upgrade).toLowerCase() !== 'websocket' || req.headers['sec-websocket-version'] !== '13') {
    socket.write('HTTP/1.1 400 Bad Request\r\nConnection: close\r\n\r\n'); return socket.destroy();
  }
  if (ALLOWED.length && ALLOWED.indexOf(req.headers.origin) < 0) {
    socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return socket.destroy();
  }
  let ip = socket.remoteAddress || '?';
  if (TRUST_PROXY && req.headers['x-forwarded-for']) ip = String(req.headers['x-forwarded-for']).split(',')[0].trim();
  if ((perIp.get(ip) || 0) >= MAX_CONN_PER_IP) {
    socket.write('HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\n\r\n'); return socket.destroy();
  }
  const accept = crypto.createHash('sha1').update(key + GUID).digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + accept + '\r\n\r\n');
  socket.setNoDelay(true);
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  const c = { socket: socket, id: (++nextId).toString(36) + crypto.randomBytes(4).toString('hex'), ip: ip, rooms: new Set(), buf: Buffer.alloc(0), frags: null, alive: true, closed: false, tokens: BURST, last: Date.now(), drops: 0 };
  socket.on('data', function (chunk) { onData(c, chunk); });
  socket.on('close', function () { cleanup(c); });
  socket.on('error', function () { cleanup(c); });
});

// keep connections alive through proxies and drop dead ones
const beat = setInterval(function () {
  rooms.forEach(function (set) {
    set.forEach(function (c) {
      if (!c.alive) { c.socket.destroy(); cleanup(c); return; }
      c.alive = false;
      try { c.socket.write(frame(0x9, Buffer.alloc(0))); } catch (e) { /* ignore */ }
    });
  });
}, 25000);
beat.unref();

server.listen(PORT, function () { console.log('Six Chambers listening on http://localhost:' + PORT); });
function shutdown() { server.close(); process.exit(0); }
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
