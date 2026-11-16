/*
 * Browser adapter: gives the game the same "room" calls it uses inside claude.ai
 * (join, emit, on, peers, onPeers, leave) on top of a plain WebSocket to server.js.
 *
 * By default it connects to the same host that served the page (/ws).
 * To use a relay on another host, set <meta name="six-ws-url" content="wss://relay.example.com/ws">.
 */
(function () {
  'use strict';
  var TOPIC_RE = /^[a-z][a-z0-9_.-]{0,47}$/;
  var sock = null, opening = null, rooms = {};
  var bcastHandlers = [], admHandlers = [], listening = false, backoff = 2000, reqId = 0, reqs = {}, adminToken = null;
  try { adminToken = sessionStorage.getItem('sc-adm') || null; } catch (e) {}

  function wsUrl() {
    var m = document.querySelector('meta[name="six-ws-url"]');
    if (m && m.content) return m.content;
    return (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/ws';
  }
  function connect() {
    if (sock && sock.readyState === 1) return Promise.resolve(sock);
    if (opening) return opening;
    opening = new Promise(function (resolve, reject) {
      var s;
      try { s = new WebSocket(wsUrl()); } catch (e) { opening = null; return reject(e); }
      var timer = setTimeout(function () { try { s.close(); } catch (e) {} opening = null; reject(new Error('timeout')); }, 8000);
      s.onopen = function () {
        clearTimeout(timer); sock = s; opening = null; backoff = 2000; resolve(s);
        if (adminToken) request({ t: 'auth', token: adminToken }).catch(function () { adminToken = null; try { sessionStorage.removeItem('sc-adm'); } catch (e) {} });
      };
      s.onerror = function () { clearTimeout(timer); opening = null; reject(new Error('socket')); };
      s.onmessage = onMessage;
      s.onclose = function () { if (sock === s) onClosed(); };
    });
    return opening;
  }

  function peerObj(id, meId) { return { peer: id, isMe: id === meId, sameTab: id === meId, kind: 'viewer', guest: false, by: null, presence: {}, updatedAt: Date.now() }; }
  function listOf(r) { return Object.keys(r.peerMap).map(function (id) { return peerObj(id, r.meId); }); }
  function notify(r, joined, left) {
    var change = { peers: listOf(r), joined: joined, left: left, updated: [] };
    r.peerHandlers.slice().forEach(function (h) { try { h(change); } catch (e) { console.error(e); } });
  }
  function dispatch(r, topic, data, peer, isMe) {
    (r.handlers[topic] || []).slice().forEach(function (h) {
      try { h({ topic: topic, data: data, peer: peer, isMe: isMe, sameTab: isMe, kind: 'viewer', guest: false, by: null }); } catch (e) { console.error(e); }
    });
  }

  function onMessage(ev) {
    var m; try { m = JSON.parse(ev.data); } catch (e) { return; }
    if (m.t === 'bcast') { bcastHandlers.slice().forEach(function (h) { try { h(String(m.text || '')); } catch (e) {} }); return; }
    if (m.t === 'a.ok' || m.t === 'a.err') { var q = reqs[m.id]; if (q) { delete reqs[m.id]; clearTimeout(q.timer); if (m.t === 'a.ok') q.resolve(m); else q.reject({ code: m.code || 'error' }); } return; }
    var r = rooms[m.room]; if (!r) return;
    if (m.t === 'adm') { admHandlers.slice().forEach(function (h) { try { h(m.cmd, m.data, m.by, r.meId); } catch (e) { console.error(e); } }); return; }
    if (m.t === 'joined') {
      r.meId = m.you; r.peerMap = {}; r.peerMap[m.you] = true;
      (m.peers || []).forEach(function (p) { r.peerMap[p] = true; });
      if (r.pending) { var p = r.pending; r.pending = null; clearTimeout(p.timer); p.resolve(r.api); }
    } else if (m.t === 'peer+') {
      if (!r.peerMap[m.peer]) { r.peerMap[m.peer] = true; notify(r, [peerObj(m.peer, r.meId)], []); }
    } else if (m.t === 'peer-') {
      if (r.peerMap[m.peer]) { delete r.peerMap[m.peer]; notify(r, [], [peerObj(m.peer, r.meId)]); }
    } else if (m.t === 'msg') {
      dispatch(r, m.topic, m.data, m.peer, false);
    } else if (m.t === 'err') {
      if (r.pending) { var q = r.pending; r.pending = null; clearTimeout(q.timer); delete rooms[r.name]; q.reject({ code: m.code || 'upstream_error', message: 'join refused' }); }
    }
  }
  // connection lost: everyone else is gone as far as the game can tell
  function onClosed() {
    sock = null;
    Object.keys(reqs).forEach(function (id) { var q = reqs[id]; delete reqs[id]; clearTimeout(q.timer); q.reject({ code: 'upstream_error' }); });
    if (listening) { setTimeout(function () { connect().catch(function () {}); }, backoff); backoff = Math.min(backoff * 2, 30000); }
    Object.keys(rooms).forEach(function (name) {
      var r = rooms[name], gone = Object.keys(r.peerMap).filter(function (id) { return id !== r.meId; });
      if (r.pending) { var p = r.pending; r.pending = null; clearTimeout(p.timer); p.reject({ code: 'upstream_error', message: 'connection lost' }); }
      r.closed = true; delete rooms[name];
      gone.forEach(function (id) { delete r.peerMap[id]; });
      if (gone.length) notify(r, [], gone.map(function (id) { return peerObj(id, r.meId); }));
      r.connHandlers.slice().forEach(function (h) { try { h(false); } catch (e) {} });
    });
  }

  function makeRoom(name) {
    var r = { name: name, meId: null, peerMap: {}, handlers: {}, peerHandlers: [], connHandlers: [], pending: null, closed: false };
    r.api = {
      name: name,
      emit: function (topic, data) {
        if (typeof topic !== 'string' || !TOPIC_RE.test(topic)) return Promise.reject({ code: 'invalid_argument', message: 'bad topic' });
        var text; try { text = JSON.stringify(data === undefined ? null : data); } catch (e) { return Promise.reject({ code: 'invalid_argument', message: 'not JSON' }); }
        if (text.length > 4096) return Promise.reject({ code: 'invalid_argument', message: 'too large' });
        if (r.closed || !sock || sock.readyState !== 1) return Promise.reject({ code: 'upstream_error', message: 'not connected' });
        sock.send(JSON.stringify({ t: 'emit', room: name, topic: topic, data: data }));
        Promise.resolve().then(function () { if (!r.closed) dispatch(r, topic, data, r.meId, true); });
        return Promise.resolve();
      },
      on: function (topic, h) {
        if (typeof h !== 'function') throw new TypeError('handler must be a function');
        (r.handlers[topic] = r.handlers[topic] || []).push(h);
        return function () { r.handlers[topic] = (r.handlers[topic] || []).filter(function (x) { return x !== h; }); };
      },
      presence: function () { return Promise.resolve(); },
      peers: function () { return listOf(r); },
      onPeers: function (h) {
        r.peerHandlers.push(h);
        Promise.resolve().then(function () { if (r.peerHandlers.indexOf(h) > -1) h({ peers: listOf(r), joined: listOf(r), left: [], updated: [] }); });
        return function () { r.peerHandlers = r.peerHandlers.filter(function (x) { return x !== h; }); };
      },
      connected: function () { return !r.closed && !!sock && sock.readyState === 1; },
      onConnection: function (h) {
        r.connHandlers.push(h); Promise.resolve().then(function () { h(r.api.connected()); });
        return function () { r.connHandlers = r.connHandlers.filter(function (x) { return x !== h; }); };
      },
      leave: function () {
        if (rooms[name] === r) { delete rooms[name]; if (sock && sock.readyState === 1) { try { sock.send(JSON.stringify({ t: 'leave', room: name })); } catch (e) {} } }
        r.closed = true; return Promise.resolve();
      }
    };
    return r;
  }

  function request(msg) {
    return connect().then(function (s) {
      return new Promise(function (resolve, reject) {
        var id = ++reqId; msg.id = id;
        reqs[id] = { resolve: resolve, reject: reject, timer: setTimeout(function () { delete reqs[id]; reject({ code: 'timeout' }); }, 8000) };
        s.send(JSON.stringify(msg));
      });
    }, function () { return Promise.reject({ code: 'upstream_error' }); });
  }

  window.SixSocket = {
    /* keep a connection open (auto-reconnect) so site-wide broadcasts reach people outside a room too */
    listen: function () { listening = true; connect().catch(function () { setTimeout(function () { if (listening) connect().catch(function () {}); }, backoff); backoff = Math.min(backoff * 2, 30000); }); },
    onBroadcast: function (h) { bcastHandlers.push(h); },
    onAdmin: function (h) { admHandlers.push(h); },
    admin: {
      isAuthed: function () { return !!adminToken; },
      login: function (token) { return request({ t: 'auth', token: token }).then(function () { adminToken = token; try { sessionStorage.setItem('sc-adm', token); } catch (e) {} }); },
      logout: function () { adminToken = null; try { sessionStorage.removeItem('sc-adm'); } catch (e) {} },
      rooms: function () { return request({ t: 'a.rooms' }); },
      broadcast: function (text) { return request({ t: 'a.bcast', text: text }); },
      command: function (room, cmd, data) { return request({ t: 'a.cmd', room: room, cmd: cmd, data: data }); }
    },
    room: function () {
      return {
        join: function (name) {
          if (rooms[name]) return Promise.resolve(rooms[name].api);
          return connect().then(function (s) {
            return new Promise(function (resolve, reject) {
              var r = makeRoom(name); rooms[name] = r;
              r.pending = { resolve: resolve, reject: reject, timer: setTimeout(function () { r.pending = null; delete rooms[name]; reject({ code: 'upstream_error', message: 'no answer' }); }, 10000) };
              s.send(JSON.stringify({ t: 'join', room: name }));
            });
          }, function () { return Promise.reject({ code: 'upstream_error', message: 'cannot reach the game server' }); });
        }
      };
    }
  };
})();
