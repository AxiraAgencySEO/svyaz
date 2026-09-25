import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { createHash, createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { resolve, extname, sep } from 'node:path';
import { WebSocketServer, WebSocket } from 'ws';

const id = () => randomBytes(18).toString('base64url');
const digest = value => createHash('sha256').update(String(value)).digest();
const same = (a, b) => timingSafeEqual(digest(a), digest(b));
const send = (ws, message) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message)); };

export function createRoomServer(options = {}) {
  const rooms = new Map();
  const root = resolve(fileURLToPath(new URL('../public/', import.meta.url)));
  const allowedOrigins = new Set(options.allowedOrigins || (process.env.ALLOWED_ORIGINS || 'http://localhost:8787,http://127.0.0.1:8787').split(',').map(x => x.trim()));
  const turnUrls = (process.env.TURN_URLS || '').split(',').filter(Boolean);
  const turnSecret = process.env.TURN_SECRET;
  const server = http.createServer(async (req, res) => {
    res.setHeader('Referrer-Policy', 'no-referrer');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Cache-Control', 'no-store');
    if (req.url === '/health') { res.writeHead(200, { 'Content-Type': 'application/json' }); return res.end(JSON.stringify({ ok: true, turn: Boolean(turnUrls.length && turnSecret) })); }
    try {
      const url = new URL(req.url, 'http://localhost');
      const path = resolve(root, '.' + decodeURIComponent(url.pathname === '/' ? '/index.html' : url.pathname));
      if (!path.startsWith(root + sep) || !['.html', '.css', '.js', '.svg'].includes(extname(path))) { res.writeHead(404); return res.end(); }
      const data = await readFile(path);
      const mime = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };
      res.writeHead(200, { 'Content-Type': `${mime[extname(path)]}; charset=utf-8` }); res.end(data);
    } catch { res.writeHead(404); res.end('Not found'); }
  });
  const wss = new WebSocketServer({ noServer: true, maxPayload: 96 * 1024, perMessageDeflate: false });
  server.on('upgrade', (req, socket, head) => {
    if (req.url !== '/ws' || !allowedOrigins.has(req.headers.origin) || wss.clients.size >= 1000) {
      socket.write('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'); return socket.destroy();
    }
    wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req));
  });
  function iceConfig(clientId) {
    const iceServers = [{ urls: process.env.STUN_URL || 'stun:stun.l.google.com:19302' }];
    if (turnUrls.length && turnSecret) {
      const username = `${Math.floor(Date.now() / 1000) + 86400}:${clientId}`;
      iceServers.push({ urls: turnUrls, username, credential: createHmac('sha1', turnSecret).update(username).digest('base64') });
    }
    return { iceServers, hasTurn: Boolean(turnUrls.length && turnSecret) };
  }
  function roster(room) {
    return [...room.members.values()].filter(m => m.ws).map(m => ({ id: m.id, name: m.name, publishing: m.publishing, sourceToken: m.sourceToken }));
  }
  function broadcast(room) {
    const members = roster(room);
    for (const c of room.clients) {
      if (c.role === 'member') send(c, { type: 'roster', members });
      else {
        const source = room.members.get(c.sourceId);
        send(c, { type: 'source-state', sourceId: c.sourceId, publishing: Boolean(source?.ws && source.publishing) });
      }
    }
  }
  function unwatch(ws, sourceId) {
    if (!ws.watching.delete(sourceId)) return;
    const publisher = ws.room?.members.get(sourceId)?.ws;
    if (publisher) send(publisher, { type: 'unwatch', viewerId: ws.clientId });
  }
  function watch(ws, sourceId) {
    if (ws.role === 'viewer' && ws.sourceId !== sourceId) throw new Error('Нет доступа к этой камере.');
    const source = ws.room.members.get(sourceId);
    if (!source || sourceId === ws.memberId) throw new Error('Камера недоступна.');
    if (ws.watching.has(sourceId)) return;
    ws.watching.add(sourceId);
    if (source.ws && source.publishing) send(source.ws, { type: 'watch', viewerId: ws.clientId });
  }
  const fail = (ws, message) => send(ws, { type: 'error', message });
  wss.on('connection', ws => {
    ws.clientId = id(); ws.watching = new Set(); ws.alive = true; ws.messageCount = 0; ws.windowStart = Date.now();
    ws.authTimer = setTimeout(() => { if (!ws.room) ws.close(1008, 'Join timeout'); }, 15000);
    ws.on('pong', () => { ws.alive = true; });
    ws.on('error', () => {});
    ws.on('message', raw => {
      try {
        if (Date.now() - ws.windowStart > 10000) { ws.windowStart = Date.now(); ws.messageCount = 0; }
        if (++ws.messageCount > 250) return ws.close(1008, 'Rate limit');
        const msg = JSON.parse(raw.toString());
        if (!msg || typeof msg !== 'object') throw new Error('Неверный запрос.');
        if (['create', 'join', 'view'].includes(msg.type)) {
          if (ws.room) throw new Error('Вы уже подключены.');
          if (msg.type !== 'view' && (typeof msg.name !== 'string' || !msg.name.trim())) throw new Error('Введите имя.');
          let room;
          let key;
          if (msg.type === 'create') {
            if (rooms.size >= 200) throw new Error('Сервер занят. Попробуйте позже.');
            key = id() + id();
            room = { id: id(), key, members: new Map(), clients: new Set(), emptySince: 0 };
            rooms.set(room.id, room);
          } else {
            room = rooms.get(msg.roomId);
            if (!room) throw new Error('Комната закрыта или не существует. Попросите новое приглашение.');
            if (msg.type === 'join' && !same(room.key, msg.key)) throw new Error('Неверная ссылка приглашения.');
          }
          if (msg.type === 'view') {
            const source = room.members.get(msg.sourceId);
            if (!source || !same(source.sourceToken, msg.token)) throw new Error('Ссылка камеры недействительна.');
            if ([...room.clients].filter(c => c.role === 'viewer').length >= 24) throw new Error('Достигнут лимит получателей комнаты.');
            ws.role = 'viewer'; ws.sourceId = source.id;
          } else {
            const name = typeof msg.name === 'string' ? msg.name.trim().slice(0, 32) : '';
            if (!name) throw new Error('Введите имя.');
            let member = msg.resumeId ? room.members.get(msg.resumeId) : null;
            if (member && !same(member.resumeToken, msg.resumeToken)) throw new Error('Невозможно восстановить участника.');
            if (!member?.ws && [...room.members.values()].filter(m => m.ws).length >= 6) throw new Error('В комнате уже 6 участников.');
            if (!member) {
              if (room.members.size >= 100) throw new Error('Создайте новую комнату.');
              member = { id: id(), resumeToken: id(), sourceToken: id() + id() };
              room.members.set(member.id, member);
            }
            if (member.ws) { member.ws.replaced = true; member.ws.close(4001, 'Reconnected elsewhere'); }
            Object.assign(member, { name, ws, publishing: false });
            ws.role = 'member'; ws.memberId = member.id;
          }
          ws.room = room; room.clients.add(ws); room.emptySince = 0; clearTimeout(ws.authTimer);
          const member = room.members.get(ws.memberId);
          send(ws, { type: 'joined', role: ws.role, roomId: room.id, ...(member ? { key: room.key, memberId: member.id, resumeToken: member.resumeToken, members: roster(room) } : {}), ...iceConfig(ws.clientId) });
          if (ws.role === 'viewer') watch(ws, ws.sourceId);
          broadcast(room); return;
        }
        const room = ws.room;
        if (!room || ws.replaced) throw new Error('Сначала войдите в комнату.');
        if (msg.type === 'publish') {
          if (ws.role !== 'member') throw new Error('Нет права передавать камеру.');
          const member = room.members.get(ws.memberId);
          const changed = member.publishing !== Boolean(msg.enabled);
          member.publishing = Boolean(msg.enabled); broadcast(room);
          if (changed && member.publishing) {
            for (const viewer of room.clients) if (viewer.watching.has(member.id)) send(ws, { type: 'watch', viewerId: viewer.clientId });
          }
        } else if (msg.type === 'watch') watch(ws, msg.sourceId);
        else if (msg.type === 'unwatch') unwatch(ws, msg.sourceId);
        else if (msg.type === 'signal') {
          const target = [...room.clients].find(c => c.clientId === msg.targetId || (c.role === 'member' && c.memberId === msg.targetId));
          if (!target) return;
          const publishingToTarget = ws.role === 'member' && room.members.get(ws.memberId)?.publishing && target.watching.has(ws.memberId);
          const receivingFromTarget = target.role === 'member' && ws.watching.has(target.memberId);
          const kind = msg.description?.type;
          if (kind && !((kind === 'offer' && publishingToTarget) || (kind === 'answer' && receivingFromTarget))) throw new Error('Нет доступа к соединению.');
          if (!kind && !(publishingToTarget || receivingFromTarget)) throw new Error('Нет доступа к соединению.');
          if (msg.description && (typeof msg.description.sdp !== 'string' || msg.description.sdp.length > 65536)) throw new Error('Неверное описание видео.');
          if (typeof msg.callId !== 'string' || msg.callId.length > 64) throw new Error('Неверный идентификатор соединения.');
          send(target, { type: 'signal', fromId: ws.clientId, sourceId: ws.memberId, callId: msg.callId, description: msg.description, candidate: msg.candidate });
        } else if (msg.type === 'rotate-source') {
          if (ws.role !== 'member') throw new Error('Нет права менять ссылку.');
          const member = room.members.get(ws.memberId); member.sourceToken = id() + id();
          for (const viewer of room.clients) if (viewer.role === 'viewer' && viewer.sourceId === member.id) { unwatch(viewer, member.id); viewer.close(4003, 'Link revoked'); }
          broadcast(room); send(ws, { type: 'rotated' });
        } else if (msg.type === 'ice-refresh') send(ws, { type: 'ice-config', ...iceConfig(ws.clientId) });
        else throw new Error('Неизвестный запрос.');
      } catch (error) { fail(ws, error instanceof SyntaxError ? 'Неверный запрос.' : error.message); }
    });
    ws.on('close', () => {
      clearTimeout(ws.authTimer);
      const room = ws.room; if (!room) return;
      for (const sourceId of [...ws.watching]) unwatch(ws, sourceId);
      room.clients.delete(ws);
      const member = room.members.get(ws.memberId);
      if (member?.ws === ws) { member.ws = null; member.publishing = false; }
      if (!room.clients.size) room.emptySince = Date.now();
      broadcast(room);
    });
  });
  const interval = setInterval(() => {
    for (const ws of wss.clients) { if (!ws.alive) ws.terminate(); else { ws.alive = false; ws.ping(); } }
    for (const [key, room] of rooms) if (room.emptySince && Date.now() - room.emptySince > 300000) rooms.delete(key);
  }, 20000);
  interval.unref();
  server.on('close', () => { clearInterval(interval); for (const ws of wss.clients) ws.terminate(); wss.close(); });
  return { server, wss };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { server } = createRoomServer();
  const port = Number(process.env.PORT || 8787);
  server.listen(port, '0.0.0.0', () => console.log(`Связка: http://localhost:${port}`));
}
