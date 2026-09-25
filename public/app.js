const $ = id => document.getElementById(id);
const query = new URLSearchParams(location.search);
const fragment = new URLSearchParams(location.hash.slice(1));
const obs = Boolean(query.get('source'));
const state = {
  roomId: query.get('room'), key: fragment.get('key'), memberId: null, members: [],
  ws: null, iceServers: [], stream: null, peers: new Map(), watching: new Set(),
  cards: new Map(), joined: false, stopped: false, reconnecting: false, retries: 0, wake: null,
};
let toastTimer, reconnectTimer, statsTimer, iceTimer;
let messageQueue = Promise.resolve();

function showError(message) {
  if (obs) { $('obs-status').textContent = message; $('obs-status').hidden = false; }
  else { $('error').textContent = message; $('error').hidden = false; }
}
function clearError() { $('error').hidden = true; }
function toast(message) { $('toast').textContent = message; $('toast').hidden = false; clearTimeout(toastTimer); toastTimer = setTimeout(() => { $('toast').hidden = true; }, 3200); }
function connection(text, online = false) { $('connection').textContent = text; $('connection').className = `connection ${online ? 'online' : 'offline'}`; }
function send(message) {
  if (state.ws?.readyState !== WebSocket.OPEN) return false;
  state.ws.send(JSON.stringify(message)); return true;
}
function signalUrl() {
  const configured = window.APP_CONFIG?.signalUrl;
  if (configured) { const url = new URL(configured); if (!['ws:', 'wss:'].includes(url.protocol)) throw new Error('Неверный адрес сервера подключения.'); url.pathname = '/ws'; return url.href; }
  if (['localhost', '127.0.0.1'].includes(location.hostname)) return `ws://${location.host}/ws`;
  throw new Error('Сервер подключения ещё не настроен. Укажите PUBLIC_SIGNAL_URL при публикации сайта.');
}
function roomLink() { const u = new URL('/', location.origin); u.searchParams.set('room', state.roomId); u.hash = new URLSearchParams({ key: state.key }).toString(); return u.href; }
function cameraLink(member) { const u = new URL('/', location.origin); u.search = new URLSearchParams({ room: state.roomId, source: member.id }).toString(); u.hash = new URLSearchParams({ token: member.sourceToken }).toString(); return u.href; }
async function copy(text) {
  try { await navigator.clipboard.writeText(text); toast('Ссылка скопирована'); }
  catch { showError('Не удалось скопировать автоматически. Разрешите сайту доступ к буферу обмена.'); }
}
function storedMember() { try { return JSON.parse(sessionStorage.getItem(`member:${state.roomId}`)) || {}; } catch { return {}; } }
function rememberMember(value) { try { sessionStorage.setItem(`member:${state.roomId}`, JSON.stringify(value)); } catch {} }
function busy(value) { $('create').disabled = value; $('join').disabled = value; }

function closePeer(remoteId) {
  const item = state.peers.get(remoteId); if (!item) return;
  state.peers.delete(remoteId); clearTimeout(item.retryTimer); item.pc.onconnectionstatechange = null; item.pc.close();
  if (item.kind === 'receive') {
    const card = state.cards.get(item.sourceId);
    if (card) { card.video.srcObject = null; card.placeholder.hidden = false; }
    if (obs) { $('obs-video').srcObject = null; $('obs-status').hidden = false; }
  }
}
function closeAllPeers() { for (const key of [...state.peers.keys()]) closePeer(key); }
function closeSource(sourceId) { for (const [key, item] of state.peers) if (item.sourceId === sourceId) closePeer(key); }
function updateWatchButton(card) {
  card.watch.textContent = state.watching.has(card.id) ? 'Скрыть' : 'Показать';
  card.placeholder.lastChild.textContent = state.watching.has(card.id) ? 'Подключение…' : 'Предпросмотр выключен';
}
function subscribe(sourceId) {
  state.watching.add(sourceId); send({ type: 'watch', sourceId });
  const card = state.cards.get(sourceId); if (card) updateWatchButton(card);
}
function unsubscribe(sourceId) {
  state.watching.delete(sourceId); send({ type: 'unwatch', sourceId }); closeSource(sourceId);
  const card = state.cards.get(sourceId); if (card) updateWatchButton(card);
}
function retrySource(sourceId) {
  if (!state.joined || state.stopped || (!obs && !state.watching.has(sourceId))) return;
  closeSource(sourceId); send({ type: 'unwatch', sourceId }); send({ type: 'watch', sourceId });
}
function makePeer(remoteId, sourceId, kind, callId) {
  const peerKey = `${kind}:${remoteId}`;
  closePeer(peerKey);
  const pc = new RTCPeerConnection({ iceServers: state.iceServers });
  const item = { pc, remoteId, sourceId, kind, callId, connectedAt: null, retryTimer: null };
  state.peers.set(peerKey, item);
  pc.onicecandidate = event => {
    if (event.candidate) send({ type: 'signal', targetId: remoteId, callId, candidate: event.candidate.toJSON() });
  };
  pc.onconnectionstatechange = () => {
    const status = pc.connectionState;
    if (status === 'connected') {
      clearTimeout(item.retryTimer); item.connectedAt = Date.now();
      if (kind === 'receive' && obs) $('obs-status').hidden = true;
    }
    if (['failed', 'disconnected'].includes(status)) {
      if (kind === 'receive') {
        if (obs) { $('obs-status').textContent = 'Восстанавливаем видео…'; $('obs-status').hidden = false; }
        const card = state.cards.get(sourceId); if (card) card.stats.textContent = 'Восстанавливаем видео…';
        clearTimeout(item.retryTimer);
        item.retryTimer = setTimeout(() => retrySource(sourceId), status === 'failed' ? 1500 : 7000);
      }
    }
  };
  if (kind === 'receive') {
    pc.ontrack = event => {
      const video = obs ? $('obs-video') : state.cards.get(sourceId)?.video;
      if (!video) return;
      video.srcObject = event.streams[0] || new MediaStream(pc.getReceivers().map(r => r.track));
      if (!obs) state.cards.get(sourceId).placeholder.hidden = true;
      video.play().catch(() => { if (obs) $('obs-play').hidden = false; });
    };
    item.retryTimer = setTimeout(() => { if (pc.connectionState !== 'connected') retrySource(sourceId); }, 18000);
  }
  return item;
}

async function handleMessage(msg) {
  if (msg.type === 'error') {
    showError(msg.message); busy(false);
    if (!state.joined) { state.stopped = true; state.ws?.close(); }
    return;
  }
  if (msg.type === 'joined') {
    state.joined = true; state.reconnecting = false; state.retries = 0; busy(false); clearError();
    state.roomId = msg.roomId; state.iceServers = msg.iceServers;
    connection('В комнате', true); $('relay-notice').hidden = msg.hasTurn;
    if (!obs) {
      state.key = msg.key; state.memberId = msg.memberId;
      rememberMember({ resumeId: msg.memberId, resumeToken: msg.resumeToken });
      history.replaceState(null, '', roomLink());
      $('lobby').hidden = true; $('studio').hidden = false;
      $('local-name').textContent = $('name').value.trim() || 'Моя камера';
      renderRoster(msg.members);
      if (state.stream?.getVideoTracks().some(track => track.readyState === 'live')) send({ type: 'publish', enabled: true });
      for (const sourceId of state.watching) send({ type: 'watch', sourceId });
    }
    clearInterval(statsTimer); statsTimer = setInterval(updateStats, 3000);
    clearInterval(iceTimer); iceTimer = setInterval(() => send({ type: 'ice-refresh' }), 1800000);
  } else if (msg.type === 'ice-config') {
    state.iceServers = msg.iceServers;
    for (const item of state.peers.values()) item.pc.setConfiguration({ iceServers: msg.iceServers });
  } else if (msg.type === 'roster') renderRoster(msg.members);
  else if (msg.type === 'source-state') {
    if (!msg.publishing) { closeSource(msg.sourceId); $('obs-status').textContent = 'Ожидаем камеру…'; $('obs-status').hidden = false; }
  } else if (msg.type === 'watch') {
    if (!state.stream) return;
    const callId = crypto.randomUUID();
    const { pc } = makePeer(msg.viewerId, state.memberId, 'send', callId);
    for (const track of state.stream.getTracks()) pc.addTrack(track, state.stream);
    await pc.setLocalDescription(await pc.createOffer());
    send({ type: 'signal', targetId: msg.viewerId, callId, description: pc.localDescription.toJSON() });
  } else if (msg.type === 'unwatch') closePeer(`send:${msg.viewerId}`);
  else if (msg.type === 'signal') {
    let item = [...state.peers.values()].find(peer => peer.remoteId === msg.fromId && peer.callId === msg.callId);
    if (msg.description?.type === 'offer') {
      if (obs ? msg.sourceId !== query.get('source') : !state.watching.has(msg.sourceId)) return;
      item = makePeer(msg.fromId, msg.sourceId, 'receive', msg.callId);
      await item.pc.setRemoteDescription(msg.description);
      await item.pc.setLocalDescription(await item.pc.createAnswer());
      send({ type: 'signal', targetId: msg.fromId, callId: msg.callId, description: item.pc.localDescription.toJSON() });
    } else if (item && item.callId === msg.callId) {
      if (msg.description) {
        await item.pc.setRemoteDescription(msg.description);
        if (item.kind === 'send') {
          const maxBitrate = $('quality').value === '720' ? 3000000 : $('quality').value === '1080-60' ? 10000000 : 6000000;
          for (const sender of item.pc.getSenders()) if (sender.track?.kind === 'video') {
            const parameters = sender.getParameters();
            if (parameters.encodings?.length) { parameters.encodings[0].maxBitrate = maxBitrate; try { await sender.setParameters(parameters); } catch {} }
          }
        }
      } else if (msg.candidate) await item.pc.addIceCandidate(msg.candidate);
    }
  } else if (msg.type === 'rotated') toast('Старая ссылка OBS отключена. Скопируйте новую.');
}

function connect(mode) {
  clearTimeout(reconnectTimer); state.stopped = false; busy(true); clearError();
  let url;
  try { url = signalUrl(); } catch (error) { showError(error.message); busy(false); return; }
  connection(state.reconnecting ? 'Переподключение…' : 'Подключение…');
  const ws = new WebSocket(url); state.ws = ws;
  const timeout = setTimeout(() => { if (!state.joined) ws.close(); }, 15000);
  ws.onopen = () => {
    const message = obs
      ? { type: 'view', roomId: state.roomId, sourceId: query.get('source'), token: fragment.get('token') }
      : { type: mode, name: $('name').value.trim(), roomId: state.roomId, key: state.key, ...storedMember() };
    ws.send(JSON.stringify(message));
  };
  ws.onmessage = event => {
    messageQueue = messageQueue.then(async () => { if (state.ws === ws) await handleMessage(JSON.parse(event.data)); }).catch(error => {
      console.warn('Connection operation failed:', error.name);
      showError('Не удалось установить видеосвязь. Проверьте сеть и попробуйте подключить камеру снова.');
    });
  };
  ws.onerror = () => { /* close event owns recovery */ };
  ws.onclose = event => {
    clearTimeout(timeout); if (state.ws !== ws) return;
    const hadSession = state.joined || state.reconnecting;
    state.joined = false; closeAllPeers(); connection('Нет соединения'); busy(false);
    if (state.stopped) return;
    if ([4001, 4003].includes(event.code)) {
      state.stopped = true; stopCapture();
      showError(event.code === 4003 ? 'Ссылка камеры отозвана владельцем.' : 'Эта сессия открыта в другом окне.'); return;
    }
    if (hadSession || obs) {
      state.reconnecting = true;
      if (obs) { $('obs-status').textContent = 'Восстанавливаем соединение…'; $('obs-status').hidden = false; }
      reconnectTimer = setTimeout(() => connect(obs ? 'view' : 'join'), Math.min(1500 * 2 ** state.retries++, 15000));
    } else showError('Сервер подключения недоступен. Проверьте интернет или адрес сервера.');
  };
}

function makeCard(member) {
  const card = document.createElement('article'); card.className = 'camera-card';
  card.innerHTML = `<div class="video-frame"><video autoplay playsinline muted></video><span class="live-badge" hidden>КАМЕРА ВКЛЮЧЕНА</span><div class="camera-placeholder"><div class="avatar"></div><span>Камера выключена</span></div></div><div class="card-bottom"><div class="card-title"><b></b><span></span></div><div class="card-buttons"><button class="button secondary watch">Показать</button><button class="button subtle copy">Ссылка OBS ↗</button></div><div class="stream-state"></div><button class="revoke button subtle" hidden>Отозвать ссылку OBS</button></div>`;
  const refs = { element: card, id: member.id, video: card.querySelector('video'), badge: card.querySelector('.live-badge'), placeholder: card.querySelector('.camera-placeholder'), avatar: card.querySelector('.avatar'), name: card.querySelector('.card-title b'), role: card.querySelector('.card-title span'), watch: card.querySelector('.watch'), copy: card.querySelector('.copy'), revoke: card.querySelector('.revoke'), stats: card.querySelector('.stream-state') };
  refs.watch.onclick = () => state.watching.has(member.id) ? unsubscribe(member.id) : subscribe(member.id);
  refs.copy.onclick = () => { const current = state.members.find(m => m.id === member.id); if (current) copy(cameraLink(current)); };
  refs.revoke.onclick = () => send({ type: 'rotate-source' });
  state.cards.set(member.id, refs); $('camera-grid').append(card); return refs;
}
function renderRoster(members) {
  state.members = members;
  $('member-count').textContent = `${members.length} / 6`;
  $('empty-team').hidden = members.length > 1;
  for (const [id, card] of state.cards) if (!members.some(m => m.id === id)) { closeSource(id); card.element.remove(); state.cards.delete(id); }
  const sorted = [...members].sort((a, b) => Number(b.id === state.memberId) - Number(a.id === state.memberId));
  for (const member of sorted) {
    const card = state.cards.get(member.id) || makeCard(member);
    const local = member.id === state.memberId;
    card.element.classList.toggle('local', local); card.name.textContent = member.name;
    card.role.textContent = local ? 'Вы' : 'Участник'; card.avatar.textContent = member.name.slice(0, 1).toUpperCase();
    card.watch.hidden = local; card.watch.disabled = !member.publishing;
    card.copy.disabled = !state.joined; card.revoke.hidden = !local;
    card.revoke.style.cssText = 'font-size:12px;min-height:28px;padding:5px 0;border:0;margin-top:5px;color:#9da8a2';
    card.badge.hidden = !member.publishing;
    if (local && state.stream) { card.video.srcObject = state.stream; card.video.muted = true; card.video.play().catch(() => {}); card.placeholder.hidden = true; }
    else if (!member.publishing) { closeSource(member.id); card.video.srcObject = null; card.placeholder.hidden = false; card.placeholder.lastChild.textContent = 'Камера выключена'; card.stats.textContent = 'Можно заранее добавить ссылку в OBS'; }
    else if (!card.video.srcObject) { card.placeholder.hidden = false; updateWatchButton(card); card.stats.textContent = 'Предпросмотр без звука'; }
  }
}

async function wakeLock() { try { if ('wakeLock' in navigator && document.visibilityState === 'visible') state.wake = await navigator.wakeLock.request('screen'); } catch {} }
async function listCameras() {
  try {
    const chosen = $('camera').value;
    const devices = (await navigator.mediaDevices.enumerateDevices()).filter(d => d.kind === 'videoinput');
    $('camera').replaceChildren(new Option('Основная камера', ''));
    devices.forEach((d, i) => $('camera').add(new Option(d.label || `Камера ${i + 1}`, d.deviceId)));
    if (devices.some(d => d.deviceId === chosen)) $('camera').value = chosen;
  } catch {}
}
function stopCapture() {
  send({ type: 'publish', enabled: false });
  for (const [key, item] of state.peers) if (item.kind === 'send') closePeer(key);
  if (state.stream) for (const track of state.stream.getTracks()) { track.onended = null; track.stop(); }
  state.stream = null; state.wake?.release().catch(() => {}); state.wake = null;
  $('publish').textContent = 'Включить камеру'; $('publish').className = 'button primary';
  $('camera').disabled = false; $('quality').disabled = false; $('mic').disabled = false;
  $('capture-info').textContent = 'Разрешение зависит от камеры и соединения.';
  const card = state.cards.get(state.memberId); if (card) { card.video.srcObject = null; card.placeholder.hidden = false; }
}
async function startCapture() {
  if (!state.joined) return showError('Дождитесь подключения к комнате.');
  if (!navigator.mediaDevices?.getUserMedia) return showError('Камера требует HTTPS и современный браузер.');
  $('publish').disabled = true; clearError();
  try {
    const quality = $('quality').value;
    const stream = await navigator.mediaDevices.getUserMedia({
      video: { width: { ideal: quality === '720' ? 1280 : 1920 }, height: { ideal: quality === '720' ? 720 : 1080 }, frameRate: { ideal: quality === '1080-60' ? 60 : 30 }, ...($('camera').value ? { deviceId: { exact: $('camera').value } } : { facingMode: { ideal: 'environment' } }) },
      audio: $('mic').checked ? { echoCancellation: true, noiseSuppression: true } : false,
    });
    if (!state.joined || state.stopped) { stream.getTracks().forEach(t => t.stop()); return; }
    state.stream = stream; const track = stream.getVideoTracks()[0];
    track.onended = () => { stopCapture(); showError('Телефон остановил камеру. Вернитесь на страницу и включите её снова.'); };
    const settings = track.getSettings();
    $('capture-info').textContent = `${settings.width || '?'} × ${settings.height || '?'} · ${Math.round(settings.frameRate || 0)} кадров/с. Держите эту страницу открытой.`;
    await listCameras();
    $('camera').disabled = true; $('quality').disabled = true; $('mic').disabled = true;
    $('publish').textContent = 'Остановить передачу'; $('publish').className = 'button danger';
    send({ type: 'publish', enabled: true }); await wakeLock();
  } catch (error) {
    const messages = { NotAllowedError: 'Доступ к камере запрещён. Разрешите камеру в настройках браузера и повторите.', NotFoundError: 'Камера не найдена. Откройте комнату на телефоне.', NotReadableError: 'Камера занята другим приложением. Закройте его и повторите.', OverconstrainedError: 'Камера не поддерживает выбранные настройки. Попробуйте 720p.' };
    showError(messages[error.name] || 'Не удалось включить камеру. Проверьте разрешения браузера.');
  } finally { $('publish').disabled = false; }
}
async function updateStats() {
  if (!state.joined) return;
  const outgoing = [...state.peers.values()].filter(x => x.kind === 'send' && x.pc.connectionState === 'connected').length;
  const local = state.cards.get(state.memberId);
  if (local && state.stream) local.stats.textContent = `Получателей: ${outgoing} · звук ${state.stream.getAudioTracks().length ? 'включён' : 'выключен'}`;
  for (const item of state.peers.values()) {
    if (item.kind !== 'receive' || item.pc.connectionState !== 'connected') continue;
    try {
      const reports = await item.pc.getStats();
      reports.forEach(report => {
        if (report.type !== 'inbound-rtp' || report.kind !== 'video') return;
        const card = state.cards.get(item.sourceId); if (!card) return;
        const total = (report.packetsReceived || 0) + Math.max(0, report.packetsLost || 0);
        const loss = total ? (Math.max(0, report.packetsLost || 0) / total * 100).toFixed(1) : '0.0';
        card.stats.textContent = `${report.frameWidth || '—'}×${report.frameHeight || '—'} · ${Math.round(report.framesPerSecond || 0)} FPS · потери ${loss}%`;
      });
    } catch {}
  }
}

$('entry-form').onsubmit = event => { event.preventDefault(); if (!$('name').reportValidity()) return; try { sessionStorage.setItem('camera-name', $('name').value); } catch {} connect(state.roomId && state.key ? 'join' : 'create'); };
$('join').onclick = () => {
  if (!$('name').reportValidity()) return;
  try {
    const url = new URL($('invite').value.trim());
    const params = new URLSearchParams(url.hash.slice(1));
    if (!url.searchParams.get('room') || !params.get('key') || url.searchParams.get('source')) throw new Error();
    state.roomId = url.searchParams.get('room'); state.key = params.get('key');
    try { sessionStorage.setItem('camera-name', $('name').value); } catch {}
    connect('join');
  } catch { showError('Вставьте полную ссылку приглашения в комнату.'); }
};
$('invite-copy').onclick = () => copy(roomLink());
$('publish').onclick = () => state.stream ? stopCapture() : startCapture();
$('leave').onclick = () => {
  state.stopped = true; clearTimeout(reconnectTimer); clearInterval(statsTimer); clearInterval(iceTimer);
  stopCapture(); closeAllPeers(); state.ws?.close(); location.assign('/');
};
$('obs-play').onclick = () => $('obs-video').play().then(() => { $('obs-play').hidden = true; }).catch(() => showError('Браузер заблокировал воспроизведение.'));
document.addEventListener('visibilitychange', () => { if (state.stream && document.visibilityState === 'visible') wakeLock(); });
window.addEventListener('online', () => { if (state.reconnecting && !state.stopped) { state.ws?.close(); connect(obs ? 'view' : 'join'); } });
window.addEventListener('pagehide', () => { state.stopped = true; stopCapture(); state.ws?.close(); });
try { $('name').value = sessionStorage.getItem('camera-name') || ''; } catch {}
if (obs) {
  document.body.classList.add('obs-mode'); $('obs-output').hidden = false;
  document.title = 'Связка — источник OBS';
  if (!state.roomId || !fragment.get('token')) showError('Ссылка камеры неполная. Скопируйте её из комнаты.');
  else connect('view');
} else if (state.roomId && state.key) {
  $('invite').value = roomLink(); $('create').textContent = 'Войти в комнату ↗';
  if ($('name').value && storedMember().resumeId) connect('join');
}

// Optional browser-native tool: read-only; never opens a camera or reveals access tokens.
if (document.modelContext?.registerTool) {
  try {
    Promise.resolve(document.modelContext.registerTool({ name: 'get_camera_room_status', title: 'Состояние комнаты', description: 'Read the current connection and camera status without opening cameras or returning invitation links.', inputSchema: { type: 'object', properties: {}, additionalProperties: false }, annotations: { readOnlyHint: true, untrustedContentHint: true }, execute(input) { if (!input || typeof input !== 'object' || Object.keys(input).length) throw new Error('Expected an empty object'); return { connected: state.joined, cameraEnabled: Boolean(state.stream), participants: state.members.map(m => ({ name: m.name, cameraEnabled: m.publishing })) }; } })).catch(() => {});
  } catch {}
}
