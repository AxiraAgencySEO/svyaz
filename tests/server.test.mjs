import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WebSocket } from 'ws';
import { createRoomServer } from '../server/index.mjs';

async function fixture(t) {
  const { server, wss } = createRoomServer({ allowedOrigins: ['http://test.local'] });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => { for (const ws of wss.clients) ws.terminate(); await new Promise(resolve => server.close(resolve)); });
  return async () => {
    const ws = new WebSocket(`ws://127.0.0.1:${server.address().port}/ws`, { origin: 'http://test.local' });
    const queue = []; const waiters = [];
    ws.on('message', data => {
      const message = JSON.parse(data.toString());
      const index = waiters.findIndex(w => w.type === message.type);
      if (index >= 0) { const [w] = waiters.splice(index, 1); clearTimeout(w.timer); w.resolve(message); }
      else queue.push(message);
    });
    await new Promise((resolve, reject) => { ws.once('open', resolve); ws.once('error', reject); });
    return {
      ws,
      send: message => ws.send(JSON.stringify(message)),
      next(type) {
        const index = queue.findIndex(m => m.type === type);
        if (index >= 0) return Promise.resolve(queue.splice(index, 1)[0]);
        return new Promise((resolve, reject) => {
          const waiter = { type, resolve, timer: setTimeout(() => { const i = waiters.indexOf(waiter); if (i >= 0) waiters.splice(i, 1); reject(new Error(`Timed out: ${type}`)); }, 1500) }; waiters.push(waiter);
        });
      },
    };
  };
}

test('room keys isolate participants; OBS tokens allow one source and cannot publish', async t => {
  const client = await fixture(t);
  const owner = await client(); owner.send({ type: 'create', name: 'Owner' });
  const room = await owner.next('joined');
  const stranger = await client(); stranger.send({ type: 'join', roomId: room.roomId, key: 'wrong', name: 'Stranger' });
  assert.match((await stranger.next('error')).message, /Неверная/);
  const source = room.members[0];
  const badViewer = await client(); badViewer.send({ type: 'view', roomId: room.roomId, sourceId: source.id, token: 'wrong' });
  assert.match((await badViewer.next('error')).message, /недействительна/);
  const viewer = await client(); viewer.send({ type: 'view', roomId: room.roomId, sourceId: source.id, token: source.sourceToken });
  const joined = await viewer.next('joined'); assert.equal(joined.role, 'viewer'); assert.equal(joined.key, undefined); assert.equal(joined.members, undefined);
  viewer.send({ type: 'publish', enabled: true }); assert.match((await viewer.next('error')).message, /Нет права/);
  viewer.send({ type: 'watch', sourceId: 'another-camera' }); assert.match((await viewer.next('error')).message, /Нет доступа/);
  owner.send({ type: 'publish', enabled: true });
  const watch = await owner.next('watch'); assert.ok(watch.viewerId);
  owner.send({ type: 'signal', targetId: watch.viewerId, callId: 'one', description: { type: 'offer', sdp: 'offer-test' } });
  const offer = await viewer.next('signal'); assert.equal(offer.description.sdp, 'offer-test'); assert.equal(offer.callId, 'one');
  viewer.send({ type: 'signal', targetId: offer.fromId, callId: 'one', description: { type: 'offer', sdp: 'malicious-offer' } });
  assert.match((await viewer.next('error')).message, /Нет доступа/);
  const closed = new Promise(resolve => viewer.ws.once('close', code => resolve(code)));
  owner.send({ type: 'rotate-source' }); await owner.next('rotated'); assert.equal(await closed, 4003);
  const oldLink = await client(); oldLink.send({ type: 'view', roomId: room.roomId, sourceId: source.id, token: source.sourceToken });
  assert.match((await oldLink.next('error')).message, /недействительна/);
});

test('six-person limit and authenticated session restoration', async t => {
  const client = await fixture(t);
  const owner = await client(); owner.send({ type: 'create', name: 'Owner' }); const room = await owner.next('joined');
  for (let i = 0; i < 5; i++) { const c = await client(); c.send({ type: 'join', roomId: room.roomId, key: room.key, name: `Member ${i}` }); await c.next('joined'); }
  const seventh = await client(); seventh.send({ type: 'join', roomId: room.roomId, key: room.key, name: 'Seven' });
  assert.match((await seventh.next('error')).message, /6 участников/);
  const thief = await client(); thief.send({ type: 'join', roomId: room.roomId, key: room.key, name: 'Thief', resumeId: room.memberId, resumeToken: 'wrong' });
  assert.match((await thief.next('error')).message, /восстановить/);
  const replacement = await client(); replacement.send({ type: 'join', roomId: room.roomId, key: room.key, name: 'Owner', resumeId: room.memberId, resumeToken: room.resumeToken });
  const restored = await replacement.next('joined'); assert.equal(restored.memberId, room.memberId); assert.equal(restored.members.length, 6);
});

test('signals cannot cross rooms and arbitrary source links cannot authenticate a member', async t => {
  const client = await fixture(t);
  const first = await client(); first.send({ type: 'create', name: 'A' }); const a = await first.next('joined');
  const second = await client(); second.send({ type: 'create', name: 'B' }); const b = await second.next('joined');
  const intruder = await client(); intruder.send({ type: 'join', roomId: a.roomId, key: a.members[0].sourceToken, name: 'Intruder' });
  assert.match((await intruder.next('error')).message, /Неверная/);
  first.send({ type: 'signal', targetId: b.memberId, callId: 'bad', description: { type: 'offer', sdp: 'cross-room' } });
  await assert.rejects(second.next('signal'), /Timed out/);
});
