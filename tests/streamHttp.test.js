import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {listen} from '../src/http.js';
import {Ledger} from '../src/service.js';
import {StreamManager} from '../src/streamManager.js';
import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {source, station} from './helpers.js';

const eventBlock = (id, revision, value) =>
  `id: ${id}\nevent: observation\ndata: ${JSON.stringify({
    station: 'p1', metric: 'rainfall', time: '2026-11-01T00:00:00Z',
    revision, value, unit: 'mm'})}\n\n`;

function sourceServer(bodies) {
  const requests = [];
  const server = http.createServer((req, res) => {
    requests.push(req.headers['last-event-id'] ?? null);
    res.writeHead(200, {'content-type': 'text/event-stream', connection: 'close'});
    res.end(bodies.shift() ?? '');
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve({
    server, requests, base: `http://127.0.0.1:${server.address().port}/events`,
  })));
}

async function harness(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'field-http-stream-'));
  const ledger = new Ledger(root);
  await ledger.registerSource(source());
  await ledger.registerStation(station());
  const manager = new StreamManager(ledger, {retryMs: 10});
  const gateway = await listen(ledger, {port: 0, manager});
  // 按相反顺序释放：先停 HTTP 与连接循环（释放句柄），再删数据目录。
  t.after(async () => {
    await new Promise(done => gateway.close(done));
    await manager.close();
    await rm(root, {recursive: true, force: true});
  });
  return {ledger, manager, gateway: `http://127.0.0.1:${gateway.address().port}`};
}

test('HTTP subscription lifecycle: register, observe position, pause and resume', async t => {
  const upstream = await sourceServer([eventBlock('e1', 1, 10), '', eventBlock('e2', 2, 11), '', '', '', '']);
  t.after(() => new Promise(done => upstream.server.close(done)));
  const {ledger, gateway} = await harness(t);
  const headers = {'content-type': 'application/json'};
  const created = await fetch(gateway + '/streams', {method: 'POST', headers,
    body: JSON.stringify({id: 'sub1', url: upstream.base, source: 's1',
      retryMs: 10, maxReconnects: 3, maxIdleRounds: 1})});
  assert.equal(created.status, 200);
  const registered = await created.json();
  assert.equal(registered.status, 'running');
  await managerWait(ledger, 'sub1', s => s.position === 'e1' && s.status === 'idle');
  const paused = await (await fetch(gateway + '/stream/pause', {method: 'POST', headers,
    body: JSON.stringify({id: 'sub1'})})).json();
  assert.equal(paused.status, 'paused');
  const afterPause = await (await fetch(gateway + '/stream/status?id=sub1')).json();
  assert.equal(afterPause.status, 'paused');
  assert.equal(afterPause.position, 'e1');
  const resumed = await (await fetch(gateway + '/stream/resume', {method: 'POST', headers,
    body: JSON.stringify({id: 'sub1'})})).json();
  assert.equal(resumed.status, 'running');
  await managerWait(ledger, 'sub1', s => s.position === 'e2');
  const status = await (await fetch(gateway + '/stream/status?id=sub1')).json();
  assert.equal(status.position, 'e2');
  assert.equal(status.counters.delivered, 2);
  const runtime = await (await fetch(gateway + '/stream/runtime?id=sub1')).json();
  assert.equal(runtime.id, 'sub1');
  assert.ok(runtime.connections >= 2);
  assert.equal((await ledger.query()).total, 1);
  const list = await (await fetch(gateway + '/streams')).json();
  assert.equal(list.length, 1);
});

test('HTTP dead letters expose raw frames and support reprocessing', async t => {
  const upstream = await sourceServer([
    eventBlock('ok1', 1, 10) + 'id: broken\nevent: observation\ndata: {nope}\n\n', '',
  ]);
  t.after(() => new Promise(done => upstream.server.close(done)));
  const {ledger, gateway} = await harness(t);
  const headers = {'content-type': 'application/json'};
  await fetch(gateway + '/streams', {method: 'POST', headers, body: JSON.stringify({
    id: 'sub1', url: upstream.base, source: 's1', retryMs: 10, maxReconnects: 3, maxIdleRounds: 1})});
  await managerWait(ledger, 'sub1', s => s.counters.deadletters >= 1);
  const items = await (await fetch(gateway + '/stream/deadletters?streamId=sub1')).json();
  assert.equal(items.length, 1);
  assert.equal(items[0].reasonCode, 'INVALID_JSON');
  const resolved = await fetch(gateway + '/stream/deadletters/reprocess', {method: 'POST', headers,
    body: JSON.stringify({id: items[0].id, action: 'discard', actor: 'operator',
      reason: 'discard malformed gateway frame'})});
  assert.equal(resolved.status, 200);
  assert.equal((await (await fetch(gateway + '/stream/deadletters?streamId=sub1')).json()).length, 0);
});

test('HTTP endpoints without an attached manager report 503 instead of silently failing', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'field-no-manager-'));
  const ledger = new Ledger(root);
  await ledger.registerSource(source());
  const gateway = await listen(ledger, {port: 0});
  t.after(async () => {
    await new Promise(done => gateway.close(done));
    await rm(root, {recursive: true, force: true});
  });
  const response = await fetch(`http://127.0.0.1:${gateway.address().port}/streams`, {
    method: 'POST', headers: {'content-type': 'application/json'},
    body: JSON.stringify({id: 'x', url: 'http://127.0.0.1:1/events', source: 's1'})});
  assert.equal(response.status, 503);
  assert.equal((await response.json()).error, 'STREAM_MANAGER');
});

function managerWait(ledger, id, predicate) {
  const deadline = Date.now() + 15000;
  return new Promise((resolve, reject) => {
    const tick = async () => {
      if (predicate(await ledger.streamStatus(id))) {
        resolve();
      } else if (Date.now() > deadline) {
        reject(new Error('timeout'));
      } else {
        setTimeout(tick, 20);
      }
    };
    tick();
  });
}
