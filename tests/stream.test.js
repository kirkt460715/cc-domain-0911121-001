import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp, rm} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {Ledger} from '../src/service.js';
import {StreamManager} from '../src/streamManager.js';
import {code, source, station} from './helpers.js';

const RECORD = (revision, value = 20 + revision) => ({
  station: 'p1', metric: 'rainfall', time: '2026-11-01T00:00:00Z', revision, value, unit: 'mm'});
const eventBlock = (id, payload, extra = '') =>
  `${extra}id: ${id}\nevent: observation\ndata: ${JSON.stringify(payload)}\n\n`;

// 可编排的本地事件源：connections 依次给出每次连接要发送的字节序列。
function scriptedServer(connections, {delayMs = 0} = {}) {
  const requests = [];
  const server = http.createServer((req, res) => {
    requests.push(req.headers['last-event-id'] ?? null);
    const body = connections.shift();
    res.writeHead(200, {'content-type': 'text/event-stream', connection: 'close'});
    const bytes = Buffer.isBuffer(body) ? body : Buffer.from(body ?? '');
    let at = 0;
    const pump = () => {
      while (at < bytes.length) {
        const part = bytes.subarray(at, at + 3);
        at += part.length;
        if (!res.write(part)) {
          res.once('drain', pump);
          return;
        }
      }
      setTimeout(() => res.end(), delayMs);
    };
    pump();
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve({
    server, requests, base: `http://127.0.0.1:${server.address().port}/events`,
  })));
}

async function setupLedger(t) {
  const root = await mkdtemp(path.join(os.tmpdir(), 'field-stream-'));
  const managers = new Set();
  // 先关闭所有连接循环（释放文件句柄），再删除数据目录，避免清理竞态。
  t.after(async () => {
    await Promise.all([...managers].map(manager => manager.close().catch(() => {})));
    await rm(root, {recursive: true, force: true});
  });
  const ledger = new Ledger(root);
  await ledger.registerSource(source());
  await ledger.registerStation(station());
  const track = manager => {
    managers.add(manager);
    return manager;
  };
  return {root, ledger, track};
}

async function settle(manager, id, predicate, timeoutMs = 5000) {
  return manager.waitFor(id, (durable, runtime) => predicate(durable, runtime), {timeoutMs});
}

test('half-character cut reconnects with Last-Event-ID and no revision is lost or doubled', async t => {
  const full = [
    ': 心跳\n\n',
    eventBlock('e1', RECORD(1, 10)),
    eventBlock('e2', RECORD(2, 11)),
    eventBlock('e3', RECORD(3, 12)),
  ].join('');
  const bytes = Buffer.from(full, 'utf8');
  const cutAt = bytes.indexOf(Buffer.from('心跳')) + 1; // 切在三字节中文字符内部
  const fixture = await scriptedServer([bytes.subarray(0, cutAt), full, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  const durable = await settle(manager, 'sub1', s => s.status === 'idle');
  assert.equal(durable.position, 'e3');
  // 首次无 Last-Event-ID；半字符断开后整流重发（仍从头，因为还没有确认过任何事件）；
  // 最后一次是带 e3 的空闲探测。
  assert.deepEqual(fixture.requests, [null, null, 'e3']);
  const rows = (await ledger.query()).rows;
  assert.deepEqual(rows.map(r => r.revision), [3]);
  assert.equal(rows[0].value, 12);
  const history = await ledger.history(rows[0].key);
  assert.deepEqual(history.history.map(h => h.revision), [1, 2, 3]);
  const runtime = manager.status('sub1');
  assert.equal(runtime.connections, 3);
  assert.ok(runtime.maxBufferedBytes < 4096);
  assert.equal((await ledger.verify()).valid, true);
});

test('server redelivery of the last event is deduplicated by opaque id and digest', async t => {
  const block = eventBlock('e1', RECORD(1, 10));
  const fixture = await scriptedServer([block, block, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.status === 'idle');
  assert.deepEqual(fixture.requests, [null, 'e1', 'e1']);
  const stream = await ledger.streamStatus('sub1');
  assert.equal(stream.counters.delivered, 1);
  assert.equal(stream.counters.duplicates, 1);
  assert.equal((await ledger.query()).total, 1);
});

test('repeated redelivery counts survive journal replay without double counting', async t => {
  const block = eventBlock('e1', RECORD(1, 10));
  const fixture = await scriptedServer([block, block, block, block, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {root, ledger, track} = await setupLedger(t);
  const manager = new StreamManager(ledger, {retryMs: 10});
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 5, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.status === 'idle');
  await manager.close();
  const before = await ledger.streamStatus('sub1');
  assert.equal(before.counters.delivered, 1);
  assert.equal(before.counters.duplicates, 3); // 三次重发，各计一次
  // 重放整条 journal：重复事件不得再次累加计数器。
  const reopened = new Ledger(root);
  const after = await reopened.streamStatus('sub1');
  assert.deepEqual(after.counters, before.counters);
  const delivery = Object.values((await reopened.store.read()).streamDeliveries)[0];
  assert.equal(delivery.redeliveries, 3);
  assert.equal((await reopened.verify()).valid, true);
});

test('same opaque id with different content keeps conflict evidence and rejects the new body', async t => {
  const good = eventBlock('e1', RECORD(1, 10));
  const bad = eventBlock('e1', RECORD(1, 99));
  const fixture = await scriptedServer([good, bad + eventBlock('e2', RECORD(2, 11)) + eventBlock('e3', RECORD(3, 12)), '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.counters.conflicts >= 1 && s.position === 'e3');
  const dead = await ledger.streamDeadLetters({streamId: 'sub1'});
  assert.equal(dead.length, 1);
  assert.equal(dead[0].reasonCode, 'SSE_EVENT_CONFLICT');
  assert.equal(dead[0].eventId, 'e1');
  assert.ok(dead[0].raw.includes('99'));
  assert.notEqual(dead[0].details.originalDigest, dead[0].details.incomingDigest);
  const rows = (await ledger.query()).rows;
  assert.equal(rows[0].value, 12); // 冲突正文 99 未生效，后续正常事件继续推进
  assert.equal(rows[0].revision, 3); // 不透明事件 ID 与业务 revision 互不替代
});

test('bad JSON is dead-lettered with raw text and the following good event still lands', async t => {
  const stream = [
    eventBlock('good1', RECORD(1, 10)),
    'id: broken\nevent: observation\ndata: {broken}\n\n',
    eventBlock('good2', RECORD(2, 11)),
  ].join('');
  const fixture = await scriptedServer([stream, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.position === 'good2');
  const dead = await ledger.streamDeadLetters({streamId: 'sub1'});
  assert.equal(dead.length, 1);
  assert.equal(dead[0].reasonCode, 'INVALID_JSON');
  assert.equal(dead[0].raw, 'id: broken\nevent: observation\ndata: {broken}');
  assert.equal((await ledger.query()).rows[0].revision, 2);
});

test('dead letters can be individually reprocessed after correction', async t => {
  const stream = [
    eventBlock('good1', RECORD(1, 10)),
    'id: broken\nevent: observation\ndata: {broken}\n\n',
  ].join('');
  const fixture = await scriptedServer([stream, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.counters.deadletters >= 1);
  const item = (await ledger.streamDeadLetters())[0];
  const result = await ledger.reprocessDeadLetter({id: item.id, action: 'replace',
    actor: 'operator', reason: 'repaired malformed gateway frame', record: RECORD(2, 33)});
  assert.equal(result.status, 'reprocessed');
  assert.equal((await ledger.streamDeadLetters({status: 'all'})).filter(x => x.status === 'open').length, 0);
  const rows = (await ledger.query()).rows;
  assert.equal(rows[0].revision, 2);
  assert.equal(rows[0].value, 33);
  assert.equal((await ledger.verify()).valid, true);
});

test('business rule failures stay in the existing quarantine while good events keep landing', async t => {
  const at = hour => `2026-11-01T${String(hour).padStart(2, '0')}:00:00Z`;
  const stream = [
    eventBlock('e1', {...RECORD(1, 10), time: at(0)}),
    eventBlock('e2', {...RECORD(1, -5), time: at(1)}), // 独立键，RANGE 拒收
    eventBlock('e3', {...RECORD(1, 12), time: at(2)}),
  ].join('');
  const fixture = await scriptedServer([stream, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.position === 'e3');
  const quarantine = await ledger.quarantine();
  assert.equal(quarantine.length, 1);
  assert.equal(quarantine[0].code, 'RANGE');
  const rows = (await ledger.query()).rows;
  assert.equal(rows.length, 2); // 坏记录隔离，前后正常记录均生效
  assert.deepEqual(rows.map(r => r.time).sort(), [at(0).replace('Z', '.000Z'), at(2).replace('Z', '.000Z')]);
  assert.deepEqual(await ledger.streamDeadLetters(), []); // 业务坏记录不进流死信
});

test('a revision gap quarantines the out-of-order row without stopping the stream', async t => {
  const stream = [
    eventBlock('e1', RECORD(1, 10)),
    eventBlock('e2', RECORD(3, 12)), // 跳过 revision 2：REVISION_GAP 隔离
  ].join('');
  const fixture = await scriptedServer([stream, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(manager, 'sub1', s => s.position === 'e2');
  const quarantine = await ledger.quarantine();
  assert.equal(quarantine.length, 1);
  assert.equal(quarantine[0].code, 'REVISION_GAP');
  assert.equal((await ledger.query()).rows[0].revision, 1);
  assert.equal((await ledger.verify()).valid, true);
});

test('pause and resume use generation guards so stale connection callbacks cannot advance the stream', async t => {
  const fixture = await scriptedServer([eventBlock('e1', RECORD(1, 10))]);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 100});
  await settle(manager, 'sub1', s => s.position === 'e1');
  const paused = await manager.pause('sub1');
  assert.equal(paused.status, 'paused');
  const staleGeneration = manager.runtimeOf('sub1').generation;
  // 直接用旧代际句柄尝试推进，应被领域层之外的运行时拦截。
  const before = await ledger.streamStatus('sub1');
  const runtime = manager.runtimeOf('sub1');
  assert.notEqual(runtime.generation, staleGeneration - 1);
  const resumed = await manager.resume('sub1');
  assert.equal(resumed.generation, before.generation + 1);
  assert.equal(resumed.position, 'e1'); // 暂停期间没有任何位置推进
});

test('reconfigure replaces the connection and old generation loops exit without writes', async t => {
  const first = eventBlock('e1', RECORD(1, 10));
  const fixtureA = await scriptedServer([first]);
  const fixtureB = await scriptedServer([
    eventBlock('f1', {...RECORD(1, 77), time: '2026-11-01T05:00:00Z'}), '',
  ]);
  t.after(() => Promise.all([
    new Promise(done => fixtureA.server.close(done)),
    new Promise(done => fixtureB.server.close(done)),
  ]));
  const {ledger, track} = await setupLedger(t);
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixtureA.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 100});
  await settle(manager, 'sub1', s => s.position === 'e1');
  // 等首个连接自然结束，确保旧循环停在重连等待，再切换代际。
  await settle(manager, 'sub1', (s, runtime) => runtime.phase === 'reconnect-wait');
  const updated = await manager.reconfigure('sub1', {url: fixtureB.base, maxIdleRounds: 1});
  assert.equal(updated.url, fixtureB.base);
  await settle(manager, 'sub1', s => s.position === 'f1');
  const stream = await ledger.streamStatus('sub1');
  assert.equal(stream.generation, updated.generation);
  const rows = (await ledger.query()).rows;
  assert.equal(rows.find(r => r.time === '2026-11-01T05:00:00.000Z').value, 77);
});

test('slow writes keep buffered bytes bounded because reads wait for durable commits', async t => {
  const blocks = Array.from({length: 8}, (_, index) =>
    eventBlock(`s${index}`, RECORD(index + 1, 100 + index))).join('');
  const fixture = await scriptedServer([blocks]);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {ledger, track} = await setupLedger(t);
  let commits = 0;
  const original = ledger.deliverStreamMessage.bind(ledger);
  ledger.deliverStreamMessage = async (id, message, actor) => {
    commits += 1;
    await new Promise(resolve => setTimeout(resolve, 20)); // 模拟慢落盘
    return original(id, message, actor);
  };
  const manager = track(new StreamManager(ledger, {retryMs: 10}));
  await manager.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1, maxEventBytes: 8192});
  await settle(manager, 'sub1', s => s.position === 's7');
  assert.equal(commits, 8);
  const runtime = manager.status('sub1');
  // 串行读取：任一时刻在途的最大单个事件受事件上限约束，不会随慢写入无限增长。
  assert.ok(runtime.peakEventBytes > 0, 'peak event size is observed');
  assert.ok(runtime.peakEventBytes <= 8192, `unexpected peak ${runtime.peakEventBytes}`);
  assert.ok(runtime.maxBufferedBytes <= 8192, `unexpected buffer ${runtime.maxBufferedBytes}`);
  assert.equal((await ledger.verify()).valid, true);
});

test('restart rehydrates subscriptions and resumes from the durable position', async t => {
  const blockA = eventBlock('e1', RECORD(1, 10));
  const redelivery = eventBlock('e1', RECORD(1, 10));
  const blockB = eventBlock('e2', RECORD(2, 11));
  const fixture = await scriptedServer([blockA, '', redelivery, blockB, '']);
  t.after(() => new Promise(done => fixture.server.close(done)));
  const {root, ledger, track} = await setupLedger(t);
  const first = track(new StreamManager(ledger, {retryMs: 10}));
  await first.register({id: 'sub1', url: fixture.base, source: 's1',
    retryMs: 10, maxReconnects: 3, maxIdleRounds: 1});
  await settle(first, 'sub1', s => s.status === 'idle'); // 连接循环已自然停止
  assert.equal((await ledger.streamStatus('sub1')).position, 'e1');
  await first.close();
  const requestsBeforeRestart = [...fixture.requests];
  assert.deepEqual(requestsBeforeRestart, [null, 'e1']);
  const reopenedLedger = new Ledger(root);
  const second = track(new StreamManager(reopenedLedger, {retryMs: 10}));
  await second.start();
  await settle(second, 'sub1', s => s.status === 'idle' && s.position === 'e2');
  assert.equal(fixture.requests[requestsBeforeRestart.length], 'e1'); // 重启后携带持久化位置
  const rows = (await reopenedLedger.query()).rows;
  assert.equal(rows[0].revision, 2);
  assert.equal((await reopenedLedger.verify()).valid, true);
});

test('unknown and stopped subscriptions fail with explicit codes', async t => {
  const {ledger, track} = await setupLedger(t);
  await assert.rejects(ledger.controlStream('missing', 'paused'), code('STREAM_MISSING'));
  const manager = track(new StreamManager(ledger));
  await manager.register({id: 'sub1', url: 'http://127.0.0.1:1/events', source: 's1',
    maxReconnects: 0, retryMs: 10});
  await manager.stop('sub1');
  await assert.rejects(manager.resume('sub1'), code('STREAM_STOPPED'));
});
