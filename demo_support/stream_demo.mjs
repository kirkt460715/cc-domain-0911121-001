#!/usr/bin/env node
// 端到端演练：用本地 text/event-stream 来源驱动 FieldLedger 订阅入口。
// 场景按需求顺序编排：半个字符处断开 → Last-Event-ID 重发 → 坏事件后跟正常事件 →
// 慢写入 → 进程重启 → 对照观测历史与接收位置，证明没有漏掉或额外生效的修订。
import assert from 'node:assert/strict';
import http from 'node:http';
import {mkdtemp, rm, readFile} from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {Ledger} from '../src/service.js';
import {StreamManager} from '../src/streamManager.js';

const record = (revision, value, time = '2026-11-11T10:00:00Z') => ({
  station: 'station1', metric: 'temperature', time, revision, value, unit: 'C'});
const block = (id, payload, extra = '') =>
  `${extra}id: ${id}\nevent: observation\ndata: ${JSON.stringify(payload)}\n\n`;

// 可编排来源：每次连接弹出下一段脚本，记录续接头；支持半字节切块与慢写入。
function scriptedGateway(script, {writeDelayMs = 0} = {}) {
  const requests = [];
  const server = http.createServer((req, res) => {
    requests.push(req.headers['last-event-id'] ?? null);
    const segment = script.shift() ?? '';
    res.writeHead(200, {'content-type': 'text/event-stream', connection: 'close'});
    const bytes = Buffer.isBuffer(segment) ? segment : Buffer.from(segment);
    let at = 0;
    const pump = () => {
      while (at < bytes.length) {
        const part = bytes.subarray(at, at + 3); // 与本地网关一致的三字节写块
        at += part.length;
        if (!res.write(part)) {
          res.once('drain', pump);
          return;
        }
      }
      setTimeout(() => res.end(), writeDelayMs);
    };
    pump();
  });
  return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve({
    server, requests, base: `http://127.0.0.1:${server.address().port}/events`,
  })));
}

const waitFor = async (manager, id, predicate, timeoutMs = 15000) =>
  manager.waitFor(id, predicate, {timeoutMs});

async function freshLedger(root) {
  const ledger = new Ledger(root);
  await ledger.registerSource({id: 'cooperative', name: 'Synthetic cooperative',
    region: 'delta', metrics: ['temperature']});
  await ledger.registerStation({id: 'station1', name: 'Synthetic station', region: 'delta',
    latitude: 10, longitude: 20, sources: ['cooperative'], activeFrom: '2026-01-01T00:00:00Z'});
  return ledger;
}

const root = await mkdtemp(path.join(os.tmpdir(), 'field-ledger-stream-demo-'));
const report = {steps: []};
try {
  // 第一段：心跳里的中文字符在半个字节处被切断（三字节 UTF-8 只发首字节）。
  const heartbeat = ': 合成站点心跳\n\n';
  const fullPrefix = heartbeat + block('opaque-a', record(1, 20));
  const prefixBytes = Buffer.from(fullPrefix, 'utf8');
  const cutAt = prefixBytes.indexOf(Buffer.from('合')) + 1;
  const phaseOne = [
    prefixBytes.subarray(0, cutAt),
    fullPrefix, // 重发：还没确认过任何事件，整流从头
    '',
  ];
  const gateway = await scriptedGateway(phaseOne);
  const ledger = await freshLedger(root);
  const manager = new StreamManager(ledger, {retryMs: 20});
  const stream = await manager.register({id: 'field-sub', url: gateway.base, source: 'cooperative',
    retryMs: 20, maxReconnects: 5, maxIdleRounds: 1});
  await waitFor(manager, 'field-sub', s => s.status === 'idle' && s.position === 'opaque-a');
  report.steps.push({name: 'half-character-cut',
    requests: [...gateway.requests], position: (await ledger.streamStatus('field-sub')).position,
    delivered: (await ledger.streamStatus('field-sub')).counters.delivered});
  assert.deepEqual(gateway.requests, [null, null, 'opaque-a']);
  assert.equal((await ledger.query()).rows[0].value, 20);
  await manager.close();
  await new Promise(done => gateway.server.close(done));

  // 第二段：坏事件（坏 JSON）后跟正常事件，验证隔离不中断流。
  const phaseTwo = [
    'id: opaque-bad\nevent: observation\ndata: {broken}\n\n',
    block('opaque-b', record(2, 21)),
    '',
  ];
  const gatewayTwo = await scriptedGateway(phaseTwo);
  const ledgerTwo = new Ledger(root);
  const managerTwo = new StreamManager(ledgerTwo, {retryMs: 20});
  // 网关地址变更走重新配置：代际提升后旧连接回调不再有效。
  await ledgerTwo.reconfigureStream('field-sub', {url: gatewayTwo.base}, 'operator');
  await managerTwo.start(); // 重启恢复：从持久化位置 opaque-a 携带 Last-Event-ID
  await waitFor(managerTwo, 'field-sub', s => s.position === 'opaque-b' && s.status === 'idle');
  const deadLetters = await ledgerTwo.streamDeadLetters({streamId: 'field-sub'});
  report.steps.push({name: 'bad-then-good', requests: [...gatewayTwo.requests],
    deadLetter: deadLetters[0]?.reasonCode, position: (await ledgerTwo.streamStatus('field-sub')).position});
  assert.equal(gatewayTwo.requests[0], 'opaque-a'); // 重连带已完成接收的最后事件 ID
  assert.equal(deadLetters.length, 1);
  assert.equal(deadLetters[0].reasonCode, 'INVALID_JSON');
  assert.ok(deadLetters[0].raw.includes('{broken}'));
  await managerTwo.close();
  await new Promise(done => gatewayTwo.server.close(done));

  // 第三段：同 ID 换正文的冲突证据 + 慢写入下的背压。
  const conflictBlock = block('opaque-b', record(2, 99)); // 与已落库的 opaque-b 同 ID 异文
  const slowBlocks = [
    conflictBlock,
    block('opaque-c', record(3, 22)),
    block('opaque-d', record(4, 23), ),
    block('opaque-e', record(5, 24)),
    '',
  ];
  const slowGateway = await scriptedGateway(slowBlocks, {writeDelayMs: 30});
  const ledgerThree = new Ledger(root);
  await ledgerThree.reconfigureStream('field-sub', {url: slowGateway.base}, 'operator');
  let slowCommits = 0;
  const originalDeliver = ledgerThree.deliverStreamMessage.bind(ledgerThree);
  ledgerThree.deliverStreamMessage = async (id, message, actor) => {
    slowCommits += 1;
    await new Promise(resolve => setTimeout(resolve, 25)); // 慢消费者
    return originalDeliver(id, message, actor);
  };
  const managerThree = new StreamManager(ledgerThree, {retryMs: 20});
  await managerThree.start();
  await waitFor(managerThree, 'field-sub', s => s.position === 'opaque-e' && s.status === 'idle');
  const conflicts = await ledgerThree.streamDeadLetters({streamId: 'field-sub'});
  const conflict = conflicts.find(item => item.reasonCode === 'SSE_EVENT_CONFLICT');
  const runtime = managerThree.status('field-sub');
  report.steps.push({name: 'conflict-and-slow-writes',
    conflict: conflict.reasonCode, slowCommits,
    maxChunkBytes: runtime.maxChunkBytes,
    peakEventBytes: runtime.peakEventBytes,
    position: (await ledgerThree.streamStatus('field-sub')).position});
  assert.ok(conflict, 'conflict evidence must be retained');
  assert.ok(conflict.raw.includes('99'));
  // 慢写入下读取循环仍按块串行推进（读一块→落库完成→再读下一块），
  // 任一时刻在途的最大单个事件远低于事件上限，慢消费者不会造成无界积压。
  assert.ok(runtime.peakEventBytes > 0 && runtime.peakEventBytes < 65536,
    'in-flight event bytes stay bounded under slow writes');
  assert.equal(slowCommits, 4);
  await managerThree.close();
  await new Promise(done => slowGateway.server.close(done));

  // 终态核对：重启新进程，对照观测历史、修订链与接收位置。
  const ledgerFinal = new Ledger(root);
  const rows = (await ledgerFinal.query()).rows;
  const history = await ledgerFinal.history(rows[0].key);
  const finalStream = await ledgerFinal.streamStatus('field-sub');
  const verification = await ledgerFinal.verify();
  const allDeadLetters = await ledgerFinal.streamDeadLetters({streamId: 'field-sub', status: 'all'});
  report.steps.push({name: 'restart-reconciliation',
    revisions: history.history.map(h => h.revision),
    values: history.history.map(h => h.value),
    position: finalStream.position,
    counters: finalStream.counters,
    deadLetters: allDeadLetters.map(d => d.reasonCode),
    valid: verification.valid});

  assert.equal(verification.valid, true);
  assert.deepEqual(history.history.map(h => h.revision), [1, 2, 3, 4, 5]);
  assert.deepEqual(history.history.map(h => h.value), [20, 21, 22, 23, 24]);
  assert.equal(finalStream.position, 'opaque-e');
  assert.equal(finalStream.counters.delivered, 5); // 5 个不透明事件各生效一次
  assert.equal(rows[0].revision, 5);
  assert.equal(rows[0].value, 24); // 冲突的 99 从未生效
  assert.deepEqual(allDeadLetters.map(d => d.reasonCode).sort(),
    ['INVALID_JSON', 'SSE_EVENT_CONFLICT']);

  // 坏事件可单独重新处理。
  const malformed = allDeadLetters.find(d => d.reasonCode === 'INVALID_JSON');
  await ledgerFinal.reprocessDeadLetter({id: malformed.id, action: 'discard',
    actor: 'operator', reason: 'malformed gateway frame discarded after review'});
  assert.equal((await ledgerFinal.streamDeadLetters({streamId: 'field-sub'})).length, 1);

  const journal = await readFile(path.join(root, 'journal.jsonl'), 'utf8');
  report.journalTransactions = journal.trim().split('\n').length;
  process.stdout.write(JSON.stringify(report, null, 2) + '\n');
  process.stdout.write('事件流订阅演练：半字符断开、重发去重、坏事件隔离、冲突留证、慢写入背压、重启核对全部通过\n');
} finally {
  await rm(root, {recursive: true, force: true});
}
