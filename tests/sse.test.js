import test from 'node:test';
import assert from 'node:assert/strict';
import {EventStreamParser} from '../src/sse.js';
import {code} from './helpers.js';

function feedInChunks(parser, text, size) {
  const bytes = Buffer.from(text, 'utf8');
  const messages = [];
  for (let at = 0; at < bytes.length; at += size) {
    messages.push(...parser.push(bytes.subarray(at, at + size)));
  }
  return messages;
}

test('parser reassembles UTF-8 characters split across network blocks', () => {
  const parser = new EventStreamParser();
  const text = 'id: x1\ndata: 农情观测\n\n';
  const messages = feedInChunks(parser, text, 2);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].id, 'x1');
  assert.equal(messages[0].data, '农情观测');
  assert.equal(messages[0].type, null);
});

test('parser handles CRLF, heartbeats, multi-line data and three-byte writes', () => {
  const parser = new EventStreamParser();
  const stream = [
    ': 合成站点心跳\r\nretry: 50\r\n\r\n',
    'id: a\r\nevent: observation\r\ndata: {"v":1}\r\n\r\n',
    'id: b\nevent: observation\ndata: {"station":"s1",\ndata: "v":2}\n\n',
    'event: status\ndata: {"phase":"ok"}\n\n',
  ].join('');
  const messages = feedInChunks(parser, stream, 3);
  assert.equal(parser.heartbeats, 1);
  // 第一个空行结束仅含注释/retry 的心跳块，随后三个真正的事件边界。
  assert.equal(messages.length, 3);
  assert.deepEqual(messages.map(m => m.id), ['a', 'b', null]);
  assert.equal(messages[1].data, '{"station":"s1",\n"v":2}');
  assert.equal(messages[2].type, 'status');
  assert.equal(messages[0].raw.includes('event: observation'), true);
});

test('a cut inside a multi-byte character leaves an unfinished event', () => {
  const full = 'id: cut\ndata: 农\n\n';
  const bytes = Buffer.from(full, 'utf8');
  const parser = new EventStreamParser();
  const first = parser.push(bytes.subarray(0, bytes.indexOf(Buffer.from('农')) + 1));
  assert.equal(first.length, 0);
  assert.equal(parser.final().unfinished, true);
  assert.ok(parser.final().leftoverBytes > 0);

  // 新解析器从服务重发处重新组装，半个字符不会产生替换字符或假消息。
  const repaired = new EventStreamParser();
  const messages = feedInChunks(repaired, full, 3);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].data, '农');
});

test('oversized events are surfaced with their id instead of growing memory', () => {
  const parser = new EventStreamParser({maxEventBytes: 32});
  const messages = feedInChunks(parser, 'id: big\ndata: ' + 'x'.repeat(120) + '\n\n', 7);
  assert.equal(messages.length, 1);
  assert.equal(messages[0].code, 'SSE_EVENT_TOO_LARGE');
  assert.equal(messages[0].id, 'big');
  assert.ok(parser.bufferedBytes <= 32);
  // 超限事件结束后解析器仍可继续处理后续正常事件。
  const next = feedInChunks(parser, 'id: ok\ndata: fine\n\n', 4);
  assert.equal(next[0].id, 'ok');
  assert.equal(next[0].data, 'fine');
});

test('id containing a NUL byte is ignored per SSE rules', () => {
  const parser = new EventStreamParser();
  const messages = feedInChunks(parser, 'id: bad\0id\ndata: x\n\n', 5);
  assert.equal(messages[0].id, null);
});

test('parser rejects invalid byte limits', () => {
  assert.throws(() => new EventStreamParser({maxEventBytes: 1}), code('INVALID_INTEGER'));
});
