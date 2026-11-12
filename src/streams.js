import {choice, digest, exactKeys, identifier, integer, jsonParse, timestamp} from './value.js';
import {fail, requireValue, LedgerError} from './errors.js';
import {event} from './state.js';
import {prepareBatch} from './ingest.js';

export const STREAM_STATUSES = Object.freeze(['running', 'paused', 'idle', 'error', 'stopped']);
const TERMINAL = new Set(['stopped']);
const MAX_URL = 2048;

export function subscriptionInput(input, {partial = false} = {}) {
  const allowed = ['id', 'url', 'source', 'receivedAt', 'retryMs', 'maxReconnects',
    'maxIdleRounds', 'maxEventBytes', 'connectTimeoutMs', 'status', 'generation'];
  if (partial) {
    // 局部更新可能带着已持久化的内部字段（counters/position 等），只校验受支持的键。
    for (const key of Object.keys(input)) {
      requireValue(allowed.includes(key), 'UNKNOWN_FIELD', `Unknown field: ${key}`);
    }
  } else {
    exactKeys(input, allowed, ['id', 'url', 'source']);
  }
  const url = new URL(input.url);
  requireValue(['http:', 'https:'].includes(url.protocol), 'STREAM_URL', 'Subscription URL must use http or https');
  requireValue(input.url.length <= MAX_URL, 'STREAM_URL', 'Subscription URL is too long');
  identifier(input.source, 'source');
  const retryMs = integer(input.retryMs ?? 250, 'retryMs', 10, 3600000);
  const maxReconnects = integer(input.maxReconnects ?? 5, 'maxReconnects', 0, 100);
  const maxIdleRounds = integer(input.maxIdleRounds ?? 3, 'maxIdleRounds', 0, 100);
  const maxEventBytes = integer(input.maxEventBytes ?? 1024 * 1024, 'maxEventBytes', 64, 16 * 1024 * 1024);
  const connectTimeoutMs = integer(input.connectTimeoutMs ?? 10000, 'connectTimeoutMs', 100, 300000);
  const status = input.status === undefined ? 'running' : choice(input.status, STREAM_STATUSES, 'status');
  return {
    id: identifier(input.id, 'stream id'),
    url: input.url,
    source: input.source,
    receivedAt: input.receivedAt == null ? null : timestamp(input.receivedAt, 'receivedAt'),
    retryMs,
    maxReconnects,
    maxIdleRounds,
    maxEventBytes,
    connectTimeoutMs,
    status,
  };
}

export function registerStream(state, input) {
  const config = subscriptionInput(input);
  requireValue(!state.streams[config.id], 'STREAM_EXISTS', 'Subscription already registered');
  requireValue(state.sources[config.source], 'SOURCE_MISSING', 'Unknown source');
  const stream = {...config, generation: 1, position: null, lastError: null,
    counters: {delivered: 0, duplicates: 0, deadletters: 0, conflicts: 0},
    registeredAt: new Date().toISOString()};
  return {events: [event('stream.registered', stream)], result: {stream}};
}

export function controlStream(state, id, status) {
  identifier(id, 'stream id');
  const stream = state.streams[id];
  requireValue(stream, 'STREAM_MISSING', 'Subscription does not exist');
  requireValue(!TERMINAL.has(stream.status), 'STREAM_STOPPED', 'Subscription has been stopped');
  requireValue(STREAM_STATUSES.includes(status), 'STREAM_STATUS', 'Unsupported status');
  const updated = {...stream, status,
    // running 表示一次新的控制代际（恢复/重新开始）；但从 idle 恢复只是继续监听，不提升代际。
    generation: status === 'running' && stream.status !== 'idle'
      ? stream.generation + 1 : stream.generation,
    lastError: status === 'running' ? null : stream.lastError};
  const result = {id, status: updated.status, generation: updated.generation, position: stream.position};
  return {events: [event('stream.updated', updated)], result};
}

export function reconfigureStream(state, id, patch) {
  identifier(id, 'stream id');
  const stream = state.streams[id];
  requireValue(stream, 'STREAM_MISSING', 'Subscription does not exist');
  requireValue(!TERMINAL.has(stream.status), 'STREAM_STOPPED', 'Subscription has been stopped');
  const configurable = ['url', 'source', 'receivedAt', 'retryMs', 'maxReconnects',
    'maxIdleRounds', 'maxEventBytes', 'connectTimeoutMs'];
  for (const key of Object.keys(patch)) {
    requireValue(configurable.includes(key), 'UNKNOWN_FIELD', `Unknown field: ${key}`);
  }
  if (patch.source !== undefined) {
    requireValue(state.sources[patch.source], 'SOURCE_MISSING', 'Unknown source');
  }
  const candidate = Object.fromEntries(Object.entries({...stream, ...patch, id,
    status: stream.status}).filter(([key]) => configurable.includes(key) || key === 'id' || key === 'status'));
  const merged = subscriptionInput(candidate, {partial: true});
  const updated = {...stream, ...merged,
    generation: stream.generation + 1, status: stream.status, lastError: null};
  const result = {id, status: updated.status, generation: updated.generation, url: updated.url, position: stream.position};
  return {events: [event('stream.updated', updated)], result};
}

export function noteStreamError(state, id, error) {
  const stream = state.streams[id];
  const updated = {...stream, status: 'error', lastError: {
    code: error instanceof LedgerError ? error.code : 'STREAM_CONNECT',
    message: String(error?.message ?? error),
    at: new Date().toISOString(),
  }};
  return {events: [event('stream.updated', updated)], result: {id, status: 'error'}};
}

export function noteStreamIdle(state, id) {
  const stream = state.streams[id];
  if (stream.status !== 'running') {
    return {events: [], result: {id, status: stream.status}};
  }
  const updated = {...stream, status: 'idle'};
  return {events: [event('stream.updated', updated)], result: {id, status: 'idle'}};
}

const deliveryKey = (streamId, eventId) => digest({streamId, eventId});
const streamBatchId = (streamId, eventId) => `stream-${deliveryKey(streamId, eventId).slice(0, 40)}`;
const deadLetterId = (streamId, eventId, code) => digest({streamId, eventId, code}).slice(0, 32);

function deadLetterEvent(state, stream, message, code, reason, details = {}) {
  const synthetic = !message.id;
  const eventId = message.id ?? `unidentified-${digest({streamId: stream.id, raw: message.raw}).slice(0, 24)}`;
  const key = deliveryKey(stream.id, eventId);
  const existing = state.streamDeadLetters[deadLetterId(stream.id, eventId, code)];
  if (existing) {
    // 重连后同一条无 id 坏事件再次送达：已留证，幂等跳过，位置仍不推进。
    return {events: [], result: existing};
  }
  const id = deadLetterId(stream.id, eventId, code);
  const item = {
    id,
    streamId: stream.id,
    eventId,
    key,
    status: 'open',
    reasonCode: code,
    reason,
    details,
    raw: message.raw,
    data: message.data ?? null,
    receivedAt: new Date().toISOString(),
    ...(synthetic ? {advancePosition: false} : {}),
  };
  return {events: [event('stream.deadlettered', item)], result: item};
}

// 处理一条边界完整的 SSE 消息；所有返回的事件都在同一个事务里提交，
// 因此“台账写入”和“接收位置推进”要么同时生效，要么同时不生效。
export function deliverStreamMessage(state, streamId, message) {
  const stream = state.streams[streamId];
  requireValue(stream, 'STREAM_MISSING', 'Subscription does not exist');
  if (message.code) {
    return deadLetterEvent(state, stream, message, message.code, message.message, message.details ?? {});
  }
  const type = message.type ?? 'observation';
  if (type !== 'observation') {
    // 非观测事件（如状态通知）不推进位置，也不写台账。
    return {events: [], result: {streamId, ignored: true, type}};
  }
  if (!message.id) {
    // 没有不透明标识的观测事件无法在重连后去重：留证进死信，但不推进位置。
    return deadLetterEvent(state, stream, message,
      'SSE_NO_ID', 'Observation event is missing its opaque id');
  }
  const key = deliveryKey(stream.id, message.id);
  const previous = state.streamDeliveries[key];
  if (previous) {
    if (previous.payloadDigest === digest(message.data ?? '')) {
      // 服务重发最后一条：正文完全一致，幂等跳过，不重复生效；累计重发次数。
      const duplicate = {...previous, outcome: 'duplicate',
        redeliveries: (previous.redeliveries ?? 0) + 1,
        redeliveredAt: new Date().toISOString()};
      return {events: [event('stream.delivered', duplicate)], result: {outcome: 'duplicate', key}};
    }
    // 同 ID 换正文：保留冲突证据，拒收新正文，位置不越过该事件。
    const item = {
      id: deadLetterId(stream.id, message.id, 'SSE_EVENT_CONFLICT'),
      streamId: stream.id,
      eventId: message.id,
      key,
      status: 'open',
      reasonCode: 'SSE_EVENT_CONFLICT',
      reason: 'An event id was redelivered with different content',
      details: {originalDigest: previous.payloadDigest, incomingDigest: digest(message.data ?? '')},
      raw: message.raw,
      data: message.data,
      receivedAt: new Date().toISOString(),
    };
    return {events: [event('stream.deadlettered', item)], result: item};
  }
  let payload;
  try {
    payload = jsonParse(message.data, 'stream event data');
  } catch (error) {
    if (!(error instanceof LedgerError)) {
      throw error;
    }
    return deadLetterEvent(state, stream, message, error.code, error.message, error.details);
  }
  if (payload === null || typeof payload !== 'object' || Array.isArray(payload)) {
    return deadLetterEvent(state, stream, message, 'SSE_PAYLOAD',
      'Stream event data must contain a single observation object');
  }
  const receivedAt = stream.receivedAt ?? new Date().toISOString();
  const batch = {id: streamBatchId(stream.id, message.id), source: stream.source,
    receivedAt, mode: 'partial', records: [payload]};
  const prepared = prepareBatch(state, batch);
  const delivery = {
    key,
    streamId: stream.id,
    eventId: message.id,
    batchId: batch.id,
    payloadDigest: digest(message.data),
    outcome: 'applied',
    receivedAt,
  };
  return {events: [...prepared.events, event('stream.delivered', delivery)],
    result: {outcome: 'applied', key, batch: prepared.result}};
}

export function listStreamDeadLetters(state, {streamId, status = 'open'} = {}) {
  requireValue(['open', 'resolved', 'all'].includes(status), 'STREAM_DL_STATUS', 'Invalid dead-letter status');
  return Object.values(state.streamDeadLetters).filter(item =>
    (!streamId || item.streamId === streamId) && (status === 'all' || item.status === status))
    .sort((a, b) => a.receivedAt.localeCompare(b.receivedAt));
}

// 重新处理坏事件。replace 由值班员给出修正后的观测记录；
// retry 仅对传输级坏事件有效，使用原始 data 重新投递。
export function reprocessDeadLetter(state, input) {
  exactKeys(input, ['id', 'action', 'actor', 'reason', 'record'],
    ['id', 'action', 'actor', 'reason']);
  const id = identifier(input.id, 'dead-letter id');
  const item = state.streamDeadLetters[id];
  requireValue(item, 'STREAM_DL_MISSING', 'Dead-letter item does not exist');
  requireValue(item.status === 'open', 'STREAM_DL_RESOLVED', 'Dead-letter item is already resolved');
  const actor = identifier(input.actor, 'actor');
  requireValue(typeof input.reason === 'string' && input.reason.trim().length >= 3,
    'REASON', 'Reprocessing requires an explanation');
  const action = choice(input.action, ['discard', 'retry', 'replace'], 'action');
  const stream = state.streams[item.streamId];
  requireValue(stream, 'STREAM_MISSING', 'Subscription does not exist');
  requireValue(!TERMINAL.has(stream.status), 'STREAM_STOPPED', 'Subscription has been stopped');
  const events = [];
  let record = null;
  if (action === 'retry') {
    requireValue(item.data !== null, 'STREAM_DL_RETRY', 'Only events with decodable data can be retried');
    record = jsonParse(item.data, 'stream event data');
  } else if (action === 'replace') {
    requireValue(input.record && typeof input.record === 'object', 'STREAM_DL_RECORD',
      'A replacement observation record is required');
    record = input.record;
  }
  if (record !== null) {
    const batch = {id: `${streamBatchId(stream.id, item.eventId)}-r${digest(id).slice(0, 8)}`,
      source: stream.source, receivedAt: item.receivedAt, mode: 'partial', records: [record]};
    const prepared = prepareBatch(state, batch);
    events.push(...prepared.events);
  }
  const resolution = {id, status: action === 'discard' ? 'discarded' : 'reprocessed',
    actor, reason: input.reason.trim(), at: new Date().toISOString()};
  events.push(event('stream.deadletter.resolved', resolution));
  return {events, result: {id, ...resolution}};
}
