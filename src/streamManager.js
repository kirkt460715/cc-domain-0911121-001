import {EventStreamParser} from './sse.js';
import {LedgerError} from './errors.js';
import {integer} from './value.js';

const delay = (ms, signal) => new Promise((resolve, reject) => {
  if (signal?.aborted) {
    reject(signal.reason ?? new Error('aborted'));
    return;
  }
  const timer = setTimeout(resolve, ms);
  signal?.addEventListener('abort', () => {
    clearTimeout(timer);
    reject(signal.reason ?? new Error('aborted'));
  }, {once: true});
});

// 每个订阅只有一个连接循环。循环持有的 generation 与持久化的 stream.generation
// 不同即视为“旧连接”，其后续回调一律不得再推进订阅。
export class StreamManager {
  constructor(ledger, options = {}) {
    this.ledger = ledger;
    this.actor = options.actor ?? 'stream-manager';
    // detached 模式只持久化订阅配置、不启动连接循环，供一次性 CLI 命令使用；
    // 真正的连接循环由长驻进程（serve / stream-run）承载。
    this.detached = options.detached ?? false;
    this.runtime = new Map();
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.delayImpl = options.delay ?? delay;
    this.onStatus = options.onStatus ?? null;
  }

  runtimeOf(id) {
    if (!this.runtime.has(id)) {
      this.runtime.set(id, {
        generation: 0,
        epoch: 0,
        controller: null,
        loop: null,
        stopRequested: false,
        phase: 'detached',
        lastEventId: null,
        maxBufferedBytes: 0,
        maxChunkBytes: 0,
        peakEventBytes: 0,
        lastHeartbeatAt: null,
        heartbeats: 0,
        reconnects: 0,
        idleRounds: 0,
        connections: 0,
        error: null,
      });
    }
    return this.runtime.get(id);
  }

  async start() {
    const streams = await this.ledger.streamStatus();
    for (const stream of streams) {
      this.runtimeOf(stream.id).generation = stream.generation;
      // running 与 idle（暂时无数据，非暂停/终止）都应在进程启动后继续监听。
      if (stream.status === 'running' || stream.status === 'idle') {
        this.ensure(stream.id);
      }
    }
    return {subscriptions: streams.length,
      running: streams.filter(s => s.status === 'running' || s.status === 'idle').length};
  }

  async register(input) {
    const result = await this.ledger.registerStream(input, this.actor);
    this.runtimeOf(result.stream.id).generation = result.stream.generation;
    if (!this.detached) {
      this.ensure(result.stream.id);
    }
    return result.stream;
  }

  async pause(id) {
    const result = await this.ledger.controlStream(id, 'paused', this.actor);
    const runtime = this.runtimeOf(id);
    runtime.phase = 'paused';
    runtime.stopRequested = true;
    runtime.controller?.abort(new Error('subscription paused'));
    return result;
  }

  async resume(id) {
    const result = await this.ledger.controlStream(id, 'running', this.actor);
    const runtime = this.runtimeOf(id);
    runtime.phase = 'running';
    runtime.stopRequested = false;
    runtime.error = null;
    if (!this.detached) {
      this.ensure(id, result.generation);
    }
    return result;
  }

  async stop(id) {
    const result = await this.ledger.controlStream(id, 'stopped', this.actor);
    const runtime = this.runtimeOf(id);
    runtime.phase = 'stopped';
    runtime.stopRequested = true;
    runtime.controller?.abort(new Error('subscription stopped'));
    return result;
  }

  async reconfigure(id, patch) {
    const result = await this.ledger.reconfigureStream(id, patch, this.actor);
    const runtime = this.runtimeOf(id);
    runtime.phase = result.status;
    runtime.stopRequested = false;
    runtime.error = null;
    runtime.idleRounds = 0;
    runtime.reconnects = 0;
    // generation 提升：旧连接循环在任意 await 后都会发现代际过期而退出。
    runtime.controller?.abort(new Error('subscription reconfigured'));
    if (!this.detached && result.status === 'running') {
      this.ensure(id, result.generation);
    }
    return result;
  }

  async reconnect(id) {
    const durable = await this.ledger.streamStatus(id);
    if (durable.status !== 'running') {
      return {id, status: durable.status, reconnected: false};
    }
    const runtime = this.runtimeOf(id);
    runtime.epoch += 1;
    runtime.phase = 'running';
    runtime.stopRequested = false;
    runtime.error = null;
    runtime.idleRounds = 0;
    this.ensure(id, durable.generation, runtime.epoch);
    return {id, status: 'running', reconnected: true, epoch: runtime.epoch};
  }

  ensure(id, knownGeneration, knownEpoch) {
    const runtime = this.runtimeOf(id);
    if (knownGeneration !== undefined) {
      runtime.generation = knownGeneration;
    }
    if (knownEpoch !== undefined) {
      runtime.epoch = knownEpoch;
    }
    if (runtime.loop) {
      return;
    }
    const loop = this.run(id, runtime.epoch);
    runtime.loop = loop;
    loop.finally(() => {
      if (runtime.loop === loop) {
        runtime.loop = null;
        runtime.controller = null;
      }
    }).catch(() => {});
  }

  async run(id, epoch) {
    const runtime = this.runtimeOf(id);
    try {
      while (true) {
        const stream = await this.ledger.streamStatus(id);
        if (stream.status !== 'running' && stream.status !== 'idle') {
          runtime.phase = stream.status;
          return;
        }
        if (stream.generation !== runtime.generation || epoch !== runtime.epoch) {
          return;
        }
        if (stream.status === 'idle') {
          // 进程重启后恢复一个此前空闲的订阅：回到 running 再建立连接。
          const resumed = await this.ledger.controlStream(id, 'running', this.actor);
          runtime.generation = resumed.generation;
          runtime.phase = 'running';
          continue;
        }
        const outcome = await this.connectOnce(id, stream, runtime);
        if (runtime.phase === 'aborted' || stream.generation !== runtime.generation || epoch !== runtime.epoch) {
          return;
        }
        if (outcome.delivered > 0) {
          // 任一连接完成了持久化投递，说明链路恢复：清空空闲与重连计数。
          runtime.idleRounds = 0;
          runtime.reconnects = 0;
        } else if (runtime.phase !== 'torn') {
          runtime.idleRounds += 1;
          if (stream.maxIdleRounds > 0 && runtime.idleRounds >= stream.maxIdleRounds) {
            await this.ledger.noteStreamIdle(id, this.actor);
            runtime.phase = 'idle';
            this.onStatus?.(id, 'idle');
            return;
          }
        }
        // torn（半个字符/半个事件断开）强制重连，不计空闲轮次；其余情况按上面的策略计数。
        runtime.reconnects += 1;
        if (runtime.reconnects > stream.maxReconnects) {
          throw new LedgerError('STREAM_RECONNECTS', 'Reconnect budget exhausted',
            {reconnects: runtime.reconnects});
        }
        runtime.phase = 'reconnect-wait';
        await this.delayImpl(stream.retryMs * runtime.reconnects, this.signalFor(id, runtime));
      }
    } catch (error) {
      const durable = await this.ledger.streamStatus(id);
      if (durable.generation !== runtime.generation || epoch !== runtime.epoch) {
        return;
      }
      if (error?.name === 'AbortError' || runtime.stopRequested) {
        return;
      }
      await this.ledger.noteStreamError(id, error, this.actor);
      runtime.phase = 'error';
      runtime.error = {code: error.code ?? 'STREAM_CONNECT', message: String(error.message ?? error)};
      this.onStatus?.(id, 'error');
    }
  }

  signalFor(id, runtime) {
    const controller = new AbortController();
    runtime.controller = controller;
    return controller.signal;
  }

  async connectOnce(id, stream, runtime) {
    const signal = this.signalFor(id, runtime);
    runtime.connections += 1;
    runtime.phase = 'connecting';
    const parser = new EventStreamParser({maxEventBytes: stream.maxEventBytes});
    const headers = {};
    if (stream.position) {
      headers['Last-Event-ID'] = stream.position;
    }
    let response;
    try {
      response = await this.fetchWithTimeout(stream, headers, signal);
    } catch (error) {
      if (signal.aborted) {
        runtime.phase = 'aborted';
      }
      throw error;
    }
    if (!response.ok || !response.body) {
      throw new LedgerError('STREAM_RESPONSE', 'Subscription endpoint rejected the connection',
        {status: response.status}, 502);
    }
    runtime.phase = 'streaming';
    const outcome = {messages: 0, applied: 0, duplicates: 0, deadletters: 0, heartbeats: 0};
    const reader = response.body.getReader();
    try {
      while (true) {
        // 一次只保留一个网络块；块内解析出的消息在落库完成前不会再读下一块，
        // 因此慢消费者不会造成无界积压。
        const {value: chunk, done} = await reader.read();
        if (done) {
          break;
        }
        runtime.maxChunkBytes = Math.max(runtime.maxChunkBytes ?? 0, chunk.length);
        const messages = parser.push(chunk);
        runtime.maxBufferedBytes = Math.max(runtime.maxBufferedBytes, parser.bufferedBytes);
        runtime.peakEventBytes = Math.max(runtime.peakEventBytes ?? 0, parser.peakEventBytes);
        for (const message of messages) {
          if (message.id === null && message.type === null && message.data === '' && !message.code) {
            runtime.lastHeartbeatAt = new Date().toISOString();
            runtime.heartbeats += 1;
            outcome.heartbeats += 1;
            continue;
          }
          const result = await this.ledger.deliverStreamMessage(id, message, this.actor);
          outcome.messages += 1;
          if (result.ignored) {
            outcome.messages -= 1;
          } else if (result.reasonCode) {
            outcome.deadletters += 1;
          } else if (result.outcome === 'duplicate') {
            outcome.duplicates += 1;
          } else {
            outcome.applied += 1;
          }
          runtime.lastEventId = (await this.ledger.streamStatus(id)).position;
        }
      }
      const tail = parser.final();
      if (tail.unfinished) {
        // 连接在半个字符/半个事件处断开：绝不确认，下一轮带着 Last-Event-ID 重发。
        runtime.phase = 'torn';
      }
      outcome.delivered = outcome.messages;
      return outcome;
    } finally {
      reader.releaseLock();
    }
  }

  async fetchWithTimeout(stream, headers, signal) {
    const fetchPromise = this.fetchImpl(stream.url, {headers, signal});
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new LedgerError('STREAM_TIMEOUT',
        'Subscription connection timed out', {connectTimeoutMs: stream.connectTimeoutMs})),
        stream.connectTimeoutMs);
    });
    try {
      return await Promise.race([fetchPromise, timeout]);
    } finally {
      clearTimeout(timer);
    }
  }

  status(id) {
    const runtime = this.runtimeOf(id);
    return {
      id,
      phase: runtime.phase,
      generation: runtime.generation,
      epoch: runtime.epoch,
      lastEventId: runtime.lastEventId,
      maxBufferedBytes: runtime.maxBufferedBytes,
      maxChunkBytes: runtime.maxChunkBytes,
      peakEventBytes: runtime.peakEventBytes,
      reconnects: runtime.reconnects,
      idleRounds: runtime.idleRounds,
      connections: runtime.connections,
      heartbeats: runtime.heartbeats,
      error: runtime.error,
    };
  }

  async waitFor(id, predicate, {timeoutMs = 10000, settleMs = 0} = {}) {
    integer(timeoutMs, 'timeoutMs', 1);
    const deadline = Date.now() + timeoutMs;
    let stableSince = null;
    let lastPosition;
    while (Date.now() < deadline) {
      const durable = await this.ledger.streamStatus(id);
      const runtime = this.status(id);
      if (predicate(durable, runtime)) {
        return durable;
      }
      if (settleMs > 0) {
        // 长连接演练源不会主动关闭：位置在 settleMs 内保持不变即视为本轮稳定。
        if (durable.position !== undefined && durable.position === lastPosition &&
          ['streaming', 'reconnect-wait'].includes(runtime.phase)) {
          stableSince ??= Date.now();
          if (Date.now() - stableSince >= settleMs) {
            return durable;
          }
        } else {
          stableSince = null;
          lastPosition = durable.position;
        }
      }
      await this.delayImpl(20);
    }
    throw new LedgerError('STREAM_WAIT_TIMEOUT', 'Subscription condition was not reached in time');
  }

  async close() {
    for (const [id, runtime] of this.runtime) {
      runtime.stopRequested = true;
      runtime.controller?.abort(new Error('manager closing'));
      if (runtime.loop) {
        await runtime.loop.catch(() => {});
      }
    }
  }
}
