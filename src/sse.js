import {integer} from './value.js';

const LF = '\n';
const CR = '\r';
const NUL = '\0';

// 字节级 text/event-stream 解析器。
// 网络块可能在任意位置切开一个 UTF-8 字符，因此解码必须保持 streaming 状态；
// 解析器只保留“当前未结束的一行/一个事件”，内存由 maxEventBytes 限制。
export class EventStreamParser {
  constructor({maxEventBytes = 1024 * 1024} = {}) {
    this.maxEventBytes = integer(maxEventBytes, 'maxEventBytes', 16, 16 * 1024 * 1024);
    this.decoder = new TextDecoder('utf-8');
    this.line = '';
    this.event = null;
    this.heartbeats = 0;
    this.peakEventBytes = 0;
  }
  startEvent() {
    this.event = {id: null, type: null, dataParts: [], rawLines: [], bytes: 0,
      tooLarge: false, sawField: false};
  }
  get bufferedBytes() {
    return (this.event?.bytes ?? 0) + Buffer.byteLength(this.line, 'utf8');
  }
  // 喂入一个网络块，返回本次解码后边界完整的消息数组。
  push(chunk) {
    const emitted = [];
    if (this.event === null) {
      this.startEvent();
    }
    this.event.bytes += chunk.length;
    this.peakEventBytes = Math.max(this.peakEventBytes, this.event.bytes);
    if (this.event.bytes > this.maxEventBytes && !this.event.tooLarge) {
      this.event.tooLarge = true;
    }
    const text = this.decoder.decode(chunk, {stream: true});
    for (const char of text) {
      this.line += char;
      if (char === LF) {
        this.consumeLine(emitted);
        this.line = '';
      }
    }
    return emitted;
  }
  consumeLine(emitted) {
    let line = this.line.slice(0, -1);
    if (line.endsWith(CR)) {
      line = line.slice(0, -1);
    }
    if (line === '') {
      this.dispatch(emitted);
      return;
    }
    if (line.startsWith(':')) {
      if (!this.event.sawField) {
        this.heartbeats += 1;
      }
      this.keepRaw(line);
      return;
    }
    this.event.sawField = true;
    this.keepRaw(line);
    let field = line;
    let value = '';
    const colon = line.indexOf(':');
    if (colon >= 0) {
      field = line.slice(0, colon);
      value = line.slice(colon + 1);
      if (value.startsWith(' ')) {
        value = value.slice(1);
      }
    }
    if (field === 'id') {
      if (!value.includes(NUL)) {
        this.event.id = value;
      }
    } else if (field === 'event') {
      this.event.type = value;
    } else if (field === 'data') {
      this.event.dataParts.push(value);
    }
    // retry 与未知字段按 SSE 约定忽略，原文仍保留在 rawLines 中作为证据。
  }
  keepRaw(line) {
    if (!this.event.tooLarge) {
      this.event.rawLines.push(line);
    }
  }
  dispatch(emitted) {
    const current = this.event;
    this.startEvent();
    if (!current.sawField) {
      // 只有注释/空行的边界（含连续空行）是心跳，不产生消息。
      return;
    }
    if (current.id === null && current.type === null && current.dataParts.length === 0) {
      // 仅含 retry 等无投递语义字段的块同样不产生消息。
      return;
    }
    const base = {id: current.id, type: current.type, raw: current.rawLines.join(LF)};
    if (current.tooLarge) {
      emitted.push({...base, code: 'SSE_EVENT_TOO_LARGE',
        message: 'Event exceeds the configured byte limit', data: null,
        details: {limit: this.maxEventBytes, bytes: current.bytes}});
      return;
    }
    emitted.push({...base, data: current.dataParts.join(LF)});
  }
  // 连接结束时调用：未遇到空行边界的事件只收到了一半，绝不能确认。
  final() {
    const unfinished = Boolean(this.event && (this.event.sawField || this.line !== ''));
    const leftoverBytes = this.bufferedBytes;
    return {unfinished, leftoverBytes};
  }
}
