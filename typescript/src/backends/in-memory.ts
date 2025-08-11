import type { Queue } from "../queue.ts";
import {
  CompareMismatchError,
  NotFoundError,
  QueueIsClosedError,
  QueueIsEmptyError,
  UnknownTopicError,
} from "../errors.ts";
import type { Cache, MemKey, Store } from "../store.ts";
import { AsyncQueue } from "../lib/async-queue.ts";
import { clearTimeout, setTimeout } from "node:timers";

export class InMemoryQueue implements Queue {
  public readonly timeout = 10;

  private readonly queues: Map<string, AsyncQueue<string>>;

  private closing = false;
  private flushing = false;

  public constructor(topics: string[]) {
    this.queues = new Map(topics.map((topic) => [topic, new AsyncQueue()]));
  }

  public async close() {
    if (this.closing) {
      throw new QueueIsClosedError();
    }
    this.closing = true;
    for (const [_, queue] of this.queues) {
      queue.shutdown();
    }
  }

  public async join() {
    await Promise.all(this.queues.values().map((queue) => queue.join()));
  }

  public async pop(topic: string): Promise<string> {
    const queue = this.getTopicQueue(topic);
    let payload: string;
    if (this.flushing) {
      try {
        payload = queue.popSync();
      } catch (err) {
        if (err instanceof QueueIsEmptyError) {
          queue.shutdown();
          throw new QueueIsClosedError();
        }
        throw err;
      }
    } else {
      const timeout = setTimeout(() => {
        throw new QueueIsEmptyError();
      }, this.timeout);
      try {
        payload = await queue.pop();
      } finally {
        clearTimeout(timeout);
      }
    }
    queue.done();
    return payload;
  }

  public async push(topic: string, message: string): Promise<void> {
    await this.getTopicQueue(topic).push(message);
  }

  public flush() {
    this.flushing = true;
  }

  private getTopicQueue(topic: string): AsyncQueue<string> {
    const queue = this.queues.get(topic);
    if (!queue) {
      throw new UnknownTopicError(topic);
    }
    return queue;
  }
}

export class InMemoryByteStore implements Store, Cache {
  private innerStore = new Map<string, Uint8Array>();
  private innerCache = new Map<string, number>();

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const currentValue = this.innerStore.get(keyStr);
    if (!currentValue || !this.isEqualBytes(currentValue, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  public async delete(key: MemKey): Promise<void> {
    const keyStr = this.key2str(key);
    if (!this.innerStore.has(keyStr)) {
      throw new NotFoundError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value) {
      throw new NotFoundError(key);
    }
    return value;
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.innerStore.has(keyStr);
  }

  public async incr(key: string): Promise<number> {
    const current = this.innerCache.get(key) ?? 0;
    const next = current + 1;
    this.innerCache.set(key, next);
    return next;
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    this.innerStore.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    if (this.innerStore.has(keyStr)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  private key2str(key: MemKey): string {
    return `${key.type}/${key.callHash}`;
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((_, i) => a[i] === b[i]);
  }
}

export class InMemoryEmitter implements Publisher, Subscriber {
  private readonly emitter = new EventEmitter();
  private readonly eventEmitter = new EventEmitter();

  public on(topic: string, listener: (callId: string) => void): void {
    this.emitter.on(topic, listener);
  }

  public onEventSymbol(
    event: typeof BrrrTaskDoneEventSymbol,
    listener: (call: Call) => void,
  ): void {
    this.eventEmitter.on(event, listener);
  }

  public async emit(topic: string, callId: string): Promise<void> {
    this.emitter.emit(topic, callId);
  }

  public async emitEventSymbol(
    event: typeof BrrrTaskDoneEventSymbol,
    call: Call,
  ): Promise<void> {
    this.eventEmitter.emit(event, call);
  }
}
