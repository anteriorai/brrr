import type { Message, Queue, QueuePopResult } from "../queue.ts";
import { NotFoundError, } from "../errors.ts";
import type { Cache, MemKey, Store } from "../store.ts";

interface Deferred<T> {
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
}

export class InMemoryQueue implements Queue {
  public readonly timeout = 10;

  private readonly queues: Map<string, {
    items: Message[];
    deferred: Deferred<Message>[];
  }[]>;

  private closing = false;
  private flushing = false;

  public constructor(topics: string[]) {
    this.queues = new Map(topics.map((topic) => [topic, []]));
  }

  async push(topic: string, message: Message): Promise<void> {
    const queue = this.getTopicQueue(topic)
    queue.push(Promise.resolve(message))
  }

  async pop(topic: string): Promise<QueuePopResult> {
    const queue = this.getTopicQueue(topic)
    const front = await queue.shift()
    if (front) {
      return { kind: "Ok", message: front }
    }
    const deferred = new Promise<Message>(() => {
    })
    queue.push(deferred)
    return deferred
  }

  public flush(): void {
    this.flushing = true
  }

  private getTopicQueue(topic: string): Promise<Message>[] {
    const queue = this.queues.get(topic);
    if (!queue) {
      throw new Error(`Could not find queue for topic "${queue}"`);
    }
    return queue;
  }
}

export class InMemoryStore implements Store {
  private store = new Map<string, Uint8Array>();

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.key2str(key);
    const value = this.store.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      return false
    }
    this.store.delete(keyStr);
    return true
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean> {
    const keyStr = this.key2str(key);
    const currentValue = this.store.get(keyStr);
    if (!currentValue || !this.isEqualBytes(currentValue, expected)) {
      return false
    }
    this.store.set(keyStr, value);
    return true
  }

  public async delete(key: MemKey): Promise<void> {
    const keyStr = this.key2str(key);
    if (!this.store.has(keyStr)) {
      throw new NotFoundError(key);
    }
    this.store.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array> {
    const keyStr = this.key2str(key);
    const value = this.store.get(keyStr);
    if (!value) {
      throw new NotFoundError(key);
    }
    return value;
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.store.has(keyStr);
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    this.store.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<boolean> {
    const keyStr = this.key2str(key);
    if (this.store.has(keyStr)) {
      return false
    }
    this.store.set(keyStr, value);
    return true
  }

  private key2str(key: MemKey): string {
    return `${key.type}/${key.callHash}`;
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((_, i) => a[i] === b[i]);
  }
}

export class InMemoryCache implements Cache {
  private readonly cache = new Map<string, number>();

  public async incr(key: string): Promise<number> {
    const next = (this.cache.get(key) ?? 0) + 1;
    this.cache.set(key, next);
    return next;
  }
}
