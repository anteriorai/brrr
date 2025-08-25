import type { QueuePopResult } from "../queue.ts";

interface Deferred<T> {
  resolve: (value: T) => void;
  reject: (err: unknown) => void;
}

export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly generator: Generator<Promise<T>, never, never>;
  private readonly deferred: Deferred<T>[] = [];

  private tasks = 0;
  private shutdownMode = false;

  private resolver: (() => void) | undefined;
  private sentinel = Promise.resolve();

  public constructor() {
    const { items, deferred } = this;
    this.generator = (function* () {
      while (true) {
        yield new Promise<T>((resolve, reject) => {
          if (items.length) {
            resolve(items.shift() as T);
          }
          deferred.push({ resolve, reject });
        });
      }
    })();
  }

  public async push(value: T): Promise<void> {
    if (this.shutdownMode) {
      throw new Error("Queue is closed");
    }
    this.tasks++;
    if (this.tasks === 1) {
      this.sentinel = new Promise((resolve) => {
        this.resolver = resolve;
      });
    }
    if (this.deferred.length) {
      return this.deferred.shift()?.resolve?.(value);
    }
    this.items.push(value);
  }

  public async pop(timeout?: number): Promise<QueuePopResult<T>> {
    if (this.items.length) {
      this.done();
      return { kind: "Ok", value: this.items.shift() as T };
    }
    if (this.shutdownMode) {
      return { kind: "QueueIsClosed" };
    }
    const result = this.generator
      .next()
      .value.then<QueuePopResult<T>>((value) => {
        this.done();
        return { kind: "Ok", value };
      });
    if (!timeout) {
      return result;
    }
    const timer = new Promise<QueuePopResult<T>>((resolve) => {
      setTimeout(() => {
        resolve({ kind: "QueueIsEmpty" });
      }, timeout);
    });
    return Promise.race([result, timer]);
  }

  public popSync(): QueuePopResult<T> {
    if (this.items.length > 0) {
      return { kind: "Ok", value: this.items.shift() as T };
    }
    if (this.shutdownMode) {
      return { kind: "QueueIsClosed" };
    }
    return { kind: "QueueIsEmpty" };
  }

  public done(): void {
    if (this.tasks === 0) {
      throw new Error("done() called too many times");
    }
    this.tasks--;
    if (this.tasks === 0 && this.resolver) {
      this.resolver();
    }
  }

  public join(): Promise<void> {
    return this.sentinel;
  }

  public shutdown(): void {
    this.shutdownMode = true;
    while (this.deferred.length > 0) {
      this.deferred.shift()?.reject(new Error("Queue is closed"));
    }
    if (this.tasks === 0 && this.resolver) {
      this.resolver();
    }
  }

  public size(): number {
    return this.items.length;
  }
}
