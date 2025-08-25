import type { QueuePopResult } from "../queue.ts";

interface Deferred<T> {
  resolve: (value: QueuePopResult<T>) => void;
  reject: (err: unknown) => void;
}

export class AsyncQueue<T> {
  private readonly items: T[] = [];
  private readonly generator: Generator<
    Promise<QueuePopResult<T>>,
    never,
    never
  >;
  private readonly deferred: Deferred<T>[] = [];

  private tasks = 0;
  private shutdownMode = false;

  private resolver: (() => void) | undefined;
  private sentinel = Promise.resolve();

  public constructor() {
    const { items, deferred } = this;
    this.generator = (function* () {
      while (true) {
        yield new Promise<QueuePopResult<T>>((resolve, reject) => {
          if (items.length) {
            resolve({
              kind: "Ok",
              value: items.shift() as T,
            });
          }
          deferred.push({ resolve, reject });
        });
      }
    })();
  }

  public async push(value: T): Promise<boolean> {
    if (this.shutdownMode) {
      return false;
    }
    this.tasks++;
    if (this.tasks === 1) {
      this.sentinel = new Promise((resolve) => {
        this.resolver = resolve;
      });
    }
    if (this.deferred.length) {
      this.deferred.shift()?.resolve?.({
        kind: "Ok",
        value,
      });
    }
    this.items.push(value);
    return true;
  }

  public async pop(timeout?: number): Promise<QueuePopResult<T>> {
    if (this.items.length) {
      this.done();
      return { kind: "Ok", value: this.items.shift() as T };
    }
    if (this.shutdownMode) {
      return { kind: "QueueIsClosed" };
    }
    const result = this.generator.next().value;
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
    if (!this.tasks) {
      throw new Error("done() called too many times");
    }
    this.tasks--;
    if (!this.tasks && this.resolver) {
      this.resolver();
    }
  }

  public join(): Promise<void> {
    return this.sentinel;
  }

  public shutdown(): void {
    while (this.deferred.length) {
      this.deferred.shift()?.resolve({
        kind: "QueueIsClosed",
      });
    }
    if (!this.tasks && this.resolver) {
      this.resolver();
    }
    this.shutdownMode = true;
  }

  public size(): number {
    return this.items.length;
  }
}
