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
      throw new QueueIsClosedError();
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

  public async pop(): Promise<T> {
    if (this.items.length > 0) {
      return this.items.shift() as T;
    }
    if (this.shutdownMode) {
      throw new QueueIsClosedError();
    }
    return this.generator.next().value;
  }

  public popSync(): T {
    if (this.items.length > 0) {
      return this.items.shift() as T;
    }
    if (this.shutdownMode) {
      throw new QueueIsClosedError();
    }
    throw new QueueIsEmptyError();
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

  public async join(): Promise<void> {
    return this.sentinel;
  }

  public shutdown(): void {
    this.shutdownMode = true;
    while (this.deferred.length > 0) {
      this.deferred.shift()?.reject(new QueueIsClosedError());
    }
    if (this.tasks === 0 && this.resolver) {
      this.resolver();
    }
  }

  public size(): number {
    return this.items.length;
  }
}
