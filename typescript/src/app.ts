import { type Connection, Defer, type DeferredCall, type Request, type Response, } from "./connection.ts";
import type { Codec } from "./codec.ts";
import { NotFoundError, TaskNotFoundError } from "./errors.ts";

export type Task<A extends unknown[] = any[], R = any> = (
  ...args: [ActiveWorker, ...A]
) => R;

export type StripLeadingActiveWorker<A extends unknown[]> = A extends [
    ActiveWorker,
    ...infer Rest,
  ]
  ? Rest
  : A;

export type NoAppTask<A extends unknown[], R> = (
  ...args: StripLeadingActiveWorker<A>
) => Promise<R>;

export type Handlers = Readonly<Record<string, Task>>;

export type TaskIdentifier<A extends unknown[], R> =
  | ((...args: A) => R | Promise<R>)
  | string;

const taskIdentifierSymbol = Symbol("brrr.taskIdentifier");

export function taskIdentifierToName<A extends unknown[], R>(
  identifier: TaskIdentifier<A, R>,
  handlers: Handlers,
): string {
  if (typeof identifier === "string") {
    return identifier;
  }
  for (const [name, handler] of Object.entries(handlers)) {
    if (
      taskIdentifierSymbol in handler &&
      handler[taskIdentifierSymbol] === identifier
    ) {
      return name;
    }
    if (handler === (identifier as unknown as Task)) {
      return name;
    }
  }
  throw new TaskNotFoundError(`Task not found: ${identifier.name}`);
}

export function taskFn<A extends unknown[], R>(
  fn: (...args: A) => R,
): Task<A, R> {
  const task: Task<A, R> = (_: ActiveWorker, ...args: A): R => fn(...args);
  return Object.defineProperty(task, taskIdentifierSymbol, {
    value: fn,
    writable: false,
    configurable: false,
  });
}

export class AppConsumer {
  protected readonly codec: Codec;
  protected readonly connection: Connection;
  protected readonly handlers: Handlers;

  public constructor(
    codec: Codec,
    connection: Connection,
    handlers: Handlers = {},
  ) {
    this.codec = codec;
    this.connection = connection;
    this.handlers = handlers;
  }

  public schedule<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<A, R>,
    topic: string,
  ): NoAppTask<A, void> {
    const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
    return async (...args: StripLeadingActiveWorker<A>) => {
      const call = await this.codec.encodeCall(taskName, args);
      await this.connection.scheduleRaw(
        topic,
        call.callHash,
        taskName,
        call.payload,
      );
    };
  }

  public read<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<A, R>,
  ): NoAppTask<A, R> {
    return async (...args: StripLeadingActiveWorker<A>) => {
      const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
      const call = await this.codec.encodeCall(taskName, args);
      const payload = await this.connection.memory.getValue(call.callHash);
      return this.codec.decodeReturn(taskName, payload) as R;
    };
  }
}

export class AppWorker extends AppConsumer {
  public readonly handle = async (
    request: Request,
    connection: Connection,
  ): Promise<Response | Defer> => {
    const handler = this.handlers[request.call.taskName];
    if (!handler) {
      throw new TaskNotFoundError(request.call.taskName);
    }
    try {
      const activeWorker = new ActiveWorker(
        connection,
        this.codec,
        this.handlers,
      );
      const payload = await this.codec.invokeTask(request.call, (...args) => {
        return handler(activeWorker, ...args);
      });
      return { payload };
    } catch (err) {
      if (err instanceof Defer) {
        return err;
      }
      throw err;
    }
  };
}

export class ActiveWorker {
  private readonly connection: Connection;
  private readonly codec: Codec;
  private readonly handlers: Handlers;

  public constructor(connection: Connection, codec: Codec, handlers: Handlers) {
    this.connection = connection;
    this.codec = codec;
    this.handlers = handlers;
  }

  public call<A extends unknown[], R>(
    taskIdentifier: TaskIdentifier<A, R>,
    topic?: string | undefined,
  ): NoAppTask<A, R> {
    const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
    return async (...args: StripLeadingActiveWorker<A>): Promise<R> => {
      const call = await this.codec.encodeCall(taskName, args);
      try {
        const payload = await this.connection.memory.getValue(call.callHash);
        return this.codec.decodeReturn(taskName, payload) as R;
      } catch (err) {
        if (err instanceof NotFoundError) {
          throw new Defer({ topic, call });
        }
        throw err;
      }
    };
  }

  public async gather<T1>(t1: T1): Promise<[Awaited<T1>]>;
  public async gather<T1, T2>(
    t1: T1,
    t2: T2,
  ): Promise<[Awaited<T1>, Awaited<T2>]>;
  public async gather<T1, T2, T3>(
    t1: T1,
    t2: T2,
    t3: T3,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>]>;
  public async gather<T1, T2, T3, T4>(
    t1: T1,
    t2: T2,
    t3: T3,
    t4: T4,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>, Awaited<T4>]>;
  public async gather<T1, T2, T3, T4, T5>(
    t1: T1,
    t2: T2,
    t3: T3,
    t4: T4,
    t5: T5,
  ): Promise<[Awaited<T1>, Awaited<T2>, Awaited<T3>, Awaited<T4>, Awaited<T5>]>;
  public async gather<T>(...promises: Promise<T>[]): Promise<Awaited<T>[]>;
  public async gather<T>(...promises: Promise<T>[]): Promise<Awaited<T>[]> {
    const deferredCalls: DeferredCall[] = [];
    const values: Awaited<T>[] = [];
    for (const promise of promises) {
      try {
        values.push(await promise);
      } catch (err) {
        if (!(err instanceof Defer)) {
          throw err;
        }
        deferredCalls.push(...err.calls);
      }
    }
    if (deferredCalls.length) {
      throw new Defer(...deferredCalls);
    }
    return values;
  }
}
