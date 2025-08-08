import {
  type Connection,
  Defer, type DeferredCall,
  type Request,
  type Response,
} from "./connection.ts";
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
  | ((...args: A) => R)
  | string;

function taskIdentifierToName<A extends unknown[], R>(
  spec: TaskIdentifier<A, R>,
): string {
  return typeof spec === "string" ? spec : spec.name;
}

export function taskify<A extends unknown[], R>(
  f: (...args: A) => R,
): Task<A, R> {
  return (_: ActiveWorker, ...args: A) => f(...args);
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
    const taskName = taskIdentifierToName(taskIdentifier);
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
      const taskName = taskIdentifierToName(taskIdentifier);
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
    try {
      const payload = await this.codec.invokeTask(
        request.call,
        async (...args) => {
          const handler = this.handlers[request.call.taskName];
          if (!handler) {
            throw new TaskNotFoundError(request.call.taskName);
          }
          const worker = new ActiveWorker(
            connection,
            this.codec,
            this.handlers,
          );
          return handler(worker, ...args);
        },
      );
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
    const taskName = taskIdentifierToName(taskIdentifier);
    return async (...args: StripLeadingActiveWorker<A>) => {
      const call = await this.codec.encodeCall(taskName, args);
      try {
        const payload = await this.connection.memory.getValue(call.callHash);
        return this.codec.decodeReturn(taskName, payload) as R;
      } catch (err) {
        if (err instanceof NotFoundError) {
          throw new Defer([
            {
              topic,
              call,
            },
          ]);
        }
        throw err;
      }
    };
  }

  public async gather(...promises: Promise<unknown>[]): Promise<unknown[]> {
    const defers: DeferredCall[] = []
    const values = []
    for (const promise of promises) {
      try {
        values.push(await promise)
      } catch (err) {
        if (!(err instanceof Defer)) {
          throw err
        }
        defers.push(...err.calls);
      }
    }
    if (defers.length) {
      throw new Defer(defers)
    }
    return values
  }
}
