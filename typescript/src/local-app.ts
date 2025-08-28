import { Server } from "./connection.ts";
import {
  AppWorker,
  type Handlers,
  type NoAppTask,
  type StripLeadingActiveWorker,
  type TaskIdentifier,
  taskIdentifierToName,
} from "./app.ts";
import type { Codec } from "./codec.ts";
import { InMemoryCache, InMemoryEmitter, InMemoryStore } from "./backends/in-memory.ts";
import { NotFoundError } from "./errors.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export class LocalApp {
  public readonly topic: string;
  public readonly server: Server;
  public readonly app: AppWorker;

  private hasRun = false;

  public constructor(topic: string, server: Server, app: AppWorker) {
    this.topic = topic;
    this.server = server;
    this.app = app;
  }

  public schedule<A extends unknown[], R>(
    handler: Parameters<typeof this.app.schedule<A, R>>[0],
  ): NoAppTask<A, void> {
    return this.app.schedule(handler, this.topic);
  }

  public read<A extends unknown[], R>(
    ...args: Parameters<typeof this.app.read<A, R>>
  ): NoAppTask<A, R> {
    return this.app.read(...args);
  }

  public run(): void {
    if (this.hasRun) {
      throw new Error("LocalApp has already been run");
    }
    this.hasRun = true;
    this.server.listen(this.topic, this.app.handle);
  }
}

export class LocalBrrr {
  private readonly topic: string;
  private readonly handlers: Handlers;
  private readonly codec: Codec;

  public constructor(topic: string, handlers: Handlers, codec: Codec) {
    this.topic = topic;
    this.handlers = handlers;
    this.codec = codec;
  }

  public run<A extends unknown[], R>(taskIdentifier: TaskIdentifier<A, R>) {
    const store = new InMemoryStore();
    const cache = new InMemoryCache();
    const emitter = new InMemoryEmitter();
    const server = new Server(store, cache, emitter);
    const worker = new AppWorker(this.codec, server, this.handlers);
    const localApp = new LocalApp(this.topic, server, worker);
    const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
    return async (...args: StripLeadingActiveWorker<A>): Promise<R> => {
      localApp.run();
      await localApp.schedule(taskName)(...args);
      const call = await this.codec.encodeCall(taskName, args);
      return new Promise((resolve) => {
        localApp.app.on(BrrrTaskDoneEventSymbol, async ({ callHash }) => {
          if (callHash === call.callHash) {
            const payload = await server.readRaw(callHash);
            if (!payload) {
              throw new NotFoundError({
                type: "value",
                callHash,
              });
            }
            const result = this.codec.decodeReturn(taskName, payload) as R;
            resolve(result);
          }
        });
      });
    };
  }
}
