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
import { InMemoryCache, InMemoryStore } from "./backends/in-memory.ts";

export class LocalApp {
  private readonly topic: string;
  private readonly server: Server;
  private readonly app: AppWorker;

  private hasRun = false;

  public constructor(
    topic: string,
    server: Server,
    app: AppWorker,
  ) {
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

  public async run(): Promise<void> {
    if (this.hasRun) {
      throw new Error("LocalApp has already been run");
    }
    this.hasRun = true;
    this.app.listen(this.topic)
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
    const server = new Server(store, cache);
    const worker = new AppWorker(this.codec, server, this.handlers);
    const app = new LocalApp(this.topic, server, worker);
    const taskName = taskIdentifierToName(taskIdentifier, this.handlers);
    return async (...args: StripLeadingActiveWorker<A>): Promise<R> => {
      await app.schedule(taskName)(...args);
      await app.run();
      return (await app.read(taskName)(...args)) as R;
    };
  }
}
