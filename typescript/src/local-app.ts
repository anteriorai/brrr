import { Server } from "./connection.ts";
import { InMemoryQueue } from "./backends/in-memory.ts";
import { AppWorker, type Handlers } from "./app.ts";
import type { Codec } from "./codec.ts";
import type { Queue } from "./queue.ts";

export class LocalApp {
  private readonly topic: string;
  private readonly server: Server;
  private readonly queue: InMemoryQueue;
  private readonly app: AppWorker;

  private hasRun = false;

  public constructor(
    topic: string,
    server: Server,
    queue: InMemoryQueue,
    app: AppWorker,
  ) {
    this.topic = topic;
    this.server = server;
    this.queue = queue;
    this.app = app;
    this.hasRun = true;
  }

  public schedule(handler: Parameters<typeof this.app.schedule>[0]) {
    return this.app.schedule(handler, this.topic);
  }

  public read(...args: Parameters<typeof this.app.read>) {
    return this.app.read(...args);
  }

  public async run(): Promise<void> {
    if (this.hasRun) {
      throw new Error("LocalApp has already been run");
    }
    this.hasRun = true;
    this.queue.flush();
    await this.server.loop(this.topic, this.app.handle);
  }
}
