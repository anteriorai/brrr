import { Server } from "./connection.ts";
import { InMemoryQueue } from "./backends/in-memory.ts";
import { AppWorker } from "./app.ts";

export class LocalApp {
  private readonly server: Server;
  private readonly appWorker: AppWorker;
  private readonly queue: InMemoryQueue;
  private readonly topic: string;
  private hasRun = false;

  public constructor(
    topic: string,
    server: Server,
    queue: InMemoryQueue,
    appWorker: AppWorker,
  ) {
    this.topic = topic;
    this.server = server;
    this.appWorker = appWorker;
    this.queue = queue;
  }
}
