import type { Server } from "./connection.ts";
import type { InMemoryQueue } from "./backends/in-memory.ts";

export class LocalApp {
  public constructor(
    topic: string,
    server: Server,
    queue: InMemoryQueue,
    app: AppWo,
  ) {}
}
