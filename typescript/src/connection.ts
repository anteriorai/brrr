import { Call } from "./call.ts";
import type { Queue } from "./queue.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import {
  QueueIsClosedError,
  QueueIsEmptyError,
  SpawnLimitError,
} from "./errors.ts";
import { randomUUID } from "node:crypto";

export interface DeferredCall {
  readonly topic: string | undefined;
  readonly call: Call;
}

export class Defer {
  public readonly calls: DeferredCall[];

  public constructor(calls: DeferredCall[]) {
    this.calls = calls;
  }
}

export interface Request {
  readonly call: Call;
}

export interface Response {
  readonly payload: Uint8Array;
}

type RequestHandler = (
  request: Request,
  connection: Connection,
) => Promise<Response | Defer>;

function parseCallId(callId: string): [string, string] {
  return callId.split("/") as [string, string];
}

export async function connect(
  queue: Queue,
  store: Store,
  cache: Cache,
): Promise<Server & AsyncDisposable> {
  return new Server(queue, store, cache);
}

export class Connection implements AsyncDisposable {
  private readonly spawnLimit = 1000;

  public readonly cache: Cache;
  public readonly memory: Memory;
  public readonly queue: Queue;

  public constructor(queue: Queue, store: Store, cache: Cache) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.queue = queue;
  }

  public async putJob(
    topic: string,
    callHash: string,
    rootId: string,
  ): Promise<void> {
    if ((await this.cache.incr(`brrr_count/${rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, rootId, callHash);
    }
    await this.queue.push(topic, `${rootId}/${callHash}`);
  }

  public async scheduleRaw(
    topic: string,
    idempotencyKey: string,
    taskName: string,
    payload: Uint8Array,
  ): Promise<void> {
    if (await this.memory.hasValue(idempotencyKey)) {
      return;
    }
    const call = new Call(taskName, payload, idempotencyKey);
    await this.memory.setCall(call);
    const rootId = randomUUID().replaceAll("-", "");
    await this.putJob(topic, idempotencyKey, rootId);
  }

  public async readRaw(callHash: string): Promise<Uint8Array | undefined> {
    return this.memory.getValue(callHash).catch(() => undefined);
  }

  public async [Symbol.asyncDispose](): Promise<void> {}
}

export class Server extends Connection {
  private static totalWorkers = 0;

  private readonly n: number;

  public constructor(queue: Queue, store: Store, cache: Cache) {
    super(queue, store, cache);
    this.n = Server.totalWorkers;
    Server.totalWorkers++;
  }

  private async scheduleReturnCall(addr: string): Promise<void> {
    const [topic, rootId, parentKey] = addr.split("/") as [
      string,
      string,
      string,
    ];
    await this.putJob(topic, parentKey, rootId);
  }

  private async scheduleCallNested(
    topic: string,
    child: DeferredCall,
    rootId: string,
    parentKey: string,
  ): Promise<void> {
    await this.memory.setCall(child.call);
    const childTopic = child.topic || topic;
    const callHash = child.call.callHash;
    await this.putJob(childTopic, callHash, rootId);
    await this.memory.addPendingReturns(callHash, `${topic}/${parentKey}`, () =>
      this.putJob(childTopic, callHash, rootId),
    );
  }

  private async handleMessage(
    requestHandler: RequestHandler,
    topic: string,
    callId: string,
  ): Promise<void> {
    const [rootId, callHash] = parseCallId(callId);
    const call = await this.memory.getCall(callHash);
    const handled = await requestHandler({ call }, this);
    if (handled instanceof Defer) {
      await Promise.all(
        handled.calls.map((child) =>
          this.scheduleCallNested(topic, child, rootId, callId),
        ),
      );
      return;
    }
    await this.memory.setValue(callHash, handled.payload);
    let spawnLimitError: SpawnLimitError;
    await this.memory.withPendingReturnsRemove(callHash, async (returns) => {
      for (const pending of returns) {
        try {
          await this.scheduleReturnCall(pending);
        } catch (err) {
          if (err instanceof SpawnLimitError) {
            spawnLimitError = err;
          }
          throw err;
        }
      }
      if (spawnLimitError) {
        throw spawnLimitError;
      }
    });
  }

  public async loop(
    topic: string,
    requestHandler: RequestHandler,
  ): Promise<void> {
    while (true) {
      try {
        const message = await this.queue.pop(topic);
        await this.handleMessage(requestHandler, topic, message);
      } catch (err) {
        if (err instanceof QueueIsEmptyError) {
          continue;
        }
        if (err instanceof QueueIsClosedError) {
          return;
        }
        throw err;
      }
    }
  }
}
