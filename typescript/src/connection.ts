import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import {
  SpawnLimitError,
} from "./errors.ts";
import { randomUUID } from "node:crypto";

export interface DeferredCall {
  readonly topic: string | undefined;
  readonly call: Call;
}

export class Defer {
  public readonly calls: DeferredCall[];

  public constructor(...calls: DeferredCall[]) {
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

class Brrr {

}

/**
 * const brrr = new Brrr({ store, cache, queue }, {
 *   foo,
 *   bar: taskfn(bar)
 * });
 *
 * brrr.on('done', /* ... * /)
 *
 * await brrr.schedule(foo)(0, 1)
 *
 * sqs.on('message', async (message) => {
 *   brrr.handle(message)
 * })
 */


export class Connection {
  public readonly cache: Cache;
  public readonly memory: Memory;
  private readonly spawnLimit = 10_000;

  public constructor(queue: Queue, store: Store, cache: Cache) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.queue = queue;
  }

  public async putJob(topic: string, job: ScheduleMessage): Promise<void> {
    if ((await this.cache.incr(`brrr_count/${job.rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, job.rootId, job.callHash);
    }
    await this.queue.push(topic, `${rootId}/${callHash}`);
  }

  public async scheduleRaw(
    topic: string,
    callHash: string,
    taskName: string,
    payload: Uint8Array,
  ): Promise<void> {
    if (await this.memory.hasValue(callHash)) {
      return;
    }
    await this.memory.setCall({ taskName, payload, callHash });
    const rootId = randomUUID().replaceAll("-", "");
    await this.putJob(topic, callHash, rootId);
  }

  public async readRaw(callHash: string): Promise<Uint8Array | undefined> {
    return this.memory.getValue(callHash).catch(() => undefined);
  }
}

export class Server extends Connection {
  public constructor(queue: Queue, store: Store, cache: Cache) {
    super(queue, store, cache);
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
    await this.memory.addPendingReturns(
      callHash,
      `${topic}/${parentKey}`,
      () => {
        return this.putJob(childTopic, callHash, rootId);
      },
    );
  }

  private async handleMessage(
    requestHandler: RequestHandler,
    topic: string,
    callId: string,
  ): Promise<void> {
    const [rootId, callHash] = callId.split("/") as [string, string];
    const call = await this.memory.getCall(callHash);
    const handled = await requestHandler({ call }, this);
    if (handled instanceof Defer) {
      await Promise.all(
        handled.calls.map((child) => {
          return this.scheduleCallNested(topic, child, rootId, callId);
        }),
      );
      return;
    }
    await this.memory.setValue(message.callHash, handled.payload);
    let spawnLimitError: SpawnLimitError;
    await this.memory.withPendingReturnsRemove(callHash, async (returns) => {
      for (const pending of returns) {
        try {
          await this.scheduleReturnCall(pending);
        } catch (err) {
          if (err instanceof SpawnLimitError) {
            spawnLimitError = err;
          }
        }
      }
      if (spawnLimitError) {
        throw spawnLimitError;
      }
    });
  }
}
