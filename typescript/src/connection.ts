import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import { SpawnLimitError, } from "./errors.ts";
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";

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

export class Brrr {
  private readonly emitter = new EventEmitter()

  public on(event: string, callback: (...args: any[]) => void) {
    this.emitter.on(event, callback);
  }
}

/**
 * const brrr = new Server({ store, cache }, {
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

  public constructor(store: Store, cache: Cache) {
    this.cache = cache;
    this.memory = new Memory(store);
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
  public constructor(store: Store, cache: Cache) {
    super(store, cache);
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
    const callHash = child.call.callHash;
    const shouldSchedule = await this.memory.addPendingReturns(
      callHash,
      `${topic}/${parentKey}`
    );
    if (shouldSchedule) {
      await this.putJob(child.topic || topic, callHash, rootId);
    }
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
}
