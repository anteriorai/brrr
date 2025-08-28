import type { Call } from "./call.ts";
import { type Cache, Memory, type Store } from "./store.ts";
import { SpawnLimitError } from "./errors.ts";
import { randomUUID } from "node:crypto";
import type { Publisher, Subscriber } from "./emitter.ts";
import { BrrrShutdownSymbol, BrrrTaskDoneEventSymbol } from "./symbol.ts";

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

export class Connection {
  public readonly cache: Cache;
  public readonly memory: Memory;
  public readonly emitter: Publisher;
  public readonly spawnLimit = 10_000;

  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    this.cache = cache;
    this.memory = new Memory(store);
    this.emitter = emitter;
  }

  public async putJob(topic: string, job: ScheduleMessage): Promise<void> {
    if ((await this.cache.incr(`brrr_count/${job.rootId}`)) > this.spawnLimit) {
      throw new SpawnLimitError(this.spawnLimit, job.rootId, job.callHash);
    }
    await this.emitter.emit(topic, `${rootId}/${callHash}`);
  }

  public async scheduleRaw(topic: string, call: Call): Promise<void> {
    if (await this.memory.hasValue(call.callHash)) {
      return;
    }
    await this.memory.setCall(call);
    const rootId = randomUUID().replaceAll("-", "");
    await this.putJob(topic, call.callHash, rootId);
  }

  public async readRaw(callHash: string): Promise<Uint8Array | undefined> {
    return this.memory.getValue(callHash);
  }
}

export class Server extends Connection {
  public constructor(store: Store, cache: Cache, emitter: Publisher) {
    super(store, cache, emitter);
  }

  public async loop(
    topic: string,
    handler: RequestHandler,
    getMessage: () => Promise<string | typeof BrrrShutdownSymbol | undefined>,
  ) {
    while (true) {
      const message = await getMessage();
      if (!message) {
        continue;
      }
      if (message === BrrrShutdownSymbol) {
        break;
      }
      const call = await this.handleMessage(handler, topic, message);
      if (call) {
        await this.emitter.emitEvent(BrrrTaskDoneEventSymbol, call);
      }
    }
  }

  protected async handleMessage(
    requestHandler: RequestHandler,
    topic: string,
    callId: string,
  ): Promise<Call | undefined> {
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
            continue;
          }
        }
      }
      if (spawnLimitError) {
        throw spawnLimitError;
      }
    });
    return call;
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
      `${topic}/${parentKey}`,
    );
    if (shouldSchedule) {
      await this.putJob(child.topic || topic, callHash, rootId);
    }
  }
}

export class SubscriberServer extends Server {
  public override readonly emitter: Publisher & Subscriber;

  public constructor(
    store: Store,
    cache: Cache,
    emitter: Publisher & Subscriber,
  ) {
    super(store, cache, emitter);
    this.emitter = emitter as Publisher & Subscriber;
  }

  public listen(topic: string, handler: RequestHandler) {
    this.emitter.on(topic, async (callId: string): Promise<void> => {
      const result = await this.handleMessage(handler, topic, callId);
      if (result) {
        await this.emitter.emitEvent(BrrrTaskDoneEventSymbol, result);
      }
    });
  }
}
