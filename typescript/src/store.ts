import type { Encoding } from "node:crypto";
import { Call, type CallInfo } from "./call.ts";
import { bencoder } from "./bencode.ts";
import { Buffer } from "node:buffer";

export interface PendingReturnsPayload {
  readonly scheduled_at: number | undefined;
  readonly returns: Buffer[];
}

export class PendingReturns {
  private static readonly encoding = "ascii" satisfies Encoding;

  public readonly scheduledAt: number | undefined;
  public readonly returns: Set<string>;

  public constructor(scheduledAt: number | undefined, returns: Set<string>) {
    this.scheduledAt = scheduledAt;
    this.returns = returns;
  }

  public encode(): Uint8Array {
    return bencoder.encode({
      scheduled_at: this.scheduledAt,
      returns: [...this.returns]
        .map((it) => Buffer.from(it, PendingReturns.encoding))
        .sort(Buffer.compare),
    } satisfies PendingReturnsPayload);
  }

  public static decode(encoded: Uint8Array): PendingReturns {
    const { scheduled_at, returns } = bencoder.decode(
      encoded,
      PendingReturns.encoding,
    ) as PendingReturnsPayload;
    return new PendingReturns(
      scheduled_at,
      new Set(returns.map((it) => it.toString(PendingReturns.encoding))),
    );
  }
}

export interface MemKey {
  readonly type: "pending_returns" | "call" | "value";
  readonly callHash: string;
}

export interface Store {
  has(key: MemKey): Promise<boolean>;

  get(key: MemKey): Promise<Uint8Array>;

  set(key: MemKey, value: Uint8Array): Promise<void>;

  delete(key: MemKey): Promise<void>;

  setNewValue(key: MemKey, value: Uint8Array): Promise<void>;

  compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<void>;

  compareAndDelete(key: MemKey, expected: Uint8Array): Promise<void>;
}

export interface Cache {
  incr(key: string): Promise<number>;
}

export class Memory {
  private static readonly encoding = "ascii" satisfies Encoding;
  private readonly store: Store;

  public constructor(store: Store) {
    this.store = store;
  }

  public async getCall(callHash: string): Promise<Call> {
    const encoded = await this.store.get({
      type: "call",
      callHash,
    });
    const { task_name, payload } = bencoder.decode(
      encoded,
      Memory.encoding,
    ) as CallInfo;
    return new Call(task_name, payload, callHash);
  }

  public async setCall(call: Call): Promise<void> {
    const encoded = bencoder.encode({
      task_name: call.taskName,
      payload: call.payload,
    } satisfies CallInfo);
    await this.store.set(
      {
        type: "call",
        callHash: call.callHash,
      },
      encoded,
    );
  }

  public async hasValue(callHash: string): Promise<boolean> {
    return this.store.has({
      type: "value",
      callHash,
    });
  }

  public async getValue(callHash: string): Promise<Uint8Array> {
    return this.store.get({
      type: "value",
      callHash,
    });
  }

  public async setValue(callHash: string, payload: Uint8Array): Promise<void> {
    await this.store.set(
      {
        type: "value",
        callHash,
      },
      payload,
    );
  }

  public async addPendingReturns(
    callHash: string,
    newReturn: string,
  ): Promise<boolean> {
    const memKey: MemKey = {
      type: "pending_returns",
      callHash,
    };
    let shouldSchedule = false;
    await this.withCas(async () => {
      let existingEncoded = await this.store.get(memKey);
      let existing: PendingReturns;
      if (existingEncoded) {
        existing = PendingReturns.decode(existingEncoded);
      } else {
        existing = new PendingReturns(Math.floor(Date.now() / 1000), new Set());
        existingEncoded = existing.encode();
        if (!(await this.store.setNewValue(memKey, existingEncoded))) {
          return false;
        }
        shouldSchedule = true;
      }
      shouldSchedule ||= existing.returns
        .values()
        .some((it) => this.isRepeatedCall(it, newReturn));
      const newReturns = new PendingReturns(
        existing.scheduledAt,
        existing.returns.union(new Set([newReturn])),
      );
      return this.store.compareAndSet(
        memKey,
        newReturns.encode(),
        existingEncoded,
      );
    });
    return shouldSchedule;
  }

  public async withPendingReturnsRemove(
    callHash: string,
    f: (returns: ReadonlySet<string>) => Promise<void>,
  ) {
    const memKey: MemKey = {
      type: "pending_returns",
      callHash,
    };
    const handled = new Set<string>();
    return this.withCas(async () => {
      const pendingEncoded = await this.store.get(memKey);
      if (!pendingEncoded) {
        return true;
      }
      const toHandle =
        PendingReturns.decode(pendingEncoded).returns.difference(handled);
      await f(toHandle);
      for (const it of toHandle) {
        handled.add(it);
      }
      return this.store.compareAndDelete(memKey, pendingEncoded);
    });
  }

  private async withCas(f: () => Promise<boolean>): Promise<void> {
    for (let i = 0; i < Memory.casRetryLimit; i++) {
      if (await f()) {
        return;
      }
    }
    throw new CasRetryLimitReachedError(Memory.casRetryLimit);
  }

  // TODO: migrate to bencode
  private isRepeatedCall(newReturn: string, existingReturn: string): boolean {
    const [newRoot, newParent, newTopic, ...newRest] = newReturn.split("/");
    if (!newRoot || !newParent || !newTopic || newRest.length) {
      throw new Error(`Invalid return address: ${newReturn}`);
    }
    const [existingRoot, existingParent, existingTopic, ...existingRest] =
      existingReturn.split("/");
    if (
      !existingRoot ||
      !existingParent ||
      !existingTopic ||
      existingRest.length
    ) {
      throw new Error(`Invalid return address: ${existingReturn}`);
    }
    return (
      newRoot !== existingRoot &&
      newParent === existingParent &&
      newTopic === existingTopic
    );
  }
}
