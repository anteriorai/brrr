import type { Encoding } from "node:crypto";
import { Call, type CallInfo } from "./call.ts";
import { bencoder } from "./bencode.ts";
import { Buffer } from "node:buffer";
import {
  CasRetryLimitReachedError,
  CompareMismatchError,
  NotFoundError,
} from "./errors.ts";

export interface PendingReturnsPayload {
  readonly scheduled_at: number;
  readonly returns: Buffer[];
}

export class PendingReturn {
  private static readonly encoding = "ascii" satisfies Encoding;

  public readonly scheduledAt: number | undefined;
  public readonly returns: ReadonlySet<string>;

  public constructor(
    scheduledAt: number | undefined,
    returns: ReadonlySet<string>,
  ) {
    this.scheduledAt = scheduledAt;
    this.returns = returns;
  }

  public encode(): Uint8Array {
    return bencoder.encode({
      scheduled_at: this.scheduledAt,
      returns: [...this.returns]
        .map((it) => Buffer.from(it, PendingReturn.encoding))
        .sort(Buffer.compare),
    } satisfies PendingReturnsPayload);
  }

  public static decode(encoded: Uint8Array): PendingReturn {
    const { scheduled_at, returns } = bencoder.decode(
      encoded,
      PendingReturn.encoding,
    ) as PendingReturnsPayload;
    return new PendingReturn(
      scheduled_at,
      new Set(returns.map((it) => it.toString(PendingReturn.encoding))),
    );
  }
}

export interface MemKey {
  readonly type: "pending_return" | "call" | "value";
  readonly callHash: string;
}

export interface Store {
  has(key: MemKey): Promise<boolean>;

  /**
   * Get the value for the given key.
   */
  get(key: MemKey): Promise<Uint8Array | undefined>;

  set(key: MemKey, value: Uint8Array): Promise<void>;

  /**
   * Delete the value for the given key.
   */
  delete(key: MemKey): Promise<boolean>;

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
  private static readonly casRetryLimit = 100;
  private readonly store: Store;

  public constructor(store: Store) {
    this.store = store;
  }

  public async getCall(callHash: string): Promise<Call> {
    const memKey: MemKey = {
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

  public async getValue(callHash: string): Promise<Uint8Array | undefined> {
    const value = this.store.get({
      type: "value",
      callHash,
    });
    if (!value) {}
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

  private async withCas<T>(f: () => Promise<T>): Promise<T> {
    for (let i = 0; i < Memory.casRetryLimit; i++) {
      try {
        return f();
      } catch (e) {
        if (e instanceof CompareMismatchError) {
          continue;
        }
        throw e;
      }
    }
    throw new CasRetryLimitReachedError(Memory.casRetryLimit);
  }

  public async addPendingReturn(
    callHash: string,
    newReturn: string,
    scheduleJob: () => Promise<void>,
  ): Promise<boolean> {
    return this.withCas(async () => {
      const memKey: MemKey = {
        type: "pending_return",
        callHash,
      };
      let shouldStoreAgain = false;
      let existingEncoded: Uint8Array | undefined;
      let existing: PendingReturn;
      try {
        existingEncoded = await this.store.get(memKey);
        existing = PendingReturn.decode(existingEncoded);
        if (!existing.returns.has(newReturn)) {
          existing = new PendingReturn(
            existing.scheduledAt,
            new Set([...existing.returns, newReturn]),
          );
          shouldStoreAgain = true;
        }
      } catch (err) {
        if (!(err instanceof NotFoundError)) {
          throw err;
        }
        existing = new PendingReturn(undefined, new Set([newReturn]));
        const initialEncoded = existing.encode();
        await this.store.setNewValue(memKey, initialEncoded);
        existingEncoded = initialEncoded;
      }
      const alreadyPending = !!existing.scheduledAt;
      if (!alreadyPending) {
        await scheduleJob();
        existing = new PendingReturn(
          Math.floor(Date.now() / 1000),
          existing.returns,
        );
        shouldStoreAgain = true;
      }
      if (shouldStoreAgain) {
        const updatedEncoded = existing.encode();
        await this.store.compareAndSet(memKey, updatedEncoded, existingEncoded);
      }
      return alreadyPending;
    });
  }

  public async withPendingReturnRemove(
    callHash: string,
    f: (returns: Iterable<string>) => Promise<void>,
  ) {
    const memKey: MemKey = {
      type: "pending_return",
      callHash,
    };
    const handled = new Set<string>();
    return this.withCas(async () => {
      let pendingEncoded: Uint8Array;
      try {
        pendingEncoded = await this.store.get(memKey);
      } catch (err) {
        if (err instanceof NotFoundError) {
          return f([]);
        }
        throw err;
      }
      const toHandle =
        PendingReturn.decode(pendingEncoded).returns.difference(handled);
      await f(toHandle);
      toHandle.forEach(handled.add);
      await this.store.compareAndDelete(memKey, pendingEncoded);
    });
  }
}
