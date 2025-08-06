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
  readonly scheduled_at: number | undefined;
  readonly returns: Buffer[];
}

export class PendingReturns {
  private static readonly encoding = "ascii" satisfies Encoding;

  public readonly scheduledAt: number | undefined;
  public readonly returns: ReadonlySet<string>;

  public constructor(
    scheduledAt: number | undefined,
    returns: ReadonlySet<string>,
  ) {
    this.scheduledAt = scheduledAt;
    this.encodedReturns = new Set([...returns].map(TaggedTuple.encodeToString));
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
  /**
   * Check if the store has a value for the given key.
   */
  has(key: MemKey): Promise<boolean>;

  /**
   * Get the value for the given key.
   */
  get(key: MemKey): Promise<Uint8Array | undefined>;

  /**
   * Set the value for the given key.
   */
  set(key: MemKey, value: Uint8Array): Promise<void>;

  /**
   * Delete the value for the given key.
   */
  delete(key: MemKey): Promise<boolean>;

  /**
   * Set a new value for the given key.
   * Returns true if the value was set, false if the key already exists.
   */
  setNewValue(key: MemKey, value: Uint8Array): Promise<boolean>;

  /**
   * Compare and set a value for the given key.
   * Returns true if the value was set, false if the expected value did not match.
   */
  compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<boolean>;

  /**
   * Compare and delete a value for the given key.
   * Returns true if the value was deleted, false if the expected value did not match.
   */
  compareAndDelete(key: MemKey, expected: Uint8Array): Promise<boolean>;
}

export interface Cache {
  /**
   * Increment the value for the given key.
   */
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

  public async addPendingReturns(
    callHash: string,
    newReturn: string,
    scheduleJob: () => Promise<void>,
  ): Promise<boolean> {
    return this.withCas(async () => {
      const memKey: MemKey = {
        type: "pending_returns",
        callHash,
      };
      let shouldStoreAgain = false;
      let existingEncoded: Uint8Array | undefined;
      let existing: PendingReturns;
      try {
        existingEncoded = await this.store.get(memKey);
        existing = PendingReturns.decode(existingEncoded);
        if (!existing.returns.has(newReturn)) {
          existing = new PendingReturns(
            existing.scheduledAt,
            new Set([...existing.returns, newReturn]),
          );
          shouldStoreAgain = true;
        }
      } catch (err) {
        if (!(err instanceof NotFoundError)) {
          throw err;
        }
        existing = new PendingReturns(undefined, new Set([newReturn]));
        const initialEncoded = existing.encode();
        await this.store.setNewValue(memKey, initialEncoded);
        existingEncoded = initialEncoded;
      }
      const alreadyPending = !!existing.scheduledAt;
      if (!alreadyPending) {
        await scheduleJob();
        existing = new PendingReturns(
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
      let pendingEncoded: Uint8Array;
      try {
        pendingEncoded = await this.store.get(memKey);
      } catch (err) {
        if (err instanceof NotFoundError) {
          return f(new Set());
        }
        throw err;
      }
      const toHandle =
        PendingReturns.decode(pendingEncoded).returns.difference(handled);
      await f(toHandle);
      toHandle.forEach((it) => handled.add(it));
      await this.store.compareAndDelete(memKey, pendingEncoded);
    });
  }
}
