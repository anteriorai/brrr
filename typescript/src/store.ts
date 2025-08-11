import type { Call } from "./call.ts";
import { bencoder } from "./bencode.ts";
import { TextDecoder } from "node:util";
import type { Encoding } from "node:crypto";

export interface PendingReturnsPayload {
  readonly scheduled_at: number;
  readonly returns: Buffer[];
}

export class PendingReturns {
  public static readonly EMPTY_SCHEDULED_AT = -1;
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

  public static decode(encoded: Uint8Array): PendingReturns {
    const { scheduled_at, returns } = bencoder.decode(
      encoded,
      PendingReturns.encoding,
    ) as PendingReturnsPayload;
    return new PendingReturns(
      scheduled_at === PendingReturns.EMPTY_SCHEDULED_AT
        ? undefined
        : scheduled_at,
      new Set(returns.map((it) => it.toString(PendingReturns.encoding))),
    );
  }

  public encode(): Uint8Array {
    return bencoder.encode({
      scheduled_at: this.scheduledAt,
      returns: [...this.returns]
        .map((it) => Buffer.from(it, PendingReturns.encoding))
        .sort(Buffer.compare),
    } satisfies PendingReturnsPayload);
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
  private static readonly casRetryLimit = 100;
  private static decoder = new TextDecoder("ascii");
  private readonly store: Store;

  public constructor(store: Store) {
    this.store = store;
  }

  public async getCall(callHash: string): Promise<Call> {
    const encoded = await this.store.get({
      type: "call",
      callHash,
    });
    const { task_name, payload } = bencoder.decode(encoded) as {
      task_name: Uint8Array;
      payload: Uint8Array;
    };
    return {
      taskName: Memory.decoder.decode(task_name),
      payload,
      callHash,
    };
  }

  public async setCall(call: Call): Promise<void> {
    const encoded = bencoder.encode({
      task_name: call.taskName,
      payload: call.payload,
    });
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
}
