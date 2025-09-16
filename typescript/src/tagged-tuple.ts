import { bencoder } from "./text-codecs.ts";
import type { Encoding } from "node:crypto";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

type Tagged<T, A extends unknown[], Tag extends number> = {
  new(...args: A): T;
  readonly tag: Tag;
};

export abstract class TaggedTuple {
  public static fromTuple<
    T extends TaggedTuple,
    A extends unknown[],
    Tag extends number,
  >(this: Tagged<T, A, Tag>, ...data: unknown[]): T {
    const [tag, ...tuple] = data as [Tag, ...A];
    if (tag !== this.tag) {
      throw new TagMismatchError(this.tag, tag);
    }
    if (tuple.length !== this.length) {
      throw new MalformedTaggedTupleError(this.name, this.length, tuple.length);
    }
    return new this(...tuple);
  }

  public asTuple(): [number, ...unknown[]] {
    const { tag } = this.constructor as Tagged<this, unknown[], number>;
    return [tag, ...Object.values(this)];
  }
}

export abstract class TaggedTupleStrings extends TaggedTuple {
  private static readonly encoding: Encoding = "utf-8";

  public encode(): Uint8Array {
    const tuple = this.asTuple();
    return bencoder.encode(tuple);
  }

  public static decode<
    T extends TaggedTuple,
    A extends unknown[],
    Tag extends number,
  >(this: Tagged<T, A, Tag>, data: Uint8Array): T {
    const decoded = bencoder.decode(data, TaggedTupleStrings.encoding) as [
      Tag,
      ...A,
    ];
    return super.fromTuple(...decoded);
  }
}

export class PendingReturn extends TaggedTuple {
  public static readonly tag = 1;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly topic: string;

  constructor(rootId: string, callHash: string, topic: string) {
    super();
    this.rootId = rootId;
    this.callHash = callHash;
    this.topic = topic;
  }
}

export class ScheduleMessage extends TaggedTupleStrings {
  public static readonly tag = 2;

  public readonly rootId: string;
  public readonly callHash: string;

  constructor(rotId: string, callHash: string) {
    super();
    this.rootId = rotId;
    this.callHash = callHash;
  }
}
