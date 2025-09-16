import { bencoder } from "./bencode.ts";
import type { Encoding } from "node:crypto";

type Tagged<T, A extends unknown[], Tag extends number> = {
  new(...args: A): T;
  readonly tag: Tag;
};

export abstract class TaggedTuple {
  public static fromTuple<
    T extends TaggedTuple,
    A extends unknown[],
    Tag extends number,
  >(this: Tagged<T, A, Tag>, tag: NoInfer<Tag>, ...tuple: A): T {
    if (tag !== this.tag) {
      throw new Error(`Tag mismatch: expected ${this.tag}, got ${tag}`);
    }
    return new this(...tuple);
  }

  public asTuple(): [number, ...unknown[]] {
    const { tag } = this.constructor as typeof TaggedTuple & { tag: number };
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
    // cheating here - but we have enough type and test coverage to be confident
    return (this as any).fromTuple(...decoded);
  }
}

export class PendingReturn extends TaggedTuple {
  public static readonly tag = 1;

  public readonly root_id: string;
  public readonly call_hash: string;
  public readonly topic: string;

  constructor(root_id: string, call_hash: string, topic: string) {
    super();
    this.root_id = root_id;
    this.call_hash = call_hash;
    this.topic = topic;
  }
}

export class ScheduleMessage extends TaggedTupleStrings {
  public static readonly tag = 2;

  public readonly root_id: string;
  public readonly call_hash: string;

  constructor(root_id: string, call_hash: string) {
    super();
    this.root_id = root_id;
    this.call_hash = call_hash;
  }
}
