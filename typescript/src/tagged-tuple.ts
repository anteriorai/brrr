import { bencoder, decoder, encoder } from "./text-codecs.ts";
import type { Encoding } from "node:crypto";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

const encoding: Encoding = "utf-8" as const;

interface TaggedTuple<T> {
  new(...args: any[]): T

  readonly tag: number
}

function fromTuple<T>(clz: TaggedTuple<T>, data: unknown[]): InstanceType<typeof clz> {
  if (data[0] !== clz.tag) {
    throw new TagMismatchError(clz.tag);
  }
  if (data.length - 1 !== clz.length) {
    throw new MalformedTaggedTupleError(clz.name, clz.length);
  }
  return new clz(...data.slice(1))
}

function asTuple<T extends object>(obj: InstanceType<TaggedTuple<T>>): unknown[] {
  return [(obj.constructor as TaggedTuple<T>).tag, ...Object.values(obj)]
}

function encode<T extends object>(obj: InstanceType<TaggedTuple<T>>): Uint8Array {
  const tuple = asTuple(obj);
  return bencoder.encode(tuple);
}

function encodeToString<T extends object>(obj: InstanceType<TaggedTuple<T>>): string {
  return decoder.decode(encode(obj));
}

function decode<T>(clz: TaggedTuple<T>, data: Uint8Array): InstanceType<typeof clz> {
  const decoded = bencoder.decode(data, encoding) as [number, ...unknown[]];
  return fromTuple(clz, decoded);
}

function decodeFromString<T>(clz: TaggedTuple<T>, data: string): InstanceType<typeof clz> {
  return decode(clz, encoder.encode(data));
}


export const taggedTuple = {
  fromTuple,
  asTuple,
  encode,
  encodeToString,
  decode,
  decodeFromString,
} as const

export class PendingReturn {
  public static readonly tag = 1;

  public readonly rootId: string;
  public readonly callHash: string;
  public readonly topic: string;

  constructor(rootId: string, callHash: string, topic: string) {
    this.rootId = rootId;
    this.callHash = callHash;
    this.topic = topic;
  }

  public isRepeatedCall(other: PendingReturn): boolean {
    return (
      this.rootId !== other.rootId &&
      this.callHash === other.callHash &&
      this.topic === other.topic
    );
  }
}

export class ScheduleMessage {
  public static readonly tag = 2;

  public readonly rootId: string;
  public readonly callHash: string;

  constructor(rootId: string, callHash: string) {
    this.rootId = rootId;
    this.callHash = callHash;
  }
}
