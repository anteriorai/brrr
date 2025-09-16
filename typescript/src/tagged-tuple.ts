import { bencoder, decoder, encoder, encoding } from "./internal-codecs.ts";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";

export interface Tagged<T = any, A extends unknown[] = any[]> {
  new(...args: A): T;

  readonly tag: number;
}

function fromTuple<T, A extends unknown[]>(
  clz: Tagged<T, A>,
  data: [number, ...A],
): InstanceType<typeof clz> {
  if (data[0] !== clz.tag) {
    throw new TagMismatchError(clz);
  }
  if (data.length - 1 !== clz.length) {
    throw new MalformedTaggedTupleError(clz);
  }
  return new clz(...(data.slice(1) as A));
}

function asTuple<T extends object, A extends unknown[]>(
  obj: InstanceType<Tagged<T, A>>,
): [number, ...A] {
  return [(obj.constructor as Tagged<T, A>).tag, ...Object.values(obj)] as [
    number,
    ...A,
  ];
}

function encode(obj: InstanceType<Tagged>): Uint8Array {
  const tuple = asTuple(obj);
  return bencoder.encode(tuple);
}

function encodeToString(obj: InstanceType<Tagged>): string {
  return decoder.decode(encode(obj));
}

function decode<T, A extends unknown[]>(
  clz: Tagged<T, A>,
  data: Uint8Array,
): InstanceType<typeof clz> {
  const decoded = bencoder.decode(data, encoding) as [number, ...A];
  return fromTuple(clz, decoded);
}

function decodeFromString<T, A extends unknown[]>(
  clz: Tagged<T, A>,
  data: string,
): InstanceType<typeof clz> {
  return decode(clz, encoder.encode(data));
}

export const TaggedTuple = {
  fromTuple,
  asTuple,
  encode,
  encodeToString,
  decode,
  decodeFromString,
} as const;

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
