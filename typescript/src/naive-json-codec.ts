import { type BinaryToTextEncoding, createHash } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";
import type { Call } from "./call.ts";
import type { Codec } from "./codec.ts";
import { parse, stringify } from "superjson";

/**
 * Naive JSON codec that uses `superjson` for serialization and deserialization.
 *
 * @remarks
 * It tries its best to ensure that the serialized data is deterministic by
 * sorting object keys recursively before serialization, but it's not
 * reccommended for production use; the primary purpose of this codec is
 * executable documentation.
 */
export class NaiveJsonCodec implements Codec {
  public static readonly algorithm = "sha256";
  public static readonly binaryToTextEncoding =
    "hex" satisfies BinaryToTextEncoding;

  private static readonly encoder = new TextEncoder();
  private static readonly decoder = new TextDecoder();

  public async decodeReturn(_: string, payload: Uint8Array): Promise<unknown> {
    const decoded = NaiveJsonCodec.decoder.decode(payload);
    return parse(decoded);
  }

  public async encodeCall<A extends unknown[]>(
    taskName: string,
    args: A,
  ): Promise<Call> {
    const sortedArgs = args.map(NaiveJsonCodec.sortObjectKeys);
    const data = stringify(sortedArgs);
    const payload = NaiveJsonCodec.encoder.encode(data);
    const callHash = await this.hashCall(taskName, sortedArgs);
    return { taskName, payload, callHash };
  }

  public async invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array> {
    const decoded = NaiveJsonCodec.decoder.decode(call.payload);
    const args = parse<A>(decoded);
    const result = await task(...args);
    const resultJson = stringify(result);
    return NaiveJsonCodec.encoder.encode(resultJson);
  }

  private async hashCall<A extends unknown>(
    taskName: string,
    args: A,
  ): Promise<string> {
    const data = stringify([taskName, args]);
    return createHash("sha256")
      .update(data)
      .digest(NaiveJsonCodec.binaryToTextEncoding);
  }

  private static sortObjectKeys<T>(unordered: T): T {
    if (!unordered || typeof unordered !== "object") {
      return unordered;
    }
    if (Array.isArray(unordered)) {
      return unordered.map(NaiveJsonCodec.sortObjectKeys) as T;
    }
    const entries = Object.keys(unordered)
      .sort()
      .map((key) => [
        key,
        NaiveJsonCodec.sortObjectKeys(unordered[key as keyof typeof unordered]),
      ]);
    return Object.fromEntries(entries);
  }
}
