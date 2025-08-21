import { type BinaryToTextEncoding, createHash } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";
import type { Call } from "./call.ts";
import type { Codec } from "./codec.ts";
import { parse, stringify } from "superjson";

export class JsonCodec implements Codec {
  public static readonly algorithm = "sha256";
  public static readonly binaryToTextEncoding =
    "hex" satisfies BinaryToTextEncoding;

  private static readonly encoder = new TextEncoder();
  private static readonly decoder = new TextDecoder();

  public async decodeReturn(_: string, payload: Uint8Array): Promise<unknown> {
    const decoded = JsonCodec.decoder.decode(payload);
    return parse(decoded);
  }

  public async encodeCall<A extends unknown[]>(
    taskName: string,
    args: A,
  ): Promise<Call> {
    const sortedArgs = args.map(JsonCodec.sortObjectKeys);
    const data = stringify(sortedArgs);
    const payload = JsonCodec.encoder.encode(data);
    const callHash = await this.hashCall(taskName, sortedArgs);
    return { taskName, payload, callHash };
  }

  public async invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array> {
    const decoded = JsonCodec.decoder.decode(call.payload);
    const args = parse<A>(decoded);
    const result = await task(...args);
    const resultJson = stringify(result);
    return JsonCodec.encoder.encode(resultJson);
  }

  private async hashCall<A extends unknown>(
    taskName: string,
    args: A,
  ): Promise<string> {
    const data = stringify([taskName, args]);
    return createHash("sha256")
      .update(data)
      .digest(JsonCodec.binaryToTextEncoding);
  }

  private static sortObjectKeys<T>(unordered: T): T {
    if (!unordered || typeof unordered !== "object") {
      return unordered;
    }
    if (Array.isArray(unordered)) {
      return unordered.map(JsonCodec.sortObjectKeys) as T;
    }
    const entries = Object.keys(unordered)
      .sort()
      .map((key) => [
        key,
        JsonCodec.sortObjectKeys(unordered[key as keyof typeof unordered]),
      ]);
    return Object.fromEntries(entries);
  }
}
