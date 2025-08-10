import { type BinaryToTextEncoding, createHash } from "node:crypto";
import { TextEncoder, TextDecoder } from "node:util";
import type { Call } from "./call.ts";
import type { Codec } from "./codec.ts";
import { stringify, parse } from "superjson";

export class NaiveCodec implements Codec {
  public static readonly algorithm = "sha256";
  public static readonly binaryToTextEncoding =
    "hex" satisfies BinaryToTextEncoding;

  private static readonly encoder = new TextEncoder();
  private static readonly decoder = new TextDecoder();

  private async hashCall<A extends unknown>(
    taskName: string,
    args: A,
  ): Promise<string> {
    const data = stringify([taskName, args]);
    return createHash("sha256")
      .update(data)
      .digest(NaiveCodec.binaryToTextEncoding);
  }

  public async decodeReturn(_: string, payload: Uint8Array): Promise<unknown> {
    const decoded = NaiveCodec.decoder.decode(payload);
    return parse(decoded);
  }

  public async encodeCall<A extends unknown[]>(
    taskName: string,
    args: A,
  ): Promise<Call> {
    const data = stringify(args);
    const payload = NaiveCodec.encoder.encode(data);
    const callHash = await this.hashCall(taskName, args);
    return { taskName, payload, callHash };
  }

  public async invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array> {
    const decoded = NaiveCodec.decoder.decode(call.payload);
    const args = parse<A>(decoded);
    const result = await task(...args);
    const resultJson = stringify(result);
    return NaiveCodec.encoder.encode(resultJson);
  }
}
