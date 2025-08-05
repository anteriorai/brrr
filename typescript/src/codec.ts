import type { Call } from "./call.ts";

export interface Codec {
  encodeCall(taskName: string, args: unknown[]): Promise<Call>;

  invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): Promise<unknown>;
}
