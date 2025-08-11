import type { Call } from "./call.ts";

export interface Codec {
  encodeCall<A extends unknown[]>(taskName: string, args: A): Promise<Call>;

  invokeTask<A extends unknown[], R>(
    call: Call,
    task: (...args: A) => Promise<R>,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): unknown;
}
