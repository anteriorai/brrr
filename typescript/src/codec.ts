import type { Call } from "./call.ts";

export interface Codec {
  encodeCall(taskName: string, args: unknown[]): Promise<Call>;

  invokeTask(
    call: Call,
    task: (...args: unknown[]) => Promise<unknown>,
  ): Promise<Uint8Array>;

  decodeReturn(taskName: string, payload: Uint8Array): Promise<unknown>;
}
