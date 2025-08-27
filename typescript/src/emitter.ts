import type { Call } from "./call.ts";

export interface Emitter {
  on(event: "done", listener: (call: Call) => void): this;

  on(event: string, listener: (callId: string) => void): this;

  emit(event: "done", call: Call): boolean;

  emit(event: string, callId: string): boolean;
}
