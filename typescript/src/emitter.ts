import type { Call } from "./call.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export interface Emitter {
  on(event: typeof BrrrTaskDoneEventSymbol, listener: (call: Call) => void): this;

  on(event: string, listener: (callId: string) => void): this;

  emit(event: typeof BrrrTaskDoneEventSymbol, call: Call): Promise<void>;

  emit(event: string, callId: string): Promise<void>;
}
