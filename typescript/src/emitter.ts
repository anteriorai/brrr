import type { Call } from "./call.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export interface Publisher {
  emit(event: typeof BrrrTaskDoneEventSymbol, call: Call): Promise<void>;

  emit(topic: string, callId: string): Promise<void>;
}

export interface Subscriber {
  on(
    event: typeof BrrrTaskDoneEventSymbol,
    listener: (call: Call) => void,
  ): this;

  on(topic: string, listener: (callId: string) => void): this;
}
