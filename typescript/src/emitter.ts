import type { Call } from "./call.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export interface Publisher {
  emit(topic: string, callId: string): Promise<void>;

  emitEventSymbol?(
    event: typeof BrrrTaskDoneEventSymbol,
    call: Call,
  ): Promise<void>;
}

export interface Subscriber {
  on(topic: string, listener: (callId: string) => void): void;

  onEventSymbol?(
    event: typeof BrrrTaskDoneEventSymbol,
    listener: (call: Call) => void,
  ): void;
}
