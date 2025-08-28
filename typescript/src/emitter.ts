import type { Call } from "./call.ts";
import { BrrrTaskDoneEventSymbol } from "./symbol.ts";

export abstract class Publisher {
  public abstract emit(topic: string, callId: string): Promise<void>;

  public async emitEvent(
    event: typeof BrrrTaskDoneEventSymbol,
    call: Call,
  ): Promise<void> {}
}

export abstract class Subscriber {
  abstract on(topic: string, listener: (callId: string) => void): this;

  public onEvent(
    event: typeof BrrrTaskDoneEventSymbol,
    listener: (call: Call) => void,
  ): this {
    return this;
  }
}
