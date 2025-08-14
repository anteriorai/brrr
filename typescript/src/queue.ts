import type { Result } from "./types.ts";

export interface Message {
  readonly body: string;
}

export type QueuePopResult<T> = Result<"QueueIsClosed" | "QueueIsEmpty", T>;

export interface Queue {
  /**
   * Push a message to the queue.
   */
  push(topic: string, message: Message): Promise<void>;

  /**
   * Pop a message from the queue.
   * Returns a result indicating whether the queue is closed, empty, or a message.
   */
  pop(topic: string): QueuePopResult<Promise<Message>>;
}
