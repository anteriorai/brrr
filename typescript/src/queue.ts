import type { Result } from "./types.ts";

export interface Message {
  readonly body: string;
}

export type QueuePopResult<T> = Result<"QueueIsClosed" | "QueueIsEmpty", T>;

export interface Queue {
  /**
   * Push a message to the queue.
   * Returns true if the message was successfully pushed, false if the queue is closed.
   */
  push(topic: string, message: Message): Promise<boolean>;

  /**
   * Pop a message from the queue.
   * Returns a result indicating whether the queue is closed, empty, or contains a message.
   */
  pop(topic: string): Promise<QueuePopResult<Message>>;
}
