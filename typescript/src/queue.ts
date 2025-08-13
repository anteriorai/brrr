export interface Message {
  readonly body: string;
}

export type QueuePopResult =
  | {
      kind: "QueueIsClosed";
    }
  | {
      kind: "QueueIsEmpty";
    }
  | {
      kind: "Ok";
      message: Message;
    };

export interface Queue {
  /**
   * Push a message to the queue.
   */
  push(topic: string, message: Message): Promise<void>;

  /**
   * Pop a message from the queue.
   * Returns a result indicating whether the queue is closed, empty, or contains a message.
   */
  pop(topic: string): Promise<QueuePopResult>;
}
