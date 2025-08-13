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
  push(topic: string, message: Message): Promise<void>;

  pop(topic: string): Promise<QueuePopResult>;
}
