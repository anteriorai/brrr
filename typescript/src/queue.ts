export interface Message {
  body: string;
}

export interface Queue {
  push(topic: string, message: Message): Promise<void>;

  pop(topic: string): Promise<string>;
}
