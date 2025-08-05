export interface Queue {
  push(topic: string, message: string): Promise<void>;

  pop(topic: string): Promise<string>;
}
