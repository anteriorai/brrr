export interface Queue {
  readonly timeout: number;

  push(topic: string, message: string): Promise<void>;

  pop(topic: string): Promise<string>;
}
