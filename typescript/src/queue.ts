export interface Queue {
  put(topic: string, message: string): Promise<void>;

  get(topic: string): Promise<string>;
}
