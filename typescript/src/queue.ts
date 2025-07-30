export abstract class Queue {
  public abstract put(topic: string, message: string): Promise<void>;

  public abstract get(topic: string): Promise<string>;

  public async close(): Promise<void> {}
}
