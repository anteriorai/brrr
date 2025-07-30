export abstract class Queue {
  public abstract put(message: string): Promise<void>;

  public abstract get(): Promise<string>;

  public async close(): Promise<void> {}
}
