export interface Message {
  readonly body: string;
}

export abstract class Queue {
  protected readonly RECV_BLOCK_SECS = 20;

  public abstract putMessage(body: string): Promise<void>;

  public abstract getMessage(): Promise<Message>;

  public abstract close(): Promise<void>;
}
