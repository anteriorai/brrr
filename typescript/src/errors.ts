import type { MemKey } from "./store.ts";

abstract class BrrrError extends Error {
  protected constructor(message: string) {
    super();
    this.name = this.constructor.name;
    this.message = message;
  }
}

export class QueueIsClosedError extends BrrrError {
  public constructor() {
    super("Queue is already closed / closing");
  }
}

export class QueueIsEmptyError extends BrrrError {
  public constructor() {
    super("Queue is empty");
  }
}

export class UnknownTopicError extends BrrrError {
  public constructor(topic: string) {
    super(`Unknown topic: ${topic}`);
  }
}

export class NotFoundError extends BrrrError {
  public constructor(key: MemKey) {
    super(`Not found: ${JSON.stringify(key)}`);
  }
}

export class CompareMismatchError extends BrrrError {
  public constructor(key: MemKey) {
    super(`Compare mismatch for key ${JSON.stringify(key)}`);
  }
}
