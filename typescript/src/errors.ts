import type { MemKey } from "./store.ts";

abstract class BrrrError extends Error {
  protected constructor(message: string) {
    super();
    this.name = this.constructor.name;
    this.message = message;
  }
}

export class QueueIsEmptyError extends BrrrError {
  public constructor() {
    super("Queue is empty");
  }
}

export class QueueIsClosedError extends BrrrError {
  public constructor() {
    super("Queue is closed");
  }
}

export class QueuePopTimeoutError extends BrrrError {
  public constructor(timeout: number) {
    super(`Queue pop timed out after ${timeout}s`);
  }
}

export class BencodeError extends BrrrError {
  public constructor(data: unknown) {
    super(`Bencode error: ${JSON.stringify(data)}`);
  }
}

export class InvalidMessageError extends BrrrError {
  public constructor(value: unknown) {
    super(`Invalid message: ${JSON.stringify(value)}`);
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

export class CasRetryLimitReachedError extends BrrrError {
  public constructor(retryLimit: number) {
    super(`CAS retry limit reached (${retryLimit})`);
  }
}
