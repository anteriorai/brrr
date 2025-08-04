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

export class QueuePopTimeoutError extends BrrrError {
  public constructor(timeout: number) {
    super(`Queue pop timed out after ${timeout}ms`);
  }
}

export class BencodeError extends BrrrError {
  public constructor(data: unknown) {
    super(`Bencode error: ${JSON.stringify(data)}`);
  }
}

export class InvalidMessageError extends BrrrError {
  public constructor() {
    super("Queue is empty");
  }
}

export class QueueIsClosedError extends BrrrError {
  public constructor() {
    super("Queue is closed");
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

export class NotFoundError extends BrrrError {
  public constructor(key: MemKey) {
    super(`Not found: ${key.type}/${key.callHash}`);
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

export class SpawnLimitError extends BrrrError {
  public constructor(limit: number, rootId: string, callHash: string) {
    super(
      `Spawn limit of ${limit} reached for rootId ${rootId} and callHash ${callHash}`,
    );
  }
}

export class TaskNotFoundError extends BrrrError {
  public constructor(taskName: string) {
    super(`Task not found: ${taskName}`);
  }
}

export class QueueIsClosedError extends BrrrError {
  public constructor() {
    super("Queue is closed");
  }
}
