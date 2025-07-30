import { inspect } from "node:util";

export class Call {
  public readonly taskName: string;
  public readonly payload: Uint8Array;
  public readonly callHash: string;

  public constructor(taskName: string, payload: Uint8Array, callHash: string) {
    this.taskName = taskName;
    this.payload = payload;
    this.callHash = callHash;
  }

  public equals(other: unknown): boolean {
    return other instanceof Call && this.callHash === other.callHash;
  }

  public toString(): string {
    return `Call(${this.taskName}, ${this.callHash.slice(0, 6)}...)`;
  }

  public [inspect.custom](): string {
    return `<Call(${this.taskName}, ${this.callHash})>`;
  }
}
