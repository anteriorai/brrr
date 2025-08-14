export interface Call {
  readonly taskName: string;
  readonly payload: Uint8Array;
  readonly callHash: string;
}
