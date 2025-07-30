import bencode from "bencode";
import { Buffer } from "node:buffer";
import type { Encoding } from "node:crypto";

/**
 * Bencode encoding and decoding utility.
 */
export const bencoder = {
  encode(data: unknown) {
    return bencode.encode(data);
  },
  decode(data: Uint8Array, encoding: Encoding): unknown {
    const buffer = Buffer.from(data);
    return bencode.decode(buffer, encoding);
  },
} as const;
