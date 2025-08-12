import bencode from "bencode";
import { Buffer } from "node:buffer";
import type { Encoding } from "node:crypto";
import { BencodeError } from "./errors.ts";

/**
 * Bencode encoding and decoding utility.
 */
export const bencoder = {
  encode(data: unknown): Uint8Array {
    try {
      return bencode.encode(data);
    } catch (e: unknown) {
      throw new BencodeError(e, data);
    }
  },
  decode(data: Uint8Array, encoding?: Encoding): unknown {
    try {
      const buffer = Buffer.from(data);
      return bencode.decode(buffer, encoding);
    } catch (e: unknown) {
      throw new BencodeError(e, data);
    }
  },
} as const;
