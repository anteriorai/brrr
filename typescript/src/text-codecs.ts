import bencode from "bencode";
import { Buffer } from "node:buffer";
import type { Encoding } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";

/**
 * Bencode encoding and decoding utility.
 */
export const bencoder = {
  encode(data: unknown): Uint8Array {
    return bencode.encode(data);
  },
  decode(data: Uint8Array, encoding?: Encoding): unknown {
    const buffer = Buffer.from(data);
    return bencode.decode(buffer, encoding);
  },
} as const;

/**
 * Exports TextEncoder and TextDecoder instances for UTF-8 encoding.
 */
export const encoder = new TextEncoder();
export const decoder = new TextDecoder("utf-8" satisfies Encoding);
