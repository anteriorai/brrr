import bencode from "bencode";
import { Buffer } from "node:buffer";
import type { Encoding } from "node:crypto";
import { TextDecoder, TextEncoder } from "node:util";

/**
 * Brrr uses UTF-8 for encoding
 */
export const encoding = "utf-8" as const satisfies Encoding

/**
 * Bencode encoding and decoding utility.
 */
export const bencoder = {
  encode(data: unknown): Uint8Array {
    return bencode.encode(data);
  },
  decode(data: Uint8Array, _encoding?: typeof encoding): unknown {
    const buffer = Buffer.from(data);
    return bencode.decode(buffer, _encoding);
  },
} as const;

/**
 * Exports TextEncoder and TextDecoder instances for UTF-8 encoding.
 */
export const encoder = new TextEncoder();
export const decoder = new TextDecoder(encoding);
