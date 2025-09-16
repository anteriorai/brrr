import { suite, test } from "node:test";
import { deepStrictEqual, ok } from "node:assert/strict";
import { throws } from "node:assert";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";
import { taggedTuple } from "./tagged-tuple.ts";

await suite(import.meta.filename, async () => {
  class Foo {
    public static readonly tag = 0;

    public readonly bar: number;
    public readonly baz: string;

    public constructor(foo: number, bar: string) {
      this.bar = foo;
      this.baz = bar;
    }
  }

  await suite("fromTuple", async () => {
    await test("create instance", async () => {
      const foo = taggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      ok(foo instanceof Foo);
      ok(foo.bar === 42);
      ok(foo.baz === "hello");
    });
    await test("tag mismatch", async () => {
      throws(
        () => taggedTuple.fromTuple(Foo, [1, 42, "hello"]),
        TagMismatchError,
      );
    });
    await test("too many args", async () => {
      throws(
        // @ts-expect-error testing runtime error
        () => taggedTuple.fromTuple(Foo, [0, 42, "hello", "a"]),
        MalformedTaggedTupleError,
      );
    });
    await test("too few args", async () => {
      throws(
        // @ts-expect-error testing runtime error
        () => taggedTuple.fromTuple(Foo, [0, 42]),
        MalformedTaggedTupleError,
      );
    });
  });

  await suite("asTuple", async () => {
    await test("basic", async () => {
      const foo: Foo = taggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      deepStrictEqual(taggedTuple.asTuple(foo), [0, 42, "hello"]);
    });
  });

  await suite("encode and decode", async () => {
    const foo: Foo = taggedTuple.fromTuple(Foo, [0, 42, "hello"]);
    const encoded: Uint8Array = taggedTuple.encode(foo);
    const decoded = taggedTuple.decode(Foo, encoded);
    deepStrictEqual(decoded, foo);
  });
});
