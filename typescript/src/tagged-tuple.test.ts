import { suite, test } from "node:test";
import { deepStrictEqual, ok } from "node:assert/strict";
import { throws } from "node:assert";
import { MalformedTaggedTupleError, TagMismatchError } from "./errors.ts";
import { TaggedTuple } from "./tagged-tuple.ts";

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
      const foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      ok(foo instanceof Foo);
      ok(foo.bar === 42);
      ok(foo.baz === "hello");
    });
    await test("tag mismatch", async () => {
      throws(
        () => TaggedTuple.fromTuple(Foo, [1, 42, "hello"]),
        TagMismatchError,
      );
    });
    await test("too many args", async () => {
      throws(
        // @ts-expect-error testing runtime error
        () => TaggedTuple.fromTuple(Foo, [0, 42, "hello", "a"]),
        MalformedTaggedTupleError,
      );
    });
    await test("too few args", async () => {
      throws(
        // @ts-expect-error testing runtime error
        () => TaggedTuple.fromTuple(Foo, [0, 42]),
        MalformedTaggedTupleError,
      );
    });
  });

  await suite("asTuple", async () => {
    await test("basic", async () => {
      const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
      deepStrictEqual(TaggedTuple.asTuple(foo), [0, 42, "hello"]);
    });
  });

  await suite("encode and decode", async () => {
    const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
    const encoded: Uint8Array = TaggedTuple.encode(foo);
    const decoded = TaggedTuple.decode(Foo, encoded);
    deepStrictEqual(decoded, foo);
  });

  await suite("encode and decode (string)", async () => {
    const foo: Foo = TaggedTuple.fromTuple(Foo, [0, 42, "hello"]);
    const encoded: string = TaggedTuple.encodeToString(foo);
    const decoded = TaggedTuple.decodeFromString(Foo, encoded);
    deepStrictEqual(decoded, foo);
  });
});
