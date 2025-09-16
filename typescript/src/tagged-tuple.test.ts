import { suite, test } from "node:test";
import { TaggedTuple, TaggedTupleStrings } from "./tagged-tuple.ts";
import { deepStrictEqual, ok } from "node:assert/strict";
import { throws } from "node:assert";

await suite(import.meta.filename, async () => {
  await suite(TaggedTuple.name, async () => {
    class Foo extends TaggedTuple {
      public static readonly tag = 0;

      public readonly bar: number;
      public readonly baz: string;

      public constructor(foo: number, bar: string) {
        super();
        this.bar = foo;
        this.baz = bar;
      }
    }

    await suite("fromTuple", async () => {
      await test("type test", async () => {
        // correct types
        const _: Foo = Foo.fromTuple(0, 42, "hello")
        // @ts-expect-error wrong tag
        throws(() => Foo.fromTuple(1, 42, "hello"))
        // @ts-expect-error wrong args
        Foo.fromTuple(0, "wrong", "args")
      })
      await test("create instance", async () => {
        const foo = Foo.fromTuple(0, 42, "hello");
        ok(foo instanceof Foo)
        ok(foo.bar === 42)
        ok(foo.baz === "hello")
      })
    })

    await suite("asTuple", async () => {
      await test("basic", async () => {
        const foo = Foo.fromTuple(0, 42, "hello")
        deepStrictEqual(foo.asTuple(), [0, 42, "hello"])
      })
    })
  })

  await suite(TaggedTupleStrings.name, async () => {
    class Foo extends TaggedTupleStrings {
      public static readonly tag = 0;

      public readonly bar: number
      public readonly baz: string

      constructor(bar: number, baz: string) {
        super();
        this.bar = bar;
        this.baz = baz;
      }
    }

    await suite("encode and decode", async () => {
      const foo: Foo = Foo.fromTuple(0, 42, "hello")
      const encoded = foo.encode()
      const decoded: Foo = Foo.decode(encoded)
      deepStrictEqual(decoded, foo)
    })
  })
})
