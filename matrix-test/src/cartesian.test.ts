import { suite, test } from "node:test";
import { cartesian } from "./cartesian.ts";
import { deepStrictEqual } from "node:assert";

function deepStrictEqualWithTypes<const T>(actual: T, expected: T): void {
  deepStrictEqual(actual, expected);
}

await suite(import.meta.filename, async () => {
  await test("basic object", async () => {
    const object = {
      a: [1, 2],
      b: ["x", "y"],
    };
    deepStrictEqualWithTypes(cartesian(object), [
      { a: 1, b: "x" },
      { a: 1, b: "y" },
      { a: 2, b: "x" },
      { a: 2, b: "y" },
    ]);
  });

  await test("nested object", async () => {
    const object = {
      a: [1, 2],
      b: { c: ["x", "y"] },
    };
    deepStrictEqualWithTypes(cartesian(object), [
      { a: 1, b: { c: "x" } },
      { a: 1, b: { c: "y" } },
      { a: 2, b: { c: "x" } },
      { a: 2, b: { c: "y" } },
    ]);
  });

  await test("mixed object", async () => {
    const object = {
      a: [1, 2],
      b: { c: ["x", "y"] },
      d: true,
    };
    deepStrictEqualWithTypes(cartesian(object), [
      { a: 1, b: { c: "x" }, d: true },
      { a: 1, b: { c: "y" }, d: true },
      { a: 2, b: { c: "x" }, d: true },
      { a: 2, b: { c: "y" }, d: true },
    ]);
  });

  await test("with functions", async () => {
    const fn1 = () => 1;
    const fn2 = () => 2;
    const object = {
      a: [fn1, fn2],
      b: { c: ["x", "y"] },
      d: true,
    };
    deepStrictEqualWithTypes(cartesian(object), [
      { a: fn1, b: { c: "x" }, d: true },
      { a: fn1, b: { c: "y" }, d: true },
      { a: fn2, b: { c: "x" }, d: true },
      { a: fn2, b: { c: "y" }, d: true },
    ]);
  });

  await test("empty object", async () => {
    const object = {};
    deepStrictEqualWithTypes(cartesian(object), [{}]);
  });
});
