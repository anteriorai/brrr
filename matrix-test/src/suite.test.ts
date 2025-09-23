import { suite, test } from "node:test";
import { createMatrixSuite, matrixSuite } from "./suite.ts";
import { deepStrictEqual } from "node:assert";
import { type Combination, combinations } from "./matrix.ts";

await suite(import.meta.filename, async () => {
  const matrix = {
    os: ["linux", "windows"],
    arch: ["x64", "arm64"],
  };

  function makeTestName(combination: Combination<typeof matrix>): string {
    return `os=${combination.os},arch=${combination.arch}`;
  }

  const combinationsExpected = combinations(matrix);

  const suiteNamesExpected = combinationsExpected.map(makeTestName);

  await test(matrixSuite.name, async () => {
    const combinations: unknown[] = [];
    await matrixSuite("sample matrix suite", matrix, async (_, combination) => {
      combinations.push(combination);
    });
    deepStrictEqual(combinations, combinationsExpected);
  });

  await test(createMatrixSuite.name, async () => {
    const combinations: unknown[] = [];
    const names: string[] = [];

    const matrixSuite = createMatrixSuite(matrix, makeTestName);

    await matrixSuite("sample matrix suite", async (context, combination) => {
      combinations.push(combination);
      names.push(context.name);
    });

    deepStrictEqual(combinations, combinationsExpected);
    deepStrictEqual(names, suiteNamesExpected);
  });
});
