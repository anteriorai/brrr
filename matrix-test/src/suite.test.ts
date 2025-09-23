import { suite, test } from "node:test";
import { createMatrixSuite, matrixSuite } from "./suite.ts";
import { deepStrictEqual } from "node:assert";
import { combinations } from "./matrix.ts";

await suite(import.meta.filename, async () => {
  const matrix = {
    os: ["linux", "windows"],
    arch: ["x64", "arm64"],
  };

  const combinationsExpected = combinations(matrix);

  const suiteNamesExpected = combinationsExpected.map(
    ({ os, arch }) => `os=${os},arch=${arch}`,
  );

  await test(matrixSuite.name, async () => {
    const combinations: unknown[] = [];
    await matrixSuite("matrix suite", matrix, async (_, combination) => {
      combinations.push(combination);
    });
    deepStrictEqual(combinations, combinationsExpected);
  });

  await test(createMatrixSuite.name, async () => {
    const combinations: unknown[] = [];
    const names: string[] = [];

    const suite = createMatrixSuite(matrix, (combination) => {
      return `os=${combination.os},arch=${combination.arch}`;
    });
    await suite("matrix suite", async (context, combination) => {
      combinations.push(combination);
      names.push(context.name);
    });
    deepStrictEqual(combinations, combinationsExpected);
    deepStrictEqual(names, suiteNamesExpected);
  });
});
