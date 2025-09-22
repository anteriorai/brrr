import { suite, type SuiteContext } from "node:test";
import { type Combination, combinations } from "./matrix.ts";

export async function matrixSuite<const M extends Record<string, any>>(
  name: string,
  matrix: M,
  fn: (_: SuiteContext, combinations: Combination<M>) => void | Promise<void>,
  childSuiteNameFn?: (combination: Combination<M>) => string,
): Promise<void> {
  await suite(name, async (_: SuiteContext): Promise<void> => {
    for (const combination of combinations(matrix)) {
      await suite(
        childSuiteNameFn?.(combination) || name,
        async (context: SuiteContext): Promise<void> => {
          await fn(context, combination);
        },
      );
    }
  });
}

export function makeMatrixSuite<const M extends Record<string, any>>(
  matrix: M,
  childSuiteNameFn?: (combination: Combination<M>) => string,
): (
  name: string,
  fn: (_: SuiteContext, combination: Combination<M>) => void | Promise<void>,
) => Promise<void> {
  return async (
    name: string,
    fn: (_: SuiteContext, combination: Combination<M>) => void | Promise<void>,
  ): Promise<void> => {
    await matrixSuite(name, matrix, fn, childSuiteNameFn);
  };
}

export type { Combination, Combinations, Matrix } from "./matrix.ts";
