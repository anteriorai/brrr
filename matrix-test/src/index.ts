import { suite, type SuiteContext } from "node:test";
import { type Cartesian, cartesian } from "./cartesian.ts";

export async function matrixSuite<const M extends Record<string, any>>(
  name: string,
  matrix: M,
  fn: (_: SuiteContext, combinations: Cartesian<M>) => void | Promise<void>,
): Promise<void> {
  await suite(name, async (_: SuiteContext): Promise<void> => {
    const combinations = cartesian(matrix);
    for (const combination of combinations) {
      await suite(name, async (context: SuiteContext): Promise<void> => {
        await fn(context, combination);
      });
    }
  });
}

export function makeMatrixSuite<const M extends Record<string, any>>(
  matrix: M,
): (
  name: string,
  fn: (_: SuiteContext, combinations: Cartesian<M>) => void | Promise<void>,
) => Promise<void> {
  return async (
    name: string,
    fn: (_: SuiteContext, combinations: Cartesian<M>) => void | Promise<void>,
  ): Promise<void> => {
    await matrixSuite(name, matrix, fn);
  };
}
