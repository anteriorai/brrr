/**
 * GitHub-style matrix type
 */
export type Matrix = Record<string, readonly any[]>;

export type Combination<M extends Matrix> = { [K in keyof M]: M[K][number] };

export type Combinations<M extends Matrix> = Combination<M>[];

/**
 * Return all combinations for a matrix
 *
 * @example
 * const matrix = {
 *   a: [1, 2],
 *   b: ["x", "y"],
 * } as const;
 *
 * combinations(matrix);
 *
 * // result:
 * [
 *   { a: 1, b: "x" },
 *   { a: 1, b: "y" },
 *   { a: 2, b: "x" },
 *   { a: 2, b: "y" },
 * ]
 */
export function combinations<const M extends Matrix>(
  matrix: M,
): Combinations<M> {
  let combinations = [{} as Combination<M>];

  for (const key of Object.keys(matrix)) {
    const next: Combinations<M> = [];
    for (const base of combinations) {
      for (const value of matrix[key] || []) {
        next.push({ ...base, [key]: value });
      }
    }
    combinations = next;
  }
  return combinations;
}
