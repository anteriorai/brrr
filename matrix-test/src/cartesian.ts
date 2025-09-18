export type Cartesian<T> = T extends readonly (infer U)[]
  ? U
  : T extends Record<string, unknown>
    ? { [K in keyof T]: Cartesian<T[K]> }
    : T;

function isRecord(x: unknown): x is Record<string, unknown> {
  return !!x && typeof x === "object" && !Array.isArray(x);
}

/**
 * "Cartesian product" of nested objects and arrays.
 *
 * @example
 * cartesian({ a: [1, 2], b: { c: [3, 4] } })
 * =>
 * [
 *   { a: 1, b: { c: 3 } },
 *   { a: 1, b: { c: 4 } },
 *   { a: 2, b: { c: 3 } },
 *   { a: 2, b: { c: 4 } },
 * ]
 */
export function cartesian<const M extends Record<string, unknown>>(
  matrix: M,
): Cartesian<M>[] {
  let acc: Record<string, unknown>[] = [{}];

  for (const [key, value] of Object.entries(matrix)) {
    const next: Record<string, unknown>[] = [];
    const options = Array.isArray(value)
      ? value
      : isRecord(value)
        ? cartesian(value)
        : [value];
    for (const partial of acc) {
      for (const option of options) {
        next.push({ ...partial, [key]: option });
      }
    }
    acc = next;
  }

  return acc as Cartesian<M>[];
}
