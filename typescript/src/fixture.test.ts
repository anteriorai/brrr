import { makeMatrixSuite } from "matrix-test";

const matrices = {
  topic: ["test-topic", "//':\"~`\\", "🇺🇸"],
} as const;

export const matrixSuite = makeMatrixSuite(matrices, (combination) => {
  return `Combination: ${JSON.stringify(combination)}`;
});
