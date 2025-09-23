import { createMatrixSuite } from "matrix-test";

const matrices = {
  topic: ["test-topic", "//':\"~`\\", "🇺🇸"],
} as const;

export const matrixSuite = createMatrixSuite(matrices, (combination) => {
  return `Combination: ${JSON.stringify(combination)}`;
});
