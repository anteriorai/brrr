import { makeMatrixSuite } from "matrix-test";

const matrices = {
  topic: ["test-topic", "//':\"~`\\", "🇺🇸"],
} as const;

export const matrixSuite = makeMatrixSuite(matrices);
