import { getPythonDocsStrings } from "./python.ts";
import { getTypeScriptDocStrings } from "./typescript.ts";
import { deepStrictEqual } from "node:assert";
import { join } from "node:path";

const path = {
  python: join(import.meta.dirname, "../../python/src/**/*.py"),
  typescript: join(import.meta.dirname, "../../typescript/src/**/*.ts"),
} as const;

const python = await getPythonDocsStrings(path.python);
const ts = await getTypeScriptDocStrings(path.typescript);

deepStrictEqual(python, ts);

console.log("Docsync completed successfully!");
