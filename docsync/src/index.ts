#!/usr/bin/env node --enable-source-maps

import { getPythonDocsStrings } from "./python.ts";
import { getTypeScriptDocStrings } from "./typescript.ts";
import { deepStrictEqual } from "node:assert";
import { join } from "node:path";

const PYTHON_DIR = join(import.meta.dirname, "../../python");
const TYPESCRIPT_DIR = join(import.meta.dirname, "../../typescript");

async function main() {
  console.log("Comparing", PYTHON_DIR, "and", TYPESCRIPT_DIR);
  const path = {
    python: join(PYTHON_DIR, "src/**/*.py"),
    typescript: join(TYPESCRIPT_DIR, "src/**/*.ts"),
  } as const;

  const python = await getPythonDocsStrings(path.python);
  const ts = await getTypeScriptDocStrings(path.typescript);

  deepStrictEqual(python, ts);

  console.log("Docsync completed successfully");
}

main();
