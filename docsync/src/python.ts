import Parser, { type SyntaxNode } from "tree-sitter";
import Python from "tree-sitter-python";
import { glob, readFile } from "node:fs/promises";
import { parseSentinel } from "./utils.ts";

const parser = new Parser();
parser.setLanguage(Python as any);

function extractDocString(node: SyntaxNode): string | undefined {
  const body = node.namedChildren.find(
    ({ type }) => type === "block" || type === "suite",
  );
  const stmt = body?.namedChildren?.at(0);
  if (
    stmt?.type === "expression_statement" &&
    stmt.firstNamedChild?.type === "string"
  ) {
    return stmt.firstNamedChild.text;
  }
}

function fetchDocStrings(rootNode: SyntaxNode): string[] {
  const docstrings: string[] = [];

  function go(node: SyntaxNode) {
    if (
      node.type === "module" ||
      node.type === "class_definition" ||
      node.type === "function_definition"
    ) {
      const doc = extractDocString(node);
      if (doc) {
        docstrings.push(doc);
      }
    }
    node.namedChildren.map(go);
  }

  go(rootNode);
  return docstrings;
}

export async function getPythonDocsStrings(path: string) {
  const files = await Array.fromAsync(glob(path));
  const docstringMap = new Map<string, string>();
  for (const file of files) {
    const content = await readFile(file, "utf-8");
    const tree = parser.parse(content);
    const docstrings = fetchDocStrings(tree.rootNode);
    for (const docstring of docstrings) {
      const sentinel = parseSentinel(docstring);
      if (!sentinel) {
        continue;
      }
      const cleaned = docstring
        .replace(/<docsync>.*?<\/docsync>/g, "") // remove <docsync> tags
        .replace(/^[ \t]*"""/, "") // remove leading triple quotes
        .replace(/"""[ \t]*$/, "") // remove trailing triple quotes
        .replace(/\s*\n\s*/g, " ") // replace newlines with spaces
        .replace(/\s+/g, " ") // normalize whitespace
        .trim();
      docstringMap.set(sentinel, cleaned);
    }
  }

  return docstringMap;
}
