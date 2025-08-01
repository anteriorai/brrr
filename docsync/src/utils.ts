const sentinelRegex = /<docsync>(.*?)<\/docsync>/;

export function parseSentinel(docstring: string) {
  return docstring.match(sentinelRegex)?.at(1);
}
