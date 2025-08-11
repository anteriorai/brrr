import type { ActiveWorker } from "brrr";
import { createServer } from 'node:http';

const topic = {
  main: "brrr-demo-main",
  side: "brrr-demo-side",
} as const;

const config = {
  redisUrl: process.env["BRRR_DEMO_REDIS_URL"],
  dynamoTableName: process.env["DYNAMODB_TABLE_NAME"] || "brrr",
  bind: {
    addr: process.env["BRRR_DEMO_LISTEN_HOST"] || "127.0.0.1",
    port: Number.parseInt(process.env["BRRR_DEMO_LISTEN_PORT"] || "8080"),
  },
} as const

async function fib(
  app: ActiveWorker,
  n: number,
  salt?: string,
): Promise<number> {
  if (n < 2) {
    return n;
  }
  const [a, b] = await app.gather(
    app.call(fib)(n - 2, salt),
    app.call(fib)(n - 1, salt),
  );
  return a + b;
}

async function fibAndPrint(app: ActiveWorker, n: string, salt?: string) {
  const f = await app.call(fib, topic.side)(Number.parseInt(n), salt);
  console.log(`fib(${n}) = ${f}`);
}

function hello(greetee: string): string {
  const greeting = `Hello, ${greetee}!`;
  console.log(greeting);
  return greeting;
}
