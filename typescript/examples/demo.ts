import { ActiveWorker } from "../src/app.ts";
import { Brrr } from "../src/brrr.ts";

const fibAndPrint = Brrr.handler(
  "fib_and_print",
  async (app: ActiveWorker, n: string, salt?: string) => {
    const f = await app.call("fib", {
      topic: "brrr-demo-side",
    })(parseInt(n), salt);
    console.log(`fib(${n}) = ${f}`);
    return 0;
  },
);

const hello = Brrr.handlerNoArg("hello", (greetee: string) => {
  console.log(`Hello, ${greetee}!`);
  return `Hello, ${greetee}!`;
});

const fib = Brrr.handler(
  "fib",
  async (app: ActiveWorker, n: number, salt?: string) => {
    if (n < 2) {
      return n;
    }
    const results = (await app.gather(
      app.call(fib)(n - 2, salt),
      app.call(fib)(n - 1, salt),
    )) as number[];
    return results.reduce((acc, val) => acc + val, 0);
  },
);
