import { ActiveWorker, CallableTask, CallableTaskNoArg } from "../src/app.ts";

class Brrr {
  static handler<A extends unknown[], R>(
    name: string,
    fn: (app: ActiveWorker, ...args: A) => R,
  ): CallableTask<A, R> {
    return new CallableTask(name, fn);
  }

  static handlerNoArg<A extends unknown[], R>(
    name: string,
    fn: (...args: A) => R,
  ): CallableTaskNoArg<A, R> {
    return new CallableTaskNoArg(name, fn);
  }
}

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
