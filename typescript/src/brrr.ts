import { ActiveWorker, CallableTask, CallableTaskNoArg } from "./app.ts";

export class Brrr {
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
