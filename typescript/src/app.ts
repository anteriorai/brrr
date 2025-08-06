export class ActiveWorker {
  call<A extends unknown[], R>(
    callableTask: CallableTask<A, R>,
  ): (...args: A) => R;
  call<A extends unknown[], R>(
    name: string,
    options?: {
      topic: string;
    },
  ): (...args: A) => R;
  call<A extends unknown[], R>(
    name: string | CallableTask<A, R>,
    options?: {
      topic: string;
    },
  ): (args: A) => R {
    return <R>(...args: unknown[]): R => {
      return null as any;
    };
  }

  gather<T1>(a1: Promise<T1>): Promise<[T1]>;
  gather<T1, T2>(a1: Promise<T1>, a2: Promise<T2>): Promise<[T1, T2]>;
  gather(...args: any[]) {
    return Promise.all(args);
  }
}

export class CallableTask<A extends unknown[], R> {
  public readonly name: string;
  public readonly fn: (app: ActiveWorker, ...args: A) => R;

  constructor(name: string, fn: (app: ActiveWorker, ...args: A) => R) {
    this.name = name;
    this.fn = fn;
  }
}

export class CallableTaskNoArg<A extends unknown[], R> {
  public readonly name: string;
  public readonly fn: (...args: A) => R;

  constructor(name: string, fn: (...args: A) => R) {
    this.name = name;
    this.fn = fn;
  }
}
