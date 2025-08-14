export type Result<K, T> =
  | {
      kind: K;
    }
  | {
      kind: "Ok";
      value: T;
    };
