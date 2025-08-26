import type { Cache, MemKey, Store } from "../store.ts";

export class InMemoryStore implements Store {
  private store = new Map<string, Uint8Array>();

  public async compareAndDelete(
    key: MemKey,
    expected: Uint8Array,
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value || !this.isEqualBytes(value, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async compareAndSet(
    key: MemKey,
    value: Uint8Array,
    expected: Uint8Array,
  ): Promise<void> {
    const keyStr = this.key2str(key);
    const currentValue = this.innerStore.get(keyStr);
    if (!currentValue || !this.isEqualBytes(currentValue, expected)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  public async delete(key: MemKey): Promise<void> {
    const keyStr = this.key2str(key);
    if (!this.innerStore.has(keyStr)) {
      throw new NotFoundError(key);
    }
    this.innerStore.delete(keyStr);
  }

  public async get(key: MemKey): Promise<Uint8Array> {
    const keyStr = this.key2str(key);
    const value = this.innerStore.get(keyStr);
    if (!value) {
      throw new NotFoundError(key);
    }
    return value;
  }

  public async has(key: MemKey): Promise<boolean> {
    const keyStr = this.key2str(key);
    return this.innerStore.has(keyStr);
  }

  public async incr(key: string): Promise<number> {
    const current = this.innerCache.get(key) ?? 0;
    const next = current + 1;
    this.innerCache.set(key, next);
    return next;
  }

  public async set(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    this.innerStore.set(keyStr, value);
  }

  public async setNewValue(key: MemKey, value: Uint8Array): Promise<void> {
    const keyStr = this.key2str(key);
    if (this.innerStore.has(keyStr)) {
      throw new CompareMismatchError(key);
    }
    this.innerStore.set(keyStr, value);
  }

  private isEqualBytes(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((_, i) => a[i] === b[i]);
  }
}
