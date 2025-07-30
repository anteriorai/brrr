abstract class BrrrError extends Error {
  protected constructor(message: string) {
    super();
    this.name = this.constructor.name;
    this.message = message;
  }
}
