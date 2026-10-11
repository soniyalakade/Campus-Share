export class LamportClock {
  #timestamp;

  constructor(initialTimestamp = 0) {
    if (!Number.isSafeInteger(initialTimestamp) || initialTimestamp < 0) {
      throw new RangeError('Lamport timestamp must be a non-negative safe integer');
    }
    this.#timestamp = initialTimestamp;
  }

  tick() {
    if (this.#timestamp >= Number.MAX_SAFE_INTEGER) {
      throw new RangeError('Lamport timestamp exceeds the safe integer range');
    }
    this.#timestamp += 1;
    return this.#timestamp;
  }

  receive(remoteTimestamp) {
    if (!Number.isSafeInteger(remoteTimestamp) || remoteTimestamp < 0) {
      throw new RangeError('Received Lamport timestamp must be a non-negative safe integer');
    }
    this.#timestamp = Math.max(this.#timestamp, remoteTimestamp);
    return this.tick();
  }

  getTimestamp() {
    return this.#timestamp;
  }
}
