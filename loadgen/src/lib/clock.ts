/**
 * Token-bucket rate limiter.
 *
 * `acquire()` resolves when a token is available. Bucket refills at
 * `ratePerSecond` and has capacity for 1 second of burst — enough to
 * absorb a tick delay without letting callers stockpile forever.
 */

export class TokenBucket {
  private readonly capacity: number;
  private readonly refillPerMs: number;
  private tokens: number;
  private lastRefill: number;

  constructor(ratePerSecond: number) {
    this.capacity = Math.max(1, ratePerSecond);
    this.refillPerMs = ratePerSecond / 1000;
    this.tokens = this.capacity;
    this.lastRefill = Date.now();
  }

  async acquire(): Promise<void> {
    while (true) {
      this.#refill();
      if (this.tokens >= 1) {
        this.tokens -= 1;
        return;
      }
      const deficit = 1 - this.tokens;
      const waitMs = Math.max(1, Math.ceil(deficit / this.refillPerMs));
      await sleep(waitMs);
    }
  }

  #refill(): void {
    const now = Date.now();
    const elapsed = now - this.lastRefill;
    if (elapsed <= 0) return;
    this.tokens = Math.min(this.capacity, this.tokens + elapsed * this.refillPerMs);
    this.lastRefill = now;
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
