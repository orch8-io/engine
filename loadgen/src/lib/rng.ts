/**
 * Seedable RNG (xoroshiro128+ style, simplified).
 *
 * We use a tiny hand-rolled PRNG so a `--seed` flag produces reproducible
 * runs across the whole process, which matters for debugging: "do it
 * again with seed=42 and I'll see the same workflow mix".
 */

export class Rng {
  private state0: bigint;
  private state1: bigint;

  constructor(seed: number) {
    // SplitMix64 to derive two 64-bit states from one seed.
    let s = BigInt(seed) & 0xffffffffffffffffn;
    const next = (): bigint => {
      s = (s + 0x9e3779b97f4a7c15n) & 0xffffffffffffffffn;
      let z = s;
      z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & 0xffffffffffffffffn;
      z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & 0xffffffffffffffffn;
      return z ^ (z >> 31n);
    };
    this.state0 = next();
    this.state1 = next();
  }

  /** Uniform float in [0, 1). */
  next(): number {
    const s0 = this.state0;
    let s1 = this.state1;
    const result = (s0 + s1) & 0xffffffffffffffffn;
    s1 ^= s0;
    this.state0 =
      (((s0 << 55n) | (s0 >> 9n)) ^ s1 ^ (s1 << 14n)) &
      0xffffffffffffffffn;
    this.state1 = ((s1 << 36n) | (s1 >> 28n)) & 0xffffffffffffffffn;
    // Use top 53 bits for a double.
    return Number(result >> 11n) / 2 ** 53;
  }

  /** Integer in [min, max] inclusive. */
  int(min: number, max: number): number {
    return Math.floor(this.next() * (max - min + 1)) + min;
  }

  /** Pick an element. */
  pick<T>(items: readonly T[]): T {
    if (items.length === 0) throw new Error("pick from empty");
    return items[Math.floor(this.next() * items.length)]!;
  }

  /** Weighted pick: items and weights must align. */
  weighted<T>(items: readonly T[], weights: readonly number[]): T {
    if (items.length === 0) throw new Error("weighted pick from empty");
    if (items.length !== weights.length) {
      throw new Error("items/weights length mismatch");
    }
    const total = weights.reduce((a, b) => a + b, 0);
    let r = this.next() * total;
    for (let i = 0; i < items.length; i++) {
      r -= weights[i]!;
      if (r <= 0) return items[i]!;
    }
    return items[items.length - 1]!;
  }

  /** True with the given probability (0..1). */
  chance(p: number): boolean {
    return this.next() < p;
  }

  /** RFC4122-ish UUIDv4 (not cryptographically strong — we're a loadgen). */
  uuid(): string {
    const bytes = new Array<number>(16);
    for (let i = 0; i < 16; i++) bytes[i] = this.int(0, 255);
    bytes[6] = (bytes[6]! & 0x0f) | 0x40;
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    const hex = bytes.map((b) => b.toString(16).padStart(2, "0"));
    return (
      `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}` +
      `-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}` +
      `-${hex.slice(10, 16).join("")}`
    );
  }
}
