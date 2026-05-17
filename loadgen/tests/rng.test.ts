import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Rng } from "../src/lib/rng.ts";

// 1. Same seed produces same first next() value
test("same seed produces same first next() value", () => {
  const a = new Rng(42);
  const b = new Rng(42);
  assert.equal(a.next(), b.next());
});

// 2. Same seed produces same sequence of 10 values
test("same seed produces same sequence of 10 values", () => {
  const a = new Rng(99);
  const b = new Rng(99);
  for (let i = 0; i < 10; i++) {
    assert.equal(a.next(), b.next(), `mismatch at index ${i}`);
  }
});

// 3. Different seeds produce different sequences
test("different seeds produce different sequences", () => {
  const a = new Rng(1);
  const b = new Rng(2);
  const seqA = Array.from({ length: 5 }, () => a.next());
  const seqB = Array.from({ length: 5 }, () => b.next());
  const allSame = seqA.every((v, i) => v === seqB[i]);
  assert.equal(allSame, false);
});

// 4. next() returns value >= 0
test("next() returns value >= 0", () => {
  const rng = new Rng(123);
  const v = rng.next();
  assert.ok(v >= 0, `expected >= 0, got ${v}`);
});

// 5. next() returns value < 1
test("next() returns value < 1", () => {
  const rng = new Rng(123);
  const v = rng.next();
  assert.ok(v < 1, `expected < 1, got ${v}`);
});

// 6. next() over 1000 calls stays in [0, 1)
test("next() over 1000 calls stays in [0, 1)", () => {
  const rng = new Rng(7);
  for (let i = 0; i < 1000; i++) {
    const v = rng.next();
    assert.ok(v >= 0 && v < 1, `out of range at call ${i}: ${v}`);
  }
});

// 7. int(0, 0) always returns 0
test("int(0, 0) always returns 0", () => {
  const rng = new Rng(10);
  for (let i = 0; i < 50; i++) {
    assert.equal(rng.int(0, 0), 0);
  }
});

// 8. int(5, 5) always returns 5
test("int(5, 5) always returns 5", () => {
  const rng = new Rng(11);
  for (let i = 0; i < 50; i++) {
    assert.equal(rng.int(5, 5), 5);
  }
});

// 9. int(1, 10) returns values in [1, 10]
test("int(1, 10) returns values in [1, 10]", () => {
  const rng = new Rng(20);
  for (let i = 0; i < 200; i++) {
    const v = rng.int(1, 10);
    assert.ok(v >= 1 && v <= 10, `out of range: ${v}`);
  }
});

// 10. int(1, 10) over 100 calls covers most of the range
test("int(1, 10) over 100 calls covers most of the range", () => {
  const rng = new Rng(30);
  const seen = new Set<number>();
  for (let i = 0; i < 100; i++) {
    seen.add(rng.int(1, 10));
  }
  // Should hit at least 8 of 10 values with 100 tries
  assert.ok(seen.size >= 8, `only saw ${seen.size} distinct values`);
});

// 11. int(-5, 5) works with negatives
test("int(-5, 5) works with negatives", () => {
  const rng = new Rng(40);
  let sawNeg = false;
  let sawPos = false;
  for (let i = 0; i < 200; i++) {
    const v = rng.int(-5, 5);
    assert.ok(v >= -5 && v <= 5, `out of range: ${v}`);
    if (v < 0) sawNeg = true;
    if (v > 0) sawPos = true;
  }
  assert.ok(sawNeg, "never saw negative");
  assert.ok(sawPos, "never saw positive");
});

// 12. pick() from single-element array returns that element
test("pick() from single-element array returns that element", () => {
  const rng = new Rng(50);
  for (let i = 0; i < 20; i++) {
    assert.equal(rng.pick(["only"]), "only");
  }
});

// 13. pick() from empty array throws
test("pick() from empty array throws", () => {
  const rng = new Rng(51);
  assert.throws(() => rng.pick([]), /pick from empty/);
});

// 14. pick() returns elements from the array
test("pick() returns elements from the array", () => {
  const rng = new Rng(52);
  const items = ["a", "b", "c"];
  for (let i = 0; i < 50; i++) {
    const v = rng.pick(items);
    assert.ok(items.includes(v), `unexpected value: ${v}`);
  }
});

// 15. pick() over many calls returns diverse elements
test("pick() over many calls returns diverse elements", () => {
  const rng = new Rng(53);
  const items = ["a", "b", "c", "d", "e"];
  const seen = new Set<string>();
  for (let i = 0; i < 100; i++) {
    seen.add(rng.pick(items));
  }
  assert.ok(seen.size >= 4, `only saw ${seen.size} distinct elements`);
});

// 16. weighted() with single item always returns it
test("weighted() with single item always returns it", () => {
  const rng = new Rng(60);
  for (let i = 0; i < 30; i++) {
    assert.equal(rng.weighted(["x"], [5]), "x");
  }
});

// 17. weighted() with zero weight for one item never picks it
test("weighted() with zero weight for one item never picks it", () => {
  const rng = new Rng(61);
  for (let i = 0; i < 200; i++) {
    const v = rng.weighted(["a", "b"], [1, 0]);
    assert.equal(v, "a", `expected 'a' but got '${v}'`);
  }
});

// 18. weighted() from empty throws
test("weighted() from empty throws", () => {
  const rng = new Rng(62);
  assert.throws(() => rng.weighted([], []), /weighted pick from empty/);
});

// 19. weighted() with mismatched lengths throws
test("weighted() with mismatched lengths throws", () => {
  const rng = new Rng(63);
  assert.throws(
    () => rng.weighted(["a", "b"], [1]),
    /items\/weights length mismatch/,
  );
});

// 20. weighted() heavily-weighted item picked most often
test("weighted() heavily-weighted item picked most often", () => {
  const rng = new Rng(64);
  let countB = 0;
  const n = 1000;
  for (let i = 0; i < n; i++) {
    if (rng.weighted(["a", "b"], [1, 100]) === "b") countB++;
  }
  // b has weight 100/101 ~ 99%, expect > 90%
  assert.ok(countB > 900, `b picked ${countB}/${n}, expected >900`);
});

// 21. chance(0) always returns false over 100 calls
test("chance(0) always returns false over 100 calls", () => {
  const rng = new Rng(70);
  for (let i = 0; i < 100; i++) {
    assert.equal(rng.chance(0), false);
  }
});

// 22. chance(1) always returns true over 100 calls
test("chance(1) always returns true over 100 calls", () => {
  const rng = new Rng(71);
  for (let i = 0; i < 100; i++) {
    assert.equal(rng.chance(1), true);
  }
});

// 23. chance(0.5) returns mix of true/false over 1000 calls
test("chance(0.5) returns mix of true/false over 1000 calls", () => {
  const rng = new Rng(72);
  let trueCount = 0;
  for (let i = 0; i < 1000; i++) {
    if (rng.chance(0.5)) trueCount++;
  }
  // Expect roughly 500, accept 350-650
  assert.ok(trueCount > 350 && trueCount < 650, `trueCount=${trueCount}`);
});

// 24. uuid() returns string of length 36
test("uuid() returns string of length 36", () => {
  const rng = new Rng(80);
  const id = rng.uuid();
  assert.equal(id.length, 36);
});

// 25. uuid() matches UUID format (8-4-4-4-12)
test("uuid() matches UUID format (8-4-4-4-12)", () => {
  const rng = new Rng(81);
  const id = rng.uuid();
  const pattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  assert.match(id, pattern);
});

// 26. uuid() has version 4 marker (char at index 14 is '4')
test("uuid() has version 4 marker at index 14", () => {
  const rng = new Rng(82);
  for (let i = 0; i < 20; i++) {
    const id = rng.uuid();
    assert.equal(id[14], "4", `uuid ${id} missing version marker`);
  }
});

// 27. uuid() has variant marker (char at index 19 is 8, 9, a, or b)
test("uuid() has variant marker at index 19", () => {
  const rng = new Rng(83);
  const valid = new Set(["8", "9", "a", "b"]);
  for (let i = 0; i < 20; i++) {
    const id = rng.uuid();
    assert.ok(valid.has(id[19]!), `uuid ${id} variant char '${id[19]}' not in {8,9,a,b}`);
  }
});

// 28. Two consecutive uuid() calls produce different values
test("two consecutive uuid() calls produce different values", () => {
  const rng = new Rng(84);
  const a = rng.uuid();
  const b = rng.uuid();
  assert.notEqual(a, b);
});

// 29. Rng with seed 0 works
test("Rng with seed 0 works", () => {
  const rng = new Rng(0);
  const v = rng.next();
  assert.ok(v >= 0 && v < 1);
});

// 30. Rng with large seed works (2^32)
test("Rng with large seed works (2^32)", () => {
  const rng = new Rng(2 ** 32);
  const v = rng.next();
  assert.ok(v >= 0 && v < 1);
});

// 31. Rng with negative seed works
test("Rng with negative seed works", () => {
  const rng = new Rng(-1);
  const v = rng.next();
  assert.ok(v >= 0 && v < 1);
});

// 32. int() distribution is roughly uniform (all buckets get hits)
test("int() distribution is roughly uniform", () => {
  const rng = new Rng(90);
  const buckets = new Map<number, number>();
  const min = 0;
  const max = 9;
  const n = 5000;
  for (let i = 0; i < n; i++) {
    const v = rng.int(min, max);
    buckets.set(v, (buckets.get(v) ?? 0) + 1);
  }
  // All 10 values should be hit
  for (let v = min; v <= max; v++) {
    const count = buckets.get(v) ?? 0;
    assert.ok(count > 0, `bucket ${v} was never hit`);
    // Each bucket should get roughly n/10 = 500, accept range [250, 750]
    assert.ok(
      count > 250 && count < 750,
      `bucket ${v} count=${count}, expected ~500`,
    );
  }
});

// 33. weighted() with equal weights picks roughly uniformly
test("weighted() with equal weights picks roughly uniformly", () => {
  const rng = new Rng(91);
  const items = ["a", "b", "c"];
  const counts = new Map<string, number>();
  const n = 3000;
  for (let i = 0; i < n; i++) {
    const v = rng.weighted(items, [1, 1, 1]);
    counts.set(v, (counts.get(v) ?? 0) + 1);
  }
  for (const item of items) {
    const c = counts.get(item) ?? 0;
    // Expect ~1000, accept range [700, 1300]
    assert.ok(c > 700 && c < 1300, `${item} count=${c}, expected ~1000`);
  }
});

// 34. next() produces different values on consecutive calls
test("next() produces different values on consecutive calls", () => {
  const rng = new Rng(100);
  const a = rng.next();
  const b = rng.next();
  assert.notEqual(a, b);
});

// 35. Rng seeded with 42 is deterministic across instantiations
test("Rng seeded with 42 is deterministic across instantiations", () => {
  const runs: number[][] = [];
  for (let r = 0; r < 3; r++) {
    const rng = new Rng(42);
    const seq: number[] = [];
    for (let i = 0; i < 20; i++) {
      seq.push(rng.next());
    }
    runs.push(seq);
  }
  assert.deepEqual(runs[0], runs[1]);
  assert.deepEqual(runs[1], runs[2]);
});
