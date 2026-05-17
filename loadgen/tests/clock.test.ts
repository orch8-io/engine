import { test } from "node:test";
import * as assert from "node:assert/strict";
import { TokenBucket, sleep } from "../src/lib/clock.ts";

// 1. sleep resolves after approximately the specified time
test("sleep resolves after approximately the specified time", async () => {
  const start = Date.now();
  await sleep(50);
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 40, `elapsed ${elapsed}ms, expected >= 40`);
  assert.ok(elapsed < 150, `elapsed ${elapsed}ms, expected < 150`);
});

// 2. sleep(0) resolves quickly
test("sleep(0) resolves quickly", async () => {
  const start = Date.now();
  await sleep(0);
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `sleep(0) took ${elapsed}ms`);
});

// 3. sleep(10) resolves in roughly 10ms
test("sleep(10) resolves in roughly 10ms", async () => {
  const start = Date.now();
  await sleep(10);
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 5, `elapsed ${elapsed}ms, expected >= 5`);
  assert.ok(elapsed < 100, `elapsed ${elapsed}ms, expected < 100`);
});

// 4. TokenBucket(1) allows first acquire immediately
test("TokenBucket(1) allows first acquire immediately", async () => {
  const bucket = new TokenBucket(1);
  const start = Date.now();
  await bucket.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `first acquire took ${elapsed}ms, expected immediate`);
});

// 5. TokenBucket(100) allows rapid burst of acquires
test("TokenBucket(100) allows rapid burst of acquires", async () => {
  const bucket = new TokenBucket(100);
  const start = Date.now();
  for (let i = 0; i < 50; i++) {
    await bucket.acquire();
  }
  const elapsed = Date.now() - start;
  // 50 out of 100 capacity should be near-instant
  assert.ok(elapsed < 200, `burst of 50 took ${elapsed}ms, expected < 200`);
});

// 6. TokenBucket acquire returns a promise
test("TokenBucket acquire returns a promise", () => {
  const bucket = new TokenBucket(10);
  const result = bucket.acquire();
  assert.ok(result instanceof Promise, "acquire() should return a Promise");
});

// 7. TokenBucket with rate 1 enforces ~1/second rate
test("TokenBucket with rate 1 enforces ~1/second rate after burst", async () => {
  const bucket = new TokenBucket(1);
  // First acquire uses the initial token
  await bucket.acquire();
  // Second acquire should wait ~1 second
  const start = Date.now();
  await bucket.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed >= 500, `second acquire took ${elapsed}ms, expected >= 500`);
});

// 8. Multiple acquires on TokenBucket(10) complete within reasonable time
test("multiple acquires on TokenBucket(10) complete within reasonable time", async () => {
  const bucket = new TokenBucket(10);
  const start = Date.now();
  for (let i = 0; i < 10; i++) {
    await bucket.acquire();
  }
  const elapsed = Date.now() - start;
  // 10 tokens available at capacity 10, all should be immediate
  assert.ok(elapsed < 200, `10 acquires took ${elapsed}ms`);
});

// 9. TokenBucket(0.5) caps at capacity of at least 1
test("TokenBucket(0.5) caps at capacity of at least 1", async () => {
  const bucket = new TokenBucket(0.5);
  // Math.max(1, 0.5) = 1, so first acquire should work immediately
  const start = Date.now();
  await bucket.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `first acquire on 0.5 rate took ${elapsed}ms`);
});

// 10. TokenBucket constructor with rate 0 uses capacity 1
test("TokenBucket constructor with rate 0 uses capacity 1", async () => {
  const bucket = new TokenBucket(0);
  // capacity = Math.max(1, 0) = 1
  const start = Date.now();
  await bucket.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `first acquire on rate 0 took ${elapsed}ms`);
});

// 11. sleep returns a Promise
test("sleep returns a Promise", () => {
  const result = sleep(1);
  assert.ok(result instanceof Promise, "sleep should return a Promise");
});

// 12. Multiple concurrent acquires on high-rate bucket work
test("multiple concurrent acquires on high-rate bucket work", async () => {
  const bucket = new TokenBucket(50);
  const start = Date.now();
  const promises = Array.from({ length: 20 }, () => bucket.acquire());
  await Promise.all(promises);
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 500, `concurrent acquires took ${elapsed}ms`);
});

// 13. TokenBucket refills tokens over time
test("TokenBucket refills tokens over time", async () => {
  const bucket = new TokenBucket(100);
  // Drain all tokens
  for (let i = 0; i < 100; i++) {
    await bucket.acquire();
  }
  // Wait 100ms for ~10 tokens to refill (100/s * 0.1s = 10)
  await sleep(100);
  const start = Date.now();
  // Should be able to acquire a few without much delay
  await bucket.acquire();
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `acquire after refill took ${elapsed}ms, expected fast`);
});

// 14. TokenBucket capacity equals ratePerSecond
test("TokenBucket capacity equals ratePerSecond for rate >= 1", async () => {
  // TokenBucket(5) has capacity 5, so 5 acquires should be instant
  const bucket = new TokenBucket(5);
  const start = Date.now();
  for (let i = 0; i < 5; i++) {
    await bucket.acquire();
  }
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `5 acquires on rate=5 took ${elapsed}ms`);

  // 6th acquire should require waiting
  const start2 = Date.now();
  await bucket.acquire();
  const elapsed2 = Date.now() - start2;
  assert.ok(elapsed2 >= 50, `6th acquire was too fast: ${elapsed2}ms`);
});

// 15. Acquire after waiting refills tokens
test("acquire after waiting refills tokens", async () => {
  const bucket = new TokenBucket(10);
  // Use all 10 tokens
  for (let i = 0; i < 10; i++) {
    await bucket.acquire();
  }
  // Wait enough for full refill (10 tokens at 10/s = 1 second)
  await sleep(1100);
  // Should now have ~10 tokens again
  const start = Date.now();
  for (let i = 0; i < 5; i++) {
    await bucket.acquire();
  }
  const elapsed = Date.now() - start;
  assert.ok(elapsed < 50, `acquire after full refill took ${elapsed}ms`);
});
