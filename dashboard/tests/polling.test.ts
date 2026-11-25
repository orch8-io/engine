import assert from "node:assert/strict";
import { test } from "node:test";
import { createPollingLoop } from "../src/lib/polling.ts";

test("polling backs off after each failure, caps the delay, and recovers", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let calls = 0;
  const poller = createPollingLoop(async () => ++calls >= 6, 100);
  await poller.refresh();

  for (const delay of [200, 300, 400, 500, 500, 100]) {
    const before = calls;
    t.mock.timers.tick(delay - 1);
    assert.equal(calls, before);
    t.mock.timers.tick(1);
    assert.equal(calls, before + 1);
    await poller.refresh();
  }
  poller.dispose();
  t.mock.timers.tick(1000);
  assert.equal(calls, 7);
});

test("polling shares pending work, pauses while hidden, and aborts on dispose", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  let calls = 0;
  let pendingSignal: AbortSignal | undefined;
  let finish: ((success: boolean) => void) | undefined;
  const poller = createPollingLoop((signal) => {
    calls++;
    pendingSignal = signal;
    return new Promise((resolve) => { finish = resolve; });
  }, 100);

  const first = poller.refresh();
  assert.equal(poller.refresh(), first);
  t.mock.timers.tick(1000);
  assert.equal(calls, 1);
  poller.pause();
  finish?.(true);
  await first;
  t.mock.timers.tick(1000);
  assert.equal(calls, 1);

  const second = poller.resume();
  assert.equal(calls, 2);
  poller.dispose();
  assert.ok(pendingSignal?.aborted);
  finish?.(true);
  await second;
  t.mock.timers.tick(1000);
  assert.equal(calls, 2);
});
