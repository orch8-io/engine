import { test } from "node:test";
import * as assert from "node:assert/strict";
import {
  requestShutdown,
  isShuttingDown,
  onShutdown,
} from "../src/lib/shutdown.ts";

// IMPORTANT: Tests that depend on isShuttingDown() === false MUST run before
// any test calls requestShutdown(). Since this module has persistent mutable
// state, test ordering matters. node:test runs tests sequentially by default,
// so the order in this file is the execution order.

// ─── Phase 1: Before shutdown ──────────────────────────────────────────────────

// 1. isShuttingDown() returns false initially
test("isShuttingDown() returns false initially", () => {
  assert.equal(isShuttingDown(), false);
});

// 2. onShutdown registers a listener without triggering
test("onShutdown registers a listener without triggering it", () => {
  let called = false;
  onShutdown(() => {
    called = true;
  });
  assert.equal(called, false, "listener should not be called on registration");
});

// 3. Multiple listeners can be registered
test("multiple listeners can be registered", () => {
  let count = 0;
  onShutdown(() => {
    count++;
  });
  onShutdown(() => {
    count++;
  });
  // None should fire yet
  assert.equal(count, 0, "no listeners should fire before shutdown");
});

// 11. Listener receives no arguments
// (Placed here so we register it before shutdown fires, then verify below.)
let listenerArgCount = -1;
onShutdown(function (this: void, ...args: unknown[]) {
  listenerArgCount = args.length;
});

// ─── Phase 2: Trigger shutdown ─────────────────────────────────────────────────

// We track listener invocations for tests 5, 6, 7.
const callOrder: string[] = [];
onShutdown(() => {
  callOrder.push("A");
});
onShutdown(() => {
  callOrder.push("B");
});
onShutdown(() => {
  throw new Error("intentional throw from listener C");
});
onShutdown(() => {
  callOrder.push("D");
});

// 4. requestShutdown sets isShuttingDown to true
test("requestShutdown sets isShuttingDown to true", () => {
  requestShutdown("test-reason");
  assert.equal(isShuttingDown(), true);
});

// 5. requestShutdown calls all registered listeners
test("requestShutdown calls all registered listeners", () => {
  // A, B, D should have been called (C throws but the rest still run)
  assert.ok(callOrder.includes("A"), "listener A not called");
  assert.ok(callOrder.includes("B"), "listener B not called");
  assert.ok(callOrder.includes("D"), "listener D not called");
});

// 6. requestShutdown calls listeners in registration order
test("requestShutdown calls listeners in registration order", () => {
  const abIdx = callOrder.indexOf("A");
  const bIdx = callOrder.indexOf("B");
  const dIdx = callOrder.indexOf("D");
  assert.ok(abIdx < bIdx, "A should come before B");
  assert.ok(bIdx < dIdx, "B should come before D");
});

// 7. Listener that throws doesn't prevent other listeners
test("listener that throws does not prevent other listeners", () => {
  // We registered A, B, throw-C, D. D still ran.
  assert.ok(callOrder.includes("D"), "D should run even though C threw");
});

// 8. Second requestShutdown is a no-op
test("second requestShutdown is a no-op", () => {
  const orderBefore = [...callOrder];
  requestShutdown("second-reason");
  // callOrder should not change -- listeners are not re-invoked
  assert.deepEqual(callOrder, orderBefore, "listeners should not fire again");
});

// 9. isShuttingDown returns true after shutdown
test("isShuttingDown returns true after shutdown", () => {
  assert.equal(isShuttingDown(), true);
});

// 10. onShutdown after shutdown doesn't auto-call listener
test("onShutdown after shutdown does not auto-call listener", () => {
  let lateCalled = false;
  onShutdown(() => {
    lateCalled = true;
  });
  assert.equal(lateCalled, false, "listener registered after shutdown should not auto-fire");
});

// 11. Listener receives no arguments (verification)
test("listener receives no arguments", () => {
  assert.equal(listenerArgCount, 0, "listener should receive 0 args");
});

// 12. requestShutdown reason appears in stderr
test("requestShutdown reason appears in stderr", async () => {
  // We spawn a child process to capture stderr, since our current process
  // has already shut down and stderr.write already happened.
  // Using child_process.execFile (array args, no shell) -- safe, no injection.
  const cp = await import("node:child_process");
  const util = await import("node:util");
  const execFileAsync = util.promisify(cp.execFile);

  const script = [
    'import { requestShutdown } from "./src/lib/shutdown.ts";',
    'requestShutdown("my-custom-reason");',
  ].join("\n");

  const { stderr } = await execFileAsync(
    "node",
    ["--import", "tsx/esm", "--input-type=module", "-e", script],
    { cwd: "/Users/oleksiivasylenko/dev/orch8.io/engine/loadgen" },
  );

  assert.ok(
    stderr.includes("my-custom-reason"),
    `expected stderr to include 'my-custom-reason', got: ${stderr}`,
  );
  assert.ok(
    stderr.includes("[loadgen] shutdown requested"),
    `expected shutdown prefix in stderr, got: ${stderr}`,
  );
});
