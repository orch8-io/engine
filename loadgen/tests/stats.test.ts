import { test } from "node:test";
import * as assert from "node:assert/strict";
import { Counters } from "../src/lib/stats.ts";

// 1. New Counters has all fields at 0
test("new Counters has all fields at 0", () => {
  const c = new Counters();
  assert.equal(c.spawned, 0);
  assert.equal(c.spawnFailed, 0);
  assert.equal(c.completed, 0);
  assert.equal(c.failed, 0);
  assert.equal(c.cancelled, 0);
  assert.equal(c.signalsSent, 0);
  assert.equal(c.workerPolls, 0);
  assert.equal(c.tasksClaimed, 0);
  assert.equal(c.tasksCompleted, 0);
  assert.equal(c.tasksFailed, 0);
  assert.equal(c.errors5xx, 0);
  assert.equal(c.errorsConn, 0);
});

// 2. Incrementing spawned works
test("incrementing spawned works", () => {
  const c = new Counters();
  c.spawned++;
  assert.equal(c.spawned, 1);
  c.spawned += 5;
  assert.equal(c.spawned, 6);
});

// 3. templateSpawned creates new template entry
test("templateSpawned creates new template entry", () => {
  const c = new Counters();
  c.templateSpawned("order-flow");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("order-flow"), "template name missing from snapshot");
  assert.ok(snap.includes("spawned=1"), "spawned count missing");
});

// 4. templateSpawned increments existing template
test("templateSpawned increments existing template", () => {
  const c = new Counters();
  c.templateSpawned("signup");
  c.templateSpawned("signup");
  c.templateSpawned("signup");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("spawned=3"), `expected spawned=3 in: ${snap}`);
});

// 5. templateCompleted increments completed count
test("templateCompleted increments completed count", () => {
  const c = new Counters();
  c.templateSpawned("checkout");
  c.templateCompleted("checkout");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("ok=1"), `expected ok=1 in: ${snap}`);
});

// 6. templateFailed increments failed count
test("templateFailed increments failed count", () => {
  const c = new Counters();
  c.templateSpawned("payment");
  c.templateFailed("payment");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("fail=1"), `expected fail=1 in: ${snap}`);
});

// 7. snapshot() returns string containing "spawned="
test("snapshot() contains 'spawned='", () => {
  const c = new Counters();
  c.spawned = 42;
  const snap = c.snapshot();
  assert.ok(snap.includes("spawned=42"), `missing spawned= in: ${snap}`);
});

// 8. snapshot() contains "done=" for completed
test("snapshot() contains 'done='", () => {
  const c = new Counters();
  c.completed = 10;
  const snap = c.snapshot();
  assert.ok(snap.includes("done=10"), `missing done= in: ${snap}`);
});

// 9. snapshot() contains "fail=" for failed
test("snapshot() contains 'fail='", () => {
  const c = new Counters();
  c.failed = 3;
  const snap = c.snapshot();
  assert.ok(snap.includes("fail=3"), `missing fail= in: ${snap}`);
});

// 10. snapshot() contains "rate=" for throughput
test("snapshot() contains 'rate='", () => {
  const c = new Counters();
  const snap = c.snapshot();
  assert.ok(snap.includes("rate="), `missing rate= in: ${snap}`);
});

// 11. snapshot() contains uptime in HH:MM:SS format
test("snapshot() contains uptime in HH:MM:SS format", () => {
  const c = new Counters();
  const snap = c.snapshot();
  // up=00:00:00 at minimum
  assert.match(snap, /up=\d{2}:\d{2}:\d{2}/);
});

// 12. snapshot() contains spawn_p50 and spawn_p95
test("snapshot() contains spawn_p50 and spawn_p95", () => {
  const c = new Counters();
  const snap = c.snapshot();
  assert.ok(snap.includes("spawn_p50="), `missing spawn_p50= in: ${snap}`);
  assert.ok(snap.includes("spawn_p95="), `missing spawn_p95= in: ${snap}`);
});

// 13. snapshot() contains e2e_p50 and e2e_p95
test("snapshot() contains e2e_p50 and e2e_p95", () => {
  const c = new Counters();
  const snap = c.snapshot();
  assert.ok(snap.includes("e2e_p50="), `missing e2e_p50= in: ${snap}`);
  assert.ok(snap.includes("e2e_p95="), `missing e2e_p95= in: ${snap}`);
});

// 14. recordLatency records values reflected in snapshot
test("recordLatency records values reflected in snapshot", () => {
  const c = new Counters();
  c.recordLatency(100);
  c.recordLatency(200);
  c.recordLatency(300);
  const snap = c.snapshot();
  // p50 should be around 200ms, p95 around 300ms
  assert.ok(snap.includes("spawn_p50=200ms"), `unexpected p50 in: ${snap}`);
});

// 15. recordE2eLatency records values reflected in snapshot
test("recordE2eLatency records values reflected in snapshot", () => {
  const c = new Counters();
  c.recordE2eLatency(500);
  c.recordE2eLatency(1000);
  c.recordE2eLatency(1500);
  const snap = c.snapshot();
  assert.ok(snap.includes("e2e_p50=1000ms"), `unexpected e2e_p50 in: ${snap}`);
});

// 16. templateSnapshot() returns empty string when no templates
test("templateSnapshot() returns empty string when no templates", () => {
  const c = new Counters();
  assert.equal(c.templateSnapshot(), "");
});

// 17. templateSnapshot() contains template name
test("templateSnapshot() contains template name", () => {
  const c = new Counters();
  c.templateSpawned("my-workflow");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("my-workflow"), `template name missing in: ${snap}`);
});

// 18. templateSnapshot() shows spawned/ok/fail/pending counts
test("templateSnapshot() shows spawned/ok/fail/pending counts", () => {
  const c = new Counters();
  c.templateSpawned("wf");
  c.templateSpawned("wf");
  c.templateSpawned("wf");
  c.templateCompleted("wf");
  c.templateFailed("wf");
  // spawned=3, ok=1, fail=1, pending=1
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("spawned=3"), `missing spawned=3 in: ${snap}`);
  assert.ok(snap.includes("ok=1"), `missing ok=1 in: ${snap}`);
  assert.ok(snap.includes("fail=1"), `missing fail=1 in: ${snap}`);
  assert.ok(snap.includes("pending=1"), `missing pending=1 in: ${snap}`);
});

// 19. templateSnapshot() shows success rate percentage
test("templateSnapshot() shows success rate percentage", () => {
  const c = new Counters();
  c.templateSpawned("wf");
  c.templateSpawned("wf");
  c.templateCompleted("wf");
  c.templateCompleted("wf");
  const snap = c.templateSnapshot();
  // 2 completed out of 2 total = 100%
  assert.ok(snap.includes("success=100%"), `missing success=100% in: ${snap}`);
});

// 20. Multiple templates appear in templateSnapshot
test("multiple templates appear in templateSnapshot", () => {
  const c = new Counters();
  c.templateSpawned("alpha");
  c.templateSpawned("beta");
  c.templateSpawned("gamma");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("alpha"), "missing alpha");
  assert.ok(snap.includes("beta"), "missing beta");
  assert.ok(snap.includes("gamma"), "missing gamma");
});

// 21. snapshot() with no latencies shows 0ms
test("snapshot() with no latencies shows 0ms", () => {
  const c = new Counters();
  const snap = c.snapshot();
  assert.ok(snap.includes("spawn_p50=0ms"), `expected 0ms spawn_p50 in: ${snap}`);
  assert.ok(snap.includes("spawn_p95=0ms"), `expected 0ms spawn_p95 in: ${snap}`);
  assert.ok(snap.includes("e2e_p50=0ms"), `expected 0ms e2e_p50 in: ${snap}`);
  assert.ok(snap.includes("e2e_p95=0ms"), `expected 0ms e2e_p95 in: ${snap}`);
});

// 22. Reservoir sampling caps at 500 (record >500 latencies, verify no crash)
test("reservoir sampling caps at 500 latencies without crash", () => {
  const c = new Counters();
  for (let i = 0; i < 1000; i++) {
    c.spawned++;
    c.recordLatency(i);
  }
  // Should not throw and should produce valid snapshot
  const snap = c.snapshot();
  assert.ok(snap.includes("spawn_p50="), "snapshot should still work");
});

// 23. Counters startedAt is close to Date.now()
test("Counters startedAt is close to Date.now()", () => {
  const before = Date.now();
  const c = new Counters();
  const after = Date.now();
  assert.ok(c.startedAt >= before, "startedAt should be >= test start");
  assert.ok(c.startedAt <= after, "startedAt should be <= test end");
});

// 24. Template with 0 completions shows "-" success rate
test("template with 0 completions shows '-' success rate", () => {
  const c = new Counters();
  c.templateSpawned("pending-wf");
  const snap = c.templateSnapshot();
  assert.ok(snap.includes("success=-%"), `expected success=-% in: ${snap}`);
});

// 25. errorsConn and errors5xx reflected correctly
test("errorsConn and errors5xx reflected correctly in snapshot", () => {
  const c = new Counters();
  c.errors5xx = 7;
  c.errorsConn = 3;
  const snap = c.snapshot();
  // Format is errs=<5xx>+<conn>
  assert.ok(snap.includes("errs=7+3"), `expected errs=7+3 in: ${snap}`);
});
