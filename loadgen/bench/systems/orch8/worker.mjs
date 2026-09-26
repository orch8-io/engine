// Orch8 external REST worker for the benchmark activity (see docs/WORKERS.md).
// Polls POST /workers/tasks/poll for handler `bench_activity`, sleeps 50 ms,
// appends one line to the activity log, then completes with the claim epoch.
// Dependency-free (Node 22 fetch).
import { appendFileSync } from "node:fs";
import { hostname } from "node:os";

const base = process.env.ORCH8_URL ?? "http://engine:8080/api/v1";
const headers = {
  "content-type": "application/json",
  "x-api-key": process.env.ORCH8_API_KEY ?? "",
  "x-tenant-id": process.env.ORCH8_TENANT_ID ?? "bench",
};
const slots = Number(process.env.WORKER_SLOTS ?? 100);
const activityLog = process.env.ACTIVITY_LOG ?? "/bench-out/activity.jsonl";
const workerId = `bench-${hostname()}-${process.pid}`;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let inFlight = 0;

async function post(path, body) {
  const response = await fetch(`${base}${path}`, { method: "POST", headers, body: JSON.stringify(body) });
  if (!response.ok) throw new Error(`${path} -> ${response.status} ${await response.text()}`);
  const text = await response.text();
  return text ? JSON.parse(text) : {};
}

async function execute(task) {
  try {
    await sleep(50);
    appendFileSync(
      activityLog,
      `${JSON.stringify({ wf: task.instance_id, step: task.block_id, t: Date.now() })}\n`,
    );
    await post(`/workers/tasks/${task.id}/complete`, {
      worker_id: workerId,
      claim_epoch: task.claim_epoch,
      output: { ok: true },
    });
  } catch (error) {
    console.error(`task ${task.id}: ${error.message}`);
  } finally {
    inFlight -= 1;
  }
}

for (;;) {
  const free = slots - inFlight;
  if (free <= 0) {
    await sleep(5);
    continue;
  }
  try {
    const response = await post("/workers/tasks/poll", {
      handler_name: "bench_activity",
      worker_id: workerId,
      limit: Math.min(free, 100),
    });
    // Current engines return an envelope ({tasks, poll_after_ms, ...});
    // older releases return a bare task array. Accept both so the harness
    // can benchmark either.
    const tasks = Array.isArray(response) ? response : (response.tasks ?? []);
    const pollAfter = Array.isArray(response) ? 100 : (response.poll_after_ms ?? 100);
    for (const task of tasks) {
      inFlight += 1;
      void execute(task);
    }
    // Poll again immediately while work is flowing; otherwise honour the
    // server's hint (capped so idle-to-busy transitions stay responsive).
    if (tasks.length === 0) await sleep(Math.min(pollAfter, 100));
  } catch (error) {
    console.error(`poll: ${error.message}`);
    await sleep(500);
  }
}
