// Trigger.dev benchmark driver. UNVERIFIED skeleton: uses the documented
// @trigger.dev/sdk `tasks.trigger` and `runs.poll` helpers and the run fields
// `status`, `startedAt`, `finishedAt`. Requires TRIGGER_SECRET_KEY and
// TRIGGER_API_URL for a completed self-hosted stack (see docker-compose.yml).
import { runs, tasks } from "@trigger.dev/sdk";
import { main } from "../../lib/driver-common.mjs";

async function setup() {
  if (!process.env.TRIGGER_SECRET_KEY) throw new Error("TRIGGER_SECRET_KEY is required");
  return { runTag: Date.now() };
}

async function runOne(index, { runTag }) {
  const key = `bench-${runTag}-${index}`;
  const handle = await tasks.trigger("bench-3step", { key }, { idempotencyKey: key });
  const run = await runs.poll(handle.id, { pollIntervalMs: 100 });
  const completed = run.status === "COMPLETED";
  return {
    id: key,
    status: completed ? "completed" : "failed",
    completed_at_ms: Date.now(),
    server_started_at_ms: run.startedAt ? new Date(run.startedAt).getTime() : undefined,
    server_closed_at_ms: run.finishedAt ? new Date(run.finishedAt).getTime() : undefined,
  };
}

await main(runOne, { setup });
