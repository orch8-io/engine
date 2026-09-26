// Hatchet workflow shared by the worker and the driver: a DAG of three tasks
// chained by `parents`, each sleeping 50 ms and appending to the activity log.
// UNVERIFIED: written against the documented Hatchet TypeScript SDK v1 API
// (HatchetClient.init, hatchet.workflow, workflow.task with parents).
import { appendFileSync } from "node:fs";
import { HatchetClient } from "@hatchet-dev/typescript-sdk/v1";

export const hatchet = HatchetClient.init();
const activityLog = process.env.ACTIVITY_LOG ?? "/bench-out/activity.jsonl";

async function activity(key, step) {
  await new Promise((resolve) => setTimeout(resolve, 50));
  appendFileSync(activityLog, `${JSON.stringify({ wf: key, step, t: Date.now() })}\n`);
  return { ok: true };
}

export const bench3Step = hatchet.workflow({ name: "bench-3step" });
const s1 = bench3Step.task({ name: "s1", fn: (input) => activity(input.key, "s1") });
const s2 = bench3Step.task({ name: "s2", parents: [s1], fn: (input) => activity(input.key, "s2") });
bench3Step.task({ name: "s3", parents: [s2], fn: (input) => activity(input.key, "s3") });
