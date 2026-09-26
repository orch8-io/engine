// Trigger.dev tasks for the benchmark. UNVERIFIED skeleton.
//
// Trigger.dev has no in-task durable "step" primitive equivalent to an
// activity; the closest durable equivalent is a parent task that awaits three
// child tasks with triggerAndWait (each child is checkpointed). This adds
// per-child scheduling overhead the other systems do not pay, which must be
// stated next to any published Trigger.dev number.
import { appendFileSync } from "node:fs";
import { task } from "@trigger.dev/sdk";

const activityLog = process.env.ACTIVITY_LOG ?? "/bench-out/activity.jsonl";

export const benchActivity = task({
  id: "bench-activity",
  run: async ({ key, step }) => {
    await new Promise((resolve) => setTimeout(resolve, 50));
    appendFileSync(activityLog, `${JSON.stringify({ wf: key, step, t: Date.now() })}\n`);
    return { ok: true };
  },
});

export const bench3Step = task({
  id: "bench-3step",
  run: async ({ key }) => {
    for (const step of ["s1", "s2", "s3"]) {
      await benchActivity.triggerAndWait({ key, step }).unwrap();
    }
    return { ok: true };
  },
});
