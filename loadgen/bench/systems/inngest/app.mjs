// Inngest app serving the benchmark function: three sequential step.run()
// calls, each sleeping 50 ms and appending to the activity log.
// UNVERIFIED: written against the documented inngest v3 SDK
// (new Inngest, createFunction, step.run, inngest/express serve).
import { appendFileSync } from "node:fs";
import express from "express";
import { Inngest } from "inngest";
import { serve } from "inngest/express";

const activityLog = process.env.ACTIVITY_LOG ?? "/bench-out/activity.jsonl";
const inngest = new Inngest({ id: "orch8-bench" });

const bench3Step = inngest.createFunction(
  { id: "bench-3step" },
  { event: "bench/start" },
  async ({ event, step }) => {
    for (const name of ["s1", "s2", "s3"]) {
      await step.run(name, async () => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        appendFileSync(activityLog, `${JSON.stringify({ wf: event.data.key, step: name, t: Date.now() })}\n`);
        return { ok: true };
      });
    }
    return { ok: true };
  },
);

const app = express();
app.use(express.json({ limit: "4mb" }));
app.use("/api/inngest", serve({ client: inngest, functions: [bench3Step] }));
app.listen(3000, () => console.log("inngest bench app listening on :3000"));
