// Temporal worker: hosts bench3Step and the 50 ms activity.
// UNVERIFIED: written against the documented @temporalio/worker 1.x API; not
// yet run end-to-end by the harness.
const { appendFileSync } = require("node:fs");
const { NativeConnection, Worker } = require("@temporalio/worker");

const activityLog = process.env.ACTIVITY_LOG ?? "/bench-out/activity.jsonl";

const activities = {
  async benchActivity(workflowKey, step) {
    await new Promise((resolve) => setTimeout(resolve, 50));
    appendFileSync(activityLog, `${JSON.stringify({ wf: workflowKey, step, t: Date.now() })}\n`);
    return { ok: true };
  },
};

async function connectWithRetry(address) {
  for (;;) {
    try {
      return await NativeConnection.connect({ address });
    } catch (error) {
      console.error(`waiting for Temporal at ${address}: ${error.message}`);
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
  }
}

async function main() {
  const slots = Number(process.env.WORKER_SLOTS ?? 100);
  const connection = await connectWithRetry(process.env.TEMPORAL_ADDRESS ?? "temporal:7233");
  const worker = await Worker.create({
    connection,
    namespace: "default",
    taskQueue: "bench",
    workflowsPath: require.resolve("./workflows.cjs"),
    activities,
    maxConcurrentActivityTaskExecutions: slots,
  });
  await worker.run();
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
