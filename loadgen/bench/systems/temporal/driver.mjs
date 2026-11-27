// Temporal benchmark driver: starts N bench3Step workflows (bounded in flight),
// awaits each result, and reads server start/close times via describe().
// UNVERIFIED: written against the documented @temporalio/client 1.x API
// (Connection.connect, Client.workflow.start/getHandle, describe().startTime/
// closeTime); run `npm install` in this directory first (run.sh does it).
import { Client, Connection, WorkflowFailedError } from "@temporalio/client";
import { main, recordError, sleep, withRetry } from "../../lib/driver-common.mjs";

const address = process.env.TEMPORAL_HOST ?? "127.0.0.1:7233";

async function setup() {
  const connection = await withRetry(() => Connection.connect({ address }), {
    deadline: Date.now() + 120_000,
    delayMs: 1000,
    label: "connect to Temporal",
  });
  return { client: new Client({ connection, namespace: "default" }), runTag: Date.now() };
}

async function runOne(index, { client, runTag }, deadline) {
  const workflowId = `bench-${runTag}-${index}`;
  // Workflow ids are unique per run, so a retried start after a server crash
  // either starts the workflow or fails with "already started"; either way
  // the handle below refers to exactly one execution.
  await withRetry(
    async () => {
      try {
        await client.withDeadline(deadline, () =>
          client.workflow.start("bench3Step", { taskQueue: "bench", workflowId, args: [workflowId] }));
      } catch (error) {
        if (error?.name !== "WorkflowExecutionAlreadyStartedError") throw error;
      }
    },
    { deadline, label: "start workflow" },
  );
  const handle = client.workflow.getHandle(workflowId);
  for (;;) {
    try {
      await client.withDeadline(deadline, () => handle.result());
      break;
    } catch (error) {
      if (error instanceof WorkflowFailedError) break;
      recordError(error);
      if (Date.now() >= deadline) {
        return { id: workflowId, status: "failed", error: "workflow did not close before the driver deadline" };
      }
      await sleep(250);
    }
  }
  const completedAt = Date.now();
  const description = await withRetry(
    () => client.withDeadline(deadline, () => handle.describe()),
    { deadline, label: "describe" },
  );
  return {
    id: workflowId,
    status: description.status?.name === "COMPLETED" ? "completed" : "failed",
    engine_state: description.status?.name,
    completed_at_ms: completedAt,
    server_started_at_ms: description.startTime?.getTime(),
    server_closed_at_ms: description.closeTime?.getTime(),
  };
}

await main(runOne, { setup });
