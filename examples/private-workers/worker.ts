import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { setTimeout as sleep } from "node:timers/promises";

type Task = {
  id: string;
  claim_epoch: number;
  checkpoint_seq?: number;
  params: { chunks?: number };
  resume_checkpoint?: { next_chunk: number };
};

class LeaseLost extends Error {}

const rawUrl = process.env.ORCH8_URL;
if (!rawUrl) throw new Error("ORCH8_URL is required");
const address = rawUrl.replace(/\/+$/, "");
const baseUrl = address.endsWith("/api/v1") ? address : address + "/api/v1";
const secret = process.env.ORCH8_API_KEY_FILE
  ? readFileSync(process.env.ORCH8_API_KEY_FILE, "utf8").trim()
  : process.env.ORCH8_API_KEY;
const tenant = process.env.ORCH8_TENANT_ID ?? "default";
const worker = process.env.ORCH8_WORKER_ID ?? randomUUID();
const version = process.env.ORCH8_WORKER_VERSION ?? "1.0.0";
const handler = process.argv[2] ?? "private_prepare";
if (handler !== "private_prepare" && handler !== "private_compute") throw new Error("Unknown handler");

const labels: Record<string, string> = { site: "private", accelerator: "cpu", handler_version: version };
if (process.env.ORCH8_RESIDENCY) labels.residency = process.env.ORCH8_RESIDENCY;
let draining = false;
process.on("SIGTERM", () => { draining = true; });
process.on("SIGINT", () => { draining = true; });

async function post<T>(path: string, body: object, lease = false): Promise<T> {
  const headers = { "content-type": "application/json", "x-tenant-id": tenant } as Record<string, string>;
  if (secret) headers["x-api-key"] = secret;
  const response = await fetch(baseUrl + path, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
    redirect: "error",
    signal: AbortSignal.timeout(10_000),
  });
  if (lease && response.status === 409) throw new LeaseLost();
  if (!response.ok) throw new Error(`${path} failed with HTTP ${response.status}`);
  const text = await response.text();
  return (text ? JSON.parse(text) : undefined) as T;
}

async function processTask(task: Task) {
  const path = `/workers/tasks/${task.id}`;
  const claim = { worker_id: worker, claim_epoch: task.claim_epoch };
  const chunks = task.params.chunks ?? 5;
  if (!Number.isInteger(chunks) || chunks < 1 || chunks > 100) {
    await post(path + "/fail", { ...claim, message: "chunks must be an integer from 1 to 100", retryable: false }, true);
    return;
  }
  const start = task.resume_checkpoint?.next_chunk ?? 0;
  let checkpointSeq = task.checkpoint_seq ?? 0;
  for (let chunk = start; chunk < chunks; chunk++) {
    await sleep(50);
    const response = await post<{ checkpoint_seq: number }>(path + "/heartbeat", {
      ...claim,
      checkpoint: { next_chunk: chunk + 1 },
      checkpoint_seq: checkpointSeq,
    }, true);
    checkpointSeq = response.checkpoint_seq;
  }
  await post(path + "/complete", { ...claim, output: { chunks, resumed_from: start } }, true);
  console.log(`Completed task ${task.id}`);
}

async function run() {
  console.log(`Worker ${worker} serving ${handler}`);
  while (!draining) {
    const now = Date.now();
    const reply = await post<{ tasks: Task[]; poll_after_ms?: number }>("/workers/tasks/poll", {
      worker_id: worker,
      handler_name: handler,
      version,
      limit: 1,
      capabilities: {
        runtime_id: worker,
        kind: "server",
        trust: "registered",
        handlers: [handler],
        labels,
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 60_000).toISOString(),
      },
    });
    for (const task of reply.tasks) {
      try {
        await processTask(task);
      } catch (error) {
        if (!(error instanceof LeaseLost)) throw error;
        console.log(`Lease changed for task ${task.id}`);
      }
    }
    if (!draining && !reply.tasks.length) await sleep(Math.max(250, reply.poll_after_ms ?? 0));
  }
}

await run();
