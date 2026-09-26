// Orch8 benchmark driver. Publishes a 3-step sequence whose steps all use the
// external `bench_activity` handler, then submits N instances (bounded by
// BENCH_CONCURRENCY in flight) and polls each one to a terminal state.
// Latency uses the server's instance created_at -> updated_at at completion.
// Dependency-free (Node 22 fetch). Contract: ../../lib/driver-common.mjs.
import { randomUUID } from "node:crypto";
import { main, recordError, sleep, withRetry } from "../../lib/driver-common.mjs";

const base = process.env.ORCH8_URL ?? "http://127.0.0.1:18080/api/v1";
const tenant = "bench";
const namespace = "bench";
const pollMs = Number(process.env.BENCH_POLL_MS ?? 100);
const headers = {
  "content-type": "application/json",
  "x-api-key": process.env.ORCH8_API_KEY ?? "bench-only-api-key-not-a-secret-0123456789",
  "x-tenant-id": tenant,
};

async function call(method, path, body) {
  const response = await fetch(`${base}${path}`, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();
  if (!response.ok) throw new Error(`${method} ${path} -> ${response.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : {};
}

const step = (n) => ({ type: "step", id: `s${n}`, handler: "bench_activity", params: { step: n } });

async function setup() {
  const runTag = Date.now();
  const sequence = await call("POST", "/sequences", {
    id: randomUUID(),
    tenant_id: tenant,
    namespace,
    name: `bench-3step-${runTag}`,
    version: 1,
    blocks: [step(1), step(2), step(3)],
    created_at: new Date().toISOString(),
  });
  return { sequenceId: sequence.id, runTag };
}

async function runOne(index, { sequenceId, runTag }, deadline) {
  // The idempotency key makes a retried create (engine killed mid-request)
  // return the original instance instead of submitting a second workflow.
  const created = await withRetry(
    () =>
      call("POST", "/instances", {
        sequence_id: sequenceId,
        tenant_id: tenant,
        namespace,
        context: { data: { index: String(index) } },
        idempotency_key: `bench-${runTag}-${index}`,
      }),
    { deadline, label: "create instance" },
  );
  for (;;) {
    if (Date.now() > deadline) throw new Error("instance did not finish before the driver deadline");
    await sleep(pollMs);
    let instance;
    try {
      instance = await call("GET", `/instances/${created.id}`);
    } catch (error) {
      recordError(error); // engine unavailable during the crash window; keep polling
      continue;
    }
    if (["completed", "failed", "cancelled"].includes(instance.state)) {
      return {
        id: created.id,
        status: instance.state === "completed" ? "completed" : "failed",
        completed_at_ms: Date.now(),
        server_started_at_ms: Date.parse(instance.created_at),
        server_closed_at_ms: Date.parse(instance.updated_at),
      };
    }
  }
}

await main(runOne, { setup });
