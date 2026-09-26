// Inngest benchmark driver: sends N `bench/start` events (bounded in flight)
// and polls each event's runs until the run reaches a terminal status.
// UNVERIFIED: uses the documented REST endpoints `POST /e/{eventKey}` and
// `GET /v1/events/{eventId}/runs` (Bearer signing key) and the run fields
// `status`, `run_started_at`, `ended_at`. Confirm them on the self-hosted
// release you benchmark; if they differ, latency_source falls back to driver.
import { main, recordError, sleep, withRetry } from "../../lib/driver-common.mjs";

const base = process.env.INNGEST_URL ?? "http://127.0.0.1:8288";
const eventKey = "0000000000000000000000000000000000000000000000000000000000000001";
const signingKey = "0000000000000000000000000000000000000000000000000000000000000002";
const pollMs = Number(process.env.BENCH_POLL_MS ?? 100);

async function json(response, label) {
  const text = await response.text();
  if (!response.ok) throw new Error(`${label} -> ${response.status} ${text.slice(0, 200)}`);
  return text ? JSON.parse(text) : {};
}

async function setup() {
  return { runTag: Date.now() };
}

async function runOne(index, { runTag }, deadline) {
  const key = `bench-${runTag}-${index}`;
  const sent = await withRetry(
    async () =>
      json(
        await fetch(`${base}/e/${eventKey}`, {
          method: "POST",
          headers: { "content-type": "application/json" },
          // `id` deduplicates a retried send after an Inngest crash.
          body: JSON.stringify({ name: "bench/start", id: key, data: { key } }),
        }),
        "send event",
      ),
    { deadline, label: "send event" },
  );
  const eventId = sent.ids?.[0];
  if (!eventId) throw new Error(`send event returned no id: ${JSON.stringify(sent)}`);
  for (;;) {
    if (Date.now() > deadline) throw new Error("run did not finish before the driver deadline");
    await sleep(pollMs);
    let runs;
    try {
      runs = await json(
        await fetch(`${base}/v1/events/${eventId}/runs`, { headers: { authorization: `Bearer ${signingKey}` } }),
        "get runs",
      );
    } catch (error) {
      recordError(error);
      continue;
    }
    const run = runs.data?.[0];
    const status = String(run?.status ?? "").toLowerCase();
    if (["completed", "failed", "cancelled"].includes(status)) {
      return {
        id: key,
        status: status === "completed" ? "completed" : "failed",
        completed_at_ms: Date.now(),
        server_started_at_ms: run.run_started_at ? Date.parse(run.run_started_at) : undefined,
        server_closed_at_ms: run.ended_at ? Date.parse(run.ended_at) : undefined,
      };
    }
  }
}

await main(runOne, { setup });
