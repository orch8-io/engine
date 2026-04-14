import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

/**
 * Parse a Prometheus text-exposition body into a map of `metric_name` => numeric sample.
 * Only keeps the first value for each name (multiple label-sets collapse).
 */
function parsePrometheus(body: string): Map<string, number> {
  const metrics = new Map<string, number>();
  const lines = body.split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    // Split on first whitespace.
    const ws = trimmed.search(/\s/);
    if (ws <= 0) continue;
    const labelled = trimmed.slice(0, ws);
    const valStr = trimmed.slice(ws).trim().split(/\s+/)[0];
    // Strip any label set `{...}` to get the bare metric name.
    const brace = labelled.indexOf("{");
    const name = brace === -1 ? labelled : labelled.slice(0, brace);
    const num = Number(valStr);
    if (!Number.isNaN(num) && !metrics.has(name)) {
      metrics.set(name, num);
    }
  }
  return metrics;
}

describe("Health + Metrics Endpoints", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("/health/live returns 200", async () => {
    const res = await client.healthLive();
    // The endpoint returns JSON; just verify we got a 2xx and a parseable body.
    assert.ok(res, "healthLive should return a non-empty body");
  });

  it("/health/ready returns 200 when storage reachable", async () => {
    const res = await client.healthReady();
    assert.ok(res, "healthReady should return a non-empty body");
  });

  it("/metrics returns Prometheus text including known counter names", async () => {
    // First, run a couple of instances so counters are exercised.
    const seq = testSequence("metrics-warmup", [step("s1", "noop")]);
    await client.createSequence(seq);
    for (let i = 0; i < 3; i += 1) {
      const { id } = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: "test",
        namespace: "default",
      });
      await client.waitForState(id, "completed");
    }

    const body = await client.metrics();
    assert.equal(typeof body, "string", "metrics body must be a string");
    assert.ok(body.length > 0, "metrics body must not be empty");

    // Known metric names from orch8-engine/src/metrics.rs. At least some of
    // these must be present in the text body. We don't require every one —
    // some (like webhooks) may not register unless their paths run.
    const expectedOneOf = [
      "orch8_instances_claimed_total",
      "orch8_instances_completed_total",
      "orch8_steps_executed_total",
    ];
    const foundAny = expectedOneOf.filter((m) => body.includes(m));
    assert.ok(
      foundAny.length > 0,
      `metrics body should include at least one of ${JSON.stringify(expectedOneOf)}`
    );

    // Parse and verify numeric samples exist for the found names.
    const parsed = parsePrometheus(body);
    for (const name of foundAny) {
      const v = parsed.get(name);
      assert.ok(
        typeof v === "number" && !Number.isNaN(v),
        `metric ${name} should parse to a finite number`
      );
    }
  });
});
