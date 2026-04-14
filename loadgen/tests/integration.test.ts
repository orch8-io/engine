/**
 * Integration smoke test.
 *
 * Assumes an orch8 engine is already running at ORCH8_E2E_BASE_URL
 * (or localhost:18080). Spawns the loadgen in `light` preset, lets it
 * run for ~12 seconds, then asserts we produced some work.
 *
 * This test is intentionally lenient: the only hard failures are
 *   (a) engine not reachable, or
 *   (b) zero instances spawned / workers never polled.
 * The preset numbers can be tuned without breaking this test.
 */

import { test } from "node:test";
import assert from "node:assert/strict";
import { setTimeout as delay } from "node:timers/promises";

import { parseArgs } from "../src/config.ts";
import { Orch8Client } from "../src/client.ts";
import { Rng } from "../src/lib/rng.ts";
import { Counters } from "../src/lib/stats.ts";
import {
  installSignalHandlers,
  requestShutdown,
} from "../src/lib/shutdown.ts";
import { ALL_TEMPLATES, uniqueHandlers } from "../src/catalog/index.ts";
import { bootstrap } from "../src/roles/bootstrapper.ts";
import { runProducer } from "../src/roles/producer.ts";
import { runWorkerPool } from "../src/roles/worker.ts";
import { runSignaller } from "../src/roles/signaller.ts";
import { runAlivePoller } from "../src/roles/alivePoller.ts";

test("loadgen smoke: ~12s in light preset produces work", async (t) => {
  installSignalHandlers();
  const cfg = parseArgs([
    "--preset=light",
    "--duration=12",
    "--seed=1",
    `--tenants=loadgen-test-a,loadgen-test-b`,
    `--namespaces=prod`,
  ]);

  const client = new Orch8Client({ baseUrl: cfg.baseUrl, timeoutMs: 10_000 });
  const ready = await client.healthReady();
  if (!ready) {
    t.skip(`engine not reachable at ${cfg.baseUrl}; skipping`);
    return;
  }

  const rng = new Rng(cfg.seed);
  const counters = new Counters();
  const templates = ALL_TEMPLATES
    .filter((t) => t.name !== "llm-chain")
    .map((t) => ({ ...t }));
  const handlers = uniqueHandlers(templates);

  const { byName } = await bootstrap(client, cfg, rng, templates);
  const alive = new Set<string>();

  // Let all roles run for the full duration, then force shutdown.
  setTimeout(() => requestShutdown("test duration reached"), 12_000).unref();

  await Promise.allSettled([
    runProducer({ client, cfg, rng, counters, templates, sequences: byName, alive }),
    runWorkerPool({ client, cfg, rng, counters, handlers }),
    runSignaller({ client, cfg, rng, counters, alive }),
    runAlivePoller({ client, counters, alive }),
  ]);

  // Give any in-flight HTTP a moment to settle.
  await delay(200);

  assert.ok(counters.spawned >= 5, `expected >=5 spawned, got ${counters.spawned}`);
  assert.ok(counters.workerPolls >= 1, `expected >=1 worker poll, got ${counters.workerPolls}`);
  assert.ok(byName.size >= templates.length, `bootstrap registered only ${byName.size} sequences`);
});
