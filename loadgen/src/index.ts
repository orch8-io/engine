#!/usr/bin/env node
/**
 * orch8-loadgen CLI entrypoint.
 *
 * Spawns five cooperating role loops:
 *   1. bootstrap          — one-shot: register sequences + cron triggers
 *   2. producer           — create instances at `rate` Hz
 *   3. worker pool        — simulate external worker execution
 *   4. signaller          — send pause/resume/cancel/context signals
 *   5. alive poller       — keep the concurrency budget accurate
 *
 * Usage:
 *   npx orch8-loadgen --preset=steady
 *   npx orch8-loadgen --preset=stress --duration=600
 *   LOADGEN_ENABLE_LLM=1 npx orch8-loadgen --preset=light
 */

import { parseArgs, renderConfig } from "./config.ts";
import { Orch8Client } from "./client.ts";
import { Rng } from "./lib/rng.ts";
import { Counters, startPrinter } from "./lib/stats.ts";
import {
  installSignalHandlers,
  isShuttingDown,
  onShutdown,
  requestShutdown,
} from "./lib/shutdown.ts";
import { ALL_TEMPLATES, uniqueHandlers } from "./catalog/index.ts";
import type { Template } from "./catalog/index.ts";

import { bootstrap } from "./roles/bootstrapper.ts";
import { runProducer } from "./roles/producer.ts";
import { runWorkerPool } from "./roles/worker.ts";
import { runSignaller } from "./roles/signaller.ts";
import { runAlivePoller } from "./roles/alivePoller.ts";
import { sleep } from "./lib/clock.ts";

async function main(): Promise<void> {
  installSignalHandlers();
  const cfg = parseArgs(process.argv.slice(2));
  process.stderr.write(`[loadgen] ${renderConfig(cfg)}\n`);

  const client = new Orch8Client({ baseUrl: cfg.baseUrl, timeoutMs: 15_000 });
  const rng = new Rng(cfg.seed);
  const counters = new Counters();

  // Wait for engine to become ready (up to ~30s) before doing anything else.
  const ready = await waitReady(client, 30);
  if (!ready) {
    process.stderr.write(`[loadgen] engine not ready at ${cfg.baseUrl}\n`);
    process.exit(1);
  }

  // Filter LLM template based on flag.
  const templates: Template[] = ALL_TEMPLATES.map((t) =>
    t.name === "llm-chain"
      ? { ...t, weight: cfg.enableLlm ? 1 : 0 }
      : { ...t },
  ).filter((t) => t.weight > 0);

  const handlers = uniqueHandlers(templates);
  process.stderr.write(
    `[loadgen] templates=${templates.length} handlers=${handlers.length}\n`,
  );

  // Bootstrap sequences + cron triggers.
  const { byName, cronsCreated } = await bootstrap(client, cfg, rng, templates);
  process.stderr.write(
    `[loadgen] bootstrap: sequences=${byName.size} crons=${cronsCreated}\n`,
  );

  // Shared state.
  const alive = new Set<string>();
  const aliveTemplates = new Map<string, string>();
  const aliveSpawnTimes = new Map<string, number>();

  // Start periodic stats printer.
  startPrinter(counters, 5000);

  // Schedule duration shutdown.
  if (cfg.duration != null && cfg.duration > 0) {
    setTimeout(() => requestShutdown("duration reached"), cfg.duration * 1000)
      .unref();
  }

  // Spawn role loops. Each role resolves only when shutdown is requested.
  const rolePromises = [
    runProducer({ client, cfg, rng, counters, templates, sequences: byName, alive, aliveTemplates, aliveSpawnTimes }),
    runWorkerPool({ client, cfg, rng, counters, handlers }),
    runSignaller({ client, cfg, rng, counters, alive }),
    runAlivePoller({ client, counters, alive, aliveTemplates, aliveSpawnTimes }),
  ];

  // Surface any unhandled role failure by shutting down the whole process.
  const settled = await Promise.allSettled(rolePromises);
  onShutdown(() => {});

  // Final snapshot.
  await sleep(100);
  process.stderr.write(`[loadgen] final ${counters.snapshot()}\n`);
  const tmplSnap = counters.templateSnapshot();
  if (tmplSnap) process.stderr.write(tmplSnap + "\n");

  const firstRejection = settled.find((r) => r.status === "rejected");
  if (firstRejection && firstRejection.status === "rejected") {
    process.stderr.write(
      `[loadgen] role error: ${String(firstRejection.reason)}\n`,
    );
    process.exit(2);
  }
}

async function waitReady(client: Orch8Client, timeoutSec: number): Promise<boolean> {
  const deadline = Date.now() + timeoutSec * 1000;
  while (Date.now() < deadline) {
    if (isShuttingDown()) return false;
    if (await client.healthReady()) return true;
    await sleep(500);
  }
  return false;
}

main().catch((err) => {
  process.stderr.write(`[loadgen] fatal: ${err?.stack ?? err}\n`);
  process.exit(1);
});
