/**
 * Producer: token-bucketed loop that spawns instances.
 *
 * Loop shape:
 *   while not shutting down:
 *     await token                     // rate limit
 *     if alive.size >= concurrency:   // concurrency limit
 *       await short sleep
 *       continue
 *     pick template (weighted)
 *     pick tenant/namespace (weighted)
 *     POST /instances
 *     on success: add id to alive set
 *     on error:   bump counters, continue
 */

import type { LoadgenConfig } from "../config.ts";
import type { Orch8Client } from "../client.ts";
import type { Rng } from "../lib/rng.ts";
import type { Counters } from "../lib/stats.ts";
import type { Template } from "../catalog/index.ts";

import { TokenBucket, sleep } from "../lib/clock.ts";
import { ApiError } from "../client.ts";
import { isShuttingDown } from "../lib/shutdown.ts";
import { seqKey } from "./bootstrapper.ts";
import type { SequenceDef } from "../types.ts";

/** Tenant distribution — 50/30/20 matches the catalog's three-tenant default. */
const TENANT_WEIGHTS = [0.5, 0.3, 0.2];
/** Namespace distribution — 70/30 prod/staging. */
const NS_WEIGHTS = [0.7, 0.3];

export interface ProducerDeps {
  client: Orch8Client;
  cfg: LoadgenConfig;
  rng: Rng;
  counters: Counters;
  templates: readonly Template[];
  sequences: Map<string, SequenceDef>;
  alive: Set<string>;
  aliveTemplates: Map<string, string>; // instance id → template name
  aliveSpawnTimes: Map<string, number>; // instance id → spawn timestamp
}

export async function runProducer(deps: ProducerDeps): Promise<void> {
  const { client, cfg, rng, counters, templates, sequences, alive, aliveTemplates, aliveSpawnTimes } = deps;
  const bucket = new TokenBucket(cfg.rate);

  const weights = templates.map((t) => t.weight);
  const tenantWeights = cfg.tenants.map((_, i) => TENANT_WEIGHTS[i] ?? 0.1);
  const nsWeights = cfg.namespaces.map((_, i) => NS_WEIGHTS[i] ?? 0.1);

  while (!isShuttingDown()) {
    await bucket.acquire();
    if (isShuttingDown()) break;

    if (alive.size >= cfg.concurrency) {
      // Don't just loop — yield for a bit so other roles get the event loop.
      await sleep(50);
      continue;
    }

    const tenant = rng.weighted(cfg.tenants, tenantWeights);
    const namespace = rng.weighted(cfg.namespaces, nsWeights);
    const template = rng.weighted(templates, weights);
    const seq = sequences.get(seqKey(tenant, namespace, template.name));
    if (!seq) {
      // Bootstrap didn't register — shouldn't happen, count and skip.
      counters.spawnFailed++;
      continue;
    }

    const context = template.buildContext(rng);
    const started = Date.now();
    try {
      const resp = await client.createInstance({
        sequence_id: seq.id,
        tenant_id: tenant,
        namespace,
        priority: rng.weighted(
          ["Low", "Normal", "High", "Critical"],
          [1, 6, 2, 0.3],
        ) as "Low" | "Normal" | "High" | "Critical",
        context,
      });
      counters.spawned++;
      counters.recordLatency(Date.now() - started);
      counters.templateSpawned(template.name);
      alive.add(resp.id);
      aliveTemplates.set(resp.id, template.name);
      aliveSpawnTimes.set(resp.id, started);
    } catch (err) {
      counters.spawnFailed++;
      if (err instanceof ApiError) {
        if (err.status >= 500) counters.errors5xx++;
      } else {
        counters.errorsConn++;
      }
      // Tiny backoff on error so we don't spin.
      await sleep(100);
    }
  }
}
