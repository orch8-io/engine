/**
 * Alive poller: every N seconds, walk the `alive` set in chunks and drop
 * instances that have reached a terminal state (completed/failed/cancelled).
 *
 * The producer uses `alive.size` for backpressure, so stale ids would
 * artificially cap throughput. Keeping this poller separate means the
 * hot-path producer doesn't block on detail lookups.
 */

import type { Orch8Client } from "../client.ts";
import type { Counters } from "../lib/stats.ts";

import { sleep } from "../lib/clock.ts";
import { ApiError } from "../client.ts";
import { isShuttingDown } from "../lib/shutdown.ts";

const TERMINAL = new Set(["completed", "failed", "cancelled"]);
const POLL_INTERVAL_MS = 5000;
const CHUNK = 25;

export interface AlivePollerDeps {
  client: Orch8Client;
  counters: Counters;
  alive: Set<string>;
  aliveTemplates: Map<string, string>; // instance id → template name
  aliveSpawnTimes: Map<string, number>; // instance id → spawn timestamp
}

export async function runAlivePoller(deps: AlivePollerDeps): Promise<void> {
  const { client, counters, alive, aliveTemplates, aliveSpawnTimes } = deps;
  while (!isShuttingDown()) {
    await sleep(POLL_INTERVAL_MS);
    if (isShuttingDown()) break;
    if (alive.size === 0) continue;

    const ids = [...alive];
    for (let i = 0; i < ids.length; i += CHUNK) {
      if (isShuttingDown()) break;
      const slice = ids.slice(i, i + CHUNK);
      await Promise.all(
        slice.map(async (id) => {
          try {
            const inst = await client.getInstance(id);
            if (TERMINAL.has(inst.state)) {
              alive.delete(id);
              const tmpl = aliveTemplates.get(id);
              const spawnedAt = aliveSpawnTimes.get(id);
              aliveTemplates.delete(id);
              aliveSpawnTimes.delete(id);
              if (spawnedAt) counters.recordE2eLatency(Date.now() - spawnedAt);
              if (inst.state === "completed") {
                counters.completed++;
                if (tmpl) counters.templateCompleted(tmpl);
              } else if (inst.state === "failed") {
                counters.failed++;
                if (tmpl) counters.templateFailed(tmpl);
              } else if (inst.state === "cancelled") {
                counters.cancelled++;
              }
            }
          } catch (err) {
            if (err instanceof ApiError && err.status === 404) {
              alive.delete(id);
              return;
            }
            // Transient — leave it in the set and try next tick.
          }
        }),
      );
    }
  }
}
