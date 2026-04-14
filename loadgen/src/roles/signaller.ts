/**
 * Signaller: picks random alive instances and sends signals.
 *
 * Per tick (signalRate Hz):
 *   - sample a running/waiting instance
 *   - dice:
 *       40% → pause, then schedule resume 3-8s later
 *       30% → update_context (merge a field)
 *       20% → cancel
 *       10% → custom signal "approval_result" with random yes/no
 *         (the approval-flow template listens for this)
 */

import type { LoadgenConfig } from "../config.ts";
import type { Orch8Client } from "../client.ts";
import type { Rng } from "../lib/rng.ts";
import type { Counters } from "../lib/stats.ts";

import { TokenBucket, sleep } from "../lib/clock.ts";
import { ApiError } from "../client.ts";
import { isShuttingDown } from "../lib/shutdown.ts";

export interface SignallerDeps {
  client: Orch8Client;
  cfg: LoadgenConfig;
  rng: Rng;
  counters: Counters;
  alive: Set<string>;
}

export async function runSignaller(deps: SignallerDeps): Promise<void> {
  const { cfg } = deps;
  if (cfg.signalRate <= 0) return;
  const bucket = new TokenBucket(cfg.signalRate);

  while (!isShuttingDown()) {
    await bucket.acquire();
    if (isShuttingDown()) break;
    await tick(deps);
  }
}

async function tick(deps: SignallerDeps): Promise<void> {
  const { client, rng, counters, alive } = deps;
  if (alive.size === 0) return;

  // Pick a random alive id. Sets aren't indexable; convert lazily.
  const ids = [...alive];
  const id = ids[rng.int(0, ids.length - 1)]!;

  const roll = rng.next();
  try {
    if (roll < 0.4) {
      await client.sendSignal(id, "pause", { reason: "loadgen pause" });
      counters.signalsSent++;
      const resumeIn = rng.int(3000, 8000);
      // Fire and forget the resume.
      void scheduleResume(client, id, resumeIn, counters);
    } else if (roll < 0.7) {
      await client.updateContext(id, {
        loadgen_tick: Date.now(),
        loadgen_field: rng.pick(["alpha", "beta", "gamma"]),
      });
      counters.signalsSent++;
    } else if (roll < 0.9) {
      await client.sendSignal(id, "cancel", { reason: "loadgen cancel" });
      counters.signalsSent++;
    } else {
      await client.sendSignal(id, "approval_result", {
        approved: rng.chance(0.7) ? "yes" : "no",
      });
      counters.signalsSent++;
    }
  } catch (err) {
    // 404/409 are expected: instance may have finished between sample and send.
    if (err instanceof ApiError) {
      if (err.status === 404 || err.status === 409 || err.status === 400) {
        // Likely terminal — drop from alive set.
        alive.delete(id);
        return;
      }
      if (err.status >= 500) counters.errors5xx++;
    } else {
      counters.errorsConn++;
    }
  }
}

async function scheduleResume(
  client: Orch8Client,
  id: string,
  delayMs: number,
  counters: Counters,
): Promise<void> {
  await sleep(delayMs);
  if (isShuttingDown()) return;
  try {
    await client.sendSignal(id, "resume", { reason: "loadgen resume" });
    counters.signalsSent++;
  } catch {
    // Ignore — instance may have terminated while paused.
  }
}
