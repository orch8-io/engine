/**
 * Worker pool: polls /workers/tasks/poll for each registered handler,
 * simulates execution with realistic outputs, completes or fails.
 *
 * Each "worker" is a long-lived async loop that cycles through handler
 * names (round-robin) and claims tasks one at a time. This avoids
 * having to run one loop per (worker, handler) pair.
 *
 * Simulation:
 *   - 80% complete, 12% fail-retryable, 8% fail-terminal
 *   - jitter 100..3000 ms per task; with 3% chance, 15..45 s long task
 *   - long tasks heartbeat every 5s
 *   - completions return handler-specific realistic payloads
 */

import type { LoadgenConfig } from "../config.ts";
import type { Orch8Client } from "../client.ts";
import type { Rng } from "../lib/rng.ts";
import type { Counters } from "../lib/stats.ts";

import { sleep } from "../lib/clock.ts";
import { ApiError } from "../client.ts";
import { isShuttingDown } from "../lib/shutdown.ts";

export interface WorkerDeps {
  client: Orch8Client;
  cfg: LoadgenConfig;
  rng: Rng;
  counters: Counters;
  handlers: readonly string[];
}

export async function runWorkerPool(deps: WorkerDeps): Promise<void> {
  const { cfg, handlers } = deps;
  if (handlers.length === 0) return;
  const tasks: Promise<void>[] = [];
  for (let i = 0; i < cfg.workers; i++) {
    const workerId = `loadgen-worker-${i}`;
    tasks.push(runWorker(workerId, deps));
  }
  await Promise.all(tasks);
}

async function runWorker(workerId: string, deps: WorkerDeps): Promise<void> {
  const { client, rng, counters, handlers } = deps;
  let idx = rng.int(0, handlers.length - 1);

  while (!isShuttingDown()) {
    const handler = handlers[idx % handlers.length]!;
    idx++;

    let claimed = false;
    try {
      counters.workerPolls++;
      const batch = await client.pollWorkerTasks(handler, workerId, 1);
      if (batch.length === 0) {
        // No work for this handler — short sleep if we cycled through all.
        if (idx % handlers.length === 0) await sleep(200);
        continue;
      }
      const task = batch[0]!;
      counters.tasksClaimed++;
      claimed = true;
      await executeTask(task.id, handler, task.params, workerId, deps);
    } catch (err) {
      if (!claimed) {
        if (err instanceof ApiError) {
          if (err.status >= 500) counters.errors5xx++;
        } else {
          counters.errorsConn++;
        }
      }
      await sleep(150);
    }
  }
}

async function executeTask(
  taskId: string,
  handler: string,
  params: unknown,
  workerId: string,
  deps: WorkerDeps,
): Promise<void> {
  const { client, rng, counters } = deps;

  // Long-running: 3% of tasks take 15-45s and require heartbeats.
  const isLong = rng.chance(0.03);
  const durationMs = isLong ? rng.int(15_000, 45_000) : rng.int(100, 3000);

  // Outcome roll.
  const roll = rng.next();
  const outcome: "ok" | "retry" | "dead" =
    roll < 0.8 ? "ok" : roll < 0.92 ? "retry" : "dead";

  const start = Date.now();
  let hbStop = false;
  const hbPromise = isLong
    ? (async () => {
        while (!hbStop && !isShuttingDown()) {
          await sleep(5000);
          if (hbStop) break;
          try {
            await client.heartbeatWorkerTask(taskId, workerId);
          } catch {
            // Heartbeat failures are non-fatal; engine will just mark
            // the task as stalled and recover it.
          }
        }
      })()
    : Promise.resolve();

  // Simulated work — just a sleep, but chunked so shutdown is responsive.
  while (Date.now() - start < durationMs) {
    if (isShuttingDown()) break;
    await sleep(Math.min(500, durationMs - (Date.now() - start)));
  }
  hbStop = true;
  await hbPromise;

  if (isShuttingDown()) return;

  try {
    if (outcome === "ok") {
      const output = buildRealisticOutput(handler, params, rng, durationMs);
      await client.completeWorkerTask(taskId, workerId, output);
      counters.tasksCompleted++;
    } else {
      const errorMsg = buildRealisticError(handler, outcome, rng);
      await client.failWorkerTask(
        taskId,
        workerId,
        errorMsg,
        outcome === "retry",
      );
      counters.tasksFailed++;
    }
  } catch (err) {
    if (err instanceof ApiError) {
      if (err.status >= 500) counters.errors5xx++;
    } else {
      counters.errorsConn++;
    }
  }
}

// ─── Realistic outputs per handler ──────────────────────────────────────────

function buildRealisticOutput(
  handler: string,
  params: unknown,
  rng: Rng,
  durationMs: number,
): Record<string, unknown> {
  const p = (params ?? {}) as Record<string, unknown>;

  switch (handler) {
    // order-fulfillment
    case "order_validate":
      return {
        valid: true,
        checks_passed: ["inventory", "fraud_score", "address"],
        fraud_score: rng.int(0, 30),
        validated_at: new Date().toISOString(),
      };
    case "order_charge":
      return {
        charge_id: `ch_${rng.uuid().slice(0, 12)}`,
        amount_cents: p.amount ?? rng.int(500, 50_000),
        currency: "USD",
        status: "succeeded",
        processor: rng.pick(["stripe", "adyen", "braintree"]),
      };
    case "order_reserve":
      return {
        reservation_id: `res_${rng.uuid().slice(0, 8)}`,
        warehouse: rng.pick(["us-east-1", "eu-central-1", "ap-southeast-1"]),
        expires_at: new Date(Date.now() + 3600_000).toISOString(),
        items_reserved: rng.int(1, 5),
      };
    case "order_ship":
      return {
        tracking_number: `1Z${rng.int(100000000, 999999999)}`,
        carrier: rng.pick(["ups", "fedex", "dhl", "usps"]),
        estimated_delivery: new Date(Date.now() + 86400_000 * rng.int(2, 7)).toISOString(),
        weight_kg: +(rng.int(100, 5000) / 100).toFixed(2),
      };
    case "order_notify":
      return {
        channel: rng.pick(["email", "sms", "push"]),
        message_id: `msg_${rng.uuid().slice(0, 10)}`,
        delivered: true,
        sent_at: new Date().toISOString(),
      };

    // approval-flow
    case "approval_kickoff":
      return {
        ticket_url: `https://internal.example.com/approvals/${rng.uuid().slice(0, 8)}`,
        notified_reviewers: rng.int(1, 3),
        created_at: new Date().toISOString(),
      };
    case "approval_ship":
      return {
        approved_by: rng.pick(["manager_a", "manager_b", "vp_ops"]),
        approved_at: new Date().toISOString(),
        sla_met: durationMs < 5000,
      };
    case "approval_reject":
      return {
        rejected_by: rng.pick(["compliance", "budget_owner", "manager_a"]),
        reason: rng.pick(["budget_exceeded", "policy_violation", "duplicate_request", "insufficient_justification"]),
        rejected_at: new Date().toISOString(),
      };
    case "approval_notify":
      return {
        notification_type: rng.pick(["slack", "email", "teams"]),
        recipients: rng.int(1, 4),
        sent: true,
      };

    // data-pipeline
    case "pipeline_transform":
      return {
        input_bytes: rng.int(1024, 1048576),
        output_bytes: rng.int(512, 524288),
        records_processed: rng.int(100, 10000),
        transform_type: rng.pick(["normalize", "deduplicate", "enrich", "filter", "aggregate"]),
        duration_ms: durationMs,
      };
    case "pipeline_summarize":
      return {
        total_records: rng.int(1000, 100000),
        total_bytes: rng.int(1048576, 104857600),
        partitions_written: rng.int(1, 16),
        output_path: `s3://data-lake/output/${new Date().toISOString().slice(0, 10)}/${rng.uuid().slice(0, 8)}.parquet`,
      };

    // eta-escalation
    case "eta_slow_task":
      return {
        processed: true,
        retries_used: rng.int(0, 2),
        response_time_ms: durationMs,
        upstream_status: rng.pick([200, 200, 200, 201, 202]),
      };
    case "eta_escalate":
      return {
        escalation_id: `esc_${rng.uuid().slice(0, 8)}`,
        escalated_to: rng.pick(["oncall-primary", "oncall-secondary", "team-lead"]),
        pager_sent: rng.chance(0.6),
        severity: rng.pick(["P1", "P2", "P3"]),
      };
    case "eta_finish":
      return {
        resolved: true,
        resolution: rng.pick(["auto_recovered", "manual_intervention", "timeout_acceptable", "fallback_used"]),
        total_duration_ms: durationMs + rng.int(1000, 10000),
      };

    // race-fetch
    case "race_fetch_primary":
      return {
        source: "primary",
        latency_ms: durationMs,
        cache_hit: false,
        payload_bytes: rng.int(256, 65536),
        status_code: 200,
      };
    case "race_fetch_mirror":
      return {
        source: "mirror",
        latency_ms: durationMs,
        cache_hit: false,
        payload_bytes: rng.int(256, 65536),
        region: rng.pick(["eu-west-1", "us-west-2"]),
      };
    case "race_fetch_cache":
      return {
        source: "cache",
        latency_ms: Math.min(durationMs, rng.int(5, 50)),
        cache_hit: true,
        ttl_remaining_ms: rng.int(1000, 60000),
      };
    case "race_converge":
      return {
        winner: rng.pick(["primary", "mirror", "cache"]),
        total_latency_ms: durationMs,
        fallbacks_tried: rng.int(0, 2),
      };

    // loop-aggregator
    case "loop_accumulate":
      return {
        batch_size: rng.int(10, 100),
        accumulated: rng.int(100, 10000),
        iteration_ms: durationMs,
      };
    case "loop_finalize":
      return {
        total_accumulated: rng.int(1000, 50000),
        iterations_run: rng.int(5, 15),
        output_key: `agg_${rng.uuid().slice(0, 8)}`,
      };

    // nested-subsequence (uses order-fulfillment handlers + its own)
    default:
      return {
        handler,
        status: "completed",
        duration_ms: durationMs,
        processed_at: new Date().toISOString(),
      };
  }
}

// ─── Realistic error messages ───────────────────────────────────────────────

function buildRealisticError(
  handler: string,
  type: "retry" | "dead",
  rng: Rng,
): string {
  if (type === "retry") {
    return rng.pick([
      "connection reset by peer",
      "upstream timeout after 5000ms",
      "503 Service Unavailable: rate limit exceeded",
      "ECONNREFUSED: target service temporarily unavailable",
      "socket hang up during response",
      "TLS handshake timeout",
      `${handler}: upstream returned 502 Bad Gateway`,
      "request cancelled: context deadline exceeded",
    ]);
  }
  // Permanent failures
  return rng.pick([
    "validation failed: required field 'id' is null",
    "400 Bad Request: malformed payload",
    "insufficient funds: charge declined",
    "resource not found: entity was deleted",
    `${handler}: invariant violation — cannot process in current state`,
    "schema mismatch: expected v2, got v1",
    "permission denied: token expired and non-refreshable",
    "duplicate key: idempotency constraint violated",
  ]);
}
