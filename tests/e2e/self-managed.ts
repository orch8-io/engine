/**
 * Single source of truth for **self-managed** E2E suites.
 *
 * A self-managed suite cannot share the one attach-mode server that
 * `run-e2e.ts` boots for the bulk of the suite. Two runners consume this
 * list, and they MUST agree — historically they were two hand-maintained
 * lists that drifted, leaving 17 suites excluded from the shared run *and*
 * absent from the standalone runner, so they ran nowhere in CI. Deriving
 * both from this module makes that drift impossible.
 *
 *   - `run-e2e.ts`      → EXCLUDES these from the shared-server run
 *                         (matched by basename).
 *   - `run-standalone.ts` → RUNS each one in spawn mode with its own
 *                         server + Postgres database.
 *
 * Paths are relative to this directory (tests/e2e). Keep them as full
 * paths — `run-standalone.ts` needs the location to spawn the file, and
 * `run-e2e.ts` only needs the basename, which it derives.
 *
 * Three reasons a suite belongs here:
 *
 *   1. Server-lifecycle: the suite stops/starts or swaps env mid-test
 *      (incompatible with attach-mode).
 *   2. Globally-scoped state — worker task queue, trigger definitions,
 *      signal inbox, worker dashboard. These tables aren't keyed by
 *      tenant_id, so rows from earlier suites leak in and break
 *      count-based assertions. Only a fresh server isolates them.
 *   3. Self-terminating: e.g. the cluster drain test kills its own
 *      server's node_id mid-run, so it needs a sacrificial own-server.
 */

export interface SelfManagedSuite {
  /** Path relative to tests/e2e, e.g. "features/triggers.test.ts". */
  file: string;
  /** Why it can't share the attach-mode server (one line, for the log). */
  reason: string;
}

export const SELF_MANAGED_SUITES: readonly SelfManagedSuite[] = [
  // Server-lifecycle: restarts mid-test.
  { file: "resilience/persistence_recovery.test.ts", reason: "restarts the server mid-test" },
  // Globally-scoped state (rule #2).
  { file: "features/triggers.test.ts", reason: "trigger defs are globally scoped" },
  { file: "signals/wait-signal.test.ts", reason: "signal inbox not keyed by tenant" },
  { file: "features/worker-dashboard.test.ts", reason: "worker queue not keyed by tenant" },
  { file: "features/workers.test.ts", reason: "worker queue not keyed by tenant" },
  { file: "resilience/worker_task_timeout.test.ts", reason: "worker queue not keyed by tenant" },
  { file: "resilience/worker_heartbeat_timeout.test.ts", reason: "worker queue not keyed by tenant" },
  { file: "resilience/retryable_false_open_circuit.test.ts", reason: "circuit-breaker state is server-global" },
  { file: "resilience/circuit_breaker_trip.test.ts", reason: "circuit-breaker state is server-global" },
  { file: "blocks/complex_patterns.test.ts", reason: "exercises globally-scoped state" },
  { file: "signals/signal_ordering.test.ts", reason: "signal inbox not keyed by tenant" },
  { file: "blocks/signal_during_finally.test.ts", reason: "signal inbox not keyed by tenant" },
  { file: "handlers/emit_event_deep_chains.test.ts", reason: "trigger defs leak across suites" },
  { file: "handlers/emit_event_invalid_target.test.ts", reason: "trigger defs leak across suites" },
  { file: "handlers/emit_event_dedupe_scope.test.ts", reason: "trigger defs leak across suites" },
  // Server-lifecycle / env-swap (rule #1).
  { file: "security/encryption_key_rotation.test.ts", reason: "swaps ORCH8_ENCRYPTION_KEY mid-test" },
  { file: "features/ab_split_determinism_restart.test.ts", reason: "restarts the server mid-test" },
  // Need their own server started with a specific env the shared server lacks.
  { file: "features/encryption_at_rest.test.ts", reason: "needs ORCH8_ENCRYPTION_KEY set at boot" },
  { file: "features/portable_continuity.test.ts", reason: "needs continuity encryption and local artifact storage" },
  { file: "security/credential_encryption_at_rest.test.ts", reason: "needs ORCH8_ENCRYPTION_KEY set at boot" },
  { file: "security/api_key_auth_enforcement.test.ts", reason: "needs ORCH8_API_KEY + ORCH8_REQUIRE_TENANT_HEADER" },
  { file: "handlers/activepieces_scenarios.test.ts", reason: "needs ORCH8_ACTIVEPIECES_URL → local mock" },
  { file: "handlers/artifacts.test.ts", reason: "needs ORCH8_ARTIFACT_BACKEND=local" },
  // Self-terminates (rule #3).
  { file: "features/cluster.test.ts", reason: "drains its own node_id, killing the server" },
];

/** Basename of a suite path (e.g. "features/triggers.test.ts" → "triggers.test.ts"). */
export function basenameOf(file: string): string {
  return file.split("/").pop() ?? file;
}

/** Set of basenames — used by run-e2e.ts to exclude these from the shared run. */
export const SELF_MANAGED_BASENAMES: ReadonlySet<string> = new Set(
  SELF_MANAGED_SUITES.map((s) => basenameOf(s.file)),
);
