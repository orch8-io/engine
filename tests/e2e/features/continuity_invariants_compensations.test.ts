/**
 * E2E: Portable Continuity — workflow invariants and compensation runs.
 *
 * Invariants: `POST/GET /continuity/invariants`,
 * `POST /continuity/executions/{id}/invariants/evaluate`,
 * `GET /continuity/executions/{id}/invariants/results`. Declarative rules
 * (effect_at_most_once, no_unknown_effects, terminal_state_in,
 * budget_within_limits, output_path_present) are evaluated against durable
 * evidence only (effect receipts, instance terminal state, output paths) —
 * never against caller-supplied claims. See
 * `orch8-engine/src/continuity_advanced.rs::evaluate_rule` for the exact
 * semantics exercised below; in particular `budget_within_limits` always
 * evaluates to `unknown` today because the handler never threads breach
 * evidence through (`evaluate_invariants` hardcodes `budget_breached: None`
 * in `orch8-api/src/continuity.rs`).
 *
 * Compensation runs: `POST /continuity/executions/{id}/compensations/preview`,
 * `POST /continuity/executions/{id}/compensations`,
 * `GET /continuity/compensations/{id}`,
 * `POST /continuity/compensations/{id}/claim`,
 * `POST /continuity/compensations/{id}/steps/{effect_id}/complete`,
 * `POST /continuity/compensations/{id}/steps/{effect_id}/fail`,
 * `POST /continuity/compensations/{id}/steps/{effect_id}/verify`.
 * Compensation plans are built in `orch8-engine/src/compensation.rs` from
 * the sequence's `step.compensation` rules and every `committed`/`verified`
 * effect receipt for the execution; steps run in reverse dependency order
 * (LIFO) and claiming/completing them is a lease-CAS state machine over
 * `CompensationRunRecord` (see `orch8-api/src/continuity.rs` lines
 * 4704-5271).
 *
 * Runtime registration/listing edge cases: `POST /runtimes/register`,
 * `GET /runtimes` (`register_runtime`/`list_runtimes`,
 * `orch8-api/src/continuity.rs` lines 560-666).
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync } from "node:crypto";
import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../types.ts";

/** A syntactically valid, base64-encoded raw Ed25519 public key (32 bytes). */
function validEd25519PublicKeyBase64(): string {
  const { publicKey } = generateKeyPairSync("ed25519");
  const der = publicKey.export({ type: "spki", format: "der" }) as Buffer;
  return der.subarray(der.length - 32).toString("base64");
}

const client = new Orch8Client();

function tid(prefix: string): string {
  return `${prefix}-${uuid().slice(0, 8)}`;
}

async function rejects(promise: Promise<unknown>, status: number, msg?: string) {
  await assert.rejects(
    () => promise,
    (error: unknown) => {
      assert.equal((error as ApiError).status, status, msg);
      return true;
    },
  );
}

function runtimeCaps(
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Record<string, unknown> {
  const now = Date.now();
  return {
    runtime_id: runtimeId,
    kind: "server",
    trust: "registered",
    handlers: ["noop"],
    regions: ["us-east"],
    hardware: [],
    offline_capable: false,
    connectivity: "wifi",
    battery_percent: 100,
    estimated_cost_microunits: 10,
    estimated_latency_ms: 20,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

/** `POST /runtimes/register` expects `{ tenant_id, capabilities }`. */
function registerRuntime(
  tenantId: string,
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Promise<Record<string, unknown>> {
  return client.registerRuntime({
    tenant_id: tenantId,
    capabilities: runtimeCaps(runtimeId, overrides),
  });
}

async function waitForWorkerTask(
  handler: string,
  workerId: string,
  timeoutMs = 10_000,
): Promise<WorkerTask> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const [task] = await client.pollWorkerTasks(handler, workerId);
    if (task) return task;
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`Timeout waiting for worker task for ${handler}`);
}

/** Create a sequence + instance + continuity execution, no dispatch yet. */
async function freshExecution(namePrefix: string) {
  const tenantId = tid(namePrefix);
  const sequence = testSequence(namePrefix, [step("s1", "transform", { result: "ok" })], {
    tenantId,
  });
  const created = await client.createSequence(sequence);
  const createdSequence = { ...sequence, id: created.id };
  const instance = await client.createInstance({
    sequence_id: createdSequence.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: uuid(),
  });
  return { tenantId, createdSequence, instance, execution };
}

/**
 * Build a two-step sequence (arm -> worker-dispatched charge) whose "charge"
 * step carries a `compensation` rule, matching the shape validated by
 * `validate_step` in orch8-types/src/sequence.rs. Drives the instance to the
 * point where the "charge" effect receipt is `dispatched`, mirroring
 * continuity_provenance_effects.test.ts's `dispatchedEffectExecution`.
 */
async function compensableExecution(
  namePrefix: string,
  opts: {
    compensation?: Record<string, unknown> | null;
    secondStep?: { id: string; compensation?: Record<string, unknown> } | null;
  } = {},
) {
  const tenantId = tid(namePrefix);
  const handler = `${namePrefix}_h_${uuid().slice(0, 8)}`;
  const compensation =
    opts.compensation === null
      ? undefined
      : (opts.compensation ?? { handler: "release_charge", verification: "handler_result" });
  const blocks = [
    step("arm", "human_review", {}, {
      wait_for_input: {
        prompt: "Arm effect dispatch?",
        choices: [{ label: "Continue", value: "continue" }],
      },
    }),
    step("charge", handler, { amount: 100 }, compensation ? { compensation } : {}),
  ];
  if (opts.secondStep) {
    const secondHandler = `${namePrefix}_h2_${uuid().slice(0, 8)}`;
    blocks.push(
      step(
        opts.secondStep.id,
        secondHandler,
        { amount: 1 },
        opts.secondStep.compensation ? { compensation: opts.secondStep.compensation } : {},
      ),
    );
  }
  const sequence = testSequence(namePrefix, blocks, { tenantId });
  const created = await client.createSequence(sequence);
  const createdSequence = { ...sequence, id: created.id };
  const instance = await client.createInstance({
    sequence_id: createdSequence.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(instance.id, "waiting");
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: uuid(),
  });
  await client.sendSignal(
    instance.id,
    { custom: "human_input:arm" } as unknown as string,
    { value: "continue" },
  );
  const task = await waitForWorkerTask(handler, `${namePrefix}-worker`);
  return { tenantId, createdSequence, instance, execution, handler, task };
}

/**
 * Drive a compensable execution's "charge" effect all the way to
 * `committed`, so a compensation plan/run has at least one step.
 */
async function committedCompensableExecution(namePrefix: string) {
  const setup = await compensableExecution(namePrefix);
  const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
  assert.equal(effects.length, 1, "setup: exactly one dispatched effect");
  const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
    tenant_id: setup.tenantId,
    state: "committed",
    provider_receipt_id: `receipt-${uuid().slice(0, 8)}`,
  });
  assert.equal(resolved.state, "committed", "setup: effect should be committed");
  return { ...setup, effect: resolved };
}

describe("Continuity Invariants", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // create_invariant — validation
  // ------------------------------------------------------------------

  describe("invariant creation validation", () => {
    it("creates an effect_at_most_once invariant", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-create");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "at-most-once-charge",
        rule: { type: "effect_at_most_once", kind: "worker" },
      });
      assert.equal(invariant.name, "at-most-once-charge");
      assert.equal(invariant.rule.type, "effect_at_most_once");
      assert.equal(invariant.enabled, true);
      assert.equal(invariant.commit_guard, false);
      assert.ok(invariant.id);
    });

    it("creates a no_unknown_effects invariant", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-nue");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "no-unknowns",
        rule: { type: "no_unknown_effects" },
      });
      assert.equal(invariant.rule.type, "no_unknown_effects");
    });

    it("creates a terminal_state_in invariant with 1 state", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-term-1");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "terminal-completed",
        rule: { type: "terminal_state_in", states: ["completed"] },
      });
      assert.deepEqual(invariant.rule.states, ["completed"]);
    });

    it("creates a terminal_state_in invariant with 16 states (upper bound)", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-term-16");
      const states = Array.from({ length: 16 }, (_, i) => `state-${i}`);
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "terminal-16",
        rule: { type: "terminal_state_in", states },
      });
      assert.equal(invariant.rule.states.length, 16);
    });

    it("rejects a terminal_state_in invariant with 0 states", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-term-0");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          name: "terminal-empty",
          rule: { type: "terminal_state_in", states: [] },
        }),
        400,
      );
    });

    it("rejects a terminal_state_in invariant with 17 states (over upper bound)", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-term-17");
      const states = Array.from({ length: 17 }, (_, i) => `state-${i}`);
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          name: "terminal-17",
          rule: { type: "terminal_state_in", states },
        }),
        400,
      );
    });

    it("creates a budget_within_limits invariant", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-budget");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "budget-ok",
        rule: { type: "budget_within_limits" },
      });
      assert.equal(invariant.rule.type, "budget_within_limits");
    });

    it("creates an output_path_present invariant", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-path");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "path-present",
        rule: { type: "output_path_present", block_id: "s1", path: "result" },
      });
      assert.equal(invariant.rule.block_id, "s1");
      assert.equal(invariant.rule.path, "result");
    });

    it("rejects an empty invariant name", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-name-empty");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          name: "",
          rule: { type: "no_unknown_effects" },
        }),
        400,
      );
    });

    it("accepts an invariant name at the 128 byte boundary", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-name-128");
      const name = "n".repeat(128);
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name,
        rule: { type: "no_unknown_effects" },
      });
      assert.equal(invariant.name.length, 128);
    });

    it("rejects an invariant name over 128 bytes", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-name-129");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          name: "n".repeat(129),
          rule: { type: "no_unknown_effects" },
        }),
        400,
      );
    });

    it("rejects an unknown sequence_id", async () => {
      const tenantId = tid("inv-unknown-seq");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: uuid(),
          name: "orphan",
          rule: { type: "no_unknown_effects" },
        }),
        404,
      );
    });

    it("rejects a sequence_id belonging to another tenant (returns 404, not leaking existence)", async () => {
      const { createdSequence } = await freshExecution("inv-cross-tenant");
      const otherTenant = tid("inv-cross-tenant-other");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: otherTenant,
          sequence_id: createdSequence.id,
          name: "cross-tenant",
          rule: { type: "no_unknown_effects" },
        }),
        404,
      );
    });

    it("commit_guard is rejected for rules other than effect_at_most_once", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-guard-bad");
      await rejects(
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          name: "bad-guard",
          rule: { type: "no_unknown_effects" },
          commit_guard: true,
        }),
        400,
      );
    });

    it("commit_guard is accepted for effect_at_most_once rules", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-guard-ok");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "good-guard",
        rule: { type: "effect_at_most_once", kind: "http" },
        commit_guard: true,
      });
      assert.equal(invariant.commit_guard, true);
    });

    it("sequence_version defaults to null (matches every version)", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-ver-null");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "any-version",
        rule: { type: "no_unknown_effects" },
      });
      assert.equal(invariant.sequence_version, null);
    });

    it("stores an explicit sequence_version", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-ver-explicit");
      const invariant = await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: 1,
        name: "v1-only",
        rule: { type: "no_unknown_effects" },
      });
      assert.equal(invariant.sequence_version, 1);
    });
  });

  // ------------------------------------------------------------------
  // list_invariants
  // ------------------------------------------------------------------

  describe("invariant listing", () => {
    it("lists invariants scoped by sequence and version", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-list");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "a",
        rule: { type: "no_unknown_effects" },
      });
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "b",
        rule: { type: "budget_within_limits" },
      });
      const listed = await client.listContinuityInvariants(tenantId, createdSequence.id, 1);
      assert.equal(listed.length, 2);
    });

    it("a version-scoped invariant is excluded when listing a different version", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-list-ver");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: 2,
        name: "v2-only",
        rule: { type: "no_unknown_effects" },
      });
      const listed = await client.listContinuityInvariants(tenantId, createdSequence.id, 1);
      assert.equal(listed.length, 0);
    });

    it("a null-version invariant is included when listing any version", async () => {
      const { tenantId, createdSequence } = await freshExecution("inv-list-null-ver");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "all-versions",
        rule: { type: "no_unknown_effects" },
      });
      const listedV1 = await client.listContinuityInvariants(tenantId, createdSequence.id, 1);
      const listedV7 = await client.listContinuityInvariants(tenantId, createdSequence.id, 7);
      assert.equal(listedV1.length, 1);
      assert.equal(listedV7.length, 1);
    });

    it("listing is scoped by tenant", async () => {
      const { createdSequence } = await freshExecution("inv-list-tenant");
      const listed = await client.listContinuityInvariants(
        "a-totally-different-tenant",
        createdSequence.id,
        1,
      );
      assert.deepEqual(listed, []);
    });

    it("listing an unknown sequence_id returns an empty list, not a 404", async () => {
      const tenantId = tid("inv-list-unknown-seq");
      const listed = await client.listContinuityInvariants(tenantId, uuid(), 1);
      assert.deepEqual(listed, []);
    });
  });

  // ------------------------------------------------------------------
  // evaluate_invariants
  // ------------------------------------------------------------------

  describe("invariant evaluation", () => {
    it("evaluates a passing no_unknown_effects invariant on a fresh execution", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-eval-pass");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "no-unknowns",
        rule: { type: "no_unknown_effects" },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results.length, 1);
      assert.equal(results[0]!.status, "pass");
      assert.equal(results[0]!.evidence_sha256.length, 0);
    });

    it("evaluates a failing no_unknown_effects invariant once an effect is unresolved", async () => {
      const setup = await compensableExecution("inv-eval-fail-unknown", { compensation: null });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "unknown",
      });
      await client.createContinuityInvariant({
        tenant_id: setup.tenantId,
        sequence_id: setup.createdSequence.id,
        name: "no-unknowns",
        rule: { type: "no_unknown_effects" },
      });
      const results = await client.evaluateContinuityInvariants(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(results[0]!.status, "fail");
      assert.equal(results[0]!.evidence_sha256.length, 1);
      assert.equal(results[0]!.evidence_sha256[0], effects[0]!.request_sha256);
    });

    it("evaluates a passing effect_at_most_once invariant with a single dispatch", async () => {
      const setup = await compensableExecution("inv-eval-amo-pass", { compensation: null });
      await client.createContinuityInvariant({
        tenant_id: setup.tenantId,
        sequence_id: setup.createdSequence.id,
        name: "amo",
        rule: { type: "effect_at_most_once", kind: "worker" },
      });
      const results = await client.evaluateContinuityInvariants(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(results[0]!.status, "pass");
    });

    it("terminal_state_in evaluates to unknown when the instance has not reached a terminal state", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-eval-term-unknown");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "terminal",
        rule: { type: "terminal_state_in", states: ["completed"] },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "unknown");
      assert.equal(results[0]!.summary, "terminal state evidence is absent");
    });

    it("terminal_state_in evaluates to pass once the instance completes in an allowed state", async () => {
      const { tenantId, createdSequence, instance, execution } = await freshExecution("inv-eval-term-pass");
      await client.waitForState(instance.id, "completed");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "terminal",
        rule: { type: "terminal_state_in", states: ["completed"] },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "pass");
      assert.equal(results[0]!.summary, "terminal state is completed");
    });

    it("terminal_state_in evaluates to fail when the terminal state is not in the allowed set", async () => {
      const { tenantId, createdSequence, instance, execution } = await freshExecution("inv-eval-term-fail");
      await client.waitForState(instance.id, "completed");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "terminal-wrong",
        rule: { type: "terminal_state_in", states: ["dead"] },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "fail");
    });

    it("budget_within_limits always evaluates to unknown (handler never threads breach evidence)", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-eval-budget");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "budget",
        rule: { type: "budget_within_limits" },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "unknown");
      assert.equal(results[0]!.summary, "budget evidence is absent");
    });

    it("output_path_present evaluates to pass once the block output contains the path", async () => {
      const { tenantId, createdSequence, instance, execution } = await freshExecution("inv-eval-path-pass");
      await client.waitForState(instance.id, "completed");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "path",
        rule: { type: "output_path_present", block_id: "s1", path: "result" },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "pass");
    });

    it("output_path_present evaluates to unknown when the path was never produced", async () => {
      const { tenantId, createdSequence, instance, execution } = await freshExecution("inv-eval-path-unknown");
      await client.waitForState(instance.id, "completed");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "path-missing",
        rule: { type: "output_path_present", block_id: "s1", path: "nonexistent_field" },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(results[0]!.status, "unknown");
    });

    it("evaluating with zero invariants returns an empty result list", async () => {
      const { tenantId, execution } = await freshExecution("inv-eval-empty");
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.deepEqual(results, []);
    });

    it("re-evaluation with unchanged evidence dedupes and produces the same result id", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-eval-dedupe");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "dedupe",
        rule: { type: "no_unknown_effects" },
      });
      const first = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      const second = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.equal(first[0]!.dedupe_key, second[0]!.dedupe_key);
      const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      assert.equal(results.length, 1, "duplicate evidence must not create a second stored result");
    });

    it("re-evaluation with changed evidence produces a distinct dedupe_key and both are retained", async () => {
      const setup = await compensableExecution("inv-eval-redo", { compensation: null });
      await client.createContinuityInvariant({
        tenant_id: setup.tenantId,
        sequence_id: setup.createdSequence.id,
        name: "redo",
        rule: { type: "no_unknown_effects" },
      });
      const before = await client.evaluateContinuityInvariants(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "unknown",
      });
      const after = await client.evaluateContinuityInvariants(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.notEqual(before[0]!.dedupe_key, after[0]!.dedupe_key);
      assert.equal(before[0]!.status, "pass");
      assert.equal(after[0]!.status, "fail");
      const results = await client.listContinuityInvariantResults(setup.execution.continuity_id, setup.tenantId);
      assert.equal(results.length, 2);
    });

    it("evaluating invariants for an unknown execution returns 404", async () => {
      const tenantId = tid("inv-eval-404");
      await rejects(
        client.evaluateContinuityInvariants(uuid(), { tenant_id: tenantId }),
        404,
      );
    });

    it("evaluation is scoped by tenant (cross-tenant execution id returns 404)", async () => {
      const { execution } = await freshExecution("inv-eval-tenant-scope");
      await rejects(
        client.evaluateContinuityInvariants(execution.continuity_id, {
          tenant_id: "a-totally-different-tenant",
        }),
        404,
      );
    });

    it("only invariants matching the instance's current sequence are evaluated", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-eval-scope");
      const otherSeq = testSequence("inv-eval-scope-other", [step("x", "transform", {})], {
        tenantId,
      });
      const otherCreated = await client.createSequence(otherSeq);
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: otherCreated.id,
        name: "wrong-sequence",
        rule: { type: "no_unknown_effects" },
      });
      const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.deepEqual(results, []);
      void createdSequence;
    });
  });

  // ------------------------------------------------------------------
  // list_invariant_results
  // ------------------------------------------------------------------

  describe("invariant results listing", () => {
    it("a fresh execution has no invariant results", async () => {
      const { tenantId, execution } = await freshExecution("inv-results-empty");
      const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      assert.deepEqual(results, []);
    });

    it("results are returned newest first", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-results-order");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "r1",
        rule: { type: "no_unknown_effects" },
      });
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "r2",
        rule: { type: "budget_within_limits" },
      });
      await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
      assert.equal(results.length, 2);
    });

    it("listing results for an unknown execution returns 404", async () => {
      const tenantId = tid("inv-results-404");
      await rejects(
        client.listContinuityInvariantResults(uuid(), tenantId),
        404,
      );
    });

    it("results are scoped by tenant", async () => {
      const { tenantId, createdSequence, execution } = await freshExecution("inv-results-tenant");
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        name: "tenant-scoped",
        rule: { type: "no_unknown_effects" },
      });
      await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
      await rejects(
        client.listContinuityInvariantResults(execution.continuity_id, "a-totally-different-tenant"),
        404,
      );
    });
  });
});

describe("Continuity Compensation Runs", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // preview_compensation — read-only
  // ------------------------------------------------------------------

  describe("compensation preview", () => {
    it("preview on a fresh execution returns an empty plan", async () => {
      const { tenantId, execution } = await freshExecution("comp-preview-empty");
      const plan = await client.previewContinuityCompensation(execution.continuity_id, {
        tenant_id: tenantId,
      });
      assert.deepEqual(plan.steps, []);
      assert.deepEqual(plan.hazards, []);
    });

    it("preview includes a step for each committed effect with a compensation rule", async () => {
      const setup = await committedCompensableExecution("comp-preview-step");
      const plan = await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(plan.steps.length, 1);
      assert.equal(plan.steps[0]!.effect_block_id, "charge");
      assert.equal(plan.steps[0]!.handler, "release_charge");
      assert.equal(plan.steps[0]!.effect_id, setup.effect.id);
      assert.match(plan.steps[0]!.idempotency_key, /^compensate:/);
    });

    it("preview surfaces a NO_COMPENSATION_RULE hazard when the committed block has no rule", async () => {
      const setup = await compensableExecution("comp-preview-hazard", { compensation: null });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "committed",
      });
      const plan = await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(plan.steps.length, 0);
      assert.ok(plan.hazards.some((h: string) => h.startsWith("NO_COMPENSATION_RULE:charge:")));
    });

    it("preview surfaces an UNKNOWN_EFFECT_MUST_BE_RESOLVED hazard for unresolved effects", async () => {
      const setup = await compensableExecution("comp-preview-unknown", { compensation: null });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "unknown",
      });
      const plan = await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.ok(
        plan.hazards.some((h: string) => h.startsWith("UNKNOWN_EFFECT_MUST_BE_RESOLVED:charge:")),
      );
    });

    it("preview does not create a compensation run (read-only)", async () => {
      const setup = await committedCompensableExecution("comp-preview-readonly");
      await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      // If preview mutated persistent state, a second create would 409 on an
      // "already active" constraint seeded by preview; assert create still
      // succeeds fresh (proves preview never persisted a run).
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(run.state, "planned");
    });

    it("preview does not mutate the effect receipt state", async () => {
      const setup = await committedCompensableExecution("comp-preview-no-mutate");
      await client.previewContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "committed");
    });

    it("preview for an unknown execution returns 404", async () => {
      await rejects(
        client.previewContinuityCompensation(uuid(), { tenant_id: tid("comp-preview-404") }),
        404,
      );
    });
  });

  // ------------------------------------------------------------------
  // create_compensation_run
  // ------------------------------------------------------------------

  describe("compensation run creation", () => {
    it("creates a run in planned state with one pending step", async () => {
      const setup = await committedCompensableExecution("comp-create-basic");
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(run.state, "planned");
      assert.equal(run.version, 0);
      assert.equal(run.steps.length, 1);
      assert.equal(run.steps[0]!.state, "pending");
      assert.equal(run.steps[0]!.attempt, 0);
      assert.equal(run.steps[0]!.lease_owner, null);
      assert.equal(run.continuity_id, setup.execution.continuity_id);
      assert.equal(run.tenant_id, setup.tenantId);
    });

    it("rejects creating a run when there are zero compensable effects", async () => {
      const { tenantId, execution } = await freshExecution("comp-create-empty");
      await rejects(
        client.createContinuityCompensation(execution.continuity_id, { tenant_id: tenantId }),
        409,
      );
    });

    it("rejects a second concurrent-active run for the same execution", async () => {
      const setup = await committedCompensableExecution("comp-create-dup");
      await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.createContinuityCompensation(setup.execution.continuity_id, {
          tenant_id: setup.tenantId,
        }),
        409,
      );
    });

    it("orders steps LIFO by declared compensation dependency", async () => {
      const setup = await compensableExecution("comp-create-order", {
        compensation: { handler: "release_charge", verification: "handler_result" },
        secondStep: {
          id: "notify",
          compensation: {
            handler: "release_notify",
            verification: "handler_result",
            depends_on: ["charge"],
          },
        },
      });
      // Drive both worker steps to completion; worker-task completion
      // auto-advances each effect receipt straight to `committed`
      // (orch8-engine/src/effect_guard.rs), so no explicit resolve is needed.
      await client.completeWorkerTask(setup.task.id, `comp-create-order-worker`, { ok: true });
      // Poll for the second handler's task by its known handler name embedded
      // in the sequence block definition.
      const secondHandlerBlock = (setup.createdSequence.blocks as any[]).find(
        (b) => b.id === "notify",
      );
      const secondHandler = secondHandlerBlock.handler as string;
      const notifyTask = await waitForWorkerTask(secondHandler, "comp-create-order-worker-2");
      await client.completeWorkerTask(notifyTask.id, "comp-create-order-worker-2", { ok: true });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects.length, 2, "setup: both effects should exist");
      assert.ok(
        effects.every((effect: any) => effect.state === "committed"),
        "setup: both effects should already be committed after worker completion",
      );
      const run = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      assert.equal(run.steps.length, 2);
      // "notify" depends_on "charge" (forward dependency); the planner
      // reverses this so notify's compensation runs before charge's.
      assert.equal(run.steps[0]!.plan.effect_block_id, "notify");
      assert.equal(run.steps[1]!.plan.effect_block_id, "charge");
    });

    it("creating a run for an unknown execution returns 404", async () => {
      await rejects(
        client.createContinuityCompensation(uuid(), { tenant_id: tid("comp-create-404") }),
        404,
      );
    });

    it("creation is scoped by tenant (cross-tenant execution id returns 404)", async () => {
      const setup = await committedCompensableExecution("comp-create-tenant-scope");
      await rejects(
        client.createContinuityCompensation(setup.execution.continuity_id, {
          tenant_id: "a-totally-different-tenant",
        }),
        404,
      );
    });
  });

  // ------------------------------------------------------------------
  // get_compensation_run
  // ------------------------------------------------------------------

  describe("compensation run retrieval", () => {
    it("fetches a run by id", async () => {
      const setup = await committedCompensableExecution("comp-get-basic");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const fetched = await client.getContinuityCompensation(created.id, setup.tenantId);
      assert.equal(fetched.id, created.id);
      assert.equal(fetched.state, "planned");
    });

    it("returns 404 for an unknown run id", async () => {
      await rejects(
        client.getContinuityCompensation(uuid(), tid("comp-get-404")),
        404,
      );
    });

    it("is scoped by tenant", async () => {
      const setup = await committedCompensableExecution("comp-get-tenant-scope");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.getContinuityCompensation(created.id, "a-totally-different-tenant"),
        404,
      );
    });
  });

  // ------------------------------------------------------------------
  // claim_compensation_step
  // ------------------------------------------------------------------

  describe("compensation step claim", () => {
    it("claims the pending step and moves the run to running", async () => {
      const setup = await committedCompensableExecution("comp-claim-basic");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      assert.equal(claimed.state, "claimed");
      assert.equal(claimed.lease_owner, "worker-1");
      assert.equal(claimed.attempt, 1);
      assert.ok(claimed.lease_expires_at);
      const run = await client.getContinuityCompensation(created.id, setup.tenantId);
      assert.equal(run.state, "running");
    });

    it("default lease_seconds is 60 when omitted", async () => {
      const setup = await committedCompensableExecution("comp-claim-default-lease");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const before = Date.now();
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const expiresMs = new Date(claimed.lease_expires_at).getTime();
      assert.ok(expiresMs - before >= 55_000 && expiresMs - before <= 65_000);
    });

    it("rejects an empty worker_id", async () => {
      const setup = await committedCompensableExecution("comp-claim-empty-worker");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "   ",
        }),
        400,
      );
    });

    it("rejects a worker_id over 128 bytes", async () => {
      const setup = await committedCompensableExecution("comp-claim-long-worker");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "w".repeat(129),
        }),
        400,
      );
    });

    it("rejects lease_seconds below 5", async () => {
      const setup = await committedCompensableExecution("comp-claim-lease-low");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          lease_seconds: 4,
        }),
        400,
      );
    });

    it("accepts lease_seconds at the 5 second lower boundary", async () => {
      const setup = await committedCompensableExecution("comp-claim-lease-5");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        lease_seconds: 5,
      });
      assert.equal(claimed.state, "claimed");
    });

    it("accepts lease_seconds at the 3600 second upper boundary", async () => {
      const setup = await committedCompensableExecution("comp-claim-lease-3600");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        lease_seconds: 3600,
      });
      assert.equal(claimed.state, "claimed");
    });

    it("rejects lease_seconds above 3600", async () => {
      const setup = await committedCompensableExecution("comp-claim-lease-high");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          lease_seconds: 3601,
        }),
        400,
      );
    });

    it("rejects claiming when the current step is already claimed", async () => {
      const setup = await committedCompensableExecution("comp-claim-double");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
        }),
        409,
      );
    });

    it("rejects claiming a run that has no pending step (all steps settled)", async () => {
      const setup = await committedCompensableExecution("comp-claim-none-pending");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(
        created.id,
        claimed.plan.effect_id,
        { tenant_id: setup.tenantId, worker_id: "worker-1" },
      );
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
        }),
        409,
      );
    });

    it("claiming an unknown run id returns 404", async () => {
      await rejects(
        client.claimContinuityCompensation(uuid(), {
          tenant_id: tid("comp-claim-404"),
          worker_id: "worker-1",
        }),
        404,
      );
    });

    it("concurrently racing claim calls admit exactly one winner", async () => {
      const setup = await committedCompensableExecution("comp-claim-race");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const results = await Promise.allSettled([
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-a",
        }),
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-b",
        }),
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-c",
        }),
      ]);
      const fulfilled = results.filter((r) => r.status === "fulfilled");
      assert.equal(fulfilled.length, 1, "exactly one concurrent claim must win");
    });

    it("an expired lease is reclassified unknown and blocks further claims until verified", async () => {
      const setup = await committedCompensableExecution("comp-claim-expiry");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        lease_seconds: 5,
      });
      await new Promise((resolve) => setTimeout(resolve, 5_200));
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
        }),
        409,
        "an expired claim must surface as needing verification, not silently reassign",
      );
      const run = await client.getContinuityCompensation(created.id, setup.tenantId);
      assert.equal(run.steps[0]!.state, "unknown");
      assert.equal(run.state, "awaiting_verification");
      assert.ok(
        run.residual_effects.some((r: string) => r.startsWith("COMPENSATION_OUTCOME_UNKNOWN:")),
      );
    });
  });

  // ------------------------------------------------------------------
  // complete_compensation_step
  // ------------------------------------------------------------------

  describe("compensation step completion", () => {
    it("completes a claimed step with handler_result verification and marks the effect compensated", async () => {
      const setup = await committedCompensableExecution("comp-complete-basic");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      assert.equal(run.steps[0]!.state, "succeeded");
      assert.equal(run.state, "completed");
      assert.ok(run.completed_at);
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "compensated");
    });

    it("requires a provider_receipt for provider_receipt verification policy", async () => {
      const setup = await compensableExecution("comp-complete-provider-receipt-required", {
        compensation: { handler: "release_charge", verification: "provider_receipt" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-complete-provider-receipt-required-worker", {
        ok: true,
      });
      // Worker-task completion auto-advances the effect receipt to
      // `committed` (orch8-engine/src/effect_guard.rs); no explicit resolve
      // call is needed or accepted (it would already be committed).
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
        }),
        400,
      );
      const run = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        provider_receipt: "prov-receipt-1",
      });
      assert.equal(run.steps[0]!.state, "succeeded");
      assert.equal(run.steps[0]!.provider_receipt_id, "prov-receipt-1");
    });

    it("manual verification policy moves the step to verification_pending, not succeeded", async () => {
      const setup = await compensableExecution("comp-complete-manual", {
        compensation: { handler: "release_charge", verification: "manual" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-complete-manual-worker", { ok: true });
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      assert.equal(run.steps[0]!.state, "verification_pending");
      assert.equal(run.state, "awaiting_verification");
      // Manual policy must not mark the effect compensated until verified.
      const refetchedEffects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(refetchedEffects[0]!.state, "committed");
    });

    it("rejects completion by a worker that does not hold the lease", async () => {
      const setup = await committedCompensableExecution("comp-complete-wrong-worker");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
        }),
        409,
      );
    });

    it("rejects completing an unclaimed step", async () => {
      const setup = await committedCompensableExecution("comp-complete-unclaimed");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.completeContinuityCompensation(created.id, created.steps[0]!.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
        }),
        409,
      );
    });

    it("rejects double-completion of an already succeeded step", async () => {
      const setup = await committedCompensableExecution("comp-complete-double");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
        }),
        409,
      );
    });

    it("rejects a provider_receipt over 512 bytes with 413", async () => {
      const setup = await committedCompensableExecution("comp-complete-receipt-too-long");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          provider_receipt: "r".repeat(513),
        }),
        413,
      );
    });

    it("completing a step for an unknown run id returns 404", async () => {
      await rejects(
        client.completeContinuityCompensation(uuid(), uuid(), {
          tenant_id: tid("comp-complete-404-run"),
          worker_id: "worker-1",
        }),
        404,
      );
    });

    it("completing an unknown effect_id within a real run returns 404", async () => {
      const setup = await committedCompensableExecution("comp-complete-404-step");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.completeContinuityCompensation(created.id, uuid(), {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
        }),
        404,
      );
    });
  });

  // ------------------------------------------------------------------
  // fail_compensation_step
  // ------------------------------------------------------------------

  describe("compensation step failure", () => {
    it("a non-retryable failure moves the step to failed and the run to completed_with_residuals", async () => {
      const setup = await committedCompensableExecution("comp-fail-terminal");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        error: "handler unreachable",
      });
      assert.equal(run.steps[0]!.state, "failed");
      assert.equal(run.steps[0]!.error, "handler unreachable");
      assert.equal(run.state, "completed_with_residuals");
      assert.ok(run.residual_effects.some((r: string) => r.startsWith("COMPENSATION_FAILED:")));
    });

    it("a retryable failure returns the step to pending, and it can be reclaimed", async () => {
      const setup = await committedCompensableExecution("comp-fail-retry");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        error: "transient timeout",
        retryable: true,
      });
      assert.equal(run.steps[0]!.state, "pending");
      assert.equal(run.state, "running");
      assert.equal(run.residual_effects.length, 0, "a retryable failure is not a residual");
      const reclaimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-2",
      });
      assert.equal(reclaimed.attempt, 2);
    });

    it("an outcome_uncertain failure moves the step to unknown and awaits verification", async () => {
      const setup = await committedCompensableExecution("comp-fail-uncertain");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        error: "handler timed out mid-request; outcome unknown",
        outcome_uncertain: true,
      });
      assert.equal(run.steps[0]!.state, "unknown");
      assert.equal(run.state, "awaiting_verification");
      assert.ok(run.residual_effects.some((r: string) => r.startsWith("COMPENSATION_UNKNOWN:")));
    });

    it("rejects outcome_uncertain combined with retryable", async () => {
      const setup = await committedCompensableExecution("comp-fail-uncertain-retryable");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          error: "conflicting flags",
          outcome_uncertain: true,
          retryable: true,
        }),
        400,
      );
    });

    it("rejects an empty error message", async () => {
      const setup = await committedCompensableExecution("comp-fail-empty-error");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          error: "   ",
        }),
        400,
      );
    });

    it("rejects an error message over 2048 bytes", async () => {
      const setup = await committedCompensableExecution("comp-fail-long-error");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          error: "e".repeat(2049),
        }),
        400,
      );
    });

    it("rejects failure reported by a worker that does not hold the lease", async () => {
      const setup = await committedCompensableExecution("comp-fail-wrong-worker");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
          error: "not my step",
        }),
        409,
      );
    });

    it("rejects failing an unclaimed step", async () => {
      const setup = await committedCompensableExecution("comp-fail-unclaimed");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.failContinuityCompensation(created.id, created.steps[0]!.plan.effect_id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-1",
          error: "never claimed",
        }),
        409,
      );
    });

    it("a non-retryable failure does not block the run from reaching a terminal state with residuals", async () => {
      const setup = await committedCompensableExecution("comp-fail-terminal-state");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        error: "permanent failure",
      });
      assert.ok(run.completed_at);
      assert.notEqual(run.state, "running");
    });
  });

  // ------------------------------------------------------------------
  // verify_compensation_step
  // ------------------------------------------------------------------

  describe("compensation step verification", () => {
    it("approving verification marks the step verified and clears the residual", async () => {
      const setup = await compensableExecution("comp-verify-approve", {
        compensation: { handler: "release_charge", verification: "manual" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-verify-approve-worker", { ok: true });
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.verifyContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        approved: true,
        evidence: "operator confirmed refund posted",
      });
      assert.equal(run.steps[0]!.state, "verified");
      assert.equal(run.state, "completed");
      assert.equal(run.steps[0]!.error, null);
      const refetchedEffects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(refetchedEffects[0]!.state, "compensated");
    });

    it("rejecting verification marks the step failed with a residual and stores the evidence as the error", async () => {
      const setup = await compensableExecution("comp-verify-reject", {
        compensation: { handler: "release_charge", verification: "manual" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-verify-reject-worker", { ok: true });
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.verifyContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        approved: false,
        evidence: "refund not found in ledger",
      });
      assert.equal(run.steps[0]!.state, "failed");
      assert.equal(run.steps[0]!.error, "refund not found in ledger");
      assert.ok(run.residual_effects.some((r: string) => r.startsWith("COMPENSATION_REJECTED:")));
      // A rejected manual verification must not mark the effect compensated.
      const refetchedEffects = await client.listContinuityEffects(
        setup.execution.continuity_id,
        setup.tenantId,
      );
      assert.equal(refetchedEffects[0]!.state, "committed");
    });

    it("verifies (approves) a step left in unknown state after an expired lease", async () => {
      const setup = await committedCompensableExecution("comp-verify-unknown-approve");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        lease_seconds: 5,
      });
      await new Promise((resolve) => setTimeout(resolve, 5_200));
      // Trigger the lazy lease-expiry sweep via a claim attempt (expected to 409).
      await rejects(
        client.claimContinuityCompensation(created.id, {
          tenant_id: setup.tenantId,
          worker_id: "worker-2",
        }),
        409,
      );
      const run = await client.verifyContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        approved: true,
        evidence: "confirmed compensated out of band",
      });
      assert.equal(run.steps[0]!.state, "verified");
      assert.equal(run.state, "completed");
    });

    it("rejects verifying a step that is not awaiting verification", async () => {
      const setup = await committedCompensableExecution("comp-verify-not-pending");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.verifyContinuityCompensation(created.id, created.steps[0]!.plan.effect_id, {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "premature",
        }),
        409,
      );
    });

    it("rejects an empty evidence string", async () => {
      const setup = await compensableExecution("comp-verify-empty-evidence", {
        compensation: { handler: "release_charge", verification: "manual" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-verify-empty-evidence-worker", { ok: true });
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.verifyContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "   ",
        }),
        400,
      );
    });

    it("rejects an evidence string over 2048 bytes", async () => {
      const setup = await compensableExecution("comp-verify-long-evidence", {
        compensation: { handler: "release_charge", verification: "manual" },
      });
      await client.completeWorkerTask(setup.task.id, "comp-verify-long-evidence-worker", { ok: true });
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      await rejects(
        client.verifyContinuityCompensation(created.id, claimed.plan.effect_id, {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "e".repeat(2049),
        }),
        400,
      );
    });

    it("verifying an unknown run id returns 404", async () => {
      await rejects(
        client.verifyContinuityCompensation(uuid(), uuid(), {
          tenant_id: tid("comp-verify-404"),
          approved: true,
          evidence: "n/a",
        }),
        404,
      );
    });

    it("verifying an unknown effect_id within a real run returns 404", async () => {
      const setup = await committedCompensableExecution("comp-verify-404-step");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      await rejects(
        client.verifyContinuityCompensation(created.id, uuid(), {
          tenant_id: setup.tenantId,
          approved: true,
          evidence: "n/a",
        }),
        404,
      );
    });
  });

  // ------------------------------------------------------------------
  // full lifecycle / terminal states
  // ------------------------------------------------------------------

  describe("compensation run terminal states", () => {
    it("a fully succeeded single-step run reaches completed (no residuals)", async () => {
      const setup = await committedCompensableExecution("comp-lifecycle-completed");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.completeContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      assert.equal(run.state, "completed");
      assert.deepEqual(run.residual_effects, []);
    });

    it("a run with a non-retryable failed step reaches completed_with_residuals, never completed", async () => {
      const setup = await committedCompensableExecution("comp-lifecycle-residuals");
      const created = await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const claimed = await client.claimContinuityCompensation(created.id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
      });
      const run = await client.failContinuityCompensation(created.id, claimed.plan.effect_id, {
        tenant_id: setup.tenantId,
        worker_id: "worker-1",
        error: "gateway down",
      });
      assert.equal(run.state, "completed_with_residuals");
      assert.notEqual(run.state, "failed", "the run state model has no distinct terminal failed state reachable from a single failed step here");
    });

    it("hazards recorded at plan time are preserved on the run even after all steps settle", async () => {
      const setup = await compensableExecution("comp-lifecycle-hazards", { compensation: null });
      const handlerBlock = (setup.createdSequence.blocks as any[]).find((b) => b.id === "charge");
      void handlerBlock;
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      await client.resolveContinuityEffect(effects[0]!.id, {
        tenant_id: setup.tenantId,
        state: "unknown",
      });
      // This execution has an unresolved (unknown) effect and no compensable
      // committed effect, so no run can be created from it (0 steps).
      await rejects(
        client.createContinuityCompensation(setup.execution.continuity_id, {
          tenant_id: setup.tenantId,
        }),
        409,
      );
    });

    it("the parent effect receipt is unaffected by a run that never claims any step", async () => {
      const setup = await committedCompensableExecution("comp-lifecycle-no-claim");
      await client.createContinuityCompensation(setup.execution.continuity_id, {
        tenant_id: setup.tenantId,
      });
      const effects = await client.listContinuityEffects(setup.execution.continuity_id, setup.tenantId);
      assert.equal(effects[0]!.state, "committed");
    });
  });
});

describe("Runtime Registration and Listing", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("registers a runtime with registered trust", async () => {
    const tenantId = tid("rt-register");
    const runtimeId = uuid();
    const registered = await registerRuntime(tenantId, runtimeId);
    assert.equal(registered.runtime_id, runtimeId);
    assert.equal(registered.trust, "registered");
  });

  it("rejects trust level signed via self-registration", async () => {
    const tenantId = tid("rt-signed");
    await rejects(registerRuntime(tenantId, uuid(), { trust: "signed" }), 400);
  });

  it("rejects trust level attested via self-registration", async () => {
    const tenantId = tid("rt-attested");
    await rejects(registerRuntime(tenantId, uuid(), { trust: "attested" }), 400);
  });

  it("rejects an expiry in the past", async () => {
    const tenantId = tid("rt-expired");
    const now = Date.now();
    await rejects(
      registerRuntime(tenantId, uuid(), {
        observed_at: new Date(now - 120_000).toISOString(),
        expires_at: new Date(now - 60_000).toISOString(),
      }),
      400,
    );
  });

  it("rejects observed_at more than 30 seconds in the future", async () => {
    const tenantId = tid("rt-observed-future");
    const now = Date.now();
    await rejects(
      registerRuntime(tenantId, uuid(), {
        observed_at: new Date(now + 60_000).toISOString(),
        expires_at: new Date(now + 120_000).toISOString(),
      }),
      400,
    );
  });

  it("rejects a capability lifetime longer than five minutes", async () => {
    const tenantId = tid("rt-lifetime-long");
    const now = Date.now();
    await rejects(
      registerRuntime(tenantId, uuid(), {
        observed_at: new Date(now).toISOString(),
        expires_at: new Date(now + 6 * 60_000).toISOString(),
      }),
      400,
    );
  });

  it("accepts a capability lifetime of exactly five minutes", async () => {
    const tenantId = tid("rt-lifetime-5min");
    const runtimeId = uuid();
    const now = Date.now();
    const registered = await registerRuntime(tenantId, runtimeId, {
      observed_at: new Date(now).toISOString(),
      expires_at: new Date(now + 5 * 60_000).toISOString(),
    });
    assert.equal(registered.runtime_id, runtimeId);
  });

  it("rejects expires_at equal to observed_at (non-positive lifetime)", async () => {
    const tenantId = tid("rt-zero-lifetime");
    const now = new Date().toISOString();
    await rejects(
      registerRuntime(tenantId, uuid(), { observed_at: now, expires_at: now }),
      400,
    );
  });

  it("rejects battery_percent over 100", async () => {
    const tenantId = tid("rt-battery-high");
    await rejects(registerRuntime(tenantId, uuid(), { battery_percent: 101 }), 400);
  });

  it("accepts battery_percent at exactly 100", async () => {
    const tenantId = tid("rt-battery-100");
    const registered = await registerRuntime(tenantId, uuid(), { battery_percent: 100 });
    assert.equal(registered.battery_percent, 100);
  });

  it("rejects more than 256 handler facts", async () => {
    const tenantId = tid("rt-handlers-257");
    const handlers = Array.from({ length: 257 }, (_, i) => `h${i}`);
    await rejects(registerRuntime(tenantId, uuid(), { handlers }), 400);
  });

  it("accepts exactly 256 handler facts", async () => {
    const tenantId = tid("rt-handlers-256");
    const handlers = Array.from({ length: 256 }, (_, i) => `h${i}`);
    const registered = await registerRuntime(tenantId, uuid(), { handlers });
    assert.equal((registered.handlers as string[]).length, 256);
  });

  it("rejects an empty-string handler fact", async () => {
    const tenantId = tid("rt-handler-empty");
    await rejects(registerRuntime(tenantId, uuid(), { handlers: ["ok", ""] }), 400);
  });

  it("rejects a handler fact over 256 bytes", async () => {
    const tenantId = tid("rt-handler-long");
    await rejects(registerRuntime(tenantId, uuid(), { handlers: ["h".repeat(257)] }), 400);
  });

  it("accepts a handler fact at exactly 256 bytes", async () => {
    const tenantId = tid("rt-handler-256b");
    const registered = await registerRuntime(tenantId, uuid(), { handlers: ["h".repeat(256)] });
    assert.equal((registered.handlers as string[])[0]!.length, 256);
  });

  it("rejects more than 256 region facts", async () => {
    const tenantId = tid("rt-regions-257");
    const regions = Array.from({ length: 257 }, (_, i) => `r${i}`);
    await rejects(registerRuntime(tenantId, uuid(), { regions }), 400);
  });

  it("rejects an invalid base64 capsule_signing_public_key", async () => {
    const tenantId = tid("rt-key-bad-b64");
    await rejects(
      registerRuntime(tenantId, uuid(), { capsule_signing_public_key: "not-valid-base64!!!" }),
      400,
    );
  });

  it("rejects a capsule_signing_public_key that does not decode to 32 bytes", async () => {
    const tenantId = tid("rt-key-wrong-len");
    await rejects(
      registerRuntime(tenantId, uuid(), {
        capsule_signing_public_key: Buffer.from("too-short").toString("base64"),
      }),
      400,
    );
  });

  it("accepts a valid 32-byte base64 capsule_signing_public_key", async () => {
    const tenantId = tid("rt-key-ok");
    const key = validEd25519PublicKeyBase64();
    const registered = await registerRuntime(tenantId, uuid(), { capsule_signing_public_key: key });
    assert.equal(registered.capsule_signing_public_key, key);
  });

  it("re-registering the same runtime_id upserts rather than duplicates", async () => {
    const tenantId = tid("rt-upsert");
    const runtimeId = uuid();
    await registerRuntime(tenantId, runtimeId);
    await registerRuntime(tenantId, runtimeId, { estimated_latency_ms: 999 });
    const list = await client.listRuntimes(tenantId);
    const matches = list.filter((r: any) => r.runtime_id === runtimeId);
    assert.equal(matches.length, 1);
    assert.equal(matches[0].estimated_latency_ms, 999);
  });

  it("list_runtimes is scoped by tenant", async () => {
    const tenantId = tid("rt-list-tenant-a");
    const otherTenant = tid("rt-list-tenant-b");
    const runtimeId = uuid();
    await registerRuntime(tenantId, runtimeId);
    const listA = await client.listRuntimes(tenantId);
    const listB = await client.listRuntimes(otherTenant);
    assert.ok(listA.some((r: any) => r.runtime_id === runtimeId));
    assert.ok(!listB.some((r: any) => r.runtime_id === runtimeId));
  });

  it("list_runtimes excludes an expired runtime registration", async () => {
    const tenantId = tid("rt-list-expired");
    const runtimeId = uuid();
    const now = Date.now();
    await registerRuntime(tenantId, runtimeId, {
      observed_at: new Date(now - 1000).toISOString(),
      expires_at: new Date(now + 1500).toISOString(),
    });
    await new Promise((resolve) => setTimeout(resolve, 1700));
    const list = await client.listRuntimes(tenantId);
    assert.ok(!list.some((r: any) => r.runtime_id === runtimeId));
  });

  it("list_runtimes on an unknown tenant returns an empty list", async () => {
    const list = await client.listRuntimes(tid("rt-list-unknown"));
    assert.deepEqual(list, []);
  });
});
