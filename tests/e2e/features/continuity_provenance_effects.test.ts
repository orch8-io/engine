/**
 * E2E: Portable Continuity — provenance chain, effect receipts, invariants.
 *
 * Provenance: `POST/GET /continuity/executions/{id}/provenance`,
 * `GET /continuity/executions/{id}/provenance/verify`. Every provenance
 * entry is payload-free (a SHA-256 digest plus a bounded redacted
 * summary), hash-chained to its predecessor, and optionally signed.
 *
 * Effects: `GET /continuity/executions/{id}/effects`,
 * `POST /continuity/effects/{id}/resolve`. Effect receipts move through
 * planned -> prepared -> dispatched -> committed | unknown, and unknown
 * effects require explicit evidence-backed resolution before they can be
 * marked verified, committed, compensated, or abandoned. This file drives
 * effects into "dispatched" state via a real worker-handler step, matching
 * the pattern in portable_continuity.test.ts's "classifies external worker
 * effects" test.
 *
 * Invariants: `POST/GET /continuity/invariants`,
 * `POST /continuity/executions/{id}/invariants/evaluate`,
 * `GET /continuity/executions/{id}/invariants/results`. Declarative rules
 * (effect_at_most_once, no_unknown_effects, terminal_state_in,
 * budget_within_limits, output_path_present) evaluated against durable
 * evidence only — never against caller-supplied claims.
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { WorkerTask } from "../types.ts";

const client = new Orch8Client();

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
  const tenantId = `${namePrefix}-${uuid().slice(0, 8)}`;
  const sequence = testSequence(namePrefix, [step("s1", "transform", { result: "ok" })], {
    tenantId,
  });
  const created = await client.createSequence(sequence);
  // createSequence's response is just {id}; the sequence's own version is
  // always 1 (testSequence() sets it), so merge to get a fully-populated
  // object without a second round-trip.
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
 * Create a continuity execution wired to a two-step sequence (arm ->
 * worker-dispatched charge) and drive it to the point where the "charge"
 * step's effect receipt is in `dispatched` state, mirroring
 * portable_continuity.test.ts's "classifies external worker effects" test.
 */
async function dispatchedEffectExecution(namePrefix: string, idempotencyKey?: string) {
  const tenantId = `${namePrefix}-${uuid().slice(0, 8)}`;
  const handler = `${namePrefix}_handler_${uuid().slice(0, 8)}`;
  const sequence = testSequence(
    namePrefix,
    [
      step("arm", "human_review", {}, {
        wait_for_input: {
          prompt: "Arm effect dispatch?",
          choices: [{ label: "Continue", value: "continue" }],
        },
      }),
      step("charge", handler, {
        amount: 100,
        ...(idempotencyKey ? { idempotency_key: idempotencyKey } : {}),
      }),
    ],
    { tenantId },
  );
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
  const dispatched = await client.listContinuityEffects(execution.continuity_id, tenantId);
  assert.equal(dispatched.length, 1, "setup: exactly one effect should be dispatched");
  assert.equal(dispatched[0]!.state, "dispatched", "setup: effect should be dispatched");
  return { tenantId, createdSequence, instance, execution, handler, task, effect: dispatched[0]! };
}

describe("Continuity Provenance", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // record_provenance_boundary — kind validation
  // ------------------------------------------------------------------

  it("execution_created is appended automatically on creation", async () => {
    const { tenantId, execution } = await freshExecution("prov-auto");
    const entries = await client.listContinuityProvenance(execution.continuity_id, tenantId);
    assert.equal(entries.length, 1);
    assert.equal(entries[0].kind, "execution_created");
    assert.equal(entries[0].previous_sha256, null);
    assert.match(entries[0].entry_sha256, /^[0-9a-f]{64}$/);
  });

  it("records a package_identity boundary", async () => {
    const { tenantId, execution } = await freshExecution("prov-pkg");
    const head = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    assert.equal(head.kind, "package_identity");
    assert.equal(head.payload_sha256, "a".repeat(64));
  });

  it("records a model_selected boundary", async () => {
    const { tenantId, execution } = await freshExecution("prov-model");
    const head = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "model_selected",
      payload_sha256: "b".repeat(64),
    });
    assert.equal(head.kind, "model_selected");
  });

  it("records a terminal_outcome boundary", async () => {
    const { tenantId, execution } = await freshExecution("prov-terminal");
    const head = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "terminal_outcome",
      payload_sha256: "c".repeat(64),
    });
    assert.equal(head.kind, "terminal_outcome");
  });

  it("rejects an unrecognized provenance kind", async () => {
    const { tenantId, execution } = await freshExecution("prov-bad-kind");
    await assert.rejects(
      () =>
        client.recordContinuityProvenance(execution.continuity_id, {
          tenant_id: tenantId,
          kind: "made_up_kind",
          payload_sha256: "d".repeat(64),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });

  it("rejects an empty provenance kind", async () => {
    const { tenantId, execution } = await freshExecution("prov-empty-kind");
    await assert.rejects(() =>
      client.recordContinuityProvenance(execution.continuity_id, {
        tenant_id: tenantId,
        kind: "",
        payload_sha256: "e".repeat(64),
      }),
    );
  });

  it("rejects a payload_sha256 that is too short", async () => {
    const { tenantId, execution } = await freshExecution("prov-short-hash");
    await assert.rejects(() =>
      client.recordContinuityProvenance(execution.continuity_id, {
        tenant_id: tenantId,
        kind: "package_identity",
        payload_sha256: "abc123",
      }),
    );
  });

  it("rejects a payload_sha256 with non-hex characters", async () => {
    const { tenantId, execution } = await freshExecution("prov-nonhex-hash");
    await assert.rejects(() =>
      client.recordContinuityProvenance(execution.continuity_id, {
        tenant_id: tenantId,
        kind: "package_identity",
        payload_sha256: "z".repeat(64),
      }),
    );
  });

  it("rejects a payload_sha256 that is too long", async () => {
    const { tenantId, execution } = await freshExecution("prov-long-hash");
    await assert.rejects(() =>
      client.recordContinuityProvenance(execution.continuity_id, {
        tenant_id: tenantId,
        kind: "package_identity",
        payload_sha256: "a".repeat(65),
      }),
    );
  });

  it("returns 404 recording provenance for a nonexistent execution", async () => {
    await assert.rejects(
      () =>
        client.recordContinuityProvenance(uuid(), {
          tenant_id: "no-such-tenant",
          kind: "package_identity",
          payload_sha256: "a".repeat(64),
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("returns 404 listing provenance for a nonexistent execution's mismatched tenant query but existing id is fine (empty list for wrong tenant is not exposed as 404)", async () => {
    const { execution } = await freshExecution("prov-tenant-mismatch");
    // list_provenance does not verify execution existence; it just scopes by
    // tenant_id, so a real id under the wrong tenant returns an empty list,
    // not a 404 — this documents that behavior explicitly.
    const entries = await client.listContinuityProvenance(
      execution.continuity_id,
      "completely-different-tenant",
    );
    assert.deepEqual(entries, []);
  });

  // ------------------------------------------------------------------
  // record_provenance_boundary — hash chain integrity
  // ------------------------------------------------------------------

  it("chains each new entry's previous_sha256 to the prior entry_sha256", async () => {
    const { tenantId, execution } = await freshExecution("prov-chain");
    const first = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    const second = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "model_selected",
      payload_sha256: "b".repeat(64),
    });
    assert.equal(second.previous_sha256, first.entry_sha256);
    assert.notEqual(second.entry_sha256, first.entry_sha256);
  });

  it("returns entries in append order from list_provenance", async () => {
    const { tenantId, execution } = await freshExecution("prov-order");
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "model_selected",
      payload_sha256: "b".repeat(64),
    });
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "terminal_outcome",
      payload_sha256: "c".repeat(64),
    });
    const entries = await client.listContinuityProvenance(execution.continuity_id, tenantId);
    assert.deepEqual(
      entries.map((e: any) => e.kind),
      ["execution_created", "package_identity", "model_selected", "terminal_outcome"],
    );
  });

  it("each entry carries a bounded redacted_summary, never a raw payload", async () => {
    const { tenantId, execution } = await freshExecution("prov-summary");
    const head = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "terminal_outcome",
      payload_sha256: "f".repeat(64),
    });
    assert.equal(typeof head.redacted_summary, "string");
    assert.ok(head.redacted_summary!.length < 500);
    assert.ok(!head.redacted_summary!.includes(head.payload_sha256));
  });

  // ------------------------------------------------------------------
  // verify_provenance
  // ------------------------------------------------------------------

  it("verifies a clean chain with no tampering", async () => {
    const { tenantId, execution } = await freshExecution("prov-verify-clean");
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    const result = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
    assert.equal(result.valid, true);
    assert.equal(result.first_invalid_index, null);
  });

  it("verification against a stale expected_head fails", async () => {
    const { tenantId, execution } = await freshExecution("prov-verify-stale-head");
    await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    const result = await client.verifyContinuityProvenance(
      execution.continuity_id,
      tenantId,
      "0".repeat(64),
    );
    assert.equal(result.valid, false);
  });

  it("verification against the current head succeeds", async () => {
    const { tenantId, execution } = await freshExecution("prov-verify-current-head");
    const head = await client.recordContinuityProvenance(execution.continuity_id, {
      tenant_id: tenantId,
      kind: "package_identity",
      payload_sha256: "a".repeat(64),
    });
    const result = await client.verifyContinuityProvenance(
      execution.continuity_id,
      tenantId,
      head.entry_sha256,
    );
    assert.equal(result.valid, true);
  });

  it("an execution with only the auto-created entry still verifies clean", async () => {
    const { tenantId, execution } = await freshExecution("prov-verify-solo");
    const result = await client.verifyContinuityProvenance(execution.continuity_id, tenantId);
    assert.equal(result.valid, true);
  });

  it("verify_provenance for an unknown execution reports an empty, valid (vacuous) chain", async () => {
    // The endpoint scopes by tenant + id but doesn't 404 on a missing
    // execution — a chain with zero entries is trivially valid.
    const result = await client.verifyContinuityProvenance(
      uuid(),
      `prov-verify-missing-${uuid().slice(0, 8)}`,
    );
    assert.equal(result.valid, true);
  });

  it("effect resolution appends an effect_resolved provenance boundary", async () => {
    const { tenantId, execution, task } = await dispatchedEffectExecution("prov-effect-link");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    const effect = effects[0]!;
    await client.resolveContinuityEffect(effect.id, {
      tenant_id: tenantId,
      state: "committed",
      provider_receipt_id: "manual-resolve-1",
    });
    const entries = await client.listContinuityProvenance(execution.continuity_id, tenantId);
    assert.ok(entries.some((e: any) => e.kind === "effect_resolved"));
    // Clean up the still-open worker task so it doesn't hang around; not
    // asserting on this, best-effort.
    await client
      .completeWorkerTask(task.id, `prov-effect-link-worker`, { ok: true })
      .catch(() => {});
  });
});

describe("Continuity Effect Receipts", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // list_effects
  // ------------------------------------------------------------------

  it("a fresh execution has no effect receipts", async () => {
    const { tenantId, execution } = await freshExecution("effect-empty");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    assert.deepEqual(effects, []);
  });

  it("dispatching a worker step creates exactly one dispatched effect receipt", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-one");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    assert.equal(effects.length, 1);
    assert.equal(effects[0]!.state, "dispatched");
    assert.equal(effects[0]!.block_id, "charge");
    assert.equal(effects[0]!.kind, "worker");
  });

  it("effect receipt request_sha256 is a well-formed sha256 hex digest", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-hash");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    assert.match(effects[0]!.request_sha256, /^[0-9a-f]{64}$/);
  });

  it("effect receipt idempotency_key round-trips from the step params", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-idem", "custom-key-1");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    assert.equal(effects[0]!.idempotency_key, "custom-key-1");
  });

  it("effect receipt idempotency_key is null when not supplied", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-no-idem");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    assert.equal(effects[0]!.idempotency_key, null);
  });

  it("effect listing is scoped by tenant", async () => {
    const { execution } = await dispatchedEffectExecution("effect-tenant-scope");
    const effects = await client.listContinuityEffects(
      execution.continuity_id,
      "a-totally-different-tenant",
    );
    assert.deepEqual(effects, []);
  });

  // ------------------------------------------------------------------
  // resolve_effect — valid transitions
  // ------------------------------------------------------------------

  it("resolves a dispatched effect to committed", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-resolve-committed");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
      provider_receipt_id: "receipt-committed-1",
    });
    assert.equal(resolved.state, "committed");
    assert.equal(resolved.provider_receipt_id, "receipt-committed-1");
  });

  it("resolves a dispatched effect to unknown", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-resolve-unknown");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "unknown",
    });
    assert.equal(resolved.state, "unknown");
  });

  it("resolves an unknown effect to verified", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-unknown-to-verified");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "verified",
    });
    assert.equal(resolved.state, "verified");
  });

  it("resolves an unknown effect to committed", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-unknown-to-committed");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
    });
    assert.equal(resolved.state, "committed");
  });

  it("resolves an unknown effect to compensated", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-unknown-to-compensated");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "compensated",
    });
    assert.equal(resolved.state, "compensated");
  });

  it("resolves an unknown effect to abandoned", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-unknown-to-abandoned");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "abandoned",
    });
    assert.equal(resolved.state, "abandoned");
  });

  it("resolves a committed effect to compensated", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-committed-to-compensated");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
    });
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "compensated",
    });
    assert.equal(resolved.state, "compensated");
  });

  // ------------------------------------------------------------------
  // resolve_effect — invalid transitions rejected
  // ------------------------------------------------------------------

  it("rejects transitioning a dispatched effect directly to verified", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-invalid-dispatched-verified");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await assert.rejects(
      () =>
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: tenantId,
          state: "verified",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("rejects transitioning a committed effect back to dispatched", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-invalid-committed-dispatched");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
    });
    await assert.rejects(
      () =>
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: tenantId,
          state: "dispatched",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 409);
        return true;
      },
    );
  });

  it("rejects transitioning a committed effect back to unknown", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-invalid-committed-unknown");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
    });
    await assert.rejects(() =>
      client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" }),
    );
  });

  it("rejects transitioning an abandoned (terminal) effect anywhere else", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-invalid-abandoned-terminal");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "abandoned",
    });
    await assert.rejects(() =>
      client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "committed" }),
    );
  });

  it("rejects resolving a nonexistent effect id", async () => {
    await assert.rejects(
      () =>
        client.resolveContinuityEffect(uuid(), {
          tenant_id: `effect-404-${uuid().slice(0, 8)}`,
          state: "committed",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("rejects resolving an effect under the wrong tenant", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-wrong-tenant");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await assert.rejects(
      () =>
        client.resolveContinuityEffect(effects[0]!.id, {
          tenant_id: "someone-elses-tenant",
          state: "committed",
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("carries provider_receipt_id through resolution when supplied", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-receipt-id");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
      provider_receipt_id: "external-receipt-xyz",
    });
    assert.equal(resolved.provider_receipt_id, "external-receipt-xyz");
  });

  it("resolution updates updated_at to a later timestamp", async () => {
    const { execution, tenantId } = await dispatchedEffectExecution("effect-updated-at");
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    const before = new Date(effects[0]!.updated_at).getTime();
    await new Promise((r) => setTimeout(r, 20));
    const resolved = await client.resolveContinuityEffect(effects[0]!.id, {
      tenant_id: tenantId,
      state: "committed",
    });
    assert.ok(new Date(resolved.updated_at).getTime() >= before);
  });
});

describe("Continuity Invariants", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // create_invariant — rule shapes
  // ------------------------------------------------------------------

  it("creates a no_unknown_effects invariant", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-no-unknown");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "no unresolved effects",
      rule: { type: "no_unknown_effects" },
    });
    assert.equal(invariant.rule.type, "no_unknown_effects");
    assert.equal(invariant.enabled, true);
    assert.equal(invariant.commit_guard, false);
  });

  it("creates an effect_at_most_once invariant with commit_guard", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-at-most-once");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "http calls at most once",
      rule: { type: "effect_at_most_once", kind: "http" },
      commit_guard: true,
    });
    assert.equal(invariant.rule.type, "effect_at_most_once");
    assert.equal(invariant.rule.kind, "http");
    assert.equal(invariant.commit_guard, true);
  });

  it("creates a terminal_state_in invariant", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-terminal-state");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "must complete or fail",
      rule: { type: "terminal_state_in", states: ["completed", "failed"] },
    });
    assert.deepEqual(invariant.rule.states, ["completed", "failed"]);
  });

  it("creates a budget_within_limits invariant", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-budget");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "stays within budget",
      rule: { type: "budget_within_limits" },
    });
    assert.equal(invariant.rule.type, "budget_within_limits");
  });

  it("creates an output_path_present invariant", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-output-path");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "s1 must produce a result",
      rule: { type: "output_path_present", block_id: "s1", path: "result" },
    });
    assert.equal(invariant.rule.block_id, "s1");
    assert.equal(invariant.rule.path, "result");
  });

  // ------------------------------------------------------------------
  // create_invariant — validation
  // ------------------------------------------------------------------

  it("rejects an empty invariant name", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-empty-name");
    await assert.rejects(
      () =>
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          sequence_version: createdSequence.version,
          name: "",
          rule: { type: "no_unknown_effects" },
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });

  it("rejects an invariant name over 128 bytes", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-long-name");
    await assert.rejects(() =>
      client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: createdSequence.version,
        name: "x".repeat(129),
        rule: { type: "no_unknown_effects" },
      }),
    );
  });

  it("accepts an invariant name at exactly 128 bytes", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-max-name");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "x".repeat(128),
      rule: { type: "no_unknown_effects" },
    });
    assert.equal(invariant.name.length, 128);
  });

  it("rejects terminal_state_in with an empty states list", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-terminal-empty");
    await assert.rejects(() =>
      client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: createdSequence.version,
        name: "bad terminal states",
        rule: { type: "terminal_state_in", states: [] },
      }),
    );
  });

  it("rejects terminal_state_in with more than 16 states", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-terminal-too-many");
    await assert.rejects(() =>
      client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: createdSequence.id,
        sequence_version: createdSequence.version,
        name: "too many terminal states",
        rule: {
          type: "terminal_state_in",
          states: Array.from({ length: 17 }, (_, i) => `state-${i}`),
        },
      }),
    );
  });

  it("accepts terminal_state_in with exactly 16 states", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-terminal-max");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "sixteen terminal states",
      rule: {
        type: "terminal_state_in",
        states: Array.from({ length: 16 }, (_, i) => `state-${i}`),
      },
    });
    assert.equal(invariant.rule.states.length, 16);
  });

  it("rejects commit_guard on a rule other than effect_at_most_once", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-bad-commit-guard");
    await assert.rejects(
      () =>
        client.createContinuityInvariant({
          tenant_id: tenantId,
          sequence_id: createdSequence.id,
          sequence_version: createdSequence.version,
          name: "invalid commit guard usage",
          rule: { type: "no_unknown_effects" },
          commit_guard: true,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 400);
        return true;
      },
    );
  });

  it("returns 404 creating an invariant for a nonexistent sequence", async () => {
    await assert.rejects(
      () =>
        client.createContinuityInvariant({
          tenant_id: `inv-no-seq-${uuid().slice(0, 8)}`,
          sequence_id: uuid(),
          sequence_version: 1,
          name: "orphan invariant",
          rule: { type: "no_unknown_effects" },
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("commit_guard defaults to false when omitted", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-default-guard");
    const invariant = await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "default guard",
      rule: { type: "effect_at_most_once", kind: "http" },
    });
    assert.equal(invariant.commit_guard, false);
  });

  // ------------------------------------------------------------------
  // list_invariants
  // ------------------------------------------------------------------

  it("lists invariants scoped to sequence + version", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-list");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "rule one",
      rule: { type: "no_unknown_effects" },
    });
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "rule two",
      rule: { type: "budget_within_limits" },
    });
    const invariants = await client.listContinuityInvariants(
      tenantId,
      createdSequence.id,
      createdSequence.version,
    );
    assert.equal(invariants.length, 2);
  });

  it("list_invariants for an unrelated sequence version is empty", async () => {
    const { tenantId, createdSequence } = await freshExecution("inv-list-empty-version");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "v1 rule",
      rule: { type: "no_unknown_effects" },
    });
    const invariants = await client.listContinuityInvariants(
      tenantId,
      createdSequence.id,
      createdSequence.version + 1,
    );
    assert.deepEqual(invariants, []);
  });

  it("list_invariants is scoped by tenant", async () => {
    const { createdSequence } = await freshExecution("inv-list-tenant-scope");
    const invariants = await client.listContinuityInvariants(
      "unrelated-tenant",
      createdSequence.id,
      createdSequence.version,
    );
    assert.deepEqual(invariants, []);
  });

  // ------------------------------------------------------------------
  // evaluate_invariants — outcomes
  // ------------------------------------------------------------------

  it("no_unknown_effects passes when there are no effects at all", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-eval-no-effects-pass");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "no unknowns",
      rule: { type: "no_unknown_effects" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results.length, 1);
    assert.equal(results[0].status, "pass");
  });

  it("no_unknown_effects fails when an effect is in unknown state", async () => {
    const { execution, tenantId, task, handler } = await dispatchedEffectExecution(
      "inv-eval-no-effects-fail",
    );
    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });

    const instance = await client.getInstance(execution.current_instance_id);
    const sequence = await client.getSequence(instance.sequence_id);
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: sequence.id,
      sequence_version: sequence.version,
      name: "no unknowns must fail",
      rule: { type: "no_unknown_effects" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results.length, 1);
    assert.equal(results[0].status, "fail");
    await client.completeWorkerTask(task.id, `${handler}-cleanup`, {}).catch(() => {});
  });

  it("terminal_state_in is unknown/inconclusive while the instance is still running", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-eval-terminal-running");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "must reach completed",
      rule: { type: "terminal_state_in", states: ["completed"] },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.notEqual(results[0].status, "pass");
  });

  it("terminal_state_in passes once the instance completes in the allowed set", async () => {
    const { tenantId, execution, createdSequence, instance } = await freshExecution(
      "inv-eval-terminal-complete",
    );
    await client.waitForState(instance.id, "completed");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "must be completed",
      rule: { type: "terminal_state_in", states: ["completed"] },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results[0].status, "pass");
  });

  it("terminal_state_in fails when the instance terminates outside the allowed set", async () => {
    const tenantId = `inv-eval-terminal-wrong-${uuid().slice(0, 8)}`;
    const sequence = testSequence("inv-eval-terminal-wrong", [step("s1", "fail")], { tenantId });
    const createdSequence = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: createdSequence.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "failed");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "must be completed, not failed",
      rule: { type: "terminal_state_in", states: ["completed"] },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results[0].status, "fail");
  });

  it("output_path_present passes once the block output contains the path", async () => {
    const { tenantId, execution, createdSequence, instance } = await freshExecution(
      "inv-eval-output-pass",
    );
    await client.waitForState(instance.id, "completed");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "s1 output present",
      rule: { type: "output_path_present", block_id: "s1", path: "result" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results.length, 1);
    assert.equal(results[0].status, "pass");
  });

  it("output_path_present is unknown (not a hard fail) for a path with no retained evidence", async () => {
    // Missing evidence is reported as `unknown`, never `fail` — absence of
    // evidence isn't proof of failure (see continuity_advanced::output_path_present).
    const { tenantId, execution, createdSequence, instance } = await freshExecution(
      "inv-eval-output-unknown",
    );
    await client.waitForState(instance.id, "completed");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "nonexistent output path",
      rule: { type: "output_path_present", block_id: "s1", path: "totally.made.up.path" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results[0].status, "unknown");
  });

  it("evaluating multiple invariants at once returns one result per invariant", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-eval-multi");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "rule a",
      rule: { type: "no_unknown_effects" },
    });
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "rule b",
      rule: { type: "budget_within_limits" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(results.length, 2);
  });

  it("returns 404 evaluating invariants for a nonexistent execution", async () => {
    await assert.rejects(
      () =>
        client.evaluateContinuityInvariants(uuid(), {
          tenant_id: `inv-eval-404-${uuid().slice(0, 8)}`,
        }),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("each invariant result carries a stable dedupe_key and evidence hash list", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-eval-dedupe-key");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "dedupe key check",
      rule: { type: "no_unknown_effects" },
    });
    const results = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(typeof results[0].dedupe_key, "string");
    assert.ok(results[0].dedupe_key.length > 0);
    assert.ok(Array.isArray(results[0].evidence_sha256));
  });

  // ------------------------------------------------------------------
  // list_invariant_results
  // ------------------------------------------------------------------

  it("list_invariant_results is empty before any evaluation", async () => {
    const { tenantId, execution } = await freshExecution("inv-results-empty");
    const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
    assert.deepEqual(results, []);
  });

  it("list_invariant_results reflects results appended by evaluate", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-results-populated");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "populate results",
      rule: { type: "no_unknown_effects" },
    });
    await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
    const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
    assert.equal(results.length, 1);
  });

  it("re-evaluating with unchanged evidence is deduplicated, not appended again", async () => {
    // append_invariant_result does `INSERT OR IGNORE` keyed by dedupe_key —
    // evaluating twice against identical evidence produces the same
    // dedupe_key, so the second append is silently ignored.
    const { tenantId, execution, createdSequence } = await freshExecution("inv-results-dedupe");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "stable evidence dedupe",
      rule: { type: "no_unknown_effects" },
    });
    await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
    await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
    const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
    assert.equal(results.length, 1);
  });

  it("re-evaluating after evidence changes appends a genuinely new result", async () => {
    const { tenantId, execution, createdSequence, task, handler } = await dispatchedEffectExecution(
      "inv-results-evidence-change",
    );
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "changing evidence",
      rule: { type: "no_unknown_effects" },
    });
    const beforeResolve = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(beforeResolve[0].status, "pass"); // dispatched isn't "unknown" yet

    const effects = await client.listContinuityEffects(execution.continuity_id, tenantId);
    await client.resolveContinuityEffect(effects[0]!.id, { tenant_id: tenantId, state: "unknown" });
    const afterResolve = await client.evaluateContinuityInvariants(execution.continuity_id, {
      tenant_id: tenantId,
    });
    assert.equal(afterResolve[0].status, "fail"); // now genuinely different evidence

    const results = await client.listContinuityInvariantResults(execution.continuity_id, tenantId);
    assert.equal(results.length, 2);
    await client.completeWorkerTask(task.id, `${handler}-cleanup`, {}).catch(() => {});
  });

  it("returns 404 listing invariant results for a nonexistent execution", async () => {
    await assert.rejects(
      () =>
        client.listContinuityInvariantResults(uuid(), `inv-results-404-${uuid().slice(0, 8)}`),
      (error: unknown) => {
        assert.equal((error as ApiError).status, 404);
        return true;
      },
    );
  });

  it("list_invariant_results is scoped by tenant", async () => {
    const { tenantId, execution, createdSequence } = await freshExecution("inv-results-tenant");
    await client.createContinuityInvariant({
      tenant_id: tenantId,
      sequence_id: createdSequence.id,
      sequence_version: createdSequence.version,
      name: "tenant scoped result",
      rule: { type: "no_unknown_effects" },
    });
    await client.evaluateContinuityInvariants(execution.continuity_id, { tenant_id: tenantId });
    await assert.rejects(() =>
      client.listContinuityInvariantResults(execution.continuity_id, "wrong-tenant-entirely"),
    );
  });
});
