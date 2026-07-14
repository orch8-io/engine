/**
 * E2E: Deterministic Fault Laboratory & Automatic DLQ Incident Reproduction.
 *
 * Covers `orch8-api/src/continuity.rs`'s `run_fault_lab` / `generate_scenarios`
 * / `reproduce_incident` handlers (gated behind `ORCH8_CONTINUITY_LAB_ENABLED`)
 * and `orch8-api/src/dlq_groups.rs`'s `reproduce_group` / `list_reproductions`
 * (always available, no gate). See CHANGELOG.md [Unreleased]: "Deterministic
 * fault laboratory and bounded state-space exploration" and "Automatic DLQ
 * incident reproduction".
 *
 * `run_ownership_fault_lab` is a pure function (orch8-engine/src/continuity_advanced.rs):
 *   - committed == (phase === "after")
 *   - final_epoch = initial_epoch + (committed && transition === "exported_to_accepted" ? 1 : 0)
 *   - retry_safe is always true
 *   - virtual_time_ms = 30000 for "delayed_approval", else 3
 *   - trace cites the transition's boundary string and the profile's fault name
 */

import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

const NINE_PROFILES = [
  "worker_death",
  "database_timeout",
  "duplicate_delivery",
  "stale_owner",
  "offline_device",
  "corrupt_capsule",
  "expired_grant",
  "provider_outage",
  "delayed_approval",
] as const;

const FIVE_TRANSITIONS: Array<[string, string]> = [
  ["request_to_quiescing", "ownership.request"],
  ["quiescing_to_exported", "storage.capsule"],
  ["exported_to_accepted", "ownership.claim"],
  ["accepted_to_resumed", "dispatch.resume"],
  ["resumed_to_completed", "receipt.terminal"],
];

/** `GeneratedScenario` requires `id`/`max_steps`/`seed` beyond the fields
 * a test cares about — fill them with harmless defaults. */
function mkScenario(overrides: Record<string, unknown>): Record<string, unknown> {
  return {
    id: uuid(),
    max_steps: 100,
    seed: 1,
    event_order: [],
    faults: [],
    policy_facts: [],
    retry_attempt: 0,
    handoff_delay_ms: 0,
    ...overrides,
  };
}

describe("Fault Lab: gated when disabled", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_LOG_LEVEL: "error" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("run_fault_lab rejects when ORCH8_CONTINUITY_LAB_ENABLED is unset", async () => {
    try {
      await client.runContinuityFaultLab({
        profile: "worker_death",
        transition: "request_to_quiescing",
        phase: "before",
      });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.ok(err.status === 503 || err.status === 403 || err.status === 404, `got ${err.status}`);
    }
  });

  it("generate_scenarios rejects when lab disabled", async () => {
    try {
      await client.generateContinuityScenarios({
        max_scenarios: 1,
        seed: 1,
      });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.ok(err.status === 503 || err.status === 403 || err.status === 404, `got ${err.status}`);
    }
  });

  it("reproduce_incident (continuity) rejects when lab disabled", async () => {
    try {
      await client.reproduceContinuityIncident({
        scenario: mkScenario({}),
        required_fault_kind: "worker_death",
      });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.ok(err.status === 503 || err.status === 403 || err.status === 404, `got ${err.status}`);
    }
  });

  it("DLQ group reproduction does NOT require the lab flag (unknown fingerprint still 404s cleanly)", async () => {
    try {
      await client.reproduceDlqGroup("0000000000000000", {
        tenant_id: `dlq-nolab-${uuid().slice(0, 8)}`,
        max_scenarios: 8,
        seed: 1,
      });
      assert.fail("should 404 for unknown fingerprint");
    } catch (err: any) {
      // Must NOT be a lab-gating error (503/403) — DLQ reproduction has no gate.
      assert.equal(err.status, 404);
    }
  });
});

describe("Fault Lab & DLQ Reproduction (enabled)", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_LOG_LEVEL: "error",
        ORCH8_CONTINUITY_LAB_ENABLED: "true",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  // ==================================================================
  // run_fault_lab: deterministic semantics
  // ==================================================================

  describe("run_fault_lab: committed <=> phase=after", () => {
    it("phase=before => committed=false", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "worker_death",
        transition: "request_to_quiescing",
        phase: "before",
        initial_epoch: 0,
      });
      assert.equal(run.committed, false);
    });

    it("phase=after => committed=true", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "worker_death",
        transition: "request_to_quiescing",
        phase: "after",
        initial_epoch: 0,
      });
      assert.equal(run.committed, true);
    });
  });

  describe("run_fault_lab: epoch only advances on exported_to_accepted + committed", () => {
    it("exported_to_accepted + after => final_epoch = initial_epoch + 1", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "stale_owner",
        transition: "exported_to_accepted",
        phase: "after",
        initial_epoch: 5,
      });
      assert.equal(run.initial_epoch, 5);
      assert.equal(run.final_epoch, 6);
    });

    it("exported_to_accepted + before => final_epoch = initial_epoch (not committed)", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "stale_owner",
        transition: "exported_to_accepted",
        phase: "before",
        initial_epoch: 5,
      });
      assert.equal(run.final_epoch, 5);
    });

    for (const [transition] of FIVE_TRANSITIONS.filter(([t]) => t !== "exported_to_accepted")) {
      it(`${transition} + after => epoch does NOT advance`, async () => {
        const run = await client.runContinuityFaultLab({
          profile: "worker_death",
          transition,
          phase: "after",
          initial_epoch: 10,
        });
        assert.equal(run.final_epoch, 10, `${transition} must not advance epoch`);
      });
    }
  });

  describe("run_fault_lab: retry_safe is always true", () => {
    for (const profile of NINE_PROFILES) {
      it(`${profile} is always retry_safe`, async () => {
        const run = await client.runContinuityFaultLab({
          profile,
          transition: "request_to_quiescing",
          phase: "before",
          initial_epoch: 0,
        });
        assert.equal(run.retry_safe, true);
      });
    }
  });

  describe("run_fault_lab: virtual_time_ms special-cases delayed_approval", () => {
    it("delayed_approval => virtual_time_ms = 30000", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "delayed_approval",
        transition: "request_to_quiescing",
        phase: "before",
        initial_epoch: 0,
      });
      assert.equal(run.virtual_time_ms, 30_000);
    });

    for (const profile of NINE_PROFILES.filter((p) => p !== "delayed_approval")) {
      it(`${profile} => virtual_time_ms = 3`, async () => {
        const run = await client.runContinuityFaultLab({
          profile,
          transition: "request_to_quiescing",
          phase: "before",
          initial_epoch: 0,
        });
        assert.equal(run.virtual_time_ms, 3);
      });
    }
  });

  describe("run_fault_lab: trace cites transition boundary and fault name", () => {
    for (const [transition, boundary] of FIVE_TRANSITIONS) {
      it(`${transition} trace mentions boundary "${boundary}"`, async () => {
        const run = await client.runContinuityFaultLab({
          profile: "corrupt_capsule",
          transition,
          phase: "before",
          initial_epoch: 0,
        });
        const trace = (run.trace as string[]).join("\n");
        assert.ok(trace.includes(boundary), `trace missing boundary: ${trace}`);
        assert.ok(trace.includes("corrupt_capsule"), `trace missing fault name: ${trace}`);
      });
    }

    it("phase=before trace has 3 entries (validated, raised_before_commit, committed_once)", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "expired_grant",
        transition: "accepted_to_resumed",
        phase: "before",
        initial_epoch: 0,
      });
      const trace = run.trace as string[];
      assert.equal(trace.length, 3);
      assert.ok(trace[1]!.includes("raised_before_commit"));
      assert.ok(trace[1]!.includes("expired_grant"));
      assert.ok(trace[2]!.includes("durable_write_committed_once"));
    });

    it("phase=after trace has 4 entries (validated, committed, raised_after_commit, cas_rejected)", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "provider_outage",
        transition: "accepted_to_resumed",
        phase: "after",
        initial_epoch: 0,
      });
      const trace = run.trace as string[];
      assert.equal(trace.length, 4);
      assert.ok(trace[1]!.includes("committed"));
      assert.ok(trace[2]!.includes("raised_after_commit"));
      assert.ok(trace[3]!.includes("cas_rejected_as_already_committed"));
    });
  });

  describe("run_fault_lab: initial_epoch round-trips for non-advancing transitions", () => {
    it("arbitrary large initial_epoch is preserved", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "duplicate_delivery",
        transition: "resumed_to_completed",
        phase: "after",
        initial_epoch: 999_999,
      });
      assert.equal(run.initial_epoch, 999_999);
      assert.equal(run.final_epoch, 999_999);
    });

    it("initial_epoch defaults to 0 when omitted", async () => {
      const run = await client.runContinuityFaultLab({
        profile: "offline_device",
        transition: "request_to_quiescing",
        phase: "before",
      });
      assert.equal(run.initial_epoch, 0);
    });
  });

  describe("run_fault_lab: profile echoed back unchanged", () => {
    for (const profile of NINE_PROFILES) {
      it(`echoes profile=${profile}`, async () => {
        const run = await client.runContinuityFaultLab({
          profile,
          transition: "quiescing_to_exported",
          phase: "before",
          initial_epoch: 0,
        });
        assert.equal(run.profile, profile);
      });
    }
  });

  // ==================================================================
  // generate_scenarios: bounds validation
  // ==================================================================

  describe("generate_scenarios: bounded inputs rejected", () => {
    it("rejects > 64 events", async () => {
      try {
        await client.generateContinuityScenarios({
          events: Array.from({ length: 65 }, (_, i) => `e${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts exactly 64 events", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: Array.from({ length: 64 }, (_, i) => `e${i}`),
        max_scenarios: 1,
        seed: 1,
      });
      assert.ok(Array.isArray(scenarios));
    });

    it("rejects > 32 faults", async () => {
      try {
        await client.generateContinuityScenarios({
          faults: Array.from({ length: 33 }, () => ({
            point: "dispatch",
            kind: "worker_death",
            occurrence: 1,
          })),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects a dimension array (input_schema_cases) > 64 entries", async () => {
      try {
        await client.generateContinuityScenarios({
          input_schema_cases: Array.from({ length: 65 }, (_, i) => `case${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects max_steps = 0", async () => {
      try {
        await client.generateContinuityScenarios({
          max_scenarios: 1,
          max_steps: 0,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects max_steps > 10000", async () => {
      try {
        await client.generateContinuityScenarios({
          max_scenarios: 1,
          max_steps: 10_001,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts max_steps = 10000 exactly", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 1,
        max_steps: 10_000,
        seed: 1,
      });
      assert.ok(Array.isArray(scenarios));
    });

    it("rejects max_virtual_time_ms > 86400000", async () => {
      try {
        await client.generateContinuityScenarios({
          max_scenarios: 1,
          max_virtual_time_ms: 86_400_001,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects a handoff_delays_ms entry exceeding max_virtual_time_ms", async () => {
      try {
        await client.generateContinuityScenarios({
          max_scenarios: 1,
          max_virtual_time_ms: 1000,
          handoff_delays_ms: [2000],
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts a handoff_delays_ms entry exactly at max_virtual_time_ms", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 1,
        max_virtual_time_ms: 1000,
        handoff_delays_ms: [1000],
        seed: 1,
      });
      assert.ok(Array.isArray(scenarios));
    });
  });

  describe("generate_scenarios: max_scenarios capped at 256", () => {
    it("requesting 1000 scenarios returns at most 256", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 1000,
        seed: 1,
      });
      assert.ok(scenarios.length <= 256, `got ${scenarios.length}`);
    });

    it("requesting 5 scenarios returns exactly 5", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 5,
        seed: 1,
      });
      assert.equal(scenarios.length, 5);
    });
  });

  describe("generate_scenarios: determinism", () => {
    it("same seed + spec produces byte-identical scenarios", async () => {
      const spec = {
        events: ["payment_received", "inventory_reserved", "shipped"],
        max_scenarios: 10,
        seed: 42,
      };
      const a = await client.generateContinuityScenarios(spec);
      const b = await client.generateContinuityScenarios(spec);
      assert.deepEqual(a, b);
    });

    it("different seeds produce different event ordering for at least one scenario", async () => {
      const base = {
        events: ["a", "b", "c", "d", "e"],
        max_scenarios: 5,
      };
      const a = await client.generateContinuityScenarios({ ...base, seed: 1 });
      const b = await client.generateContinuityScenarios({ ...base, seed: 999 });
      const anyDiffer = a.some(
        (scenario, i) =>
          JSON.stringify((scenario as any).event_order) !==
          JSON.stringify((b[i] as any).event_order),
      );
      assert.ok(anyDiffer, "expected at least one scenario to differ by seed");
    });

    it("empty spec still returns max_scenarios trivial scenarios", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 3,
        seed: 0,
      });
      assert.equal(scenarios.length, 3);
      for (const scenario of scenarios) {
        assert.deepEqual((scenario as any).event_order, []);
        assert.deepEqual((scenario as any).faults, []);
      }
    });
  });

  describe("generate_scenarios: fault cycling", () => {
    it("cycles through N faults via modulo when more scenarios than faults are requested", async () => {
      const scenarios = await client.generateContinuityScenarios({
        faults: [
          { point: "dispatch", kind: "worker_death", occurrence: 1 },
          { point: "external_call", kind: "provider_outage", occurrence: 1 },
        ],
        max_scenarios: 4,
        seed: 0,
      });
      assert.equal(scenarios.length, 4);
      const kinds = scenarios.map((s: any) => s.faults[0]?.kind);
      assert.deepEqual(kinds, [
        "worker_death",
        "provider_outage",
        "worker_death",
        "provider_outage",
      ]);
    });
  });

  // ==================================================================
  // reproduce_incident (continuity endpoint): minimization
  // ==================================================================

  describe("reproduce_incident (continuity): shrinking", () => {
    it("scenario with the required fault reproduces and shrinks to a minimal set", async () => {
      const scenario = mkScenario({
        faults: [
          { point: "dispatch", kind: "worker_death", occurrence: 1 },
          { point: "external_call", kind: "provider_outage", occurrence: 1 },
        ],
        event_order: ["a", "b"],
        retry_attempt: 2,
        handoff_delay_ms: 500,
      });
      const result = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      assert.equal(result.stable_failure_code, "REPRODUCED");
      assert.ok(result.scenario, "minimized scenario must be present");
      const minimized = result.scenario as any;
      // The provider_outage fault is irrelevant to worker_death reproduction
      // and should have been shrunk away.
      assert.equal(minimized.faults.length, 1);
      assert.equal(minimized.faults[0].kind, "worker_death");
    });

    it("scenario without the required fault does not reproduce", async () => {
      const scenario = mkScenario({
        faults: [{ point: "dispatch", kind: "provider_outage", occurrence: 1 }],
      });
      const result = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "inconclusive");
      assert.equal(result.stable_failure_code, "NOT_REPRODUCED");
      assert.equal(result.scenario, null);
      assert.equal(result.attempts, 1);
    });

    it("empty scenario (no faults) never reproduces any required kind", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({}),
        required_fault_kind: "corrupt_capsule",
      });
      assert.equal(result.status, "inconclusive");
    });

    it("irrelevant event_order entries are shrunk away when only faults matter", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "storage_before_write", kind: "database_timeout", occurrence: 1 }],
          event_order: ["irrelevant1", "irrelevant2", "irrelevant3"],
          policy_facts: ["irrelevant_fact"],
          retry_attempt: 3,
          handoff_delay_ms: 1000,
        }),
        required_fault_kind: "database_timeout",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.deepEqual(minimized.event_order, []);
      assert.deepEqual(minimized.policy_facts, []);
      assert.equal(minimized.retry_attempt, 0);
      assert.equal(minimized.handoff_delay_ms, 0);
    });
  });

  // ==================================================================
  // DLQ group reproduction: validation
  // ==================================================================

  describe("DLQ reproduction: fingerprint validation", () => {
    it("rejects a fingerprint shorter than 16 hex chars", async () => {
      try {
        await client.reproduceDlqGroup("abc123", {
          tenant_id: `dlq-fp-${uuid().slice(0, 8)}`,
          max_scenarios: 8,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects a fingerprint longer than 16 hex chars", async () => {
      try {
        await client.reproduceDlqGroup("0123456789abcdef00", {
          tenant_id: `dlq-fp-${uuid().slice(0, 8)}`,
          max_scenarios: 8,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects a non-hex fingerprint of the right length", async () => {
      try {
        await client.reproduceDlqGroup("zzzzzzzzzzzzzzzz", {
          tenant_id: `dlq-fp-${uuid().slice(0, 8)}`,
          max_scenarios: 8,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts a well-formed 16-hex-char fingerprint shape (404s on unknown, not 400)", async () => {
      try {
        await client.reproduceDlqGroup("0123456789abcdef", {
          tenant_id: `dlq-fp-${uuid().slice(0, 8)}`,
          max_scenarios: 8,
          seed: 1,
        });
        assert.fail("should 404");
      } catch (err: any) {
        assert.equal(err.status, 404);
      }
    });
  });

  describe("DLQ reproduction: max_scenarios bounds", () => {
    it("rejects max_scenarios = 0", async () => {
      try {
        await client.reproduceDlqGroup("0123456789abcdef", {
          tenant_id: `dlq-ms-${uuid().slice(0, 8)}`,
          max_scenarios: 0,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects max_scenarios = 65", async () => {
      try {
        await client.reproduceDlqGroup("0123456789abcdef", {
          tenant_id: `dlq-ms-${uuid().slice(0, 8)}`,
          max_scenarios: 65,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("rejects allowlisted_fields with > 256 entries", async () => {
      try {
        await client.reproduceDlqGroup("0123456789abcdef", {
          tenant_id: `dlq-al-${uuid().slice(0, 8)}`,
          allowlisted_fields: Array.from({ length: 257 }, (_, i) => `field${i}`),
          max_scenarios: 8,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });
  });

  // ==================================================================
  // DLQ group reproduction: real failure -> reproduction lifecycle
  // ==================================================================

  describe("DLQ reproduction: real failures reproduce by error class", () => {
    async function failInstanceWithMessage(tenantId: string, message: string) {
      const sequence = testSequence(
        `dlq-repro-${uuid().slice(0, 8)}`,
        [step("s1", "fail", { message, retryable: false })],
        { tenantId },
      );
      const created = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
        next_fire_at: new Date(Date.now() + 2_000).toISOString(),
      });
      // Reproduction requires the source instance to have a linked continuity
      // execution AND a retained checkpoint — without both,
      // `extract_reproduction_fixture` reports missing_evidence and
      // short-circuits to insufficient_evidence regardless of fault mapping.
      await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      await client.saveCheckpoint(instance.id, {
        block_id: "s1",
        completed_blocks: [],
        context_snapshot: {},
      });
      await client.waitForState(instance.id, "scheduled");
      await client.waitForState(instance.id, "failed", { timeoutMs: 10_000 });
      return instance.id as string;
    }

    it("a 'timeout' failure maps to database_timeout and reproduces", async () => {
      const tenantId = `dlq-timeout-${uuid().slice(0, 8)}`;
      const instanceId = await failInstanceWithMessage(tenantId, "operation timed out");

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instanceId),
      );
      assert.ok(group, "expected a DLQ group for the failed instance");

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 16,
        seed: 7,
      });
      assert.equal(reproduction.status, "reproduced", JSON.stringify(reproduction));
      assert.equal(reproduction.source_instance_id, instanceId);
      assert.ok(
        (reproduction.scenario as any).faults.some((f: any) => f.kind === "database_timeout"),
      );
    });

    it("an application-class failure (no fault mapping) yields not_reproduced with missing supported_fault_profile", async () => {
      const tenantId = `dlq-app-${uuid().slice(0, 8)}`;
      // A message with none of the classifier's recognized substrings falls
      // through to the generic "unknown" -> ErrorClass::Application bucket,
      // which fault_for_failure() explicitly maps to None. With a complete
      // fixture (execution + checkpoint present) but no applicable fault
      // profile, the outcome is "not_reproduced" — NOT "insufficient_evidence"
      // (that status is reserved for an incomplete fixture).
      const instanceId = await failInstanceWithMessage(tenantId, "widget frobnication failed");

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instanceId),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 16,
        seed: 3,
      });
      assert.equal(reproduction.status, "not_reproduced", JSON.stringify(reproduction));
      assert.ok(
        (reproduction.missing_evidence as string[]).includes("supported_fault_profile"),
      );
      assert.equal(reproduction.scenario, null);
    });

    it("a failure with no linked continuity execution yields insufficient_evidence", async () => {
      const tenantId = `dlq-noexec-${uuid().slice(0, 8)}`;
      // No createContinuityExecution / saveCheckpoint here — the fixture
      // extraction can't find retained evidence at all, which must report
      // insufficient_evidence regardless of the error's fault mapping.
      const sequence = testSequence(
        `dlq-noexec-${uuid().slice(0, 8)}`,
        [step("s1", "fail", { message: "connection timeout", retryable: false })],
        { tenantId },
      );
      const created = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await client.waitForState(instance.id, "failed", { timeoutMs: 10_000 });

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instance.id),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 16,
        seed: 6,
      });
      assert.equal(reproduction.status, "insufficient_evidence", JSON.stringify(reproduction));
      assert.equal(reproduction.scenario, null);
      assert.ok((reproduction.missing_evidence as string[]).length > 0);
    });

    it("reproduction never leaks secret-shaped values from the original params", async () => {
      const tenantId = `dlq-secret-${uuid().slice(0, 8)}`;
      const sequence = testSequence(
        `dlq-secret-${uuid().slice(0, 8)}`,
        [
          step("s1", "assert", {
            condition: "false",
            message: "connection timeout to upstream",
            api_key: "sk-super-secret-value-must-not-leak",
          }),
        ],
        { tenantId },
      );
      const created = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
        next_fire_at: new Date(Date.now() + 2_000).toISOString(),
      });
      await client.createContinuityExecution({
        tenant_id: tenantId,
        instance_id: instance.id,
        runtime_id: uuid(),
      });
      await client.waitForState(instance.id, "failed", { timeoutMs: 10_000 });

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instance.id),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 16,
        seed: 11,
      });
      assert.ok(!JSON.stringify(reproduction).includes("sk-super-secret-value-must-not-leak"));
    });

    it("suggested_remediation is present and non-empty for a reproduced incident", async () => {
      const tenantId = `dlq-remed-${uuid().slice(0, 8)}`;
      const instanceId = await failInstanceWithMessage(tenantId, "request timeout waiting for reply");

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instanceId),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 8,
        seed: 5,
      });
      assert.ok(Array.isArray(reproduction.suggested_remediation));
      assert.ok((reproduction.suggested_remediation as unknown[]).length > 0);
    });

    it("stable_failure_code on the reproduction record matches the original error code", async () => {
      const tenantId = `dlq-code-${uuid().slice(0, 8)}`;
      const instanceId = await failInstanceWithMessage(tenantId, "deadline exceeded waiting for lock");

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instanceId),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 8,
        seed: 2,
      });
      assert.equal(reproduction.stable_failure_code, "TIMEOUT");
    });
  });

  describe("DLQ reproduction: listing persisted reproductions", () => {
    it("a reproduction is retrievable via list_reproductions afterward", async () => {
      const tenantId = `dlq-list-${uuid().slice(0, 8)}`;
      const sequence = testSequence(
        `dlq-list-${uuid().slice(0, 8)}`,
        [step("s1", "fail", { message: "connection timeout", retryable: false })],
        { tenantId },
      );
      const created = await client.createSequence(sequence);
      const instance = await client.createInstance({
        sequence_id: created.id,
        tenant_id: tenantId,
        namespace: "default",
      });
      await client.waitForState(instance.id, "failed", { timeoutMs: 10_000 });

      const groups = await client.listDlqGroups(tenantId);
      const group = groups.find((g) =>
        (g.sample_instance_ids as unknown[]).includes(instance.id),
      );
      assert.ok(group);

      const reproduction = await client.reproduceDlqGroup(String(group!.fingerprint), {
        tenant_id: tenantId,
        allowlisted_fields: [],
        max_scenarios: 8,
        seed: 4,
      });

      const listed = await client.listDlqReproductions(String(group!.fingerprint), tenantId);
      assert.ok(listed.some((r) => r.id === reproduction.id));
    });

    it("list_reproductions for a fingerprint with none attached returns an empty array", async () => {
      const tenantId = `dlq-empty-${uuid().slice(0, 8)}`;
      const listed = await client.listDlqReproductions("fedcba9876543210", tenantId);
      assert.deepEqual(listed, []);
    });

    it("list_reproductions is tenant-scoped: another tenant cannot see it via a shared fingerprint collision path", async () => {
      const tenantA = `dlq-tenA-${uuid().slice(0, 8)}`;
      const tenantB = `dlq-tenB-${uuid().slice(0, 8)}`;
      const listedB = await client.listDlqReproductions("1111111111111111", tenantB);
      const listedA = await client.listDlqReproductions("1111111111111111", tenantA);
      assert.deepEqual(listedA, []);
      assert.deepEqual(listedB, []);
    });
  });
});
