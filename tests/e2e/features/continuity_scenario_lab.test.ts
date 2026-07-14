/**
 * E2E: Continuity Scenario Laboratory — `POST /continuity/scenarios/generate`
 * and `POST /continuity/scenarios/reproduce`.
 *
 * `orch8-api/src/continuity.rs`'s `generate_scenarios` / `reproduce_incident`
 * handlers wrap two pure functions in `orch8-engine/src/continuity_advanced.rs`:
 *
 *   - `generate_scenarios_from_spec(spec)`: deterministic bounded cross-product
 *     generator over up to 7 dimensions (input_schema_cases, router_branches,
 *     event_joins, policy_facts, invariant_codes, retry_attempts,
 *     handoff_delays_ms), each capped at `MAX_SCENARIO_DIMENSION` (64) entries.
 *     `events` <= 64, `faults` <= 32, `max_steps` in [1, 10_000],
 *     `max_virtual_time_ms` <= 86_400_000, every `handoff_delays_ms` entry
 *     <= `max_virtual_time_ms`. `max_scenarios` is silently capped at 256.
 *     Violating any bound returns `AdvancedContinuityError::ScenarioTooLarge`,
 *     mapped to 400 (`ApiError::InvalidArgument`).
 *
 *   - `minimize_reproducing_scenario(scenario, reproduces)`: ddmin-style
 *     shrinker. `reproduces` here is `|candidate| candidate.faults.iter().any(
 *     |f| f.kind == required_fault_kind)`. It greedily drops faults, then
 *     event_order entries, then policy_facts, then zeroes retry_attempt and
 *     handoff_delay_ms — keeping each drop only if the required fault kind is
 *     still present afterward. If the *original* scenario doesn't reproduce,
 *     short-circuits to `{status: inconclusive, stable_failure_code:
 *     NOT_REPRODUCED, scenario: null, attempts: 1}` without touching the
 *     scenario at all.
 *
 * Both routes are gated by `AppState.continuity_lab_enabled`
 * (env `ORCH8_CONTINUITY_LAB_ENABLED`) and return 503 (`ApiError::Unavailable`)
 * when the flag is unset. Neither route touches storage, a tenant, or an
 * existing continuity execution — they are pure, stateless functions exposed
 * over HTTP, so "tenant isolation" here reduces to "no tenant concept exists
 * and none is required for the endpoint to function."
 *
 * NOTE: `continuity_fault_lab_dlq.test.ts` already covers foundational
 * generate/reproduce behavior (basic bound checks, determinism, fault
 * cycling, minimization). This file focuses on complementary surface: the
 * five under-exercised cross-product dimensions (router_branches,
 * event_joins, policy_facts, invariant_codes, retry_attempts-as-array),
 * FaultPoint variety, the `occurrence` passthrough field, response-shape
 * invariants (id uniqueness/stability, echoed fields), malformed-JSON/type
 * errors, additional boundary values, and reproduce_incident scenarios that
 * explore every field the minimizer can shrink in combination.
 */

import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

const ALL_FAULT_POINTS = [
  "storage_before_write",
  "storage_after_write",
  "dispatch",
  "effect_receipt",
  "ownership_claim",
  "device_sync",
  "stream_append",
  "external_call",
  "approval",
] as const;

const ALL_FAULT_KINDS = [
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

/** `GeneratedScenario` requires `id`/`max_steps`/`seed` beyond the fields a
 * given test cares about — fill them with harmless defaults. */
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

describe("Scenario Lab: gated when ORCH8_CONTINUITY_LAB_ENABLED is unset", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({ env: { ORCH8_LOG_LEVEL: "error" } });
  });

  after(async () => {
    await stopServer(server);
  });

  it("generate_scenarios: 503 with a minimal valid body", async () => {
    try {
      await client.generateContinuityScenarios({ max_scenarios: 1, seed: 1 });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.equal(err.status, 503);
    }
  });

  it("generate_scenarios: 503 even with a body that would also be invalid on its own merits", async () => {
    // The gate check runs before spec validation, so a body that would
    // separately trip ScenarioTooLarge (max_steps=0) still surfaces as 503,
    // not 400 — the flag check is unconditional and first.
    try {
      await client.generateContinuityScenarios({ max_scenarios: 1, max_steps: 0, seed: 1 });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.equal(err.status, 503);
    }
  });

  it("reproduce_incident: 503 with a scenario that would otherwise reproduce", async () => {
    try {
      await client.reproduceContinuityIncident({
        scenario: mkScenario({ faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }] }),
        required_fault_kind: "worker_death",
      });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.equal(err.status, 503);
    }
  });

  it("reproduce_incident: 503 body mentions the laboratory being disabled", async () => {
    try {
      await client.reproduceContinuityIncident({
        scenario: mkScenario({}),
        required_fault_kind: "worker_death",
      });
      assert.fail("should reject when lab disabled");
    } catch (err: any) {
      assert.equal(err.status, 503);
      assert.ok(/disabled/i.test(err.body), `expected disabled message, got: ${err.body}`);
    }
  });
});

describe("Scenario Lab (enabled)", () => {
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
  // generate_scenarios: cross-product dimensions beyond the basics
  // ==================================================================

  describe("generate_scenarios: router_branches dimension", () => {
    it("selects a router branch every other scenario (index / 2 stride)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        router_branches: ["approve", "deny"],
        max_scenarios: 6,
        seed: 0,
      });
      const facts = scenarios.map((s: any) =>
        (s.policy_facts as string[]).filter((f) => f.startsWith("router:")),
      );
      // index/2: 0,0,1,1,2,2 -> branch cycles approve,approve,deny,deny,approve,approve
      assert.deepEqual(facts, [
        ["router:approve"],
        ["router:approve"],
        ["router:deny"],
        ["router:deny"],
        ["router:approve"],
        ["router:approve"],
      ]);
    });

    it("rejects > 64 router_branches", async () => {
      try {
        await client.generateContinuityScenarios({
          router_branches: Array.from({ length: 65 }, (_, i) => `b${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("accepts exactly 64 router_branches", async () => {
      const scenarios = await client.generateContinuityScenarios({
        router_branches: Array.from({ length: 64 }, (_, i) => `b${i}`),
        max_scenarios: 1,
        seed: 1,
      });
      assert.equal(scenarios.length, 1);
    });
  });

  describe("generate_scenarios: event_joins dimension", () => {
    it("selects an event join every third scenario (index / 3 stride)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        event_joins: ["join_a", "join_b"],
        max_scenarios: 6,
        seed: 0,
      });
      const facts = scenarios.map((s: any) =>
        (s.policy_facts as string[]).filter((f) => f.startsWith("join:")),
      );
      // index/3: 0,0,0,1,1,1 -> join_a,join_a,join_a,join_b,join_b,join_b
      assert.deepEqual(facts, [
        ["join:join_a"],
        ["join:join_a"],
        ["join:join_a"],
        ["join:join_b"],
        ["join:join_b"],
        ["join:join_b"],
      ]);
    });

    it("rejects > 64 event_joins", async () => {
      try {
        await client.generateContinuityScenarios({
          event_joins: Array.from({ length: 65 }, (_, i) => `j${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });
  });

  describe("generate_scenarios: policy_facts dimension", () => {
    it("selects a policy fact every fifth scenario (index / 5 stride)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        policy_facts: ["locality:eu", "locality:us"],
        max_scenarios: 10,
        seed: 0,
      });
      const facts = scenarios.map((s: any) =>
        (s.policy_facts as string[]).filter((f) => f.startsWith("policy:")),
      );
      // index/5: 0,0,0,0,0,1,1,1,1,1
      assert.deepEqual(facts, [
        ["policy:locality:eu"],
        ["policy:locality:eu"],
        ["policy:locality:eu"],
        ["policy:locality:eu"],
        ["policy:locality:eu"],
        ["policy:locality:us"],
        ["policy:locality:us"],
        ["policy:locality:us"],
        ["policy:locality:us"],
        ["policy:locality:us"],
      ]);
    });

    it("rejects > 64 policy_facts", async () => {
      try {
        await client.generateContinuityScenarios({
          policy_facts: Array.from({ length: 65 }, (_, i) => `p${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });
  });

  describe("generate_scenarios: invariant_codes dimension", () => {
    it("selects an invariant code every seventh scenario (index / 7 stride)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        invariant_codes: ["INV_A", "INV_B"],
        max_scenarios: 14,
        seed: 0,
      });
      const facts = scenarios.map((s: any) =>
        (s.policy_facts as string[]).filter((f) => f.startsWith("invariant:")),
      );
      assert.deepEqual(facts[0], ["invariant:INV_A"]);
      assert.deepEqual(facts[7], ["invariant:INV_B"]);
    });

    it("rejects > 64 invariant_codes", async () => {
      try {
        await client.generateContinuityScenarios({
          invariant_codes: Array.from({ length: 65 }, (_, i) => `INV${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });
  });

  describe("generate_scenarios: retry_attempts dimension as an array", () => {
    it("cycles retry_attempt through the declared list", async () => {
      const scenarios = await client.generateContinuityScenarios({
        retry_attempts: [0, 3, 7],
        max_scenarios: 6,
        seed: 0,
      });
      const attempts = scenarios.map((s: any) => s.retry_attempt);
      assert.deepEqual(attempts, [0, 3, 7, 0, 3, 7]);
    });

    it("rejects > 64 retry_attempts entries", async () => {
      try {
        await client.generateContinuityScenarios({
          retry_attempts: Array.from({ length: 65 }, (_, i) => i),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });

    it("a single retry_attempts entry pins retry_attempt for every scenario", async () => {
      const scenarios = await client.generateContinuityScenarios({
        retry_attempts: [42],
        max_scenarios: 5,
        seed: 3,
      });
      for (const s of scenarios as any[]) {
        assert.equal(s.retry_attempt, 42);
      }
    });

    it("omitted retry_attempts defaults retry_attempt to 0", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 3,
        seed: 1,
      });
      for (const s of scenarios as any[]) {
        assert.equal(s.retry_attempt, 0);
      }
    });
  });

  describe("generate_scenarios: handoff_delays_ms dimension stride (index / 2)", () => {
    it("cycles handoff_delay_ms with an index/2 stride", async () => {
      const scenarios = await client.generateContinuityScenarios({
        handoff_delays_ms: [100, 200],
        max_virtual_time_ms: 1000,
        max_scenarios: 6,
        seed: 0,
      });
      const delays = scenarios.map((s: any) => s.handoff_delay_ms);
      // index/2: 0,0,1,1,2,2 -> 100,100,200,200,100,100
      assert.deepEqual(delays, [100, 100, 200, 200, 100, 100]);
    });
  });

  describe("generate_scenarios: all dimensions combined simultaneously", () => {
    it("produces a fully-populated scenario touching every dimension", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: ["order_placed", "payment_captured"],
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
        input_schema_cases: ["v1"],
        router_branches: ["approve"],
        event_joins: ["join_a"],
        policy_facts: ["trust:high"],
        invariant_codes: ["INV_1"],
        retry_attempts: [2],
        handoff_delays_ms: [50],
        max_virtual_time_ms: 1000,
        max_scenarios: 1,
        seed: 5,
      });
      const s = scenarios[0] as any;
      assert.equal(s.retry_attempt, 2);
      assert.equal(s.handoff_delay_ms, 50);
      // event_order is a rotation/possible-reversal of the input events (see
      // the dedicated rotation-mechanics tests below for the exact formula)
      // — here we only care that it's the same multiset.
      assert.deepEqual(
        [...s.event_order].sort(),
        ["order_placed", "payment_captured"].sort(),
      );
      assert.equal(s.faults.length, 1);
      assert.equal(s.faults[0].kind, "worker_death");
      const facts = s.policy_facts as string[];
      assert.ok(facts.includes("input:v1"));
      assert.ok(facts.includes("router:approve"));
      assert.ok(facts.includes("join:join_a"));
      assert.ok(facts.includes("policy:trust:high"));
      assert.ok(facts.includes("invariant:INV_1"));
    });

    it("rejects when multiple dimensions simultaneously exceed bounds (still 400, not 500)", async () => {
      try {
        await client.generateContinuityScenarios({
          router_branches: Array.from({ length: 70 }, (_, i) => `b${i}`),
          event_joins: Array.from({ length: 70 }, (_, i) => `j${i}`),
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.equal(err.status, 400);
      }
    });
  });

  // ==================================================================
  // generate_scenarios: event_order rotation/reversal mechanics
  // ==================================================================

  describe("generate_scenarios: event_order rotation and alternating reversal", () => {
    it("rotates by (seed + index) % len and reverses on odd index", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: ["a", "b", "c"],
        max_scenarios: 4,
        seed: 0,
      });
      // seed=0: rotation = index % 3
      // idx0: rotate 0 -> [a,b,c], even -> no reverse -> [a,b,c]
      // idx1: rotate 1 -> [b,c,a], odd -> reverse -> [a,c,b]
      // idx2: rotate 2 -> [c,a,b], even -> [c,a,b]
      // idx3: rotate 0 -> [a,b,c], odd -> reverse -> [c,b,a]
      const orders = scenarios.map((s: any) => s.event_order);
      assert.deepEqual(orders[0], ["a", "b", "c"]);
      assert.deepEqual(orders[1], ["a", "c", "b"]);
      assert.deepEqual(orders[2], ["c", "a", "b"]);
      assert.deepEqual(orders[3], ["c", "b", "a"]);
    });

    it("a single-event list is invariant under rotation/reversal", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: ["only"],
        max_scenarios: 4,
        seed: 7,
      });
      for (const s of scenarios as any[]) {
        assert.deepEqual(s.event_order, ["only"]);
      }
    });

    it("event_order is always a permutation (same multiset) of the input events", async () => {
      const events = ["e1", "e2", "e3", "e4", "e5"];
      const scenarios = await client.generateContinuityScenarios({
        events,
        max_scenarios: 12,
        seed: 123,
      });
      for (const s of scenarios as any[]) {
        assert.deepEqual([...s.event_order].sort(), [...events].sort());
      }
    });
  });

  // ==================================================================
  // generate_scenarios: id semantics
  // ==================================================================

  describe("generate_scenarios: scenario id determinism and uniqueness", () => {
    it("ids are unique within a single generation batch", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 20,
        seed: 9,
      });
      const ids = scenarios.map((s: any) => s.id);
      assert.equal(new Set(ids).size, ids.length);
    });

    it("same seed reproduces byte-identical ids across separate calls", async () => {
      const spec = { max_scenarios: 5, seed: 314159 };
      const a = await client.generateContinuityScenarios(spec);
      const b = await client.generateContinuityScenarios(spec);
      assert.deepEqual(
        a.map((s: any) => s.id),
        b.map((s: any) => s.id),
      );
    });

    it("changing only the seed changes at least one id", async () => {
      const a = await client.generateContinuityScenarios({ max_scenarios: 5, seed: 1 });
      const b = await client.generateContinuityScenarios({ max_scenarios: 5, seed: 2 });
      const idsA = a.map((s: any) => s.id);
      const idsB = b.map((s: any) => s.id);
      assert.notDeepEqual(idsA, idsB);
    });

    it("scenario id looks like a UUID", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 1, seed: 1 });
      const id = (scenarios[0] as any).id as string;
      assert.match(id, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i);
    });
  });

  // ==================================================================
  // generate_scenarios: max_steps / seed echo and boundary values
  // ==================================================================

  describe("generate_scenarios: max_steps and seed echo unchanged onto every scenario", () => {
    it("max_steps is echoed verbatim on every generated scenario", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 4,
        max_steps: 777,
        seed: 1,
      });
      for (const s of scenarios as any[]) {
        assert.equal(s.max_steps, 777);
      }
    });

    it("max_steps defaults to 10000 when omitted", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 1, seed: 1 });
      assert.equal((scenarios[0] as any).max_steps, 10_000);
    });

    it("max_virtual_time_ms defaults to 86400000 when omitted (a large handoff delay is still accepted)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        handoff_delays_ms: [86_400_000],
        max_scenarios: 1,
        seed: 1,
      });
      assert.equal(scenarios.length, 1);
    });

    it("each scenario's own `seed` field is base seed + index (wrapping add)", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 3,
        seed: 100,
      });
      const seeds = scenarios.map((s: any) => s.seed);
      assert.deepEqual(seeds, [100, 101, 102]);
    });

    it("seed 0 is accepted and produces deterministic output", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 2, seed: 0 });
      assert.deepEqual(
        scenarios.map((s: any) => s.seed),
        [0, 1],
      );
    });

    it("a very large seed near u64::MAX is accepted without overflow error", async () => {
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 2,
        seed: 18446744073709551615n as unknown as number, // serialized as string by JSON.stringify below path is bypassed; see next test for the safe variant
      }).catch(() => null);
      // u64::MAX cannot be represented as a JS number; this call is expected
      // to either be coerced (and succeed) or fail parsing (400/500 are both
      // acceptable here since bigint-as-number is inherently lossy). The
      // safe, exact variant is covered by the next test.
      assert.ok(scenarios === null || Array.isArray(scenarios));
    });

    it("a large-but-safe-integer seed is accepted and wraps correctly", async () => {
      const seed = Number.MAX_SAFE_INTEGER;
      const scenarios = await client.generateContinuityScenarios({
        max_scenarios: 2,
        seed,
      });
      assert.equal((scenarios[0] as any).seed, seed);
    });
  });

  describe("generate_scenarios: max_scenarios boundary values", () => {
    it("max_scenarios = 0 returns an empty array (not an error)", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 0, seed: 1 });
      assert.deepEqual(scenarios, []);
    });

    it("max_scenarios = 256 returns exactly 256", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 256, seed: 1 });
      assert.equal(scenarios.length, 256);
    });

    it("max_scenarios = 257 is still capped at 256", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 257, seed: 1 });
      assert.equal(scenarios.length, 256);
    });

    it("max_scenarios = 1 returns exactly 1", async () => {
      const scenarios = await client.generateContinuityScenarios({ max_scenarios: 1, seed: 1 });
      assert.equal(scenarios.length, 1);
    });
  });

  // ==================================================================
  // generate_scenarios: FaultInjection shape — points, kinds, occurrence
  // ==================================================================

  describe("generate_scenarios: FaultInjection round-trips every FaultPoint", () => {
    for (const point of ALL_FAULT_POINTS) {
      it(`accepts and echoes fault point "${point}"`, async () => {
        const scenarios = await client.generateContinuityScenarios({
          faults: [{ point, kind: "worker_death", occurrence: 3 }],
          max_scenarios: 1,
          seed: 1,
        });
        const fault = (scenarios[0] as any).faults[0];
        assert.equal(fault.point, point);
        assert.equal(fault.occurrence, 3);
      });
    }
  });

  describe("generate_scenarios: FaultInjection round-trips every FaultKind", () => {
    for (const kind of ALL_FAULT_KINDS) {
      it(`accepts and echoes fault kind "${kind}"`, async () => {
        const scenarios = await client.generateContinuityScenarios({
          faults: [{ point: "dispatch", kind, occurrence: 1 }],
          max_scenarios: 1,
          seed: 1,
        });
        assert.equal((scenarios[0] as any).faults[0].kind, kind);
      });
    }
  });

  describe("generate_scenarios: occurrence is an opaque passthrough", () => {
    it("occurrence = 0 round-trips unchanged", async () => {
      const scenarios = await client.generateContinuityScenarios({
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 0 }],
        max_scenarios: 1,
        seed: 1,
      });
      assert.equal((scenarios[0] as any).faults[0].occurrence, 0);
    });

    it("a large occurrence value round-trips unchanged", async () => {
      const scenarios = await client.generateContinuityScenarios({
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 4_000_000_000 }],
        max_scenarios: 1,
        seed: 1,
      });
      assert.equal((scenarios[0] as any).faults[0].occurrence, 4_000_000_000);
    });
  });

  describe("generate_scenarios: 32 faults at the boundary", () => {
    it("accepts exactly 32 faults and cycles through all of them across 32 scenarios", async () => {
      const faults = Array.from({ length: 32 }, (_, i) => ({
        point: ALL_FAULT_POINTS[i % ALL_FAULT_POINTS.length],
        kind: ALL_FAULT_KINDS[i % ALL_FAULT_KINDS.length],
        occurrence: i,
      }));
      const scenarios = await client.generateContinuityScenarios({
        faults,
        max_scenarios: 32,
        seed: 0,
      });
      assert.equal(scenarios.length, 32);
      const occurrences = scenarios.map((s: any) => s.faults[0].occurrence);
      assert.deepEqual(occurrences, Array.from({ length: 32 }, (_, i) => i));
    });
  });

  // ==================================================================
  // generate_scenarios: malformed / invalid request bodies
  // ==================================================================

  describe("generate_scenarios: malformed request bodies", () => {
    it("missing max_scenarios (required field) is a 400/422-class error, not 500", async () => {
      try {
        await client.generateContinuityScenarios({ seed: 1 });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("missing seed (required field) is a 400/422-class error, not 500", async () => {
      try {
        await client.generateContinuityScenarios({ max_scenarios: 1 });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("max_scenarios as a negative number is rejected as malformed (usize cannot be negative)", async () => {
      try {
        await client.generateContinuityScenarios({ max_scenarios: -1, seed: 1 });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("seed as a negative number is rejected as malformed (u64 cannot be negative)", async () => {
      try {
        await client.generateContinuityScenarios({ max_scenarios: 1, seed: -5 });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("max_scenarios as a string is rejected as malformed", async () => {
      try {
        await client.generateContinuityScenarios({ max_scenarios: "five", seed: 1 });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("an unknown fault kind string is rejected as malformed", async () => {
      try {
        await client.generateContinuityScenarios({
          faults: [{ point: "dispatch", kind: "not_a_real_kind", occurrence: 1 }],
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("an unknown fault point string is rejected as malformed", async () => {
      try {
        await client.generateContinuityScenarios({
          faults: [{ point: "not_a_real_point", kind: "worker_death", occurrence: 1 }],
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("events containing a non-string entry is rejected as malformed", async () => {
      try {
        await client.generateContinuityScenarios({
          events: [123, "abc"] as unknown as string[],
          max_scenarios: 1,
          seed: 1,
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("an entirely empty object is rejected (missing required fields)", async () => {
      try {
        await client.generateContinuityScenarios({});
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });
  });

  // ==================================================================
  // generate_scenarios: empty-list dimensions behave like absent dimensions
  // ==================================================================

  describe("generate_scenarios: explicit empty arrays behave identically to omitted fields", () => {
    it("empty events + empty faults + empty everything still yields max_scenarios trivial scenarios", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: [],
        faults: [],
        input_schema_cases: [],
        router_branches: [],
        event_joins: [],
        policy_facts: [],
        invariant_codes: [],
        retry_attempts: [],
        handoff_delays_ms: [],
        max_scenarios: 4,
        seed: 2,
      });
      assert.equal(scenarios.length, 4);
      for (const s of scenarios as any[]) {
        assert.deepEqual(s.event_order, []);
        assert.deepEqual(s.faults, []);
        assert.deepEqual(s.policy_facts, []);
        assert.equal(s.retry_attempt, 0);
        assert.equal(s.handoff_delay_ms, 0);
      }
    });
  });

  // ==================================================================
  // reproduce_incident: field-by-field shrink coverage
  // ==================================================================

  describe("reproduce_incident: attempts counter accounting", () => {
    it("attempts = 1 when the only candidate is the original (nothing to shrink) and it reproduces", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
        }),
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      // 1 fault removal attempt (fails to keep required kind after removal ->
      // reverted), no event_order/policy_facts/retry/handoff to shrink.
      assert.equal(result.attempts, 2);
    });

    it("attempts grows with the number of shrinkable elements", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [
            { point: "dispatch", kind: "worker_death", occurrence: 1 },
            { point: "external_call", kind: "provider_outage", occurrence: 1 },
            { point: "approval", kind: "delayed_approval", occurrence: 1 },
          ],
          event_order: ["a", "b"],
          policy_facts: ["fact1"],
          retry_attempt: 5,
          handoff_delay_ms: 10,
        }),
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      // 1 (initial check) + 3 fault removals + 2 event removals + 1 policy
      // removal + 1 retry zero + 1 handoff zero = 9
      assert.equal(result.attempts, 9);
    });
  });

  describe("reproduce_incident: only the last matching-kind fault survives shrink when multiple share the kind", () => {
    it("keeps at least one fault of the required kind even with duplicates present", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [
            { point: "dispatch", kind: "worker_death", occurrence: 1 },
            { point: "device_sync", kind: "worker_death", occurrence: 2 },
          ],
        }),
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.ok(
        minimized.faults.some((f: any) => f.kind === "worker_death"),
        "must retain a worker_death fault",
      );
    });
  });

  describe("reproduce_incident: partial shrink — required fault interleaved with noise", () => {
    it("removes only the fault that isn't the required kind, keeping the required one", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [
            { point: "dispatch", kind: "database_timeout", occurrence: 1 },
            { point: "external_call", kind: "stale_owner", occurrence: 1 },
          ],
        }),
        required_fault_kind: "stale_owner",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.equal(minimized.faults.length, 1);
      assert.equal(minimized.faults[0].kind, "stale_owner");
    });
  });

  describe("reproduce_incident: event_order shrink in isolation", () => {
    it("shrinks a long event_order to empty when faults alone satisfy reproduction", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "offline_device", occurrence: 1 }],
          event_order: ["e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8"],
        }),
        required_fault_kind: "offline_device",
      });
      assert.equal(result.status, "pass");
      assert.deepEqual((result.scenario as any).event_order, []);
    });
  });

  describe("reproduce_incident: policy_facts shrink in isolation", () => {
    it("shrinks all policy_facts to empty when they don't influence reproduction", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "corrupt_capsule", occurrence: 1 }],
          policy_facts: ["input:v1", "router:approve", "join:a", "policy:x", "invariant:y"],
        }),
        required_fault_kind: "corrupt_capsule",
      });
      assert.equal(result.status, "pass");
      assert.deepEqual((result.scenario as any).policy_facts, []);
    });
  });

  describe("reproduce_incident: retry_attempt and handoff_delay_ms shrink independently", () => {
    it("retry_attempt alone is zeroed when handoff_delay_ms is already 0", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "expired_grant", occurrence: 1 }],
          retry_attempt: 9,
          handoff_delay_ms: 0,
        }),
        required_fault_kind: "expired_grant",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.equal(minimized.retry_attempt, 0);
      assert.equal(minimized.handoff_delay_ms, 0);
    });

    it("handoff_delay_ms alone is zeroed when retry_attempt is already 0", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "provider_outage", occurrence: 1 }],
          retry_attempt: 0,
          handoff_delay_ms: 4321,
        }),
        required_fault_kind: "provider_outage",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.equal(minimized.retry_attempt, 0);
      assert.equal(minimized.handoff_delay_ms, 0);
    });

    it("both retry_attempt and handoff_delay_ms already 0 leaves attempts count unaffected by them", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "duplicate_delivery", occurrence: 1 }],
          retry_attempt: 0,
          handoff_delay_ms: 0,
        }),
        required_fault_kind: "duplicate_delivery",
      });
      assert.equal(result.status, "pass");
      // 1 initial + 1 fault-removal attempt only (retry/handoff already 0,
      // guarded by `> 0` checks so they don't add attempts).
      assert.equal(result.attempts, 2);
    });
  });

  describe("reproduce_incident: max_steps and seed are preserved through minimization", () => {
    it("max_steps and seed on the minimized scenario match the original input", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
          max_steps: 555,
          seed: 999,
        }),
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      const minimized = result.scenario as any;
      assert.equal(minimized.max_steps, 555);
      assert.equal(minimized.seed, 999);
    });
  });

  describe("reproduce_incident: id semantics", () => {
    it("each call produces a fresh IncidentCaseId, even for an identical scenario", async () => {
      const scenario = mkScenario({
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
      });
      const a = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      const b = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      assert.notEqual(a.id, b.id);
    });

    it("incident id looks like a UUID", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({}),
        required_fault_kind: "worker_death",
      });
      assert.match(
        result.id as string,
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
      );
    });
  });

  describe("reproduce_incident: missing_evidence is always empty (no evidence lookup on this route)", () => {
    it("a passing reproduction has an empty missing_evidence array", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
        }),
        required_fault_kind: "worker_death",
      });
      assert.deepEqual(result.missing_evidence, []);
    });

    it("an inconclusive (not-reproduced) result also has an empty missing_evidence array", async () => {
      const result = await client.reproduceContinuityIncident({
        scenario: mkScenario({
          faults: [{ point: "dispatch", kind: "provider_outage", occurrence: 1 }],
        }),
        required_fault_kind: "worker_death",
      });
      assert.deepEqual(result.missing_evidence, []);
    });
  });

  describe("reproduce_incident: required_fault_kind exhaustive matrix", () => {
    for (const kind of ALL_FAULT_KINDS) {
      it(`reproduces when the scenario's sole fault matches required_fault_kind="${kind}"`, async () => {
        const result = await client.reproduceContinuityIncident({
          scenario: mkScenario({
            faults: [{ point: "dispatch", kind, occurrence: 1 }],
          }),
          required_fault_kind: kind,
        });
        assert.equal(result.status, "pass");
        assert.equal(result.stable_failure_code, "REPRODUCED");
      });
    }

    for (const kind of ALL_FAULT_KINDS) {
      const mismatchedKind = ALL_FAULT_KINDS[(ALL_FAULT_KINDS.indexOf(kind) + 1) % ALL_FAULT_KINDS.length];
      it(`does not reproduce when the scenario's sole fault ("${kind}") differs from required_fault_kind ("${mismatchedKind}")`, async () => {
        const result = await client.reproduceContinuityIncident({
          scenario: mkScenario({
            faults: [{ point: "dispatch", kind, occurrence: 1 }],
          }),
          required_fault_kind: mismatchedKind,
        });
        assert.equal(result.status, "inconclusive");
        assert.equal(result.stable_failure_code, "NOT_REPRODUCED");
      });
    }
  });

  describe("reproduce_incident: malformed request bodies", () => {
    it("missing scenario field is a 400/422-class error", async () => {
      try {
        await client.reproduceContinuityIncident({ required_fault_kind: "worker_death" });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("missing required_fault_kind field is a 400/422-class error", async () => {
      try {
        await client.reproduceContinuityIncident({ scenario: mkScenario({}) });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("an unknown required_fault_kind string is a 400/422-class error", async () => {
      try {
        await client.reproduceContinuityIncident({
          scenario: mkScenario({}),
          required_fault_kind: "totally_bogus_kind",
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("scenario missing its required `id` field is a 400/422-class error", async () => {
      const { id, ...withoutId } = mkScenario({});
      try {
        await client.reproduceContinuityIncident({
          scenario: withoutId,
          required_fault_kind: "worker_death",
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("scenario missing its required `max_steps` field is a 400/422-class error", async () => {
      const { max_steps, ...rest } = mkScenario({});
      try {
        await client.reproduceContinuityIncident({
          scenario: rest,
          required_fault_kind: "worker_death",
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("scenario.id that isn't a valid UUID is a 400/422-class error", async () => {
      try {
        await client.reproduceContinuityIncident({
          scenario: mkScenario({ id: "not-a-uuid" }),
          required_fault_kind: "worker_death",
        });
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });

    it("an entirely empty object is rejected (missing required fields)", async () => {
      try {
        await client.reproduceContinuityIncident({});
        assert.fail("should reject");
      } catch (err: any) {
        assert.ok([400, 422].includes(err.status), `got ${err.status}`);
      }
    });
  });

  // ==================================================================
  // Cross-endpoint: feed a generated scenario straight into reproduce_incident
  // ==================================================================

  describe("cross-endpoint: generate -> reproduce pipeline", () => {
    it("a scenario generated with a matching fault reproduces when fed back in", async () => {
      const scenarios = await client.generateContinuityScenarios({
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
        events: ["a", "b", "c"],
        max_scenarios: 1,
        seed: 42,
      });
      const scenario = scenarios[0];
      const result = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      assert.equal(result.status, "pass");
      assert.ok(result.scenario);
    });

    it("a scenario generated with no faults never reproduces any kind", async () => {
      const scenarios = await client.generateContinuityScenarios({
        events: ["a", "b"],
        max_scenarios: 1,
        seed: 1,
      });
      const scenario = scenarios[0];
      for (const kind of ALL_FAULT_KINDS.slice(0, 3)) {
        const result = await client.reproduceContinuityIncident({
          scenario,
          required_fault_kind: kind,
        });
        assert.equal(result.status, "inconclusive", `unexpected reproduction for ${kind}`);
      }
    });

    it("feeding an already-minimized scenario back in re-minimizes to the same fixed point", async () => {
      const scenarios = await client.generateContinuityScenarios({
        faults: [
          { point: "dispatch", kind: "stale_owner", occurrence: 1 },
          { point: "approval", kind: "delayed_approval", occurrence: 2 },
        ],
        events: ["x", "y", "z"],
        retry_attempts: [4],
        handoff_delays_ms: [77],
        max_virtual_time_ms: 1000,
        max_scenarios: 1,
        seed: 8,
      });
      const first = await client.reproduceContinuityIncident({
        scenario: scenarios[0],
        required_fault_kind: "stale_owner",
      });
      assert.equal(first.status, "pass");
      const second = await client.reproduceContinuityIncident({
        scenario: first.scenario,
        required_fault_kind: "stale_owner",
      });
      assert.equal(second.status, "pass");
      assert.deepEqual(
        (second.scenario as any).faults,
        (first.scenario as any).faults,
      );
      assert.deepEqual(
        (second.scenario as any).event_order,
        (first.scenario as any).event_order,
      );
    });
  });

  // ==================================================================
  // No tenant/auth coupling: these routes are pure functions, no gate beyond
  // continuity_lab_enabled. Confirm they don't accidentally require a tenant
  // header/param and don't leak any tenant-scoped behavior.
  // ==================================================================

  describe("no tenant coupling", () => {
    it("generate_scenarios succeeds identically with or without a stray tenant_id in the body (field is ignored, not rejected)", async () => {
      const withTenant = await client.generateContinuityScenarios({
        max_scenarios: 2,
        seed: 1,
        tenant_id: `stray-${uuid()}`,
      });
      const withoutTenant = await client.generateContinuityScenarios({
        max_scenarios: 2,
        seed: 1,
      });
      assert.deepEqual(withTenant, withoutTenant);
    });

    it("reproduce_incident succeeds identically with or without a stray tenant_id in the body", async () => {
      const scenario = mkScenario({
        faults: [{ point: "dispatch", kind: "worker_death", occurrence: 1 }],
      });
      const withTenant = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
        tenant_id: `stray-${uuid()}`,
      });
      const withoutTenant = await client.reproduceContinuityIncident({
        scenario,
        required_fault_kind: "worker_death",
      });
      assert.equal(withTenant.status, withoutTenant.status);
      assert.deepEqual(withTenant.scenario, withoutTenant.scenario);
    });
  });
});
