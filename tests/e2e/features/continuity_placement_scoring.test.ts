/**
 * E2E: Portable Continuity — placement scoring factor breakdown.
 *
 * Existing continuity suites only assert `selected_runtime_id` on
 * POST /continuity/executions/{id}/placement. This suite asserts the
 * auditable `score_factors` breakdown each candidate carries, matching the
 * formula in orch8-engine/src/placement.rs (choose_runtime /
 * rank_candidate_metric, pinned by orch8-engine/src/placement_coverage_tests.rs):
 *
 *   score = trust + current_runtime + offline_capable
 *         + battery_rank + cost_rank + latency_rank
 *
 *   trust:           unverified 0, registered 10 (signed/attested are rejected
 *                    by validate_runtime_registration, so e2e can only use
 *                    these two levels)
 *   current_runtime: +5 for the execution's incumbent (owner_runtime_id)
 *   offline_capable: +3
 *   metric ranks:    per-factor ordering score (count - index) * 10 / count
 *                    with tie carry-over; battery prefers high, cost/latency
 *                    prefer low; a missing metric contributes 0.
 *                    With two known values: best 10, runner-up 5; ties both 10.
 *
 * Candidates are sorted score-desc then runtime_id-asc, and the selected
 * runtime is the first candidate with outcome "allow".
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";

import { Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

interface ScoreFactors {
  trust: number;
  current_runtime: number;
  offline_capable: number;
  battery_rank: number;
  cost_rank: number;
  latency_rank: number;
}

interface PlacementCandidate {
  runtime_id: string;
  outcome: string;
  score: number;
  score_factors: ScoreFactors;
  finding_codes: string[];
}

interface PlacementDecision {
  id: string;
  tenant_id: string;
  continuity_id: string;
  selected_runtime_id: string | null;
  classification: string;
  candidates: PlacementCandidate[];
}

function tid(prefix: string): string {
  return `${prefix}-${uuid().slice(0, 8)}`;
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
    regions: ["br-south"],
    hardware: [],
    offline_capable: false,
    connectivity: "wifi",
    battery_percent: 80,
    estimated_cost_microunits: 10,
    estimated_latency_ms: 20,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

async function registerRuntime(
  tenantId: string,
  runtimeId: string,
  overrides: Record<string, unknown> = {},
): Promise<void> {
  await client.registerRuntime({
    tenant_id: tenantId,
    capabilities: runtimeCaps(runtimeId, overrides),
  });
}

/** Minimal fresh execution; returns the (unregistered) owner runtime id. */
async function setupExecution(tenantId: string): Promise<{
  continuityId: string;
  ownerRuntimeId: string;
}> {
  const seq = testSequence("placement-scoring", [step("s1", "noop")], { tenantId });
  const createdSeq = await client.createSequence(seq);
  const instance = await client.createInstance({
    sequence_id: createdSeq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  const ownerRuntimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: ownerRuntimeId,
  });
  return { continuityId: execution.continuity_id, ownerRuntimeId };
}

async function choosePlacement(
  tenantId: string,
  continuityId: string,
): Promise<PlacementDecision> {
  return client.choosePlacement(continuityId, {
    tenant_id: tenantId,
    requirements: { handlers: ["noop"] },
    classification: "internal",
  }) as Promise<PlacementDecision>;
}

function candidateById(
  decision: PlacementDecision,
  runtimeId: string,
): PlacementCandidate {
  const candidate = decision.candidates.find((c) => c.runtime_id === runtimeId);
  assert.ok(candidate, `candidate ${runtimeId} present in decision`);
  return candidate;
}

function factorSum(factors: ScoreFactors): number {
  return (
    factors.trust +
    factors.current_runtime +
    factors.offline_capable +
    factors.battery_rank +
    factors.cost_rank +
    factors.latency_rank
  );
}

describe("continuity placement — scoring factor breakdown", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("selects the higher-trust, better-metric runtime with the exact factor breakdown", async () => {
    const tenantId = tid("ps-factors");
    const { continuityId } = await setupExecution(tenantId);
    const strong = uuid();
    const weak = uuid();
    await registerRuntime(tenantId, strong, {
      trust: "registered",
      battery_percent: 90,
      estimated_cost_microunits: 10,
      estimated_latency_ms: 50,
      offline_capable: true,
    });
    await registerRuntime(tenantId, weak, {
      trust: "unverified",
      battery_percent: 20,
      estimated_cost_microunits: 1000,
      estimated_latency_ms: 500,
      offline_capable: false,
    });

    const decision = await choosePlacement(tenantId, continuityId);

    assert.equal(decision.selected_runtime_id, strong);
    assert.equal(decision.candidates.length, 2);
    for (const candidate of decision.candidates) {
      assert.equal(candidate.outcome, "allow");
      // score is exactly the sum of its auditable factors.
      assert.equal(candidate.score, factorSum(candidate.score_factors));
    }
    // Candidates are sorted score-desc, so the winner leads.
    const leader = decision.candidates[0];
    assert.ok(leader, "at least one candidate");
    assert.equal(leader.runtime_id, strong);

    const winner = candidateById(decision, strong);
    assert.deepEqual(winner.score_factors, {
      trust: 10, // registered
      current_runtime: 0, // owner runtime was never registered
      offline_capable: 3,
      battery_rank: 10, // best of 2: (2 - 0) * 10 / 2
      cost_rank: 10,
      latency_rank: 10,
    });
    assert.equal(winner.score, 43);

    const loser = candidateById(decision, weak);
    assert.deepEqual(loser.score_factors, {
      trust: 0, // unverified
      current_runtime: 0,
      offline_capable: 0,
      battery_rank: 5, // runner-up of 2: (2 - 1) * 10 / 2
      cost_rank: 5,
      latency_rank: 5,
    });
    assert.equal(loser.score, 15);
  });

  it("stickiness bonus keeps the incumbent on repeated placement calls", async () => {
    const tenantId = tid("ps-sticky");
    const { continuityId, ownerRuntimeId } = await setupExecution(tenantId);
    const challenger = uuid();
    // Identical capabilities: every metric rank ties (tie carry-over gives
    // both candidates 10 per metric), so the incumbent's +5 current_runtime
    // bonus is the only difference.
    await registerRuntime(tenantId, ownerRuntimeId);
    await registerRuntime(tenantId, challenger);

    const first = await choosePlacement(tenantId, continuityId);
    assert.equal(first.selected_runtime_id, ownerRuntimeId);

    const incumbent = candidateById(first, ownerRuntimeId);
    assert.equal(incumbent.score_factors.current_runtime, 5);
    assert.equal(incumbent.score, 45); // 10 trust + 5 incumbent + 3 * 10 tied ranks
    const other = candidateById(first, challenger);
    assert.equal(other.score_factors.current_runtime, 0);
    assert.equal(other.score, 40);
    assert.equal(
      incumbent.score - other.score,
      incumbent.score_factors.current_runtime,
      "incumbent wins by exactly the stickiness bonus",
    );

    // Repeating the call keeps the incumbent — the bonus is stable, and the
    // owner_runtime_id has not moved (no handoff happened).
    const second = await choosePlacement(tenantId, continuityId);
    assert.equal(second.selected_runtime_id, ownerRuntimeId);
    assert.equal(
      candidateById(second, ownerRuntimeId).score_factors.current_runtime,
      5,
    );
  });

  it("offline-capable candidate outranks an otherwise-equal online-only candidate", async () => {
    const tenantId = tid("ps-offline");
    const { continuityId } = await setupExecution(tenantId);
    const offlineRuntime = uuid();
    const onlineRuntime = uuid();
    await registerRuntime(tenantId, offlineRuntime, { offline_capable: true });
    await registerRuntime(tenantId, onlineRuntime, { offline_capable: false });

    const decision = await choosePlacement(tenantId, continuityId);

    assert.equal(decision.selected_runtime_id, offlineRuntime);
    const offline = candidateById(decision, offlineRuntime);
    const online = candidateById(decision, onlineRuntime);
    assert.equal(offline.score_factors.offline_capable, 3);
    assert.equal(online.score_factors.offline_capable, 0);
    // All other factors tie (same trust, same metrics, no incumbent), so the
    // offline bonus is the whole margin.
    assert.equal(offline.score - online.score, 3);
    assert.equal(offline.score, 43);
    assert.equal(online.score, 40);
  });
});
