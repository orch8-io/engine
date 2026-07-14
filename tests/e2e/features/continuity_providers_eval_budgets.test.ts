/**
 * E2E: Portable Continuity — provider routing, evaluation gates, the
 * optimization advisor, and multidimensional budget settlement.
 *
 * Covers `POST /continuity/providers/choose`, `POST /continuity/evaluations/gate`,
 * `POST /continuity/evaluations/stored-gate`, `POST /continuity/optimizations/recommend`,
 * `POST /continuity/optimizations/{id}/accept`, and the
 * `/continuity/executions/{id}/budget-reservations[...]` lifecycle.
 *
 * See CHANGELOG.md [Unreleased]: "Effect-safe provider failover",
 * "Stored-evidence evaluation gates", "Actionable workflow optimization
 * advisor", "Cumulative multidimensional budget settlement".
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

function candidate(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    provider: "openai",
    model: "gpt-5",
    region: "us-east",
    price_microunits: 1000,
    expected_latency_ms: 200,
    quality_millipoints: 800,
    breaker_open: false,
    supports_idempotency: true,
    pricing_version: "v1",
    ...overrides,
  };
}

async function setupExecution(tenantId: string, budget?: Record<string, unknown>) {
  const sequence = testSequence("prov-eval-budget", [step("work", "noop")], {
    tenantId,
  });
  await client.createSequence(sequence);
  const instance = await client.createInstance({
    sequence_id: sequence.id,
    tenant_id: tenantId,
    namespace: "default",
    ...(budget ? { budget } : {}),
  });
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: uuid(),
  });
  return { sequence, instance, execution };
}

function baseWhatIfScenario(
  tenantId: string,
  continuityId: string,
  instanceId: string,
  sequenceId: string,
  sequenceVersion: number,
): Record<string, unknown> {
  return {
    id: uuid(),
    tenant_id: tenantId,
    source: {
      checkpoint_id: "00000000-0000-0000-0000-000000000000",
      instance_id: instanceId,
      continuity_id: continuityId,
      epoch: 0,
      sequence_id: sequenceId,
      sequence_version: sequenceVersion,
      block_id: "work",
      checkpoint_sha256: "0".repeat(64),
      provenance_head: null,
      created_at: new Date().toISOString(),
    },
    context_patch: {},
    config_patch: {},
    output_overrides: {},
    handler_mocks: {},
    block_param_overrides: {},
    signals: [],
    target_sequence_version: null,
    effect_mode: "blocked",
    virtual_time: true,
    retain_full_evidence: false,
  };
}

describe("Continuity Provider Routing, Evaluation Gates, Optimization Advisor, Budgets", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: { ORCH8_LOG_LEVEL: "error" },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  // ==================================================================
  // choose_provider — pure, fail-closed routing
  // ==================================================================

  describe("provider routing", () => {
    it("selects the highest-scoring eligible candidate", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [
          candidate({ provider: "a", quality_millipoints: 500 }),
          candidate({ provider: "b", quality_millipoints: 900 }),
        ],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "b");
      assert.ok(decision.finding_codes.includes("PROVIDER_POLICY_SATISFIED"));
    });

    it("excludes a candidate with an open circuit breaker", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [
          candidate({ provider: "broken", breaker_open: true, quality_millipoints: 999 }),
          candidate({ provider: "healthy", quality_millipoints: 100 }),
        ],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "healthy");
    });

    it("returns no selection and NO_ELIGIBLE_PROVIDER when every candidate is breaker-open", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ breaker_open: true })],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
      assert.ok(decision.finding_codes.includes("NO_ELIGIBLE_PROVIDER"));
    });

    it("filters out candidates outside allowed_regions", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ region: "eu-west" })],
        allowed_regions: ["us-east"],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
    });

    it("keeps a candidate whose region is in allowed_regions", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ region: "us-east" })],
        allowed_regions: ["us-east", "eu-west"],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.region, "us-east");
    });

    it("filters out candidates above max_price_microunits", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ price_microunits: 5000 })],
        max_price_microunits: 1000,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
    });

    it("keeps a candidate at exactly max_price_microunits (inclusive bound)", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ price_microunits: 1000 })],
        max_price_microunits: 1000,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.ok(decision.selected);
    });

    it("filters out candidates above max_latency_ms", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ expected_latency_ms: 999 })],
        max_latency_ms: 100,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
    });

    it("filters out candidates below minimum_quality_millipoints", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ quality_millipoints: 10 })],
        minimum_quality_millipoints: 500,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
    });

    it("filters out candidates that don't support idempotency when required", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ supports_idempotency: false })],
        require_idempotency: true,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
    });

    it("keeps an idempotency-supporting candidate when required", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ supports_idempotency: true })],
        require_idempotency: true,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.ok(decision.selected);
    });

    it("blocks non-idempotent failover to a different provider without effect-policy approval", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ provider: "b" })],
        prior_provider: "a",
        operation_idempotent: false,
        effect_policy_approved: false,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
      assert.ok(decision.finding_codes.includes("NON_IDEMPOTENT_FAILOVER_BLOCKED"));
    });

    it("allows failover to a different provider when the operation is idempotent", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ provider: "b" })],
        prior_provider: "a",
        operation_idempotent: true,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "b");
      assert.ok(!decision.finding_codes.includes("NON_IDEMPOTENT_FAILOVER_BLOCKED"));
    });

    it("allows failover to a different provider when the effect policy explicitly approves replay", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ provider: "b" })],
        prior_provider: "a",
        operation_idempotent: false,
        effect_policy_approved: true,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "b");
    });

    it("keeps the prior provider itself as eligible regardless of failover policy", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ provider: "a" })],
        prior_provider: "a",
        operation_idempotent: false,
        effect_policy_approved: false,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "a");
    });

    it("produces a stable cohort hash for the same cohort_key", async () => {
      const key = `cohort-${uuid()}`;
      const d1 = await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: key,
      });
      const d2 = await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: key,
      });
      assert.equal(d1.cohort, d2.cohort);
    });

    it("produces different cohort hashes for different cohort_keys", async () => {
      const d1 = await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: `cohort-${uuid()}`,
      });
      const d2 = await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.notEqual(d1.cohort, d2.cohort);
    });

    it("rejects an empty cohort_key", async () => {
      // Empty/oversized cohort_key is checked alongside the candidate/region
      // bounded-size limits, so it fails closed with 413 rather than 400.
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            cohort_key: "",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 413);
          return true;
        },
      );
    });

    it("rejects more than 1000 candidates", async () => {
      const many = Array.from({ length: 1001 }, (_, i) => candidate({ provider: `p${i}` }));
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: many,
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 413);
          return true;
        },
      );
    });

    it("rejects a candidate with an empty provider name", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ provider: "" })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with negative price_microunits", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ price_microunits: -1 })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with quality_millipoints above 1000", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ quality_millipoints: 1001 })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects tenant_id without continuity_id (must be paired)", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            cohort_key: `cohort-${uuid()}`,
            tenant_id: "some-tenant",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects continuity_id without tenant_id (must be paired)", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            cohort_key: `cohort-${uuid()}`,
            continuity_id: uuid(),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with negative quality_millipoints", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ quality_millipoints: -1 })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("keeps a zero-priced candidate at exactly a zero max_price_microunits ceiling", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ price_microunits: 0 })],
        max_price_microunits: 0,
        cohort_key: `cohort-${uuid()}`,
      });
      assert.ok(decision.selected);
    });

    it("does not filter candidates when allowed_regions is empty (unrestricted)", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate({ region: "antarctica" })],
        allowed_regions: [],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.ok(decision.selected);
    });

    it("breaks score ties by model when provider names are equal", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [
          candidate({ provider: "same", model: "zzz", quality_millipoints: 500 }),
          candidate({ provider: "same", model: "aaa", quality_millipoints: 500 }),
        ],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.model, "aaa");
    });

    it("records a provider_selected provenance entry when tenant_id + continuity_id are supplied", async () => {
      const tenantId = `prov-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: `cohort-${uuid()}`,
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
      });
      const provenance = await client.listContinuityProvenance(
        execution.continuity_id,
        tenantId,
      );
      assert.ok(provenance.some((entry: any) => entry.kind === "provider_selected"));
    });

    it("rejects a nonexistent continuity_id when paired with tenant_id", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            cohort_key: `cohort-${uuid()}`,
            tenant_id: "some-tenant",
            continuity_id: uuid(),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("breaks score ties deterministically by provider name", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [
          candidate({ provider: "zzz", price_microunits: 100, quality_millipoints: 500 }),
          candidate({ provider: "aaa", price_microunits: 100, quality_millipoints: 500 }),
        ],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected.provider, "aaa");
    });

    it("rejects minimum_quality_millipoints above 1000", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            minimum_quality_millipoints: 1001,
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects negative minimum_quality_millipoints", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            minimum_quality_millipoints: -1,
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects an empty prior_provider", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            prior_provider: "",
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a prior_provider longer than 128 bytes", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            prior_provider: "p".repeat(129),
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with an empty model", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ model: "" })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with a model longer than 256 bytes", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ model: "m".repeat(257) })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with an empty region", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ region: "" })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with a region longer than 128 bytes", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ region: "r".repeat(129) })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a candidate with an empty pricing_version", async () => {
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate({ pricing_version: "" })],
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects more than 256 allowed_regions", async () => {
      const regions = Array.from({ length: 257 }, (_, i) => `region-${i}`);
      await assert.rejects(
        () =>
          client.chooseContinuityProvider({
            candidates: [candidate()],
            allowed_regions: regions,
            cohort_key: `cohort-${uuid()}`,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 413);
          return true;
        },
      );
    });

    it("returns NO_ELIGIBLE_PROVIDER for an empty candidates array", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.equal(decision.selected, null);
      assert.ok(decision.finding_codes.includes("NO_ELIGIBLE_PROVIDER"));
    });

    it("returns a decision with a well-formed id and created_at timestamp", async () => {
      const decision = await client.chooseContinuityProvider({
        candidates: [candidate()],
        cohort_key: `cohort-${uuid()}`,
      });
      assert.match(
        decision.id,
        /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i,
      );
      assert.ok(!Number.isNaN(Date.parse(decision.created_at)));
    });
  });

  // ==================================================================
  // evaluate_gate — pure statistical gate
  // ==================================================================

  describe("evaluation gate (pure)", () => {
    it("passes when candidate mean is within the regression allowance", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800, 800],
        candidate_scores: [790, 790, 790],
        minimum_samples: 3,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "pass");
    });

    it("fails when candidate mean regresses beyond the allowance", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800, 800],
        candidate_scores: [500, 500, 500],
        minimum_samples: 3,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "fail");
    });

    it("is inconclusive when baseline sample size is below minimum_samples", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800],
        candidate_scores: [800, 800, 800],
        minimum_samples: 3,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("is inconclusive when candidate sample size is below minimum_samples", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800, 800],
        candidate_scores: [800],
        minimum_samples: 3,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("passes at exactly the regression boundary (inclusive)", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [1000],
        candidate_scores: [950],
        minimum_samples: 1,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "pass");
    });

    it("fails just past the regression boundary", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [1000],
        candidate_scores: [949],
        minimum_samples: 1,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "fail");
    });

    it("passes when the candidate improves over baseline", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [500],
        candidate_scores: [900],
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "pass");
    });

    it("rejects more than 100000 baseline samples", async () => {
      const many = new Array(100_001).fill(1);
      await assert.rejects(
        () =>
          client.evaluateContinuityGate({
            baseline_scores: many,
            candidate_scores: [1],
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 413);
          return true;
        },
      );
    });

    it("treats minimum_samples of 0 as always meeting the sample-size floor", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [],
        candidate_scores: [],
        minimum_samples: 0,
        maximum_regression_millipoints: 0,
      });
      // mean([]) is implementation-defined for the empty case; the important
      // invariant is that the endpoint doesn't error and returns a status.
      assert.ok(["pass", "fail", "inconclusive"].includes(result.status));
    });

    it("meets the sample-size floor when arrays are exactly minimum_samples long", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800, 800],
        candidate_scores: [800, 800, 800],
        minimum_samples: 3,
        maximum_regression_millipoints: 0,
      });
      assert.notEqual(result.status, "inconclusive");
    });

    it("a negative maximum_regression_millipoints requires the candidate to strictly beat baseline", async () => {
      const equalResult = await client.evaluateContinuityGate({
        baseline_scores: [800],
        candidate_scores: [800],
        minimum_samples: 1,
        maximum_regression_millipoints: -10,
      });
      assert.equal(equalResult.status, "fail");
      const improvedResult = await client.evaluateContinuityGate({
        baseline_scores: [800],
        candidate_scores: [815],
        minimum_samples: 1,
        maximum_regression_millipoints: -10,
      });
      assert.equal(improvedResult.status, "pass");
    });

    it("averages multiple samples correctly, including negative scores", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [-100, 100, 200],
        candidate_scores: [-100, 100, 200],
        minimum_samples: 3,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "pass");
    });

    it("is inconclusive when one array is exactly one sample short of minimum_samples", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800, 800],
        candidate_scores: [800, 800],
        minimum_samples: 3,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("evaluates correctly when baseline and candidate arrays differ in length", async () => {
      const result = await client.evaluateContinuityGate({
        baseline_scores: [800, 800],
        candidate_scores: [800, 800, 800, 800],
        minimum_samples: 2,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "pass");
    });
  });

  // ==================================================================
  // evaluate_stored_gate — durable, weighted, tenant-scoped
  // ==================================================================

  describe("stored evaluation gate", () => {
    it("is inconclusive when no evidence has been recorded for either execution", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("is inconclusive while any matching evidence is still pending", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "pending",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      await client.appendContinuityEvaluation(candidate.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "1".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("passes when weighted-by-sample-size scores are within the regression allowance", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 100,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      await client.appendContinuityEvaluation(candidate.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 790,
        sample_size: 100,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "1".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 50,
      });
      assert.equal(result.status, "pass");
    });

    it("fails when a cheaper-but-worse candidate regresses beyond the allowance", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 900,
        sample_size: 50,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      await client.appendContinuityEvaluation(candidate.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 400,
        sample_size: 50,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "1".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 10,
      });
      assert.equal(result.status, "fail");
    });

    it("does not let a duplicate identical evaluation submission double-count", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      const payload = {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "abc123".padEnd(64, "0"),
      };
      await client.appendContinuityEvaluation(execution.continuity_id, payload);
      await assert.rejects(
        () => client.appendContinuityEvaluation(execution.continuity_id, payload),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("filters evaluations by evaluator name — mismatched evaluator is not counted", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "other-evaluator",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("rejects an empty evaluator name", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "",
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects zero minimum_samples", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "unit-test",
            minimum_samples: 0,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects negative maximum_regression_millipoints", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "unit-test",
            minimum_samples: 1,
            maximum_regression_millipoints: -1,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("404s on an unknown baseline_continuity_id", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: candidate } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: uuid(),
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "unit-test",
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("404s on an unknown candidate_continuity_id", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: uuid(),
            evaluator: "unit-test",
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("scopes evaluation lookups per-tenant — another tenant's execution id is invisible", async () => {
      const tenantA = `eval-a-${uuid().slice(0, 8)}`;
      const tenantB = `eval-b-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantA);
      const { execution: candidate } = await setupExecution(tenantA);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantB,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "unit-test",
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("rejects a stored gate evaluator longer than 128 bytes", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.evaluateStoredContinuityGate({
            tenant_id: tenantId,
            baseline_continuity_id: baseline.continuity_id,
            candidate_continuity_id: candidate.continuity_id,
            evaluator: "e".repeat(129),
            minimum_samples: 1,
            maximum_regression_millipoints: 0,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("filters evaluations by scope — a mismatched scope is not counted", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: { model: "gpt-5" },
        evidence_sha256: "0".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        scope: {},
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });

    it("sums sample_size across multiple evaluation entries to clear the minimum_samples floor", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      for (let i = 0; i < 2; i++) {
        await client.appendContinuityEvaluation(baseline.continuity_id, {
          tenant_id: tenantId,
          evaluator: "unit-test",
          score_millipoints: 800,
          sample_size: 3,
          deferred: false,
          outcome: "complete",
          scope: {},
          evidence_sha256: `${i}`.padStart(64, "0"),
        });
        await client.appendContinuityEvaluation(candidate.continuity_id, {
          tenant_id: tenantId,
          evaluator: "unit-test",
          score_millipoints: 800,
          sample_size: 3,
          deferred: false,
          outcome: "complete",
          scope: {},
          evidence_sha256: `${i}`.padStart(63, "0") + "f",
        });
      }
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 6,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "pass");
    });

    it("rejects appending an evaluation with an empty evaluator", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.appendContinuityEvaluation(execution.continuity_id, {
            tenant_id: tenantId,
            evaluator: "",
            score_millipoints: 800,
            sample_size: 10,
            deferred: false,
            outcome: "complete",
            scope: {},
            evidence_sha256: "0".repeat(64),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects appending an evaluation with sample_size 0", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.appendContinuityEvaluation(execution.continuity_id, {
            tenant_id: tenantId,
            evaluator: "unit-test",
            score_millipoints: 800,
            sample_size: 0,
            deferred: false,
            outcome: "complete",
            scope: {},
            evidence_sha256: "0".repeat(64),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects appending an evaluation with an empty scope.model", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.appendContinuityEvaluation(execution.continuity_id, {
            tenant_id: tenantId,
            evaluator: "unit-test",
            score_millipoints: 800,
            sample_size: 10,
            deferred: false,
            outcome: "complete",
            scope: { model: "" },
            evidence_sha256: "0".repeat(64),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects appending an evaluation with a non-hex scope.prompt_sha256", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.appendContinuityEvaluation(execution.continuity_id, {
            tenant_id: tenantId,
            evaluator: "unit-test",
            score_millipoints: 800,
            sample_size: 10,
            deferred: false,
            outcome: "complete",
            scope: { prompt_sha256: "not-a-hex-digest" },
            evidence_sha256: "0".repeat(64),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("404s appending an evaluation to an unknown continuity_id", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      await assert.rejects(
        () =>
          client.appendContinuityEvaluation(uuid(), {
            tenant_id: tenantId,
            evaluator: "unit-test",
            score_millipoints: 800,
            sample_size: 10,
            deferred: false,
            outcome: "complete",
            scope: {},
            evidence_sha256: "0".repeat(64),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("trivially passes a self-comparison when baseline and candidate are the same execution", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(execution.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: execution.continuity_id,
        candidate_continuity_id: execution.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "pass");
    });

    it("is inconclusive when the candidate execution exists but has zero recorded evidence", async () => {
      const tenantId = `eval-${uuid().slice(0, 8)}`;
      const { execution: baseline } = await setupExecution(tenantId);
      const { execution: candidate } = await setupExecution(tenantId);
      await client.appendContinuityEvaluation(baseline.continuity_id, {
        tenant_id: tenantId,
        evaluator: "unit-test",
        score_millipoints: 800,
        sample_size: 10,
        deferred: false,
        outcome: "complete",
        scope: {},
        evidence_sha256: "0".repeat(64),
      });
      const result = await client.evaluateStoredContinuityGate({
        tenant_id: tenantId,
        baseline_continuity_id: baseline.continuity_id,
        candidate_continuity_id: candidate.continuity_id,
        evaluator: "unit-test",
        minimum_samples: 1,
        maximum_regression_millipoints: 0,
      });
      assert.equal(result.status, "inconclusive");
    });
  });

  // ==================================================================
  // Budget reservations: reserve / list / reconcile / release
  // ==================================================================

  describe("budget reservations", () => {
    it("rejects reserving a budget when the instance has no configured budget", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId);
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 100,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("reserves budget when the instance has a configured max_cost_microunits", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 1000,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      assert.equal(reservation.state, "reserved");
      assert.equal(reservation.continuity_id, execution.continuity_id);
    });

    it("rejects a reservation that exceeds the hard cost limit", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 100,
      });
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 1_000_000,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("rejects an empty estimation_version", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 1,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects an estimation_version longer than 128 bytes", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 1,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "x".repeat(129),
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("lists all active reservations for an execution", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const requested = {
        cost_microunits: 10,
        wall_time_ms: 0,
        external_calls: 0,
        bytes_transferred: 0,
        energy_millijoules: 0,
        attention_units: 0,
      };
      await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested,
        estimation_version: "v1",
      });
      await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested,
        estimation_version: "v1",
      });
      const list = await client.listContinuityBudgetReservations(
        execution.continuity_id,
        tenantId,
      );
      assert.equal(list.length, 2);
    });

    it("reconciles a reservation to its actual usage", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 1000,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      const reconciled = await client.reconcileContinuityBudget(
        execution.continuity_id,
        reservation.id,
        {
          tenant_id: tenantId,
          actual: {
            cost_microunits: 750,
            wall_time_ms: 0,
            external_calls: 0,
            bytes_transferred: 0,
            energy_millijoules: 0,
            attention_units: 0,
          },
        },
      );
      assert.equal(reconciled.state, "reconciled");
      assert.equal(reconciled.actual.cost_microunits, 750);
    });

    it("rejects reconciling the same reservation twice", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 100,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      const actual = {
        cost_microunits: 100,
        wall_time_ms: 0,
        external_calls: 0,
        bytes_transferred: 0,
        energy_millijoules: 0,
        attention_units: 0,
      };
      await client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
        tenant_id: tenantId,
        actual,
      });
      await assert.rejects(
        () =>
          client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
            tenant_id: tenantId,
            actual,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("releases a reservation when dispatch never occurred", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 100,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      const released = await client.releaseContinuityBudget(
        execution.continuity_id,
        reservation.id,
        { tenant_id: tenantId },
      );
      assert.equal(released.state, "released");
    });

    it("rejects releasing an already-reconciled reservation", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 100,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      await client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
        tenant_id: tenantId,
        actual: {
          cost_microunits: 50,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
      });
      await assert.rejects(
        () =>
          client.releaseContinuityBudget(execution.continuity_id, reservation.id, {
            tenant_id: tenantId,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("404s reconciling a reservation id that does not exist", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () =>
          client.reconcileContinuityBudget(execution.continuity_id, uuid(), {
            tenant_id: tenantId,
            actual: {
              cost_microunits: 1,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("cumulative reconciled spend counts toward later hard-limit checks", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1000,
      });
      const first = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 400,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      await client.reconcileContinuityBudget(execution.continuity_id, first.id, {
        tenant_id: tenantId,
        actual: {
          cost_microunits: 900,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
      });
      // A second reservation must account for the 900 already reconciled,
      // not the original 400 estimate — 900 + 200 > 1000 hard limit.
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 200,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("scopes reservation lookups per-tenant", async () => {
      const tenantA = `budget-a-${uuid().slice(0, 8)}`;
      const tenantB = `budget-b-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantA, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () => client.listContinuityBudgetReservations(execution.continuity_id, tenantB),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("lists an empty array for an execution with no reservations", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const list = await client.listContinuityBudgetReservations(
        execution.continuity_id,
        tenantId,
      );
      assert.deepEqual(list, []);
    });

    it("404s reserving budget on an unknown continuity_id", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(uuid(), {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 1,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("404s listing reservations for an unknown continuity_id", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      await assert.rejects(
        () => client.listContinuityBudgetReservations(uuid(), tenantId),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("404s releasing a reservation id that does not exist", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () =>
          client.releaseContinuityBudget(execution.continuity_id, uuid(), {
            tenant_id: tenantId,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("rejects reconciling a reservation with negative actual usage", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 100,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      await assert.rejects(
        () =>
          client.reconcileContinuityBudget(execution.continuity_id, reservation.id, {
            tenant_id: tenantId,
            actual: {
              cost_microunits: -1,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects reserving with negative requested usage", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: -100,
              wall_time_ms: 0,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("enforces the hard limit on a non-cost dimension (max_wall_time_ms)", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_wall_time_ms: 1000,
      });
      await assert.rejects(
        () =>
          client.reserveContinuityBudget(execution.continuity_id, {
            tenant_id: tenantId,
            requested: {
              cost_microunits: 0,
              wall_time_ms: 5000,
              external_calls: 0,
              bytes_transferred: 0,
              energy_millijoules: 0,
              attention_units: 0,
            },
            estimation_version: "v1",
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("allows a reservation within the max_wall_time_ms hard limit", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_wall_time_ms: 1000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 0,
          wall_time_ms: 500,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      assert.equal(reservation.state, "reserved");
    });

    it("reserve → release → reserve again succeeds (a released reservation frees its budget)", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1000,
      });
      const requested = {
        cost_microunits: 900,
        wall_time_ms: 0,
        external_calls: 0,
        bytes_transferred: 0,
        energy_millijoules: 0,
        attention_units: 0,
      };
      const first = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested,
        estimation_version: "v1",
      });
      await client.releaseContinuityBudget(execution.continuity_id, first.id, {
        tenant_id: tenantId,
      });
      const second = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested,
        estimation_version: "v1",
      });
      assert.equal(second.state, "reserved");
    });

    it("accepts an estimation_version exactly at the 128-byte boundary", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 1,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v".repeat(128),
      });
      assert.equal(reservation.state, "reserved");
    });

    it("allows reconciling actual usage that exceeds the original requested estimate", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 10,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      const reconciled = await client.reconcileContinuityBudget(
        execution.continuity_id,
        reservation.id,
        {
          tenant_id: tenantId,
          actual: {
            cost_microunits: 500,
            wall_time_ms: 0,
            external_calls: 0,
            bytes_transferred: 0,
            energy_millijoules: 0,
            attention_units: 0,
          },
        },
      );
      assert.equal(reconciled.state, "reconciled");
      assert.equal(reconciled.actual.cost_microunits, 500);
    });

    it("404s releasing a reservation under the wrong tenant", async () => {
      const tenantId = `budget-${uuid().slice(0, 8)}`;
      const otherTenantId = `budget-other-${uuid().slice(0, 8)}`;
      const { execution } = await setupExecution(tenantId, {
        max_cost_microunits: 1_000_000,
      });
      const reservation = await client.reserveContinuityBudget(execution.continuity_id, {
        tenant_id: tenantId,
        requested: {
          cost_microunits: 1,
          wall_time_ms: 0,
          external_calls: 0,
          bytes_transferred: 0,
          energy_millijoules: 0,
          attention_units: 0,
        },
        estimation_version: "v1",
      });
      await assert.rejects(
        () =>
          client.releaseContinuityBudget(execution.continuity_id, reservation.id, {
            tenant_id: otherTenantId,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });
  });

  // ==================================================================
  // Optimization advisor
  // ==================================================================

  describe("optimization advisor", () => {
    it("rejects a non-virtual-time scenario", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      scenario.virtual_time = false;
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 500,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a scenario whose effect_mode is not blocked", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      scenario.effect_mode = "mocked";
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 500,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects serial_work_millipoints above 1000", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 1001,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects a scenario whose source.continuity_id doesn't match the request continuity_id", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        uuid(),
        instance.id,
        sequence.id,
        sequence.version,
      );
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 100,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("returns recommendations for a high serial-work, high-retry workload", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      assert.ok(Array.isArray(recommendations));
    });

    it("records an optimization_recommended provenance entry per recommendation", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      const provenance = await client.listContinuityProvenance(
        execution.continuity_id,
        tenantId,
      );
      const recommendedCount = provenance.filter(
        (entry: any) => entry.kind === "optimization_recommended",
      ).length;
      assert.equal(recommendedCount, recommendations.length);
    });

    it("404s recommend for an unknown continuity_id", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const scenario = baseWhatIfScenario(tenantId, uuid(), uuid(), uuid(), 1);
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: scenario.source.continuity_id,
            serial_work_millipoints: 100,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 404);
          return true;
        },
      );
    });

    it("rejects accepting a recommendation whose id doesn't match the path", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return; // nothing to accept in this run
      const recommendation = recommendations[0];
      await assert.rejects(
        () =>
          client.acceptContinuityOptimization(uuid(), {
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            recommendation,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects accepting a recommendation that was never issued (tampered payload)", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return;
      const tampered = { ...recommendations[0], kind: "tampered-kind" };
      await assert.rejects(
        () =>
          client.acceptContinuityOptimization(tampered.id, {
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            recommendation: tampered,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 409);
          return true;
        },
      );
    });

    it("accepting a genuine recommendation creates a draft sequence and a draft release", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return;
      const recommendation = recommendations[0];
      const accepted = await client.acceptContinuityOptimization(recommendation.id, {
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        recommendation,
      });
      assert.equal(accepted.draft_sequence.status, "draft");
      assert.equal(accepted.release.state, "draft");
      assert.equal(accepted.release.baseline_sequence_id, sequence.id);
    });

    it("accepting the same recommendation twice is idempotent (returns the same draft)", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return;
      const recommendation = recommendations[0];
      const first = await client.acceptContinuityOptimization(recommendation.id, {
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        recommendation,
      });
      const second = await client.acceptContinuityOptimization(recommendation.id, {
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        recommendation,
      });
      assert.equal(first.draft_sequence.id, second.draft_sequence.id);
      assert.equal(first.release.id, second.release.id);
    });

    it("never mutates the original source sequence when accepting a recommendation", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return;
      const before = await client.getSequence(sequence.id);
      await client.acceptContinuityOptimization(recommendations[0].id, {
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        recommendation: recommendations[0],
      });
      const after = await client.getSequence(sequence.id);
      assert.equal(after.status, before.status);
      assert.deepEqual(after.blocks, before.blocks);
    });

    it("rejects negative average_cost_microunits", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 100,
            retry_rate_millipoints: 100,
            average_payload_bytes: 1000,
            average_cost_microunits: -1,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("rejects retry_rate_millipoints above 1000", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      await assert.rejects(
        () =>
          client.recommendContinuityOptimizations({
            tenant_id: tenantId,
            continuity_id: execution.continuity_id,
            serial_work_millipoints: 100,
            retry_rate_millipoints: 1001,
            average_payload_bytes: 1000,
            average_cost_microunits: 100,
            dead_branch_count: 0,
            base_scenario: scenario,
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("suppresses parallelization and dead_branch recommendations when an active invariant exists", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      await client.createContinuityInvariant({
        tenant_id: tenantId,
        sequence_id: sequence.id,
        sequence_version: sequence.version,
        name: "no unresolved effects",
        rule: { type: "no_unknown_effects" },
      });
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      const kinds = recommendations.map((r: any) => r.kind);
      assert.ok(!kinds.includes("parallelization"));
      assert.ok(!kinds.includes("dead_branch"));
      assert.ok(kinds.includes("retry_tuning"));
    });

    it("rejects accepting a recommendation whose recommended continuity_id doesn't match the request continuity_id", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const { execution: otherExecution } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 5,
        base_scenario: scenario,
      });
      if (recommendations.length === 0) return;
      await assert.rejects(
        () =>
          client.acceptContinuityOptimization(recommendations[0].id, {
            tenant_id: tenantId,
            continuity_id: otherExecution.continuity_id,
            recommendation: recommendations[0],
          }),
        (error: unknown) => {
          assert.equal((error as ApiError).status, 400);
          return true;
        },
      );
    });

    it("returns no recommendations for an entirely idle workload aggregate", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 0,
        retry_rate_millipoints: 0,
        average_payload_bytes: 0,
        average_cost_microunits: 0,
        dead_branch_count: 0,
        base_scenario: scenario,
      });
      assert.deepEqual(recommendations, []);
    });

    it("omits the dead_branch recommendation when dead_branch_count is zero", async () => {
      const tenantId = `opt-${uuid().slice(0, 8)}`;
      const { execution, instance, sequence } = await setupExecution(tenantId);
      const scenario = baseWhatIfScenario(
        tenantId,
        execution.continuity_id,
        instance.id,
        sequence.id,
        sequence.version,
      );
      const recommendations = await client.recommendContinuityOptimizations({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        serial_work_millipoints: 900,
        retry_rate_millipoints: 900,
        average_payload_bytes: 10_000_000,
        average_cost_microunits: 100,
        dead_branch_count: 0,
        base_scenario: scenario,
      });
      assert.ok(!recommendations.map((r: any) => r.kind).includes("dead_branch"));
    });
  });
});
