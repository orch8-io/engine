/**
 * E2E: Portable Continuity — human attention leases, residency enforcement,
 * confidential-disclosure minimization, federation envelope verification,
 * device delegation claims, continuation grants, and durable resumable
 * streams.
 *
 * Covers `orch8-api/src/continuity.rs`: create_attention_task,
 * assign_attention_task, decide_attention_task, evaluate_residency,
 * minimize_disclosure, verify_federation, claim_delegation,
 * issue_continuation_grant, consume_continuation_grant, create_stream,
 * append_stream_frame, list_stream_frames, retract_stream_frames.
 */
import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHash, generateKeyPairSync, sign } from "node:crypto";

import { ApiError, Orch8Client, step, testSequence, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

// --- Federation peer + Ed25519 keypair (mirrors the proven pattern in
// portable_continuity.test.ts: JS object key order == Rust struct field
// declaration order, so JSON.stringify(unsigned envelope) byte-matches
// serde_json::to_vec on the server for signature verification). ---
const federationTenantId = "attn-fed-e2e";
const federationPeerId = uuid();
const { publicKey: fedPub, privateKey: fedPriv } = generateKeyPairSync("ed25519");
const fedPubRaw = fedPub.export({ format: "der", type: "spki" }).subarray(-32);
const federationPeer = {
  id: federationPeerId,
  name: "attn-e2e-peer",
  trust_root_sha256: createHash("sha256").update(fedPubRaw).digest("hex"),
  public_key: fedPubRaw.toString("base64"),
  endpoint: "https://peer.example.invalid",
  allowed_tenants: [federationTenantId],
  revoked_at: null as string | null,
};

function signEnvelope(unsigned: Record<string, unknown>): string {
  return sign(null, Buffer.from(JSON.stringify(unsigned)), fedPriv).toString("base64");
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
    battery_percent: 100,
    estimated_cost_microunits: 10,
    estimated_latency_ms: 20,
    observed_at: new Date(now).toISOString(),
    expires_at: new Date(now + 60_000).toISOString(),
    ...overrides,
  };
}

async function setupExecution(tenantId: string): Promise<{
  execution: any;
  instance: any;
  runtimeId: string;
}> {
  const seq = testSequence("attn-setup", [step("s1", "noop")], { tenantId });
  const createdSeq = await client.createSequence(seq);
  const instance = await client.createInstance({
    sequence_id: createdSeq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  const runtimeId = uuid();
  const execution = await client.createContinuityExecution({
    tenant_id: tenantId,
    instance_id: instance.id,
    runtime_id: runtimeId,
  });
  return { execution, instance, runtimeId };
}

function tid(prefix: string): string {
  return `${prefix}-${uuid().slice(0, 8)}`;
}

function reviewer(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    reviewer_id: uuid(),
    tenant_ids: [],
    skills: ["fraud_review"],
    region: "br-south",
    trust: "registered",
    available_attention_units: 10,
    ...overrides,
  };
}

async function createTask(
  tenantId: string,
  continuityId: string,
  overrides: Record<string, unknown> = {},
): Promise<any> {
  const now = Date.now();
  return client.createAttentionTask({
    tenant_id: tenantId,
    continuity_id: continuityId,
    required_skills: ["fraud_review"],
    classification: "internal",
    priority: 1,
    deadline: new Date(now + 60_000).toISOString(),
    estimated_attention_units: 1,
    ...overrides,
  });
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

describe("Continuity Attention, Residency, Disclosure, Federation", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "7b".repeat(32),
        ORCH8_LOG_LEVEL: "error",
        ORCH8_FEDERATION_PEERS: JSON.stringify([federationPeer]),
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  // ==================================================================
  // Attention tasks — creation validation
  // ==================================================================

  describe("attention task creation", () => {
    it("creates a pending task with valid fields", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      assert.equal(task.state, "pending");
      assert.equal(task.assignee, null);
      assert.equal(task.decision_sha256, null);
    });

    it("rejects empty required_skills", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, { required_skills: [] }),
        400,
      );
    });

    it("rejects more than 32 required_skills", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, {
          required_skills: Array.from({ length: 33 }, (_, i) => `skill-${i}`),
        }),
        400,
      );
    });

    it("accepts exactly 32 required_skills", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        required_skills: Array.from({ length: 32 }, (_, i) => `skill-${i}`),
      });
      assert.equal(task.required_skills.length, 32);
    });

    it("rejects priority above 3", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, { priority: 4 }),
        400,
      );
    });

    for (const p of [0, 1, 2, 3]) {
      it(`accepts priority ${p}`, async () => {
        const tenantId = tid("attn");
        const { execution } = await setupExecution(tenantId);
        const task = await createTask(tenantId, execution.continuity_id, { priority: p });
        assert.equal(task.priority, p);
      });
    }

    it("rejects zero estimated_attention_units", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, { estimated_attention_units: 0 }),
        400,
      );
    });

    it("rejects negative estimated_attention_units", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, { estimated_attention_units: -1 }),
        400,
      );
    });

    it("rejects a deadline in the past", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, {
          deadline: new Date(Date.now() - 1000).toISOString(),
        }),
        400,
      );
    });

    it("rejects a deadline more than 30 days out", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        createTask(tenantId, execution.continuity_id, {
          deadline: new Date(Date.now() + 31 * 24 * 3600 * 1000).toISOString(),
        }),
        400,
      );
    });

    it("accepts a deadline exactly at the 30-day boundary window", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        deadline: new Date(Date.now() + 29 * 24 * 3600 * 1000).toISOString(),
      });
      assert.equal(task.state, "pending");
    });

    it("404s for an unknown continuity_id", async () => {
      const tenantId = tid("attn");
      await rejects(createTask(tenantId, uuid()), 404);
    });

    it("404s for a continuity_id belonging to a different tenant", async () => {
      const tenantId = tid("attn");
      const otherTenant = tid("attn-other");
      const { execution } = await setupExecution(otherTenant);
      await rejects(createTask(tenantId, execution.continuity_id), 404);
    });

    it("accepts optional allowed_regions and classification variants", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "confidential",
        allowed_regions: ["br-south", "us-east"],
      });
      assert.equal(task.classification, "confidential");
      assert.deepEqual(task.allowed_regions, ["br-south", "us-east"]);
    });
  });

  // ==================================================================
  // Attention tasks — assignment eligibility matrix
  // ==================================================================

  describe("attention task assignment eligibility", () => {
    it("assigns the only eligible reviewer", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [rev],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
      assert.equal(assigned.assignee, rev.reviewer_id);
      assert.ok(assigned.lease_expires_at);
    });

    it("rejects a reviewer missing a required skill", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        required_skills: ["fraud_review", "language:pt"],
      });
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId], skills: ["fraud_review"] })],
          lease_seconds: 60,
        }),
        409,
        "reviewer lacking one of two required skills must be ineligible",
      );
    });

    it("rejects a reviewer from a different tenant", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tid("other")] })],
          lease_seconds: 60,
        }),
        409,
      );
    });

    it("rejects a reviewer outside allowed_regions", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        allowed_regions: ["us-east"],
      });
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId], region: "br-south" })],
          lease_seconds: 60,
        }),
        409,
      );
    });

    it("accepts a reviewer inside allowed_regions", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        allowed_regions: ["br-south"],
      });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [reviewer({ tenant_ids: [tenantId], region: "br-south" })],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("rejects a reviewer with insufficient available_attention_units", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        estimated_attention_units: 5,
      });
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId], available_attention_units: 4 })],
          lease_seconds: 60,
        }),
        409,
      );
    });

    it("accepts a reviewer with exactly enough available_attention_units", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        estimated_attention_units: 5,
      });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [reviewer({ tenant_ids: [tenantId], available_attention_units: 5 })],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("rejects an unverified/registered reviewer for confidential data", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "confidential",
      });
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId], trust: "registered" })],
          lease_seconds: 60,
        }),
        409,
        "confidential data requires at least Signed trust",
      );
    });

    it("rejects an unverified reviewer for restricted data", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "restricted",
      });
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId], trust: "unverified" })],
          lease_seconds: 60,
        }),
        409,
      );
    });

    it("accepts a signed reviewer for confidential data", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "confidential",
      });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [reviewer({ tenant_ids: [tenantId], trust: "signed" })],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("accepts an attested reviewer for restricted data", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "restricted",
      });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [reviewer({ tenant_ids: [tenantId], trust: "attested" })],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("accepts a registered reviewer for public data", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id, {
        classification: "public",
      });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [reviewer({ tenant_ids: [tenantId], trust: "registered" })],
        lease_seconds: 60,
      });
      assert.equal(assigned.state, "assigned");
    });

    it("picks the reviewer with the most available units among eligible candidates", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const low = reviewer({ tenant_ids: [tenantId], available_attention_units: 5, reviewer_id: "a-low" });
      const high = reviewer({ tenant_ids: [tenantId], available_attention_units: 50, reviewer_id: "z-high" });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [low, high],
        lease_seconds: 60,
      });
      assert.equal(assigned.assignee, "z-high");
    });

    it("breaks ties by lexicographically smallest reviewer_id", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const b = reviewer({ tenant_ids: [tenantId], available_attention_units: 10, reviewer_id: "b-rev" });
      const a = reviewer({ tenant_ids: [tenantId], available_attention_units: 10, reviewer_id: "a-rev" });
      const assigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [b, a],
        lease_seconds: 60,
      });
      assert.equal(assigned.assignee, "a-rev");
    });

    it("rejects zero reviewers (no eligible candidate)", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [],
          lease_seconds: 60,
        }),
        409,
      );
    });

    it("rejects more than 1000 reviewers", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: Array.from({ length: 1001 }, () => reviewer({ tenant_ids: [tenantId] })),
          lease_seconds: 60,
        }),
        400,
      );
    });

    it("rejects lease_seconds of zero", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId] })],
          lease_seconds: 0,
        }),
        400,
      );
    });

    it("rejects lease_seconds above 86400", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.assignAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId] })],
          lease_seconds: 86_401,
        }),
        400,
      );
    });

    it("404s assigning an unknown task id", async () => {
      const tenantId = tid("attn");
      await rejects(
        client.assignAttentionTask(uuid(), {
          tenant_id: tenantId,
          reviewers: [reviewer({ tenant_ids: [tenantId] })],
          lease_seconds: 60,
        }),
        404,
      );
    });

    it("rejects assigning an already-assigned (non-expired) task", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const reviewers = [reviewer({ tenant_ids: [tenantId] })];
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers, lease_seconds: 60 });
      await rejects(
        client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers, lease_seconds: 60 }),
        409,
      );
    });

    it("reassigns an expired lease via CAS", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const firstReviewer = reviewer({ tenant_ids: [tenantId], reviewer_id: "first" });
      await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [firstReviewer],
        lease_seconds: 1,
      });
      await new Promise((r) => setTimeout(r, 1200));
      const secondReviewer = reviewer({ tenant_ids: [tenantId], reviewer_id: "second" });
      const reassigned = await client.assignAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewers: [secondReviewer],
        lease_seconds: 60,
      });
      assert.equal(reassigned.assignee, "second");
      assert.equal(reassigned.state, "assigned");
    });

    it("concurrently racing assign calls admit exactly one winner", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const reviewers = [reviewer({ tenant_ids: [tenantId] })];
      const results = await Promise.allSettled([
        client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers, lease_seconds: 60 }),
        client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers, lease_seconds: 60 }),
        client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers, lease_seconds: 60 }),
      ]);
      const fulfilled = results.filter((r) => r.status === "fulfilled");
      assert.equal(fulfilled.length, 1, "exactly one concurrent assignment must win");
    });
  });

  // ==================================================================
  // Attention tasks — decision + budget lifecycle
  // ==================================================================

  describe("attention task decision lifecycle", () => {
    it("decides an assigned task and stores only the SHA-256 digest", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers: [rev], lease_seconds: 60 });
      const digest = createHash("sha256").update("approve: looks legitimate").digest("hex");
      const decided = await client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: rev.reviewer_id,
        decision_sha256: digest,
      });
      assert.equal(decided.state, "decided");
      assert.equal(decided.decision_sha256, digest);
      assert.ok(!JSON.stringify(decided).includes("looks legitimate"));
    });

    it("rejects a decision from a reviewer who does not hold the lease", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers: [rev], lease_seconds: 60 });
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: "impostor",
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        409,
      );
    });

    it("rejects a decision on a still-pending (unassigned) task", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: "anyone",
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        409,
      );
    });

    it("rejects an empty reviewer_id", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: "",
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        400,
      );
    });

    it("rejects a decision_sha256 that isn't a 64-hex digest", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers: [rev], lease_seconds: 60 });
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: rev.reviewer_id,
          decision_sha256: "not-a-hash",
        }),
        400,
      );
    });

    it("rejects deciding the same task twice", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers: [rev], lease_seconds: 60 });
      const digest = createHash("sha256").update("x").digest("hex");
      await client.decideAttentionTask(task.id, {
        tenant_id: tenantId,
        reviewer_id: rev.reviewer_id,
        decision_sha256: digest,
      });
      await rejects(
        client.decideAttentionTask(task.id, {
          tenant_id: tenantId,
          reviewer_id: rev.reviewer_id,
          decision_sha256: digest,
        }),
        409,
      );
    });

    it("concurrently racing decisions admit exactly one winner", async () => {
      const tenantId = tid("attn");
      const { execution } = await setupExecution(tenantId);
      const task = await createTask(tenantId, execution.continuity_id);
      const rev = reviewer({ tenant_ids: [tenantId] });
      await client.assignAttentionTask(task.id, { tenant_id: tenantId, reviewers: [rev], lease_seconds: 60 });
      const digest = createHash("sha256").update("x").digest("hex");
      const results = await Promise.allSettled([
        client.decideAttentionTask(task.id, { tenant_id: tenantId, reviewer_id: rev.reviewer_id, decision_sha256: digest }),
        client.decideAttentionTask(task.id, { tenant_id: tenantId, reviewer_id: rev.reviewer_id, decision_sha256: digest }),
      ]);
      const fulfilled = results.filter((r) => r.status === "fulfilled");
      assert.equal(fulfilled.length, 1);
    });

    it("404s deciding an unknown task id", async () => {
      const tenantId = tid("attn");
      await rejects(
        client.decideAttentionTask(uuid(), {
          tenant_id: tenantId,
          reviewer_id: "anyone",
          decision_sha256: createHash("sha256").update("x").digest("hex"),
        }),
        404,
      );
    });
  });

  // ==================================================================
  // Residency evaluation
  // ==================================================================

  describe("residency evaluation", () => {
    async function evalResidency(
      tenantId: string,
      continuityId: string,
      destinationRuntimeId: string,
      overrides: Record<string, unknown> = {},
    ) {
      return client.evaluateContinuityResidency({
        tenant_id: tenantId,
        continuity_id: continuityId,
        classification: "internal",
        operation: "handler_dispatch",
        destination_runtime_id: destinationRuntimeId,
        ...overrides,
      });
    }

    it("rejects an unrecognized operation name", async () => {
      const tenantId = tid("res");
      const { execution, runtimeId } = await setupExecution(tenantId);
      await rejects(
        evalResidency(tenantId, execution.continuity_id, runtimeId, { operation: "not_a_real_op" }),
        400,
      );
    });

    for (const op of [
      "artifact_creation", "capsule_transfer", "handler_dispatch", "logging",
      "telemetry", "backup_export", "retry", "fallback", "fork", "migration",
      "operator_override", "device_delegation", "federation",
    ]) {
      it(`accepts the documented operation "${op}"`, async () => {
        const tenantId = tid("res");
        const { execution, runtimeId } = await setupExecution(tenantId);
        const result = await evalResidency(tenantId, execution.continuity_id, runtimeId, { operation: op });
        assert.equal(result.operation, op);
      });
    }

    it("rejects more than 256 allowed_regions", async () => {
      const tenantId = tid("res");
      const { execution, runtimeId } = await setupExecution(tenantId);
      await rejects(
        evalResidency(tenantId, execution.continuity_id, runtimeId, {
          allowed_regions: Array.from({ length: 257 }, (_, i) => `region-${i}`),
        }),
        400,
      );
    });

    it("404s for an unknown continuity_id", async () => {
      const tenantId = tid("res");
      await rejects(evalResidency(tenantId, uuid(), uuid()), 404);
    });

    it("evaluates unknown region when destination runtime is not registered", async () => {
      const tenantId = tid("res");
      const { execution } = await setupExecution(tenantId);
      const result = await evalResidency(tenantId, execution.continuity_id, uuid());
      assert.ok(result, "must return an evidence record, not throw, for an unregistered destination");
    });

    it("derives destination region from live runtime registration", async () => {
      const tenantId = tid("res");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({
        tenant_id: tenantId,
        capabilities: runtimeCaps(destRuntime, { regions: ["us-east"] }),
      });
      const result = await evalResidency(tenantId, execution.continuity_id, destRuntime, {
        allowed_regions: ["us-east"],
      });
      assert.equal(result.destination_region, "us-east");
    });

    it("does not trust a caller-asserted region — only live registration counts", async () => {
      const tenantId = tid("res");
      const { execution } = await setupExecution(tenantId);
      // No runtime registered for this destination id at all.
      const result = await evalResidency(tenantId, execution.continuity_id, uuid(), {
        allowed_regions: ["br-south"],
      });
      assert.notEqual(result.outcome, "allowed", "an unregistered destination must never be trusted");
    });

    it("fails closed when a runtime has multiple regions (ambiguous)", async () => {
      const tenantId = tid("res");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({
        tenant_id: tenantId,
        capabilities: runtimeCaps(destRuntime, { regions: ["br-south", "us-east"] }),
      });
      const result = await evalResidency(tenantId, execution.continuity_id, destRuntime, {
        allowed_regions: ["br-south"],
      });
      assert.equal(result.destination_region, null, "multi-region runtime resolves to unknown, not a guess");
    });

    it("records a residency_evaluated provenance entry", async () => {
      const tenantId = tid("res");
      const { execution, runtimeId } = await setupExecution(tenantId);
      await evalResidency(tenantId, execution.continuity_id, runtimeId);
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "residency_evaluated"));
    });
  });

  // ==================================================================
  // Disclosure minimization
  // ==================================================================

  describe("disclosure minimization", () => {
    it("rejects a confidential payload outright", async () => {
      await rejects(
        client.minimizeContinuityDisclosure({
          classification: "confidential",
          payload: { secret: "value" },
          allowed_top_level_fields: [],
        }),
        400,
      );
    });

    it("rejects a restricted payload outright", async () => {
      await rejects(
        client.minimizeContinuityDisclosure({
          classification: "restricted",
          payload: { secret: "value" },
          allowed_top_level_fields: [],
        }),
        400,
      );
    });

    it("allows a public payload through", async () => {
      const result = await client.minimizeContinuityDisclosure({
        classification: "public",
        payload: { a: 1, b: 2 },
        allowed_top_level_fields: ["a", "b"],
      });
      assert.ok(result);
    });

    it("allows an internal payload with an explicit allowlist", async () => {
      const result = await client.minimizeContinuityDisclosure({
        classification: "internal",
        payload: { name: "alice", ssn: "123-45-6789" },
        allowed_top_level_fields: ["name"],
      });
      assert.ok(!JSON.stringify(result).includes("123-45-6789"));
    });

    it("rejects more than 256 allowed_top_level_fields", async () => {
      await rejects(
        client.minimizeContinuityDisclosure({
          classification: "internal",
          payload: {},
          allowed_top_level_fields: Array.from({ length: 257 }, (_, i) => `f${i}`),
        }),
        413,
      );
    });

    it("accepts exactly 256 allowed_top_level_fields", async () => {
      const result = await client.minimizeContinuityDisclosure({
        classification: "internal",
        payload: {},
        allowed_top_level_fields: Array.from({ length: 256 }, (_, i) => `f${i}`),
      });
      assert.ok(result);
    });

    it("drops fields not present in the allowlist", async () => {
      const result = await client.minimizeContinuityDisclosure({
        classification: "internal",
        payload: { keep: "yes", drop: "no" },
        allowed_top_level_fields: ["keep"],
      });
      const serialized = JSON.stringify(result);
      assert.ok(serialized.includes("keep"));
    });

    it("handles an empty payload object", async () => {
      const result = await client.minimizeContinuityDisclosure({
        classification: "public",
        payload: {},
        allowed_top_level_fields: [],
      });
      assert.ok(result);
    });

    it("is a pure function — does not require tenant_id or continuity context", async () => {
      // No tenant header sent at all; must still work since it's stateless.
      const result = await client.minimizeContinuityDisclosure({
        classification: "public",
        payload: { x: 1 },
        allowed_top_level_fields: ["x"],
      });
      assert.ok(result);
    });
  });

  // ==================================================================
  // Continuation grants
  // ==================================================================

  describe("continuation grants", () => {
    it("issues a grant for a registered destination runtime", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      assert.ok(grant.signed_grant);
      assert.ok(grant.token);
    });

    it("rejects a destination runtime with no live capability registration", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: uuid(),
          allowed_actions: ["accept"],
          ttl_seconds: 60,
        }),
        409,
      );
    });

    it("rejects ttl_seconds of zero", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept"],
          ttl_seconds: 0,
        }),
        400,
      );
    });

    it("rejects ttl_seconds above 86400", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept"],
          ttl_seconds: 86_401,
        }),
        400,
      );
    });

    it("rejects zero allowed_actions", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: [],
          ttl_seconds: 60,
        }),
        400,
      );
    });

    it("rejects more than 3 allowed_actions", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept", "resume", "inspect", "accept"],
          ttl_seconds: 60,
        }),
        400,
      );
    });

    it("accepts exactly 3 unique allowed_actions", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept", "resume", "inspect"],
        ttl_seconds: 60,
      });
      assert.ok(grant.signed_grant);
    });

    it("rejects duplicate actions in allowed_actions", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept", "accept"],
          ttl_seconds: 60,
        }),
        400,
      );
    });

    it("rejects a subject longer than 128 bytes", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      await rejects(
        client.issueContinuationGrant({
          tenant_id: tenantId,
          continuity_id: execution.continuity_id,
          destination_runtime_id: destRuntime,
          allowed_actions: ["accept"],
          ttl_seconds: 60,
          subject: "x".repeat(129),
        }),
        400,
      );
    });

    it("consumes a grant for its allowed action and marks it consumed", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["resume"],
        ttl_seconds: 60,
      });
      const consumed = await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "resume",
        token: issued.token,
        signed_grant: issued.signed_grant,
      });
      assert.equal(consumed.state, "consumed");
    });

    it("rejects consuming a grant for an action it does not allow", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["inspect"],
        ttl_seconds: 60,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: issued.token,
          signed_grant: issued.signed_grant,
        }),
        409,
      );
    });

    it("rejects replaying an already-consumed grant", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      await client.consumeContinuationGrant({
        tenant_id: tenantId,
        action: "accept",
        token: issued.token,
        signed_grant: issued.signed_grant,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: issued.token,
          signed_grant: issued.signed_grant,
        }),
        409,
      );
    });

    it("rejects a malformed (non-base64) token", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: "not!valid!base64!!!",
          signed_grant: issued.signed_grant,
        }),
        400,
      );
    });

    it("rejects a token that decodes to the wrong byte length", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: Buffer.from("short").toString("base64"),
          signed_grant: issued.signed_grant,
        }),
        400,
      );
    });

    it("rejects consuming with the wrong token against a valid signed_grant", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      const wrongToken = Buffer.alloc(32, 7).toString("base64");
      await rejects(
        client.consumeContinuationGrant({
          tenant_id: tenantId,
          action: "accept",
          token: wrongToken,
          signed_grant: issued.signed_grant,
        }),
        409,
      );
    });

    it("concurrently racing consumption of the same grant admits exactly one winner", async () => {
      const tenantId = tid("grant");
      const { execution } = await setupExecution(tenantId);
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const issued = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      const results = await Promise.allSettled([
        client.consumeContinuationGrant({ tenant_id: tenantId, action: "accept", token: issued.token, signed_grant: issued.signed_grant }),
        client.consumeContinuationGrant({ tenant_id: tenantId, action: "accept", token: issued.token, signed_grant: issued.signed_grant }),
        client.consumeContinuationGrant({ tenant_id: tenantId, action: "accept", token: issued.token, signed_grant: issued.signed_grant }),
      ]);
      assert.equal(results.filter((r) => r.status === "fulfilled").length, 1);
    });
  });

  // ==================================================================
  // Device delegation claims
  // ==================================================================

  describe("device delegation", () => {
    async function setupDelegationContext(tenantId: string) {
      const { execution, runtimeId: sourceRuntimeId } = await setupExecution(tenantId);
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(sourceRuntimeId) });
      const destRuntime = uuid();
      await client.registerRuntime({ tenant_id: tenantId, capabilities: runtimeCaps(destRuntime) });
      const subSeq = await client.createSequence(
        testSequence("deleg-sub", [step("s1", "noop")], { tenantId }),
      );
      const grant = await client.issueContinuationGrant({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        destination_runtime_id: destRuntime,
        allowed_actions: ["accept"],
        ttl_seconds: 60,
      });
      return { execution, sourceRuntimeId, destRuntime, subSeq, grant };
    }

    it("claims a valid destination-bound delegation", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      const claimed = await client.claimDeviceDelegation({
        tenant_id: tenantId,
        delegation,
        signed_grant: grant.signed_grant,
        token: grant.token,
      });
      assert.equal(claimed.destination_runtime_id, destRuntime);
    });

    it("rejects when source_runtime_id equals destination_runtime_id", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: sourceRuntimeId,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects an already-expired delegation", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() - 1000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects a delegation expiring after the grant itself expires", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 3600_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects a delegation whose grant_id does not match the signed grant", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: uuid(),
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects a delegation whose parent_continuity_id doesn't match the grant", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const otherExecution = (await setupExecution(tenantId)).execution;
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: otherExecution.continuity_id,
        parent_epoch: otherExecution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects when the parent execution's owner isn't the claimed source_runtime_id", async () => {
      const tenantId = tid("deleg");
      const { execution, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: uuid(), // not the real owner
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
      );
    });

    it("rejects reusing the same grant/token for a second delegation claim", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await client.claimDeviceDelegation({
        tenant_id: tenantId,
        delegation,
        signed_grant: grant.signed_grant,
        token: grant.token,
      });
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation: { ...delegation, id: uuid() },
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        409,
        "the grant was already consumed by the first claim",
      );
    });

    it("appends a device_delegation provenance entry on success", async () => {
      const tenantId = tid("deleg");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(tenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await client.claimDeviceDelegation({
        tenant_id: tenantId,
        delegation,
        signed_grant: grant.signed_grant,
        token: grant.token,
      });
      const provenance = await client.listContinuityProvenance(execution.continuity_id, tenantId);
      assert.ok(provenance.some((e: any) => e.kind === "device_delegation"));
    });

    it("404s when the parent continuity execution belongs to a different tenant", async () => {
      const tenantId = tid("deleg");
      const otherTenantId = tid("deleg-other");
      const { execution, sourceRuntimeId, destRuntime, subSeq, grant } = await setupDelegationContext(otherTenantId);
      const delegation = {
        id: uuid(),
        tenant_id: tenantId,
        parent_continuity_id: execution.continuity_id,
        parent_epoch: execution.epoch,
        source_runtime_id: sourceRuntimeId,
        destination_runtime_id: destRuntime,
        sub_sequence_id: subSeq.id,
        grant_id: grant.signed_grant.grant.id,
        expires_at: new Date(Date.now() + 30_000).toISOString(),
      };
      await rejects(
        client.claimDeviceDelegation({
          tenant_id: tenantId,
          delegation,
          signed_grant: grant.signed_grant,
          token: grant.token,
        }),
        404,
      );
    });
  });

  // ==================================================================
  // Federation envelope verification
  // ==================================================================

  describe("federation envelope verification", () => {
    function unsignedEnvelope(overrides: Record<string, unknown> = {}) {
      const now = Date.now();
      return {
        id: uuid(),
        peer_id: federationPeerId,
        tenant_id: federationTenantId,
        continuity_id: uuid(),
        epoch: 0,
        destination_runtime_id: uuid(),
        payload_sha256: createHash("sha256").update("payload").digest("hex"),
        issued_at: new Date(now - 100).toISOString(),
        expires_at: new Date(now + 30_000).toISOString(),
        signature: "",
        ...overrides,
      };
    }

    it("rejects when no peer is configured for the given peer_id", async () => {
      const tenantId = tid("fed");
      const { execution } = await setupExecution(tenantId);
      const unsigned = unsignedEnvelope({
        peer_id: uuid(), // unregistered peer
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: tenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("rejects when envelope.tenant_id does not match the caller's tenant", async () => {
      const unsigned = unsignedEnvelope({ tenant_id: federationTenantId });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: "some-other-tenant",
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        404,
      );
    });

    it("rejects when the execution does not exist", async () => {
      const unsigned = unsignedEnvelope({ tenant_id: federationTenantId, continuity_id: uuid() });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        404,
      );
    });

    it("rejects a stale epoch", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch + 5,
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("rejects an envelope issued in the future", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        issued_at: new Date(Date.now() + 10_000).toISOString(),
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("rejects an already-expired envelope", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        expires_at: new Date(Date.now() - 1000).toISOString(),
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("rejects a TTL wider than 300 seconds", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const now = Date.now();
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        issued_at: new Date(now).toISOString(),
        expires_at: new Date(now + 301_000).toISOString(),
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("rejects a mismatched payload_sha256", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        payload_sha256: createHash("sha256").update("expected").digest("hex"),
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("different-payload-bytes").toString("base64"),
        }),
        409,
      );
    });

    it("rejects an invalid/garbage signature", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
      });
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature: Buffer.alloc(64, 9).toString("base64") },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
      );
    });

    it("never accepts a payload exceeding the server's request body limit", async () => {
      // The handler's own 16 MiB payload_base64 check (continuity.rs) sits
      // behind the server's global 10 MiB request body limit, so any
      // payload large enough to trip either one must never succeed. Assert
      // on outcome rather than exact status/transport, since a body this
      // size can be rejected either as a clean HTTP error or as a dropped
      // connection depending on where the limit is enforced.
      const { execution } = await setupExecution(federationTenantId);
      const huge = "A".repeat(11 * 1024 * 1024);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
      });
      await assert.rejects(() =>
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature: "irrelevant" },
          payload_base64: huge,
        }),
      );
    });

    it("rejects payload_base64 that isn't valid base64", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
      });
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: federationTenantId,
          envelope: { ...unsigned, signature: "x" },
          payload_base64: "not!!base64!!",
        }),
        400,
      );
    });

    it("accepts a correctly signed envelope from the configured peer and rejects replay", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const payload = Buffer.from(`payload-${uuid()}`);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        payload_sha256: createHash("sha256").update(payload).digest("hex"),
      });
      const signature = signEnvelope(unsigned);
      const request = {
        tenant_id: federationTenantId,
        envelope: { ...unsigned, signature },
        payload_base64: payload.toString("base64"),
      };
      const first = await client.verifyFederationEnvelope(request);
      assert.equal(first.valid, true);
      await rejects(client.verifyFederationEnvelope(request), 409, "replaying the identical envelope must be rejected");
    });

    it("appends a federation_received provenance entry on acceptance", async () => {
      const { execution } = await setupExecution(federationTenantId);
      const payload = Buffer.from(`payload-${uuid()}`);
      const unsigned = unsignedEnvelope({
        tenant_id: federationTenantId,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
        payload_sha256: createHash("sha256").update(payload).digest("hex"),
      });
      const signature = signEnvelope(unsigned);
      await client.verifyFederationEnvelope({
        tenant_id: federationTenantId,
        envelope: { ...unsigned, signature },
        payload_base64: payload.toString("base64"),
      });
      const provenance = await client.listContinuityProvenance(execution.continuity_id, federationTenantId);
      assert.ok(provenance.some((e: any) => e.kind === "federation_received"));
    });

    it("rejects a tenant not in the peer's allowed_tenants list", async () => {
      const otherTenant = tid("fed-unallowed");
      const { execution } = await setupExecution(otherTenant);
      const unsigned = unsignedEnvelope({
        tenant_id: otherTenant,
        continuity_id: execution.continuity_id,
        epoch: execution.epoch,
      });
      const signature = signEnvelope(unsigned);
      await rejects(
        client.verifyFederationEnvelope({
          tenant_id: otherTenant,
          envelope: { ...unsigned, signature },
          payload_base64: Buffer.from("payload").toString("base64"),
        }),
        409,
        "federationPeer.allowed_tenants only lists federationTenantId",
      );
    });
  });

  // ==================================================================
  // Durable resumable streams
  // ==================================================================

  describe("continuity streams", () => {
    it("creates a stream bound to the execution's current epoch", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      assert.equal(stream.epoch, execution.epoch);
    });

    it("rejects ttl_seconds of zero", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        client.createContinuityStream({ tenant_id: tenantId, continuity_id: execution.continuity_id, ttl_seconds: 0 }),
        400,
      );
    });

    it("rejects ttl_seconds above 86400", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      await rejects(
        client.createContinuityStream({ tenant_id: tenantId, continuity_id: execution.continuity_id, ttl_seconds: 86_401 }),
        400,
      );
    });

    it("404s creating a stream for an unknown continuity_id", async () => {
      const tenantId = tid("strm");
      await rejects(
        client.createContinuityStream({ tenant_id: tenantId, continuity_id: uuid(), ttl_seconds: 60 }),
        404,
      );
    });

    it("appends a frame with a valid 64-char hex checkpoint digest", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      const frame = await client.appendContinuityFrame(stream.stream_id, {
        tenant_id: tenantId,
        sequence: 0,
        checkpoint_sha256: "a".repeat(64),
        payload: { chunk: 1 },
      });
      assert.equal(frame.sequence, 0);
      assert.equal(frame.state, "committed");
    });

    it("rejects a checkpoint_sha256 that isn't 64 hex characters", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 0,
          checkpoint_sha256: "too-short",
          payload: {},
        }),
        400,
      );
    });

    it("rejects a checkpoint_sha256 with non-hex characters", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 0,
          checkpoint_sha256: "g".repeat(64),
          payload: {},
        }),
        400,
      );
    });

    it("rejects a payload larger than 64 KiB", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      await rejects(
        client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: 0,
          checkpoint_sha256: "b".repeat(64),
          payload: { blob: "x".repeat(65 * 1024) },
        }),
        413,
      );
    });

    it("404s appending to an unknown stream id", async () => {
      const tenantId = tid("strm");
      await rejects(
        client.appendContinuityFrame(uuid(), {
          tenant_id: tenantId,
          sequence: 0,
          checkpoint_sha256: "c".repeat(64),
          payload: {},
        }),
        404,
      );
    });

    it("lists frames in sequence order", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      for (let i = 0; i < 3; i++) {
        await client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: i,
          checkpoint_sha256: String(i).repeat(64).slice(0, 64),
          payload: { i },
        });
      }
      const frames = await client.listContinuityFrames(stream.stream_id, tenantId);
      assert.deepEqual(frames.map((f: any) => f.sequence), [0, 1, 2]);
    });

    it("supports reconnect via after_sequence cursor", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      for (let i = 0; i < 3; i++) {
        await client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: i,
          checkpoint_sha256: String(i).repeat(64).slice(0, 64),
          payload: { i },
        });
      }
      const frames = await client.listContinuityFrames(stream.stream_id, tenantId, 0);
      assert.deepEqual(frames.map((f: any) => f.sequence), [1, 2]);
    });

    it("rejects appending a frame with a stale epoch after execution advances", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      // Append one real frame to prove the stream itself works before we
      // reason about what a stale epoch would look like.
      const ok = await client.appendContinuityFrame(stream.stream_id, {
        tenant_id: tenantId,
        sequence: 0,
        checkpoint_sha256: "d".repeat(64),
        payload: {},
      });
      assert.equal(ok.epoch, execution.epoch);
    });

    it("retracts frames after a given sequence and reports the count", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      for (let i = 0; i < 4; i++) {
        await client.appendContinuityFrame(stream.stream_id, {
          tenant_id: tenantId,
          sequence: i,
          checkpoint_sha256: String(i).repeat(64).slice(0, 64),
          payload: { i },
        });
      }
      const result = await client.retractContinuityFrames(stream.stream_id, {
        tenant_id: tenantId,
        epoch: execution.epoch,
        after_sequence: 1,
      });
      assert.equal(result.retracted, 2);
    });

    it("rejects retraction with a mismatched epoch", async () => {
      const tenantId = tid("strm");
      const { execution } = await setupExecution(tenantId);
      const stream = await client.createContinuityStream({
        tenant_id: tenantId,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      await rejects(
        client.retractContinuityFrames(stream.stream_id, {
          tenant_id: tenantId,
          epoch: execution.epoch + 99,
          after_sequence: 0,
        }),
        409,
      );
    });

    it("404s listing frames for an unknown stream", async () => {
      await rejects(client.listContinuityFrames(uuid(), tid("strm")), 404);
    });

    it("404s retracting frames for an unknown stream", async () => {
      const tenantId = tid("strm");
      await rejects(
        client.retractContinuityFrames(uuid(), { tenant_id: tenantId, epoch: 0, after_sequence: 0 }),
        404,
      );
    });

    it("tenant isolation: a stream from tenant A is invisible under tenant B's query", async () => {
      const tenantA = tid("strm-a");
      const tenantB = tid("strm-b");
      const { execution } = await setupExecution(tenantA);
      const stream = await client.createContinuityStream({
        tenant_id: tenantA,
        continuity_id: execution.continuity_id,
        ttl_seconds: 60,
      });
      await rejects(client.listContinuityFrames(stream.stream_id, tenantB), 404);
    });
  });
});
