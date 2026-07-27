/**
 * GET /instances/{id}/diagnosis — actionable execution doctor, read side.
 *
 * Scenario matrix: each instance state maps to a deterministic finding
 * (code, category, confidence, health, severity), with remediations
 * described but never executed by this endpoint.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

const CATEGORY_RANK: Record<string, number> = {
  direct_evidence: 0,
  probable_cause: 1,
  health_warning: 2,
};

/** Instance paused while its long sleep runs → PAUSED finding. */
async function makePaused(tenantId: string): Promise<string> {
  const seq = testSequence(
    "doc-paused",
    [step("s", "sleep", { duration_ms: 60_000 })],
    { tenantId },
  );
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "running", { timeoutMs: 10_000 });
  await client.updateState(id, "paused");
  return id;
}

/** Instance whose only step always fails → failed/TERMINAL_STATE. */
async function makeFailed(tenantId: string): Promise<string> {
  const seq = testSequence(
    "doc-failed",
    [step("boom", "fail", { message: "kaboom" })],
    { tenantId },
  );
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "failed", { timeoutMs: 10_000 });
  return id;
}

/** Instance waiting on human input → PENDING_APPROVAL. */
async function makeWaitingApproval(tenantId: string): Promise<string> {
  const review = step(
    "review",
    "human_review",
    { prompt: "approve?" },
    {
      wait_for_input: {
        prompt: "approve?",
        choices: [
          { label: "Approve", value: "approve" },
          { label: "Reject", value: "reject" },
        ],
      },
    },
  );
  const seq = testSequence("doc-approval", [review], { tenantId });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "waiting", { timeoutMs: 15_000 });
  return id;
}

/** Waiting with no timer/approval/child/task → WAITING_EXTERNAL_EVENT. */
async function makeWaitingExternal(tenantId: string): Promise<string> {
  const seq = testSequence(
    "doc-wait-ext",
    [step("s", "sleep", { duration_ms: 60_000 })],
    { tenantId },
  );
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "running", { timeoutMs: 10_000 });
  await client.updateState(id, "waiting");
  return id;
}

/** Scheduled with a far-future timer → WAITING_UNTIL. */
async function makeWaitingUntil(tenantId: string): Promise<string> {
  const id = await makePaused(tenantId);
  const fireAt = new Date(Date.now() + 3_600_000).toISOString();
  await client.updateState(id, "scheduled", fireAt);
  return id;
}

/** Queued for a handler no worker serves → NO_COMPATIBLE_WORKER. */
async function makeNoWorker(tenantId: string): Promise<string> {
  const handler = `ghost_handler_${uuid().slice(0, 8)}`;
  const seq = testSequence("doc-noworker", [step("s1", handler, {})], {
    tenantId,
  });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "waiting", { timeoutMs: 10_000 });
  return id;
}

describe("Execution Doctor — diagnosis classification", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("failed instance: TERMINAL_STATE, direct evidence, certain, retry remediation", async () => {
    const tenantId = `doc-f-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);

    const report = await client.getDiagnosis(id);
    assert.equal(report.instance_id, id);
    assert.equal(report.state, "failed");
    assert.ok(!Number.isNaN(Date.parse(report.generated_at)));
    assert.ok(
      Math.abs(Date.now() - Date.parse(report.generated_at)) < 60_000,
      "generated_at must be fresh",
    );

    assert.equal(report.diagnoses.length, 1);
    const d = report.diagnoses[0];
    assert.equal(d.code, "TERMINAL_STATE");
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "certain");
    assert.equal(d.health, "expected");
    assert.equal(d.severity, "info");
    assert.match(d.summary, /failed/);

    assert.equal(d.remediation.length, 1);
    assert.equal(d.remediation[0].command, `orch8 instance retry ${id}`);
    assert.equal(
      d.remediation[0].side_effect_risk,
      true,
      "retrying may repeat external side effects",
    );
  });

  it("completed instance: TERMINAL_STATE with no remediation offered", async () => {
    const tenantId = `doc-c-${uuid().slice(0, 8)}`;
    const seq = testSequence("doc-completed", [step("a", "noop")], { tenantId });
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "completed", { timeoutMs: 10_000 });

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "completed");
    assert.equal(report.diagnoses.length, 1);
    const d = report.diagnoses[0];
    assert.equal(d.code, "TERMINAL_STATE");
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "certain");
    assert.ok(
      !d.remediation || d.remediation.length === 0,
      "a completed instance has nothing to remediate",
    );
  });

  it("paused instance: PAUSED, direct evidence, certain, resume remediation", async () => {
    const tenantId = `doc-p-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "paused");
    const d = report.diagnoses.find((x: any) => x.code === "PAUSED");
    assert.ok(d, `expected PAUSED in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "certain");
    assert.equal(d.health, "expected");
    assert.equal(d.remediation.length, 1);
    assert.equal(d.remediation[0].command, `orch8 signal ${id} resume`);
    assert.equal(d.remediation[0].side_effect_risk ?? false, false);
  });

  it("waiting-for-input: PENDING_APPROVAL names the blocked step", async () => {
    const tenantId = `doc-a-${uuid().slice(0, 8)}`;
    const id = await makeWaitingApproval(tenantId);

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "waiting");
    const d = report.diagnoses.find((x: any) => x.code === "PENDING_APPROVAL");
    assert.ok(d, `expected PENDING_APPROVAL in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "certain");
    assert.match(d.summary, /'review'/);
    assert.equal(d.remediation.length, 1);
    assert.match(
      d.remediation[0].command,
      new RegExp(`orch8 signal ${id} custom:human_input:review`),
    );
  });

  it("waiting without explanation: WAITING_EXTERNAL_EVENT at medium confidence", async () => {
    const tenantId = `doc-w-${uuid().slice(0, 8)}`;
    const id = await makeWaitingExternal(tenantId);

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "waiting");
    const d = report.diagnoses.find(
      (x: any) => x.code === "WAITING_EXTERNAL_EVENT",
    );
    assert.ok(d, `expected WAITING_EXTERNAL_EVENT in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "probable_cause");
    assert.equal(d.confidence, "medium");
    assert.equal(d.health, "expected");
    assert.match(d.remediation[0].command, new RegExp(`orch8 signal ${id} custom:<name>`));
  });

  it("scheduled with a future timer: WAITING_UNTIL certain with next_fire_at evidence", async () => {
    const tenantId = `doc-t-${uuid().slice(0, 8)}`;
    const id = await makeWaitingUntil(tenantId);

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "scheduled");
    const d = report.diagnoses.find((x: any) => x.code === "WAITING_UNTIL");
    assert.ok(d, `expected WAITING_UNTIL in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "certain");
    assert.equal(d.health, "expected");
    assert.equal(d.severity, "info");
    const ev = (d.evidence ?? []).find((e: any) => e.label === "next_fire_at");
    assert.ok(ev, "next_fire_at evidence must be attached");
    assert.ok(Date.parse(ev.summary) > Date.now(), "timer must be in the future");
  });

  it("queued with no serving worker: NO_COMPATIBLE_WORKER, high confidence, degraded", async () => {
    const tenantId = `doc-nw-${uuid().slice(0, 8)}`;
    const id = await makeNoWorker(tenantId);

    const report = await client.getDiagnosis(id);
    const d = report.diagnoses.find(
      (x: any) => x.code === "NO_COMPATIBLE_WORKER",
    );
    assert.ok(d, `expected NO_COMPATIBLE_WORKER in ${JSON.stringify(report.diagnoses)}`);
    assert.equal(d.category, "direct_evidence");
    assert.equal(d.confidence, "high");
    assert.equal(d.health, "degraded");
    assert.equal(d.severity, "error");
    assert.match(d.summary, /no\s+live worker/);
    assert.equal(d.remediation.length, 1);
    assert.equal(
      d.remediation[0].command ?? null,
      null,
      "starting a worker cannot be done by the engine — no command",
    );
    // The open worker task explains the wait: no WAITING_EXTERNAL_EVENT.
    assert.ok(
      !report.diagnoses.some((x: any) => x.code === "WAITING_EXTERNAL_EVENT"),
    );
  });

  it("worker-driven failure also classifies as retryable TERMINAL_STATE", async () => {
    const tenantId = `doc-wf-${uuid().slice(0, 8)}`;
    const handler = `failing_handler_${uuid().slice(0, 8)}`;
    const seq = testSequence("doc-worker-fail", [step("s1", handler, {})], {
      tenantId,
    });
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(id, "waiting", { timeoutMs: 10_000 });

    const workerId = `w-${uuid().slice(0, 6)}`;
    const tasks = await client.pollWorkerTasks(handler, workerId);
    assert.equal(tasks.length, 1);
    await client.failWorkerTask(tasks[0]!.id, workerId, "fatal", false);
    await client.waitForState(id, "failed", { timeoutMs: 10_000 });

    const report = await client.getDiagnosis(id);
    assert.equal(report.state, "failed");
    assert.equal(report.diagnoses.length, 1);
    assert.equal(report.diagnoses[0].code, "TERMINAL_STATE");
    assert.equal(
      report.diagnoses[0].remediation[0].command,
      `orch8 instance retry ${id}`,
    );
  });

  it("diagnoses are ranked: direct evidence before probable cause before health warnings", async () => {
    const tenantId = `doc-rank-${uuid().slice(0, 8)}`;
    // Exercise the ranking invariant across a spread of scenarios; every
    // report must be non-decreasing in category rank.
    const ids = [
      await makeFailed(tenantId),
      await makePaused(tenantId),
      await makeWaitingApproval(tenantId),
      await makeWaitingExternal(tenantId),
      await makeNoWorker(tenantId),
    ];
    for (const id of ids) {
      const report = await client.getDiagnosis(id);
      const ranks = report.diagnoses.map(
        (d: any) => CATEGORY_RANK[d.category],
      );
      assert.ok(
        ranks.every((r: number) => r !== undefined),
        `unknown category in ${JSON.stringify(ranks)}`,
      );
      for (let i = 1; i < ranks.length; i++) {
        assert.ok(
          ranks[i]! >= ranks[i - 1]!,
          `diagnoses out of order for ${id}: ${JSON.stringify(ranks)}`,
        );
      }
      // Enum hygiene: only known health/severity/confidence values.
      for (const d of report.diagnoses) {
        assert.match(d.health, /^(expected|degraded|inconsistent)$/);
        assert.match(d.severity, /^(info|warning|error|critical)$/);
        assert.match(d.confidence, /^(low|medium|high|certain)$/);
      }
    }
  });

  it("unknown instance returns 404 on diagnosis", async () => {
    await assert.rejects(client.getDiagnosis(uuid()), (err: unknown) => {
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 404);
      return true;
    });
  });

  it("malformed instance id returns 400", async () => {
    await assert.rejects(client.getDiagnosis("not-a-uuid"), (err: unknown) => {
      assert.ok(err instanceof ApiError);
      assert.equal(err.status, 400);
      return true;
    });
  });

  it("cross-tenant diagnosis via X-Tenant-Id returns 404, not the report", async () => {
    const tenantId = `doc-iso-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);

    const res = await fetch(`${client.baseUrl}/instances/${id}/diagnosis`, {
      headers: { "X-Tenant-Id": `other-${uuid().slice(0, 8)}` },
    });
    assert.equal(res.status, 404);
    // And without a header the report is still reachable (harness auth off).
    const ok = await client.getDiagnosis(id);
    assert.equal(ok.instance_id, id);
  });
});
