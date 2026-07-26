/**
 * GET /instances/{id}/remediations — state-bound remediation previews.
 *
 * Previews never mutate state; each `preview_id` embeds
 * `{instance}:{state}:{finding}:{diagnosis}:{remediation}` so any state
 * transition invalidates it. Actions classify into `resume_instance`,
 * `retry_instance`, or `manual`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function makePaused(tenantId: string): Promise<string> {
  const seq = testSequence(
    "prev-paused",
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

async function makeFailed(tenantId: string): Promise<string> {
  const seq = testSequence(
    "prev-failed",
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

async function makeCompleted(tenantId: string): Promise<string> {
  const seq = testSequence("prev-completed", [step("a", "noop")], { tenantId });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "completed", { timeoutMs: 10_000 });
  return id;
}

async function makeWaitingApproval(tenantId: string): Promise<string> {
  const review = step(
    "review",
    "human_review",
    { prompt: "approve?" },
    {
      wait_for_input: {
        prompt: "approve?",
        choices: [{ label: "Approve", value: "approve" }],
      },
    },
  );
  const seq = testSequence("prev-approval", [review], { tenantId });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "waiting", { timeoutMs: 15_000 });
  return id;
}

async function makeNoWorker(tenantId: string): Promise<string> {
  const handler = `ghost_handler_${uuid().slice(0, 8)}`;
  const seq = testSequence("prev-noworker", [step("s1", handler, {})], {
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

describe("Execution Doctor — remediation previews", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("failed instance yields exactly one retry_instance preview bound to :failed:", async () => {
    const tenantId = `prev-f-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);

    const previews = await client.listRemediationPreviews(id);
    assert.equal(previews.length, 1);
    const p = previews[0];
    assert.equal(p.action, "retry_instance");
    assert.equal(p.finding_code, "TERMINAL_STATE");
    assert.equal(p.remediation_index, 0);
    assert.equal(p.side_effect_risk, true);
    assert.equal(p.expected_state, "failed");
    assert.equal(p.command, `orch8 instance retry ${id}`);
    assert.equal(p.preview_id, `${id}:failed:TERMINAL_STATE:0:0`);
    assert.equal(typeof p.summary, "string");
    assert.ok(p.summary.length > 0);
  });

  it("paused instance yields a resume_instance preview without side-effect risk", async () => {
    const tenantId = `prev-p-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);

    const previews = await client.listRemediationPreviews(id);
    assert.equal(previews.length, 1);
    const p = previews[0];
    assert.equal(p.action, "resume_instance");
    assert.equal(p.finding_code, "PAUSED");
    assert.equal(p.side_effect_risk, false);
    assert.equal(p.expected_state, "paused");
    assert.equal(p.command, `orch8 signal ${id} resume`);
    assert.equal(p.preview_id, `${id}:paused:PAUSED:0:0`);
  });

  it("completed instance yields no previews at all", async () => {
    const tenantId = `prev-c-${uuid().slice(0, 8)}`;
    const id = await makeCompleted(tenantId);

    const previews = await client.listRemediationPreviews(id);
    assert.deepEqual(previews, []);
  });

  it("waiting-for-input yields a manual preview carrying the human resolution command", async () => {
    const tenantId = `prev-a-${uuid().slice(0, 8)}`;
    const id = await makeWaitingApproval(tenantId);

    const previews = await client.listRemediationPreviews(id);
    assert.equal(previews.length, 1);
    const p = previews[0];
    assert.equal(p.action, "manual");
    assert.equal(p.finding_code, "PENDING_APPROVAL");
    assert.equal(p.expected_state, "waiting");
    assert.equal(p.side_effect_risk, false);
    assert.match(p.command, new RegExp(`orch8 signal ${id} custom:human_input:review`));
    assert.ok(p.preview_id.startsWith(`${id}:waiting:PENDING_APPROVAL:`));
  });

  it("no-worker finding yields a manual preview with no engine command", async () => {
    const tenantId = `prev-nw-${uuid().slice(0, 8)}`;
    const id = await makeNoWorker(tenantId);

    const previews = await client.listRemediationPreviews(id);
    assert.equal(previews.length, 1);
    const p = previews[0];
    assert.equal(p.action, "manual");
    assert.equal(p.finding_code, "NO_COMPATIBLE_WORKER");
    assert.equal(p.command ?? null, null, "no engine command exists for starting a worker");
    assert.match(p.summary, /worker/i);
  });

  it("previews are deterministic across repeated calls", async () => {
    const tenantId = `prev-det-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);

    const first = await client.listRemediationPreviews(id);
    const second = await client.listRemediationPreviews(id);
    assert.deepEqual(second, first);
  });

  it("every preview id embeds the instance id, state, and finding code", async () => {
    const tenantId = `prev-fmt-${uuid().slice(0, 8)}`;
    const scenarios: Array<[string, string]> = [
      [await makeFailed(tenantId), "failed"],
      [await makePaused(tenantId), "paused"],
      [await makeWaitingApproval(tenantId), "waiting"],
      [await makeNoWorker(tenantId), "waiting"],
    ];
    for (const [id, expectedState] of scenarios) {
      const previews = await client.listRemediationPreviews(id);
      const report = await client.getDiagnosis(id);
      assert.equal(report.state, expectedState);
      for (const p of previews) {
        const parts = p.preview_id.split(":");
        assert.equal(parts[0], id);
        assert.equal(parts[1], report.state);
        assert.equal(parts[2], p.finding_code);
        assert.match(parts[3]!, /^\d+$/);
        assert.match(parts[4]!, /^\d+$/);
        assert.equal(p.expected_state, report.state);
      }
    }
  });

  it("unknown instance returns 404, malformed id returns 400", async () => {
    await assert.rejects(
      client.listRemediationPreviews(uuid()),
      (err: unknown) => {
        assert.ok(err instanceof ApiError);
        assert.equal(err.status, 404);
        return true;
      },
    );
    await assert.rejects(
      client.listRemediationPreviews("not-a-uuid"),
      (err: unknown) => {
        assert.ok(err instanceof ApiError);
        assert.equal(err.status, 400);
        return true;
      },
    );
  });

  it("cross-tenant preview request via X-Tenant-Id returns 404", async () => {
    const tenantId = `prev-iso-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);

    const res = await fetch(`${client.baseUrl}/instances/${id}/remediations`, {
      headers: { "X-Tenant-Id": `other-${uuid().slice(0, 8)}` },
    });
    assert.equal(res.status, 404);
  });
});
