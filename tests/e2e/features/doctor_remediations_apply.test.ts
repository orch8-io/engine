/**
 * POST /instances/{id}/remediations/apply — applying state-bound previews.
 *
 * The server re-diagnoses immediately before applying: stale preview ids,
 * cross-instance ids, manual recipes, and unacknowledged side-effect risk
 * all fail closed with 400 and leave the instance untouched. Successful
 * applies return before/after evidence and really move the instance.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { ApiError, Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function makePaused(tenantId: string): Promise<string> {
  const seq = testSequence(
    "apply-paused",
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
    "apply-failed",
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
  const seq = testSequence("apply-completed", [step("a", "noop")], { tenantId });
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
  const seq = testSequence("apply-approval", [review], { tenantId });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "waiting", { timeoutMs: 15_000 });
  return id;
}

async function expectApplyError(
  id: string,
  body: Record<string, unknown>,
  status: number,
  pattern?: RegExp,
): Promise<void> {
  await assert.rejects(client.applyRemediation(id, body as any), (err: unknown) => {
    assert.ok(err instanceof ApiError);
    assert.equal(err.status, status, err.body);
    if (pattern) assert.match(err.body, pattern);
    return true;
  });
}

// The apply handler writes Scheduled with next_fire_at = now and the e2e
// server ticks every 100ms, so by the time the response is re-read the
// instance may already be running — accept either state.
function assertScheduledOrRunning(state: string): void {
  assert.ok(
    state === "scheduled" || state === "running",
    `expected after_state scheduled|running, got ${state}`,
  );
}

describe("Execution Doctor — remediation apply", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("resume apply on a paused instance returns evidence and moves it to scheduled", async () => {
    const tenantId = `ap-r-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);
    const [preview] = await client.listRemediationPreviews(id);
    assert.equal(preview.action, "resume_instance");

    const evidence = await client.applyRemediation(id, {
      preview_id: preview.preview_id,
    });
    assert.equal(evidence.preview_id, preview.preview_id);
    assert.equal(evidence.action, "resume_instance");
    assert.equal(evidence.before_state, "paused");
    assertScheduledOrRunning(evidence.after_state);
    assert.ok(!Number.isNaN(Date.parse(evidence.applied_at)));

    const inst = await client.getInstance(id);
    assert.notEqual(inst.state, "paused", "instance must have left paused");
  });

  it("replaying a consumed resume preview fails closed with 400", async () => {
    const tenantId = `ap-replay-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);
    const [preview] = await client.listRemediationPreviews(id);

    await client.applyRemediation(id, { preview_id: preview.preview_id });
    await expectApplyError(
      id,
      { preview_id: preview.preview_id },
      400,
      /stale|does not belong/,
    );
  });

  it("retry apply without acknowledge_side_effect_risk is rejected, instance stays failed", async () => {
    const tenantId = `ap-noack-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);
    const [preview] = await client.listRemediationPreviews(id);
    assert.equal(preview.side_effect_risk, true);

    await expectApplyError(
      id,
      { preview_id: preview.preview_id },
      400,
      /acknowledge_side_effect_risk/,
    );
    const inst = await client.getInstance(id);
    assert.equal(inst.state, "failed", "rejected apply must not move the instance");
  });

  it("retry apply with acknowledgement re-runs the instance (which fails again)", async () => {
    const tenantId = `ap-ack-${uuid().slice(0, 8)}`;
    const id = await makeFailed(tenantId);
    const [preview] = await client.listRemediationPreviews(id);

    const evidence = await client.applyRemediation(id, {
      preview_id: preview.preview_id,
      acknowledge_side_effect_risk: true,
    });
    assert.equal(evidence.action, "retry_instance");
    assert.equal(evidence.before_state, "failed");
    assertScheduledOrRunning(evidence.after_state);

    // The retry is real: the fail handler executes again and the instance
    // lands back in failed — proving the execution tree was reset.
    const rerun = await client.waitForState(id, "failed", { timeoutMs: 15_000 });
    assert.equal(rerun.state, "failed");

    // And the doctor once again offers a fresh, differently-usable preview.
    const previews = await client.listRemediationPreviews(id);
    assert.equal(previews.length, 1);
    assert.equal(previews[0].action, "retry_instance");
  });

  it("acknowledge flag on a risk-free resume is harmless", async () => {
    const tenantId = `ap-harmless-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);
    const [preview] = await client.listRemediationPreviews(id);
    assert.equal(preview.side_effect_risk, false);

    const evidence = await client.applyRemediation(id, {
      preview_id: preview.preview_id,
      acknowledge_side_effect_risk: true,
    });
    assertScheduledOrRunning(evidence.after_state);
  });

  it("preview goes stale when the instance state changes after previewing", async () => {
    const tenantId = `ap-stale-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);
    const [preview] = await client.listRemediationPreviews(id);

    // Operator resumes through the plain state API between preview and apply.
    await client.updateState(id, "scheduled");

    await expectApplyError(
      id,
      { preview_id: preview.preview_id },
      400,
      /stale|does not belong/,
    );
  });

  it("a preview minted for instance A cannot be applied to instance B", async () => {
    const tenantId = `ap-cross-${uuid().slice(0, 8)}`;
    const idA = await makePaused(tenantId);
    const idB = await makePaused(tenantId);
    const [previewA] = await client.listRemediationPreviews(idA);

    await expectApplyError(
      idB,
      { preview_id: previewA.preview_id },
      400,
      /stale|does not belong/,
    );
    const instB = await client.getInstance(idB);
    assert.equal(instB.state, "paused", "instance B must be untouched");
  });

  it("a fabricated preview id is rejected", async () => {
    const tenantId = `ap-bogus-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);

    await expectApplyError(
      id,
      { preview_id: `${id}:paused:PAUSED:9:9` },
      400,
      /stale|does not belong/,
    );
    await expectApplyError(
      id,
      { preview_id: "totally-bogus" },
      400,
      /stale|does not belong/,
    );
  });

  it("manual remediations cannot be applied through the API", async () => {
    const tenantId = `ap-manual-${uuid().slice(0, 8)}`;
    const id = await makeWaitingApproval(tenantId);
    const [preview] = await client.listRemediationPreviews(id);
    assert.equal(preview.action, "manual");

    await expectApplyError(
      id,
      { preview_id: preview.preview_id, acknowledge_side_effect_risk: true },
      400,
      /manually/,
    );
    const inst = await client.getInstance(id);
    assert.equal(inst.state, "waiting", "manual apply attempt must not move the instance");
  });

  it("applying anything to a completed instance fails (no previews exist)", async () => {
    const tenantId = `ap-done-${uuid().slice(0, 8)}`;
    const id = await makeCompleted(tenantId);

    await expectApplyError(
      id,
      { preview_id: `${id}:completed:TERMINAL_STATE:0:0` },
      400,
      /stale|does not belong/,
    );
  });

  it("apply on an unknown instance returns 404", async () => {
    await expectApplyError(uuid(), { preview_id: "x" }, 404);
  });

  it("cross-tenant apply via X-Tenant-Id returns 404 and changes nothing", async () => {
    const tenantId = `ap-iso-${uuid().slice(0, 8)}`;
    const id = await makePaused(tenantId);
    const [preview] = await client.listRemediationPreviews(id);

    const res = await fetch(
      `${client.baseUrl}/instances/${id}/remediations/apply`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Tenant-Id": `other-${uuid().slice(0, 8)}`,
        },
        body: JSON.stringify({ preview_id: preview.preview_id }),
      },
    );
    assert.equal(res.status, 404);

    const inst = await client.getInstance(id);
    assert.equal(inst.state, "paused", "cross-tenant apply must not move the instance");
  });
});
