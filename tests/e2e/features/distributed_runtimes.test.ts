/**
 * Distributed execution (runtime nodes): handoff fencing with a worker in
 * flight, browser-session token scope, and "browsers never receive secrets".
 *
 * SELF_MANAGED (`self-managed.ts`): capsule export needs
 * ORCH8_ENCRYPTION_KEY at boot. Every scenario still uses unique tenants and
 * handler names. The server runs `--insecure`, where browser-session tokens
 * are still verified (process-local key) and bound.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { Orch8Client, ApiError, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

async function raw(
  method: string,
  path: string,
  body: unknown,
  headers: Record<string, string> = {},
): Promise<{ status: number; json: any }> {
  const init: RequestInit = { method, headers: { "Content-Type": "application/json", ...headers } };
  if (body !== undefined) init.body = JSON.stringify(body);
  const res = await fetch(`${client.baseUrl}${path}`, init);
  const text = await res.text();
  return { status: res.status, json: text ? JSON.parse(text) : {} };
}

async function waitFor<T>(fn: () => Promise<T | undefined>, timeoutMs = 15_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value !== undefined) return value;
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(`timeout after ${timeoutMs}ms`);
}

function caps(runtimeId: string, kind: string, handler: string) {
  const now = new Date();
  return {
    runtime_id: runtimeId,
    kind,
    trust: "registered",
    handlers: [handler],
    offline_capable: false,
    connectivity: "wifi",
    observed_at: now.toISOString(),
    expires_at: new Date(now.getTime() + 240_000).toISOString(),
  };
}

async function mintBrowserSession(tenantId: string, handlers: string[]) {
  const minted = await raw(
    "POST",
    "/runtimes/browser-sessions",
    { handlers, ttl_secs: 600 },
    { "X-Tenant-Id": tenantId },
  );
  assert.equal(minted.status, 201, JSON.stringify(minted.json));
  return minted.json as { token: string; runtime_id: string; expires_at: string };
}

describe("Distributed runtimes", () => {
  let server: ServerHandle | undefined;
  const artifactDir = `/tmp/o8-distributed-e2e-${uuid().slice(0, 8)}`;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_ENCRYPTION_KEY: "44".repeat(32),
        ORCH8_ARTIFACT_BACKEND: "local",
        ORCH8_ARTIFACT_PATH: artifactDir,
        ORCH8_LOG_LEVEL: "error",
      },
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("refuses a handoff export while a worker task is in flight", async () => {
    const tenantId = `dist-handoff-${uuid().slice(0, 8)}`;
    const handler = `dist_ext_${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "dist-handoff",
      [
        step("gate", "human_review", {}, {
          wait_for_input: { prompt: "go?", choices: [{ label: "go", value: "go" }] },
        }),
        step("remote", handler, {}),
      ],
      { tenantId },
    );
    const created = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");
    const execution = await client.createContinuityExecution({
      tenant_id: tenantId,
      instance_id: instance.id,
      runtime_id: uuid(),
    });
    // The handoff is authorized while the instance is parked at the gate
    // (no unresolved effects yet)...
    const destination = uuid();
    await client.registerRuntime({
      tenant_id: tenantId,
      capabilities: { ...caps(destination, "mobile", handler), offline_capable: true },
    });
    const preview = await client.previewHandoff(execution.continuity_id, {
      tenant_id: tenantId,
      destination_runtime_id: destination,
      requirements: { handlers: [handler] },
    });
    const handoff = await client.createHandoff({
      tenant_id: tenantId,
      continuity_id: execution.continuity_id,
      destination_runtime_id: destination,
      requirements: { handlers: [handler] },
      placement_decision_id: preview.placement_decision.id,
      preview_sha256: preview.preview_sha256,
    });

    // ...then the workflow moves on and a remote node claims the next step.
    await client.sendSignal(
      instance.id,
      { custom: "human_input:gate" } as unknown as string,
      { value: "go" },
    );
    const task = await waitFor(async () => {
      const tasks = await client.pollWorkerTasks(handler, "server-worker");
      return tasks[0];
    });
    assert.ok(task.effect_id, "the claimed task carries its effect id");
    assert.equal(task.continuity_epoch, 0);
    await client.waitForState(instance.id, "waiting");
    await client.saveCheckpoint(instance.id, { safe_boundary: "remote", context_snapshot: {} });

    // Exporting now would let the node's completion land on a moved
    // execution: refused while the task is in flight.
    await assert.rejects(
      client.exportHandoff(handoff.id, {
        tenant_id: tenantId,
        requirements: { handlers: [handler] },
        expires_in_seconds: 300,
        payload_key_base64: randomBytes(32).toString("base64"),
      }),
      (error: unknown) =>
        error instanceof ApiError && error.status === 409 && /worker task/.test(error.body),
    );

    // The owner never changed, so the in-flight node still completes.
    await client.completeWorkerTask(task.id, "server-worker", { ok: true }, task.claim_epoch);
    await client.waitForState(instance.id, "completed");
  });

  it("scopes a browser-session token to its runtime, handlers, and the lease protocol", async () => {
    const tenantId = `dist-browser-${uuid().slice(0, 8)}`;
    const handler = `read_dom_${uuid().slice(0, 8)}`;
    const sequence = testSequence("dist-browser", [step("read", handler, {})], { tenantId });
    const created = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: { cart: 3 }, config: { stripe: "sk_live_never" }, audit: [] },
    });
    await client.waitForState(instance.id, "waiting");
    const session = await mintBrowserSession(tenantId, [handler]);
    assert.match(session.token, /^bst_/);
    const auth = { "x-api-key": session.token };

    for (const [method, path] of [
      ["GET", "/workers/tasks"],
      ["POST", "/workers/commands"],
      ["GET", `/instances/${instance.id}`],
      ["POST", "/runtimes/browser-sessions"],
    ] as const) {
      const res = await raw(method, path, method === "GET" ? undefined : {}, auth);
      assert.equal(res.status, 403, `${method} ${path}`);
    }
    for (const body of [
      { handler_name: "charge_card", worker_id: session.runtime_id },
      { handler_name: handler, worker_id: uuid() },
      { handler_name: handler, worker_id: session.runtime_id, capabilities: caps(session.runtime_id, "server", handler) },
    ]) {
      assert.equal((await raw("POST", "/workers/tasks/poll", body, auth)).status, 403);
    }

    const polled = await raw(
      "POST",
      "/workers/tasks/poll",
      { handler_name: handler, worker_id: session.runtime_id, capabilities: caps(session.runtime_id, "browser", handler) },
      auth,
    );
    assert.equal(polled.status, 200);
    const [task] = polled.json.tasks;
    assert.ok(task, "the browser claims its task");
    assert.equal(task.lease_secs, 30);
    assert.equal(task.context.config, undefined, "config never reaches the browser");

    const release = await raw(
      "POST",
      `/workers/tasks/${task.id}/release`,
      { worker_id: session.runtime_id, claim_epoch: task.claim_epoch, started: false },
      auth,
    );
    assert.equal(release.status, 204);
    const stale = await raw(
      "POST",
      `/workers/tasks/${task.id}/release`,
      { worker_id: session.runtime_id, claim_epoch: task.claim_epoch, started: false },
      auth,
    );
    assert.equal(stale.status, 409, "stale claims are 409, not 404");

    const again = await raw(
      "POST",
      "/workers/tasks/poll",
      { handler_name: handler, worker_id: session.runtime_id },
      { authorization: `Bearer ${session.token}` },
    );
    const [reclaimed] = again.json.tasks;
    assert.equal(reclaimed.id, task.id);
    const done = await raw(
      "POST",
      `/workers/tasks/${task.id}/complete`,
      { worker_id: session.runtime_id, claim_epoch: reclaimed.claim_epoch, output: { total: 42 } },
      auth,
    );
    assert.equal(done.status, 200);
    await client.waitForState(instance.id, "completed");
  });

  it("never hands credential-bearing work to a browser", async () => {
    const tenantId = `dist-secrets-${uuid().slice(0, 8)}`;
    const credentialId = `cred-${uuid().slice(0, 8)}`;
    await client.createCredential({
      id: credentialId,
      name: "stripe",
      kind: "api_key",
      value: "sk_live_browser_must_not_see",
      tenant_id: tenantId,
    });
    const handler = `sync_${uuid().slice(0, 8)}`;
    const sequence = testSequence(
      "dist-secrets",
      [step("sync", handler, { api_key: `credentials://${credentialId}` })],
      { tenantId },
    );
    const created = await client.createSequence(sequence);
    const instance = await client.createInstance({
      sequence_id: created.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(instance.id, "waiting");

    const session = await mintBrowserSession(tenantId, [handler]);
    const browserPoll = await raw(
      "POST",
      "/workers/tasks/poll",
      { handler_name: handler, worker_id: session.runtime_id },
      { "x-api-key": session.token },
    );
    assert.equal(browserPoll.status, 200);
    assert.equal(browserPoll.json.tasks.length, 0, "browser never claims it");

    const [serverTask] = await client.pollWorkerTasks(handler, "server-worker");
    assert.ok(serverTask, "a server worker claims it");
    assert.equal(serverTask.params.api_key, "sk_live_browser_must_not_see", "server workers keep credentials");

    // A browser-only placement that references credentials fails at dispatch.
    const placed = testSequence(
      "dist-secrets-placed",
      [step("form", `${handler}_b`, {
        api_key: `credentials://${credentialId}`,
        $runtime: { runtime_kinds: ["browser"] },
      })],
      { tenantId },
    );
    const placedSeq = await client.createSequence(placed);
    const failing = await client.createInstance({
      sequence_id: placedSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await client.waitForState(failing.id, "failed");
  });
});
