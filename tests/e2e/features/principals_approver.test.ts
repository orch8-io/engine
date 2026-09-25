/**
 * Capability-scoped principals — the `approver` capability.
 *
 * Approver keys serve human-in-the-loop actors: they may list the approval
 * inbox (`/approvals`) and answer waiting instances via
 * `POST /instances/{id}/signals` — nothing else. The headline test runs a
 * full wait_for_input approval flow resolved end-to-end by an approver key.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-ap-${uuid().slice(0, 8)}`;

describe("Principals — approver capability", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantId: string;
  let approver: Orch8Client;

  before(async () => {
    server = await startServer({
      env: {
        ORCH8_API_KEY: ROOT_KEY,
        ORCH8_ALLOW_NO_TENANT_ISOLATION: "1",
      },
    });
    root = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": ROOT_KEY,
    });
    tenantId = `ap-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "approver",
      capabilities: ["approver"],
    });
    approver = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": key.secret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  /** Plant a waiting wait_for_input instance; returns its id and block id. */
  async function plantWaitingInstance(): Promise<string> {
    const seq = testSequence(
      "ap-flow",
      [
        step("review", "noop", {}, {
          wait_for_input: {
            prompt: "Approve?",
            choices: [
              { label: "Approve", value: "approve" },
              { label: "Reject", value: "reject" },
            ],
            store_as: "decision",
          },
        }),
      ],
      { tenantId },
    );
    await root.createSequence(seq);
    const { id } = await root.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    await root.waitForState(id, "waiting", { timeoutMs: 10_000 });
    return id;
  }

  it("lists the approval inbox (empty for a fresh tenant)", async () => {
    const inbox = await approver.listApprovals({ tenant_id: tenantId });
    assert.ok(Array.isArray((inbox as any).items), "inbox shape");
  });

  it("resolves a waiting approval end-to-end with only the approver key", async () => {
    const id = await plantWaitingInstance();

    // 1. The approver sees the pending item in its inbox.
    const inbox = await approver.listApprovals({ tenant_id: tenantId });
    const item = (inbox as any).items.find(
      (a: any) => a.instance_id === id,
    );
    assert.ok(item, "waiting instance appears in the approver inbox");
    assert.equal(item.block_id, "review");
    assert.equal(item.choices.length, 2);

    // 2. The approver answers via the instance signal route with a custom
    // signal (externally-tagged enum → object form on the wire).
    await approver.sendCustomSignal(id, "human_input:review", {
      value: "approve",
    });

    // 3. The instance completes and the decision is recorded.
    const done = await root.waitForState(id, "completed", {
      timeoutMs: 10_000,
    });
    const data = (done.context as any)?.data ?? {};
    assert.equal(data.decision, "approve");

    // 4. The inbox no longer carries the resolved item.
    const afterInbox = await approver.listApprovals({
      tenant_id: tenantId,
    });
    assert.ok(
      !(afterInbox as any).items.some((a: any) => a.instance_id === id),
      "resolved approval leaves the inbox",
    );
  });

  it("passes the capability gate on signals for a bogus instance (404, not 403)", async () => {
    await assert.rejects(approver.sendSignal(uuid(), "cancel"), {
      status: 404,
    } as object);
  });

  it("denies reading the very instance it may approve (GET is outside the family)", async () => {
    const id = await plantWaitingInstance();
    await assert.rejects(approver.getInstance(id), {
      status: 403,
    } as object);
  });

  it("denies sequence writes and instance creation with 403", async () => {
    await assert.rejects(
      approver.createSequence(
        testSequence("ap-deny", [step("s", "noop")], { tenantId }),
      ),
      { status: 403 } as object,
    );
    await assert.rejects(
      approver.createInstance({
        sequence_id: uuid(),
        tenant_id: tenantId,
        namespace: "default",
      }),
      { status: 403 } as object,
    );
  });

  it("denies instance state mutation — only /signals is allowed on instances", async () => {
    const id = await plantWaitingInstance();
    await assert.rejects(approver.updateState(id, "cancelled"), {
      status: 403,
    } as object);
    await assert.rejects(approver.updateContext(id, { data: {} }), {
      status: 403,
    } as object);
  });

  it("denies worker, publisher, and admin families with 403", async () => {
    await assert.rejects(approver.pollWorkerTasks("h", "w-1"), {
      status: 403,
    } as object);
    await assert.rejects(approver.listPlugins({ tenant_id: tenantId }), {
      status: 403,
    } as object);
    await assert.rejects(approver.listApiKeys(tenantId), {
      status: 403,
    } as object);
  });
});
