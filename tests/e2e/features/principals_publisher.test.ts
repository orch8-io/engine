/**
 * Capability-scoped principals — the `publisher` capability.
 *
 * Publisher keys drive CI/CD release automation: `/sequences/**`,
 * `/releases/**`, and `/plugins/**` — and nothing else. The suite walks a
 * full publish flow (sequence → release → canary evaluation surface, plugin
 * registration) using only a publisher key, then checks the runtime,
 * worker, approval, and admin families all answer 403.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import {
  Orch8Client,
  ApiError,
  testSequence,
  step,
  uuid,
} from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const ROOT_KEY = `root-principals-pb-${uuid().slice(0, 8)}`;

describe("Principals — publisher capability", () => {
  let server: ServerHandle | undefined;
  let root: Orch8Client;
  let tenantId: string;
  let publisher: Orch8Client;
  let publisherSecret: string;

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
    tenantId = `pb-${uuid().slice(0, 8)}`;
    const key = await root.createApiKey({
      tenant_id: tenantId,
      name: "publisher",
      capabilities: ["publisher"],
    });
    publisherSecret = key.secret;
    publisher = new Orch8Client(`http://localhost:${server.port}`, {
      "X-API-Key": publisherSecret,
    });
  });

  after(async () => {
    await stopServer(server);
  });

  it("runs a full sequence lifecycle: create → read → list → delete", async () => {
    const seq = testSequence("pb-life", [step("s", "noop")], { tenantId });
    await publisher.createSequence(seq);

    const fetched = await publisher.getSequence(seq.id);
    assert.equal(fetched.name, seq.name);

    const listed = await publisher.listSequences({ tenant_id: tenantId });
    assert.ok(listed.some((s) => s.id === seq.id));

    await publisher.deleteSequence(seq.id);
    await assert.rejects(publisher.getSequence(seq.id), {
      status: 404,
    } as object);
  });

  it("creates a release between two of its own sequences and reads it back", async () => {
    const baseline = testSequence("pb-rel", [step("s", "noop")], {
      tenantId,
    });
    // A release routes between two VERSIONS of one sequence: same
    // tenant/namespace/name, bumped version, fresh id.
    const candidate = {
      ...testSequence("pb-rel", [step("s", "noop")], { tenantId }),
      id: uuid(),
      name: baseline.name,
      namespace: baseline.namespace,
      version: 2,
    };
    await publisher.createSequence(baseline);
    await publisher.createSequence(candidate);

    const create = await fetch(`http://localhost:${server!.port}/releases`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-API-Key": publisherSecret,
      },
      body: JSON.stringify({
        tenant_id: tenantId,
        baseline_sequence_id: baseline.id,
        candidate_sequence_id: candidate.id,
      }),
    });
    const createText = await create.text();
    assert.equal(
      create.status,
      201,
      `publisher key must create releases, got ${create.status}: ${createText}`,
    );
    const release = JSON.parse(createText) as { id: string };
    assert.ok(release.id, "release id returned");

    const list = await fetch(`http://localhost:${server!.port}/releases`, {
      headers: { "X-API-Key": publisherSecret },
    });
    assert.equal(list.status, 200, "publisher key lists releases");
    const releases = (await list.json()) as unknown;
    assert.ok(
      JSON.stringify(releases).includes(baseline.id),
      "created release visible in the list",
    );
  });

  it("registers and lists plugins", async () => {
    const name = `plugin-${uuid().slice(0, 8)}`;
    await publisher.createPlugin({
      name,
      plugin_type: "wasm",
      source: "https://example.com/plugin.wasm",
      tenant_id: tenantId,
    });
    const plugins = await publisher.listPlugins({ tenant_id: tenantId });
    assert.ok(
      plugins.some((p: any) => p.name === name),
      "plugin registered by the publisher key is listed",
    );
  });

  it("denies instance creation and reads with 403", async () => {
    await assert.rejects(
      publisher.createInstance({
        sequence_id: uuid(),
        tenant_id: tenantId,
        namespace: "default",
      }),
      { status: 403 } as object,
    );
    await assert.rejects(publisher.listInstances({ tenant_id: tenantId }), {
      status: 403,
    } as object);
    await assert.rejects(publisher.getInstance(uuid()), {
      status: 403,
    } as object);
  });

  it("denies the worker family with 403", async () => {
    await assert.rejects(publisher.pollWorkerTasks("h", "w-1"), {
      status: 403,
    } as object);
    const res = await fetch(`http://localhost:${server!.port}/handlers`, {
      headers: { "X-API-Key": publisherSecret },
    });
    assert.equal(res.status, 403, "publisher has no /handlers access");
  });

  it("denies approvals and signals with 403", async () => {
    await assert.rejects(publisher.listApprovals({ tenant_id: tenantId }), {
      status: 403,
    } as object);
    await assert.rejects(publisher.sendSignal(uuid(), "cancel"), {
      status: 403,
    } as object);
  });

  it("denies instance state mutation with 403", async () => {
    await assert.rejects(publisher.updateState(uuid(), "cancelled"), {
      status: 403,
    } as object);
    await assert.rejects(publisher.updateContext(uuid(), { data: {} }), {
      status: 403,
    } as object);
  });

  it("denies key management with 403", async () => {
    await assert.rejects(publisher.listApiKeys(tenantId), {
      status: 403,
    } as object);
  });
});
