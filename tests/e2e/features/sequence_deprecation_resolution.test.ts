/**
 * Sequence Deprecation and Resolution — verifies that trigger firing
 * resolves to the latest non-deprecated version, and that deprecated
 * versions are excluded from list_versions.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Deprecation and Resolution", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("list_versions excludes deprecated sequence", async () => {
    const name = `seq-dep-list-${uuid().slice(0, 8)}`;
    const v1 = { ...testSequence(name, [step("s1", "noop")]), name, version: 1 };
    await client.createSequence(v1);

    const v2 = { ...testSequence(name, [step("s1", "noop"), step("s2", "noop")]), name, version: 2 };
    await client.createSequence(v2);

    await client.deprecateSequence(v2.id);

    const versions = await client.listSequenceVersions("test", "default", name);
    assert.ok(versions.every((v: any) => v.id !== v2.id), "deprecated v2 should be absent");
    assert.ok(versions.some((v: any) => v.id === v1.id), "v1 should still be present");
  });

  it("trigger uses latest non-deprecated version", async () => {
    const name = `seq-dep-res-${uuid().slice(0, 8)}`;
    const v1 = { ...testSequence(name, [step("s1", "noop")]), name, version: 1 };
    await client.createSequence(v1);

    const v2 = { ...testSequence(name, [step("s1", "noop"), step("s2", "noop")]), name, version: 2 };
    await client.createSequence(v2);

    const slug = `trig-dep-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
    });

    const res = await client.fireTrigger(slug, {});
    const inst = await client.getInstance(res.instance_id as string);
    assert.equal((inst as any).sequence_id, v2.id, "should use latest non-deprecated v2");
  });

  it("trigger falls back to older version after deprecation", async () => {
    const name = `seq-dep-fb-${uuid().slice(0, 8)}`;
    const v1 = { ...testSequence(name, [step("s1", "noop")]), name, version: 1 };
    await client.createSequence(v1);

    const v2 = { ...testSequence(name, [step("s1", "noop"), step("s2", "noop")]), name, version: 2 };
    await client.createSequence(v2);

    const slug = `trig-fb-${uuid().slice(0, 8)}`;
    await client.createTrigger({
      slug,
      sequence_name: name,
      tenant_id: "test",
      namespace: "default",
      trigger_type: "webhook",
    });

    // Deprecate v2 — next fire should use v1.
    await client.deprecateSequence(v2.id);

    const res = await client.fireTrigger(slug, {});
    const inst = await client.getInstance(res.instance_id as string);
    assert.equal((inst as any).sequence_id, v1.id, "should fall back to v1");
  });

  it("get by id still returns deprecated sequence", async () => {
    const name = `seq-dep-get-${uuid().slice(0, 8)}`;
    const v1 = { ...testSequence(name, [step("s1", "noop")]), name, version: 1 };
    await client.createSequence(v1);

    await client.deprecateSequence(v1.id);

    const fetched = await client.getSequence(v1.id);
    assert.equal(fetched.id, v1.id);
    assert.equal((fetched as any).deprecated, true);
  });

  it("deprecating already deprecated sequence is idempotent", async () => {
    const name = `seq-dep-idem-${uuid().slice(0, 8)}`;
    const v1 = { ...testSequence(name, [step("s1", "noop")]), name, version: 1 };
    await client.createSequence(v1);

    await client.deprecateSequence(v1.id);
    // Should not throw.
    await client.deprecateSequence(v1.id);

    const fetched = await client.getSequence(v1.id);
    assert.equal((fetched as any).deprecated, true);
  });
});
