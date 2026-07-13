/**
 * E2E: Block Output Schemas (Feature #09)
 *
 * Validates that `output_schema` on a step definition gates handler
 * output at runtime via the HTTP API:
 * - Creating a sequence with a valid output_schema succeeds
 * - Creating a sequence with an invalid (non-object) output_schema fails
 * - A step whose handler returns conforming output completes
 * - A step whose handler returns non-conforming output fails permanently
 */

import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Block Output Schemas", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // ------------------------------------------------------------------
  // Sequence creation with output_schema
  // ------------------------------------------------------------------

  it("creates a sequence with a valid output_schema on a step", async () => {
    const seq = testSequence("os-valid", [
      step("s1", "noop", {}, {
        output_schema: {
          type: "object",
          properties: { score: { type: "number" } },
          required: ["score"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id, "sequence should have an id");

    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.deepStrictEqual(s1.output_schema, {
      type: "object",
      properties: { score: { type: "number" } },
      required: ["score"],
    });
  });

  it("creates a sequence without output_schema (null/absent)", async () => {
    const seq = testSequence("os-none", [
      step("s1", "noop", {}),
    ]);
    const created = await client.createSequence(seq);
    assert.ok(created.id);

    const fetched = await client.getSequence(created.id);
    const s1 = fetched.blocks[0] as any;
    assert.ok(
      s1.output_schema === undefined || s1.output_schema === null,
      "output_schema should be absent or null",
    );
  });

  it("rejects a sequence with a non-object output_schema", async () => {
    const seq = testSequence("os-bad", [
      step("s1", "noop", {}, { output_schema: "not-an-object" }),
    ]);
    try {
      await client.createSequence(seq);
      assert.fail("should reject non-object output_schema");
    } catch (err: any) {
      assert.ok(
        err.status === 400 || err.status === 422,
        `expected 4xx, got ${err.status}`,
      );
    }
  });

  // ------------------------------------------------------------------
  // Runtime validation — conforming output succeeds
  // ------------------------------------------------------------------

  it("step with conforming output completes successfully", async () => {
    const seq = testSequence("os-conform", [
      step("s1", "transform", { result: "ok", count: 42 }, {
        output_schema: {
          type: "object",
          properties: {
            result: { type: "string" },
            count: { type: "number" },
          },
          required: ["result", "count"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(final_.state, "completed", "conforming output should complete");

    const outputs = await client.getOutputs(inst.id);
    const s1out = outputs.find((o: any) => o.block_id === "s1");
    assert.ok(s1out, "output for s1 should exist");
    assert.equal((s1out!.output as any).result, "ok");
    assert.equal((s1out!.output as any).count, 42);
  });

  // ------------------------------------------------------------------
  // Runtime validation — non-conforming output fails
  // ------------------------------------------------------------------

  it("step with non-conforming output fails the instance", async () => {
    const seq = testSequence("os-reject", [
      step("s1", "transform", { result: "ok", count: "not-a-number" }, {
        output_schema: {
          type: "object",
          properties: {
            result: { type: "string" },
            count: { type: "number" },
          },
          required: ["result", "count"],
        },
      }),
    ]);
    const created = await client.createSequence(seq);
    const inst = await client.createInstance({
      sequence_id: created.id,
      tenant_id: seq.tenant_id,
      namespace: seq.namespace,
    });

    const final_ = await client.waitForState(inst.id, ["completed", "failed"], {
      timeoutMs: 10_000,
    });
    assert.equal(
      final_.state,
      "failed",
      "non-conforming output should fail the instance",
    );
  });

  // ------------------------------------------------------------------
  // Preflight check via API
  // ------------------------------------------------------------------

  it("preflight passes for valid output_schema", async () => {
    const seq = testSequence("os-preflight-ok", [
      step("s1", "noop", {}, {
        output_schema: {
          type: "object",
          properties: { x: { type: "number" } },
        },
      }),
    ]);
    const created = await client.createSequence(seq);

    const res = await fetch(`${client.baseUrl}/sequences/${created.id}/preflight`);
    if (res.status === 404) {
      // preflight endpoint may not exist in minimal builds — skip
      return;
    }
    assert.ok(res.ok, `preflight should succeed: ${res.status}`);
    const report = await res.json() as any;
    const check = report.checks?.find(
      (c: any) => c.id === "output_schemas_valid",
    );
    if (check) {
      assert.equal(check.status, "pass", `expected pass: ${JSON.stringify(check)}`);
    }
  });
});
