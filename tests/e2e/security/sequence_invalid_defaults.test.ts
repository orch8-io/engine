/**
 * Verifies sequence creation rejects steps whose defaults contain malformed JSON or wrong types.
 *
 * Note: orch8's sequence DSL is validated at deserialization time by serde. Required fields
 * (e.g. `ForEachDef.collection`, `StepDef.handler`) have no `#[serde(default)]`, and
 * `BlockDefinition` is tagged — unknown `type` values fail to deserialize. These failures
 * surface as HTTP 400/422 before any DB write (see orch8-api/src/sequences.rs::create_sequence).
 *
 * This test asserts rejection across three shapes: (a) syntactically malformed JSON body,
 * (b) a `for_each` block missing its required `collection` field, (c) an unknown block `type`.
 * Each case must return a 4xx and leave no sequence row readable by id.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, uuid, ApiError } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Validation — Invalid Defaults JSON", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - arrange: build sequence payload where step.defaults fails schema (wrong type or unterminated JSON)
  //   - act: POST /sequences with the invalid payload
  //   - assert: API returns 400 with field path pointing at step.defaults
  //   - assert: no sequence row written to DB (list/get returns empty)
  it("should reject sequence creation when step/block defaults are malformed", async () => {
    const tenantId = `invalid-def-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Case (a): syntactically malformed JSON — unterminated object.
    {
      const res = await fetch(`${client.baseUrl}/sequences`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: '{"id":"not-a-uuid","tenant_id":"t","namespace":"n","name":"x","version":1,"blocks":[',
      });
      assert.ok(
        res.status >= 400 && res.status < 500,
        `malformed JSON body must be rejected with 4xx, got ${res.status}`
      );
    }

    // Case (b): for_each block missing the required `collection` field.
    // BlockDefinition::ForEach has `pub collection: String` without #[serde(default)],
    // so serde's deserializer will refuse the body.
    const foreachId = uuid();
    const invalidForeach = {
      id: foreachId,
      tenant_id: tenantId,
      namespace,
      name: `bad-foreach-${uuid().slice(0, 8)}`,
      version: 1,
      blocks: [
        {
          type: "for_each",
          id: "fe-1",
          // collection is deliberately omitted
          body: [{ type: "step", id: "s1", handler: "noop" }],
        },
      ],
      created_at: new Date().toISOString(),
    };

    await assert.rejects(
      () => client.createSequence(invalidForeach),
      (err: unknown) => {
        assert.ok(err instanceof ApiError, "expected ApiError from invalid payload");
        assert.ok(
          err.status >= 400 && err.status < 500,
          `expected 4xx for missing required field, got ${err.status}`
        );
        return true;
      }
    );

    // Nothing should have been persisted for the invalid sequence id.
    await assert.rejects(
      () => client.getSequence(foreachId),
      (err: unknown) => {
        assert.ok(err instanceof ApiError, "expected ApiError");
        assert.equal(err.status, 404, "invalid sequence must not be persisted");
        return true;
      }
    );

    // Case (c): unknown block type — BlockDefinition uses #[serde(tag = "type")]
    // with a closed set of variants, so an unrecognized tag fails to deserialize.
    const unknownTypeId = uuid();
    const invalidUnknownType = {
      id: unknownTypeId,
      tenant_id: tenantId,
      namespace,
      name: `bad-type-${uuid().slice(0, 8)}`,
      version: 1,
      blocks: [
        {
          type: "this_block_type_does_not_exist",
          id: "x1",
          handler: "noop",
        },
      ],
      created_at: new Date().toISOString(),
    };

    await assert.rejects(
      () => client.createSequence(invalidUnknownType),
      (err: unknown) => {
        assert.ok(err instanceof ApiError, "expected ApiError");
        assert.ok(
          err.status >= 400 && err.status < 500,
          `expected 4xx for unknown block type, got ${err.status}`
        );
        return true;
      }
    );

    await assert.rejects(
      () => client.getSequence(unknownTypeId),
      (err: unknown) => {
        assert.ok(err instanceof ApiError, "expected ApiError");
        assert.equal(err.status, 404, "invalid sequence must not be persisted");
        return true;
      }
    );

    // Case (d): step with a non-string handler — defaults / params shape mismatch.
    // StepDef.handler is `String`; providing an object must be rejected.
    const badStepId = uuid();
    const invalidStepHandler = {
      id: badStepId,
      tenant_id: tenantId,
      namespace,
      name: `bad-step-${uuid().slice(0, 8)}`,
      version: 1,
      blocks: [
        {
          type: "step",
          id: "s1",
          handler: { not: "a-string" },
        },
      ],
      created_at: new Date().toISOString(),
    };

    await assert.rejects(
      () => client.createSequence(invalidStepHandler),
      (err: unknown) => {
        assert.ok(err instanceof ApiError, "expected ApiError");
        assert.ok(
          err.status >= 400 && err.status < 500,
          `expected 4xx for wrong-type handler, got ${err.status}`
        );
        return true;
      }
    );
  });
});
