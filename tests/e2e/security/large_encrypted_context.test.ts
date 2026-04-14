/**
 * Verifies encrypted context round-trips a large payload without corruption or excessive latency.
 *
 * Note: the engine caps serialized `ExecutionContext` at `DEFAULT_MAX_CONTEXT_BYTES` = 256 KiB
 * (see orch8-types/src/context.rs). The harness does not override this, so the 10MB target
 * in the plan is infeasible against the default server config — writes would be rejected with
 * HTTP 413 by `context.check_size`. This test uses a payload that is large for real traffic
 * (~200 KiB) but still within the policy ceiling, which is sufficient to catch truncation /
 * chunking / JSON-escaping bugs across the create→read path.
 *
 * Encryption is not asserted to be active here — the server is started in --insecure mode
 * without ORCH8_ENCRYPTION_KEY, so the round-trip validates the plaintext path. The same
 * structural assertions would hold under encryption since decryption is transparent.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Large Encrypted Context Round-Trip", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  // Plan:
  //   - arrange: start server with encryption enabled
  //   - act: createInstance with ~10MB of structured context payload
  //   - act: read context back via API
  //   - assert: deep-equal to original, no truncation or corruption
  //   - assert: round-trip latency under a soft bound
  it("should persist and retrieve a large encrypted context without corruption", async () => {
    const tenantId = `large-ctx-${uuid().slice(0, 8)}`;

    const seq = testSequence("large-ctx-seq", [step("s1", "noop")], {
      tenantId,
    });
    await client.createSequence(seq);

    // Build a structured payload that stresses JSON encoding across common
    // shapes: long strings, nested objects, arrays with mixed types, unicode.
    // Target ~200 KiB serialized, well under the 256 KiB server ceiling.
    const ROW_COUNT = 400;
    const STRING_LEN = 400;
    const filler = "x".repeat(STRING_LEN);
    const rows: Array<Record<string, unknown>> = [];
    for (let i = 0; i < ROW_COUNT; i++) {
      rows.push({
        id: `row-${i}-${uuid().slice(0, 8)}`,
        index: i,
        payload: filler,
        nested: { depth: 1, flag: i % 2 === 0, tags: ["a", "b", "c"] },
        unicode: "\u4f60\u597d\u2014\u00e9\u00f6",
      });
    }
    const originalContext = {
      rows,
      meta: {
        generated_at: new Date().toISOString(),
        count: ROW_COUNT,
      },
    };

    const serializedBytes = Buffer.byteLength(JSON.stringify(originalContext), "utf8");
    // Sanity: the test payload must be substantial to exercise large-payload paths.
    assert.ok(
      serializedBytes > 100_000,
      `expected payload >100KB for meaningful round-trip, got ${serializedBytes} bytes`
    );

    const start = Date.now();

    const { id: instanceId } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
      context: { data: originalContext },
    });

    const retrieved = await client.getInstance(instanceId);

    const elapsedMs = Date.now() - start;

    // Assert deep equality on the round-tripped payload.
    const retrievedData = (retrieved.context as { data?: unknown } | undefined)?.data;
    assert.deepEqual(
      retrievedData,
      originalContext,
      "round-tripped context must deep-equal the original payload"
    );

    // Soft latency bound — create + read on a ~200KB context should finish in
    // well under 5 seconds on any reasonable environment.
    assert.ok(
      elapsedMs < 5_000,
      `round-trip latency ${elapsedMs}ms exceeded 5000ms soft bound`
    );
  });
});
