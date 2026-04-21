/**
 * Sequence Interceptor CRUD Round-Trip — verifies that creating a sequence
 * with all 5 interceptor hooks and then reading it back preserves the
 * definitions exactly.
 *
 * Plan #23: create sequence with interceptors round-trips correctly.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";

const client = new Orch8Client();

describe("Sequence Interceptor Round-Trip", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("persists and returns all 5 interceptor hooks on GET", async () => {
    const tenantId = `int-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "interceptor-roundtrip",
      [step("s1", "noop")],
      { tenantId },
    );

    const interceptors = {
      before_step: {
        handler: "log",
        params: { message: "before-{{step.id}}" },
      },
      after_step: {
        handler: "log",
        params: { message: "after-{{step.id}}" },
      },
      on_signal: {
        handler: "log",
        params: { message: "signal-{{signal.type}}" },
      },
      on_complete: {
        handler: "log",
        params: { message: "complete-{{instance.id}}" },
      },
      on_failure: {
        handler: "log",
        params: { message: "failure-{{instance.id}}" },
      },
    };

    (seq as Record<string, unknown>).interceptors = interceptors;

    const created = await client.createSequence(seq);
    assert.ok(created.id, "created sequence should have an id");

    const fetched = await client.getSequence(created.id as string);
    assert.ok(
      fetched.interceptors,
      "fetched sequence should include interceptors",
    );

    const fetchedInt = fetched.interceptors as Record<string, unknown>;
    assert.deepEqual(
      (fetchedInt.before_step as Record<string, unknown>).handler,
      "log",
      "before_step handler should round-trip",
    );
    assert.deepEqual(
      (fetchedInt.after_step as Record<string, unknown>).handler,
      "log",
      "after_step handler should round-trip",
    );
    assert.deepEqual(
      (fetchedInt.on_signal as Record<string, unknown>).handler,
      "log",
      "on_signal handler should round-trip",
    );
    assert.deepEqual(
      (fetchedInt.on_complete as Record<string, unknown>).handler,
      "log",
      "on_complete handler should round-trip",
    );
    assert.deepEqual(
      (fetchedInt.on_failure as Record<string, unknown>).handler,
      "log",
      "on_failure handler should round-trip",
    );

    // Params should also round-trip.
    assert.deepEqual(
      ((fetchedInt.before_step as Record<string, unknown>).params as Record<string, unknown>).message,
      "before-{{step.id}}",
      "before_step params should round-trip",
    );
  });

  it("GET by name returns interceptors for the latest version", async () => {
    const tenantId = `int-name-${uuid().slice(0, 8)}`;

    const seq = testSequence(
      "interceptor-by-name",
      [step("s1", "noop")],
      { tenantId },
    );

    (seq as Record<string, unknown>).interceptors = {
      before_step: { handler: "noop", params: {} },
    };

    await client.createSequence(seq);

    const fetched = await client.getSequenceByName(
      tenantId,
      seq.namespace,
      seq.name,
    );
    assert.ok(
      fetched.interceptors,
      "get-by-name should include interceptors",
    );
    const fi = fetched.interceptors as Record<string, unknown>;
    assert.equal(
      (fi.before_step as Record<string, unknown>).handler,
      "noop",
    );
  });
});
