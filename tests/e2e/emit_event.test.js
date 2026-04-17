import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "./client.js";
import { startServer, stopServer } from "./harness.js";

const client = new Orch8Client();
const BASE = "http://localhost:18080";

/**
 * Create an event trigger via the API. Not part of the shared client yet.
 */
async function createTrigger(body) {
  const res = await fetch(`${BASE}/triggers`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    throw new Error(`createTrigger failed: HTTP ${res.status} ${await res.text()}`);
  }
  return res.json();
}

/**
 * Find a block's output object.
 */
function findOutput(outputs, blockId) {
  const match = outputs.find((o) => o.block_id === blockId);
  if (!match) {
    throw new Error(
      `block ${blockId} not found in outputs: ${JSON.stringify(outputs.map((o) => o.block_id))}`
    );
  }
  return match.output;
}

describe("emit_event / query_instance / dedupe E2E", () => {
  let server;

  before(async () => {
    server = await startServer({ build: false });
  });

  after(async () => {
    await stopServer(server);
  });

  it("producer emits event → child reaches terminal state with inherited context", async () => {
    const tenantId = `emit-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Consumer sequence: a single noop so it completes immediately.
    const consumer = testSequence("consumer", [step("c1", "noop")], { tenantId, namespace });
    await client.createSequence(consumer);

    // Event trigger pointing at the consumer sequence.
    const slug = `on-emit-${uuid().slice(0, 8)}`;
    await createTrigger({
      slug,
      sequence_name: consumer.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    // Producer sequence: emit_event only (no template reference — step
    // params do not currently support `{{outputs.*}}` resolution).
    const producer = testSequence(
      "producer",
      [
        step("emit", "emit_event", {
          trigger_slug: slug,
          data: { hello: "from-parent" },
        }),
      ],
      { tenantId, namespace }
    );
    await client.createSequence(producer);

    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace,
    });

    const finished = await client.waitForState(producerId, "completed", { timeoutMs: 20000 });
    assert.equal(finished.state, "completed");

    const producerOutputs = await client.getOutputs(producerId);
    const emitOut = findOutput(producerOutputs, "emit");
    assert.ok(emitOut.instance_id, "emit step output missing instance_id");
    assert.equal(emitOut.sequence_name, consumer.name);
    assert.equal(emitOut.deduped, false);
    const childId = emitOut.instance_id;

    // Child reaches terminal state.
    const child = await client.waitForState(childId, ["completed", "failed"], {
      timeoutMs: 20000,
    });
    assert.equal(child.state, "completed", `child should complete, got ${child.state}`);

    // Child's context carries the trigger `data` payload (equivalent to what
    // query_instance would return for `context`).
    assert.deepEqual(child.context.data, { hello: "from-parent" });

    // Child's metadata records the parent_instance_id set by emit_event.
    assert.equal(child.metadata._trigger, slug);
    assert.equal(child.metadata._trigger_event.source, "emit_event");
    assert.equal(child.metadata._trigger_event.parent_instance_id, producerId);
  });

  it("query_instance step returns target's context and state", async () => {
    const tenantId = `query-${uuid().slice(0, 8)}`;
    const namespace = "default";

    // Target sequence: sleeps so the target stays non-terminal long enough
    // for the producer to observe it mid-flight.
    const target = testSequence("target", [step("sleep", "sleep", { duration_ms: 3000 })], {
      tenantId,
      namespace,
    });
    await client.createSequence(target);

    const { id: targetId } = await client.createInstance({
      sequence_id: target.id,
      tenant_id: tenantId,
      namespace,
      context: { data: { payload: "target-context" } },
    });

    // Give the target a moment to start (state transitions scheduled→running).
    await new Promise((r) => setTimeout(r, 150));

    // Producer embeds the target's id as a literal string in step params
    // (there is no template resolution for step params).
    const producer = testSequence(
      "producer-query",
      [step("q", "query_instance", { instance_id: targetId })],
      { tenantId, namespace }
    );
    await client.createSequence(producer);

    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace,
    });

    await client.waitForState(producerId, "completed", { timeoutMs: 10000 });

    const outputs = await client.getOutputs(producerId);
    const queryOut = findOutput(outputs, "q");
    assert.equal(queryOut.found, true, "query_instance should find the target");
    assert.ok(queryOut.state, "query_instance output should include state");
    assert.deepEqual(queryOut.context.data, { payload: "target-context" });

    // query_instance on a non-existent id returns { found: false }.
    const ghostId = uuid();
    const ghostProducer = testSequence(
      "producer-ghost",
      [step("q", "query_instance", { instance_id: ghostId })],
      { tenantId, namespace }
    );
    await client.createSequence(ghostProducer);
    const { id: ghostProducerId } = await client.createInstance({
      sequence_id: ghostProducer.id,
      tenant_id: tenantId,
      namespace,
    });
    await client.waitForState(ghostProducerId, "completed", { timeoutMs: 10000 });
    const ghostOutputs = await client.getOutputs(ghostProducerId);
    const ghostOut = findOutput(ghostOutputs, "q");
    assert.equal(ghostOut.found, false);
  });

  it("two emit_event calls with same dedupe_key → only one child instance", async () => {
    const tenantId = `dedupe-${uuid().slice(0, 8)}`;
    const namespace = "default";

    const consumer = testSequence("consumer-dedupe", [step("c1", "noop")], {
      tenantId,
      namespace,
    });
    await client.createSequence(consumer);

    const slug = `on-dedupe-${uuid().slice(0, 8)}`;
    await createTrigger({
      slug,
      sequence_name: consumer.name,
      tenant_id: tenantId,
      namespace,
      trigger_type: "event",
    });

    // Dedupe scope is (parent_instance_id, dedupe_key), so a single producer
    // with two emit_event steps sharing a key is the only way to exercise
    // dedupe end-to-end. (Two separate producer runs would have distinct
    // parent_instance_id values and therefore never collide.) The design
    // doc's E2E description says "run producer twice with same dedupe_key",
    // which contradicts per-parent scoping — the test below follows the
    // implemented semantics.
    const dedupeKey = `order-${uuid().slice(0, 8)}`;
    const producer = testSequence(
      "producer-dedupe",
      [
        step("emit1", "emit_event", {
          trigger_slug: slug,
          data: { n: 1 },
          dedupe_key: dedupeKey,
        }),
        step("emit2", "emit_event", {
          trigger_slug: slug,
          data: { n: 2 },
          dedupe_key: dedupeKey,
        }),
      ],
      { tenantId, namespace }
    );
    await client.createSequence(producer);

    const { id: producerId } = await client.createInstance({
      sequence_id: producer.id,
      tenant_id: tenantId,
      namespace,
    });

    await client.waitForState(producerId, "completed", { timeoutMs: 20000 });

    const outputs = await client.getOutputs(producerId);
    const first = findOutput(outputs, "emit1");
    const second = findOutput(outputs, "emit2");

    assert.equal(first.deduped, false, "first emit should NOT be deduped");
    assert.equal(second.deduped, true, "second emit with same key should be deduped");
    assert.equal(first.instance_id, second.instance_id, "both emits should return the same child id");

    // Verify exactly one child instance exists for this tenant.
    await new Promise((r) => setTimeout(r, 200));
    const list = await client.listInstances({ tenant_id: tenantId });
    const children = list.filter((i) => i.id !== producerId);
    assert.equal(children.length, 1, `expected exactly one child, got ${children.length}`);
    assert.equal(children[0].id, first.instance_id);
  });
});
