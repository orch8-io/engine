/**
 * Shared helpers for the `changefeed_*` suites.
 *
 * The change feed is audit-backed, so tests need deterministic entry
 * producers: each `batch_action` call against a long-sleeping instance
 * appends exactly one feed entry (unknown custom signals are not otherwise
 * processed). Every suite drives its own tenant so feeds never overlap.
 */

import assert from "node:assert/strict";
import { Orch8Client, testSequence, step } from "../client.ts";

const client = new Orch8Client();

/**
 * Start a long-sleeping instance that feed entries can be produced against.
 * Returns the instance id once it is `running`.
 */
export async function startProducer(
  tenantId: string,
  name = "feed-producer",
): Promise<string> {
  const seq = testSequence(name, [step("s", "sleep", { duration_ms: 60_000 })], {
    tenantId,
  });
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "running", { timeoutMs: 10_000 });
  return id;
}

/** Append exactly one `batch_action` feed entry via a custom signal. */
export async function poke(tenantId: string, signalType: string): Promise<void> {
  const res = await client.batchAction({
    filter: { tenant_id: tenantId, states: ["running"] },
    action: "signal",
    signal_type: signalType,
  });
  assert.equal(res.applied, 1, `batch action ${signalType} must apply`);
}

/**
 * Start a producer and append exactly `count` feed entries.
 * Returns the producing instance id.
 */
export async function produceChanges(
  tenantId: string,
  count: number,
  name = "feed-producer",
): Promise<string> {
  const id = await startProducer(tenantId, name);
  for (let i = 0; i < count; i++) {
    await poke(tenantId, `poke_${i}`);
  }
  return id;
}

/** Craft a cursor the way the server does: base64url(JSON{created_at,id}). */
export function craftCursor(createdAt: string, id: string): string {
  return Buffer.from(JSON.stringify({ created_at: createdAt, id })).toString(
    "base64url",
  );
}
