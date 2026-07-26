/**
 * Governed memory — retention & scope governance (orch8-engine
 * src/handlers/memory.rs + src/memory_governance.rs), end-to-end.
 *
 * Scope: DEFAULT policies only. Tenant-scope governance requires a
 * MemoryNamespacePolicy installed in the reserved `__orch8_memory_policies_v1`
 * shared namespace, and only trusted control-plane Rust code
 * (`install_namespace_policy`) can write there — no REST API exposes it, and
 * workflow handlers reject `__orch8_` namespaces outright. So tenant-scope
 * happy paths (cross-instance reads, legacy fail-closed filtering) are pinned
 * by the Rust unit tests; here we verify the fail-closed default: without a
 * policy, tenant-scope store AND search are rejected permanently.
 *
 * Covered (instance scope, policy_version=1, residency "local"):
 *   - default retention (30d) stamped when `retention_secs` is omitted;
 *     explicit in-range values echoed back; max boundary (365d) accepted.
 *   - `retention_secs: 0` and above-max retention → permanent step error
 *     (instance fails).
 *   - expired records are excluded from search results, counted in
 *     `expired_deleted`, and purged.
 *   - instance-scope memories are NOT visible from a different instance of
 *     the same tenant (KV is per-instance).
 *   - tenant scope fails closed without a governance policy (store + search).
 *   - ranked results carry `provenance`; legacy records (no governance
 *     envelope, seeded via `set_state` with a `__mem__:` key) stay readable
 *     in instance scope with `provenance: null`.
 */
import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import { Orch8Client, testSequence, step, uuid } from "../client.ts";
import { startServer, stopServer } from "../harness.ts";
import type { ServerHandle } from "../harness.ts";
import type { Block, BlockOutput, SequenceDef } from "../client.ts";

const client = new Orch8Client();

// Mirror of orch8-engine/src/handlers/memory.rs constants.
const INSTANCE_DEFAULT_RETENTION_SECS = 30 * 24 * 60 * 60; // 30 days
const INSTANCE_MAX_RETENTION_SECS = 365 * 24 * 60 * 60; // 1 year

interface StoreOutput {
  key: string;
  stored: boolean;
  dimensions: number;
  scope: string;
  namespace: string;
  residency: string;
  retention_secs: number;
  policy_version: number;
}

interface SearchOutput {
  results: Array<{
    key: string;
    text: unknown;
    score: number;
    metadata: unknown;
    provenance: Record<string, unknown> | null;
  }>;
  count: number;
  scope: string;
  namespace: string;
  residency: string;
  policy_version: number;
  expired_deleted: number;
}

/** Create a sequence, run one instance of it to completion, return outputs. */
async function runToCompletion(
  seq: SequenceDef,
  tenantId: string,
): Promise<{ id: string; outputs: BlockOutput[] }> {
  await client.createSequence(seq);
  const { id } = await client.createInstance({
    sequence_id: seq.id,
    tenant_id: tenantId,
    namespace: "default",
  });
  await client.waitForState(id, "completed", { timeoutMs: 20_000 });
  return { id, outputs: await client.getOutputs(id) };
}

function output<T>(outputs: BlockOutput[], blockId: string): T {
  const entry = outputs.find((o) => o.block_id === blockId);
  assert.ok(entry, `expected output for block '${blockId}'`);
  return entry.output as T;
}

/** A `memory_store` step that never touches the network (precomputed vector). */
function storeStep(
  id: string,
  key: string,
  extra: Record<string, unknown> = {},
): Block {
  return step(id, "memory_store", {
    key,
    text: `memory for ${key}`,
    embedding: [1.0, 0.0, 0.0],
    ...extra,
  });
}

describe("Governed memory — retention & scope rules", () => {
  let server: ServerHandle | undefined;

  before(async () => {
    server = await startServer();
  });

  after(async () => {
    await stopServer(server);
  });

  it("stamps the default retention when retention_secs is omitted", async () => {
    const tenantId = `memgov-def-${uuid().slice(0, 8)}`;
    const namespace = `gov-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "memgov-default-retention",
      [
        storeStep("store_default", "def", { namespace }),
        storeStep("store_explicit", "exp", { namespace, retention_secs: 3600 }),
        storeStep("store_max", "max", {
          namespace,
          retention_secs: INSTANCE_MAX_RETENTION_SECS,
        }),
      ],
      { tenantId },
    );
    const { outputs } = await runToCompletion(seq, tenantId);

    const def = output<StoreOutput>(outputs, "store_default");
    assert.equal(def.stored, true);
    assert.equal(
      def.retention_secs,
      INSTANCE_DEFAULT_RETENTION_SECS,
      "omitted retention must fall back to the 30-day default",
    );
    assert.equal(def.scope, "instance");
    assert.equal(def.namespace, namespace);
    assert.equal(def.residency, "local");
    assert.equal(def.policy_version, 1);

    const explicit = output<StoreOutput>(outputs, "store_explicit");
    assert.equal(explicit.retention_secs, 3600, "in-range retention echoed back");

    const max = output<StoreOutput>(outputs, "store_max");
    assert.equal(
      max.retention_secs,
      INSTANCE_MAX_RETENTION_SECS,
      "the max boundary itself must be accepted",
    );
  });

  it("rejects retention_secs: 0", async () => {
    const tenantId = `memgov-zero-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "memgov-zero-retention",
      [storeStep("bad_store", "zero", { retention_secs: 0 })],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    // Rejection is a permanent step error ("retention_secs must be 1..=…"),
    // which fails the instance — no `stored: false` output is produced.
    const inst = await client.waitForState(id, "failed", { timeoutMs: 20_000 });
    assert.equal(inst.state, "failed", "retention_secs: 0 must be rejected");
  });

  it("rejects retention above the instance-scope max", async () => {
    const tenantId = `memgov-overmax-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "memgov-overmax-retention",
      [
        storeStep("bad_store", "over", {
          retention_secs: INSTANCE_MAX_RETENTION_SECS + 1,
        }),
      ],
      { tenantId },
    );
    await client.createSequence(seq);
    const { id } = await client.createInstance({
      sequence_id: seq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const inst = await client.waitForState(id, "failed", { timeoutMs: 20_000 });
    assert.equal(inst.state, "failed", "above-max retention must be rejected");
  });

  it("excludes expired records from search and counts them in expired_deleted", async () => {
    const tenantId = `memgov-exp-${uuid().slice(0, 8)}`;
    const namespace = `gov-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "memgov-expiry",
      [
        storeStep("store_fresh", "fresh", { namespace, retention_secs: 3600 }),
        storeStep("store_stale", "stale", { namespace, retention_secs: 1 }),
        // 2s >> 1s retention, comfortably tolerant of the 100ms server tick.
        step("wait_expiry", "sleep", { duration_ms: 2000 }),
        step("search", "memory_search", {
          namespace,
          query_embedding: [1.0, 0.0, 0.0],
          top_k: 10,
        }),
      ],
      { tenantId },
    );
    const { outputs } = await runToCompletion(seq, tenantId);

    const search = output<SearchOutput>(outputs, "search");
    assert.equal(search.scope, "instance");
    assert.equal(search.namespace, namespace);
    assert.equal(search.policy_version, 1);
    assert.equal(
      search.expired_deleted,
      1,
      "the expired record must be counted as purged",
    );
    assert.equal(search.count, 1, "only the fresh record survives");
    assert.equal(search.results[0]!.key, "fresh");
    assert.ok(!search.results.some((r) => r.key === "stale"));
  });

  it("instance-scope memories are not visible from another instance of the tenant", async () => {
    const tenantId = `memgov-iso-${uuid().slice(0, 8)}`;
    const namespace = `gov-${uuid().slice(0, 8)}`;

    // Instance A stores a memory and can read it back itself.
    const seqA = testSequence(
      "memgov-iso-a",
      [
        storeStep("store", "shared_key", { namespace }),
        step("search_self", "memory_search", {
          namespace,
          query_embedding: [1.0, 0.0, 0.0],
          top_k: 10,
        }),
      ],
      { tenantId },
    );
    const a = await runToCompletion(seqA, tenantId);
    const selfSearch = output<SearchOutput>(a.outputs, "search_self");
    assert.equal(selfSearch.count, 1, "writer instance sees its own memory");
    assert.equal(selfSearch.results[0]!.key, "shared_key");

    // Instance B (same tenant, same namespace param) sees nothing: the
    // instance-scope KV store is strictly per-instance.
    const seqB = testSequence(
      "memgov-iso-b",
      [
        step("search_other", "memory_search", {
          namespace,
          query_embedding: [1.0, 0.0, 0.0],
          top_k: 10,
        }),
      ],
      { tenantId },
    );
    const b = await runToCompletion(seqB, tenantId);
    assert.notEqual(b.id, a.id);
    const otherSearch = output<SearchOutput>(b.outputs, "search_other");
    assert.equal(
      otherSearch.count,
      0,
      "instance-scope records must not leak into another instance",
    );
    assert.deepEqual(otherSearch.results, []);
    assert.equal(otherSearch.expired_deleted, 0);
  });

  it("tenant scope fails closed when the namespace has no governance policy", async () => {
    const tenantId = `memgov-tenant-${uuid().slice(0, 8)}`;
    // A policy for this namespace was never installed (installing one is
    // control-plane only; unreachable from the API), so both operations must
    // be rejected with a permanent error.
    const namespace = `gov-tenant-${uuid().slice(0, 8)}`;

    const storeSeq = testSequence(
      "memgov-tenant-store",
      [storeStep("tenant_store", "k", { scope: "tenant", namespace })],
      { tenantId },
    );
    await client.createSequence(storeSeq);
    const storeInst = await client.createInstance({
      sequence_id: storeSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const storeResult = await client.waitForState(storeInst.id, "failed", {
      timeoutMs: 20_000,
    });
    assert.equal(
      storeResult.state,
      "failed",
      "tenant-scope store without a policy must fail closed",
    );

    const searchSeq = testSequence(
      "memgov-tenant-search",
      [
        step("tenant_search", "memory_search", {
          scope: "tenant",
          namespace,
          query_embedding: [1.0, 0.0, 0.0],
        }),
      ],
      { tenantId },
    );
    await client.createSequence(searchSeq);
    const searchInst = await client.createInstance({
      sequence_id: searchSeq.id,
      tenant_id: tenantId,
      namespace: "default",
    });
    const searchResult = await client.waitForState(searchInst.id, "failed", {
      timeoutMs: 20_000,
    });
    assert.equal(
      searchResult.state,
      "failed",
      "tenant-scope search without a policy must fail closed",
    );
  });

  it("ranked results carry provenance; legacy records report null", async () => {
    const tenantId = `memgov-prov-${uuid().slice(0, 8)}`;
    const namespace = `gov-${uuid().slice(0, 8)}`;
    const seq = testSequence(
      "memgov-provenance",
      [
        // A pre-governance record: plain {text, embedding, metadata} with no
        // governance envelope, written straight into the instance KV under
        // the `__mem__:` prefix the search handler scans. Instance scope
        // keeps legacy records readable for compatibility.
        step("seed_legacy", "set_state", {
          key: "__mem__:legacy_fact",
          value: {
            text: "legacy fact",
            embedding: [1.0, 0.0, 0.0],
            metadata: { origin: "pre-governance" },
          },
        }),
        step("store_governed", "memory_store", {
          key: "governed_fact",
          text: "governed fact",
          embedding: [0.0, 1.0, 0.0],
          namespace,
          retention_secs: 3600,
        }),
        step("search", "memory_search", {
          namespace,
          query_embedding: [1.0, 0.0, 0.0],
          top_k: 10,
        }),
      ],
      { tenantId },
    );
    const { id, outputs } = await runToCompletion(seq, tenantId);

    const search = output<SearchOutput>(outputs, "search");
    assert.equal(search.count, 2);

    const legacy = search.results.find((r) => r.key === "legacy_fact");
    assert.ok(legacy, "legacy record stays readable in instance scope");
    assert.equal(legacy.provenance, null, "legacy records have null provenance");
    assert.ok(legacy.score > 0.99, "exact vector match ranks the legacy record top");

    const governed = search.results.find((r) => r.key === "governed_fact");
    assert.ok(governed, "governed record present in results");
    const prov = governed.provenance;
    assert.ok(prov, "governed records carry a provenance envelope");
    assert.equal(prov.schema_version, 1);
    assert.equal(prov.tenant_id, tenantId);
    assert.equal(prov.instance_id, id, "provenance binds the writer instance");
    assert.equal(prov.block_id, "store_governed");
    assert.equal(prov.residency, "local");
    assert.equal(prov.policy_version, 1);
    const hash = prov.content_sha256;
    assert.equal(typeof hash, "string");
    assert.match(hash as string, /^[0-9a-f]{64}$/, "content hash is 64 hex chars");

    // expires_at - created_at == retention_secs (tolerant of tick rounding).
    const created = Date.parse(prov.created_at as string);
    const expires = Date.parse(prov.expires_at as string);
    assert.ok(Number.isFinite(created) && Number.isFinite(expires));
    const skewSecs = Math.abs((expires - created) / 1000 - 3600);
    assert.ok(skewSecs <= 2, `expiry delta should be ~3600s, off by ${skewSecs}s`);
  });
});
