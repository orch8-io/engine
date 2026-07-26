# Governed durable memory

Orch8 supports private instance memory and tenant-shared knowledge without
making the workflow definition itself an authority boundary. Every new memory
record carries bounded retention and immutable provenance, while tenant-shared
namespaces require a policy installed by trusted host code.

## Install a tenant namespace policy

Applications install policy through the Rust host boundary, not a workflow
handler:

```rust
use orch8_engine::memory_governance::{
    install_namespace_policy, MemoryNamespacePolicy, MemoryOperation,
};

let policy = MemoryNamespacePolicy {
    policy_version: 1,
    allowed_sequence_ids: vec![support_sequence_id],
    operations: vec![
        MemoryOperation::Store,
        MemoryOperation::Search,
        MemoryOperation::Delete,
    ],
    residency: "br-south-1".into(),
    default_retention_secs: 30 * 24 * 60 * 60,
    max_retention_secs: 90 * 24 * 60 * 60,
};

install_namespace_policy(storage, &tenant_id, "support", &policy).await?;
```

Policies are tenant-isolated and bind a namespace to explicit sequence IDs,
operations, a residency label, and positive default/maximum retention. Policy
storage uses a reserved namespace which workflow handlers cannot access.

## Workflow handlers

- `memory_store` accepts `scope` (`instance` by default), `namespace`, optional
  `retention_secs`, and optional `residency`. Tenant scope requires an active
  policy and rejects retention above its maximum or a mismatched residency.
- `memory_search` returns only records whose tenant and residency provenance
  match the active boundary. It deletes expired records before returning and
  includes each result's provenance envelope.
- `memory_delete` is idempotent. Tenant deletion requires the policy's
  `delete` operation; dry runs never mutate storage.

New records include tenant, sequence, instance, block, policy version,
residency, creation and expiration times, and a SHA-256 commitment to their
text, embedding, and metadata.

## Guarantees

- A workflow cannot grant itself access to tenant-shared memory.
- Policy, namespace, key, retention, result count, and residency inputs are
  bounded and validated.
- Tenant-shared operations fail closed when policy is missing, malformed, or
  does not authorize the running sequence and requested operation.
- Instance memories remain tied to the persisted instance and caller tenant.
- Records written before governance remain readable only from their original
  instance KV. Legacy tenant-shared records remain hidden until rewritten
  through a governed store operation.

## Non-guarantees

- `residency` is an operator-asserted label. This layer validates and propagates
  it; the configured storage/placement layer must enforce physical location.
- Similarity search is a bounded in-process scan, not a vector database.
- Expiration is enforced on search. It is not a wall-clock deletion scheduler,
  so an expired record can remain at rest until that namespace is searched or
  an operator performs separate storage retention maintenance.
- Deleting a memory does not erase copies already exported to backups or
  external systems; those systems need their own retention process.
