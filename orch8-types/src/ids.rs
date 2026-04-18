use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Newtype wrappers prevent mixing up UUIDs at compile time.
/// Zero cost at runtime (transparent newtypes).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(transparent)]
pub struct InstanceId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(transparent)]
pub struct SequenceId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(transparent)]
pub struct ExecutionNodeId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct BlockId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct TenantId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct Namespace(pub String);

/// Resource key for rate limiting, e.g. "mailbox:john@acme.com"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct ResourceKey(pub String);

// UUIDv7 = 48-bit unix_ms timestamp || 74 bits of randomness. For
// instance/sequence/node IDs this gives two real wins over v4:
//
//   1. Temporal order within index. B-tree inserts on a v7 primary key are
//      sequential (newest rows always go at the tail), so Postgres avoids
//      splitting random inner pages on every insert. v4 forces random
//      inserts across the whole index, which fragments pages and pollutes
//      the page cache.
//
//   2. Natural FIFO by id for the concurrency gate. The concurrency
//      position query used to order by `id` alone — with v4 that's a
//      lexicographic race between random UUIDs, so a newer instance could
//      sort before an already-running older one and claim its slot. With
//      v7 the prefix IS the creation time, so `id`-ordering collapses
//      back onto FIFO for everything created in a different millisecond.
//      Same-ms collisions still fall back on the 74 random bits, so the
//      scheduler additionally uses `(created_at, id)` as the composite
//      comparator to break those ties deterministically.
impl InstanceId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl SequenceId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl ExecutionNodeId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for InstanceId {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SequenceId {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for ExecutionNodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for SequenceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for ExecutionNodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::fmt::Display for ResourceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newtype_ids_are_distinct_types() {
        let instance_id = InstanceId::new();
        let sequence_id = SequenceId::new();
        // These are different types — mixing them is a compile error.
        assert_ne!(instance_id.0, sequence_id.0);
    }
}
