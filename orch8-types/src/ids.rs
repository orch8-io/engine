use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Newtype wrappers prevent mixing up UUIDs at compile time.
/// Zero cost at runtime (transparent newtypes).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(transparent)]
pub struct InstanceId(Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, sqlx::Type, ToSchema)]
#[sqlx(transparent)]
pub struct SequenceId(Uuid);

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    sqlx::Type,
    ToSchema,
)]
#[sqlx(transparent)]
pub struct ExecutionNodeId(Uuid);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema)]
pub struct BlockId(String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, ToSchema)]
pub struct TenantId(String);

struct TenantIdVisitor;

impl serde::de::Visitor<'_> for TenantIdVisitor {
    type Value = TenantId;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a non-empty tenant_id string")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        TenantId::new(value).map_err(serde::de::Error::custom)
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        TenantId::new(value).map_err(serde::de::Error::custom)
    }
}

impl<'de> serde::Deserialize<'de> for TenantId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TenantIdVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct Namespace(String);

/// Resource key for rate limiting, e.g. "mailbox:john@acme.com"
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct ResourceKey(String);

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

    /// Construct from an existing UUID (e.g. from the database).
    #[must_use]
    pub fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    /// Borrow the inner UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consume and return the inner UUID.
    #[must_use]
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl SequenceId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Construct from an existing UUID (e.g. from the database).
    #[must_use]
    pub fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    /// Borrow the inner UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consume and return the inner UUID.
    #[must_use]
    pub fn into_uuid(self) -> Uuid {
        self.0
    }
}

impl ExecutionNodeId {
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    /// Construct from an existing UUID (e.g. from the database).
    #[must_use]
    pub fn from_uuid(u: Uuid) -> Self {
        Self(u)
    }

    /// Borrow the inner UUID.
    #[must_use]
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }

    /// Consume and return the inner UUID.
    #[must_use]
    pub fn into_uuid(self) -> Uuid {
        self.0
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

impl BlockId {
    /// Create a new `BlockId`.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl TenantId {
    /// Create a new validated `TenantId`.
    /// Rejects empty strings so empty tenant IDs cannot be constructed.
    pub fn new(s: impl Into<String>) -> Result<Self, String> {
        let s = s.into();
        if s.trim().is_empty() {
            Err("tenant_id cannot be empty".to_string())
        } else {
            Ok(Self(s))
        }
    }

    /// Create a `TenantId` without validation.
    /// Use in tests or when the value is known-valid (e.g. deserialized from a
    /// trusted source). Prefer [`TenantId::new`] for untrusted input.
    #[must_use]
    #[doc(hidden)]
    pub fn unchecked(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for TenantId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl Namespace {
    /// Create a new `Namespace`.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
    }
}

impl std::fmt::Display for Namespace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl ResourceKey {
    /// Create a new `ResourceKey`.
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the inner string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume and return the inner string.
    #[must_use]
    pub fn into_string(self) -> String {
        self.0
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
        assert_ne!(instance_id.as_uuid(), sequence_id.as_uuid());
    }

    #[test]
    fn instance_id_default_uses_v7() {
        let id = InstanceId::default();
        assert_eq!(id.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn sequence_id_default_uses_v7() {
        let id = SequenceId::default();
        assert_eq!(id.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn execution_node_id_default_uses_v7() {
        let id = ExecutionNodeId::default();
        assert_eq!(id.as_uuid().get_version_num(), 7);
    }

    #[test]
    fn tenant_id_rejects_empty() {
        assert!(TenantId::new("").is_err());
        assert!(TenantId::new("   ").is_err());
    }

    #[test]
    fn tenant_id_accepts_non_empty() {
        assert_eq!(TenantId::new("acme").unwrap().as_str(), "acme");
    }

    #[test]
    fn tenant_id_deserialize_rejects_empty() {
        let result: Result<TenantId, _> = serde_json::from_str("\"\"");
        assert!(result.is_err());
        let result: Result<TenantId, _> = serde_json::from_str("\"   \"");
        assert!(result.is_err());
    }

    #[test]
    fn tenant_id_deserialize_accepts_non_empty() {
        let tid: TenantId = serde_json::from_str("\"acme\"").unwrap();
        assert_eq!(tid.as_str(), "acme");
    }

    #[test]
    fn block_id_display() {
        let id = BlockId::new("step_1");
        assert_eq!(id.to_string(), "step_1");
    }

    #[test]
    fn namespace_display() {
        let ns = Namespace::new("default");
        assert_eq!(ns.to_string(), "default");
    }

    #[test]
    fn resource_key_display() {
        let rk = ResourceKey::new("api:prod");
        assert_eq!(rk.to_string(), "api:prod");
    }
}
