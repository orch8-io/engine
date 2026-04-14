//! Scope selector for `emit_event` dedupe keys.
//!
//! The `emit_event` handler supports two dedupe scopes:
//!
//! - [`DedupeScope::Parent`] (default) — keys are isolated per parent
//!   workflow instance. A retry of the same parent with the same key is
//!   deduped; a different parent using the same key gets its own child. This
//!   is idempotent retry within a single workflow run.
//! - [`DedupeScope::Tenant`] — keys are isolated per tenant. Any caller in
//!   the same tenant using the same key observes the same child instance.
//!   This enables tenant-wide "at-most-once" fan-out (e.g. one
//!   `send-welcome-email` per user across every workflow run in a tenant)
//!   without the caller maintaining external state.
//!
//! The scope is part of the dedupe table's primary key, so the two scopes
//! are independent namespaces — a `Parent` row and a `Tenant` row can share
//! a dedupe key without colliding.
//!
//! Cross-tenant dedupe is intentionally not representable: when a caller
//! passes `dedupe_scope: "tenant"` the handler derives the tenant from
//! `StepContext::tenant_id`, never from a caller-supplied value.
use crate::ids::{InstanceId, TenantId};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DedupeScope {
    /// Per-parent scope: `(parent_instance_id, dedupe_key)` is the dedupe
    /// identity. Default; preserves pre-R7 behavior.
    Parent(InstanceId),
    /// Per-tenant scope: `(tenant_id, dedupe_key)` is the dedupe identity.
    /// All callers in the same tenant share the namespace.
    Tenant(TenantId),
}

impl DedupeScope {
    /// Short tag stored in the dedupe table's `scope_kind` column. Kept
    /// lowercase to match the on-the-wire spelling of the `emit_event`
    /// `dedupe_scope` param.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            DedupeScope::Parent(_) => "parent",
            DedupeScope::Tenant(_) => "tenant",
        }
    }

    /// String encoding of the scope's identifier stored in the dedupe
    /// table's `scope_value` column. `Parent` uses the canonical hyphenated
    /// UUID; `Tenant` uses the tenant id verbatim (already a `String`).
    #[must_use]
    pub fn value(&self) -> String {
        match self {
            DedupeScope::Parent(id) => id.0.to_string(),
            DedupeScope::Tenant(t) => t.0.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parent_scope_kind_and_value() {
        let id = InstanceId::new();
        let s = DedupeScope::Parent(id);
        assert_eq!(s.kind(), "parent");
        assert_eq!(s.value(), id.0.to_string());
    }

    #[test]
    fn tenant_scope_kind_and_value() {
        let s = DedupeScope::Tenant(TenantId("acme".into()));
        assert_eq!(s.kind(), "tenant");
        assert_eq!(s.value(), "acme");
    }

    #[test]
    fn parent_and_tenant_are_distinct_even_with_matching_value_strings() {
        // Edge case: a tenant id that happens to collide with a uuid string
        // should still be a different scope because `kind` differs, which is
        // part of the dedupe PK on the storage side.
        let id = InstanceId::new();
        let p = DedupeScope::Parent(id);
        let t = DedupeScope::Tenant(TenantId(id.0.to_string()));
        assert_ne!(p.kind(), t.kind());
        assert_eq!(p.value(), t.value());
    }
}
