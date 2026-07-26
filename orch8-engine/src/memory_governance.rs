//! Authoritative policies for tenant-shared durable memory namespaces.

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use orch8_types::ids::{SequenceId, TenantId};
use serde::{Deserialize, Serialize};

/// Reserved tenant-private namespace. Workflow handlers reject this namespace;
/// only trusted control-plane code should call `install_namespace_policy`.
pub const POLICY_NAMESPACE: &str = "__orch8_memory_policies_v1";
const MAX_RETENTION_SECS: u64 = 10 * 365 * 24 * 60 * 60;
const MAX_ALLOWED_SEQUENCES: usize = 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryOperation {
    Store,
    Search,
    Delete,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryNamespacePolicy {
    pub policy_version: u64,
    pub allowed_sequence_ids: Vec<SequenceId>,
    pub operations: Vec<MemoryOperation>,
    /// Operator-asserted data location label, for example `br-south-1`.
    pub residency: String,
    pub default_retention_secs: u64,
    pub max_retention_secs: u64,
}

impl MemoryNamespacePolicy {
    pub fn validate(&self) -> Result<(), String> {
        if self.policy_version == 0 {
            return Err("memory policy version must be positive".into());
        }
        if self.allowed_sequence_ids.is_empty()
            || self.allowed_sequence_ids.len() > MAX_ALLOWED_SEQUENCES
        {
            return Err("memory policy must authorize 1-1024 sequences".into());
        }
        if self.operations.is_empty() || self.operations.len() > 3 {
            return Err("memory policy must authorize 1-3 operations".into());
        }
        let unique_sequences = self
            .allowed_sequence_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        let unique_operations = self
            .operations
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>();
        if unique_sequences.len() != self.allowed_sequence_ids.len()
            || unique_operations.len() != self.operations.len()
        {
            return Err("memory policy entries must be unique".into());
        }
        validate_residency(&self.residency)?;
        if self.default_retention_secs == 0
            || self.max_retention_secs == 0
            || self.default_retention_secs > self.max_retention_secs
            || self.max_retention_secs > MAX_RETENTION_SECS
        {
            return Err(
                "memory retention must be positive, default <= max, and max <= 10 years".into(),
            );
        }
        Ok(())
    }

    #[must_use]
    pub fn authorizes(&self, sequence_id: SequenceId, operation: MemoryOperation) -> bool {
        self.allowed_sequence_ids.contains(&sequence_id) && self.operations.contains(&operation)
    }
}

/// Install or replace a policy from trusted control-plane code.
pub async fn install_namespace_policy(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    namespace: &str,
    policy: &MemoryNamespacePolicy,
) -> Result<(), StorageError> {
    validate_target_namespace(namespace).map_err(StorageError::Unsupported)?;
    policy.validate().map_err(StorageError::Unsupported)?;
    let value = serde_json::to_value(policy)?;
    storage
        .set_shared_knowledge(tenant_id.as_str(), POLICY_NAMESPACE, namespace, &value)
        .await
}

pub(crate) async fn load_namespace_policy(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    namespace: &str,
) -> Result<Option<MemoryNamespacePolicy>, StorageError> {
    let value = storage
        .get_shared_knowledge(tenant_id.as_str(), POLICY_NAMESPACE, namespace)
        .await?;
    value
        .map(|value| {
            let policy: MemoryNamespacePolicy = serde_json::from_value(value)?;
            policy.validate().map_err(StorageError::Unsupported)?;
            Ok(policy)
        })
        .transpose()
}

pub(crate) fn validate_target_namespace(namespace: &str) -> Result<(), String> {
    if namespace == POLICY_NAMESPACE || namespace.starts_with("__orch8_") {
        return Err("reserved memory namespaces cannot be accessed by workflows".into());
    }
    if namespace.is_empty()
        || namespace.len() > 128
        || !namespace
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/'))
    {
        return Err(
            "memory namespace must be 1-128 characters using letters, digits, '-', '_', '.', or '/'"
                .into(),
        );
    }
    Ok(())
}

pub(crate) fn validate_residency(residency: &str) -> Result<(), String> {
    if residency.is_empty()
        || residency.len() > 64
        || !residency
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("memory residency must be 1-64 ASCII letters, digits, '-', '_' or '.'".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_storage::sqlite::SqliteStorage;

    #[tokio::test]
    async fn policy_round_trip_is_tenant_isolated() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("tenant-a");
        let policy = MemoryNamespacePolicy {
            policy_version: 2,
            allowed_sequence_ids: vec![SequenceId::new()],
            operations: vec![MemoryOperation::Store, MemoryOperation::Search],
            residency: "br-south-1".into(),
            default_retention_secs: 3_600,
            max_retention_secs: 86_400,
        };
        install_namespace_policy(&storage, &tenant, "support", &policy)
            .await
            .unwrap();
        assert_eq!(
            load_namespace_policy(&storage, &tenant, "support")
                .await
                .unwrap(),
            Some(policy)
        );
        assert!(
            load_namespace_policy(&storage, &TenantId::unchecked("tenant-b"), "support")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn rejects_unbounded_or_reserved_policies() {
        assert!(validate_target_namespace(POLICY_NAMESPACE).is_err());
        let policy = MemoryNamespacePolicy {
            policy_version: 0,
            allowed_sequence_ids: Vec::new(),
            operations: Vec::new(),
            residency: String::new(),
            default_retention_secs: 0,
            max_retention_secs: u64::MAX,
        };
        assert!(policy.validate().is_err());
    }
}

#[cfg(test)]
#[path = "memory_governance_boundary_tests.rs"]
mod boundary_tests;
