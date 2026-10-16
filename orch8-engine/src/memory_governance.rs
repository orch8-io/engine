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
    use orch8_storage::ResourceStore;
    use orch8_storage::sqlite::SqliteStorage;
    use serde_json::json;

    const RESERVED: &str = "reserved memory namespaces";
    const NAMESPACE_SHAPE: &str = "memory namespace must be 1-128 characters";
    const RESIDENCY_SHAPE: &str = "memory residency must be 1-64";
    const VERSION: &str = "memory policy version must be positive";
    const SEQUENCES: &str = "memory policy must authorize 1-1024 sequences";
    const OPERATIONS: &str = "memory policy must authorize 1-3 operations";
    const UNIQUE: &str = "memory policy entries must be unique";
    const RETENTION: &str = "memory retention must be positive";

    fn valid_policy() -> MemoryNamespacePolicy {
        MemoryNamespacePolicy {
            policy_version: 1,
            allowed_sequence_ids: vec![SequenceId::new()],
            operations: vec![MemoryOperation::Store],
            residency: "br-south-1".into(),
            default_retention_secs: 60,
            max_retention_secs: 3_600,
        }
    }

    fn assert_outcome(case: &str, result: Result<(), String>, expected_error: Option<&str>) {
        match (result, expected_error) {
            (Ok(()), None) => {}
            (Err(message), Some(needle)) => assert!(
                message.contains(needle),
                "{case}: expected error containing {needle:?}, got {message:?}"
            ),
            (result, expected) => panic!("{case}: expected {expected:?}, got {result:?}"),
        }
    }

    #[test]
    fn target_namespace_must_be_short_safe_ascii_and_not_reserved() {
        let long_ok = "a".repeat(128);
        let too_long = "a".repeat(129);
        let cases: &[(&str, &str, Option<&str>)] = &[
            ("empty", "", Some(NAMESPACE_SHAPE)),
            ("single char", "a", None),
            ("every allowed punctuation", "A9-b_c.d/e", None),
            ("128 bytes", &long_ok, None),
            ("129 bytes", &too_long, Some(NAMESPACE_SHAPE)),
            ("space", "customer support", Some(NAMESPACE_SHAPE)),
            ("colon", "customer:support", Some(NAMESPACE_SHAPE)),
            ("backslash", "customer\\support", Some(NAMESPACE_SHAPE)),
            ("newline", "customer\nsupport", Some(NAMESPACE_SHAPE)),
            ("non-ascii letter", "memória", Some(NAMESPACE_SHAPE)),
            ("policy namespace", POLICY_NAMESPACE, Some(RESERVED)),
            ("reserved prefix", "__orch8_custom", Some(RESERVED)),
            ("reserved prefix alone", "__orch8_", Some(RESERVED)),
            ("near-miss prefix", "_orch8_custom", None),
            ("reserved text not at start", "team/__orch8_custom", None),
        ];
        for (case, namespace, expected) in cases {
            assert_outcome(case, validate_target_namespace(namespace), *expected);
        }
    }

    #[test]
    fn residency_label_must_be_short_safe_ascii() {
        let long_ok = "r".repeat(64);
        let too_long = "r".repeat(65);
        let cases: &[(&str, &str, Option<&str>)] = &[
            ("empty", "", Some(RESIDENCY_SHAPE)),
            ("single char", "1", None),
            (
                "mixed case and every allowed punctuation",
                "BR_2.south-prod",
                None,
            ),
            ("64 bytes", &long_ok, None),
            ("65 bytes", &too_long, Some(RESIDENCY_SHAPE)),
            (
                "slash is not allowed for residency",
                "br/south",
                Some(RESIDENCY_SHAPE),
            ),
            ("space", "br south", Some(RESIDENCY_SHAPE)),
            ("non-ascii letter", "são-paulo", Some(RESIDENCY_SHAPE)),
        ];
        for (case, residency, expected) in cases {
            assert_outcome(case, validate_residency(residency), *expected);
        }
    }

    #[test]
    #[allow(clippy::too_many_lines)] // one row per validation rule
    fn policy_validation_bounds_version_allowlists_and_retention() {
        type Mutate = fn(&mut MemoryNamespacePolicy);
        let cases: &[(&str, Mutate, Option<&str>)] = &[
            ("baseline", |_| {}, None),
            ("zero version", |p| p.policy_version = 0, Some(VERSION)),
            ("max version", |p| p.policy_version = u64::MAX, None),
            (
                "no sequences",
                |p| p.allowed_sequence_ids.clear(),
                Some(SEQUENCES),
            ),
            (
                "1024 sequences",
                |p| p.allowed_sequence_ids = (0..1024).map(|_| SequenceId::new()).collect(),
                None,
            ),
            (
                "1025 sequences",
                |p| p.allowed_sequence_ids = (0..1025).map(|_| SequenceId::new()).collect(),
                Some(SEQUENCES),
            ),
            (
                "duplicate sequence",
                |p| p.allowed_sequence_ids.push(p.allowed_sequence_ids[0]),
                Some(UNIQUE),
            ),
            ("no operations", |p| p.operations.clear(), Some(OPERATIONS)),
            (
                "all three operations",
                |p| {
                    p.operations = vec![
                        MemoryOperation::Store,
                        MemoryOperation::Search,
                        MemoryOperation::Delete,
                    ];
                },
                None,
            ),
            (
                "four operations",
                |p| {
                    p.operations = vec![
                        MemoryOperation::Store,
                        MemoryOperation::Search,
                        MemoryOperation::Delete,
                        MemoryOperation::Store,
                    ];
                },
                Some(OPERATIONS),
            ),
            (
                "duplicate operation",
                |p| p.operations = vec![MemoryOperation::Store, MemoryOperation::Store],
                Some(UNIQUE),
            ),
            (
                "invalid residency",
                |p| p.residency = "br/south".into(),
                Some(RESIDENCY_SHAPE),
            ),
            (
                "zero default retention",
                |p| p.default_retention_secs = 0,
                Some(RETENTION),
            ),
            (
                // `max == 0` is also implied by `default > 0 && default <= max`;
                // the row pins the observable rule, not which check fires.
                "zero max retention",
                |p| p.max_retention_secs = 0,
                Some(RETENTION),
            ),
            (
                "default equal to max",
                |p| p.default_retention_secs = p.max_retention_secs,
                None,
            ),
            (
                "default above max",
                |p| p.default_retention_secs = p.max_retention_secs + 1,
                Some(RETENTION),
            ),
            (
                "one second retention",
                |p| {
                    p.default_retention_secs = 1;
                    p.max_retention_secs = 1;
                },
                None,
            ),
            (
                "ten year cap",
                |p| {
                    p.default_retention_secs = MAX_RETENTION_SECS;
                    p.max_retention_secs = MAX_RETENTION_SECS;
                },
                None,
            ),
            (
                "max above ten years",
                |p| p.max_retention_secs = MAX_RETENTION_SECS + 1,
                Some(RETENTION),
            ),
        ];
        for (case, mutate, expected) in cases {
            let mut policy = valid_policy();
            mutate(&mut policy);
            assert_outcome(case, policy.validate(), *expected);
        }
    }

    #[test]
    fn policy_authorizes_only_listed_sequence_and_operation_pairs() {
        let policy = valid_policy();
        let listed = policy.allowed_sequence_ids[0];

        assert!(policy.authorizes(listed, MemoryOperation::Store));
        assert!(!policy.authorizes(SequenceId::new(), MemoryOperation::Store));
        assert!(!policy.authorizes(listed, MemoryOperation::Search));
        assert!(!policy.authorizes(listed, MemoryOperation::Delete));
    }

    #[test]
    fn policy_documents_with_unknown_fields_are_rejected() {
        let mut value = serde_json::to_value(valid_policy()).unwrap();
        value["unexpected"] = json!(true);

        assert!(serde_json::from_value::<MemoryNamespacePolicy>(value).is_err());
    }

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

    #[tokio::test]
    async fn install_rejects_reserved_namespace_and_invalid_policy_without_persisting() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("tenant-a");
        let mut invalid = valid_policy();
        invalid.policy_version = 0;
        let cases = [
            (
                "reserved namespace",
                POLICY_NAMESPACE,
                valid_policy(),
                RESERVED,
            ),
            ("invalid policy", "support", invalid, VERSION),
        ];
        for (case, namespace, policy, needle) in cases {
            let error = install_namespace_policy(&storage, &tenant, namespace, &policy)
                .await
                .unwrap_err();
            let StorageError::Unsupported(message) = error else {
                panic!("{case}: expected Unsupported, got {error:?}");
            };
            assert!(message.contains(needle), "{case}: {message}");
        }
        assert!(
            storage
                .get_shared_knowledge(tenant.as_str(), POLICY_NAMESPACE, "support")
                .await
                .unwrap()
                .is_none(),
            "a rejected install must not persist anything"
        );
    }

    #[tokio::test]
    async fn reinstall_replaces_the_previous_policy() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("tenant-a");
        let mut second = valid_policy();
        second.policy_version = 2;
        second.residency = "eu-west-1".into();
        install_namespace_policy(&storage, &tenant, "support", &valid_policy())
            .await
            .unwrap();

        install_namespace_policy(&storage, &tenant, "support", &second)
            .await
            .unwrap();

        let loaded = load_namespace_policy(&storage, &tenant, "support")
            .await
            .unwrap();
        assert_eq!(loaded, Some(second));
    }

    #[tokio::test]
    async fn load_revalidates_stored_policy_and_fails_closed_as_unsupported() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("tenant-a");
        let mut value = serde_json::to_value(valid_policy()).unwrap();
        value["policy_version"] = json!(0);
        storage
            .set_shared_knowledge(tenant.as_str(), POLICY_NAMESPACE, "support", &value)
            .await
            .unwrap();

        let error = load_namespace_policy(&storage, &tenant, "support")
            .await
            .unwrap_err();

        assert!(
            matches!(&error, StorageError::Unsupported(message) if message.contains(VERSION)),
            "{error:?}"
        );
    }

    #[tokio::test]
    async fn load_of_a_corrupted_policy_document_is_a_serialization_error() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("tenant-a");
        storage
            .set_shared_knowledge(
                tenant.as_str(),
                POLICY_NAMESPACE,
                "support",
                &json!({"bogus": true}),
            )
            .await
            .unwrap();

        let error = load_namespace_policy(&storage, &tenant, "support")
            .await
            .unwrap_err();

        assert!(matches!(error, StorageError::Serialization(_)), "{error:?}");
    }
}
