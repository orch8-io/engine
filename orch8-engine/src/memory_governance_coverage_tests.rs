//! Coverage tests for durable agent memory governance.
//!
//! Pins authorization checks, policy (de)serialization strictness, and the
//! install/load round-trip semantics of namespace policies.
//!
//! Count contract: 12 independently named unit tests.

use orch8_storage::ResourceStore;
use orch8_storage::sqlite::SqliteStorage;
use serde_json::json;

use super::*;

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

#[test]
fn coverage_memory_governance_001_authorizes_listed_sequence_and_operation() {
    let policy = valid_policy();
    let sequence = policy.allowed_sequence_ids[0];
    assert!(policy.authorizes(sequence, MemoryOperation::Store));
}

#[test]
fn coverage_memory_governance_002_denies_unlisted_sequence() {
    let policy = valid_policy();
    assert!(!policy.authorizes(SequenceId::new(), MemoryOperation::Store));
}

#[test]
fn coverage_memory_governance_003_denies_unlisted_operation() {
    let policy = valid_policy();
    let sequence = policy.allowed_sequence_ids[0];
    assert!(!policy.authorizes(sequence, MemoryOperation::Delete));
    assert!(!policy.authorizes(sequence, MemoryOperation::Search));
}

#[test]
fn coverage_memory_governance_004_operations_serialize_snake_case() {
    assert_eq!(
        serde_json::to_value(MemoryOperation::Store).unwrap(),
        json!("store")
    );
    assert_eq!(
        serde_json::to_value(MemoryOperation::Search).unwrap(),
        json!("search")
    );
    assert_eq!(
        serde_json::to_value(MemoryOperation::Delete).unwrap(),
        json!("delete")
    );
}

#[test]
fn coverage_memory_governance_005_policy_round_trips_through_serde() {
    let policy = valid_policy();
    let value = serde_json::to_value(&policy).unwrap();
    let restored: MemoryNamespacePolicy = serde_json::from_value(value).unwrap();
    assert_eq!(restored, policy);
}

#[test]
fn coverage_memory_governance_006_unknown_fields_are_rejected() {
    let mut value = serde_json::to_value(valid_policy()).unwrap();
    value["unexpected"] = json!(true);
    assert!(serde_json::from_value::<MemoryNamespacePolicy>(value).is_err());
}

#[tokio::test]
async fn coverage_memory_governance_007_install_rejects_reserved_namespace() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = TenantId::unchecked("tenant-a");
    let error = install_namespace_policy(&storage, &tenant, POLICY_NAMESPACE, &valid_policy())
        .await
        .unwrap_err();
    let StorageError::Unsupported(message) = error else {
        panic!("expected Unsupported, got {error:?}");
    };
    assert!(message.contains("reserved memory namespaces"), "{message}");
}

#[tokio::test]
async fn coverage_memory_governance_008_install_rejects_invalid_policy() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = TenantId::unchecked("tenant-a");
    let mut policy = valid_policy();
    policy.policy_version = 0;
    let error = install_namespace_policy(&storage, &tenant, "support", &policy)
        .await
        .unwrap_err();
    let StorageError::Unsupported(message) = error else {
        panic!("expected Unsupported, got {error:?}");
    };
    assert!(
        message.contains("memory policy version must be positive"),
        "{message}"
    );
}

#[tokio::test]
async fn coverage_memory_governance_009_load_returns_none_for_unset_namespace() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = TenantId::unchecked("tenant-a");
    assert!(
        load_namespace_policy(&storage, &tenant, "missing")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coverage_memory_governance_010_reinstall_overwrites_previous_policy() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = TenantId::unchecked("tenant-a");
    let first = valid_policy();
    let mut second = valid_policy();
    second.policy_version = 2;
    second.residency = "eu-west-1".into();
    install_namespace_policy(&storage, &tenant, "support", &first)
        .await
        .unwrap();
    install_namespace_policy(&storage, &tenant, "support", &second)
        .await
        .unwrap();
    let loaded = load_namespace_policy(&storage, &tenant, "support")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(loaded, second);
    assert_ne!(loaded, first);
}

#[tokio::test]
async fn coverage_memory_governance_011_stored_policy_failing_validation_is_rejected() {
    let storage = SqliteStorage::in_memory().await.unwrap();
    let tenant = TenantId::unchecked("tenant-a");
    // Write a structurally valid but semantically invalid policy directly,
    // bypassing `install_namespace_policy`'s validation.
    let mut value = serde_json::to_value(valid_policy()).unwrap();
    value["policy_version"] = json!(0);
    storage
        .set_shared_knowledge(tenant.as_str(), POLICY_NAMESPACE, "support", &value)
        .await
        .unwrap();
    assert!(
        load_namespace_policy(&storage, &tenant, "support")
            .await
            .is_err()
    );
}

#[tokio::test]
async fn coverage_memory_governance_012_corrupted_policy_value_is_rejected() {
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
    assert!(
        load_namespace_policy(&storage, &tenant, "support")
            .await
            .is_err()
    );
}
