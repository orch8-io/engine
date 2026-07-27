//! Coverage tests for governed durable-memory handler helpers.
//!
//! Pins retention bound enforcement, governance-envelope filtering
//! (tenant/residency/instance binding, expiry, legacy fail-closed rules),
//! provenance hashing, and provenance surfacing in ranked results.
//!
//! Count contract: 26 independently named unit tests.

use std::sync::Arc;

use chrono::{Duration, TimeZone};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, SequenceId, TenantId};

use super::*;

fn authorization() -> MemoryAuthorization {
    MemoryAuthorization {
        sequence_id: SequenceId::new(),
        tenant_id: "tenant-a".into(),
        instance_id: "instance-a".into(),
        residency: "br-south-1".into(),
        policy_version: 3,
        default_retention_secs: 60,
        max_retention_secs: 120,
    }
}

fn governed_record(auth: &MemoryAuthorization, expires_at: DateTime<Utc>) -> Value {
    json!({
        "text": "fact",
        "embedding": [1.0],
        "metadata": {},
        "governance": {
            "tenant_id": auth.tenant_id,
            "instance_id": auth.instance_id,
            "residency": auth.residency,
            "expires_at": expires_at.to_rfc3339(),
        }
    })
}

fn now() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 7, 25, 12, 0, 0).unwrap()
}

#[test]
fn coverage_governed_memory_001_missing_retention_uses_policy_default() {
    let auth = authorization();
    assert_eq!(retention_secs(&json!({}), &auth).unwrap(), 60);
}

#[test]
fn coverage_governed_memory_002_explicit_retention_inside_range_is_accepted() {
    let auth = authorization();
    assert_eq!(
        retention_secs(&json!({"retention_secs": 30}), &auth).unwrap(),
        30
    );
}

#[test]
fn coverage_governed_memory_003_zero_retention_is_rejected() {
    let auth = authorization();
    let error = retention_secs(&json!({"retention_secs": 0}), &auth).unwrap_err();
    assert!(error.to_string().contains("1..=120"), "{error}");
}

#[test]
fn coverage_governed_memory_004_max_retention_is_accepted() {
    let auth = authorization();
    assert_eq!(
        retention_secs(&json!({"retention_secs": 120}), &auth).unwrap(),
        120
    );
}

#[test]
fn coverage_governed_memory_005_above_max_retention_is_rejected() {
    let auth = authorization();
    let error = retention_secs(&json!({"retention_secs": 121}), &auth).unwrap_err();
    assert!(error.to_string().contains("1..=120"), "{error}");
}

#[test]
fn coverage_governed_memory_006_non_numeric_retention_falls_back_to_default() {
    let auth = authorization();
    assert_eq!(
        retention_secs(&json!({"retention_secs": "soon"}), &auth).unwrap(),
        60
    );
}

#[test]
fn coverage_governed_memory_007_active_record_is_kept() {
    let auth = authorization();
    let record = governed_record(&auth, now() + Duration::seconds(10));
    let (active, expired) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert_eq!(active.len(), 1);
    assert!(expired.is_empty());
}

#[test]
fn coverage_governed_memory_008_expired_record_moves_to_purge_list() {
    let auth = authorization();
    let record = governed_record(&auth, now() - Duration::seconds(1));
    let (active, expired) = governed_records(
        vec![("old".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
    assert_eq!(expired, vec!["old"]);
}

#[test]
fn coverage_governed_memory_009_expiry_at_exactly_now_is_expired() {
    let auth = authorization();
    let record = governed_record(&auth, now());
    let (_, expired) = governed_records(
        vec![("edge".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert_eq!(expired, vec!["edge"]);
}

#[test]
fn coverage_governed_memory_010_legacy_record_survives_in_instance_scope() {
    let auth = authorization();
    let legacy = json!({"text": "old", "embedding": [1.0], "metadata": {}});
    let (active, expired) = governed_records(
        vec![("legacy".into(), legacy)],
        &auth,
        MemoryScope::Instance,
        now(),
    );
    assert_eq!(active.len(), 1);
    assert!(expired.is_empty());
}

#[test]
fn coverage_governed_memory_011_legacy_record_fails_closed_in_tenant_scope() {
    let auth = authorization();
    let legacy = json!({"text": "old", "embedding": [1.0], "metadata": {}});
    let (active, expired) = governed_records(
        vec![("legacy".into(), legacy)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
    assert!(expired.is_empty());
}

#[test]
fn coverage_governed_memory_012_foreign_tenant_record_is_dropped() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]["tenant_id"] = json!("tenant-b");
    let (active, _) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
}

#[test]
fn coverage_governed_memory_013_wrong_residency_record_is_dropped() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]["residency"] = json!("eu-west-1");
    let (active, _) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
}

#[test]
fn coverage_governed_memory_014_missing_expiry_is_dropped() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]
        .as_object_mut()
        .unwrap()
        .remove("expires_at");
    let (active, expired) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
    assert!(expired.is_empty());
}

#[test]
fn coverage_governed_memory_015_malformed_expiry_is_dropped() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]["expires_at"] = json!("not-a-date");
    let (active, _) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert!(active.is_empty());
}

#[test]
fn coverage_governed_memory_016_instance_scope_rejects_other_instances() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]["instance_id"] = json!("instance-b");
    let (active, _) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Instance,
        now(),
    );
    assert!(active.is_empty());
}

#[test]
fn coverage_governed_memory_017_tenant_scope_allows_other_instances() {
    let auth = authorization();
    let mut record = governed_record(&auth, now() + Duration::seconds(10));
    record["governance"]["instance_id"] = json!("instance-b");
    let (active, _) = governed_records(
        vec![("k".into(), record)],
        &auth,
        MemoryScope::Tenant,
        now(),
    );
    assert_eq!(active.len(), 1);
}

#[test]
fn coverage_governed_memory_018_mixed_batch_partitions_by_expiry() {
    let auth = authorization();
    let records = vec![
        (
            "fresh".into(),
            governed_record(&auth, now() + Duration::seconds(10)),
        ),
        (
            "stale".into(),
            governed_record(&auth, now() - Duration::seconds(10)),
        ),
    ];
    let (active, expired) = governed_records(records, &auth, MemoryScope::Tenant, now());
    assert_eq!(active.len(), 1);
    assert_eq!(active[0].0, "fresh");
    assert_eq!(expired, vec!["stale"]);
}

async fn step_context() -> StepContext {
    let storage: Arc<dyn orch8_storage::StorageBackend> =
        Arc::new(SqliteStorage::in_memory().await.unwrap());
    StepContext {
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("tenant-a"),
        block_id: BlockId::new("mem"),
        params: json!({}),
        context: Arc::new(ExecutionContext::default()),
        attempt: 1,
        storage,
        wait_for_input: None,
    }
}

#[tokio::test]
async fn coverage_governed_memory_019_record_envelope_records_provenance() {
    let auth = authorization();
    let ctx = step_context().await;
    let record = memory_record(
        Some("the sky"),
        &[0.1, 0.2],
        &json!({"src": "test"}),
        &auth,
        60,
        &ctx,
    )
    .unwrap();
    let governance = &record["governance"];
    assert_eq!(governance["schema_version"], 1);
    assert_eq!(governance["tenant_id"], "tenant-a");
    assert_eq!(
        governance["sequence_id"],
        json!(auth.sequence_id.to_string())
    );
    assert_eq!(governance["instance_id"], "instance-a");
    assert_eq!(governance["block_id"], "mem");
    assert_eq!(governance["policy_version"], 3);
    assert_eq!(governance["residency"], "br-south-1");
    assert_eq!(record["text"], "the sky");
    assert_eq!(record["embedding"][1], 0.2);
}

#[tokio::test]
async fn coverage_governed_memory_020_expiry_equals_creation_plus_retention() {
    let auth = authorization();
    let ctx = step_context().await;
    let record = memory_record(None, &[1.0], &json!({}), &auth, 90, &ctx).unwrap();
    let created =
        DateTime::parse_from_rfc3339(record["governance"]["created_at"].as_str().unwrap()).unwrap();
    let expires =
        DateTime::parse_from_rfc3339(record["governance"]["expires_at"].as_str().unwrap()).unwrap();
    assert_eq!(expires - created, Duration::seconds(90));
}

#[tokio::test]
async fn coverage_governed_memory_021_content_hash_is_64_hex_chars() {
    let auth = authorization();
    let ctx = step_context().await;
    let record = memory_record(Some("fact"), &[1.0], &json!({}), &auth, 60, &ctx).unwrap();
    let hash = record["governance"]["content_sha256"].as_str().unwrap();
    assert_eq!(hash.len(), 64);
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
}

#[tokio::test]
async fn coverage_governed_memory_022_content_hash_depends_only_on_content() {
    let auth = authorization();
    let ctx = step_context().await;
    let first = memory_record(Some("fact"), &[1.0], &json!({"a": 1}), &auth, 60, &ctx).unwrap();
    let second = memory_record(Some("fact"), &[1.0], &json!({"a": 1}), &auth, 60, &ctx).unwrap();
    let different =
        memory_record(Some("other"), &[1.0], &json!({"a": 1}), &auth, 60, &ctx).unwrap();
    assert_eq!(
        first["governance"]["content_sha256"],
        second["governance"]["content_sha256"]
    );
    assert_ne!(
        first["governance"]["content_sha256"],
        different["governance"]["content_sha256"]
    );
}

#[tokio::test]
async fn coverage_governed_memory_023_unrepresentable_retention_is_rejected() {
    let auth = authorization();
    let ctx = step_context().await;
    let error = memory_record(Some("fact"), &[1.0], &json!({}), &auth, u64::MAX, &ctx).unwrap_err();
    assert!(error.to_string().contains("too large"), "{error}");
}

#[test]
fn coverage_governed_memory_024_ranked_results_expose_provenance() {
    let auth = authorization();
    let record = governed_record(&auth, now() + Duration::seconds(10));
    let results = rank_memories(&[1.0], vec![("k".into(), record)], 5);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["provenance"]["residency"], "br-south-1");
}

#[test]
fn coverage_governed_memory_025_legacy_results_have_null_provenance() {
    let legacy = json!({"text": "old", "embedding": [1.0], "metadata": {}});
    let results = rank_memories(&[1.0], vec![("k".into(), legacy)], 5);
    assert_eq!(results[0]["provenance"], Value::Null);
}

#[test]
fn coverage_governed_memory_026_ranked_results_keep_key_text_and_metadata() {
    let record = json!({
        "text": "fact",
        "embedding": [1.0, 0.0],
        "metadata": {"src": "unit"},
    });
    let results = rank_memories(&[1.0, 0.0], vec![("key-1".into(), record)], 5);
    assert_eq!(results[0]["key"], "key-1");
    assert_eq!(results[0]["text"], "fact");
    assert_eq!(results[0]["metadata"]["src"], "unit");
    assert!(results[0]["score"].as_f64().unwrap() > 0.99);
}
