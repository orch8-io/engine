//! Mobile storage coverage: lightweight projections that keep idle sync
//! memory bounded — version-only manifests, eviction candidates, dedup
//! pruning, and telemetry wire shape.
//!
//! Count contract: 12 independently named unit tests.

use super::*;
use orch8_storage::StorageBackend;
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};

async fn setup() -> (MobileStorage, Arc<SqliteStorage>) {
    let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
    (MobileStorage::new(sqlite.clone()), sqlite)
}

fn sequence(name: &str, created_at: DateTime<Utc>) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: TenantId::new("mobile").unwrap(),
        namespace: Namespace::new("default"),
        name: name.to_string(),
        version: 1,
        deprecated: false,
        status: SequenceStatus::default(),
        blocks: vec![],
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at,
    }
}

async fn insert_instance(storage: &MobileStorage, id: &str, state: &str) {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO task_instances
         (id, sequence_id, tenant_id, namespace, state, context, created_at, updated_at)
         VALUES (?1, ?2, 'mobile', 'default', ?3, '{}', ?4, ?4)",
    )
    .bind(id)
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(state)
    .bind(now)
    .execute(storage.pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn coverage_storage_001_version_projection_empty_store_returns_empty() {
    let (storage, _sqlite) = setup().await;

    let versions = storage
        .list_local_sequence_versions(
            &TenantId::new("mobile").unwrap(),
            &Namespace::new("default"),
            1000,
        )
        .await
        .unwrap();

    assert!(versions.is_empty());
}

#[tokio::test]
async fn coverage_storage_002_version_projection_scopes_and_sorts() {
    let (storage, sqlite) = setup().await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    backend
        .create_sequence(&sequence("beta", Utc::now()))
        .await
        .unwrap();
    backend
        .create_sequence(&sequence("alpha", Utc::now()))
        .await
        .unwrap();

    let versions = storage
        .list_local_sequence_versions(
            &TenantId::new("mobile").unwrap(),
            &Namespace::new("default"),
            1000,
        )
        .await
        .unwrap();
    assert_eq!(
        versions,
        vec![("alpha".to_string(), 1), ("beta".to_string(), 1)]
    );

    let other_tenant = storage
        .list_local_sequence_versions(
            &TenantId::new("other").unwrap(),
            &Namespace::new("default"),
            1000,
        )
        .await
        .unwrap();
    assert!(other_tenant.is_empty());
}

#[tokio::test]
async fn coverage_storage_003_excess_candidates_empty_at_exact_retain_limit() {
    let (storage, sqlite) = setup().await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    for index in 0..3 {
        let created_at = Utc::now() - chrono::Duration::hours(index);
        backend
            .create_sequence(&sequence(&format!("seq-{index}"), created_at))
            .await
            .unwrap();
    }

    let candidates = storage
        .list_excess_oldest_local_sequences(
            &TenantId::new("mobile").unwrap(),
            &Namespace::new("default"),
            3,
        )
        .await
        .unwrap();

    assert!(candidates.is_empty());
}

#[tokio::test]
async fn coverage_storage_004_excess_candidates_select_oldest_first() {
    let (storage, sqlite) = setup().await;
    let backend: Arc<dyn StorageBackend> = sqlite;
    for (name, age_hours) in [("oldest", 72), ("middle", 48), ("newest", 1)] {
        let created_at = Utc::now() - chrono::Duration::hours(age_hours);
        backend
            .create_sequence(&sequence(name, created_at))
            .await
            .unwrap();
    }

    let candidates = storage
        .list_excess_oldest_local_sequences(
            &TenantId::new("mobile").unwrap(),
            &Namespace::new("default"),
            1,
        )
        .await
        .unwrap();

    let names: Vec<_> = candidates.iter().map(|(_, name)| name.as_str()).collect();
    assert_eq!(names, ["oldest", "middle"]);
}

#[tokio::test]
async fn coverage_storage_005_delete_oldest_beyond_count_clears_buffer() {
    let (storage, _sqlite) = setup().await;
    storage.append_telemetry_event("A", "1").await.unwrap();
    storage.append_telemetry_event("B", "2").await.unwrap();

    let deleted = storage.delete_oldest_telemetry_events(10).await.unwrap();

    assert_eq!(deleted, 2);
    assert_eq!(storage.count_telemetry_events().await.unwrap(), 0);
}

#[tokio::test]
async fn coverage_storage_006_dedup_prune_keeps_active_instance_rows() {
    let (storage, _sqlite) = setup().await;
    let id = InstanceId::new().to_string();
    insert_instance(&storage, &id, "scheduled").await;
    storage.set_dedup("dk-active", &id).await.unwrap();

    assert_eq!(storage.prune_stale_dedup().await.unwrap(), 0);
    assert_eq!(
        storage.get_dedup_instance("dk-active").await.unwrap(),
        Some(id)
    );
}

#[tokio::test]
async fn coverage_storage_007_dedup_prune_removes_terminal_instance_rows() {
    let (storage, _sqlite) = setup().await;
    let failed = InstanceId::new().to_string();
    let cancelled = InstanceId::new().to_string();
    insert_instance(&storage, &failed, "failed").await;
    insert_instance(&storage, &cancelled, "cancelled").await;
    storage.set_dedup("dk-failed", &failed).await.unwrap();
    storage.set_dedup("dk-cancelled", &cancelled).await.unwrap();

    assert_eq!(storage.prune_stale_dedup().await.unwrap(), 2);
    assert!(storage.list_all_dedup().await.unwrap().is_empty());
}

#[test]
fn coverage_storage_008_telemetry_event_hides_row_id_on_the_wire() {
    let event = TelemetryEvent {
        id: 99,
        event_type: "SyncCompleted".to_string(),
        payload: "{}".to_string(),
        created_at: "2026-07-25T12:00:00Z".to_string(),
    };

    let json = serde_json::to_value(&event).unwrap();

    assert!(json.get("id").is_none(), "row id must never leak");
    assert!(json.get("created_at").is_none());
    assert_eq!(json["timestamp"], "2026-07-25T12:00:00Z");
    assert_eq!(json["event_type"], "SyncCompleted");
}

#[test]
fn coverage_storage_009_projection_error_names_field_and_value() {
    let error = projection_error("state", "borked", "unknown variant");
    let message = error.to_string();

    assert!(message.contains("task_instances.state"), "{message}");
    assert!(message.contains("'borked'"), "{message}");
    assert!(message.contains("unknown variant"), "{message}");
}

#[test]
fn coverage_storage_010_invalid_instance_id_reports_query_error() {
    let error = parse_instance_id("not-a-uuid").unwrap_err();
    let message = error.to_string();

    assert!(message.contains("task_instances.id"), "{message}");
    assert!(message.contains("'not-a-uuid'"), "{message}");
}

#[tokio::test]
async fn coverage_storage_011_execution_steps_empty_id_list_skips_query() {
    let (storage, _sqlite) = setup().await;

    let steps = storage.list_sync_execution_steps(&[]).await.unwrap();

    assert!(steps.is_empty());
}

#[tokio::test]
async fn coverage_storage_012_terminal_projection_rejects_malformed_instance_id() {
    let (storage, _sqlite) = setup().await;
    insert_instance(&storage, "not-a-uuid", "completed").await;

    let result = storage.list_terminal_instance_states(100).await;

    let error = result.unwrap_err().to_string();
    assert!(error.contains("task_instances.id"), "{error}");
    assert!(error.contains("'not-a-uuid'"), "{error}");
}
