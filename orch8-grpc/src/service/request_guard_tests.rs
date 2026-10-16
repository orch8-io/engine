//! Request guards shared by every RPC: storage and engine failures map to
//! stable gRPC codes (retryable outages are `Unavailable`, state conflicts are
//! `FailedPrecondition`/`Aborted`, everything else is an opaque `Internal`);
//! JSON envelopes are capped at 10 MiB; bulk mutations must name a tenant;
//! stream requests carry the caller's tenant; and a worker-task lookup by a
//! foreign tenant is indistinguishable from a missing task.

use super::*;

use orch8_engine::error::EngineError;
use orch8_types::error::StorageError;
use orch8_types::filter::InstanceFilter;

#[test]
fn storage_errors_map_to_stable_grpc_codes() {
    let cases = [
        (
            "not found",
            StorageError::NotFound {
                entity: "instance",
                id: "abc".into(),
            },
            tonic::Code::NotFound,
        ),
        (
            "conflict",
            StorageError::Conflict("duplicate key".into()),
            tonic::Code::AlreadyExists,
        ),
        (
            "terminal target",
            StorageError::TerminalTarget {
                entity: "instance".into(),
                id: "abc".into(),
            },
            tonic::Code::FailedPrecondition,
        ),
        (
            "constraint",
            StorageError::Constraint("check violated".into()),
            tonic::Code::FailedPrecondition,
        ),
        (
            "connection",
            StorageError::Connection("refused".into()),
            tonic::Code::Unavailable,
        ),
        (
            "pool exhausted",
            StorageError::PoolExhausted,
            tonic::Code::Unavailable,
        ),
        (
            "encryption",
            StorageError::Encryption("bad key".into()),
            tonic::Code::Internal,
        ),
        (
            "unsupported",
            StorageError::Unsupported("no artifact backend".into()),
            tonic::Code::Internal,
        ),
    ];
    for (case, error, code) in cases {
        assert_eq!(storage_err(error).code(), code, "case: {case}");
    }
}

#[test]
fn not_found_message_names_entity_and_id() {
    let status = storage_err(StorageError::NotFound {
        entity: "sequence",
        id: "42".into(),
    });
    assert_eq!(status.message(), "sequence 42");
}

#[test]
fn encryption_failure_does_not_leak_the_underlying_cause() {
    let status = storage_err(StorageError::Encryption("key id kms-123 rejected".into()));
    assert_eq!(status.message(), "storage encryption failed");
}

#[test]
fn engine_storage_errors_keep_the_storage_mapping() {
    let status = engine_err(EngineError::Storage(StorageError::PoolExhausted));
    assert_eq!(status.code(), tonic::Code::Unavailable);
}

#[test]
fn engine_effect_conflicts_are_aborted() {
    let status = engine_err(EngineError::EffectTransition(
        "receipt is not dispatched".into(),
    ));
    assert_eq!(status.code(), tonic::Code::Aborted);
    assert!(status.message().contains("receipt is not dispatched"));
}

#[test]
fn bulk_mutation_without_tenant_is_invalid_argument() {
    let status = require_bulk_tenant(&InstanceFilter::default()).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn bulk_mutation_scoped_to_a_tenant_is_allowed() {
    let filter = InstanceFilter {
        tenant_id: Some(TenantId::unchecked("acme")),
        ..InstanceFilter::default()
    };
    assert!(require_bulk_tenant(&filter).is_ok());
}

#[test]
fn malformed_json_payload_is_invalid_argument() {
    let status = from_json_str::<serde_json::Value>("{oops").unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn json_payload_of_exactly_ten_mib_is_accepted() {
    let document = format!(r#"{{"d":"{}"}}"#, "x".repeat(10 * 1024 * 1024 - 8));
    assert_eq!(document.len(), 10 * 1024 * 1024);
    assert!(from_json_str::<serde_json::Value>(&document).is_ok());
}

#[test]
fn json_payload_one_byte_over_ten_mib_is_invalid_argument() {
    let document = format!(r#"{{"d":"{}"}}"#, "x".repeat(10 * 1024 * 1024 - 7));
    assert_eq!(document.len(), 10 * 1024 * 1024 + 1);
    let status = from_json_str::<serde_json::Value>(&document).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn stream_request_carries_the_caller_tenant() {
    let request = stream_request((), Some(&TenantId::unchecked("acme")));
    assert_eq!(
        caller_tenant(&request).map(orch8_types::ids::TenantId::as_str),
        Some("acme")
    );
}

#[test]
fn stream_request_without_tenant_stays_unscoped() {
    let request = stream_request((), None);
    assert!(caller_tenant(&request).is_none());
}

// --- fixtures ---

fn claimed_task() -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(Uuid::now_v7()),
        block_id: orch8_types::ids::BlockId::new("step_1"),
        handler_name: "payments".into(),
        queue_name: Some("critical".into()),
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({"amount": 42}),
        context: serde_json::json!({"data": {}}),
        attempt: 3,
        timeout_ms: Some(5_000),
        state: WorkerTaskState::Claimed,
        worker_id: Some("worker-a".into()),
        claimed_at: Some(chrono::Utc::now()),
        heartbeat_at: Some(chrono::Utc::now()),
        claim_epoch: 3,
        resume_checkpoint: Some(serde_json::json!({"cursor": 10})),
        checkpoint_seq: 7,
        completed_at: Some(chrono::Utc::now()),
        output: Some(serde_json::json!({"receipt": "r1"})),
        error_message: Some("boom".into()),
        error_retryable: Some(true),
        created_at: chrono::Utc::now() - chrono::Duration::hours(1),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
    }
}

// --- get_worker_task_checked ---

async fn storage() -> Arc<dyn StorageBackend> {
    Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    )
}

const SEQ: &str = "00000000-0000-0000-0000-000000000301";
const INST: &str = "00000000-0000-0000-0000-000000000302";

fn sequence_with_retry(seq_id: &str, max_attempts: u32) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": seq_id,
        "tenant_id": "test",
        "namespace": "default",
        "name": format!("seq_{seq_id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{
            "type": "step",
            "id": "step_1",
            "handler": "noop",
            "params": {},
            "retry": {
                "max_attempts": max_attempts,
                "initial_backoff": 100,
                "max_backoff": 1000
            }
        }],
        "created_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap()
}

fn plain_instance(inst_id: &str, seq_id: &str) -> TaskInstance {
    serde_json::from_value(serde_json::json!({
        "id": inst_id,
        "sequence_id": seq_id,
        "tenant_id": "test",
        "namespace": "default",
        "state": "scheduled",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": {"data": {}, "config": {}, "audit": [], "runtime": {}},
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap()
}

async fn seed_retry_topology(storage: &Arc<dyn StorageBackend>, max_attempts: u32) -> WorkerTask {
    storage
        .create_sequence(&sequence_with_retry(SEQ, max_attempts))
        .await
        .unwrap();
    storage
        .create_instance(&plain_instance(INST, SEQ))
        .await
        .unwrap();
    let mut task = claimed_task();
    task.instance_id = InstanceId::from_uuid(Uuid::parse_str(INST).unwrap());
    task
}

#[tokio::test]
async fn worker_task_lookup_hides_foreign_tenant_tasks_as_not_found() {
    let storage = storage().await;
    let task = seed_retry_topology(&storage, 3).await;
    storage.create_worker_task(&task).await.unwrap();

    let (stored, instance) = get_worker_task_checked(&storage, None, task.id)
        .await
        .unwrap();
    assert_eq!(stored.id, task.id);
    assert_eq!(instance.tenant_id.as_str(), "test");

    let status = get_worker_task_checked(&storage, Some(TenantId::unchecked("foreign")), task.id)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn worker_task_lookup_of_unknown_task_is_not_found() {
    let storage = storage().await;
    let status = get_worker_task_checked(&storage, None, Uuid::now_v7())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}
