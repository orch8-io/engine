//! Coverage tests for the shared service helpers the new worker-session,
//! telemetry, and artifact features are built on: storage-error mapping,
//! JSON envelope bounds, and tenant-checked worker task lookup. (Retry
//! admission moved to `orch8_engine::worker_lease::fail_worker_task`, which
//! the gRPC and HTTP fail paths share.)

use super::*;

use orch8_types::error::StorageError;

// --- storage_err mapping ---

macro_rules! storage_err_case {
    ($name:ident, $error:expr, $code:expr) => {
        #[test]
        fn $name() {
            let status = storage_err($error);
            assert_eq!(status.code(), $code);
        }
    };
}

storage_err_case!(
    coverage_helpers_001_not_found_maps_to_not_found,
    StorageError::NotFound {
        entity: "instance",
        id: "abc".into()
    },
    tonic::Code::NotFound
);
storage_err_case!(
    coverage_helpers_002_conflict_maps_to_already_exists,
    StorageError::Conflict("duplicate key".into()),
    tonic::Code::AlreadyExists
);
storage_err_case!(
    coverage_helpers_003_terminal_target_maps_to_failed_precondition,
    StorageError::TerminalTarget {
        entity: "instance".into(),
        id: "abc".into()
    },
    tonic::Code::FailedPrecondition
);
storage_err_case!(
    coverage_helpers_004_connection_maps_to_unavailable,
    StorageError::Connection("refused".into()),
    tonic::Code::Unavailable
);
storage_err_case!(
    coverage_helpers_005_pool_exhausted_maps_to_unavailable,
    StorageError::PoolExhausted,
    tonic::Code::Unavailable
);
storage_err_case!(
    coverage_helpers_006_backend_maps_to_unavailable,
    StorageError::Backend("object store throttled".into()),
    tonic::Code::Unavailable
);
storage_err_case!(
    coverage_helpers_007_query_maps_to_internal,
    StorageError::Query("bad sql".into()),
    tonic::Code::Internal
);
storage_err_case!(
    coverage_helpers_008_unsupported_maps_to_internal,
    StorageError::Unsupported("no artifact backend".into()),
    tonic::Code::Internal
);

#[test]
fn coverage_helpers_009_not_found_message_carries_entity_and_id() {
    let status = storage_err(StorageError::NotFound {
        entity: "sequence",
        id: "42".into(),
    });
    assert_eq!(status.message(), "sequence 42");
}

// --- JSON envelope ---

#[test]
fn coverage_helpers_010_from_json_str_parses_valid_payload() {
    let value: serde_json::Value = from_json_str(r#"{"a": 1}"#).unwrap();
    assert_eq!(value, serde_json::json!({"a": 1}));
}

#[test]
fn coverage_helpers_011_from_json_str_rejects_malformed_payload() {
    let status = from_json_str::<serde_json::Value>("{oops").unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_helpers_012_from_json_str_accepts_payload_at_ten_mib() {
    let document = format!(r#"{{"d":"{}"}}"#, "x".repeat(10 * 1024 * 1024 - 8));
    assert_eq!(document.len(), 10 * 1024 * 1024);
    assert!(from_json_str::<serde_json::Value>(&document).is_ok());
}

#[test]
fn coverage_helpers_013_from_json_str_rejects_payload_over_ten_mib() {
    let document = format!(r#"{{"d":"{}"}}"#, "x".repeat(10 * 1024 * 1024 - 7));
    assert_eq!(document.len(), 10 * 1024 * 1024 + 1);
    let status = from_json_str::<serde_json::Value>(&document).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_helpers_014_to_json_string_round_trips() {
    let json = to_json_string(&serde_json::json!({"k": [1, 2, 3]})).unwrap();
    let back: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(back, serde_json::json!({"k": [1, 2, 3]}));
}

// --- request scaffolding ---

#[test]
fn coverage_helpers_015_stream_request_stamps_caller_tenant() {
    let request = stream_request((), Some(&TenantId::unchecked("acme")));
    assert_eq!(
        caller_tenant(&request).map(orch8_types::ids::TenantId::as_str),
        Some("acme")
    );
}

#[test]
fn coverage_helpers_016_stream_request_without_tenant_stays_anonymous() {
    let request = stream_request((), None);
    assert!(caller_tenant(&request).is_none());
}

#[test]
fn coverage_helpers_017_worker_server_frame_wraps_payload() {
    let frame = worker_server_frame(proto::worker_stream_server::Payload::Ack(
        proto::WorkerStreamAck {
            operation: "complete".into(),
            task_id: "1".into(),
        },
    ));
    assert!(matches!(
        frame.payload,
        Some(proto::worker_stream_server::Payload::Ack(_))
    ));
}

#[test]
fn coverage_helpers_018_artifact_server_frame_wraps_payload() {
    let frame = artifact_server_frame(proto::artifact_transfer_server::Payload::Chunk(
        proto::ArtifactTransferChunk {
            transfer_id: "t".into(),
            offset: 0,
            data: vec![1],
            sha256: vec![2],
            final_chunk: true,
        },
    ));
    assert!(matches!(
        frame.payload,
        Some(proto::artifact_transfer_server::Payload::Chunk(_))
    ));
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
async fn coverage_helpers_044_get_worker_task_checked_enforces_tenant() {
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
async fn coverage_helpers_045_get_worker_task_checked_rejects_unknown_task() {
    let storage = storage().await;
    let status = get_worker_task_checked(&storage, None, Uuid::now_v7())
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::NotFound);
}
