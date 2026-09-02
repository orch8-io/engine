//! Coverage tests for the shared service helpers the new worker-session,
//! telemetry, and artifact features are built on: storage-error mapping,
//! JSON envelope bounds, retry cloning, block-tree lookup, retry admission,
//! and new-instance sanitization.
//!
//! Count contract: 47 independently named unit tests.

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

// --- retry_worker_task ---

fn claimed_task() -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(Uuid::now_v7()),
        block_id: orch8_types::ids::BlockId::new("step_1"),
        handler_name: "payments".into(),
        queue_name: Some("critical".into()),
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
    }
}

#[test]
fn coverage_helpers_019_retry_increments_attempt() {
    let retried = retry_worker_task(&claimed_task());
    assert_eq!(retried.attempt, 4);
}

#[test]
fn coverage_helpers_020_retry_saturates_attempt_at_u16_max() {
    let mut task = claimed_task();
    task.attempt = u16::MAX;
    let retried = retry_worker_task(&task);
    assert_eq!(retried.attempt, u16::MAX);
}

#[test]
fn coverage_helpers_021_retry_gets_a_fresh_task_id() {
    let task = claimed_task();
    let retried = retry_worker_task(&task);
    assert_ne!(retried.id, task.id);
}

#[test]
fn coverage_helpers_022_retry_returns_task_to_pending() {
    let retried = retry_worker_task(&claimed_task());
    assert_eq!(retried.state, WorkerTaskState::Pending);
}

#[test]
fn coverage_helpers_023_retry_clears_claim_and_heartbeat_metadata() {
    let retried = retry_worker_task(&claimed_task());
    assert!(retried.worker_id.is_none());
    assert!(retried.claimed_at.is_none());
    assert!(retried.heartbeat_at.is_none());
}

#[test]
fn coverage_helpers_024_retry_preserves_resume_checkpoint_and_sequence() {
    let retried = retry_worker_task(&claimed_task());
    assert_eq!(
        retried.resume_checkpoint,
        Some(serde_json::json!({"cursor": 10}))
    );
    assert_eq!(retried.checkpoint_seq, 7);
}

#[test]
fn coverage_helpers_025_retry_clears_terminal_outcome_fields() {
    let retried = retry_worker_task(&claimed_task());
    assert!(retried.completed_at.is_none());
    assert!(retried.output.is_none());
    assert!(retried.error_message.is_none());
    assert!(retried.error_retryable.is_none());
}

#[test]
fn coverage_helpers_026_retry_copies_routing_and_payload_fields() {
    let task = claimed_task();
    let retried = retry_worker_task(&task);
    assert_eq!(retried.instance_id, task.instance_id);
    assert_eq!(retried.block_id, task.block_id);
    assert_eq!(retried.handler_name, "payments");
    assert_eq!(retried.queue_name.as_deref(), Some("critical"));
    assert_eq!(retried.params, task.params);
    assert_eq!(retried.context, task.context);
    assert_eq!(retried.timeout_ms, Some(5_000));
}

#[test]
fn coverage_helpers_027_retry_refreshes_created_at() {
    let task = claimed_task();
    let retried = retry_worker_task(&task);
    assert!(retried.created_at > task.created_at);
}

// --- find_step_block ---

fn blocks(value: serde_json::Value) -> Vec<BlockDefinition> {
    serde_json::from_value(value).unwrap()
}

fn step(id: &str) -> serde_json::Value {
    serde_json::json!({"type": "step", "id": id, "handler": "noop", "params": {}})
}

fn find_id(blocks: &[BlockDefinition], id: &str) -> Option<String> {
    find_step_block(blocks, &orch8_types::ids::BlockId::new(id))
        .map(|step| step.id.as_str().to_owned())
}

#[test]
fn coverage_helpers_028_finds_top_level_step() {
    let tree = blocks(serde_json::json!([step("a"), step("b")]));
    assert_eq!(find_id(&tree, "b").as_deref(), Some("b"));
}

#[test]
fn coverage_helpers_029_finds_step_inside_parallel_branch() {
    let tree = blocks(serde_json::json!([
        {"type": "parallel", "id": "p", "branches": [[step("deep")], [step("other")]]}
    ]));
    assert_eq!(find_id(&tree, "deep").as_deref(), Some("deep"));
}

#[test]
fn coverage_helpers_030_finds_step_inside_race_branch() {
    let tree = blocks(serde_json::json!([
        {"type": "race", "id": "r", "branches": [[step("winner")]]}
    ]));
    assert_eq!(find_id(&tree, "winner").as_deref(), Some("winner"));
}

#[test]
fn coverage_helpers_031_finds_step_inside_loop_body() {
    let tree = blocks(serde_json::json!([
        {"type": "loop", "id": "l", "condition": "true", "body": [step("iter")]}
    ]));
    assert_eq!(find_id(&tree, "iter").as_deref(), Some("iter"));
}

#[test]
fn coverage_helpers_032_finds_step_inside_for_each_body() {
    let tree = blocks(serde_json::json!([
        {"type": "for_each", "id": "f", "collection": "items", "body": [step("per_item")]}
    ]));
    assert_eq!(find_id(&tree, "per_item").as_deref(), Some("per_item"));
}

#[test]
fn coverage_helpers_033_finds_step_inside_router_route() {
    let tree = blocks(serde_json::json!([
        {"type": "router", "id": "r", "routes": [{"condition": "c", "blocks": [step("routed")]}]}
    ]));
    assert_eq!(find_id(&tree, "routed").as_deref(), Some("routed"));
}

#[test]
fn coverage_helpers_034_finds_step_inside_router_default() {
    let tree = blocks(serde_json::json!([
        {"type": "router", "id": "r", "routes": [], "default": [step("fallback")]}
    ]));
    assert_eq!(find_id(&tree, "fallback").as_deref(), Some("fallback"));
}

#[test]
fn coverage_helpers_035_finds_step_inside_try_catch_arms() {
    let tree = blocks(serde_json::json!([
        {
            "type": "try_catch",
            "id": "t",
            "try_block": [step("try_step")],
            "catch_block": [step("catch_step")],
            "finally_block": [step("finally_step")]
        }
    ]));
    assert_eq!(find_id(&tree, "try_step").as_deref(), Some("try_step"));
    assert_eq!(find_id(&tree, "catch_step").as_deref(), Some("catch_step"));
    assert_eq!(
        find_id(&tree, "finally_step").as_deref(),
        Some("finally_step")
    );
}

#[test]
fn coverage_helpers_036_finds_step_inside_ab_split_variant() {
    let tree = blocks(serde_json::json!([
        {
            "type": "a_b_split",
            "id": "ab",
            "variants": [
                {"name": "va", "weight": 50, "blocks": [step("variant_step")]}
            ]
        }
    ]));
    assert_eq!(
        find_id(&tree, "variant_step").as_deref(),
        Some("variant_step")
    );
}

#[test]
fn coverage_helpers_037_finds_step_inside_cancellation_scope() {
    let tree = blocks(serde_json::json!([
        {"type": "cancellation_scope", "id": "cs", "blocks": [step("shielded")]}
    ]));
    assert_eq!(find_id(&tree, "shielded").as_deref(), Some("shielded"));
}

#[test]
fn coverage_helpers_038_finds_saga_action_and_compensation_steps() {
    let tree = blocks(serde_json::json!([
        {
            "type": "saga",
            "id": "s",
            "steps": [{
                "id": "s1",
                "action": step("action_step"),
                "compensation": step("compensation_step")
            }]
        }
    ]));
    assert_eq!(
        find_id(&tree, "action_step").as_deref(),
        Some("action_step")
    );
    assert_eq!(
        find_id(&tree, "compensation_step").as_deref(),
        Some("compensation_step")
    );
}

#[test]
fn coverage_helpers_039_missing_block_yields_none() {
    let tree = blocks(serde_json::json!([step("a")]));
    assert!(find_id(&tree, "ghost").is_none());
}

#[test]
fn coverage_helpers_040_sub_sequence_block_is_not_a_step_match() {
    let tree = blocks(serde_json::json!([
        {"type": "sub_sequence", "id": "child", "sequence_name": "other_seq"}
    ]));
    assert!(find_id(&tree, "child").is_none());
}

// --- worker_task_can_retry / get_worker_task_checked ---

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
async fn coverage_helpers_041_retry_allowed_before_attempt_budget_is_spent() {
    let storage = storage().await;
    let mut task = seed_retry_topology(&storage, 3).await;
    task.attempt = 1;
    assert!(worker_task_can_retry(&storage, &task).await.unwrap());
}

#[tokio::test]
async fn coverage_helpers_042_retry_denied_once_attempt_budget_is_spent() {
    let storage = storage().await;
    let mut task = seed_retry_topology(&storage, 3).await;
    task.attempt = 2;
    assert!(!worker_task_can_retry(&storage, &task).await.unwrap());
}

#[tokio::test]
async fn coverage_helpers_043_retry_denied_when_instance_is_gone() {
    let storage = storage().await;
    let task = claimed_task();
    assert!(!worker_task_can_retry(&storage, &task).await.unwrap());
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

fn sequence_without_retry(seq_id: &str) -> SequenceDefinition {
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
            "params": {}
        }],
        "created_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap()
}

#[tokio::test]
async fn coverage_helpers_046_retry_denied_when_step_has_no_retry_policy() {
    let storage = storage().await;
    storage
        .create_sequence(&sequence_without_retry(SEQ))
        .await
        .unwrap();
    storage
        .create_instance(&plain_instance(INST, SEQ))
        .await
        .unwrap();
    let mut task = claimed_task();
    task.instance_id = InstanceId::from_uuid(Uuid::parse_str(INST).unwrap());
    // Attempt 0 leaves budget to spare, so the denial can only come from the
    // missing retry policy.
    task.attempt = 0;
    assert!(!worker_task_can_retry(&storage, &task).await.unwrap());
}

#[tokio::test]
async fn coverage_helpers_047_retry_denied_when_block_is_not_a_step() {
    let storage = storage().await;
    let mut task = seed_retry_topology(&storage, 3).await;
    task.block_id = orch8_types::ids::BlockId::new("ghost");
    // Attempt 0 leaves budget to spare, so the denial can only come from the
    // unresolvable block id.
    task.attempt = 0;
    assert!(!worker_task_can_retry(&storage, &task).await.unwrap());
}
