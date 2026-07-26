//! Coverage tests for negotiated worker stream sessions: feature
//! negotiation, per-frame feature gating, session ownership checks, demand
//! flow control, and heartbeat-driven cancellation.
//!
//! Count contract: 49 independently named unit tests.

use super::*;

use tokio::sync::mpsc;

// --- pure negotiation helpers ---

fn requested(features: &[&str]) -> Vec<String> {
    features
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect()
}

macro_rules! negotiation_case {
    ($name:ident, $requested:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let negotiated = negotiated_worker_features(&requested($requested));
            assert_eq!(negotiated, requested($expected));
        }
    };
}

negotiation_case!(
    coverage_session_001_empty_request_negotiates_nothing,
    &[],
    &[]
);
negotiation_case!(
    coverage_session_002_all_features_negotiate_in_server_order,
    &[
        "task_delivery",
        "completion",
        "failure",
        "heartbeat",
        "cancellation",
        "runtime_capabilities",
        "draining",
        "placement_commands"
    ],
    &[
        "task_delivery",
        "completion",
        "failure",
        "heartbeat",
        "cancellation",
        "runtime_capabilities",
        "draining",
        "placement_commands"
    ]
);
negotiation_case!(
    coverage_session_003_reversed_request_still_yields_server_order,
    &[
        "placement_commands",
        "draining",
        "runtime_capabilities",
        "cancellation",
        "heartbeat",
        "failure",
        "completion",
        "task_delivery"
    ],
    &[
        "task_delivery",
        "completion",
        "failure",
        "heartbeat",
        "cancellation",
        "runtime_capabilities",
        "draining",
        "placement_commands"
    ]
);
negotiation_case!(
    coverage_session_004_single_task_delivery_negotiates,
    &["task_delivery"],
    &["task_delivery"]
);
negotiation_case!(
    coverage_session_005_unknown_feature_alone_negotiates_nothing,
    &["quantum_delivery"],
    &[]
);
negotiation_case!(
    coverage_session_006_unknown_features_are_dropped_from_mixed_request,
    &["task_delivery", "quantum_delivery", "heartbeat"],
    &["task_delivery", "heartbeat"]
);
negotiation_case!(
    coverage_session_007_duplicate_requests_collapse_to_one_feature,
    &["heartbeat", "heartbeat", "heartbeat"],
    &["heartbeat"]
);
negotiation_case!(
    coverage_session_008_feature_matching_is_case_sensitive,
    &["Task_Delivery", "TASK_DELIVERY"],
    &[]
);
negotiation_case!(
    coverage_session_009_hyphenated_feature_name_is_not_recognized,
    &["task-delivery"],
    &[]
);
negotiation_case!(
    coverage_session_010_whitespace_padded_feature_is_not_recognized,
    &[" task_delivery", "task_delivery "],
    &[]
);
negotiation_case!(
    coverage_session_011_completion_and_failure_pair_negotiates,
    &["completion", "failure"],
    &["completion", "failure"]
);
negotiation_case!(
    coverage_session_012_control_plane_features_negotiate_in_server_order,
    &["placement_commands", "draining", "runtime_capabilities"],
    &["runtime_capabilities", "draining", "placement_commands"]
);
negotiation_case!(
    coverage_session_013_empty_string_feature_is_ignored,
    &["", "cancellation"],
    &["cancellation"]
);

fn open(features: &[&str]) -> proto::WorkerStreamOpen {
    proto::WorkerStreamOpen {
        worker_id: "worker-a".into(),
        handler_names: vec!["payments".into()],
        supported_features: requested(features),
        max_in_flight: 4,
        protocol_version: WORKER_STREAM_PROTOCOL_VERSION,
        runtime_capabilities_json: String::new(),
        tenant_id: "test".into(),
    }
}

#[test]
fn coverage_session_014_feature_enabled_when_requested() {
    let open = open(&["task_delivery", "heartbeat"]);
    assert!(worker_feature_enabled(&open, "heartbeat"));
}

#[test]
fn coverage_session_015_feature_disabled_when_not_requested() {
    let open = open(&["task_delivery"]);
    assert!(!worker_feature_enabled(&open, "heartbeat"));
}

#[test]
fn coverage_session_016_unknown_feature_never_enabled_even_if_requested() {
    let open = open(&["task_delivery", "quantum_delivery"]);
    assert!(!worker_feature_enabled(&open, "quantum_delivery"));
}

#[test]
fn coverage_session_017_no_features_enabled_on_empty_request() {
    let open = open(&[]);
    for feature in WORKER_STREAM_FEATURES {
        assert!(!worker_feature_enabled(&open, feature));
    }
}

#[test]
fn coverage_session_018_feature_enablement_is_case_sensitive() {
    let open = open(&["Heartbeat"]);
    assert!(!worker_feature_enabled(&open, "heartbeat"));
}

#[test]
fn coverage_session_019_duplicate_request_still_enables_feature() {
    let open = open(&["draining", "draining"]);
    assert!(worker_feature_enabled(&open, "draining"));
}

#[test]
fn coverage_session_020_protocol_constants_match_documented_contract() {
    assert_eq!(WORKER_STREAM_PROTOCOL_VERSION, 1);
    assert_eq!(WORKER_STREAM_MAX_IN_FLIGHT, 256);
    assert_eq!(WORKER_STREAM_MAX_MESSAGE_BYTES, 1024 * 1024);
    assert_eq!(WORKER_STREAM_HEARTBEAT_SECS, 15);
}

#[test]
fn coverage_session_021_feature_catalog_has_no_duplicates() {
    let mut seen = std::collections::HashSet::new();
    for feature in WORKER_STREAM_FEATURES {
        assert!(seen.insert(feature), "duplicate feature {feature}");
    }
    assert_eq!(seen.len(), 8);
}

#[test]
fn coverage_session_022_parse_uuid_accepts_canonical_v4() {
    let uuid = parse_uuid("123e4567-e89b-42d3-a456-426614174000").unwrap();
    assert_eq!(uuid.to_string(), "123e4567-e89b-42d3-a456-426614174000");
}

#[test]
fn coverage_session_023_parse_uuid_rejects_garbage_with_invalid_argument() {
    let status = parse_uuid("not-a-uuid").unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(status.message().contains("not-a-uuid"));
}

#[test]
fn coverage_session_024_parse_uuid_rejects_empty_string() {
    let status = parse_uuid("").unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_session_025_parse_uuid_accepts_nil_uuid() {
    let uuid = parse_uuid("00000000-0000-0000-0000-000000000000").unwrap();
    assert_eq!(uuid, Uuid::nil());
}

// --- frame dispatch against an in-memory session ---

struct Session {
    service: Orch8GrpcService,
    storage: Arc<dyn StorageBackend>,
    open: proto::WorkerStreamOpen,
    tenant: Option<TenantId>,
    max_in_flight: u32,
    outstanding: std::collections::HashSet<Uuid>,
    runtime_id: Option<RuntimeId>,
    draining: bool,
    sender: mpsc::Sender<Result<proto::WorkerStreamServer, Status>>,
    receiver: mpsc::Receiver<Result<proto::WorkerStreamServer, Status>>,
}

async fn session(features: &[&str], tenant: Option<&str>) -> Session {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let (sender, receiver) = mpsc::channel(16);
    Session {
        service: Orch8GrpcService::new(Arc::clone(&storage)),
        storage,
        open: open(features),
        tenant: tenant.map(|value| TenantId::unchecked(value.to_owned())),
        max_in_flight: 4,
        outstanding: std::collections::HashSet::new(),
        runtime_id: None,
        draining: false,
        sender,
        receiver,
    }
}

impl Session {
    async fn dispatch(
        &mut self,
        payload: proto::worker_stream_client::Payload,
    ) -> Result<(), Status> {
        self.service
            .handle_worker_stream_frame(
                proto::WorkerStreamClient {
                    payload: Some(payload),
                },
                &self.open,
                self.tenant.as_ref(),
                self.max_in_flight,
                &mut self.outstanding,
                &mut self.runtime_id,
                &mut self.draining,
                &self.sender,
            )
            .await
    }

    fn next_frame(&mut self) -> proto::worker_stream_server::Payload {
        self.receiver
            .try_recv()
            .expect("a server frame must be queued")
            .expect("frame must not be an error")
            .payload
            .expect("frame must carry a payload")
    }

    fn assert_no_frames(&mut self) {
        assert!(
            self.receiver.try_recv().is_err(),
            "no server frame expected"
        );
    }
}

fn sequence_definition(seq_id: &str, tenant: &str) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": seq_id,
        "tenant_id": tenant,
        "namespace": "default",
        "name": format!("seq_{seq_id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "step_1", "handler": "noop", "params": {}}],
        "created_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap()
}

fn task_instance(inst_id: &str, seq_id: &str, tenant: &str) -> TaskInstance {
    serde_json::from_value(serde_json::json!({
        "id": inst_id,
        "sequence_id": seq_id,
        "tenant_id": tenant,
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

fn pending_task(inst_id: &str, handler: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(Uuid::parse_str(inst_id).unwrap()),
        block_id: orch8_types::ids::BlockId::new("step_1"),
        handler_name: handler.into(),
        queue_name: None,
        params: serde_json::json!({"amount": 42}),
        context: serde_json::json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    }
}

fn capabilities_json(runtime_id: Uuid, draining: bool) -> String {
    serde_json::json!({
        "runtime_id": runtime_id,
        "kind": "edge",
        "trust": "attested",
        "handlers": ["payments"],
        "plugins": ["card-reader"],
        "credentials": [],
        "regions": ["br-south"],
        "hardware": ["secure-enclave"],
        "offline_capable": true,
        "connectivity": "ethernet",
        "draining": draining,
        "observed_at": "2020-01-01T00:00:00Z",
        "expires_at": "2020-01-01T00:00:01Z"
    })
    .to_string()
}

#[tokio::test]
async fn coverage_session_026_second_open_frame_is_failed_precondition() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Open(open(&[
            "task_delivery",
        ])))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    session.assert_no_frames();
}

#[tokio::test]
async fn coverage_session_027_payloadless_frame_is_invalid_argument() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .service
        .handle_worker_stream_frame(
            proto::WorkerStreamClient { payload: None },
            &session.open,
            None,
            session.max_in_flight,
            &mut session.outstanding,
            &mut session.runtime_id,
            &mut session.draining,
            &session.sender,
        )
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_session_028_runtime_heartbeat_requires_negotiated_feature() {
    let mut session = session(&["task_delivery"], Some("test")).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(Uuid::now_v7(), false),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_029_command_ack_requires_negotiated_feature() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::CommandAck(
            proto::WorkerCommandAck {
                command_id: Uuid::now_v7().to_string(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_030_completion_requires_negotiated_feature() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_031_failure_requires_negotiated_feature() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Fail(
            proto::FailTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                message: "boom".into(),
                retryable: false,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_032_heartbeat_requires_negotiated_feature() {
    let mut session = session(&["task_delivery"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_033_completion_with_malformed_task_id_is_invalid_argument() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: "not-a-uuid".into(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_session_034_completion_from_foreign_worker_is_denied() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
                output_json: "{}".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(session.outstanding.contains(&task_id));
}

#[tokio::test]
async fn coverage_session_035_completion_for_task_outside_session_is_denied() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn coverage_session_036_failure_from_foreign_worker_is_denied() {
    let mut session = session(&["task_delivery", "failure"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Fail(
            proto::FailTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
                message: "boom".into(),
                retryable: false,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn coverage_session_037_heartbeat_from_foreign_worker_is_denied() {
    let mut session = session(&["task_delivery", "heartbeat"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn coverage_session_038_demand_while_draining_is_failed_precondition() {
    let mut session = session(&["task_delivery", "draining"], None).await;
    session.draining = true;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_039_zero_capacity_demand_delivers_nothing() {
    let mut session = session(&["task_delivery"], None).await;
    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 0 },
        ))
        .await
        .unwrap();
    session.assert_no_frames();
    assert!(session.outstanding.is_empty());
}

#[tokio::test]
async fn coverage_session_040_demand_claims_pending_task_into_session() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000101";
    const INST: &str = "00000000-0000-0000-0000-000000000102";
    let mut session = session(&["task_delivery"], None).await;
    session
        .storage
        .create_sequence(&sequence_definition(SEQ, "test"))
        .await
        .unwrap();
    session
        .storage
        .create_instance(&task_instance(INST, SEQ, "test"))
        .await
        .unwrap();
    let task = pending_task(INST, "payments");
    session.storage.create_worker_task(&task).await.unwrap();

    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Task(delivered) = session.next_frame() else {
        panic!("demand must produce a task frame");
    };
    let claimed: WorkerTask = serde_json::from_str(&delivered.task_json).unwrap();
    assert_eq!(claimed.id, task.id);
    assert_eq!(claimed.worker_id.as_deref(), Some("worker-a"));
    assert!(session.outstanding.contains(&task.id));
}

#[tokio::test]
async fn coverage_session_041_demand_is_capped_by_outstanding_tasks() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000103";
    const INST: &str = "00000000-0000-0000-0000-000000000104";
    let mut session = session(&["task_delivery"], None).await;
    session.max_in_flight = 1;
    session
        .storage
        .create_sequence(&sequence_definition(SEQ, "test"))
        .await
        .unwrap();
    session
        .storage
        .create_instance(&task_instance(INST, SEQ, "test"))
        .await
        .unwrap();
    let task = pending_task(INST, "payments");
    session.storage.create_worker_task(&task).await.unwrap();
    // One task already in flight consumes the entire in-flight budget.
    session.outstanding.insert(Uuid::now_v7());

    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 4 },
        ))
        .await
        .unwrap();

    session.assert_no_frames();
    let stored = session
        .storage
        .get_worker_task(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, WorkerTaskState::Pending);
}

#[tokio::test]
async fn coverage_session_042_heartbeat_on_dead_task_cancels_and_evicts() {
    let mut session = session(&["task_delivery", "heartbeat"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);

    session
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-a".into(),
            },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Cancellation(cancellation) = session.next_frame()
    else {
        panic!("dead-task heartbeat must produce a cancellation frame");
    };
    assert_eq!(cancellation.task_id, task_id.to_string());
    assert!(!session.outstanding.contains(&task_id));
}

#[tokio::test]
async fn coverage_session_043_command_ack_with_malformed_id_is_invalid_argument() {
    let mut session = session(&["task_delivery", "placement_commands"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::CommandAck(
            proto::WorkerCommandAck {
                command_id: "not-a-uuid".into(),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn coverage_session_044_command_ack_acknowledges_and_confirms() {
    let mut session = session(&["task_delivery", "placement_commands"], None).await;
    let command_id = Uuid::now_v7();
    session
        .dispatch(proto::worker_stream_client::Payload::CommandAck(
            proto::WorkerCommandAck {
                command_id: command_id.to_string(),
            },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("command ack must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "command");
    assert_eq!(ack.task_id, command_id.to_string());
}

#[tokio::test]
async fn coverage_session_045_runtime_heartbeat_without_tenant_is_failed_precondition() {
    let mut session = session(&["task_delivery", "runtime_capabilities"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(Uuid::now_v7(), false),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_046_runtime_heartbeat_persists_and_acks() {
    let mut session = session(&["task_delivery", "runtime_capabilities"], Some("test")).await;
    let runtime_id = Uuid::now_v7();

    session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(runtime_id, false),
            },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("runtime heartbeat must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "runtime_heartbeat");
    assert_eq!(ack.task_id, runtime_id.to_string());
    assert_eq!(session.runtime_id, Some(RuntimeId::from_uuid(runtime_id)));
    let persisted = session
        .storage
        .list_runtime_capabilities(
            &TenantId::unchecked("test"),
            chrono::Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].trust, RuntimeTrustLevel::Registered);
}

#[tokio::test]
async fn coverage_session_047_runtime_heartbeat_cannot_swap_runtime_id() {
    let mut session = session(&["task_delivery", "runtime_capabilities"], Some("test")).await;
    let first = Uuid::now_v7();
    session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(first, false),
            },
        ))
        .await
        .unwrap();
    session.next_frame();

    let status = session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(Uuid::now_v7(), false),
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn coverage_session_048_draining_heartbeat_flips_session_draining() {
    let mut session = session(
        &["task_delivery", "runtime_capabilities", "draining"],
        Some("test"),
    )
    .await;
    session
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(Uuid::now_v7(), true),
            },
        ))
        .await
        .unwrap();
    assert!(session.draining);

    let status = session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_session_049_heartbeat_on_live_claimed_task_is_acked() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000105";
    const INST: &str = "00000000-0000-0000-0000-000000000106";
    let mut session = session(&["task_delivery", "heartbeat"], None).await;
    session
        .storage
        .create_sequence(&sequence_definition(SEQ, "test"))
        .await
        .unwrap();
    session
        .storage
        .create_instance(&task_instance(INST, SEQ, "test"))
        .await
        .unwrap();
    let task = pending_task(INST, "payments");
    session.storage.create_worker_task(&task).await.unwrap();

    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();
    session.next_frame(); // task delivery frame

    session
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task.id.to_string(),
                worker_id: "worker-a".into(),
            },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("live-task heartbeat must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "heartbeat");
    assert_eq!(ack.task_id, task.id.to_string());
    // A successful heartbeat keeps the task in the session's outstanding set.
    assert!(session.outstanding.contains(&task.id));
}
