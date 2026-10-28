//! Negotiated worker stream sessions: only server-known features are
//! negotiated (exact match, server order); each frame kind is refused unless
//! its feature was negotiated; a session may only complete, fail or heartbeat
//! tasks it was given under its own worker id; demand is bounded by the
//! in-flight budget, refused while draining, and scoped to the session's
//! tenant; a reclaimed task fences the previous claim generation.

use super::*;

use tokio::sync::mpsc;

fn requested(features: &[&str]) -> Vec<String> {
    features
        .iter()
        .map(|feature| (*feature).to_owned())
        .collect()
}

#[test]
fn only_known_features_negotiate_in_server_order() {
    let all_in_server_order = [
        "task_delivery",
        "completion",
        "failure",
        "heartbeat",
        "cancellation",
        "runtime_capabilities",
        "draining",
        "placement_commands",
    ];
    let mut reversed = all_in_server_order;
    reversed.reverse();
    let cases: [(&str, &[&str], &[&str]); 5] = [
        ("empty request", &[], &[]),
        ("reversed request", &reversed, &all_in_server_order),
        (
            "unknown features dropped",
            &["task_delivery", "quantum_delivery", "heartbeat"],
            &["task_delivery", "heartbeat"],
        ),
        (
            "duplicates collapse",
            &["heartbeat", "heartbeat", "heartbeat"],
            &["heartbeat"],
        ),
        (
            "match is exact",
            &["Task_Delivery", "task-delivery", " task_delivery", ""],
            &[],
        ),
    ];
    for (case, request, expected) in cases {
        assert_eq!(
            negotiated_worker_features(&requested(request)),
            requested(expected),
            "case: {case}"
        );
    }
}

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
fn feature_is_enabled_only_when_requested_and_known() {
    let session_open = open(&["task_delivery", "heartbeat", "quantum_delivery"]);
    assert!(worker_feature_enabled(&session_open, "heartbeat"));
    assert!(!worker_feature_enabled(&session_open, "completion"));
    assert!(!worker_feature_enabled(&session_open, "quantum_delivery"));
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
    session_with_storage(features, tenant, storage)
}

fn session_with_storage(
    features: &[&str],
    tenant: Option<&str>,
    storage: Arc<dyn StorageBackend>,
) -> Session {
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
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({"amount": 42}),
        context: serde_json::json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        claim_epoch: 0,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
        effect_id: None,
        continuity_epoch: None,
        lease_secs: None,
        carries_credentials: false,
        claimed_runtime_kind: None,
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
async fn second_open_frame_is_failed_precondition() {
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
async fn payloadless_frame_is_invalid_argument() {
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
async fn completion_with_malformed_task_id_is_invalid_argument() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: "not-a-uuid".into(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn completion_naming_another_worker_is_denied_and_keeps_the_task_outstanding() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
                output_json: "{}".into(),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
    assert!(session.outstanding.contains(&task_id));
}

#[tokio::test]
async fn completion_for_a_task_this_session_was_not_given_is_denied() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn demand_while_draining_is_failed_precondition() {
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
async fn zero_capacity_demand_delivers_nothing() {
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
async fn demand_claims_a_pending_task_into_the_session() {
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
async fn demand_is_capped_by_the_in_flight_budget() {
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
async fn heartbeat_for_a_dead_task_cancels_and_evicts_it() {
    let mut session = session(&["task_delivery", "heartbeat"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);

    session
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-a".into(),
                claim_epoch: 1,
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
async fn command_ack_with_malformed_id_is_invalid_argument() {
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
async fn runtime_heartbeat_persists_registered_capabilities_and_acks() {
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
async fn runtime_heartbeat_cannot_swap_the_session_runtime_id() {
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
async fn draining_heartbeat_stops_further_demand() {
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
async fn heartbeat_for_a_live_claimed_task_is_acked() {
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
                claim_epoch: 1,
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

#[tokio::test]
async fn reclaim_fences_the_old_generation_with_the_same_worker_id() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000107";
    const INST: &str = "00000000-0000-0000-0000-000000000108";
    let mut old_process = session(&["task_delivery", "heartbeat"], None).await;
    old_process
        .storage
        .create_sequence(&sequence_definition(SEQ, "test"))
        .await
        .unwrap();
    old_process
        .storage
        .create_instance(&task_instance(INST, SEQ, "test"))
        .await
        .unwrap();
    let task = pending_task(INST, "payments");
    old_process.storage.create_worker_task(&task).await.unwrap();

    old_process
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Task(first_frame) = old_process.next_frame() else {
        panic!("first process must receive the task");
    };
    let first_claim: WorkerTask = serde_json::from_str(&first_frame.task_json).unwrap();
    assert_eq!(first_claim.claim_epoch, 1);

    old_process
        .storage
        .reap_stale_worker_tasks(std::time::Duration::ZERO)
        .await
        .unwrap();
    let mut replacement = session_with_storage(
        &["task_delivery", "heartbeat"],
        None,
        Arc::clone(&old_process.storage),
    );
    replacement
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Task(second_frame) = replacement.next_frame() else {
        panic!("replacement process must receive the reclaimed task");
    };
    let second_claim: WorkerTask = serde_json::from_str(&second_frame.task_json).unwrap();
    assert_eq!(second_claim.claim_epoch, 2);
    assert_eq!(second_claim.worker_id.as_deref(), Some("worker-a"));

    old_process
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task.id.to_string(),
                worker_id: "worker-a".into(),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Cancellation(cancelled) = old_process.next_frame()
    else {
        panic!("stale generation must be cancelled");
    };
    assert_eq!(cancelled.task_id, task.id.to_string());

    replacement
        .dispatch(proto::worker_stream_client::Payload::Heartbeat(
            proto::HeartbeatTaskRequest {
                task_id: task.id.to_string(),
                worker_id: "worker-a".into(),
                claim_epoch: 2,
            },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Ack(ack) = replacement.next_frame() else {
        panic!("current generation heartbeat must be acknowledged");
    };
    assert_eq!(ack.operation, "heartbeat");

    let evidence = replacement
        .storage
        .list_worker_task_attempt_events(task.id, 10)
        .await
        .unwrap();
    assert_eq!(
        evidence
            .iter()
            .map(|event| event.event.as_str())
            .collect::<Vec<_>>(),
        ["claimed", "reclaimed", "claimed", "stale_mutation_rejected"]
    );
}

#[tokio::test]
async fn frames_for_unnegotiated_features_are_failed_precondition() {
    use proto::worker_stream_client::Payload;
    let frames = [
        (
            "runtime heartbeat",
            Payload::RuntimeHeartbeat(proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities_json(Uuid::now_v7(), false),
            }),
        ),
        (
            "command ack",
            Payload::CommandAck(proto::WorkerCommandAck {
                command_id: Uuid::now_v7().to_string(),
            }),
        ),
        (
            "completion",
            Payload::Complete(proto::CompleteTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                output_json: "{}".into(),
                claim_epoch: 1,
            }),
        ),
        (
            "failure",
            Payload::Fail(proto::FailTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                message: "boom".into(),
                retryable: false,
                claim_epoch: 1,
            }),
        ),
        (
            "task heartbeat",
            Payload::Heartbeat(proto::HeartbeatTaskRequest {
                task_id: Uuid::now_v7().to_string(),
                worker_id: "worker-a".into(),
                claim_epoch: 1,
            }),
        ),
    ];
    for (case, frame) in frames {
        let mut session = session(&["task_delivery"], Some("test")).await;
        let status = session.dispatch(frame).await.unwrap_err();
        assert_eq!(
            status.code(),
            tonic::Code::FailedPrecondition,
            "case: {case}"
        );
        session.assert_no_frames();
    }
}

#[tokio::test]
async fn failure_or_heartbeat_naming_another_worker_is_denied() {
    use proto::worker_stream_client::Payload;
    let task_id = Uuid::now_v7();
    let frames = [
        (
            "failure",
            Payload::Fail(proto::FailTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
                message: "boom".into(),
                retryable: false,
                claim_epoch: 1,
            }),
        ),
        (
            "heartbeat",
            Payload::Heartbeat(proto::HeartbeatTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-b".into(),
                claim_epoch: 1,
            }),
        ),
    ];
    for (case, frame) in frames {
        let mut session = session(&["task_delivery", "failure", "heartbeat"], None).await;
        session.outstanding.insert(task_id);
        let status = session.dispatch(frame).await.unwrap_err();
        assert_eq!(status.code(), tonic::Code::PermissionDenied, "case: {case}");
        assert!(session.outstanding.contains(&task_id), "case: {case}");
    }
}

#[tokio::test]
async fn frame_over_the_message_limit_is_resource_exhausted() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    let task_id = Uuid::now_v7();
    session.outstanding.insert(task_id);
    let limit = usize::try_from(WORKER_STREAM_MAX_MESSAGE_BYTES).unwrap();
    let status = session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: task_id.to_string(),
                worker_id: "worker-a".into(),
                output_json: format!(r#"{{"blob":"{}"}}"#, "x".repeat(limit)),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
    assert!(session.outstanding.contains(&task_id));
}

#[tokio::test]
async fn command_ack_deletes_the_command_and_is_confirmed() {
    let mut session = session(&["task_delivery", "placement_commands"], Some("test")).await;
    let command = orch8_types::worker::WorkerCommand {
        id: Uuid::now_v7(),
        worker_id: "worker-a".into(),
        tenant_id: "test".into(),
        command: orch8_types::worker::WorkerCommandKind::Ping,
        payload: serde_json::json!({}),
        created_at: chrono::Utc::now(),
    };
    session
        .storage
        .enqueue_worker_command(&command)
        .await
        .unwrap();
    session
        .dispatch(proto::worker_stream_client::Payload::CommandAck(
            proto::WorkerCommandAck {
                command_id: command.id.to_string(),
            },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("command ack must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "command");
    assert_eq!(ack.task_id, command.id.to_string());
    assert!(
        session
            .storage
            .list_worker_commands("worker-a")
            .await
            .unwrap()
            .is_empty()
    );
}

/// Seed one sequence + instance for `tenant` and a pending `payments` task.
async fn seed_pending_task(
    storage: &Arc<dyn StorageBackend>,
    seq: &str,
    inst: &str,
    tenant: &str,
) -> WorkerTask {
    storage
        .create_sequence(&sequence_definition(seq, tenant))
        .await
        .unwrap();
    // This task represents an already dispatched flat activity, with durable
    // native identity available to fenced failure and retry resolution.
    let mut instance = task_instance(inst, seq, tenant);
    instance.state = InstanceState::Waiting;
    instance.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    instance.context.runtime.current_step = Some(orch8_types::ids::BlockId::new("step_1"));
    instance.context.runtime.current_step_started_at = Some(chrono::Utc::now());
    storage.create_instance(&instance).await.unwrap();
    let task = pending_task(inst, "payments");
    storage.create_worker_task(&task).await.unwrap();
    task
}

async fn claim_one(session: &mut Session) -> WorkerTask {
    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Task(delivered) = session.next_frame() else {
        panic!("demand must produce a task frame");
    };
    serde_json::from_str(&delivered.task_json).unwrap()
}

#[tokio::test]
async fn tenant_scoped_demand_never_claims_another_tenants_task() {
    let mut session = session(&["task_delivery"], Some("test")).await;
    let foreign = seed_pending_task(
        &session.storage,
        "00000000-0000-0000-0000-000000000111",
        "00000000-0000-0000-0000-000000000112",
        "other",
    )
    .await;

    session
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 4 },
        ))
        .await
        .unwrap();

    session.assert_no_frames();
    let stored = session
        .storage
        .get_worker_task(foreign.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, WorkerTaskState::Pending);
    assert!(stored.worker_id.is_none());
}

#[tokio::test]
async fn completion_of_a_claimed_task_persists_the_output_and_releases_the_slot() {
    let mut session = session(&["task_delivery", "completion"], None).await;
    seed_pending_task(
        &session.storage,
        "00000000-0000-0000-0000-000000000113",
        "00000000-0000-0000-0000-000000000114",
        "test",
    )
    .await;
    let claimed = claim_one(&mut session).await;

    session
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: claimed.id.to_string(),
                worker_id: "worker-a".into(),
                output_json: r#"{"receipt":"r1"}"#.into(),
                claim_epoch: claimed.claim_epoch,
            },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("completion must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "complete");
    assert_eq!(ack.task_id, claimed.id.to_string());
    assert!(!session.outstanding.contains(&claimed.id));
    let stored = session
        .storage
        .get_worker_task(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, WorkerTaskState::Completed);
    assert_eq!(stored.output, Some(serde_json::json!({"receipt": "r1"})));
}

#[tokio::test]
async fn permanent_failure_of_a_claimed_task_is_persisted_and_releases_the_slot() {
    let mut session = session(&["task_delivery", "failure"], None).await;
    seed_pending_task(
        &session.storage,
        "00000000-0000-0000-0000-000000000115",
        "00000000-0000-0000-0000-000000000116",
        "test",
    )
    .await;
    let claimed = claim_one(&mut session).await;

    session
        .dispatch(proto::worker_stream_client::Payload::Fail(
            proto::FailTaskRequest {
                task_id: claimed.id.to_string(),
                worker_id: "worker-a".into(),
                message: "card declined".into(),
                retryable: false,
                claim_epoch: claimed.claim_epoch,
            },
        ))
        .await
        .unwrap();

    let proto::worker_stream_server::Payload::Ack(ack) = session.next_frame() else {
        panic!("failure must be confirmed with an ack frame");
    };
    assert_eq!(ack.operation, "fail");
    assert_eq!(ack.task_id, claimed.id.to_string());
    assert!(!session.outstanding.contains(&claimed.id));
    let stored = session
        .storage
        .get_worker_task(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.state, WorkerTaskState::Failed);
    assert_eq!(stored.error_message.as_deref(), Some("card declined"));
    assert_eq!(stored.error_retryable, Some(false));
}

#[tokio::test]
async fn oversized_completion_keeps_claim_retryable_and_does_not_commit_output() {
    let mut session = session(&["task_delivery", "completion"], Some("test")).await;
    session.service.max_context_bytes = 1024;
    seed_pending_task(
        &session.storage,
        "00000000-0000-0000-0000-000000000121",
        "00000000-0000-0000-0000-000000000122",
        "test",
    )
    .await;
    let claimed = claim_one(&mut session).await;
    let before = session
        .storage
        .get_instance(claimed.instance_id)
        .await
        .unwrap()
        .unwrap();
    let request = |output: serde_json::Value| {
        stream_request(
            proto::CompleteTaskRequest {
                task_id: claimed.id.to_string(),
                worker_id: "worker-a".into(),
                claim_epoch: claimed.claim_epoch,
                output_json: output.to_string(),
            },
            session.tenant.as_ref(),
        )
    };
    let error = session
        .service
        .complete_task(request(serde_json::json!({"huge": "x".repeat(2048)})))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    let retained = session
        .storage
        .get_worker_task(claimed.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        retained.state,
        WorkerTaskState::Claimed,
        "size rejection consumed the worker lease"
    );
    assert_eq!(retained.claim_epoch, claimed.claim_epoch);
    assert!(retained.output.is_none());
    assert!(
        session
            .storage
            .get_block_output(claimed.instance_id, &claimed.block_id)
            .await
            .unwrap()
            .is_none()
    );
    let after = session
        .storage
        .get_instance(claimed.instance_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(after.context).unwrap(),
        serde_json::to_value(before.context).unwrap()
    );
    session
        .service
        .complete_task(request(serde_json::json!({"small": true})))
        .await
        .unwrap();
    assert_eq!(
        session
            .storage
            .get_worker_task(claimed.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerTaskState::Completed
    );
}

async fn seed_admitted_flat_task(storage: &Arc<dyn StorageBackend>) -> WorkerTask {
    use orch8_storage::{
        worker_admission::{
            WorkerDispatchIntent, WorkerDispatchPublication, WorkerEffectPlan, dispatch_effect,
            dispatch_scope,
        },
        worker_invocation::WorkerDispatchSelection,
    };
    use orch8_types::continuity::{EffectId, EffectReceipt, EffectState, ExecutionEpoch};
    let sequence_id = Uuid::now_v7().to_string();
    let instance_id = Uuid::now_v7().to_string();
    let mut sequence = sequence_definition(&sequence_id, "test");
    if let orch8_types::sequence::BlockDefinition::Step(step) = &mut sequence.blocks[0] {
        step.handler = "payments".into();
    }
    storage.create_sequence(&sequence).await.unwrap();
    let mut instance = task_instance(&instance_id, &sequence_id, "test");
    instance.state = InstanceState::Scheduled;
    instance.next_fire_at = Some(chrono::Utc::now());
    instance.context.runtime.run_id = Some(Uuid::now_v7().to_string());
    instance.context.runtime.current_step = Some(orch8_types::ids::BlockId::new("step_1"));
    instance.context.runtime.current_step_started_at = Some(chrono::Utc::now());
    storage.create_instance(&instance).await.unwrap();
    let instance = storage
        .claim_due_instances(chrono::Utc::now() + chrono::Duration::seconds(1), 1, 0)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let mut task = pending_task(&instance_id, "payments");
    task.context = serde_json::to_value(&instance.context).unwrap();
    let fence = storage
        .prepare_worker_dispatch(
            &instance,
            &task.block_id,
            &WorkerDispatchSelection::Flat {
                step_started_at: instance.context.runtime.current_step_started_at.unwrap(),
            },
        )
        .await
        .unwrap()
        .unwrap();
    task.continuity_epoch = fence.continuity.as_ref().map(|scope| scope.epoch);
    let scope = dispatch_scope(&fence);
    let proof = dispatch_effect(&task, EffectId::new());
    let now = chrono::Utc::now();
    let effect = WorkerEffectPlan::new(
        EffectReceipt {
            id: proof.effect_id,
            tenant_id: instance.tenant_id.clone(),
            continuity_id: scope.continuity_id,
            epoch: ExecutionEpoch::from_u64(scope.epoch),
            instance_id: instance.id,
            block_id: task.block_id.clone(),
            kind: proof.kind,
            state: EffectState::Planned,
            request_sha256: proof.request_sha256,
            destination_fingerprint: proof.destination_fingerprint,
            idempotency_key: None,
            provider_receipt_id: None,
            attempt: u32::from(task.attempt),
            created_at: now,
            updated_at: now,
        },
        false,
    );
    let intent = WorkerDispatchIntent::new(task, fence, Some(effect)).unwrap();
    let WorkerDispatchPublication::Published { task_id, .. } =
        storage.admit_worker_dispatch(&intent).await.unwrap()
    else {
        panic!("fixture admission refused")
    };
    storage
        .update_instance_state(instance.id, InstanceState::Waiting, None)
        .await
        .unwrap();
    storage.get_worker_task(task_id).await.unwrap().unwrap()
}

#[tokio::test]
async fn completed_outcome_replay_after_hot_gc_keeps_original_payload_while_paused() {
    let mut session = session(&["task_delivery", "completion"], Some("test")).await;
    seed_admitted_flat_task(&session.storage).await;
    let claimed = claim_one(&mut session).await;
    session
        .storage
        .update_instance_state(claimed.instance_id, InstanceState::Paused, None)
        .await
        .unwrap();
    let request = |output: serde_json::Value| {
        stream_request(
            proto::CompleteTaskRequest {
                task_id: claimed.id.to_string(),
                worker_id: claimed.worker_id.clone().unwrap(),
                claim_epoch: claimed.claim_epoch,
                output_json: output.to_string(),
            },
            session.tenant.as_ref(),
        )
    };
    session
        .service
        .complete_task(request(serde_json::json!({"result":"original"})))
        .await
        .unwrap();
    session
        .storage
        .delete_worker_task(claimed.id)
        .await
        .unwrap();
    let replay = session
        .service
        .complete_task(request(serde_json::json!({"result":"changed"})))
        .await;
    assert!(
        replay.is_ok(),
        "lost completion acknowledgement depended on the hot task: {replay:?}"
    );
    let parent = session
        .storage
        .get_instance(claimed.instance_id)
        .await
        .unwrap()
        .unwrap();
    let claim = orch8_types::worker::WorkerClaim::new(
        claimed.worker_id.clone().unwrap(),
        claimed.claim_epoch,
    );
    let accepted = session
        .storage
        .get_worker_accepted_outcome(session.tenant.as_ref(), claimed.id, &claim)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(accepted.output, serde_json::json!({"result":"original"}));
    assert_eq!(parent.state, InstanceState::Paused);
    assert_eq!(parent.context.data["result"], "original");
    assert_ne!(
        parent.context.data.get("result"),
        Some(&serde_json::json!("changed"))
    );
}
