//! Worker stream reconnect semantics (MS-05) and capability lookup under a
//! large phone fleet (MS-11): a new stream adopts the tasks its worker id
//! already holds (fencing by `claim_epoch` stays intact), a fenced completion
//! is a per-task error that keeps the stream alive, and a `Demand` finds the
//! session's runtime by primary key rather than in a capped listing.

use super::*;

use tokio::sync::mpsc;

fn open(features: &[&str]) -> proto::WorkerStreamOpen {
    proto::WorkerStreamOpen {
        worker_id: "worker-a".into(),
        handler_names: vec!["payments".into()],
        supported_features: features.iter().map(|f| (*f).to_owned()).collect(),
        max_in_flight: 4,
        protocol_version: WORKER_STREAM_PROTOCOL_VERSION,
        runtime_capabilities_json: String::new(),
        tenant_id: "test".into(),
    }
}

struct Stream {
    service: Orch8GrpcService,
    open: proto::WorkerStreamOpen,
    tenant: Option<TenantId>,
    outstanding: std::collections::HashSet<Uuid>,
    runtime_id: Option<RuntimeId>,
    draining: bool,
    sender: mpsc::Sender<Result<proto::WorkerStreamServer, Status>>,
    receiver: mpsc::Receiver<Result<proto::WorkerStreamServer, Status>>,
}

fn stream_on(storage: Arc<dyn StorageBackend>, features: &[&str]) -> Stream {
    let (sender, receiver) = mpsc::channel(16);
    Stream {
        service: Orch8GrpcService::new(storage),
        open: open(features),
        tenant: Some(TenantId::unchecked("test")),
        outstanding: std::collections::HashSet::new(),
        runtime_id: None,
        draining: false,
        sender,
        receiver,
    }
}

async fn fresh_storage() -> Arc<dyn StorageBackend> {
    Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    )
}

impl Stream {
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
                &orch8_storage::worker_claim::WorkerClaimPrincipal::Unsecured {},
                4,
                &mut self.outstanding,
                &mut self.runtime_id,
                &mut self.draining,
                &self.sender,
            )
            .await
    }

    async fn adopt(&mut self) -> Result<(), Status> {
        self.service
            .adopt_held_tasks(
                &self.open,
                self.tenant.as_ref(),
                &orch8_storage::worker_claim::WorkerClaimPrincipal::Unsecured {},
                4,
                &mut self.outstanding,
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

fn sequence_definition(seq_id: &str) -> SequenceDefinition {
    serde_json::from_value(serde_json::json!({
        "id": seq_id,
        "tenant_id": "test",
        "namespace": "default",
        "name": format!("seq_{seq_id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "step_1", "handler": "noop", "params": {}}],
        "created_at": "2024-01-01T00:00:00Z"
    }))
    .unwrap()
}

fn task_instance(inst_id: &str, seq_id: &str) -> TaskInstance {
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

fn pending_task(inst_id: &str, handler: &str) -> WorkerTask {
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(Uuid::parse_str(inst_id).unwrap()),
        block_id: orch8_types::ids::BlockId::new("step_1"),
        handler_name: handler.into(),
        queue_name: None,
        requirements: orch8_types::continuity::CapsuleRequirements::default(),
        params: serde_json::json!({}),
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

async fn seed(storage: &Arc<dyn StorageBackend>, seq: &str, inst: &str) -> WorkerTask {
    storage
        .create_sequence(&sequence_definition(seq))
        .await
        .unwrap();
    storage
        .create_instance(&task_instance(inst, seq))
        .await
        .unwrap();
    let task = pending_task(inst, "payments");
    storage.create_worker_task(&task).await.unwrap();
    task
}

async fn claim_via_demand(stream: &mut Stream) -> WorkerTask {
    stream
        .dispatch(proto::worker_stream_client::Payload::Demand(
            proto::WorkerStreamDemand { capacity: 1 },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Task(frame) = stream.next_frame() else {
        panic!("demand must deliver a task");
    };
    serde_json::from_str(&frame.task_json).unwrap()
}

#[tokio::test]
async fn reconnecting_stream_adopts_tasks_the_worker_already_holds_without_changing_the_fence() {
    let storage = fresh_storage().await;
    seed(
        &storage,
        "00000000-0000-0000-0000-000000000201",
        "00000000-0000-0000-0000-000000000202",
    )
    .await;
    let mut first = stream_on(Arc::clone(&storage), &["task_delivery", "completion"]);
    let claimed = claim_via_demand(&mut first).await;
    assert_eq!(claimed.claim_epoch, 1);
    drop(first); // network flap: the stream's `outstanding` set is gone

    let mut second = stream_on(Arc::clone(&storage), &["task_delivery", "completion"]);
    second.adopt().await.unwrap();
    let proto::worker_stream_server::Payload::Task(frame) = second.next_frame() else {
        panic!("the held task must be redelivered on the new stream");
    };
    let redelivered: WorkerTask = serde_json::from_str(&frame.task_json).unwrap();
    assert_eq!(redelivered.id, claimed.id);
    assert_eq!(
        redelivered.claim_epoch, 1,
        "adoption must not mint a new claim generation"
    );
    assert!(second.outstanding.contains(&claimed.id));

    // The new stream can complete it with the original epoch.
    second
        .dispatch(proto::worker_stream_client::Payload::Complete(
            proto::CompleteTaskRequest {
                task_id: claimed.id.to_string(),
                worker_id: "worker-a".into(),
                output_json: r#"{"ok":true}"#.into(),
                claim_epoch: 1,
            },
        ))
        .await
        .unwrap();
    let proto::worker_stream_server::Payload::Ack(ack) = second.next_frame() else {
        panic!("completion must be acked, not permission_denied");
    };
    assert_eq!(ack.operation, "complete");
    let stored = storage.get_worker_task(claimed.id).await.unwrap().unwrap();
    assert_eq!(stored.state, WorkerTaskState::Completed);
}

#[tokio::test]
async fn adoption_skips_other_workers_and_other_handlers() {
    let storage = fresh_storage().await;
    seed(
        &storage,
        "00000000-0000-0000-0000-000000000211",
        "00000000-0000-0000-0000-000000000212",
    )
    .await;
    // Claimed by a different worker id.
    let mut other = stream_on(Arc::clone(&storage), &["task_delivery"]);
    other.open.worker_id = "worker-b".into();
    let theirs = claim_via_demand(&mut other).await;
    assert_eq!(theirs.worker_id.as_deref(), Some("worker-b"));

    let mut mine = stream_on(Arc::clone(&storage), &["task_delivery"]);
    mine.adopt().await.unwrap();
    mine.assert_no_frames();
    assert!(mine.outstanding.is_empty());

    // Same worker id but the new stream does not serve that handler.
    let mut same_worker_other_handler = stream_on(Arc::clone(&storage), &["task_delivery"]);
    same_worker_other_handler.open.worker_id = "worker-b".into();
    same_worker_other_handler.open.handler_names = vec!["something-else".into()];
    same_worker_other_handler.adopt().await.unwrap();
    same_worker_other_handler.assert_no_frames();
}

#[tokio::test]
async fn fenced_completion_is_a_per_task_error_and_the_stream_survives() {
    let storage = fresh_storage().await;
    seed(
        &storage,
        "00000000-0000-0000-0000-000000000221",
        "00000000-0000-0000-0000-000000000222",
    )
    .await;
    let mut old = stream_on(
        Arc::clone(&storage),
        &["task_delivery", "completion", "heartbeat"],
    );
    let task = claim_via_demand(&mut old).await;

    // The task is reclaimed and re-claimed by a replacement generation.
    storage
        .reap_stale_worker_tasks(std::time::Duration::ZERO)
        .await
        .unwrap();
    let mut replacement = stream_on(Arc::clone(&storage), &["task_delivery"]);
    let reclaimed = claim_via_demand(&mut replacement).await;
    assert_eq!(reclaimed.claim_epoch, 2);

    // The old generation's late completion is fenced...
    old.dispatch(proto::worker_stream_client::Payload::Complete(
        proto::CompleteTaskRequest {
            task_id: task.id.to_string(),
            worker_id: "worker-a".into(),
            output_json: "{}".into(),
            claim_epoch: 1,
        },
    ))
    .await
    .expect("a fenced completion must not end the stream");
    // ...answered with a cancellation for that task only...
    let proto::worker_stream_server::Payload::Cancellation(cancelled) = old.next_frame() else {
        panic!("fenced completion must be answered with a per-task cancellation");
    };
    assert_eq!(cancelled.task_id, task.id.to_string());
    assert!(cancelled.reason.contains("completion rejected"));
    assert!(!old.outstanding.contains(&task.id));
    // ...and the stream still serves other frames.
    old.dispatch(proto::worker_stream_client::Payload::Demand(
        proto::WorkerStreamDemand { capacity: 1 },
    ))
    .await
    .unwrap();
    // The current generation's task is untouched.
    let stored = storage.get_worker_task(task.id).await.unwrap().unwrap();
    assert_eq!(stored.state, WorkerTaskState::Claimed);
    assert_eq!(stored.claim_epoch, 2);
}

#[tokio::test]
async fn demand_finds_the_session_runtime_even_among_thousands_of_newer_phones() {
    let storage = fresh_storage().await;
    let tenant = TenantId::unchecked("test");
    let mut stream = stream_on(
        Arc::clone(&storage),
        &["task_delivery", "runtime_capabilities"],
    );
    let runtime_id = Uuid::now_v7();
    stream
        .dispatch(proto::worker_stream_client::Payload::RuntimeHeartbeat(
            proto::RuntimeHeartbeat {
                runtime_capabilities_json: serde_json::json!({
                    "runtime_id": runtime_id,
                    "kind": "edge",
                    "trust": "attested",
                    "handlers": ["payments"],
                    "regions": ["br-south"],
                    "observed_at": "2020-01-01T00:00:00Z",
                    "expires_at": "2020-01-01T00:00:01Z"
                })
                .to_string(),
            },
        ))
        .await
        .unwrap();
    let _ack = stream.next_frame();

    // 1,100 phones observed after the session's runtime.
    for _ in 0..1_100 {
        let now = chrono::Utc::now();
        let phone = RuntimeCapabilities {
            runtime_id: RuntimeId::new(),
            kind: orch8_types::continuity::RuntimeKind::Mobile,
            trust: RuntimeTrustLevel::Registered,
            handlers: Vec::new(),
            plugins: Vec::new(),
            credentials: Vec::new(),
            regions: Vec::new(),
            hardware: Vec::new(),
            offline_capable: false,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            labels: std::collections::BTreeMap::new(),
            observed_at: now + chrono::Duration::seconds(1),
            expires_at: now + chrono::Duration::minutes(5),
        };
        storage
            .upsert_runtime_capabilities(&tenant, &phone)
            .await
            .unwrap();
    }

    // A placed task only this runtime's advertisement (region) satisfies.
    storage
        .create_sequence(&sequence_definition("00000000-0000-0000-0000-000000000231"))
        .await
        .unwrap();
    storage
        .create_instance(&task_instance(
            "00000000-0000-0000-0000-000000000232",
            "00000000-0000-0000-0000-000000000231",
        ))
        .await
        .unwrap();
    let mut task = pending_task("00000000-0000-0000-0000-000000000232", "payments");
    task.requirements.regions = vec!["br-south".into()];
    storage.create_worker_task(&task).await.unwrap();

    let claimed = claim_via_demand(&mut stream).await;
    assert_eq!(
        claimed.id, task.id,
        "the capped listing would have lost the runtime and delivered nothing"
    );
}
