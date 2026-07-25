//! gRPC end-to-end smoke tests.

use std::net::SocketAddr;
use std::sync::Arc;

use orch8_grpc::proto::orch8_service_client::Orch8ServiceClient;
use orch8_grpc::proto::{
    CreateInstanceRequest, CreateSequenceRequest, GetInstanceRequest, IngestTelemetryBatchRequest,
    RetryInstanceRequest, SendSignalRequest, TelemetryEventInput,
};
use orch8_grpc::{Orch8ServiceServer, service::Orch8GrpcService};
use orch8_storage::WorkerStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_storage::{ContinuityStore, InstanceStore, ResourceStore};
use orch8_types::continuity::RuntimeTrustLevel;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;
use orch8_types::worker::{WorkerTask, WorkerTaskState};

/// Spawn the gRPC server on an ephemeral port; return the bound address and
/// a handle to the storage so tests can force states the RPC surface
/// validates against (e.g. driving an instance to `Failed`).
async fn spawn_test_server() -> (SocketAddr, Arc<SqliteStorage>) {
    let storage = Arc::new(
        SqliteStorage::in_memory()
            .await
            .expect("in-memory sqlite")
            .with_artifact_store(Arc::new(
                orch8_storage::artifacts::ObjectArtifactStore::memory(),
            )),
    );
    let service = Orch8GrpcService::new(storage.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(Orch8ServiceServer::new(service))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("server died");
    });

    // Give the server a moment to start accepting.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    (addr, storage)
}

#[tokio::test]
async fn grpc_telemetry_batch_accepts_valid_events_and_reports_partial_rejections() {
    let (addr, _storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to test server");
    let event = |event_type: &str, payload_json: &str, created_at: &str| TelemetryEventInput {
        event_type: event_type.into(),
        payload_json: payload_json.into(),
        device_id: "device-1".into(),
        os_name: "linux".into(),
        os_version: "6.12".into(),
        app_version: "1.0.0".into(),
        sdk_version: "1.0.0".into(),
        created_at: created_at.into(),
    };
    let response = client
        .ingest_telemetry_batch(IngestTelemetryBatchRequest {
            tenant_id: "test".into(),
            events: vec![
                event(
                    "worker.started",
                    r#"{"worker":"alpha"}"#,
                    "2026-07-25T10:00:00Z",
                ),
                event("worker.bad", "not-json", "2026-07-25T10:00:01Z"),
                event("worker.ready", r#"{"ready":true}"#, "not-a-timestamp"),
            ],
        })
        .await
        .expect("ingest telemetry")
        .into_inner();

    assert_eq!(response.accepted, 1);
    assert_eq!(response.rejected.len(), 2);
    assert_eq!(response.rejected[0].index, 1);
    assert_eq!(response.rejected[0].code, "invalid_payload_json");
    assert_eq!(response.rejected[1].index, 2);
    assert_eq!(response.rejected[1].code, "invalid_created_at");
}

#[tokio::test]
async fn grpc_create_and_get_instance_smoke() {
    let (addr, _storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to test server");

    // 1. Create a sequence
    let seq_def = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000001",
        "tenant_id": "test",
        "namespace": "default",
        "name": "smoke_seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "step_1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "created_at": "2024-01-01T00:00:00Z"
    });

    let create_seq = CreateSequenceRequest {
        definition_json: seq_def.to_string(),
    };
    let seq_resp = client
        .create_sequence(create_seq)
        .await
        .expect("create sequence");
    let seq_body: serde_json::Value =
        serde_json::from_str(&seq_resp.into_inner().definition_json).unwrap();
    assert_eq!(seq_body["name"], "smoke_seq");

    // 2. Create an instance
    let inst_def = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000002",
        "sequence_id": "00000000-0000-0000-0000-000000000001",
        "tenant_id": "test",
        "namespace": "default",
        "state": "scheduled",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": {
            "data": {},
            "config": {},
            "audit": [],
            "runtime": {}
        },
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    });

    let create_inst = CreateInstanceRequest {
        instance_json: inst_def.to_string(),
    };
    let inst_resp = client
        .create_instance(create_inst)
        .await
        .expect("create instance");
    let inst_body: serde_json::Value =
        serde_json::from_str(&inst_resp.into_inner().instance_json).unwrap();
    assert_eq!(inst_body["tenant_id"], "test");

    // 3. Get the instance back
    let get_inst = GetInstanceRequest {
        id: "00000000-0000-0000-0000-000000000002".into(),
    };
    let get_resp = client.get_instance(get_inst).await.expect("get instance");
    let got: serde_json::Value =
        serde_json::from_str(&get_resp.into_inner().instance_json).unwrap();
    assert_eq!(got["id"], "00000000-0000-0000-0000-000000000002");
    drop(client);
}

#[tokio::test]
async fn grpc_create_instance_rejects_foreign_sequence() {
    let (addr, _storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to test server");

    // Sequence owned by tenant-a.
    let seq_def = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000003",
        "tenant_id": "tenant-a",
        "namespace": "default",
        "name": "private_seq",
        "version": 1,
        "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {} }],
        "created_at": "2024-01-01T00:00:00Z"
    });
    client
        .create_sequence(CreateSequenceRequest {
            definition_json: seq_def.to_string(),
        })
        .await
        .expect("create sequence");

    // Instance claims to be tenant-b but references tenant-a's sequence.
    let inst_def = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000004",
        "sequence_id": "00000000-0000-0000-0000-000000000003",
        "tenant_id": "tenant-b",
        "namespace": "default",
        "state": "scheduled",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": { "data": {}, "config": {}, "audit": [], "runtime": {} },
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    });
    let err = client
        .create_instance(CreateInstanceRequest {
            instance_json: inst_def.to_string(),
        })
        .await
        .expect_err("foreign sequence must be rejected");
    assert_eq!(err.code(), tonic::Code::NotFound);
}

/// Create a sequence + instance pair (both under tenant `test`) and return
/// the instance id string. Shared boilerplate for the lifecycle tests below.
async fn create_seq_and_instance(
    client: &mut Orch8ServiceClient<tonic::transport::Channel>,
    seq_id: &str,
    inst_id: &str,
) {
    let seq_def = serde_json::json!({
        "id": seq_id,
        "tenant_id": "test",
        "namespace": "default",
        "name": format!("seq_{seq_id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "step_1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "created_at": "2024-01-01T00:00:00Z"
    });
    client
        .create_sequence(CreateSequenceRequest {
            definition_json: seq_def.to_string(),
        })
        .await
        .expect("create sequence");

    let inst_def = serde_json::json!({
        "id": inst_id,
        "sequence_id": seq_id,
        "tenant_id": "test",
        "namespace": "default",
        "state": "scheduled",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": { "data": {}, "config": {}, "audit": [], "runtime": {} },
        "created_at": "2024-01-01T00:00:00Z",
        "updated_at": "2024-01-01T00:00:00Z"
    });
    client
        .create_instance(CreateInstanceRequest {
            instance_json: inst_def.to_string(),
        })
        .await
        .expect("create instance");
}

/// Regression: gRPC retry must reset the run identity (`run_id`, step
/// counters) like the HTTP path does — previously it only flipped the state
/// back to `scheduled`, correlating the new run to the failed one.
#[tokio::test]
async fn grpc_retry_instance_resets_run_state() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000005";
    const INST: &str = "00000000-0000-0000-0000-000000000006";
    let (addr, storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to test server");

    create_seq_and_instance(&mut client, SEQ, INST).await;

    // Drive the instance to Failed directly (the RPC surface validates
    // transitions and would reject scheduled -> failed from a client).
    let iid = InstanceId::from_uuid(uuid::Uuid::parse_str(INST).unwrap());
    storage
        .update_instance_state(iid, InstanceState::Failed, None)
        .await
        .expect("force failed state");

    let resp = client
        .retry_instance(RetryInstanceRequest { id: INST.into() })
        .await
        .expect("retry instance");
    let body: serde_json::Value = serde_json::from_str(&resp.into_inner().instance_json).unwrap();
    assert_eq!(body["state"], "scheduled");
    // reset_instance_run stamps a fresh run id and zeroes the step counter.
    assert!(
        body["context"]["runtime"]["run_id"].is_string(),
        "retry must stamp a fresh run_id, got: {}",
        body["context"]["runtime"]
    );
    assert_eq!(body["context"]["runtime"]["total_steps_executed"], 0);
}

/// Regression: `SendSignalRequest.instance_id` must agree with the
/// `instance_id` embedded in the signal JSON — a mismatch used to be
/// silently redirected to the JSON value's target.
#[tokio::test]
async fn grpc_send_signal_validates_instance_id_field() {
    const SEQ: &str = "00000000-0000-0000-0000-000000000007";
    const INST: &str = "00000000-0000-0000-0000-000000000008";
    let (addr, _storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect to test server");

    create_seq_and_instance(&mut client, SEQ, INST).await;

    let signal_json = serde_json::json!({
        "id": "00000000-0000-0000-0000-000000000009",
        "instance_id": INST,
        "signal_type": "pause",
        "payload": {},
        "delivered": false,
        "created_at": "2024-01-01T00:00:00Z",
        "delivered_at": null
    })
    .to_string();

    // Mismatched field -> InvalidArgument.
    let err = client
        .send_signal(SendSignalRequest {
            instance_id: "00000000-0000-0000-0000-0000000000aa".into(),
            signal_json: signal_json.clone(),
        })
        .await
        .expect_err("mismatched instance_id must be rejected");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // Matching field -> accepted.
    client
        .send_signal(SendSignalRequest {
            instance_id: INST.into(),
            signal_json,
        })
        .await
        .expect("matching instance_id must be accepted");
}

fn worker_stream_frame(
    payload: orch8_grpc::proto::worker_stream_client::Payload,
) -> orch8_grpc::proto::WorkerStreamClient {
    orch8_grpc::proto::WorkerStreamClient {
        payload: Some(payload),
    }
}

#[tokio::test]
async fn grpc_worker_stream_negotiates_bounds_and_delivers_on_demand() {
    use orch8_grpc::proto::worker_stream_client::Payload as ClientPayload;
    use orch8_grpc::proto::worker_stream_server::Payload as ServerPayload;

    let (addr, storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let sequence_id = "00000000-0000-0000-0000-000000000011";
    let instance_id = "00000000-0000-0000-0000-000000000012";
    create_seq_and_instance(&mut client, sequence_id, instance_id).await;
    let task = WorkerTask {
        id: uuid::Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(uuid::Uuid::parse_str(instance_id).unwrap()),
        block_id: orch8_types::ids::BlockId::new("step"),
        handler_name: "payments".into(),
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
    };
    storage.create_worker_task(&task).await.unwrap();
    let outbound = tokio_stream::iter(vec![
        worker_stream_frame(ClientPayload::Open(orch8_grpc::proto::WorkerStreamOpen {
            worker_id: "worker-a".into(),
            handler_names: vec!["payments".into()],
            supported_features: vec!["task_delivery".into(), "heartbeat".into()],
            max_in_flight: 10_000,
            protocol_version: 1,
            runtime_capabilities_json: String::new(),
            tenant_id: "test".into(),
        })),
        worker_stream_frame(ClientPayload::Demand(
            orch8_grpc::proto::WorkerStreamDemand { capacity: 1 },
        )),
    ]);
    let mut inbound = client.worker_stream(outbound).await.unwrap().into_inner();

    let hello = inbound.message().await.unwrap().unwrap();
    let Some(ServerPayload::Hello(hello)) = hello.payload else {
        panic!("first server frame must be hello");
    };
    assert_eq!(hello.protocol_version, 1);
    assert_eq!(hello.max_in_flight, 256);
    assert_eq!(hello.negotiated_features, ["task_delivery", "heartbeat"]);
    let delivered = inbound.message().await.unwrap().unwrap();
    let Some(ServerPayload::Task(delivered)) = delivered.payload else {
        panic!("demand must produce a task frame");
    };
    let claimed: WorkerTask = serde_json::from_str(&delivered.task_json).unwrap();
    assert_eq!(claimed.id, task.id);
    assert_eq!(claimed.worker_id.as_deref(), Some("worker-a"));
}

#[tokio::test]
async fn grpc_worker_stream_rejects_unsupported_protocol() {
    use orch8_grpc::proto::worker_stream_client::Payload as ClientPayload;

    let (addr, _storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let outbound = tokio_stream::iter([worker_stream_frame(ClientPayload::Open(
        orch8_grpc::proto::WorkerStreamOpen {
            worker_id: "worker-a".into(),
            handler_names: vec!["payments".into()],
            supported_features: vec!["task_delivery".into()],
            max_in_flight: 1,
            protocol_version: 99,
            runtime_capabilities_json: String::new(),
            tenant_id: "test".into(),
        },
    ))]);
    let error = client.worker_stream(outbound).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn grpc_runtime_session_persists_capabilities_streams_commands_and_drains() {
    use orch8_grpc::proto::worker_stream_client::Payload as ClientPayload;
    use orch8_grpc::proto::worker_stream_server::Payload as ServerPayload;
    use orch8_types::worker::{WorkerCommand, WorkerCommandKind};

    let (addr, storage) = spawn_test_server().await;
    let command = WorkerCommand {
        id: uuid::Uuid::now_v7(),
        worker_id: "runtime-worker".into(),
        command: WorkerCommandKind::Place,
        payload: serde_json::json!({"instance_id": "instance-1", "target": "edge"}),
        created_at: chrono::Utc::now(),
    };
    storage.enqueue_worker_command(&command).await.unwrap();
    let runtime_id = uuid::Uuid::now_v7();
    let capabilities = |draining: bool| {
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
    };
    let outbound = tokio_stream::iter(vec![
        worker_stream_frame(ClientPayload::Open(orch8_grpc::proto::WorkerStreamOpen {
            worker_id: "runtime-worker".into(),
            handler_names: vec!["payments".into()],
            supported_features: vec![
                "task_delivery".into(),
                "runtime_capabilities".into(),
                "draining".into(),
                "placement_commands".into(),
            ],
            max_in_flight: 4,
            protocol_version: 1,
            runtime_capabilities_json: capabilities(false),
            tenant_id: "test".into(),
        })),
        worker_stream_frame(ClientPayload::CommandAck(
            orch8_grpc::proto::WorkerCommandAck {
                command_id: command.id.to_string(),
            },
        )),
        worker_stream_frame(ClientPayload::RuntimeHeartbeat(
            orch8_grpc::proto::RuntimeHeartbeat {
                runtime_capabilities_json: capabilities(true),
            },
        )),
        worker_stream_frame(ClientPayload::Demand(
            orch8_grpc::proto::WorkerStreamDemand { capacity: 1 },
        )),
    ]);
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let mut inbound = client.worker_stream(outbound).await.unwrap().into_inner();

    assert!(matches!(
        inbound.message().await.unwrap().unwrap().payload,
        Some(ServerPayload::Hello(_))
    ));
    let streamed = inbound.message().await.unwrap().unwrap();
    let Some(ServerPayload::Command(streamed)) = streamed.payload else {
        panic!("durable placement command must follow hello");
    };
    let streamed: WorkerCommand = serde_json::from_str(&streamed.command_json).unwrap();
    assert_eq!(streamed.id, command.id);
    assert_eq!(streamed.command, WorkerCommandKind::Place);
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().payload,
        Some(ServerPayload::Ack(_))
    ));
    assert!(matches!(
        inbound.message().await.unwrap().unwrap().payload,
        Some(ServerPayload::Ack(_))
    ));
    let drain_error = inbound.message().await.unwrap_err();
    assert_eq!(drain_error.code(), tonic::Code::FailedPrecondition);

    assert!(
        storage
            .list_worker_commands("runtime-worker")
            .await
            .unwrap()
            .is_empty()
    );
    let persisted = storage
        .list_runtime_capabilities(
            &orch8_types::ids::TenantId::unchecked("test"),
            chrono::Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    assert_eq!(persisted.len(), 1);
    assert_eq!(persisted[0].runtime_id.to_string(), runtime_id.to_string());
    assert_eq!(persisted[0].trust, RuntimeTrustLevel::Registered);
    assert!(persisted[0].draining);
    assert!(persisted[0].expires_at > chrono::Utc::now());
}

#[tokio::test]
async fn grpc_artifact_transfer_resumes_with_chunk_acknowledgements() {
    use orch8_grpc::proto::artifact_transfer_client::Payload as ClientPayload;
    use orch8_grpc::proto::artifact_transfer_server::Payload as ServerPayload;
    use sha2::{Digest, Sha256};

    let (addr, storage) = spawn_test_server().await;
    let mut client = Orch8ServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let sequence_id = "00000000-0000-0000-0000-000000000021";
    let instance_id = "00000000-0000-0000-0000-000000000022";
    create_seq_and_instance(&mut client, sequence_id, instance_id).await;
    let instance_id = InstanceId::from_uuid(uuid::Uuid::parse_str(instance_id).unwrap());
    let original: Vec<u8> = (0_u32..10_000)
        .map(|value| u8::try_from(value % 251).unwrap())
        .collect();
    let artifact = storage
        .put_artifact(
            instance_id,
            "application/octet-stream",
            original.clone().into(),
        )
        .await
        .unwrap();

    let (ack_sender, outbound_frames) = tokio::sync::mpsc::channel(2);
    ack_sender
        .send(orch8_grpc::proto::ArtifactTransferClient {
            payload: Some(ClientPayload::Open(
                orch8_grpc::proto::ArtifactTransferOpen {
                    object_key: artifact.key,
                    resume_offset: 4096,
                    chunk_bytes: 4096,
                    expected_sha256: Sha256::digest(&original).to_vec(),
                    transfer_kind: "continuity".into(),
                },
            )),
        })
        .await
        .unwrap();
    let mut inbound = client
        .artifact_transfer(tokio_stream::wrappers::ReceiverStream::new(outbound_frames))
        .await
        .unwrap()
        .into_inner();
    let hello = inbound.message().await.unwrap().unwrap();
    let Some(ServerPayload::Hello(hello)) = hello.payload else {
        panic!("first transfer frame must be hello");
    };
    assert_eq!(hello.resume_offset, 4096);
    assert_eq!(hello.total_bytes, 10_000);

    let mut reconstructed = Vec::new();
    loop {
        let frame = inbound.message().await.unwrap().unwrap();
        let Some(ServerPayload::Chunk(chunk)) = frame.payload else {
            panic!("expected chunk");
        };
        assert_eq!(chunk.sha256, Sha256::digest(&chunk.data).to_vec());
        reconstructed.extend_from_slice(&chunk.data);
        let next_offset = chunk.offset + u64::try_from(chunk.data.len()).unwrap();
        ack_sender
            .send(orch8_grpc::proto::ArtifactTransferClient {
                payload: Some(ClientPayload::Ack(orch8_grpc::proto::ArtifactTransferAck {
                    next_offset,
                })),
            })
            .await
            .unwrap();
        if chunk.final_chunk {
            break;
        }
    }
    assert_eq!(reconstructed, original[4096..]);
}
