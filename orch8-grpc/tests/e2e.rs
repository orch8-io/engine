//! gRPC end-to-end smoke tests.

use std::net::SocketAddr;
use std::sync::Arc;

use orch8_grpc::proto::orch8_service_client::Orch8ServiceClient;
use orch8_grpc::proto::{
    CreateInstanceRequest, CreateSequenceRequest, GetInstanceRequest, RetryInstanceRequest,
    SendSignalRequest,
};
use orch8_grpc::{Orch8ServiceServer, service::Orch8GrpcService};
use orch8_storage::InstanceStore;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;

/// Spawn the gRPC server on an ephemeral port; return the bound address and
/// a handle to the storage so tests can force states the RPC surface
/// validates against (e.g. driving an instance to `Failed`).
async fn spawn_test_server() -> (SocketAddr, Arc<SqliteStorage>) {
    let storage = Arc::new(SqliteStorage::in_memory().await.expect("in-memory sqlite"));
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
