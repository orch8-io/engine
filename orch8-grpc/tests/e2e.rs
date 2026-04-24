//! gRPC end-to-end smoke tests.

use std::net::SocketAddr;
use std::sync::Arc;

use orch8_grpc::proto::orch8_service_client::Orch8ServiceClient;
use orch8_grpc::proto::{CreateInstanceRequest, CreateSequenceRequest, GetInstanceRequest};
use orch8_grpc::{service::Orch8GrpcService, Orch8ServiceServer};
use orch8_storage::sqlite::SqliteStorage;

/// Spawn the gRPC server on an ephemeral port and return the bound address.
async fn spawn_test_server() -> SocketAddr {
    let storage = SqliteStorage::in_memory().await.expect("in-memory sqlite");
    let service = Orch8GrpcService::new(Arc::new(storage));

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
    addr
}

#[tokio::test]
async fn grpc_create_and_get_instance_smoke() {
    let addr = spawn_test_server().await;
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
