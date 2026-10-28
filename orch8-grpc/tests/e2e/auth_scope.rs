//! Stored API-key grants must hold for the lifetime of an authenticated stream.
use super::*;
use orch8_types::api_key::{ApiCapability, NewApiKey, mint_scoped};

struct ScopedServer {
    client: Orch8ServiceClient<tonic::transport::Channel>,
    storage: Arc<SqliteStorage>,
    key: NewApiKey,
    shutdown: tokio_util::sync::CancellationToken,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Drop for ScopedServer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.server.abort();
    }
}
impl ScopedServer {
    async fn new(capability: ApiCapability, expire: bool) -> Self {
        let storage = Arc::new(
            SqliteStorage::in_memory()
                .await
                .unwrap()
                .with_artifact_store(Arc::new(
                    orch8_storage::artifacts::ObjectArtifactStore::memory(),
                )),
        );
        let key = mint_scoped(
            "test",
            "stream",
            expire.then(|| chrono::Utc::now() + chrono::Duration::seconds(2)),
            vec![capability],
        );
        orch8_storage::AdminStore::create_api_key(storage.as_ref(), &key.record)
            .await
            .unwrap();
        let shutdown = tokio_util::sync::CancellationToken::new();
        let service = Orch8GrpcService::new(storage.clone()).with_shutdown(shutdown.clone());
        let auth = orch8_grpc::auth::GrpcAuthLayer::new(
            storage.clone(),
            Some(orch8_types::auth::precompute_secret_digest("root")),
            true,
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stopping = shutdown.clone();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .layer(auth)
                .add_service(Orch8ServiceServer::new(service))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    stopping.cancelled_owned(),
                )
                .await
        });
        let client = Orch8ServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        Self {
            client,
            storage,
            key,
            shutdown,
            server,
        }
    }
    fn request<T>(&self, body: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(body);
        request
            .metadata_mut()
            .insert("x-api-key", self.key.secret.parse().unwrap());
        request
    }
    async fn revoke(&self) {
        assert!(
            orch8_storage::AdminStore::revoke_api_key(self.storage.as_ref(), &self.key.record.id)
                .await
                .unwrap()
        );
    }
}
fn worker_open() -> orch8_grpc::proto::WorkerStreamClient {
    worker_stream_frame(orch8_grpc::proto::worker_stream_client::Payload::Open(
        orch8_grpc::proto::WorkerStreamOpen {
            worker_id: "worker-key-lifetime".into(),
            handler_names: vec!["h".into()],
            supported_features: vec!["task_delivery".into()],
            max_in_flight: 1,
            protocol_version: 2,
            runtime_capabilities_json: String::new(),
            tenant_id: "test".into(),
        },
    ))
}
async fn assert_idle_worker_stream_loses_inactive_key(expire: bool) {
    let mut server = ScopedServer::new(ApiCapability::Worker, expire).await;
    let outbound = tokio_stream::StreamExt::chain(
        tokio_stream::iter([worker_open()]),
        tokio_stream::pending(),
    );
    let request = server.request(outbound);
    let mut inbound = server
        .client
        .worker_stream(request)
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().expect("hello");
    if !expire {
        server.revoke().await;
    }
    let observed = tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message()).await;
    assert!(
        matches!(observed,Ok(Err(ref status)) if status.code()==tonic::Code::Unauthenticated),
        "idle stream retained an inactive credential: {observed:?}"
    );
}
#[tokio::test]
async fn grpc_idle_worker_stream_closes_after_key_revocation() {
    assert_idle_worker_stream_loses_inactive_key(false).await;
}
#[tokio::test]
async fn grpc_idle_worker_stream_closes_after_key_expiry() {
    assert_idle_worker_stream_loses_inactive_key(true).await;
}
#[tokio::test]
async fn grpc_worker_stream_waiting_for_open_closes_after_key_expiry() {
    let mut server = ScopedServer::new(ApiCapability::Worker, true).await;
    let request = server.request(tokio_stream::pending::<orch8_grpc::proto::WorkerStreamClient>());
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(4),
        server.client.worker_stream(request),
    )
    .await;
    assert!(
        matches!(observed,Ok(Err(ref status)) if status.code()==tonic::Code::Unauthenticated),
        "pre-open stream retained an expired credential: {observed:?}"
    );
}
async fn assert_artifact_ack_wait_loses_inactive_key(expire: bool) {
    let mut server = ScopedServer::new(ApiCapability::Auditor, expire).await;
    let sequence_id = uuid::Uuid::now_v7();
    let instance_id = InstanceId::new();
    let sequence:orch8_types::sequence::SequenceDefinition=serde_json::from_value(serde_json::json!({
        "id":sequence_id,"tenant_id":"test","namespace":"default","name":"auth-artifact","version":1,"blocks":[{"type":"step","id":"work","handler":"noop","params":{}}],"created_at":chrono::Utc::now()
    })).unwrap();
    orch8_storage::SequenceStore::create_sequence(server.storage.as_ref(), &sequence)
        .await
        .unwrap();
    let instance:orch8_types::instance::TaskInstance=serde_json::from_value(serde_json::json!({
        "id":instance_id,"sequence_id":sequence_id,"tenant_id":"test","namespace":"default","state":"scheduled","priority":"Normal","timezone":"UTC","metadata":{},"context":{},"created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()
    })).unwrap();
    server.storage.create_instance(&instance).await.unwrap();
    let artifact = server
        .storage
        .put_artifact(
            instance_id,
            "application/octet-stream",
            vec![7; 8192].into(),
        )
        .await
        .unwrap();
    let open = orch8_grpc::proto::ArtifactTransferClient {
        payload: Some(orch8_grpc::proto::artifact_transfer_client::Payload::Open(
            orch8_grpc::proto::ArtifactTransferOpen {
                object_key: artifact.key,
                resume_offset: 0,
                chunk_bytes: 4096,
                expected_sha256: vec![],
                transfer_kind: "artifact".into(),
            },
        )),
    };
    let request = server.request(tokio_stream::StreamExt::chain(
        tokio_stream::iter([open]),
        tokio_stream::pending(),
    ));
    let mut inbound = server
        .client
        .artifact_transfer(request)
        .await
        .unwrap()
        .into_inner();
    inbound.message().await.unwrap().expect("hello");
    let chunk = inbound.message().await.unwrap().expect("chunk");
    assert!(matches!(
        chunk.payload,
        Some(orch8_grpc::proto::artifact_transfer_server::Payload::Chunk(
            _
        ))
    ));
    if !expire {
        server.revoke().await;
    }
    let observed = tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message()).await;
    assert!(
        matches!(observed,Ok(Err(ref status)) if status.code()==tonic::Code::Unauthenticated),
        "artifact ACK wait retained an inactive credential: {observed:?}"
    );
}
#[tokio::test]
async fn grpc_artifact_ack_wait_closes_after_key_revocation() {
    assert_artifact_ack_wait_loses_inactive_key(false).await;
}
#[tokio::test]
async fn grpc_artifact_ack_wait_closes_after_key_expiry() {
    assert_artifact_ack_wait_loses_inactive_key(true).await;
}
