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

async fn pending_issuer_offer(storage: &SqliteStorage) -> WorkerTask {
    let sequence_id = uuid::Uuid::now_v7();
    let instance_id = InstanceId::new();
    let sequence = serde_json::from_value(serde_json::json!({
        "id":sequence_id,"tenant_id":"test","namespace":"default","name":"issuer",
        "version":1,"blocks":[{"type":"step","id":"work","handler":"h","params":{}}],
        "created_at":chrono::Utc::now()
    }))
    .unwrap();
    orch8_storage::SequenceStore::create_sequence(storage, &sequence)
        .await
        .unwrap();
    let instance = serde_json::from_value(serde_json::json!({
        "id":instance_id,"sequence_id":sequence_id,"tenant_id":"test","namespace":"default",
        "state":"scheduled","priority":"Normal","timezone":"UTC","metadata":{},"context":{},
        "created_at":chrono::Utc::now(),"updated_at":chrono::Utc::now()
    }))
    .unwrap();
    storage.create_instance(&instance).await.unwrap();
    let task = pending_worker_task(instance_id, "work", "h", serde_json::json!({}));
    storage.create_worker_task(&task).await.unwrap();
    task
}

#[tokio::test]
async fn grpc_reconnect_filters_before_limit_and_preserves_frame_actor() {
    use orch8_grpc::proto::worker_stream_client::Payload as ClientPayload;
    use orch8_grpc::proto::worker_stream_server::Payload as ServerPayload;
    let mut server = ScopedServer::new(ApiCapability::Worker, false).await;
    let other = mint_scoped("test", "reconnect", None, vec![ApiCapability::Worker]);
    orch8_storage::AdminStore::create_api_key(server.storage.as_ref(), &other.record)
        .await
        .unwrap();
    let foreign = pending_issuer_offer(server.storage.as_ref()).await;
    let request = server.request(orch8_grpc::proto::PollTasksRequest {
        handler_name: "h".into(),
        worker_id: "worker-key-lifetime".into(),
        limit: 1,
    });
    server.client.poll_tasks(request).await.unwrap();
    let own = pending_issuer_offer(server.storage.as_ref()).await;
    let mut request = server.request(orch8_grpc::proto::PollTasksRequest {
        handler_name: "h".into(),
        worker_id: "worker-key-lifetime".into(),
        limit: 1,
    });
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    server.client.poll_tasks(request).await.unwrap();
    let (sender, receiver) = tokio::sync::mpsc::channel(4);
    let mut open = worker_open();
    let Some(ClientPayload::Open(ref mut config)) = open.payload else {
        panic!("fixture must open a worker stream")
    };
    config.supported_features.push("completion".into());
    sender.send(open).await.unwrap();
    let mut request = server.request(tokio_stream::wrappers::ReceiverStream::new(receiver));
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    let mut inbound = server
        .client
        .worker_stream(request)
        .await
        .unwrap()
        .into_inner();
    let hello = tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(hello.payload, Some(ServerPayload::Hello(_))));
    let frame = tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(ServerPayload::Task(delivery)) = frame.payload else {
        panic!("reconnect must deliver its own held task")
    };
    let held: WorkerTask = serde_json::from_str(&delivery.task_json).unwrap();
    assert_eq!(
        held.id, own.id,
        "an older foreign lease consumed max_in_flight=1 or leaked its payload"
    );
    sender
        .send(worker_stream_frame(ClientPayload::Complete(
            orch8_grpc::proto::CompleteTaskRequest {
                task_id: held.id.to_string(),
                worker_id: "worker-key-lifetime".into(),
                claim_epoch: held.claim_epoch,
                output_json: "{}".into(),
            },
        )))
        .await
        .unwrap();
    let frame = tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(frame.payload,Some(ServerPayload::Ack(ref ack)) if ack.operation=="complete" && ack.task_id==own.id.to_string()),
        "stream completion lost the authenticated actor: {frame:?}"
    );
    assert_eq!(
        server
            .storage
            .get_worker_task(foreign.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerTaskState::Claimed
    );
}

#[tokio::test]
async fn grpc_another_key_cannot_mutate_or_replay_a_recorded_lease() {
    let mut server = ScopedServer::new(ApiCapability::Worker, false).await;
    let other = mint_scoped("test", "replacement", None, vec![ApiCapability::Worker]);
    orch8_storage::AdminStore::create_api_key(server.storage.as_ref(), &other.record)
        .await
        .unwrap();
    let task = pending_issuer_offer(server.storage.as_ref()).await;
    let request = server.request(orch8_grpc::proto::PollTasksRequest {
        handler_name: "h".into(),
        worker_id: "shared-label".into(),
        limit: 1,
    });
    let claimed: WorkerTask = serde_json::from_str(
        &server
            .client
            .poll_tasks(request)
            .await
            .unwrap()
            .into_inner()
            .tasks_json[0],
    )
    .unwrap();
    let mut request = server.request(orch8_grpc::proto::HeartbeatTaskRequest {
        task_id: task.id.to_string(),
        worker_id: "shared-label".into(),
        claim_epoch: claimed.claim_epoch,
    });
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    assert!(
        matches!(server.client.heartbeat_task(request).await,Err(status) if status.code()==tonic::Code::PermissionDenied),
        "replacement key inherited the original lease"
    );
    let mut request = server.request(orch8_grpc::proto::CompleteTaskRequest {
        task_id: task.id.to_string(),
        worker_id: "shared-label".into(),
        claim_epoch: claimed.claim_epoch,
        output_json: "{}".into(),
    });
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    assert!(
        matches!(server.client.complete_task(request).await,Err(status) if status.code()==tonic::Code::PermissionDenied)
    );
    let mut request = server.request(orch8_grpc::proto::FailTaskRequest {
        task_id: task.id.to_string(),
        worker_id: "shared-label".into(),
        claim_epoch: claimed.claim_epoch,
        message: "foreign".into(),
        retryable: false,
    });
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    assert!(
        matches!(server.client.fail_task(request).await,Err(status) if status.code()==tonic::Code::PermissionDenied)
    );
    let mut request = server.request(orch8_grpc::proto::ReleaseTaskRequest {
        task_id: task.id.to_string(),
        worker_id: "shared-label".into(),
        claim_epoch: claimed.claim_epoch,
        started: false,
    });
    request
        .metadata_mut()
        .insert("x-api-key", other.secret.parse().unwrap());
    assert!(
        matches!(server.client.release_task(request).await,Err(status) if status.code()==tonic::Code::PermissionDenied)
    );
    let request = server.request(orch8_grpc::proto::CompleteTaskRequest {
        task_id: task.id.to_string(),
        worker_id: "shared-label".into(),
        claim_epoch: claimed.claim_epoch,
        output_json: "{\"value\":7}".into(),
    });
    server.client.complete_task(request).await.unwrap();
    server.storage.delete_worker_task(task.id).await.unwrap();
    for foreign in [true, false] {
        let mut request = server.request(orch8_grpc::proto::CompleteTaskRequest {
            task_id: task.id.to_string(),
            worker_id: "shared-label".into(),
            claim_epoch: claimed.claim_epoch,
            output_json: "{\"ignored\":true}".into(),
        });
        if foreign {
            request
                .metadata_mut()
                .insert("x-api-key", other.secret.parse().unwrap());
        }
        let response = server.client.complete_task(request).await;
        if foreign {
            assert!(matches!(response,Err(status) if status.code()==tonic::Code::PermissionDenied));
        } else {
            response.unwrap();
        }
    }
}

#[tokio::test]
async fn grpc_poll_and_stream_retain_the_verified_issuer() {
    use orch8_storage::worker_claim::WorkerClaimPrincipal;
    use orch8_types::{ids::TenantId, worker::WorkerClaim};
    for root in [false, true] {
        for streaming in [false, true] {
            let mut server = ScopedServer::new(ApiCapability::Worker, false).await;
            let task = pending_issuer_offer(server.storage.as_ref()).await;
            let claimed: WorkerTask = if streaming {
                let outbound = tokio_stream::iter([
                    worker_open(),
                    worker_stream_frame(orch8_grpc::proto::worker_stream_client::Payload::Demand(
                        orch8_grpc::proto::WorkerStreamDemand { capacity: 1 },
                    )),
                ]);
                let mut request = server.request(outbound);
                if root {
                    request
                        .metadata_mut()
                        .insert("x-api-key", "root".parse().unwrap());
                    request
                        .metadata_mut()
                        .insert("x-tenant-id", "test".parse().unwrap());
                }
                let mut inbound = server
                    .client
                    .worker_stream(request)
                    .await
                    .unwrap()
                    .into_inner();
                let hello =
                    tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                assert!(matches!(
                    hello.payload,
                    Some(orch8_grpc::proto::worker_stream_server::Payload::Hello(_))
                ));
                let frame =
                    tokio::time::timeout(std::time::Duration::from_secs(4), inbound.message())
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap();
                let Some(orch8_grpc::proto::worker_stream_server::Payload::Task(delivered)) =
                    frame.payload
                else {
                    panic!("demand must deliver a task")
                };
                serde_json::from_str(&delivered.task_json).unwrap()
            } else {
                let mut request = server.request(orch8_grpc::proto::PollTasksRequest {
                    handler_name: "h".into(),
                    worker_id: "worker-key-lifetime".into(),
                    limit: 1,
                });
                if root {
                    request
                        .metadata_mut()
                        .insert("x-api-key", "root".parse().unwrap());
                    request
                        .metadata_mut()
                        .insert("x-tenant-id", "test".parse().unwrap());
                }
                let response = server
                    .client
                    .poll_tasks(request)
                    .await
                    .unwrap()
                    .into_inner();
                serde_json::from_str(&response.tasks_json[0]).unwrap()
            };
            assert_eq!(claimed.id, task.id);
            let issued = server
                .storage
                .get_worker_issued_claim(
                    Some(&TenantId::unchecked("test")),
                    task.id,
                    &WorkerClaim::new("worker-key-lifetime", claimed.claim_epoch),
                )
                .await
                .unwrap()
                .unwrap();
            let expected = if root {
                WorkerClaimPrincipal::Root {}
            } else {
                WorkerClaimPrincipal::ApiKey {
                    tenant_id: TenantId::unchecked("test"),
                    key_id: server.key.record.id.clone(),
                }
            };
            assert_eq!(
                issued.principal, expected,
                "streaming={streaming}, root={root}"
            );
        }
    }
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
