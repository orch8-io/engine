use super::*;
use orch8_types::api_key::{ApiCapability, mint_scoped};
use tower::ServiceExt as _;

#[tokio::test]
async fn scoped_key_cannot_reach_rpc_outside_its_grant() {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    for (capability, methods) in [
        (
            ApiCapability::Auditor,
            vec![
                "PollTasks",
                "CompleteTask",
                "WorkerStream",
                "UpdateContext",
                "CreateSequence",
                "IngestTelemetryBatch",
            ],
        ),
        (
            ApiCapability::Worker,
            vec![
                "CreateInstance",
                "UpdateState",
                "SendSignal",
                "CreatePool",
                "ArtifactTransfer",
            ],
        ),
        (
            ApiCapability::Publisher,
            vec!["PollTasks", "CreateInstance", "UpdateContext"],
        ),
        (
            ApiCapability::Approver,
            vec!["PollTasks", "CreateSequence", "UpdateState"],
        ),
        (
            ApiCapability::Device,
            vec!["PollTasks", "CreateInstance", "CreateSequence"],
        ),
    ] {
        let key = mint_scoped("acme", "restricted", None, vec![capability]);
        storage.create_api_key(&key.record).await.unwrap();
        for method in methods {
            let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let observed = Arc::clone(&called);
            let inner = tower::service_fn(move |_| {
                observed.store(true, std::sync::atomic::Ordering::SeqCst);
                async {
                    Ok::<_, std::convert::Infallible>(http::Response::new(
                        tonic::body::Body::empty(),
                    ))
                }
            });
            let layer = GrpcAuthLayer::new(
                Arc::clone(&storage),
                Some(orch8_types::auth::precompute_secret_digest("root")),
                true,
            );
            let response = layer
                .layer(inner)
                .oneshot(
                    http::Request::builder()
                        .uri(format!("/orch8.Orch8Service/{method}"))
                        .header("x-api-key", &key.secret)
                        .body(tonic::body::Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response
                    .headers()
                    .get("grpc-status")
                    .and_then(|v| v.to_str().ok()),
                Some("7"),
                "{capability:?} reached {method}"
            );
            assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
        }
    }
}

#[test]
fn capability_matrix_classifies_the_actual_proto_surface() {
    let methods: Vec<_> = include_str!("../../proto/orch8.proto")
        .lines()
        .filter_map(|line| line.trim().strip_prefix("rpc "))
        .map(|line| line.split('(').next().unwrap())
        .collect();
    assert_eq!(
        methods.len(),
        37,
        "review capability policy when RPC surface changes"
    );
    for (capability, allowed) in [
        (
            ApiCapability::Worker,
            vec![
                "Health",
                "PollTasks",
                "CompleteTask",
                "FailTask",
                "HeartbeatTask",
                "ReleaseTask",
                "WorkerStream",
            ],
        ),
        (
            ApiCapability::Publisher,
            vec![
                "Health",
                "CreateSequence",
                "GetSequence",
                "GetSequenceByName",
            ],
        ),
        (ApiCapability::Approver, vec!["Health", "SendSignal"]),
        (ApiCapability::Device, vec!["Health"]),
        (
            ApiCapability::Auditor,
            vec![
                "Health",
                "GetSequence",
                "GetSequenceByName",
                "GetInstance",
                "ListInstances",
                "GetOutputs",
                "GetExecutionTree",
                "GetCron",
                "ListCron",
                "GetPool",
                "ListPools",
                "ListResources",
                "ArtifactTransfer",
            ],
        ),
        (ApiCapability::BrowserWorker, vec![]),
        (ApiCapability::DeviceNode, vec![]),
    ] {
        for method in &methods {
            let path = format!("/orch8.Orch8Service/{method}");
            assert_eq!(
                capabilities_allow(&[capability], &path),
                allowed.contains(method),
                "{capability:?} {method}"
            );
            assert!(!capabilities_allow(&[], &path));
            assert!(capabilities_allow(&[ApiCapability::Operator], &path));
        }
        assert!(!capabilities_allow(
            &[capability],
            "/orch8.Orch8Service/GetFutureSensitiveData"
        ));
        assert!(!capabilities_allow(&[capability], "/other.Service/Health"));
    }
    assert!(capabilities_allow(
        &[ApiCapability::Worker, ApiCapability::Auditor],
        "/orch8.Orch8Service/GetInstance"
    ));
    assert!(capabilities_allow(
        &[ApiCapability::Worker, ApiCapability::Auditor],
        "/orch8.Orch8Service/PollTasks"
    ));
}

#[tokio::test]
async fn allowed_rpc_carries_verified_key_identity_and_grant() {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let key = mint_scoped("acme", "worker", None, vec![ApiCapability::Worker]);
    storage.create_api_key(&key.record).await.unwrap();
    let expected = key.record.clone();
    let inner = tower::service_fn(move |req: http::Request<tonic::body::Body>| {
        let principal = req.extensions().get::<CallerPrincipal>().unwrap();
        assert_eq!(principal.record.id, expected.id);
        assert_eq!(principal.record.tenant_id, "acme");
        assert_eq!(principal.record.capabilities, vec![ApiCapability::Worker]);
        assert!(!format!("{principal:?}").contains(&expected.key_hash));
        async { Ok::<_, std::convert::Infallible>(http::Response::new(tonic::body::Body::empty())) }
    });
    let layer = GrpcAuthLayer::new(
        storage,
        Some(orch8_types::auth::precompute_secret_digest("root")),
        true,
    );
    let response = layer
        .layer(inner)
        .oneshot(
            http::Request::builder()
                .uri("/orch8.Orch8Service/PollTasks")
                .header("x-api-key", &key.secret)
                .body(tonic::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert!(response.headers().get("grpc-status").is_none());
}

#[tokio::test]
async fn retained_principal_reauthorization_rejects_revocation_and_expiry() {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let key = mint_scoped("acme", "worker", None, vec![ApiCapability::Worker]);
    storage.create_api_key(&key.record).await.unwrap();
    let principal = CallerPrincipal {
        record: key.record.clone(),
    };
    let path = "/orch8.Orch8Service/WorkerStream";
    principal.reauthorize(&storage, path).await.unwrap();
    assert!(storage.revoke_api_key(&key.record.id).await.unwrap());
    assert_eq!(
        principal
            .reauthorize(&storage, path)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
    let key = mint_scoped(
        "acme",
        "expiring",
        Some(chrono::Utc::now() + chrono::Duration::seconds(2)),
        vec![ApiCapability::Worker],
    );
    storage.create_api_key(&key.record).await.unwrap();
    let principal = CallerPrincipal { record: key.record };
    principal.reauthorize(&storage, path).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(2100)).await;
    assert_eq!(
        principal
            .reauthorize(&storage, path)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unauthenticated
    );
}
