//! Runtime control sessions: a runtime may describe itself but never elevate
//! itself. Capability reports are bounded (list sizes, entry lengths, labels,
//! signing-key size), must cover every handler the session serves, cannot
//! swap the session's `runtime_id`, and always persist with server-pinned
//! `Registered` trust and a server-stamped lease. Durable worker commands are
//! streamed in order to the owning worker of the owning tenant only, and an
//! ack deletes exactly the acknowledged command.

use super::*;

use orch8_types::worker::{WorkerCommand, WorkerCommandKind};
use tokio::sync::mpsc;

const RUNTIME: &str = "123e4567-e89b-42d3-a456-426614174000";

fn runtime_id() -> RuntimeId {
    RuntimeId::from_uuid(Uuid::parse_str(RUNTIME).unwrap())
}

fn capabilities_value() -> serde_json::Value {
    serde_json::json!({
        "runtime_id": RUNTIME,
        "kind": "edge",
        "trust": "attested",
        "handlers": ["payments"],
        "plugins": ["card-reader"],
        "credentials": ["vault/payments"],
        "regions": ["br-south"],
        "hardware": ["secure-enclave"],
        "offline_capable": true,
        "connectivity": "ethernet",
        "battery_percent": 88,
        "draining": false,
        "observed_at": "2020-01-01T00:00:00Z",
        "expires_at": "2020-01-01T00:00:01Z"
    })
}

fn capabilities_json() -> String {
    capabilities_value().to_string()
}

fn handlers() -> Vec<String> {
    vec!["payments".to_owned()]
}

#[test]
fn runtime_list_bounds_allow_64_entries_of_256_bytes() {
    let cases: [(&str, Vec<String>, bool); 6] = [
        ("empty list", vec![], true),
        (
            "64 entries",
            (0..64).map(|i| format!("entry-{i}")).collect(),
            true,
        ),
        (
            "65 entries",
            (0..65).map(|i| format!("entry-{i}")).collect(),
            false,
        ),
        ("empty entry", vec![String::new()], false),
        ("256-byte entry", vec!["e".repeat(256)], true),
        ("257-byte entry", vec!["e".repeat(257)], false),
    ];
    for (case, values, valid) in cases {
        assert_eq!(bounded_runtime_list(&values), valid, "case: {case}");
    }
}

#[test]
fn out_of_bounds_capability_reports_are_invalid_argument() {
    let many = |prefix: &str, n: usize| {
        serde_json::json!((0..n).map(|i| format!("{prefix}-{i}")).collect::<Vec<_>>())
    };
    let mut handlers_over_limit = vec!["payments".to_owned()];
    handlers_over_limit.extend((0..64).map(|i| format!("handler-{i}")));
    let too_many_labels: serde_json::Map<String, serde_json::Value> = (0..33)
        .map(|i| (format!("k{i}"), serde_json::json!("v")))
        .collect();
    let cases: Vec<(&str, &str, serde_json::Value)> = vec![
        (
            "session handler not advertised",
            "handlers",
            serde_json::json!(["billing"]),
        ),
        (
            "65 handlers",
            "handlers",
            serde_json::json!(handlers_over_limit),
        ),
        (
            "empty handler entry",
            "handlers",
            serde_json::json!(["payments", ""]),
        ),
        ("65 plugins", "plugins", many("plugin", 65)),
        ("empty credential", "credentials", serde_json::json!([""])),
        (
            "257-byte region",
            "regions",
            serde_json::json!(["r".repeat(257)]),
        ),
        ("65 hardware entries", "hardware", many("hw", 65)),
        (
            "1025-byte capsule key",
            "capsule_signing_public_key",
            serde_json::json!("k".repeat(1_025)),
        ),
        (
            "33 labels",
            "labels",
            serde_json::Value::Object(too_many_labels),
        ),
        (
            "blank label value",
            "labels",
            serde_json::json!({"gpu": " "}),
        ),
        (
            "129-byte label key",
            "labels",
            serde_json::json!({ "k".repeat(129): "v" }),
        ),
    ];
    for (case, field, value) in cases {
        let mut report = capabilities_value();
        report[field] = value;
        let status =
            prepare_runtime_capabilities(&report.to_string(), &handlers(), None).expect_err(case);
        assert_eq!(status.code(), tonic::Code::InvalidArgument, "case: {case}");
    }
}

#[test]
fn malformed_capabilities_json_is_invalid_argument() {
    let status = prepare_runtime_capabilities("{not json", &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn reports_at_the_bounds_are_accepted() {
    let mut value = capabilities_value();
    value["handlers"] = serde_json::json!(["billing", "payments", "email"]);
    value["capsule_signing_public_key"] = serde_json::json!("k".repeat(1_024));
    value["labels"] = serde_json::json!({ "gpu": "a".repeat(128) });
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    assert_eq!(
        capabilities.capsule_signing_public_key.map(|key| key.len()),
        Some(1_024)
    );
    assert_eq!(capabilities.labels["gpu"].len(), 128);
}

#[test]
fn self_reported_trust_is_pinned_to_registered() {
    for trust in ["unverified", "registered", "signed", "attested"] {
        let mut value = capabilities_value();
        value["trust"] = serde_json::Value::String(trust.into());
        let capabilities =
            prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
        assert_eq!(
            capabilities.trust,
            RuntimeTrustLevel::Registered,
            "reported trust: {trust}"
        );
    }
}

#[test]
fn client_supplied_lease_is_replaced_by_a_server_stamped_lease() {
    let before = chrono::Utc::now();
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    let after = chrono::Utc::now();
    assert!(capabilities.observed_at >= before && capabilities.observed_at <= after);
    assert_eq!(
        capabilities.expires_at - capabilities.observed_at,
        chrono::Duration::seconds(RUNTIME_CAPABILITY_LEASE_SECS)
    );
}

#[test]
fn heartbeat_for_the_session_runtime_id_is_accepted() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), Some(runtime_id()))
            .unwrap();
    assert_eq!(capabilities.runtime_id, runtime_id());
}

#[test]
fn heartbeat_that_swaps_runtime_id_is_permission_denied() {
    let other = RuntimeId::from_uuid(Uuid::now_v7());
    let status =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), Some(other)).unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[test]
fn draining_report_is_preserved() {
    let mut value = capabilities_value();
    value["draining"] = serde_json::json!(true);
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    assert!(capabilities.draining);
}

// --- persist_runtime_capabilities against in-memory storage ---

async fn service_and_storage() -> (Orch8GrpcService, Arc<dyn StorageBackend>) {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    (Orch8GrpcService::new(Arc::clone(&storage)), storage)
}

fn test_tenant() -> TenantId {
    TenantId::unchecked("test")
}

async fn persisted(storage: &Arc<dyn StorageBackend>) -> Vec<RuntimeCapabilities> {
    storage
        .list_runtime_capabilities(
            &test_tenant(),
            chrono::Utc::now() - chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn persisting_capabilities_without_a_tenant_is_failed_precondition() {
    let (service, _storage) = service_and_storage().await;
    let status = service
        .persist_runtime_capabilities(&capabilities_json(), &handlers(), None, None)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn second_heartbeat_upserts_the_same_runtime_with_pinned_trust() {
    let (service, storage) = service_and_storage().await;
    service
        .persist_runtime_capabilities(
            &capabilities_json(),
            &handlers(),
            Some(&test_tenant()),
            None,
        )
        .await
        .unwrap();
    let mut draining = capabilities_value();
    draining["draining"] = serde_json::json!(true);
    service
        .persist_runtime_capabilities(
            &draining.to_string(),
            &handlers(),
            Some(&test_tenant()),
            Some(runtime_id()),
        )
        .await
        .unwrap();
    let rows = persisted(&storage).await;
    assert_eq!(rows.len(), 1);
    assert!(rows[0].draining);
    assert_eq!(rows[0].trust, RuntimeTrustLevel::Registered);
    assert!(rows[0].expires_at > chrono::Utc::now());
}

#[tokio::test]
async fn distinct_runtimes_persist_side_by_side() {
    let (service, storage) = service_and_storage().await;
    service
        .persist_runtime_capabilities(
            &capabilities_json(),
            &handlers(),
            Some(&test_tenant()),
            None,
        )
        .await
        .unwrap();
    let mut other = capabilities_value();
    other["runtime_id"] = serde_json::json!(Uuid::now_v7().to_string());
    service
        .persist_runtime_capabilities(&other.to_string(), &handlers(), Some(&test_tenant()), None)
        .await
        .unwrap();
    assert_eq!(persisted(&storage).await.len(), 2);
}

// --- durable worker commands ---

fn command(worker_id: &str, kind: WorkerCommandKind) -> WorkerCommand {
    WorkerCommand {
        id: Uuid::now_v7(),
        worker_id: worker_id.into(),
        tenant_id: "test".into(),
        command: kind,
        payload: serde_json::json!({"reason": "test"}),
        created_at: chrono::Utc::now(),
    }
}

type CommandFrame = Result<proto::WorkerStreamServer, Status>;

fn command_channel() -> (mpsc::Sender<CommandFrame>, mpsc::Receiver<CommandFrame>) {
    mpsc::channel(16)
}

fn streamed_command(frame: Result<proto::WorkerStreamServer, Status>) -> WorkerCommand {
    let Some(proto::worker_stream_server::Payload::Command(command)) = frame.unwrap().payload
    else {
        panic!("expected a command frame");
    };
    serde_json::from_str(&command.command_json).unwrap()
}

#[tokio::test]
async fn no_pending_commands_streams_nothing_and_requests_no_drain() {
    let (service, _storage) = service_and_storage().await;
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(!drain);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn drain_command_requests_draining() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-a", WorkerCommandKind::Drain))
        .await
        .unwrap();
    let (sender, _receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(drain);
}

#[tokio::test]
async fn ping_command_does_not_request_draining() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-a", WorkerCommandKind::Ping))
        .await
        .unwrap();
    let (sender, _receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(!drain);
}

#[tokio::test]
async fn pending_commands_are_streamed_in_enqueue_order() {
    let (service, storage) = service_and_storage().await;
    let first = command("worker-a", WorkerCommandKind::Ping);
    let second = command("worker-a", WorkerCommandKind::Place);
    storage.enqueue_worker_command(&first).await.unwrap();
    storage.enqueue_worker_command(&second).await.unwrap();
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(!drain);
    let streamed_first = streamed_command(receiver.try_recv().unwrap());
    let streamed_second = streamed_command(receiver.try_recv().unwrap());
    assert_eq!(
        [streamed_first.id, streamed_second.id],
        [first.id, second.id]
    );
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn commands_for_other_workers_are_not_streamed() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-b", WorkerCommandKind::Drain))
        .await
        .unwrap();
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(!drain);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn acknowledgement_removes_the_matching_command() {
    let (service, storage) = service_and_storage().await;
    let target = command("worker-a", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&target).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", Some(&test_tenant()), target.id)
        .await
        .unwrap();
    assert!(
        storage
            .list_worker_commands("worker-a")
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn acknowledging_an_unknown_command_is_a_noop() {
    let (service, storage) = service_and_storage().await;
    let surviving = command("worker-a", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&surviving).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", Some(&test_tenant()), Uuid::now_v7())
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-a").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, surviving.id);
}

#[tokio::test]
async fn acknowledgement_leaves_sibling_commands_intact() {
    let (service, storage) = service_and_storage().await;
    let target = command("worker-a", WorkerCommandKind::Ping);
    let surviving = command("worker-a", WorkerCommandKind::Reload);
    storage.enqueue_worker_command(&target).await.unwrap();
    storage.enqueue_worker_command(&surviving).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", Some(&test_tenant()), target.id)
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-a").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, surviving.id);
}

#[tokio::test]
async fn acknowledgement_is_scoped_to_the_command_owner() {
    let (service, storage) = service_and_storage().await;
    // worker-b's command id presented by worker-a's session must not delete it.
    let foreign = command("worker-b", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&foreign).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", Some(&test_tenant()), foreign.id)
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-b").await.unwrap();
    assert_eq!(remaining.len(), 1);
}

/// GRPC-H1: `worker_id` is client-chosen, so a tenant session that names a
/// foreign tenant's worker must neither receive nor ack its commands.
#[tokio::test]
async fn worker_commands_are_tenant_scoped() {
    let (service, storage) = service_and_storage().await;
    let mut foreign = command("worker-a", WorkerCommandKind::Drain);
    foreign.tenant_id = "other".into();
    storage.enqueue_worker_command(&foreign).await.unwrap();

    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap();
    assert!(!drain);
    assert!(receiver.try_recv().is_err());
    service
        .acknowledge_worker_command("worker-a", Some(&test_tenant()), foreign.id)
        .await
        .unwrap();
    assert_eq!(
        storage
            .list_worker_commands("worker-a")
            .await
            .unwrap()
            .len(),
        1
    );

    // The owning tenant (and an unscoped root session) still see it.
    let other = TenantId::unchecked("other");
    assert!(
        service
            .send_worker_commands("worker-a", Some(&other), &sender)
            .await
            .unwrap()
    );
    assert!(
        service
            .send_worker_commands("worker-a", None, &sender)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn oversized_command_is_resource_exhausted() {
    let (service, storage) = service_and_storage().await;
    let mut oversized = command("worker-a", WorkerCommandKind::Ping);
    oversized.payload = serde_json::json!({"blob": "x".repeat(1024 * 1024)});
    storage.enqueue_worker_command(&oversized).await.unwrap();
    let (sender, _receiver) = command_channel();
    let status = service
        .send_worker_commands("worker-a", Some(&test_tenant()), &sender)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
}
