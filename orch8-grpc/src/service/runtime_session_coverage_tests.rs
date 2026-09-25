//! Coverage tests for persisted runtime control sessions: capability-list
//! bounds, heartbeat normalization (trust pinning, lease stamping), session
//! handler enforcement, capability persistence, and durable worker commands.
//!
//! Count contract: 48 independently named unit tests.

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

// --- bounded_runtime_list ---

macro_rules! runtime_list_case {
    ($name:ident, $values:expr, $valid:expr) => {
        #[test]
        fn $name() {
            assert_eq!(bounded_runtime_list(&$values), $valid);
        }
    };
}

runtime_list_case!(
    coverage_runtime_001_empty_list_is_valid,
    Vec::<String>::new(),
    true
);
runtime_list_case!(
    coverage_runtime_002_64_entries_are_valid,
    (0..64).map(|i| format!("entry-{i}")).collect::<Vec<_>>(),
    true
);
runtime_list_case!(
    coverage_runtime_003_65_entries_are_rejected,
    (0..65).map(|i| format!("entry-{i}")).collect::<Vec<_>>(),
    false
);
runtime_list_case!(
    coverage_runtime_004_empty_entry_is_rejected,
    [String::new()],
    false
);
runtime_list_case!(
    coverage_runtime_005_256_byte_entry_is_valid,
    ["e".repeat(256)],
    true
);
runtime_list_case!(
    coverage_runtime_006_257_byte_entry_is_rejected,
    ["e".repeat(257)],
    false
);

// --- prepare_runtime_capabilities ---

#[test]
fn coverage_runtime_007_valid_capabilities_are_normalized() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    assert_eq!(capabilities.runtime_id, runtime_id());
    assert_eq!(capabilities.handlers, vec!["payments".to_owned()]);
    assert_eq!(capabilities.plugins, vec!["card-reader".to_owned()]);
    assert!(capabilities.offline_capable);
    assert!(!capabilities.draining);
}

#[test]
fn coverage_runtime_008_self_reported_trust_is_pinned_to_registered() {
    for trust in ["unverified", "registered", "signed", "attested"] {
        let mut value = capabilities_value();
        value["trust"] = serde_json::Value::String(trust.into());
        let capabilities =
            prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
        assert_eq!(capabilities.trust, RuntimeTrustLevel::Registered);
    }
}

#[test]
fn coverage_runtime_009_lease_is_stamped_at_preparation_time() {
    let before = chrono::Utc::now();
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    let after = chrono::Utc::now();
    assert!(capabilities.observed_at >= before);
    assert!(capabilities.observed_at <= after);
}

#[test]
fn coverage_runtime_010_lease_expires_exactly_45_seconds_after_observation() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    assert_eq!(
        capabilities.expires_at - capabilities.observed_at,
        chrono::Duration::seconds(RUNTIME_CAPABILITY_LEASE_SECS)
    );
    assert_eq!(RUNTIME_CAPABILITY_LEASE_SECS, 45);
}

#[test]
fn coverage_runtime_011_expected_runtime_id_match_is_accepted() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), Some(runtime_id()))
            .unwrap();
    assert_eq!(capabilities.runtime_id, runtime_id());
}

#[test]
fn coverage_runtime_012_runtime_id_swap_is_permission_denied() {
    let other = RuntimeId::from_uuid(Uuid::now_v7());
    let status =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), Some(other)).unwrap_err();
    assert_eq!(status.code(), tonic::Code::PermissionDenied);
}

#[test]
fn coverage_runtime_013_missing_session_handler_is_invalid_argument() {
    let status = prepare_runtime_capabilities(&capabilities_json(), &["billing".to_owned()], None)
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_014_session_handler_among_many_is_accepted() {
    let mut value = capabilities_value();
    value["handlers"] = serde_json::json!(["billing", "payments", "email"]);
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    assert_eq!(capabilities.handlers.len(), 3);
}

#[test]
fn coverage_runtime_015_oversized_handler_list_is_invalid_argument() {
    let mut value = capabilities_value();
    let mut list = vec!["payments".to_owned()];
    list.extend((0..64).map(|i| format!("handler-{i}")));
    value["handlers"] = serde_json::json!(list);
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_016_empty_handler_entry_is_invalid_argument() {
    let mut value = capabilities_value();
    value["handlers"] = serde_json::json!(["payments", ""]);
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_017_oversized_plugin_list_is_invalid_argument() {
    let mut value = capabilities_value();
    value["plugins"] =
        serde_json::json!((0..65).map(|i| format!("plugin-{i}")).collect::<Vec<_>>());
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_018_empty_credential_entry_is_invalid_argument() {
    let mut value = capabilities_value();
    value["credentials"] = serde_json::json!([""]);
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_019_oversized_region_entry_is_invalid_argument() {
    let mut value = capabilities_value();
    value["regions"] = serde_json::json!(["r".repeat(257)]);
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_020_oversized_hardware_list_is_invalid_argument() {
    let mut value = capabilities_value();
    value["hardware"] = serde_json::json!((0..65).map(|i| format!("hw-{i}")).collect::<Vec<_>>());
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_021_capsule_key_at_1024_bytes_is_accepted() {
    let mut value = capabilities_value();
    value["capsule_signing_public_key"] = serde_json::json!("k".repeat(1_024));
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    assert_eq!(
        capabilities.capsule_signing_public_key.as_deref(),
        Some("k".repeat(1_024).as_str())
    );
}

#[test]
fn coverage_runtime_022_capsule_key_at_1025_bytes_is_invalid_argument() {
    let mut value = capabilities_value();
    value["capsule_signing_public_key"] = serde_json::json!("k".repeat(1_025));
    let status = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_023_malformed_capabilities_json_is_invalid_argument() {
    let status = prepare_runtime_capabilities("{not json", &handlers(), None).unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coverage_runtime_024_draining_report_is_preserved() {
    let mut value = capabilities_value();
    value["draining"] = serde_json::json!(true);
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    assert!(capabilities.draining);
}

#[test]
fn coverage_runtime_025_optional_reports_are_preserved() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    assert_eq!(capabilities.battery_percent, Some(88));
    assert_eq!(
        capabilities.connectivity,
        Some(orch8_types::continuity::RuntimeConnectivity::Ethernet)
    );
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
async fn coverage_runtime_026_persist_without_tenant_is_failed_precondition() {
    let (service, _storage) = service_and_storage().await;
    let status = service
        .persist_runtime_capabilities(&capabilities_json(), &handlers(), None, None)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn coverage_runtime_027_persisted_capabilities_are_retrievable() {
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
    let rows = persisted(&storage).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].runtime_id, runtime_id());
    assert_eq!(rows[0].trust, RuntimeTrustLevel::Registered);
    assert!(rows[0].expires_at > chrono::Utc::now());
}

#[tokio::test]
async fn coverage_runtime_028_second_heartbeat_upserts_the_same_runtime() {
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
}

#[tokio::test]
async fn coverage_runtime_029_distinct_runtimes_persist_side_by_side() {
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

#[tokio::test]
async fn coverage_runtime_030_stale_observations_are_filtered_by_listing() {
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
    let future = storage
        .list_runtime_capabilities(
            &test_tenant(),
            chrono::Utc::now() + chrono::Duration::minutes(1),
            10,
        )
        .await
        .unwrap();
    assert!(future.is_empty());
}

// --- durable worker commands ---

fn command(worker_id: &str, kind: WorkerCommandKind) -> WorkerCommand {
    WorkerCommand {
        id: Uuid::now_v7(),
        worker_id: worker_id.into(),
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
async fn coverage_runtime_031_no_commands_reports_no_drain() {
    let (service, _storage) = service_and_storage().await;
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap();
    assert!(!drain);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn coverage_runtime_032_drain_command_requests_draining() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-a", WorkerCommandKind::Drain))
        .await
        .unwrap();
    let (sender, _receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap();
    assert!(drain);
}

#[tokio::test]
async fn coverage_runtime_033_ping_command_does_not_request_draining() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-a", WorkerCommandKind::Ping))
        .await
        .unwrap();
    let (sender, _receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap();
    assert!(!drain);
}

#[tokio::test]
async fn coverage_runtime_034_all_pending_commands_are_streamed_in_order() {
    let (service, storage) = service_and_storage().await;
    let first = command("worker-a", WorkerCommandKind::Ping);
    let second = command("worker-a", WorkerCommandKind::Place);
    storage.enqueue_worker_command(&first).await.unwrap();
    storage.enqueue_worker_command(&second).await.unwrap();
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", &sender)
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
async fn coverage_runtime_035_command_json_round_trips_all_fields() {
    let (service, storage) = service_and_storage().await;
    let original = command("worker-a", WorkerCommandKind::Place);
    storage.enqueue_worker_command(&original).await.unwrap();
    let (sender, mut receiver) = command_channel();
    service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap();
    let streamed = streamed_command(receiver.try_recv().unwrap());
    assert_eq!(streamed.id, original.id);
    assert_eq!(streamed.worker_id, "worker-a");
    assert_eq!(streamed.command, WorkerCommandKind::Place);
    assert_eq!(streamed.payload, serde_json::json!({"reason": "test"}));
}

#[tokio::test]
async fn coverage_runtime_036_commands_for_other_workers_are_not_streamed() {
    let (service, storage) = service_and_storage().await;
    storage
        .enqueue_worker_command(&command("worker-b", WorkerCommandKind::Drain))
        .await
        .unwrap();
    let (sender, mut receiver) = command_channel();
    let drain = service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap();
    assert!(!drain);
    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn coverage_runtime_037_acknowledgement_removes_the_matching_command() {
    let (service, storage) = service_and_storage().await;
    let target = command("worker-a", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&target).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", target.id)
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
async fn coverage_runtime_038_acknowledgement_of_unknown_command_is_a_noop() {
    let (service, storage) = service_and_storage().await;
    let surviving = command("worker-a", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&surviving).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", Uuid::now_v7())
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-a").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, surviving.id);
}

#[tokio::test]
async fn coverage_runtime_039_acknowledgement_leaves_sibling_commands_intact() {
    let (service, storage) = service_and_storage().await;
    let target = command("worker-a", WorkerCommandKind::Ping);
    let surviving = command("worker-a", WorkerCommandKind::Reload);
    storage.enqueue_worker_command(&target).await.unwrap();
    storage.enqueue_worker_command(&surviving).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", target.id)
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-a").await.unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].id, surviving.id);
}

#[tokio::test]
async fn coverage_runtime_040_acknowledgement_is_scoped_to_the_command_owner() {
    let (service, storage) = service_and_storage().await;
    // worker-b's command id presented by worker-a's session must not delete it.
    let foreign = command("worker-b", WorkerCommandKind::Ping);
    storage.enqueue_worker_command(&foreign).await.unwrap();
    service
        .acknowledge_worker_command("worker-a", foreign.id)
        .await
        .unwrap();
    let remaining = storage.list_worker_commands("worker-b").await.unwrap();
    assert_eq!(remaining.len(), 1);
}

// --- session serialization determinism ---

#[test]
fn coverage_runtime_041_worker_command_kind_serializes_snake_case() {
    for (kind, expected) in [
        (WorkerCommandKind::Drain, "\"drain\""),
        (WorkerCommandKind::Reload, "\"reload\""),
        (WorkerCommandKind::Ping, "\"ping\""),
        (WorkerCommandKind::Place, "\"place\""),
    ] {
        assert_eq!(serde_json::to_string(&kind).unwrap(), expected);
    }
}

#[test]
fn coverage_runtime_042_worker_command_json_round_trip_is_stable() {
    let original = command("worker-a", WorkerCommandKind::Place);
    let json = serde_json::to_string(&original).unwrap();
    let parsed: WorkerCommand = serde_json::from_str(&json).unwrap();
    assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
}

#[test]
fn coverage_runtime_043_prepared_capabilities_round_trip_is_stable() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    let json = serde_json::to_string(&capabilities).unwrap();
    let parsed: RuntimeCapabilities = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, capabilities);
    assert_eq!(serde_json::to_string(&parsed).unwrap(), json);
}

#[test]
fn coverage_runtime_044_empty_capability_lists_are_omitted_from_json() {
    let mut value = capabilities_value();
    value["plugins"] = serde_json::json!([]);
    value["credentials"] = serde_json::json!([]);
    value["regions"] = serde_json::json!([]);
    value["hardware"] = serde_json::json!([]);
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    let json = serde_json::to_value(&capabilities).unwrap();
    assert!(json.get("plugins").is_none());
    assert!(json.get("credentials").is_none());
    assert!(json.get("regions").is_none());
    assert!(json.get("hardware").is_none());
    assert!(json.get("handlers").is_some());
}

#[test]
fn coverage_runtime_045_capability_deserialization_defaults_absent_lists() {
    let capabilities: RuntimeCapabilities = serde_json::from_value(serde_json::json!({
        "runtime_id": RUNTIME,
        "kind": "server",
        "trust": "unverified",
        "handlers": ["payments"],
        "observed_at": "2020-01-01T00:00:00Z",
        "expires_at": "2020-01-01T00:00:01Z"
    }))
    .unwrap();
    assert!(capabilities.plugins.is_empty());
    assert!(capabilities.credentials.is_empty());
    assert!(!capabilities.offline_capable);
    assert!(!capabilities.draining);
}

#[test]
fn coverage_runtime_046_prepared_serialization_contains_no_client_supplied_trust() {
    let mut value = capabilities_value();
    value["trust"] = serde_json::json!("attested");
    let capabilities = prepare_runtime_capabilities(&value.to_string(), &handlers(), None).unwrap();
    let json = serde_json::to_value(&capabilities).unwrap();
    assert_eq!(json["trust"], serde_json::json!("registered"));
}

#[test]
fn coverage_runtime_047_serialized_lease_fields_are_rfc3339_utc() {
    let capabilities =
        prepare_runtime_capabilities(&capabilities_json(), &handlers(), None).unwrap();
    let json = serde_json::to_value(&capabilities).unwrap();
    let observed = chrono::DateTime::parse_from_rfc3339(json["observed_at"].as_str().unwrap());
    let expires = chrono::DateTime::parse_from_rfc3339(json["expires_at"].as_str().unwrap());
    assert!(observed.is_ok());
    assert!(expires.is_ok());
}

#[tokio::test]
async fn coverage_runtime_048_oversized_command_is_resource_exhausted() {
    let (service, storage) = service_and_storage().await;
    let mut oversized = command("worker-a", WorkerCommandKind::Ping);
    oversized.payload = serde_json::json!({"blob": "x".repeat(1024 * 1024)});
    storage.enqueue_worker_command(&oversized).await.unwrap();
    let (sender, _receiver) = command_channel();
    let status = service
        .send_worker_commands("worker-a", &sender)
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);
}
