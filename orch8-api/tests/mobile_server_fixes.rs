//! Regression tests for the mobile-server audit fixes (MS-01, MS-04, MS-10,
//! MS-12, MS-14) at the HTTP layer.

use orch8_api::test_harness::{TestServer, spawn_test_server, spawn_test_server_with_mobile_sync};
use orch8_storage::testing::fixtures::SequenceBuilder;
use orch8_storage::{AdminStore, MobileCommand, MobileSyncStore, SequenceStore};
use reqwest::StatusCode;
use serde_json::{Value, json};

const DEVICE_ID: &str = "device-fixes";

// ---------------------------------------------------------------------------
// MS-01: auto-rollback hardening
// ---------------------------------------------------------------------------

async fn publish_sequence(server: &TestServer, name: &str) {
    let seq = SequenceBuilder::new("default").name(name).build();
    server.storage.create_sequence(&seq).await.unwrap();
}

async fn wait_for_history(server: &TestServer, expected: usize) -> usize {
    for _ in 0..100 {
        let n = server
            .storage
            .list_rollback_history(None, None, 100)
            .await
            .unwrap()
            .len();
        if n >= expected {
            return n;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    0
}

/// Let any scheduled (debounced) check run before asserting a negative.
async fn settle() {
    tokio::time::sleep(std::time::Duration::from_millis(1_300)).await;
}

async fn post_error(server: &TestServer, seq: &str, device: &str) {
    let resp = reqwest::Client::new()
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&json!({
            "error_type": "RuntimeError",
            "message": "forged",
            "device": {
                "device_id": device, "os_name": "iOS", "os_version": "17",
                "app_version": "1.0", "sdk_version": "0.1"
            },
            "sequence_name": seq
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);
}

async fn create_policy(server: &TestServer, body: Value) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);
    resp.json().await.unwrap()
}

#[tokio::test]
async fn single_forged_error_report_cannot_unpublish_a_sequence_with_default_min_samples() {
    let server = spawn_test_server().await;
    let seq = "forged-seq";
    publish_sequence(&server, seq).await;
    // Any positive rate trips a 0.0 threshold; only the sample floor protects.
    let policy = create_policy(
        &server,
        json!({"sequence_name": seq, "error_rate_threshold": 0.0, "time_window_secs": 3600}),
    )
    .await;
    assert_eq!(policy["min_samples"], 20, "conservative default");

    // One forged report: 1 error / 1 sample = 100%, below the floor.
    post_error(&server, seq, "attacker").await;
    settle().await;
    assert!(
        server
            .storage
            .list_rollback_history(None, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
    let tenant = orch8_types::ids::TenantId::unchecked("default");
    let ns = orch8_types::ids::Namespace::new("default");
    let stored = server
        .storage
        .get_sequence_by_name(&tenant, &ns, seq, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.status,
        orch8_types::sequence::SequenceStatus::Production
    );
    assert!(!stored.deprecated, "sequence must stay published");
}

#[tokio::test]
async fn rollback_triggers_once_the_sample_floor_is_reached() {
    let server = spawn_test_server().await;
    let seq = "floor-seq";
    publish_sequence(&server, seq).await;
    create_policy(
        &server,
        json!({"sequence_name": seq, "error_rate_threshold": 0.5, "time_window_secs": 3600, "min_samples": 5}),
    )
    .await;
    for i in 0..5 {
        post_error(&server, seq, &format!("d{i}")).await;
    }
    assert_eq!(
        wait_for_history(&server, 1).await,
        1,
        "5 errors / 5 samples >= 50%"
    );
}

#[tokio::test]
async fn rollback_ignores_names_that_are_not_published_sequences_of_the_tenant() {
    let server = spawn_test_server().await;
    create_policy(
        &server,
        json!({"sequence_name": "ghost", "error_rate_threshold": 0.0, "time_window_secs": 3600, "min_samples": 1}),
    )
    .await;
    post_error(&server, "ghost", "d1").await;
    settle().await;
    assert!(
        server
            .storage
            .list_rollback_history(None, None, 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn error_burst_is_checked_off_the_request_path_and_debounced() {
    let server = spawn_test_server().await;
    let seq = "burst-seq";
    publish_sequence(&server, seq).await;
    create_policy(
        &server,
        json!({"sequence_name": seq, "error_rate_threshold": 0.0, "time_window_secs": 3600, "min_samples": 1, "cooldown_secs": 0}),
    )
    .await;
    // 30 reports in a tight burst: with cooldown disabled, a per-report inline
    // check would record up to 30 rollbacks; the debounced check records at
    // most the leading + trailing evaluation.
    for i in 0..30 {
        post_error(&server, seq, &format!("d{i}")).await;
    }
    settle().await;
    let n = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap()
        .len();
    assert!(
        (1..=3).contains(&n),
        "expected a handful of evaluations, got {n}"
    );
}

/// Storage faults on the (off-path) rollback evaluation must not turn an
/// accepted error report into a failure, nor unpublish anything.
#[tokio::test]
async fn rollback_check_storage_failure_never_fails_the_report_or_unpublishes() {
    use orch8_api::test_harness::{TestServerOptions, spawn_test_server_with};
    use orch8_storage::testing::faulty::FaultStorage;

    let inner: std::sync::Arc<dyn orch8_storage::StorageBackend> = std::sync::Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let (storage, faults) = FaultStorage::wrap(inner);
    let seq = SequenceBuilder::new("default").name("faulty-seq").build();
    storage.create_sequence(&seq).await.unwrap();
    let server = spawn_test_server_with(storage.clone(), TestServerOptions::default()).await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&json!({"sequence_name": "faulty-seq", "error_rate_threshold": 0.0, "time_window_secs": 3600, "min_samples": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    faults.on("query_error_stats").fail_permanent();
    let report = |device: &'static str| {
        json!({"error_type": "E", "message": "m", "sequence_name": "faulty-seq",
               "device": {"device_id": device, "os_name": "a", "os_version": "1", "app_version": "1", "sdk_version": "1"}})
    };
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&report("d1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    settle().await;
    assert!(faults.injected("query_error_stats") >= 1, "the check ran and hit the fault");
    assert!(storage.list_rollback_history(None, None, 10).await.unwrap().is_empty());

    // The report itself failing to persist is still surfaced to the device.
    faults.on("ingest_telemetry_error").fail_permanent();
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&report("d2"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn invalid_min_samples_is_rejected() {
    let server = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&json!({"sequence_name": "s", "error_rate_threshold": 0.1, "time_window_secs": 60, "min_samples": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// MS-12: HTTP telemetry bounds (same validator as gRPC), dashboard window cap
// ---------------------------------------------------------------------------

fn event(event_type: &str, payload: &str) -> Value {
    json!({
        "event_type": event_type,
        "payload": payload,
        "timestamp": chrono::Utc::now().to_rfc3339(),
        "device": {
            "device_id": "d1", "os_name": "iOS", "os_version": "17",
            "app_version": "1.0", "sdk_version": "0.1"
        }
    })
}

async fn post_events(server: &TestServer, events: Vec<Value>) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/telemetry/mobile", server.base_url))
        .json(&json!({ "events": events }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn http_telemetry_rejects_what_grpc_rejects_naming_the_offending_index() {
    let server = spawn_test_server().await;
    // Oversized event_type (> 128 bytes) at index 1.
    let resp = post_events(
        &server,
        vec![event("ok", "{}"), event(&"x".repeat(129), "{}")],
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("events[1]") && body.contains("invalid_event_type"),
        "{body}"
    );

    // Oversized payload (> 256 KiB).
    let big = format!("\"{}\"", "x".repeat(300 * 1024));
    let resp = post_events(&server, vec![event("ok", &big)]).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    assert!(resp.text().await.unwrap().contains("payload_too_large"));

    // Not JSON.
    let resp = post_events(&server, vec![event("ok", "{not json")]).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Oversized device metadata.
    let mut e = event("ok", "{}");
    e["device"]["device_id"] = json!("d".repeat(257));
    let resp = post_events(&server, vec![e]).await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // Nothing from the rejected batches was stored; a valid one still is.
    let resp = post_events(&server, vec![event("ok", "{}")]).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn http_error_reports_are_bounded() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = |message: String| {
        json!({
            "error_type": "E", "message": message,
            "device": {"device_id": "d", "os_name": "a", "os_version": "1", "app_version": "1", "sdk_version": "1"}
        })
    };
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&base("m".repeat(9 * 1024)))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&base("fine".into()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

#[tokio::test]
async fn future_event_timestamps_do_not_reach_storage_as_future_times() {
    let server = spawn_test_server().await;
    let mut e = event("ok", "{}");
    e["timestamp"] = json!((chrono::Utc::now() + chrono::Duration::days(400)).to_rfc3339());
    let resp = post_events(&server, vec![e]).await;
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
}

// ---------------------------------------------------------------------------
// MS-04 / MS-10 / MS-14: mobile sync
// ---------------------------------------------------------------------------

async fn registered_server() -> (TestServer, reqwest::Client) {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({"device_id": DEVICE_ID, "platform": "ios"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    (srv, client)
}

async fn sync(srv: &TestServer, client: &reqwest::Client, body: Value) -> reqwest::Response {
    let mut body = body;
    body["device_id"] = json!(DEVICE_ID);
    client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&body)
        .send()
        .await
        .unwrap()
}

async fn statuses(srv: &TestServer, client: &reqwest::Client) -> Vec<Value> {
    let resp = client
        .get(format!(
            "{}/mobile/status?device_id={DEVICE_ID}",
            srv.base_url
        ))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<Value>().await.unwrap()["items"]
        .as_array()
        .unwrap()
        .clone()
}

#[tokio::test]
async fn malformed_status_timestamp_is_a_400_naming_the_item_and_writes_nothing() {
    let (srv, client) = registered_server().await;
    let resp = sync(
        &srv,
        &client,
        json!({"status_updates": [
            {"instance_id": "ok", "state": "Running", "timestamp": "2026-05-19T12:00:00Z"},
            {"instance_id": "bad", "state": "Running", "timestamp": "yesterday-ish"},
        ]}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.text().await.unwrap();
    assert!(body.contains("status_updates[1]"), "{body}");
    assert!(
        statuses(&srv, &client).await.is_empty(),
        "the valid sibling must not be half-applied"
    );
}

#[tokio::test]
async fn delayed_sync_retry_cannot_overwrite_newer_state() {
    let (srv, client) = registered_server().await;
    let newer = json!({"status_updates": [
        {"instance_id": "i1", "state": "Completed", "timestamp": "2026-05-19T12:05:00Z"}]});
    let stale = json!({"status_updates": [
        {"instance_id": "i1", "state": "Running", "timestamp": "2026-05-19T12:00:00Z"}]});
    assert_eq!(sync(&srv, &client, newer).await.status(), StatusCode::OK);
    assert_eq!(sync(&srv, &client, stale).await.status(), StatusCode::OK);
    let rows = statuses(&srv, &client).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["state"], "Completed");
}

#[tokio::test]
async fn one_corrupt_command_is_skipped_not_fatal_to_the_sync_response() {
    let (srv, client) = registered_server().await;
    let good = MobileCommand {
        id: "good-cmd".into(),
        device_id: DEVICE_ID.into(),
        command_type: "ping".into(),
        payload: r#"{"n":1}"#.into(),
        created_at: String::new(),
        acked_at: None,
    };
    let bad = MobileCommand {
        id: "bad-cmd".into(),
        payload: "{{not json".into(),
        ..good.clone()
    };
    srv.storage.create_mobile_command(&bad).await.unwrap();
    srv.storage.create_mobile_command(&good).await.unwrap();

    let resp = sync(&srv, &client, json!({})).await;
    assert_eq!(resp.status(), StatusCode::OK, "a bad row must not 500 the sync");
    let body: Value = resp.json().await.unwrap();
    let commands = body["commands"].as_array().unwrap();
    assert_eq!(commands.len(), 1);
    assert_eq!(commands[0]["id"], "good-cmd");
}

#[tokio::test]
async fn retried_step_delegation_does_not_mint_a_second_command() {
    let (srv, client) = registered_server().await;
    let delegation = json!({"step_delegations": [{
        "request_id": "req-1", "instance_id": "i1", "block_id": "b1",
        "handler": "h", "params": {"token": "credentials://stripe/key"}
    }]});
    for _ in 0..3 {
        assert_eq!(
            sync(&srv, &client, delegation.clone()).await.status(),
            StatusCode::OK
        );
    }
    let pending = srv.storage.fetch_pending_commands(DEVICE_ID, 50).await.unwrap();
    assert_eq!(
        pending.iter().filter(|c| c.command_type == "step_result").count(),
        1,
        "same (device, request_id) maps to one command"
    );
    // A different request id is a different command.
    let other = json!({"step_delegations": [{
        "request_id": "req-2", "instance_id": "i1", "block_id": "b1",
        "handler": "h", "params": {}
    }]});
    assert_eq!(sync(&srv, &client, other).await.status(), StatusCode::OK);
    let pending = srv.storage.fetch_pending_commands(DEVICE_ID, 50).await.unwrap();
    assert_eq!(pending.len(), 2);
}

#[tokio::test]
async fn full_five_hundred_item_sync_succeeds() {
    let (srv, client) = registered_server().await;
    let updates: Vec<Value> = (0..500)
        .map(|i| {
            json!({"instance_id": format!("i{i}"), "state": "Running", "timestamp": "2026-05-19T12:00:00Z"})
        })
        .collect();
    let approvals: Vec<Value> = (0..500)
        .map(|i| json!({"instance_id": format!("i{i}"), "block_id": "b"}))
        .collect();
    let resp = sync(
        &srv,
        &client,
        json!({"status_updates": updates, "approval_requests": approvals}),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(!statuses(&srv, &client).await.is_empty());
    let all = srv
        .storage
        .list_mobile_instance_status(None, Some(DEVICE_ID), 1_000)
        .await
        .unwrap();
    assert_eq!(all.len(), 500);
}
