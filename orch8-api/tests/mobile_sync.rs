//! E2E tests for mobile sync endpoints.
//!
//! Tests the full phone <-> server flow: device registration, status reporting,
//! approval requests, command delivery, and adaptive sync intervals.

use orch8_api::test_harness::spawn_test_server_with_mobile_sync;
use reqwest::StatusCode;
use serde_json::json;

const DEVICE_ID: &str = "device-abc-123";

#[allow(clippy::needless_pass_by_value)]
fn sync_body(
    status_updates: serde_json::Value,
    approval_requests: serde_json::Value,
    command_acks: serde_json::Value,
) -> serde_json::Value {
    json!({
        "device_id": DEVICE_ID,
        "status_updates": status_updates,
        "approval_requests": approval_requests,
        "command_acks": command_acks,
    })
}

// ---------------------------------------------------------------------------
// Device registration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn register_device() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
            "push_token": "apns-token-xyz",
            "app_version": "1.0.0",
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::CREATED);
}

// ---------------------------------------------------------------------------
// Sync: empty request returns empty commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_empty_returns_ok() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register device first so tenant ownership check passes.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
        }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"], json!([]));
    assert_eq!(body["sync_interval_secs"], 30);
}

// ---------------------------------------------------------------------------
// Status updates: phone reports instance state, server stores it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_status_updates_are_stored() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register device first.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "android",
        }))
        .send()
        .await
        .unwrap();

    // Send status update via sync.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([{
                "instance_id": "inst-001",
                "sequence_name": "onboarding",
                "state": "Running",
                "current_step": "step-1",
                "handler": "show_welcome",
                "timestamp": "2026-05-19T12:00:00Z",
            }]),
            json!([]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify via GET /mobile/status.
    let resp = client
        .get(format!(
            "{}/mobile/status?device_id={}",
            srv.base_url, DEVICE_ID
        ))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 1);
    assert_eq!(body["items"][0]["instance_id"], "inst-001");
    assert_eq!(body["items"][0]["state"], "Running");
    assert_eq!(body["items"][0]["sequence_name"], "onboarding");
}

// ---------------------------------------------------------------------------
// Status coalescing: latest update wins per instance
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_status_coalesces_per_instance() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register device first so tenant ownership check passes.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
        }))
        .send()
        .await
        .unwrap();

    // First sync: Running.
    client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([{
                "instance_id": "inst-002",
                "state": "Running",
                "timestamp": "2026-05-19T12:00:00Z",
            }]),
            json!([]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();

    // Second sync: Completed.
    client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([{
                "instance_id": "inst-002",
                "state": "Completed",
                "timestamp": "2026-05-19T12:01:00Z",
            }]),
            json!([]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!(
            "{}/mobile/status?device_id={}",
            srv.base_url, DEVICE_ID
        ))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 1, "should coalesce into one entry");
    assert_eq!(body["items"][0]["state"], "Completed");
}

// ---------------------------------------------------------------------------
// Approval requests: phone reports wait_for_input, server stores it
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_approval_request_is_stored() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register device first so tenant ownership check passes.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
        }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([]),
            json!([{
                "instance_id": "inst-003",
                "block_id": "approval-step",
                "sequence_name": "kyc-flow",
                "prompt": "Please verify your identity",
                "choices": ["approve", "reject"],
                "store_as": "kyc_result",
                "timeout_seconds": 3600,
            }]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify via GET /mobile/approvals.
    let resp = client
        .get(format!("{}/mobile/approvals?state=pending", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["total"], 1);
    assert_eq!(body["items"][0]["instance_id"], "inst-003");
    assert_eq!(body["items"][0]["block_id"], "approval-step");
    assert_eq!(body["items"][0]["prompt"], "Please verify your identity");
    assert_eq!(body["items"][0]["state"], "pending");
}

// ---------------------------------------------------------------------------
// Approval resolution -> command creation -> delivery via sync
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resolve_approval_creates_command_delivered_on_next_sync() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register device.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
        }))
        .send()
        .await
        .unwrap();

    // Phone sends an approval request via sync.
    client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([]),
            json!([{
                "instance_id": "inst-004",
                "block_id": "review-step",
                "prompt": "Approve?",
            }]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();

    // Admin fetches and resolves the approval.
    let approvals: serde_json::Value = client
        .get(format!("{}/mobile/approvals?state=pending", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let approval_id = approvals["items"][0]["id"].as_str().unwrap();

    let resp = client
        .post(format!(
            "{}/mobile/approvals/{}/resolve",
            srv.base_url, approval_id
        ))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({ "output": { "decision": "approved", "comment": "Looks good" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Phone syncs again — should receive the complete_step command.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    let cmd = &body["commands"][0];
    assert_eq!(cmd["type"], "complete_step");
    assert_eq!(cmd["payload"]["instance_id"], "inst-004");
    assert_eq!(cmd["payload"]["output"]["decision"], "approved");

    // Sync interval should be low (5s) since commands were pending.
    assert_eq!(body["sync_interval_secs"], 5);
}

// ---------------------------------------------------------------------------
// Command ACK: phone acknowledges command, server stops re-delivering
// ---------------------------------------------------------------------------

#[tokio::test]
async fn command_ack_prevents_redelivery() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register + create a command directly.
    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({ "device_id": DEVICE_ID, "platform": "ios" }))
        .send()
        .await
        .unwrap();

    client
        .post(format!("{}/mobile/commands", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "command_type": "cancel_instance",
            "payload": { "instance_id": "inst-005" },
        }))
        .send()
        .await
        .unwrap();

    // First sync: receives the command.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    let cmd_id = body["commands"][0]["id"].as_str().unwrap().to_string();

    // Second sync: ACK the command.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([cmd_id])))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();

    // No more commands — the ACKed one is gone.
    assert_eq!(body["commands"].as_array().unwrap().len(), 0);
    // Back to normal sync interval since no pending commands.
    assert_eq!(body["sync_interval_secs"], 30);
}

// ---------------------------------------------------------------------------
// Create command endpoint: server pushes arbitrary command to device
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_command_delivered_on_sync() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({ "device_id": DEVICE_ID, "platform": "android" }))
        .send()
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/mobile/commands", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "command_type": "complete_step",
            "payload": {
                "instance_id": "inst-006",
                "step_name": "collect-email",
                "output": { "email": "user@example.com" },
            },
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // Sync picks it up.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    assert_eq!(body["commands"][0]["type"], "complete_step");
    assert_eq!(body["commands"][0]["payload"]["instance_id"], "inst-006");
}

// ---------------------------------------------------------------------------
// Full round-trip: register -> sync status -> approval -> resolve -> command -> ACK
// ---------------------------------------------------------------------------

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_round_trip() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // 1. Register device.
    let resp = client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": DEVICE_ID,
            "platform": "ios",
            "push_token": "apns-tok",
            "app_version": "2.1.0",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    // 2. Phone sends status update + approval request in a single sync.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([{
                "instance_id": "inst-100",
                "sequence_name": "onboarding-v2",
                "state": "Waiting",
                "current_step": "verify-id",
                "handler": "id_check",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }]),
            json!([{
                "instance_id": "inst-100",
                "block_id": "verify-id",
                "sequence_name": "onboarding-v2",
                "prompt": "Upload your ID document",
                "timeout_seconds": 7200,
            }]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 0);
    assert_eq!(body["sync_interval_secs"], 30);

    // 3. Admin sees the status and approval on the dashboard.
    let statuses: serde_json::Value = client
        .get(format!(
            "{}/mobile/status?device_id={}",
            srv.base_url, DEVICE_ID
        ))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(statuses["total"], 1);
    assert_eq!(statuses["items"][0]["state"], "Waiting");

    let approvals: serde_json::Value = client
        .get(format!("{}/mobile/approvals?state=pending", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(approvals["total"], 1);
    let approval_id = approvals["items"][0]["id"].as_str().unwrap();

    // 4. Admin resolves the approval.
    let resp = client
        .post(format!(
            "{}/mobile/approvals/{}/resolve",
            srv.base_url, approval_id
        ))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({ "output": { "verified": true, "doc_type": "passport" } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 5. Phone syncs and receives the command (simulating push wake-up).
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);
    let cmd = &body["commands"][0];
    assert_eq!(cmd["type"], "complete_step");
    assert_eq!(cmd["payload"]["instance_id"], "inst-100");
    assert_eq!(cmd["payload"]["output"]["verified"], true);
    // Fast interval because commands were pending.
    assert_eq!(body["sync_interval_secs"], 5);

    let cmd_id = cmd["id"].as_str().unwrap().to_string();

    // 6. Phone ACKs the command and reports updated status.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&sync_body(
            json!([{
                "instance_id": "inst-100",
                "sequence_name": "onboarding-v2",
                "state": "Running",
                "current_step": "next-step",
                "timestamp": chrono::Utc::now().to_rfc3339(),
            }]),
            json!([]),
            json!([cmd_id]),
        ))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 0);
    // Back to normal interval.
    assert_eq!(body["sync_interval_secs"], 30);

    // 7. Dashboard shows updated status.
    let statuses: serde_json::Value = client
        .get(format!(
            "{}/mobile/status?device_id={}",
            srv.base_url, DEVICE_ID
        ))
        .header("X-Tenant-Id", "tenant-1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(statuses["items"][0]["state"], "Running");
    assert_eq!(statuses["items"][0]["current_step"], "next-step");
}

// ---------------------------------------------------------------------------
// Sync endpoints return 404 when mobile sync is disabled
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_endpoints_404_when_disabled() {
    let srv = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .json(&sync_body(json!([]), json!([]), json!([])))
        .send()
        .await
        .unwrap();
    // Routes aren't mounted, so 404.
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// Multiple devices: commands are scoped to the correct device
// ---------------------------------------------------------------------------

#[tokio::test]
async fn commands_scoped_to_device() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    // Register two devices.
    for dev in ["device-A", "device-B"] {
        client
            .post(format!("{}/mobile/devices/register", srv.base_url))
            .header("X-Tenant-Id", "tenant-1")
            .json(&json!({ "device_id": dev, "platform": "ios" }))
            .send()
            .await
            .unwrap();
    }

    // Create command for device-A only.
    client
        .post(format!("{}/mobile/commands", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": "device-A",
            "command_type": "cancel_instance",
            "payload": { "instance_id": "inst-007" },
        }))
        .send()
        .await
        .unwrap();

    // Device-A sync: should see the command.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": "device-A",
            "status_updates": [],
            "approval_requests": [],
            "command_acks": [],
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 1);

    // Device-B sync: should NOT see the command.
    let resp = client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "tenant-1")
        .json(&json!({
            "device_id": "device-B",
            "status_updates": [],
            "approval_requests": [],
            "command_acks": [],
        }))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["commands"].as_array().unwrap().len(), 0);
}

// ---------------------------------------------------------------------------
// Resolving same approval twice returns 404
// ---------------------------------------------------------------------------

#[tokio::test]
async fn resolve_approval_twice_returns_not_found() {
    let srv = spawn_test_server_with_mobile_sync().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/mobile/devices/register", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "device_id": DEVICE_ID, "platform": "ios" }))
        .send()
        .await
        .unwrap();

    // Create an approval.
    client
        .post(format!("{}/mobile/sync", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&sync_body(
            json!([]),
            json!([{
                "instance_id": "inst-008",
                "block_id": "approve-step",
                "prompt": "Approve?",
            }]),
            json!([]),
        ))
        .send()
        .await
        .unwrap();

    let approvals: serde_json::Value = client
        .get(format!("{}/mobile/approvals?state=pending", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let id = approvals["items"][0]["id"].as_str().unwrap();

    // First resolve: OK.
    let resp = client
        .post(format!("{}/mobile/approvals/{}/resolve", srv.base_url, id))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "output": "yes" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Second resolve: already resolved -> 404.
    let resp = client
        .post(format!("{}/mobile/approvals/{}/resolve", srv.base_url, id))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "output": "no" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
