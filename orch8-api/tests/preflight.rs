//! E2E tests for the sequence preflight endpoints.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

fn draft_sequence(tenant: &str, blocks: Value) -> Value {
    json!({
        "id": Uuid::now_v7(),
        "tenant_id": tenant,
        "namespace": "default",
        "name": "preflight-e2e",
        "version": 1,
        "blocks": blocks,
        "created_at": chrono::Utc::now().to_rfc3339(),
    })
}

fn check<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("missing check {id} in {report}"))
}

#[tokio::test]
async fn draft_preflight_reports_missing_worker_and_credential() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = draft_sequence(
        "t1",
        json!([
            {"type": "step", "id": "charge", "handler": "charge_card",
             "params": {"key": "credentials://billing_key"}}
        ]),
    );

    let resp = client
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();

    assert_eq!(report["overall"], "fail");
    let handlers = check(&report, "handlers_have_workers");
    assert_eq!(handlers["status"], "fail");
    assert_eq!(handlers["findings"][0]["code"], "NO_COMPATIBLE_WORKER");

    let creds = check(&report, "credentials_present");
    assert_eq!(creds["status"], "fail");
    assert_eq!(creds["findings"][0]["code"], "CREDENTIAL_MISSING");
}

#[tokio::test]
async fn builtin_only_draft_passes() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = draft_sequence(
        "t1",
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    );

    let resp = client
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["overall"], "pass", "{report}");
}

#[tokio::test]
async fn stored_sequence_preflight_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create a sequence with a missing external handler.
    let body = draft_sequence(
        "t1",
        json!([{"type": "step", "id": "x", "handler": "external_thing", "params": {}}]),
    );
    let create = client
        .post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    let seq_id = body["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/sequences/{seq_id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["overall"], "fail");
    assert_eq!(report["sequence_name"], "preflight-e2e");
}

#[tokio::test]
async fn stored_preflight_is_tenant_isolated() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = draft_sequence(
        "t1",
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    );
    client
        .post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let seq_id = body["id"].as_str().unwrap();

    // Another tenant must get 404, not the report.
    let resp = client
        .get(format!("{}/sequences/{seq_id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_input_schema_fails_preflight() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let mut body = draft_sequence(
        "t1",
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    );
    // Structurally an object, but not a compilable JSON Schema.
    body["input_schema"] = json!({"type": "definitely_not_a_type"});

    let resp = client
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    let schema_check = check(&report, "input_schema_valid");
    assert_eq!(schema_check["status"], "fail", "{report}");
    assert_eq!(report["overall"], "fail");
}
