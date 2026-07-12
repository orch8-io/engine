//! E2E tests for the template resolution inspector.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

fn sequence_body(id: Uuid) -> Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "default",
        "name": format!("inspect-seq-{id}"),
        "version": 1,
        "blocks": [
            {"type": "step", "id": "fetch", "handler": "noop", "params": {}},
            {"type": "step", "id": "notify", "handler": "noop", "params": {
                "email": "{{ context.data.customer.email }}",
                "amount": "{{ outputs.fetch.amount }}",
                "api_key": "{{ context.data.secret_value }}"
            }}
        ],
        "created_at": chrono::Utc::now().to_rfc3339(),
    })
}

fn entry<'a>(trace: &'a Value, path: &str) -> &'a Value {
    trace["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["param_path"] == path)
        .unwrap_or_else(|| panic!("no entry {path}: {trace}"))
}

#[tokio::test]
async fn draft_inspection_resolves_and_redacts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let req = json!({
        "sequence": sequence_body(Uuid::now_v7()),
        "block_id": "notify",
        "context_data": {"customer": {"email": "a@b.c"}, "secret_value": "hunter2"},
        "outputs": {"fetch": {"amount": 42}}
    });
    let resp = client
        .post(format!("{}/sequences/inspect-template", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&req)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();

    assert_eq!(entry(&trace, "email")["status"], "ok");
    assert_eq!(entry(&trace, "email")["value"], "a@b.c");
    assert_eq!(entry(&trace, "amount")["value"], 42);
    assert_eq!(entry(&trace, "amount")["result_type"], "number");
    // Sensitive param name → redacted, secret never leaves.
    assert_eq!(entry(&trace, "api_key")["status"], "redacted");
    assert_eq!(entry(&trace, "api_key")["value"], "[REDACTED]");
    assert!(!trace.to_string().contains("hunter2"));
}

#[tokio::test]
async fn draft_inspection_reports_missing_paths() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let req = json!({
        "sequence": sequence_body(Uuid::now_v7()),
        "block_id": "notify",
        "context_data": {},
        "outputs": {}
    });
    let resp = client
        .post(format!("{}/sequences/inspect-template", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&req)
        .send()
        .await
        .unwrap();
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "email")["status"], "missing");
}

#[tokio::test]
async fn unknown_block_is_404_and_both_sources_rejected() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/sequences/inspect-template", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence": sequence_body(Uuid::now_v7()),
            "block_id": "ghost",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = client
        .post(format!("{}/sequences/inspect-template", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"block_id": "x"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn instance_resolved_input_uses_instance_snapshot() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let seq_id = Uuid::now_v7();
    let create = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&sequence_body(seq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);

    let inst_resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "default",
            "context": {"data": {"customer": {"email": "real@customer.io"}}}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(inst_resp.status(), StatusCode::CREATED);
    let inst: Value = inst_resp.json().await.unwrap();
    let inst_id = inst["id"].as_str().unwrap();

    let resp = client
        .get(format!(
            "{base}/instances/{inst_id}/blocks/notify/resolved-input"
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let trace: Value = resp.json().await.unwrap();
    assert_eq!(entry(&trace, "email")["value"], "real@customer.io");
    // fetch never ran → its output is missing, honestly reported.
    assert_eq!(entry(&trace, "amount")["status"], "missing");

    // Tenant isolation.
    let resp = client
        .get(format!(
            "{base}/instances/{inst_id}/blocks/notify/resolved-input"
        ))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
