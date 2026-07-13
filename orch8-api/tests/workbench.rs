//! E2E tests for the execution workbench, run comparison, and fork preview.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::OutputStore;
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

async fn publish(base: &str, client: &reqwest::Client, blocks: Value) -> Uuid {
    let id = Uuid::now_v7();
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "id": id, "tenant_id": "t1", "namespace": "default",
            "name": format!("wb-seq-{id}"), "version": 1,
            "blocks": blocks,
            "created_at": chrono::Utc::now().to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    id
}

async fn spawn_instance(base: &str, client: &reqwest::Client, seq: Uuid, data: Value) -> Uuid {
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": seq, "tenant_id": "t1", "namespace": "default",
            "context": {"data": data},
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: Value = resp.json().await.unwrap();
    Uuid::parse_str(v["id"].as_str().unwrap()).unwrap()
}

async fn record_output(
    srv: &orch8_api::test_harness::TestServer,
    instance: Uuid,
    block: &str,
    output: Value,
) {
    srv.storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: InstanceId::from_uuid(instance),
            block_id: BlockId::new(block),
            output,
            output_ref: None,
            output_size: 10,
            attempt: 1,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn workbench_joins_and_redacts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "b", "handler": "noop", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(
        &base,
        &client,
        seq,
        json!({"customer": "ada", "api_key": "sk_live_secret"}),
    )
    .await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;
    record_output(&srv, inst, "b", json!({"ok": 2})).await;

    let resp = client
        .get(format!("{base}/instances/{inst}/workbench"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let view: Value = resp.json().await.unwrap();

    // Context redacted, never the secret in clear text.
    assert_eq!(view["context_data"]["api_key"], "[REDACTED]");
    assert_eq!(view["context_data"]["customer"], "ada");
    assert!(!view.to_string().contains("sk_live_secret"));

    // Output summaries carry sizes/status, not payloads.
    let outputs = view["outputs"].as_array().unwrap();
    assert_eq!(outputs.len(), 2);
    assert!(outputs.iter().all(|o| o.get("output").is_none()));

    // Timeline includes both block outputs, ordered.
    let events = view["events"].as_array().unwrap();
    let block_events: Vec<&Value> = events
        .iter()
        .filter(|e| e["kind"] == "block_output")
        .collect();
    assert_eq!(block_events.len(), 2);
    assert_eq!(view["share_path"], format!("/instances/{inst}/workbench"));

    // Deterministic ordering: a second fetch returns the same event ids.
    let view2: Value = client
        .get(format!("{base}/instances/{inst}/workbench"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let ids1: Vec<&str> = view["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    let ids2: Vec<&str> = view2["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids1, ids2);

    // Tenant isolation.
    let resp = client
        .get(format!("{base}/instances/{inst}/workbench"))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn comparison_reports_paths_and_output_diffs() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    )
    .await;
    let left = spawn_instance(&base, &client, seq, json!({})).await;
    let right = spawn_instance(&base, &client, seq, json!({})).await;

    record_output(&srv, left, "shared_same", json!({"v": 1})).await;
    record_output(&srv, right, "shared_same", json!({"v": 1})).await;
    record_output(&srv, left, "shared_diff", json!({"v": "left"})).await;
    record_output(&srv, right, "shared_diff", json!({"v": "right"})).await;
    record_output(&srv, left, "only_left_block", json!({})).await;
    record_output(&srv, right, "only_right_block", json!({})).await;

    let resp = client
        .get(format!("{base}/instances/{left}/compare/{right}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cmp: Value = resp.json().await.unwrap();
    assert_eq!(cmp["only_left"], json!(["only_left_block"]));
    assert_eq!(cmp["only_right"], json!(["only_right_block"]));
    assert_eq!(cmp["differing_outputs"], json!(["shared_diff"]));
    assert_eq!(cmp["matching_blocks"], 1);
}

#[tokio::test]
async fn fork_preview_is_read_only_and_flags_side_effects() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "fetch", "handler": "noop", "params": {}},
            {"type": "step", "id": "charge", "handler": "http_request", "params": {"url": "x"}},
            {"type": "step", "id": "log_it", "handler": "log", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, &client, seq, json!({})).await;
    record_output(&srv, inst, "fetch", json!({"data": 1})).await;

    let resp = client
        .get(format!(
            "{base}/instances/{inst}/fork-preview?from_block_id=charge"
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let preview: Value = resp.json().await.unwrap();
    assert_eq!(preview["copied_blocks"], json!(["fetch"]));
    assert!(
        preview["re_executed_blocks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b == "charge")
    );
    // http_request has external side effects → must be flagged.
    assert_eq!(preview["side_effect_blocks"], json!(["charge"]));
    assert_eq!(preview["sandbox_default"], true);

    // Unknown fork point → 404; nothing mutated either way.
    let resp = client
        .get(format!(
            "{base}/instances/{inst}/fork-preview?from_block_id=ghost"
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_carries_sandbox_marker() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = publish(
        &base,
        &client,
        json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}},
            {"type": "step", "id": "b", "handler": "noop", "params": {}}
        ]),
    )
    .await;
    let inst = spawn_instance(&base, &client, seq, json!({})).await;
    record_output(&srv, inst, "a", json!({"ok": 1})).await;

    let resp = client
        .post(format!("{base}/instances/{inst}/fork"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"from_block_id": "b"}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "{:?}",
        resp.text().await
    );
    // Re-fetch to inspect the fork's metadata.
    let forks: Vec<Value> = Vec::new();
    drop(forks);
    let listed = client
        .get(format!("{base}/instances?tenant_id=t1&limit=50"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let body: Value = listed.json().await.unwrap();
    let items = body
        .get("items")
        .and_then(Value::as_array)
        .cloned()
        .or_else(|| body.as_array().cloned())
        .unwrap();
    let fork = items
        .iter()
        .find(|i| i["metadata"].get("forked_from").is_some())
        .expect("fork exists");
    // Forks default to dry-run → sandbox marker set.
    assert_eq!(fork["metadata"]["sandbox"], true, "{fork}");
}
