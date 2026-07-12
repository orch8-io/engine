//! E2E tests for `GET /instances/{id}/diagnosis`.

use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

async fn create_sequence(base: &str, client: &reqwest::Client, blocks: Value) -> String {
    let id = Uuid::now_v7().to_string();
    let body = json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "default",
        "name": format!("diag-seq-{id}"),
        "version": 1,
        "blocks": blocks,
        "created_at": chrono::Utc::now().to_rfc3339(),
    });
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    id
}

async fn create_instance(base: &str, client: &reqwest::Client, seq_id: &str, extra: Value) -> String {
    let mut body = json!({
        "tenant_id": "t1",
        "sequence_id": seq_id,
        "namespace": "default",
    });
    if let (Some(obj), Some(extra_obj)) = (body.as_object_mut(), extra.as_object()) {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: Value = resp.json().await.unwrap();
    v["id"].as_str().unwrap().to_string()
}

fn codes(report: &Value) -> Vec<String> {
    report["diagnoses"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn future_timer_diagnosed_as_waiting_until() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let seq_id = create_sequence(
        &base,
        &client,
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    )
    .await;
    let fire_at = chrono::Utc::now() + chrono::Duration::hours(6);
    let inst = create_instance(
        &base,
        &client,
        &seq_id,
        json!({"next_fire_at": fire_at.to_rfc3339()}),
    )
    .await;

    let resp = client
        .get(format!("{base}/instances/{inst}/diagnosis"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["diagnoses"][0]["code"], "WAITING_UNTIL");
    assert_eq!(report["diagnoses"][0]["category"], "direct_evidence");
    assert_eq!(report["diagnoses"][0]["confidence"], "certain");
}

#[tokio::test]
async fn diagnosis_is_tenant_isolated() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let seq_id = create_sequence(
        &base,
        &client,
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    )
    .await;
    let inst = create_instance(&base, &client, &seq_id, json!({})).await;

    let resp = client
        .get(format!("{base}/instances/{inst}/diagnosis"))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn unknown_instance_is_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/instances/{}/diagnosis",
            srv.v1_url(),
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn due_instance_reports_no_blockers_or_lag_only() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let seq_id = create_sequence(
        &base,
        &client,
        json!([{"type": "step", "id": "a", "handler": "noop", "params": {}}]),
    )
    .await;
    let inst = create_instance(&base, &client, &seq_id, json!({})).await;

    let resp = client
        .get(format!("{base}/instances/{inst}/diagnosis"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let report: Value = resp.json().await.unwrap();
    // Freshly created + no scheduler running in the test server: either no
    // blocker found or nothing at all — but never a hard failure code.
    let cs = codes(&report);
    assert!(
        cs.iter()
            .all(|c| c == "NO_BLOCKER_FOUND" || c == "EVIDENCE_INCOMPLETE" || c == "SCHEDULER_LAG"),
        "{cs:?}"
    );
}
