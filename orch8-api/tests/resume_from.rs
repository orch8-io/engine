//! E2E tests for `POST /instances/{id}/resume-from/{block_id}` (DLQ surgery).
//!
//! The API test harness has no engine loop, so executed steps are simulated
//! by writing `block_outputs` rows and driving instance state directly
//! through the storage handle exposed by `spawn_test_server()`.

use chrono::Utc;
use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn step(id: &str) -> serde_json::Value {
    json!({
        "type": "step",
        "id": id,
        "handler": "noop",
        "params": {},
        "cancellable": true
    })
}

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "resume-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [step("s1"), step("s2"), step("s3")],
        "interceptors": null,
        "created_at": Utc::now().to_rfc3339()
    })
}

async fn create_sequence(client: &reqwest::Client, base_url: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{base_url}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    seq_id
}

async fn create_instance(client: &reqwest::Client, base_url: &str, seq_id: Uuid) -> Uuid {
    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "seed": 1 }, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{base_url}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    created["id"].as_str().unwrap().parse().unwrap()
}

fn mk_output(inst: Uuid, block: &str) -> BlockOutput {
    BlockOutput {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(inst),
        block_id: BlockId::new(block),
        output: json!({"result": format!("{block}-ok")}),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn resume_from_middle_block_wipes_tail_and_reschedules() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    // Simulate: s1 completed, s2 has a real output, a __retry__ marker and an
    // __in_progress__ sentinel (failed mid-flight), s3 completed.
    srv.storage
        .save_block_output(&mk_output(inst, "s1"))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&mk_output(inst, "s2"))
        .await
        .unwrap();
    let mut retry_marker = mk_output(inst, "s2");
    retry_marker.output_ref = Some("__retry__".into());
    srv.storage.save_block_output(&retry_marker).await.unwrap();
    let mut sentinel = mk_output(inst, "s2");
    sentinel.output_ref = Some("__in_progress__".into());
    srv.storage.save_block_output(&sentinel).await.unwrap();
    srv.storage
        .save_block_output(&mk_output(inst, "s3"))
        .await
        .unwrap();

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Failed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s2", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["state"], "scheduled");
    assert_eq!(body["resumed_from"], "s2");
    // s2 real output + retry marker + sentinel + s3 output = 4 rows.
    assert_eq!(body["outputs_deleted"], 4);

    // Only s1's output survives.
    let outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(inst))
        .await
        .unwrap();
    assert_eq!(outputs.len(), 1);
    assert_eq!(outputs[0].block_id.as_str(), "s1");

    // Instance is back to scheduled with an immediate fire time.
    let resp = client
        .get(format!("{}/instances/{inst}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["state"], "scheduled");
    assert!(fetched["next_fire_at"].is_string());
}

#[tokio::test]
async fn resume_from_applies_context_patch_shallow_merge() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Failed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "context": { "api_key": "fixed", "retries": 3 } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/instances/{inst}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    // Patched keys are merged in; pre-existing keys are preserved.
    assert_eq!(fetched["context"]["data"]["api_key"], "fixed");
    assert_eq!(fetched["context"]["data"]["retries"], 3);
    assert_eq!(fetched["context"]["data"]["seed"], 1);
}

#[tokio::test]
async fn resume_from_rejects_non_object_context_patch() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Failed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "context": [1, 2, 3] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn resume_from_unknown_block_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Failed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!(
            "{}/instances/{inst}/resume-from/no-such-block",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("no-such-block"));
}

#[tokio::test]
async fn resume_from_running_instance_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Running, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("running"));
}

#[tokio::test]
async fn resume_from_completed_instance_reruns_tail() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    // All three blocks completed; instance reached Completed.
    for block in ["s1", "s2", "s3"] {
        srv.storage
            .save_block_output(&mk_output(inst, block))
            .await
            .unwrap();
    }
    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Completed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s3", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["state"], "scheduled");
    assert_eq!(body["outputs_deleted"], 1);

    let outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(inst))
        .await
        .unwrap();
    let mut remaining: Vec<&str> = outputs.iter().map(|o| o.block_id.as_str()).collect();
    remaining.sort_unstable();
    assert_eq!(remaining, ["s1", "s2"]);
}

#[tokio::test]
async fn resume_from_unknown_instance_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/instances/{}/resume-from/s1",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn resume_from_cross_tenant_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Failed, None)
        .await
        .unwrap();

    // A different tenant must not see (or resume) the instance.
    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s1", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Untouched: still failed, outputs intact.
    let resp = client
        .get(format!("{}/instances/{inst}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["state"], "failed");
}

#[tokio::test]
async fn resume_from_paused_instance_works_via_v1_prefix() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Paused, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/resume-from/s1", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["state"], "scheduled");
    assert_eq!(body["outputs_deleted"], 0);
}
