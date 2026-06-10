//! E2E tests for the time-travel operations:
//! `GET /instances/{id}/timeline` and `POST /instances/{id}/fork`.
//!
//! The API test harness has no engine loop, so executed steps are simulated
//! by writing `block_outputs` rows and driving instance state directly
//! through the storage handle exposed by `spawn_test_server()` (same pattern
//! as `resume_from.rs`).

use chrono::{Duration, Utc};
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
        "name": "timeline-fork-seq",
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
        "metadata": { "owner": "alice" },
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

/// An output row with a deterministic timestamp offset so ordering
/// assertions are stable.
fn mk_output_at(inst: Uuid, block: &str, offset_secs: i64) -> BlockOutput {
    BlockOutput {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(inst),
        block_id: BlockId::new(block),
        output: json!({"result": format!("{block}-ok")}),
        output_ref: None,
        output_size: 0,
        attempt: 0,
        created_at: Utc::now() - Duration::seconds(100) + Duration::seconds(offset_secs),
    }
}

// ====================================================================
// Timeline
// ====================================================================

#[tokio::test]
async fn timeline_returns_entries_in_execution_order_with_sentinels_flagged() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    // s1 ok, then a retry marker for s2, then s2's real output, then s3.
    srv.storage
        .save_block_output(&mk_output_at(inst, "s1", 0))
        .await
        .unwrap();
    let mut retry_marker = mk_output_at(inst, "s2", 1);
    retry_marker.output_ref = Some("__retry__".into());
    retry_marker.attempt = 1;
    srv.storage.save_block_output(&retry_marker).await.unwrap();
    srv.storage
        .save_block_output(&mk_output_at(inst, "s2", 2))
        .await
        .unwrap();
    srv.storage
        .save_block_output(&mk_output_at(inst, "s3", 3))
        .await
        .unwrap();

    let resp = client
        .get(format!("{}/instances/{inst}/timeline", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    // Instance-level summary: id, current state, context.
    assert_eq!(body["instance"]["id"], inst.to_string());
    assert_eq!(body["instance"]["state"], "scheduled");
    assert_eq!(body["instance"]["context"]["data"]["seed"], 1);
    assert!(body["instance"]["created_at"].is_string());

    // Entries in chronological execution order; sentinel flagged, not hidden.
    let entries = body["entries"].as_array().unwrap();
    let ids: Vec<&str> = entries
        .iter()
        .map(|e| e["block_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["s1", "s2", "s2", "s3"]);
    assert_eq!(entries[0]["is_sentinel"], false);
    assert_eq!(entries[1]["is_sentinel"], true);
    assert_eq!(entries[1]["output_ref"], "__retry__");
    assert_eq!(entries[1]["attempt"], 1);
    assert_eq!(entries[2]["is_sentinel"], false);
    assert_eq!(entries[2]["output"]["result"], "s2-ok");
    assert!(entries[3]["completed_at"].is_string());
    assert_eq!(body["has_more"], false);
}

#[tokio::test]
async fn timeline_pagination_pages_through_entries() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    for (i, block) in ["s1", "s2", "s3"].iter().enumerate() {
        srv.storage
            .save_block_output(&mk_output_at(inst, block, i64::try_from(i).unwrap()))
            .await
            .unwrap();
    }

    let resp = client
        .get(format!(
            "{}/instances/{inst}/timeline?limit=2&offset=0",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let page1: serde_json::Value = resp.json().await.unwrap();
    let entries = page1["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["block_id"], "s1");
    assert_eq!(entries[1]["block_id"], "s2");
    assert_eq!(page1["has_more"], true);
    assert_eq!(page1["limit"], 2);
    assert_eq!(page1["offset"], 0);

    let resp = client
        .get(format!(
            "{}/instances/{inst}/timeline?limit=2&offset=2",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let page2: serde_json::Value = resp.json().await.unwrap();
    let entries = page2["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["block_id"], "s3");
    assert_eq!(page2["has_more"], false);
}

#[tokio::test]
async fn timeline_include_outputs_false_returns_metadata_only() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .save_block_output(&mk_output_at(inst, "s1", 0))
        .await
        .unwrap();

    let resp = client
        .get(format!(
            "{}/instances/{inst}/timeline?include_outputs=false",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();

    let entry = &body["entries"][0];
    assert_eq!(entry["block_id"], "s1");
    assert!(entry.get("output").is_none(), "output payload omitted");
    assert!(entry["completed_at"].is_string());
    assert!(
        body["instance"].get("context").is_none(),
        "context omitted in metadata-only mode"
    );
}

#[tokio::test]
async fn timeline_cross_tenant_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .get(format!("{}/instances/{inst}/timeline", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ====================================================================
// Fork
// ====================================================================

#[tokio::test]
async fn fork_from_middle_block_copies_pre_fork_outputs_and_leaves_source_untouched() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    for (i, block) in ["s1", "s2", "s3"].iter().enumerate() {
        srv.storage
            .save_block_output(&mk_output_at(inst, block, i64::try_from(i).unwrap()))
            .await
            .unwrap();
    }
    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Completed, None)
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s3" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["forked_from"], inst.to_string());
    assert_eq!(body["state"], "scheduled");
    assert_eq!(body["copied_blocks"], 2);
    assert_eq!(body["rerun_blocks"].as_array().unwrap().len(), 0);
    assert_eq!(body["dry_run"], true, "dry-run is the default");

    let fork_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    assert_ne!(fork_id, inst, "fork is a new instance");

    // The fork carries copies of s1 and s2's outputs — s3 will (re-)run.
    let fork_outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(fork_id))
        .await
        .unwrap();
    let mut blocks: Vec<&str> = fork_outputs.iter().map(|o| o.block_id.as_str()).collect();
    blocks.sort_unstable();
    assert_eq!(blocks, ["s1", "s2"]);
    assert!(
        fork_outputs
            .iter()
            .all(|o| o.instance_id.into_uuid() == fork_id),
        "copied rows belong to the fork"
    );

    // Fork instance: scheduled now, dry-run stamped, provenance metadata,
    // source metadata preserved.
    let fork_inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(fork_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fork_inst.state, InstanceState::Scheduled);
    assert!(fork_inst.next_fire_at.is_some());
    assert!(fork_inst.context.runtime.dry_run);
    assert_eq!(fork_inst.metadata["forked_from"], inst.to_string());
    assert_eq!(fork_inst.metadata["forked_at_block"], "s3");
    assert_eq!(fork_inst.metadata["owner"], "alice");
    assert!(fork_inst.idempotency_key.is_none());

    // Source untouched: still completed, all three outputs intact.
    let source = srv
        .storage
        .get_instance(InstanceId::from_uuid(inst))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.state, InstanceState::Completed);
    assert!(!source.context.runtime.dry_run);
    let source_outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(inst))
        .await
        .unwrap();
    assert_eq!(source_outputs.len(), 3);
}

#[tokio::test]
async fn fork_applies_context_patch_without_touching_source_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .save_block_output(&mk_output_at(inst, "s1", 0))
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "from_block_id": "s2",
            "context": { "api_key": "sandbox", "retries": 3 },
            "dry_run": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["dry_run"], false);
    let fork_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();

    let fork_inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(fork_id))
        .await
        .unwrap()
        .unwrap();
    // Patched keys merged in; pre-existing keys preserved; dry-run off.
    assert_eq!(fork_inst.context.data["api_key"], "sandbox");
    assert_eq!(fork_inst.context.data["retries"], 3);
    assert_eq!(fork_inst.context.data["seed"], 1);
    assert!(!fork_inst.context.runtime.dry_run);

    // Source context unchanged.
    let source = srv
        .storage
        .get_instance(InstanceId::from_uuid(inst))
        .await
        .unwrap()
        .unwrap();
    assert!(source.context.data.get("api_key").is_none());
}

#[tokio::test]
async fn fork_from_first_block_copies_nothing() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    for (i, block) in ["s1", "s2", "s3"].iter().enumerate() {
        srv.storage
            .save_block_output(&mk_output_at(inst, block, i64::try_from(i).unwrap()))
            .await
            .unwrap();
    }

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["copied_blocks"], 0);
    assert_eq!(body["rerun_blocks"].as_array().unwrap().len(), 0);

    let fork_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let fork_outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(fork_id))
        .await
        .unwrap();
    assert!(fork_outputs.is_empty(), "full re-run: nothing copied");
}

#[tokio::test]
async fn fork_artifact_backed_pre_fork_block_goes_to_rerun_set() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    // s1's output was externalized (artifact-backed): its payload reference
    // is keyed by the SOURCE instance and cannot be shared with the fork.
    let mut externalized = mk_output_at(inst, "s1", 0);
    externalized.output = json!({"_externalized": true, "_ref": format!("{inst}:s1")});
    externalized.output_ref = Some(format!("{inst}:s1"));
    srv.storage.save_block_output(&externalized).await.unwrap();
    // s2 completed inline.
    srv.storage
        .save_block_output(&mk_output_at(inst, "s2", 1))
        .await
        .unwrap();

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s3" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let body: serde_json::Value = resp.json().await.unwrap();
    // s1 re-runs (artifact-backed), s2 was copied.
    assert_eq!(body["copied_blocks"], 1);
    assert_eq!(body["rerun_blocks"], json!(["s1"]));

    let fork_id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    let fork_outputs = srv
        .storage
        .get_all_outputs(InstanceId::from_uuid(fork_id))
        .await
        .unwrap();
    assert_eq!(fork_outputs.len(), 1);
    assert_eq!(fork_outputs[0].block_id.as_str(), "s2");
    assert!(
        fork_outputs[0].output_ref.is_none(),
        "only inline rows are ever copied"
    );
}

#[tokio::test]
async fn fork_is_allowed_from_a_running_source() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    srv.storage
        .save_block_output(&mk_output_at(inst, "s1", 0))
        .await
        .unwrap();
    srv.storage
        .update_instance_state(InstanceId::from_uuid(inst), InstanceState::Running, None)
        .await
        .unwrap();

    // Fork is a read + clone: unlike resume-from, the source does not need
    // to be quiescent.
    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s2" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let source = srv
        .storage
        .get_instance(InstanceId::from_uuid(inst))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(source.state, InstanceState::Running, "source untouched");
}

#[tokio::test]
async fn fork_unknown_block_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "no-such-block" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("no-such-block"));
}

#[tokio::test]
async fn fork_rejects_non_object_context_patch() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s2", "context": [1, 2, 3] }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn fork_cross_tenant_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst = create_instance(&client, &srv.base_url, seq_id).await;

    let resp = client
        .post(format!("{}/instances/{inst}/fork", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&json!({ "from_block_id": "s2" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn fork_unknown_instance_returns_404_via_v1_prefix() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!(
            "{}/instances/{}/fork",
            srv.v1_url(),
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "from_block_id": "s1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
