//! E2E tests for DLQ root-cause grouping and guided recovery.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

async fn create_sequence(base: &str, client: &reqwest::Client) -> Uuid {
    let id = Uuid::now_v7();
    let body = json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "default",
        "name": format!("dlq-seq-{id}"),
        "version": 1,
        "blocks": [
            {"type": "step", "id": "charge", "handler": "charge_card", "params": {}}
        ],
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

/// Create an instance, plant an error output, and force it into Failed —
/// simulating what the scheduler records on permanent failure.
async fn plant_failed_instance(
    srv: &orch8_api::test_harness::TestServer,
    client: &reqwest::Client,
    base: &str,
    seq_id: Uuid,
    error_message: &str,
) -> Uuid {
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "default",
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let inst: Value = resp.json().await.unwrap();
    let id = Uuid::parse_str(inst["id"].as_str().unwrap()).unwrap();
    let instance_id = InstanceId::from_uuid(id);

    srv.storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id,
            block_id: BlockId::new("charge"),
            output: json!({"__error__": true, "retryable": false, "message": error_message}),
            output_ref: Some("__error__".into()),
            output_size: 0,
            attempt: 1,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    srv.storage
        .update_instance_state(instance_id, InstanceState::Running, None)
        .await
        .unwrap();
    srv.storage
        .update_instance_state(instance_id, InstanceState::Failed, None)
        .await
        .unwrap();
    id
}

#[tokio::test]
async fn same_cause_groups_into_one_incident() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = create_sequence(&base, &client).await;

    // Three failures with the same cause (volatile request ids differ),
    // one with a different cause.
    for req_id in ["req-11111111", "req-22222222", "req-33333333"] {
        plant_failed_instance(
            &srv,
            &client,
            &base,
            seq,
            &format!("http 503 from upstream ({req_id})"),
        )
        .await;
    }
    plant_failed_instance(&srv, &client, &base, seq, "credential 'billing' expired").await;

    let resp = client
        .get(format!("{base}/instances/dlq/groups"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let groups: Vec<Value> = resp.json().await.unwrap();
    assert_eq!(groups.len(), 2, "{groups:#?}");

    // Largest group first.
    assert_eq!(groups[0]["count"], 3);
    assert_eq!(groups[0]["error_code"], "HTTP_STATUS");
    assert_eq!(groups[0]["error_class"], "external_dependency");
    assert_eq!(groups[0]["blocks"], json!(["charge"]));
    assert_eq!(groups[0]["handlers"], json!(["charge_card"]));
    assert_eq!(
        groups[0]["sample_instance_ids"].as_array().unwrap().len(),
        3
    );
    // Components make the grouping explainable.
    assert!(
        groups[0]["components"]
            .as_array()
            .unwrap()
            .iter()
            .any(|c| c == "code:HTTP_STATUS"),
        "{:?}",
        groups[0]["components"]
    );

    assert_eq!(groups[1]["count"], 1);
    assert_eq!(groups[1]["error_code"], "CREDENTIAL");
    assert_eq!(groups[1]["error_class"], "credential");
}

#[tokio::test]
async fn bulk_retry_requires_sample_or_force() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = create_sequence(&base, &client).await;

    for _ in 0..3 {
        plant_failed_instance(&srv, &client, &base, seq, "http 503").await;
    }

    let groups: Vec<Value> = client
        .get(format!("{base}/instances/dlq/groups"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let fp = groups[0]["fingerprint"].as_str().unwrap().to_string();

    // Bulk without sample or force → 409.
    let resp = client
        .post(format!("{base}/instances/dlq/groups/{fp}/retry"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "bulk"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // Sample retry works and reschedules exactly one instance.
    let resp = client
        .post(format!("{base}/instances/dlq/groups/{fp}/retry"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "sample"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let sample: Value = resp.json().await.unwrap();
    assert_eq!(sample["retried"].as_array().unwrap().len(), 1);
    let sample_id = Uuid::parse_str(sample["retried"][0].as_str().unwrap()).unwrap();

    // The sampled instance is now scheduled and carries the group marker.
    let inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(sample_id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inst.state, InstanceState::Scheduled);
    assert_eq!(inst.metadata["dlq_sample_retry"]["fingerprint"], json!(fp));

    // Bulk citing the sample while it has NOT completed → still 409.
    let resp = client
        .post(format!("{base}/instances/dlq/groups/{fp}/retry"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "bulk", "sample_verified_instance_id": sample_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);

    // Simulate the sample completing, then bulk unlocks and retries the
    // remaining two.
    srv.storage
        .update_instance_state(
            InstanceId::from_uuid(sample_id),
            InstanceState::Running,
            None,
        )
        .await
        .unwrap();
    srv.storage
        .update_instance_state(
            InstanceId::from_uuid(sample_id),
            InstanceState::Completed,
            None,
        )
        .await
        .unwrap();
    let resp = client
        .post(format!("{base}/instances/dlq/groups/{fp}/retry"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "bulk", "sample_verified_instance_id": sample_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bulk: Value = resp.json().await.unwrap();
    assert_eq!(bulk["retried"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn bulk_force_overrides_sample_requirement() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = create_sequence(&base, &client).await;
    for _ in 0..2 {
        plant_failed_instance(&srv, &client, &base, seq, "rate limit exhausted").await;
    }

    let groups: Vec<Value> = client
        .get(format!("{base}/instances/dlq/groups"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let fp = groups[0]["fingerprint"].as_str().unwrap();

    let resp = client
        .post(format!("{base}/instances/dlq/groups/{fp}/retry"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "bulk", "force": true, "limit": 1}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bulk: Value = resp.json().await.unwrap();
    // The limit bounds the action.
    assert_eq!(bulk["retried"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn unknown_fingerprint_is_404_and_groups_are_tenant_scoped() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let seq = create_sequence(&base, &client).await;
    plant_failed_instance(&srv, &client, &base, seq, "http 500").await;

    let resp = client
        .post(format!(
            "{base}/instances/dlq/groups/0000000000000000/retry"
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"mode": "sample"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // A different tenant sees no groups.
    let groups: Vec<Value> = client
        .get(format!("{base}/instances/dlq/groups"))
        .header("X-Tenant-Id", "someone-else")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(groups.is_empty(), "{groups:#?}");
}
