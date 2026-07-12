//! E2E tests for the safe workflow release control plane.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

struct Fixture {
    baseline_id: Uuid,
    candidate_id: Uuid,
}

/// Publish v1 (baseline) and v2 (candidate, adds a step) of one sequence.
async fn publish_versions(base: &str, client: &reqwest::Client) -> Fixture {
    let name = format!("release-seq-{}", Uuid::now_v7());
    let baseline_id = Uuid::now_v7();
    let candidate_id = Uuid::now_v7();
    for (id, version, blocks) in [
        (
            baseline_id,
            1,
            json!([{"type": "step", "id": "work", "handler": "noop", "params": {}}]),
        ),
        (
            candidate_id,
            2,
            json!([
                {"type": "step", "id": "work", "handler": "noop", "params": {}},
                {"type": "step", "id": "extra", "handler": "transform", "params": {}}
            ]),
        ),
    ] {
        let resp = client
            .post(format!("{base}/sequences"))
            .header("X-Tenant-Id", "t1")
            .json(&json!({
                "id": id,
                "tenant_id": "t1",
                "namespace": "default",
                "name": name,
                "version": version,
                "blocks": blocks,
                "created_at": chrono::Utc::now().to_rfc3339(),
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }
    Fixture {
        baseline_id,
        candidate_id,
    }
}

async fn create_release(base: &str, client: &reqwest::Client, fx: &Fixture, gates: Value) -> Value {
    let resp = client
        .post(format!("{base}/releases"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
            "gates": gates,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    resp.json().await.unwrap()
}

async fn post(base: &str, client: &reqwest::Client, path: &str, body: Value) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{base}{path}"))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let value = resp.json().await.unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn release_lifecycle_validate_canary_promote() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &fx, json!([])).await;
    let rid = release["id"].as_str().unwrap().to_string();
    assert_eq!(release["state"], "draft");
    assert_eq!(release["baseline_version"], 1);
    assert_eq!(release["candidate_version"], 2);

    // Semantic diff sees the added block.
    let resp = client
        .get(format!("{base}/releases/{rid}/diff"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let diff: Value = resp.json().await.unwrap();
    assert!(
        diff["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["category"] == "block_added" && e["block_id"] == "extra"),
        "{diff}"
    );

    // Plant one completed historical run of the baseline with a recorded
    // output for 'work' so validation has material.
    let inst_resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
        }))
        .send()
        .await
        .unwrap();
    let inst: Value = inst_resp.json().await.unwrap();
    let inst_id = InstanceId::from_uuid(Uuid::parse_str(inst["id"].as_str().unwrap()).unwrap());
    srv.storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: inst_id,
            block_id: BlockId::new("work"),
            output: json!({"done": true}),
            output_ref: None,
            output_size: 0,
            attempt: 1,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
    srv.storage
        .update_instance_state(inst_id, InstanceState::Running, None)
        .await
        .unwrap();
    srv.storage
        .update_instance_state(inst_id, InstanceState::Completed, None)
        .await
        .unwrap();

    // Validate: the candidate replays offline; the added 'extra' block is
    // a divergence (executed in replay, absent from the recording).
    let (status, report) = post(&base, &client, &format!("/releases/{rid}/validate"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 1);
    assert_eq!(report["release_state"], "ready");
    let divergences = report["divergences"].as_array().unwrap();
    assert_eq!(divergences.len(), 1, "{report}");
    assert!(
        divergences[0]["blocks_added"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b == "extra"),
        "{report}"
    );

    // Start canary at 100% so routing is deterministic in the test.
    let (status, canary) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 100}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{canary}");
    assert_eq!(canary["state"], "canary");

    // New instances targeting the baseline id now route to the candidate.
    let (status, routed) = post(
        &base,
        &client,
        "/instances",
        json!({
            "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
            "idempotency_key": "cohort-check-1",
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let routed_id = InstanceId::from_uuid(
        Uuid::parse_str(routed["id"].as_str().unwrap()).unwrap(),
    );
    let stored = srv.storage.get_instance(routed_id).await.unwrap().unwrap();
    assert_eq!(
        stored.sequence_id.as_uuid(),
        &fx.candidate_id,
        "instance should run the candidate version"
    );
    assert_eq!(stored.metadata["release"]["variant"], "candidate");
    assert_eq!(stored.metadata["release"]["release_id"], rid);

    // Promote (no gates configured → allowed), then routing continues to
    // the candidate and the audit trail is complete.
    let (status, promoted) = post(&base, &client, &format!("/releases/{rid}/promote"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{promoted}");
    assert_eq!(promoted["state"], "promoted");

    let resp = client
        .get(format!("{base}/releases/{rid}/decisions"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let decisions: Vec<Value> = resp.json().await.unwrap();
    let transitions: Vec<String> = decisions
        .iter()
        .map(|d| format!("{}→{}", d["from_state"].as_str().unwrap(), d["to_state"].as_str().unwrap()))
        .collect();
    assert!(transitions.contains(&"draft→validating".to_string()), "{transitions:?}");
    assert!(transitions.contains(&"validating→ready".to_string()), "{transitions:?}");
    assert!(transitions.contains(&"ready→canary".to_string()), "{transitions:?}");
    assert!(transitions.contains(&"canary→promoted".to_string()), "{transitions:?}");
}

#[tokio::test]
async fn failing_gate_auto_rolls_back_and_traffic_returns_to_baseline() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;
    let release = create_release(
        &base,
        &client,
        &fx,
        json!([{"metric": "error_rate", "max_regression": 0.05, "min_sample": 3}]),
    )
    .await;
    let rid = release["id"].as_str().unwrap().to_string();

    // Skip validation, canary at 100% so every new instance is candidate.
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/validate"), json!({"skip": true})).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/canary"), json!({"percent": 100})).await;
    assert_eq!(status, StatusCode::OK);

    // Create 3 routed instances and force them terminal: all failed
    // (candidate error rate 100%). The baseline needs samples too — pin 3
    // instances directly on the candidate=baseline? Baseline variant gets
    // no traffic at 100%; use 50/50 style by planting baseline-annotated
    // instances manually.
    let mut candidate_ids = Vec::new();
    for i in 0..3 {
        let (_, v) = post(
            &base,
            &client,
            "/instances",
            json!({
                "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
                "idempotency_key": format!("gate-c-{i}"),
            }),
        )
        .await;
        candidate_ids.push(
            InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap()),
        );
    }
    for id in &candidate_ids {
        srv.storage.update_instance_state(*id, InstanceState::Running, None).await.unwrap();
        srv.storage.update_instance_state(*id, InstanceState::Failed, None).await.unwrap();
    }
    // Plant 3 healthy baseline-variant observations.
    for i in 0..3 {
        let (_, v) = post(
            &base,
            &client,
            "/instances",
            json!({
                "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
                "idempotency_key": format!("gate-b-{i}"),
            }),
        )
        .await;
        let id = InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap());
        // Rewrite the annotation to the baseline variant (simulating the
        // cohort that stayed on baseline) and move the row there.
        srv.storage
            .merge_instance_metadata(
                id,
                &json!({"release": {"release_id": rid, "variant": "baseline"}}),
            )
            .await
            .unwrap();
        srv.storage
            .update_instance_sequence(id, orch8_types::ids::SequenceId::from_uuid(fx.baseline_id))
            .await
            .unwrap();
        srv.storage.update_instance_state(id, InstanceState::Running, None).await.unwrap();
        srv.storage.update_instance_state(id, InstanceState::Completed, None).await.unwrap();
    }

    // Evaluate: candidate error rate 1.0 vs baseline 0.0 → gate fails →
    // automatic rollback.
    let (status, eval) = post(&base, &client, &format!("/releases/{rid}/evaluate"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "fail", "{eval}");
    assert_eq!(eval["auto_rolled_back"], true, "{eval}");
    assert_eq!(eval["release_state"], "rolled_back");

    // Evaluate again: idempotent, no second rollback decision.
    let (_, eval2) = post(&base, &client, &format!("/releases/{rid}/evaluate"), json!({})).await;
    assert_eq!(eval2["auto_rolled_back"], false);

    // New traffic returns to the baseline: no routing annotation at all.
    let (_, after) = post(
        &base,
        &client,
        "/instances",
        json!({
            "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
            "idempotency_key": "after-rollback",
        }),
    )
    .await;
    let after_id = InstanceId::from_uuid(Uuid::parse_str(after["id"].as_str().unwrap()).unwrap());
    let stored = srv.storage.get_instance(after_id).await.unwrap().unwrap();
    assert_eq!(stored.sequence_id.as_uuid(), &fx.baseline_id);
    assert!(stored.metadata.get("release").is_none(), "{:?}", stored.metadata);

    // Exactly one rollback decision in the audit trail.
    let resp = client
        .get(format!("{base}/releases/{rid}/decisions"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let decisions: Vec<Value> = resp.json().await.unwrap();
    let rollbacks = decisions
        .iter()
        .filter(|d| d["to_state"] == "rolled_back")
        .count();
    assert_eq!(rollbacks, 1);
    assert!(
        decisions
            .iter()
            .any(|d| d["actor"].as_str().unwrap().starts_with("gate:")),
        "{decisions:#?}"
    );
}

#[tokio::test]
async fn cohort_routing_is_deterministic_at_partial_percent() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &fx, json!([])).await;
    let rid = release["id"].as_str().unwrap().to_string();
    post(&base, &client, &format!("/releases/{rid}/validate"), json!({"skip": true})).await;
    post(&base, &client, &format!("/releases/{rid}/canary"), json!({"percent": 40})).await;

    // Same cohort key (idempotency key) always lands on the same variant:
    // creating with the same key dedupes, so use metadata cohort_key with
    // distinct idempotency keys instead.
    let mut variants = Vec::new();
    for attempt in 0..3 {
        let (_, v) = post(
            &base,
            &client,
            "/instances",
            json!({
                "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
                "idempotency_key": format!("det-{attempt}"),
                "metadata": {"cohort_key": "customer-42"},
            }),
        )
        .await;
        let id = InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap());
        let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
        variants.push(stored.metadata["release"]["variant"].as_str().unwrap().to_string());
    }
    assert!(
        variants.windows(2).all(|w| w[0] == w[1]),
        "same cohort key must be stable: {variants:?}"
    );
}

#[tokio::test]
async fn create_rejects_mismatched_sequences_and_duplicate_routing() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;

    // Same id on both sides.
    let (status, _) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.baseline_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A second routing release for the same baseline is rejected while
    // one is active.
    let release = create_release(&base, &client, &fx, json!([])).await;
    let rid = release["id"].as_str().unwrap().to_string();
    post(&base, &client, &format!("/releases/{rid}/validate"), json!({"skip": true})).await;
    post(&base, &client, &format!("/releases/{rid}/canary"), json!({"percent": 10})).await;
    let (status, _) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn illegal_transitions_are_conflicts_and_tenant_isolated() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &fx, json!([])).await;
    let rid = release["id"].as_str().unwrap().to_string();

    // Canary from draft (must validate or skip first) → 409.
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/canary"), json!({"percent": 10})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Promote from draft → 409.
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/promote"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Another tenant cannot see the release.
    let resp = client
        .get(format!("{base}/releases/{rid}"))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
