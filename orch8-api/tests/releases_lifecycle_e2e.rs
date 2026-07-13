//! E2E lifecycle tests for the safe workflow release control plane:
//! every legal/illegal HTTP transition, historical validation, canary
//! routing, gate evaluation, promotion, audit, tenant isolation, diff
//! endpoints, and listing.

use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{InstanceStore, OutputStore};
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::instance::InstanceState;
use orch8_types::output::BlockOutput;
use orch8_types::release::{ReleaseVariant, assign_variant};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

struct Fixture {
    baseline_id: Uuid,
    candidate_id: Uuid,
}

async fn setup() -> (TestServer, reqwest::Client, String) {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    (srv, client, base)
}

#[allow(clippy::too_many_arguments)]
async fn publish_sequence(
    base: &str,
    client: &reqwest::Client,
    tenant: &str,
    namespace: &str,
    name: &str,
    id: Uuid,
    version: i32,
    blocks: Value,
) {
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "id": id,
            "tenant_id": tenant,
            "namespace": namespace,
            "name": name,
            "version": version,
            "blocks": blocks,
            "created_at": chrono::Utc::now().to_rfc3339(),
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "publishing {name} v{version}"
    );
}

fn baseline_blocks() -> Value {
    json!([{"type": "step", "id": "work", "handler": "noop", "params": {}}])
}

fn candidate_blocks_with_extra() -> Value {
    json!([
        {"type": "step", "id": "work", "handler": "noop", "params": {}},
        {"type": "step", "id": "extra", "handler": "transform", "params": {}}
    ])
}

/// Publish v1 (baseline) and v2 (candidate adds a step) of one sequence.
async fn publish_versions(base: &str, client: &reqwest::Client) -> Fixture {
    publish_versions_with(base, client, candidate_blocks_with_extra()).await
}

/// Publish v1 (baseline) and v2 identical to v1 (replays match exactly).
async fn publish_identical_versions(base: &str, client: &reqwest::Client) -> Fixture {
    publish_versions_with(base, client, baseline_blocks()).await
}

async fn publish_versions_with(
    base: &str,
    client: &reqwest::Client,
    candidate_blocks: Value,
) -> Fixture {
    let name = format!("release-seq-{}", Uuid::now_v7());
    let baseline_id = Uuid::now_v7();
    let candidate_id = Uuid::now_v7();
    publish_sequence(
        base,
        client,
        "t1",
        "default",
        &name,
        baseline_id,
        1,
        baseline_blocks(),
    )
    .await;
    publish_sequence(
        base,
        client,
        "t1",
        "default",
        &name,
        candidate_id,
        2,
        candidate_blocks,
    )
    .await;
    Fixture {
        baseline_id,
        candidate_id,
    }
}

async fn post_t(
    base: &str,
    client: &reqwest::Client,
    tenant: &str,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{base}{path}"))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let value = resp.json().await.unwrap_or(Value::Null);
    (status, value)
}

async fn post(
    base: &str,
    client: &reqwest::Client,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
    post_t(base, client, "t1", path, body).await
}

async fn get_t(
    base: &str,
    client: &reqwest::Client,
    tenant: &str,
    path: &str,
) -> (StatusCode, Value) {
    let resp = client
        .get(format!("{base}{path}"))
        .header("X-Tenant-Id", tenant)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let value = resp.json().await.unwrap_or(Value::Null);
    (status, value)
}

async fn get(base: &str, client: &reqwest::Client, path: &str) -> (StatusCode, Value) {
    get_t(base, client, "t1", path).await
}

async fn create_release(
    base: &str,
    client: &reqwest::Client,
    fx: &Fixture,
    gates: Value,
) -> String {
    let (status, release) = post(
        base,
        client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
            "gates": gates,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{release}");
    release["id"].as_str().unwrap().to_string()
}

/// Create a release and skip validation → `ready`.
async fn ready_release(base: &str, client: &reqwest::Client, fx: &Fixture, gates: Value) -> String {
    let rid = create_release(base, client, fx, gates).await;
    let (status, _) = post(
        base,
        client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    rid
}

/// Create + skip validation + start canary at `percent` → `canary`.
async fn canary_release(
    base: &str,
    client: &reqwest::Client,
    fx: &Fixture,
    gates: Value,
    percent: u8,
) -> String {
    let rid = ready_release(base, client, fx, gates).await;
    let (status, body) = post(
        base,
        client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": percent}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    rid
}

/// Create an instance and return its id (asserts 201).
async fn spawn_instance(
    base: &str,
    client: &reqwest::Client,
    sequence_id: Uuid,
    idempotency_key: &str,
    metadata: Value,
) -> InstanceId {
    let (status, v) = post(
        base,
        client,
        "/instances",
        json!({
            "sequence_id": sequence_id, "tenant_id": "t1", "namespace": "default",
            "idempotency_key": idempotency_key,
            "metadata": metadata,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{v}");
    InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap())
}

/// Drive an instance to a terminal state through storage.
async fn finish(srv: &TestServer, id: InstanceId, terminal: InstanceState) {
    srv.storage
        .update_instance_state(id, InstanceState::Running, None)
        .await
        .unwrap();
    srv.storage
        .update_instance_state(id, terminal, None)
        .await
        .unwrap();
}

/// Record an output for block `work` so validation replays it verbatim.
async fn record_work_output(srv: &TestServer, id: InstanceId) {
    srv.storage
        .save_block_output(&BlockOutput {
            id: Uuid::now_v7(),
            instance_id: id,
            block_id: BlockId::new("work"),
            output: json!({"done": true}),
            output_ref: None,
            output_size: 0,
            attempt: 1,
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();
}

/// Plant `n` terminal candidate-variant observations (canary must be at
/// 100% so every new instance routes to the candidate).
async fn plant_candidate_observations(
    srv: &TestServer,
    base: &str,
    client: &reqwest::Client,
    fx: &Fixture,
    prefix: &str,
    n: usize,
    terminal: InstanceState,
) {
    for i in 0..n {
        let id = spawn_instance(
            base,
            client,
            fx.baseline_id,
            &format!("{prefix}-c-{i}"),
            json!({}),
        )
        .await;
        finish(srv, id, terminal).await;
    }
}

/// Plant `n` terminal baseline-variant observations by rewriting the
/// routing annotation (simulating the cohort that stayed on baseline).
#[allow(clippy::too_many_arguments)]
async fn plant_baseline_observations(
    srv: &TestServer,
    base: &str,
    client: &reqwest::Client,
    fx: &Fixture,
    rid: &str,
    prefix: &str,
    n: usize,
    terminal: InstanceState,
) {
    for i in 0..n {
        let id = spawn_instance(
            base,
            client,
            fx.baseline_id,
            &format!("{prefix}-b-{i}"),
            json!({}),
        )
        .await;
        srv.storage
            .merge_instance_metadata(
                id,
                &json!({"release": {"release_id": rid, "variant": "baseline"}}),
            )
            .await
            .unwrap();
        srv.storage
            .update_instance_sequence(id, SequenceId::from_uuid(fx.baseline_id))
            .await
            .unwrap();
        finish(srv, id, terminal).await;
    }
}

fn error_gate(max_regression: f64, min_sample: u32) -> Value {
    json!([{"metric": "error_rate", "max_regression": max_regression, "min_sample": min_sample}])
}

// ===========================================================================
// Create-time validations
// ===========================================================================

#[tokio::test]
async fn create_release_returns_draft_with_versions_and_gates() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let (status, release) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
            "gates": error_gate(0.05, 10),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{release}");
    assert_eq!(release["state"], "draft");
    assert_eq!(release["baseline_version"], 1);
    assert_eq!(release["candidate_version"], 2);
    assert_eq!(release["canary_percent"], 0);
    assert_eq!(release["gates"][0]["metric"], "error_rate");
    assert_eq!(release["gates"][0]["min_sample"], 10);
    assert!(release.get("canary_started_at").is_none(), "{release}");
}

#[tokio::test]
async fn create_rejects_same_sequence_on_both_sides() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
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
}

#[tokio::test]
async fn create_rejects_cross_name_sequences() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    // A sequence with a completely different name cannot be the candidate.
    let other_id = Uuid::now_v7();
    publish_sequence(
        &base,
        &client,
        "t1",
        "default",
        &format!("unrelated-{}", Uuid::now_v7()),
        other_id,
        1,
        baseline_blocks(),
    )
    .await;
    let (status, body) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": other_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn create_rejects_cross_namespace_sequences() {
    let (_srv, client, base) = setup().await;
    let name = format!("cross-ns-{}", Uuid::now_v7());
    let baseline_id = Uuid::now_v7();
    let candidate_id = Uuid::now_v7();
    publish_sequence(
        &base,
        &client,
        "t1",
        "default",
        &name,
        baseline_id,
        1,
        baseline_blocks(),
    )
    .await;
    publish_sequence(
        &base,
        &client,
        "t1",
        "other",
        &name,
        candidate_id,
        2,
        baseline_blocks(),
    )
    .await;
    let (status, body) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": baseline_id,
            "candidate_sequence_id": candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn create_with_missing_baseline_is_404() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let (status, _) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": Uuid::now_v7(),
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_with_missing_candidate_is_404() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let (status, _) = post(
        &base,
        &client,
        "/releases",
        json!({
            "tenant_id": "t1",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": Uuid::now_v7(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn second_release_allowed_while_first_is_draft() {
    // Only *routing* releases (canary/promoted) block a new release.
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let _first = create_release(&base, &client, &fx, json!([])).await;
    let (status, body) = post(
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
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

#[tokio::test]
async fn second_routing_release_conflicts_during_canary() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let _rid = canary_release(&base, &client, &fx, json!([]), 10).await;
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
async fn release_creation_allowed_again_after_rollback() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 10).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = post(
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
    assert_eq!(status, StatusCode::CREATED, "{body}");
}

// ===========================================================================
// State machine over HTTP: illegal transitions are 409s
// ===========================================================================

#[tokio::test]
async fn canary_from_draft_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn promote_from_draft_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn pause_from_draft_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn rollback_from_draft_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn validate_skip_moves_draft_to_ready() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 0);
    assert_eq!(report["matches"], 0);
    assert_eq!(report["inconclusive"], 0);
    assert_eq!(report["release_state"], "ready");
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "ready");
}

#[tokio::test]
async fn validate_twice_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn promote_from_ready_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "ready");
}

#[tokio::test]
async fn pause_from_ready_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn rollback_from_ready_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn canary_from_ready_succeeds() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, release) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 25}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{release}");
    assert_eq!(release["state"], "canary");
    assert_eq!(release["canary_percent"], 25);
}

#[tokio::test]
async fn canary_from_canary_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 50}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn pause_then_resume_canary() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    let (status, paused) = post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    assert_eq!(status, StatusCode::OK, "{paused}");
    assert_eq!(paused["state"], "paused");
    // Paused → canary resumes.
    let (status, resumed) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 25}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resumed}");
    assert_eq!(resumed["state"], "canary");
}

#[tokio::test]
async fn resume_canary_can_change_percent() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 10).await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    let (status, resumed) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 90}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{resumed}");
    assert_eq!(resumed["canary_percent"], 90);
}

#[tokio::test]
async fn paused_rollback_succeeds() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    let (status, rolled) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rolled}");
    assert_eq!(rolled["state"], "rolled_back");
}

#[tokio::test]
async fn paused_promote_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "paused");
}

#[tokio::test]
async fn paused_pause_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    let (status, _) = post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn canary_rollback_succeeds() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    let (status, rolled) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rolled}");
    assert_eq!(rolled["state"], "rolled_back");
}

#[tokio::test]
async fn promoted_release_is_frozen() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 100).await;
    let (status, promoted) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{promoted}");
    assert_eq!(promoted["state"], "promoted");

    // Every further action is rejected: promoted is terminal.
    for (path, body) in [
        ("promote", json!({})),
        ("promote", json!({"force": true})),
        ("pause", json!({})),
        ("rollback", json!({})),
        ("canary", json!({"percent": 50})),
        ("validate", json!({"skip": true})),
    ] {
        let (status, resp) = post(&base, &client, &format!("/releases/{rid}/{path}"), body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{path} after promote: {resp}");
    }
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "promoted");
}

#[tokio::test]
async fn double_promote_conflicts() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 100).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn rollback_is_idempotent_with_single_decision() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    let (status, first) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    // Repeating the rollback is a 200 no-op, not a conflict.
    let (status, second) = post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["state"], "rolled_back");

    let (_, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    let rollbacks = decisions
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["to_state"] == "rolled_back")
        .count();
    assert_eq!(rollbacks, 1, "{decisions:#?}");
}

#[tokio::test]
async fn rolled_back_release_rejects_canary_and_promote() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 25).await;
    post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;
    for (path, body) in [
        ("canary", json!({"percent": 25})),
        ("promote", json!({})),
        ("pause", json!({})),
        ("validate", json!({"skip": true})),
    ] {
        let (status, resp) = post(&base, &client, &format!("/releases/{rid}/{path}"), body).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "{path} after rollback: {resp}"
        );
    }
}

// ===========================================================================
// Historical validation
// ===========================================================================

#[tokio::test]
async fn validate_with_matching_history_reports_matches() {
    let (srv, client, base) = setup().await;
    // Candidate is byte-identical to the baseline: replays must match.
    let fx = publish_identical_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    for i in 0..2 {
        let id = spawn_instance(
            &base,
            &client,
            fx.baseline_id,
            &format!("hist-{i}"),
            json!({}),
        )
        .await;
        record_work_output(&srv, id).await;
        finish(&srv, id, InstanceState::Completed).await;
    }
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 2, "{report}");
    assert_eq!(report["matches"], 2, "{report}");
    assert_eq!(
        report["divergences"].as_array().unwrap().len(),
        0,
        "{report}"
    );
    assert_eq!(report["inconclusive"], 0, "{report}");
    assert_eq!(report["release_state"], "ready");
}

#[tokio::test]
async fn validate_reports_divergence_for_added_block() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await; // candidate adds "extra"
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let id = spawn_instance(&base, &client, fx.baseline_id, "hist-div", json!({})).await;
    record_work_output(&srv, id).await;
    finish(&srv, id, InstanceState::Completed).await;

    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 1);
    assert_eq!(report["matches"], 0);
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
}

#[tokio::test]
async fn validate_reports_state_mismatch_divergence() {
    let (srv, client, base) = setup().await;
    let fx = publish_identical_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    // Recorded run failed, but the candidate replay completes: the state
    // mismatch alone is a divergence (no block-path difference).
    let id = spawn_instance(&base, &client, fx.baseline_id, "hist-failed", json!({})).await;
    record_work_output(&srv, id).await;
    finish(&srv, id, InstanceState::Failed).await;

    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    let divergences = report["divergences"].as_array().unwrap();
    assert_eq!(divergences.len(), 1, "{report}");
    assert_eq!(divergences[0]["recorded_state"], "failed", "{report}");
    assert_eq!(divergences[0]["replayed_state"], "completed", "{report}");
    assert_eq!(divergences[0]["blocks_added"].as_array().unwrap().len(), 0);
    assert_eq!(
        divergences[0]["blocks_removed"].as_array().unwrap().len(),
        0
    );
}

#[tokio::test]
async fn validate_sample_clamps_to_minimum_one() {
    let (srv, client, base) = setup().await;
    let fx = publish_identical_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    for i in 0..3 {
        let id = spawn_instance(
            &base,
            &client,
            fx.baseline_id,
            &format!("clamp-lo-{i}"),
            json!({}),
        )
        .await;
        record_work_output(&srv, id).await;
        finish(&srv, id, InstanceState::Completed).await;
    }
    // sample=0 clamps to 1: exactly one replay despite 3 available runs.
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"sample": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 1, "{report}");
}

#[tokio::test]
async fn validate_sample_clamps_to_maximum_fifty() {
    let (srv, client, base) = setup().await;
    let fx = publish_identical_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    // 52 terminal runs, none with recorded outputs (each replay diverges,
    // and the divergence detail list is bounded at 20).
    for i in 0..52 {
        let id = spawn_instance(
            &base,
            &client,
            fx.baseline_id,
            &format!("clamp-hi-{i}"),
            json!({}),
        )
        .await;
        finish(&srv, id, InstanceState::Completed).await;
    }
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"sample": 9999}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 50, "{report}");
    assert_eq!(report["matches"], 0, "{report}");
    assert_eq!(
        report["divergences"].as_array().unwrap().len(),
        20,
        "divergence detail is bounded: {report}"
    );
}

#[tokio::test]
async fn validate_with_no_history_still_concludes_ready() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["replayed"], 0);
    assert_eq!(report["release_state"], "ready");
}

#[tokio::test]
async fn validation_summary_persisted_on_release() {
    let (srv, client, base) = setup().await;
    let fx = publish_identical_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let id = spawn_instance(&base, &client, fx.baseline_id, "summary-1", json!({})).await;
    record_work_output(&srv, id).await;
    finish(&srv, id, InstanceState::Completed).await;

    post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;

    let (status, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(status, StatusCode::OK);
    let summary = &release["validation_summary"];
    assert_eq!(summary["replayed"], 1, "{release}");
    assert_eq!(summary["matches"], 1, "{release}");
    assert!(summary["validated_at"].is_string(), "{release}");

    // The audit trail records validation as the actor.
    let (_, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    assert!(
        decisions.as_array().unwrap().iter().any(|d| {
            d["from_state"] == "validating"
                && d["to_state"] == "ready"
                && d["actor"] == "validation"
        }),
        "{decisions:#?}"
    );
}

// ===========================================================================
// Canary percent bounds
// ===========================================================================

#[tokio::test]
async fn canary_percent_zero_is_rejected() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 0}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // And the release did not move.
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "ready");
}

#[tokio::test]
async fn canary_percent_over_hundred_is_rejected() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 101}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn canary_percent_hundred_is_accepted_and_recorded() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;
    let (status, release) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 100}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{release}");
    assert_eq!(release["canary_percent"], 100);
    assert!(release["canary_started_at"].is_string(), "{release}");
}

// ===========================================================================
// Cohort routing
// ===========================================================================

#[tokio::test]
async fn cohort_determinism_via_metadata_cohort_key() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 40).await;
    let release_uuid = Uuid::parse_str(&rid).unwrap();

    // No idempotency key: metadata.cohort_key is the cohort. Repeated
    // creations must all land on the same variant — and exactly the one
    // `assign_variant` computes for that key.
    let expected = match assign_variant(release_uuid, "customer-42", 40) {
        ReleaseVariant::Baseline => "baseline",
        ReleaseVariant::Candidate => "candidate",
    };
    for attempt in 0..4 {
        let (status, v) = post(
            &base,
            &client,
            "/instances",
            json!({
                "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
                "metadata": {"cohort_key": "customer-42"},
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{v}");
        let id = InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap());
        let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
        let variant = stored.metadata["release"]["variant"].as_str().unwrap();
        assert_eq!(variant, expected, "attempt {attempt}: cohort flapped");
    }

    // Actual precedence: a non-empty idempotency key *overrides* the
    // metadata cohort_key as the cohort.
    let id = spawn_instance(
        &base,
        &client,
        fx.baseline_id,
        "precedence-check",
        json!({"cohort_key": "customer-42"}),
    )
    .await;
    let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
    let expected_idem = match assign_variant(release_uuid, "precedence-check", 40) {
        ReleaseVariant::Baseline => "baseline",
        ReleaseVariant::Candidate => "candidate",
    };
    assert_eq!(
        stored.metadata["release"]["variant"].as_str().unwrap(),
        expected_idem,
        "idempotency key must take precedence as the cohort key"
    );
}

#[tokio::test]
async fn idempotency_key_cohorts_match_assign_variant() {
    // The idempotency key IS the cohort key; the routed variant must be
    // exactly what `assign_variant` computes for it.
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 50).await;
    let release_uuid = Uuid::parse_str(&rid).unwrap();

    for i in 0..6 {
        let key = format!("cohort-key-{i}");
        let expected = assign_variant(release_uuid, &key, 50);
        let id = spawn_instance(&base, &client, fx.baseline_id, &key, json!({})).await;
        let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
        let variant = stored.metadata["release"]["variant"].as_str().unwrap();
        let expected_str = match expected {
            ReleaseVariant::Baseline => "baseline",
            ReleaseVariant::Candidate => "candidate",
        };
        assert_eq!(variant, expected_str, "key {key}");
        let expected_seq = match expected {
            ReleaseVariant::Baseline => fx.baseline_id,
            ReleaseVariant::Candidate => fx.candidate_id,
        };
        assert_eq!(stored.sequence_id.as_uuid(), &expected_seq, "key {key}");
    }
}

#[tokio::test]
async fn promoted_release_routes_all_new_instances_to_candidate() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 1).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Even cohorts that were baseline at 1% now land on the candidate.
    for i in 0..5 {
        let id = spawn_instance(
            &base,
            &client,
            fx.baseline_id,
            &format!("post-promote-{i}"),
            json!({}),
        )
        .await;
        let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
        assert_eq!(
            stored.sequence_id.as_uuid(),
            &fx.candidate_id,
            "instance {i}"
        );
        assert_eq!(stored.metadata["release"]["variant"], "candidate");
        assert_eq!(stored.metadata["release"]["release_id"], rid);
    }
}

#[tokio::test]
async fn paused_release_routes_to_baseline_without_annotation() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 100).await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;

    let id = spawn_instance(&base, &client, fx.baseline_id, "while-paused", json!({})).await;
    let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(stored.sequence_id.as_uuid(), &fx.baseline_id);
    assert!(
        stored.metadata.get("release").is_none(),
        "paused release must not annotate: {:?}",
        stored.metadata
    );
}

#[tokio::test]
async fn rollback_returns_traffic_to_baseline_without_annotation() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 100).await;

    // While the canary runs, traffic goes to the candidate.
    let during = spawn_instance(&base, &client, fx.baseline_id, "during-canary", json!({})).await;
    let stored = srv.storage.get_instance(during).await.unwrap().unwrap();
    assert_eq!(stored.sequence_id.as_uuid(), &fx.candidate_id);

    post(
        &base,
        &client,
        &format!("/releases/{rid}/rollback"),
        json!({}),
    )
    .await;

    let after = spawn_instance(&base, &client, fx.baseline_id, "after-rollback", json!({})).await;
    let stored = srv.storage.get_instance(after).await.unwrap().unwrap();
    assert_eq!(stored.sequence_id.as_uuid(), &fx.baseline_id);
    assert!(
        stored.metadata.get("release").is_none(),
        "{:?}",
        stored.metadata
    );
}

#[tokio::test]
async fn direct_candidate_targeting_bypasses_release_routing() {
    // Explicit version selection stays authoritative: targeting the
    // candidate id directly never consults the release.
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let _rid = canary_release(&base, &client, &fx, json!([]), 100).await;

    let id = spawn_instance(
        &base,
        &client,
        fx.candidate_id,
        "direct-candidate",
        json!({}),
    )
    .await;
    let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
    assert_eq!(stored.sequence_id.as_uuid(), &fx.candidate_id);
    assert!(
        stored.metadata.get("release").is_none(),
        "direct targeting must not annotate: {:?}",
        stored.metadata
    );
}

// ===========================================================================
// Gate evaluation
// ===========================================================================

#[tokio::test]
async fn evaluate_inconclusive_does_not_roll_back() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 5), 100).await;
    // Only 2 candidate observations against min_sample 5 (all failed —
    // looks terrible, but the sample is too small to conclude).
    plant_candidate_observations(&srv, &base, &client, &fx, "inc", 2, InstanceState::Failed).await;

    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "inconclusive", "{eval}");
    assert_eq!(eval["auto_rolled_back"], false);
    assert_eq!(eval["release_state"], "canary");
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "canary");
}

#[tokio::test]
async fn evaluate_failing_gate_rolls_back_exactly_once() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 3), 100).await;
    plant_candidate_observations(&srv, &base, &client, &fx, "fail", 3, InstanceState::Failed).await;
    plant_baseline_observations(
        &srv,
        &base,
        &client,
        &fx,
        &rid,
        "fail",
        3,
        InstanceState::Completed,
    )
    .await;

    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "fail", "{eval}");
    assert_eq!(eval["candidate"]["total"], 3, "{eval}");
    assert_eq!(eval["candidate"]["failed"], 3, "{eval}");
    assert_eq!(eval["baseline"]["total"], 3, "{eval}");
    assert_eq!(eval["auto_rolled_back"], true, "{eval}");
    assert_eq!(eval["release_state"], "rolled_back");

    // Re-evaluating repeatedly stays idempotent.
    for _ in 0..2 {
        let (_, again) = post(
            &base,
            &client,
            &format!("/releases/{rid}/evaluate"),
            json!({}),
        )
        .await;
        assert_eq!(again["auto_rolled_back"], false, "{again}");
        assert_eq!(again["release_state"], "rolled_back");
    }

    // Exactly one rollback decision, authored by the gate.
    let (_, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    let rollbacks: Vec<&Value> = decisions
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["to_state"] == "rolled_back")
        .collect();
    assert_eq!(rollbacks.len(), 1, "{decisions:#?}");
    assert!(
        rollbacks[0]["actor"].as_str().unwrap().starts_with("gate:"),
        "{decisions:#?}"
    );
}

#[tokio::test]
async fn evaluate_passing_gate_keeps_canary_and_allows_promote() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 3), 100).await;
    plant_candidate_observations(
        &srv,
        &base,
        &client,
        &fx,
        "pass",
        3,
        InstanceState::Completed,
    )
    .await;
    plant_baseline_observations(
        &srv,
        &base,
        &client,
        &fx,
        &rid,
        "pass",
        3,
        InstanceState::Completed,
    )
    .await;

    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "pass", "{eval}");
    assert_eq!(eval["auto_rolled_back"], false);
    assert_eq!(eval["release_state"], "canary");

    // With all gates passing, promotion needs no force.
    let (status, promoted) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{promoted}");
    assert_eq!(promoted["state"], "promoted");
}

#[tokio::test]
async fn evaluate_with_no_gates_reports_empty_and_never_rolls_back() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, json!([]), 100).await;
    plant_candidate_observations(
        &srv,
        &base,
        &client,
        &fx,
        "nogate",
        3,
        InstanceState::Failed,
    )
    .await;

    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"].as_array().unwrap().len(), 0, "{eval}");
    assert_eq!(eval["auto_rolled_back"], false);
    assert_eq!(eval["release_state"], "canary");
}

#[tokio::test]
async fn evaluate_ignores_instances_not_routed_by_the_release() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.0, 1), 100).await;

    // A directly-targeted candidate instance (no routing annotation)
    // fails — it must not count toward the candidate's gate stats.
    let direct = spawn_instance(&base, &client, fx.candidate_id, "direct-fail", json!({})).await;
    finish(&srv, direct, InstanceState::Failed).await;

    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["candidate"]["total"], 0, "{eval}");
    assert_eq!(eval["baseline"]["total"], 0, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "inconclusive", "{eval}");
    assert_eq!(eval["auto_rolled_back"], false);
}

#[tokio::test]
async fn evaluate_after_promotion_never_rolls_back() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 2), 100).await;
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({"force": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Outcomes recorded after promotion fail the gate on paper…
    plant_candidate_observations(&srv, &base, &client, &fx, "post", 2, InstanceState::Failed).await;
    plant_baseline_observations(
        &srv,
        &base,
        &client,
        &fx,
        &rid,
        "post",
        2,
        InstanceState::Completed,
    )
    .await;

    // …but a promoted release is frozen: no automatic rollback.
    let (status, eval) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "fail", "{eval}");
    assert_eq!(eval["auto_rolled_back"], false);
    assert_eq!(eval["release_state"], "promoted");
    let (_, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    assert!(
        decisions
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["to_state"] != "rolled_back"),
        "{decisions:#?}"
    );
}

// ===========================================================================
// Promotion gate enforcement
// ===========================================================================

#[tokio::test]
async fn promote_blocked_by_inconclusive_gates() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 5), 100).await;
    // No observations at all: gates inconclusive → promotion refused.
    let (status, body) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "canary");
}

#[tokio::test]
async fn promote_force_overrides_inconclusive_gates() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.05, 5), 100).await;
    let (status, promoted) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({"force": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{promoted}");
    assert_eq!(promoted["state"], "promoted");
    // The audit trail marks the forced promotion.
    let (_, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    assert!(
        decisions.as_array().unwrap().iter().any(|d| {
            d["to_state"] == "promoted" && d["reason"].as_str().unwrap().contains("forced")
        }),
        "{decisions:#?}"
    );
}

#[tokio::test]
async fn promote_blocked_by_failing_gates() {
    let (srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = canary_release(&base, &client, &fx, error_gate(0.0, 1), 100).await;
    plant_candidate_observations(&srv, &base, &client, &fx, "pf", 1, InstanceState::Failed).await;
    plant_baseline_observations(
        &srv,
        &base,
        &client,
        &fx,
        &rid,
        "pf",
        1,
        InstanceState::Completed,
    )
    .await;

    let (status, body) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{body}");
    // Refusing promotion does not itself roll back.
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "canary");
}

// ===========================================================================
// Decision audit trail
// ===========================================================================

#[tokio::test]
async fn decisions_audit_is_complete_and_ordered() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 20}),
    )
    .await;
    post(&base, &client, &format!("/releases/{rid}/pause"), json!({})).await;
    post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 40}),
    )
    .await;
    post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;

    let (status, decisions) = get(&base, &client, &format!("/releases/{rid}/decisions")).await;
    assert_eq!(status, StatusCode::OK);
    let decisions = decisions.as_array().unwrap();
    let transitions: Vec<(String, String)> = decisions
        .iter()
        .map(|d| {
            (
                d["from_state"].as_str().unwrap().to_string(),
                d["to_state"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    let expected: Vec<(String, String)> = [
        ("draft", "draft"), // creation record
        ("draft", "ready"),
        ("ready", "canary"),
        ("canary", "paused"),
        ("paused", "canary"),
        ("canary", "promoted"),
    ]
    .iter()
    .map(|(a, b)| ((*a).to_string(), (*b).to_string()))
    .collect();
    assert_eq!(transitions, expected, "{decisions:#?}");

    // Oldest-first: timestamps never decrease; every decision carries an
    // actor and a reason and points at this release.
    let mut prev: Option<chrono::DateTime<chrono::Utc>> = None;
    for d in decisions {
        assert_eq!(d["release_id"], rid.as_str(), "{d}");
        assert_eq!(d["actor"], "operator", "{d}");
        assert!(!d["reason"].as_str().unwrap().is_empty(), "{d}");
        let at: chrono::DateTime<chrono::Utc> = d["decided_at"].as_str().unwrap().parse().unwrap();
        if let Some(p) = prev {
            assert!(at >= p, "decisions out of order: {decisions:#?}");
        }
        prev = Some(at);
    }
    // The canary decisions record the requested percentages.
    assert!(
        decisions
            .iter()
            .any(|d| d["reason"].as_str().unwrap().contains("20%")),
        "{decisions:#?}"
    );
    assert!(
        decisions
            .iter()
            .any(|d| d["reason"].as_str().unwrap().contains("40%")),
        "{decisions:#?}"
    );
}

// ===========================================================================
// Tenant isolation
// ===========================================================================

#[tokio::test]
async fn get_release_is_tenant_isolated() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, _) = get_t(&base, &client, "intruder", &format!("/releases/{rid}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    // The owner still sees it.
    let (status, _) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn release_actions_are_tenant_isolated() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = ready_release(&base, &client, &fx, json!([])).await;

    for (path, body) in [
        ("validate", json!({"skip": true})),
        ("canary", json!({"percent": 10})),
        ("evaluate", json!({})),
        ("promote", json!({})),
        ("pause", json!({})),
        ("rollback", json!({})),
    ] {
        let (status, resp) = post_t(
            &base,
            &client,
            "intruder",
            &format!("/releases/{rid}/{path}"),
            body,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path}: {resp}");
    }
    let (status, _) = get_t(
        &base,
        &client,
        "intruder",
        &format!("/releases/{rid}/decisions"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = get_t(&base, &client, "intruder", &format!("/releases/{rid}/diff")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Nothing moved.
    let (_, release) = get(&base, &client, &format!("/releases/{rid}")).await;
    assert_eq!(release["state"], "ready");
}

#[tokio::test]
async fn create_release_with_foreign_sequences_is_404() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await; // owned by t1
    let (status, _) = post_t(
        &base,
        &client,
        "intruder",
        "/releases",
        json!({
            "tenant_id": "intruder",
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn list_releases_scoped_by_header_tenant() {
    let (_srv, client, base) = setup().await;
    // One release for t1…
    let fx1 = publish_versions(&base, &client).await;
    let rid1 = create_release(&base, &client, &fx1, json!([])).await;
    // …and one for t2.
    let name = format!("t2-seq-{}", Uuid::now_v7());
    let b2 = Uuid::now_v7();
    let c2 = Uuid::now_v7();
    publish_sequence(
        &base,
        &client,
        "t2",
        "default",
        &name,
        b2,
        1,
        baseline_blocks(),
    )
    .await;
    publish_sequence(
        &base,
        &client,
        "t2",
        "default",
        &name,
        c2,
        2,
        candidate_blocks_with_extra(),
    )
    .await;
    let (status, r2) = post_t(
        &base,
        &client,
        "t2",
        "/releases",
        json!({
            "tenant_id": "t2",
            "baseline_sequence_id": b2,
            "candidate_sequence_id": c2,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{r2}");
    let rid2 = r2["id"].as_str().unwrap().to_string();

    let (_, t1_list) = get_t(&base, &client, "t1", "/releases").await;
    let t1_ids: Vec<&str> = t1_list
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(t1_ids.contains(&rid1.as_str()), "{t1_list}");
    assert!(
        !t1_ids.contains(&rid2.as_str()),
        "t2 release leaked: {t1_list}"
    );

    let (_, t2_list) = get_t(&base, &client, "t2", "/releases").await;
    let t2_ids: Vec<&str> = t2_list
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    assert!(t2_ids.contains(&rid2.as_str()), "{t2_list}");
    assert!(
        !t2_ids.contains(&rid1.as_str()),
        "t1 release leaked: {t2_list}"
    );
}

// ===========================================================================
// Diff endpoints
// ===========================================================================

#[tokio::test]
async fn diff_by_release_id_reports_added_block() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (status, diff) = get(&base, &client, &format!("/releases/{rid}/diff")).await;
    assert_eq!(status, StatusCode::OK, "{diff}");
    assert!(
        diff["entries"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["category"] == "block_added" && e["block_id"] == "extra"),
        "{diff}"
    );
    assert_eq!(diff["max_severity"], "behavioral", "{diff}");
}

#[tokio::test]
async fn diff_by_body_matches_release_diff() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let rid = create_release(&base, &client, &fx, json!([])).await;
    let (_, by_id) = get(&base, &client, &format!("/releases/{rid}/diff")).await;
    let (status, by_body) = post(
        &base,
        &client,
        "/sequences/releases/diff",
        json!({
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{by_body}");
    assert_eq!(by_body, by_id, "the two diff endpoints must agree");
}

#[tokio::test]
async fn diff_by_body_with_unknown_sequences_is_404() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await;
    let (status, _) = post(
        &base,
        &client,
        "/sequences/releases/diff",
        json!({
            "baseline_sequence_id": Uuid::now_v7(),
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = post(
        &base,
        &client,
        "/sequences/releases/diff",
        json!({
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": Uuid::now_v7(),
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn diff_by_body_is_tenant_isolated() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await; // t1 sequences
    let (status, _) = post_t(
        &base,
        &client,
        "intruder",
        "/sequences/releases/diff",
        json!({
            "baseline_sequence_id": fx.baseline_id,
            "candidate_sequence_id": fx.candidate_id,
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

// ===========================================================================
// Listing
// ===========================================================================

#[tokio::test]
async fn list_releases_newest_first() {
    let (_srv, client, base) = setup().await;
    let fx1 = publish_versions(&base, &client).await;
    let first = create_release(&base, &client, &fx1, json!([])).await;
    // Distinct creation timestamps so the order is unambiguous.
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let fx2 = publish_versions(&base, &client).await;
    let second = create_release(&base, &client, &fx2, json!([])).await;

    let (status, list) = get(&base, &client, "/releases").await;
    assert_eq!(status, StatusCode::OK);
    let ids: Vec<&str> = list
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["id"].as_str().unwrap())
        .collect();
    let pos_first = ids.iter().position(|i| *i == first).unwrap();
    let pos_second = ids.iter().position(|i| *i == second).unwrap();
    assert!(pos_second < pos_first, "newest first: {ids:?}");
}

#[tokio::test]
async fn list_releases_respects_limit() {
    let (_srv, client, base) = setup().await;
    for _ in 0..3 {
        let fx = publish_versions(&base, &client).await;
        create_release(&base, &client, &fx, json!([])).await;
    }
    let (status, list) = get(&base, &client, "/releases?limit=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(list.as_array().unwrap().len(), 1, "{list}");
    let (_, all) = get(&base, &client, "/releases").await;
    assert_eq!(all.as_array().unwrap().len(), 3, "{all}");
}

#[tokio::test]
async fn list_releases_filtered_by_query_tenant_without_header() {
    let (_srv, client, base) = setup().await;
    let fx = publish_versions(&base, &client).await; // t1
    let rid = create_release(&base, &client, &fx, json!([])).await;

    // No X-Tenant-Id header: the query parameter scopes the list.
    let resp = client
        .get(format!("{base}/releases?tenant_id=t1"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let list: Value = resp.json().await.unwrap();
    assert!(
        list.as_array()
            .unwrap()
            .iter()
            .any(|r| r["id"] == rid.as_str()),
        "{list}"
    );

    let resp = client
        .get(format!("{base}/releases?tenant_id=nobody"))
        .send()
        .await
        .unwrap();
    let list: Value = resp.json().await.unwrap();
    assert_eq!(list.as_array().unwrap().len(), 0, "{list}");
}
