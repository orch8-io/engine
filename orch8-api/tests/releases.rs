//! E2E tests for the safe workflow release control plane.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{InstanceStore, OutputStore, SequenceStore};
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

#[tokio::test]
async fn release_inventory_filters_before_paging_and_breaks_timestamp_ties() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let timestamp = "2026-11-27T12:30:00.123456789Z";
    let first = "10000000-0000-4000-8000-000000000001";
    let second = "10000000-0000-4000-8000-000000000002";
    for (id, tenant, name, state) in [
        (first, "t1", "Invoice % follow-up", "ready"),
        (second, "t1", "INVOICE % intake", "ready"),
        (
            "10000000-0000-4000-8000-000000000003",
            "t1",
            "Invoice % draft",
            "draft",
        ),
        (
            "10000000-0000-4000-8000-000000000004",
            "t2",
            "Invoice % foreign",
            "ready",
        ),
        (
            "10000000-0000-4000-8000-000000000005",
            "t1",
            "Invoice without marker",
            "ready",
        ),
    ] {
        let release = serde_json::from_value(json!({
            "id":id,"tenant_id":tenant,"namespace":"Billing","sequence_name":name,
            "baseline_sequence_id":Uuid::nil(),"baseline_version":1,
            "candidate_sequence_id":Uuid::nil(),"candidate_version":-2,"state":state,
            "canary_percent":0,"created_at":timestamp,"updated_at":timestamp
        }))
        .unwrap();
        srv.storage.create_release(&release).await.unwrap();
    }
    let query = [
        ("view", "page"),
        ("page_size", "1"),
        ("search", "billing invoice % v-2"),
        ("release_state", "ready"),
    ];
    let read = || {
        client
            .get(format!("{}/releases", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .query(&query)
    };
    let page: Value = read()
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"][0]["id"], second);
    assert_eq!(page["next_before"]["id"], second);
    let next = page["next_before"]["created_at"].as_str().unwrap();
    let older: Value = read()
        .query(&[("before_id", second), ("before_created_at", next)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(older["items"][0]["id"], first);
    assert!(older["next_before"].is_null());
    assert_eq!(older["before"], page["next_before"]);
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
    let release: Value = resp.json().await.unwrap();
    let id = Uuid::parse_str(release["id"].as_str().unwrap()).unwrap();
    assert_eq!(id.get_version_num(), 4);
    release
}

async fn post(
    base: &str,
    client: &reqwest::Client,
    path: &str,
    body: Value,
) -> (StatusCode, Value) {
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
async fn canary_requires_each_designated_executor_to_have_current_revision_readiness() {
    use orch8_storage::AdminStore as _;
    use orch8_types::cluster::{ClusterNode, NodeStatus};
    use orch8_types::ids::SequenceId;

    let server = spawn_test_server().await;
    let base = server.v1_url();
    let client = reqwest::Client::new();
    let versions = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &versions, json!([])).await;
    let path = format!("/releases/{}", release["id"].as_str().unwrap());
    assert_eq!(
        post(
            &base,
            &client,
            &format!("{path}/validate"),
            json!({"skip":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    let now = chrono::Utc::now();
    let ids = [Uuid::now_v7(), Uuid::now_v7()];
    for id in ids {
        server
            .storage
            .register_node(&ClusterNode {
                id,
                name: id.to_string(),
                status: NodeStatus::Active,
                registered_at: now,
                last_heartbeat_at: now,
                drain: false,
                drain_started_at: None,
                stopped_at: None,
                capabilities_withdrawn: false,
                execution_handoff_evidence: None,
            })
            .await
            .unwrap();
    }
    let candidate = SequenceId::from_uuid(versions.candidate_id);
    let baseline = SequenceId::from_uuid(versions.baseline_id);
    let expiry = now + chrono::Duration::seconds(90);
    server
        .storage
        .record_wasm_revision_ready(ids[0], candidate, expiry)
        .await
        .unwrap();
    server
        .storage
        .record_wasm_revision_ready(ids[1], baseline, expiry)
        .await
        .unwrap();
    let start = format!("{path}/canary");
    let request = json!({"percent":10, "ready_executor_ids": ids});
    assert_eq!(
        post(&base, &client, &start, request.clone()).await.0,
        StatusCode::CONFLICT
    );
    server
        .storage
        .record_wasm_revision_ready(ids[1], candidate, now - chrono::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(
        post(&base, &client, &start, request.clone()).await.0,
        StatusCode::CONFLICT
    );
    server
        .storage
        .record_wasm_revision_ready(ids[1], candidate, expiry)
        .await
        .unwrap();
    assert_eq!(
        post(&base, &client, &start, request).await.0,
        StatusCode::OK
    );
    server.storage.drain_node(ids[1]).await.unwrap();
    assert_eq!(
        post(
            &base,
            &client,
            &format!("{path}/promote"),
            json!({"force":true, "ready_executor_ids":ids})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // Keep the release and executor lifecycle in one walkthrough.
async fn executor_readiness_inventory_and_checked_transitions_use_the_current_candidate() {
    use orch8_api::test_harness::spawn_test_server_on;
    use orch8_types::cluster::{ClusterNode, NodeStatus};
    use orch8_types::ids::SequenceId;
    use orch8_types::release::ReleaseState;

    let fixture = spawn_test_server().await;
    let server = spawn_test_server_on(fixture.storage.clone(), Some("release-test-root")).await;
    let client = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::HeaderName::from_static("x-api-key"),
            reqwest::header::HeaderValue::from_static("release-test-root"),
        )]))
        .build()
        .unwrap();
    let base = server.v1_url();
    let versions = publish_versions(&base, &client).await;
    let release = create_release(
        &base,
        &client,
        &versions,
        json!([{"metric":"error_rate","max_regression":0.05,"min_sample":1}]),
    )
    .await;
    let path = format!("/releases/{}", release["id"].as_str().unwrap());
    assert_eq!(
        post(
            &base,
            &client,
            &format!("{path}/validate"),
            json!({"skip":true})
        )
        .await
        .0,
        StatusCode::OK
    );

    let executor = Uuid::now_v7();
    let now = chrono::Utc::now();
    let node = ClusterNode {
        id: executor,
        name: "worker-a".into(),
        status: NodeStatus::Active,
        registered_at: now,
        last_heartbeat_at: now,
        drain: false,
        drain_started_at: None,
        stopped_at: None,
        capabilities_withdrawn: false,
        execution_handoff_evidence: None,
    };
    server.storage.register_node(&node).await.unwrap();
    let endpoint = format!("{path}/executor-readiness");
    let inspect = |tenant| {
        client
            .get(format!("{base}{endpoint}"))
            .header("X-Tenant-Id", tenant)
            .send()
    };
    assert_eq!(inspect("t2").await.unwrap().status(), StatusCode::NOT_FOUND);
    let read: Value = inspect("t1").await.unwrap().json().await.unwrap();
    assert_eq!(read["release_id"], release["id"]);
    assert_eq!(read["candidate_sequence_id"], json!(versions.candidate_id));
    assert_eq!(
        read["executors"],
        json!([{"id":executor,"name":"worker-a","ready":false,"readiness_status":"awaiting_candidate"}])
    );

    let start = json!({"action":"canary","percent":23,"ready_executor_ids":[executor]});
    assert_eq!(
        post(
            &base,
            &client,
            &endpoint,
            json!({"action":"canary","percent":23,"ready_executor_ids":[]})
        )
        .await
        .0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        post(&base, &client, &endpoint, start.clone()).await.0,
        StatusCode::CONFLICT
    );
    let stored = server
        .storage
        .get_release(Uuid::parse_str(release["id"].as_str().unwrap()).unwrap())
        .await
        .unwrap()
        .unwrap();
    server
        .storage
        .record_wasm_revision_ready(executor, stored.candidate_sequence_id, now)
        .await
        .unwrap();
    assert!(
        !server
            .storage
            .cas_release_state_with_executors(&stored, ReleaseState::Canary, Some(23), &[executor])
            .await
            .unwrap()
    );
    assert_eq!(
        server
            .storage
            .get_release(stored.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ReleaseState::Ready
    );
    server
        .storage
        .record_wasm_revision_ready(
            executor,
            SequenceId::from_uuid(versions.candidate_id),
            now + chrono::Duration::seconds(90),
        )
        .await
        .unwrap();
    let read: Value = inspect("t1").await.unwrap().json().await.unwrap();
    assert_eq!(read["executors"][0]["readiness_status"], "ready");
    let (status, fresh) = post(&base, &client, &endpoint, start).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(fresh["canary_percent"], 23);
    assert_eq!(fresh["state"], "canary");
    server.storage.drain_node(executor).await.unwrap();
    let read: Value = inspect("t1").await.unwrap().json().await.unwrap();
    assert_eq!(read["executors"][0]["readiness_status"], "draining");
    assert_eq!(read["executors"][0]["ready"], false);
    assert_eq!(
        post(
            &base,
            &client,
            &endpoint,
            json!({"action":"promote","force":true,"ready_executor_ids":[executor]})
        )
        .await
        .0,
        StatusCode::CONFLICT
    );
    let decisions: Vec<Value> = client
        .get(format!("{base}{path}/decisions"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let canary: Vec<_> = decisions
        .iter()
        .filter(|decision| decision["to_state"] == "canary")
        .collect();
    assert_eq!(canary.len(), 1);
    assert_eq!(
        canary[0]["reason"],
        "canary at 23% · readiness verified for 1 executor"
    );
    assert!(
        decisions
            .iter()
            .all(|decision| decision["to_state"] != "promoted")
    );
    let replacement = Uuid::now_v7();
    server
        .storage
        .register_node(&ClusterNode {
            id: replacement,
            ..node
        })
        .await
        .unwrap();
    server
        .storage
        .record_wasm_revision_ready(
            replacement,
            SequenceId::from_uuid(versions.candidate_id),
            now + chrono::Duration::seconds(90),
        )
        .await
        .unwrap();
    let (rejected, _) = post(
        &base,
        &client,
        &endpoint,
        json!({"action":"promote","ready_executor_ids":[replacement]}),
    )
    .await;
    assert_eq!(rejected, StatusCode::CONFLICT);
    let (snapshot, _) = read_release_snapshot(&base, &client, &path).await;
    let (status, promoted) = post(
        &base,
        &client,
        &format!("{path}/rollout"),
        json!({
            "snapshot":snapshot,
            "transition":{"action":"promote","force":true,"ready_executor_ids":[replacement]}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(promoted["state"], "promoted");
}

async fn read_release_snapshot(
    base: &str,
    client: &reqwest::Client,
    path: &str,
) -> (String, Value) {
    let response = client
        .get(format!("{base}{path}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    assert_eq!(response.headers()["cache-control"], "no-store");
    let proof = response.headers()["x-orch8-release-snapshot"]
        .to_str()
        .unwrap()
        .to_owned();
    (proof, response.json().await.unwrap())
}

#[tokio::test]
async fn guarded_rollout_rejects_a_snapshot_taken_before_a_target_edit() {
    let fixture = spawn_test_server().await;
    let server = orch8_api::test_harness::spawn_test_server_on(
        fixture.storage.clone(),
        Some("guarded-rollout-test-root"),
    )
    .await;
    let key = orch8_types::api_key::mint("t1", "rollout-test", None);
    orch8_storage::AdminStore::create_api_key(server.storage.as_ref(), &key.record)
        .await
        .unwrap();
    let client = reqwest::Client::builder()
        .default_headers(reqwest::header::HeaderMap::from_iter([(
            reqwest::header::HeaderName::from_static("x-api-key"),
            reqwest::header::HeaderValue::from_str(&key.secret).unwrap(),
        )]))
        .build()
        .unwrap();
    let base = server.v1_url();
    let versions = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &versions, json!([])).await;
    let path = format!("/releases/{}", release["id"].as_str().unwrap());
    let guarded = format!("{path}/rollout");
    assert_eq!(
        post(
            &base,
            &client,
            &format!("{path}/validate"),
            json!({"skip":true})
        )
        .await
        .0,
        StatusCode::OK
    );
    let read = || read_release_snapshot(&base, &client, &path);
    let (snapshot, observed) = read().await;
    assert_eq!(observed["state"], "ready");
    assert_eq!(read().await.0, snapshot);
    let start = json!({"action":"canary","percent":25,"ready_executor_ids":[]});
    assert_eq!(
        client
            .put(format!("{base}{path}/target"))
            .header("X-Tenant-Id", "t1")
            .json(&json!({"percentage":40}))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let (status, conflict) = post(
        &base,
        &client,
        &guarded,
        json!({"snapshot":snapshot,"transition":start}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"]["code"], "release_rollout_not_applied");
    let (fresh, still_ready) = read().await;
    assert_ne!(fresh, snapshot);
    assert_eq!(still_ready["state"], "ready");
    let (status, _) = post(
        &base,
        &client,
        &guarded,
        json!({
            "snapshot":fresh,
            "transition":{"action":"canary","percent":25,"ready_executor_ids":[Uuid::now_v7()]}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, canary) = post(
        &base,
        &client,
        &guarded,
        json!({"snapshot":fresh,"transition":start}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(canary["state"], "canary");
    let (ready_to_promote, _) = read().await;
    let (status, promoted) = post(&base, &client, &guarded,
        json!({"snapshot":ready_to_promote,"transition":{"action":"promote","ready_executor_ids":[]}})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(promoted["state"], "promoted");
    assert_eq!(promoted["target"]["percentage"], 40);
}

#[tokio::test]
async fn guarded_target_rejects_a_stale_snapshot_and_requires_an_explicit_target() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let versions = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &versions, json!([])).await;
    let path = format!("/releases/{}", release["id"].as_str().unwrap());
    let (original, _) = read_release_snapshot(&base, &client, &path).await;
    let put = |body: Value| {
        client
            .put(format!("{base}{path}/target/guarded"))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
    };
    assert!(
        put(json!({"snapshot":original}))
            .await
            .unwrap()
            .status()
            .is_client_error()
    );
    assert!(
        put(json!({"snapshot":original,"target":{"percentag":25}}))
            .await
            .unwrap()
            .status()
            .is_client_error()
    );
    assert_eq!(
        put(json!({"snapshot":original,"target":{"percentage":25}}))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    let conflict = put(json!({"snapshot":original,"target":null}))
        .await
        .unwrap();
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(
        conflict.json::<Value>().await.unwrap()["error"]["code"],
        "release_target_not_applied"
    );
    let (fresh, stored) = read_release_snapshot(&base, &client, &path).await;
    assert_eq!(stored["target"]["percentage"], 25);
    assert_eq!(
        put(json!({"snapshot":fresh,"target":null}))
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(read_release_snapshot(&base, &client, &path).await.1["target"].is_null());
}

#[tokio::test]
async fn guarded_pause_and_rollback_require_the_reviewed_snapshot() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let versions = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &versions, json!([])).await;
    let path = format!("/releases/{}", release["id"].as_str().unwrap());
    let guarded = format!("{path}/rollout");
    let canary = format!("{path}/canary");
    let validate = format!("{path}/validate");
    assert_eq!(
        post(&base, &client, &validate, json!({"skip":true}))
            .await
            .0,
        StatusCode::OK
    );
    assert_eq!(
        post(&base, &client, &canary, json!({"percent":25})).await.0,
        StatusCode::OK
    );
    let (original, _) = read_release_snapshot(&base, &client, &path).await;
    let update = client
        .put(format!("{base}{path}/target/guarded"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"snapshot":original,"target":{"percentage":30}}))
        .send()
        .await
        .unwrap();
    assert_eq!(update.status(), StatusCode::OK);

    let (current, stored) = read_release_snapshot(&base, &client, &path).await;
    assert_eq!(stored["target"]["percentage"], 30);
    assert_ne!(original, current);
    let (status, error) = post(
        &base,
        &client,
        &guarded,
        json!({"snapshot":original,"transition":{"action":"pause"}}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["error"]["code"], "release_rollout_not_applied");
    let (status, row) = post(
        &base,
        &client,
        &guarded,
        json!({"snapshot":current,"transition":{"action":"pause"}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["state"], "paused");
    let (paused, _) = read_release_snapshot(&base, &client, &path).await;
    let rollback = json!({"snapshot":paused,"transition":{"action":"rollback"}});
    let (status, row) = post(&base, &client, &guarded, rollback.clone()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row["state"], "rolled_back");
    assert_eq!(
        post(&base, &client, &guarded, rollback).await.0,
        StatusCode::CONFLICT
    );
    let (finished, _) = read_release_snapshot(&base, &client, &path).await;
    assert_eq!(
        post(
            &base,
            &client,
            &guarded,
            json!({"snapshot":finished,"transition":{"action":"rollback"}})
        )
        .await
        .0,
        StatusCode::OK
    );
}

#[tokio::test]
async fn guarded_validation_only_starts_for_the_reviewed_draft() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    for options in [json!({"sample":2}), json!({"skip":true})] {
        let versions = publish_versions(&base, &client).await;
        let release = create_release(&base, &client, &versions, json!([])).await;
        let path = format!("/releases/{}", release["id"].as_str().unwrap());
        let guarded = format!("{path}/validate/guarded");
        let (reviewed, _) = read_release_snapshot(&base, &client, &path).await;
        let response = client
            .put(format!("{base}{path}/target/guarded"))
            .header("X-Tenant-Id", "t1")
            .json(&json!({"snapshot":reviewed,"target":{"percentage":30}}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let (status, error) = post(
            &base,
            &client,
            &guarded,
            json!({"snapshot":reviewed,"options":options}),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(error["error"]["code"], "release_validation_not_applied");
        let (fresh, draft) = read_release_snapshot(&base, &client, &path).await;
        assert_eq!(draft["state"], "draft");
        let (status, report) = post(
            &base,
            &client,
            &guarded,
            json!({"snapshot":fresh,"options":options}),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(report["release_state"], "ready");
        let (_, ready) = read_release_snapshot(&base, &client, &path).await;
        assert_eq!(ready["state"], "ready");
        assert_eq!(ready["target"]["percentage"], 30);
        assert_eq!(
            ready["validation_summary"].is_null(),
            options["skip"] == true
        );
    }
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // full lifecycle walkthrough reads better linear
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
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
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
    let routed_id = InstanceId::from_uuid(Uuid::parse_str(routed["id"].as_str().unwrap()).unwrap());
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
    let (status, promoted) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
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
        .map(|d| {
            format!(
                "{}→{}",
                d["from_state"].as_str().unwrap(),
                d["to_state"].as_str().unwrap()
            )
        })
        .collect();
    assert!(
        transitions.contains(&"draft→validating".to_string()),
        "{transitions:?}"
    );
    assert!(
        transitions.contains(&"validating→ready".to_string()),
        "{transitions:?}"
    );
    assert!(
        transitions.contains(&"ready→canary".to_string()),
        "{transitions:?}"
    );
    assert!(
        transitions.contains(&"canary→promoted".to_string()),
        "{transitions:?}"
    );
}

#[tokio::test]
#[allow(clippy::too_many_lines)] // full lifecycle walkthrough reads better linear
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
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 100}),
    )
    .await;
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
        candidate_ids.push(InstanceId::from_uuid(
            Uuid::parse_str(v["id"].as_str().unwrap()).unwrap(),
        ));
    }
    for id in &candidate_ids {
        srv.storage
            .update_instance_state(*id, InstanceState::Running, None)
            .await
            .unwrap();
        srv.storage
            .update_instance_state(*id, InstanceState::Failed, None)
            .await
            .unwrap();
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
        srv.storage
            .update_instance_state(id, InstanceState::Running, None)
            .await
            .unwrap();
        srv.storage
            .update_instance_state(id, InstanceState::Completed, None)
            .await
            .unwrap();
    }

    let routed = srv
        .storage
        .get_instance(candidate_ids[0])
        .await
        .unwrap()
        .unwrap();
    let later = chrono::Utc::now() + chrono::Duration::minutes(1);
    let other_release_id = Uuid::now_v7();
    let noise: Vec<_> = (0..1_004)
        .map(|index| {
            let mut row = routed.clone();
            row.id = InstanceId::new();
            row.idempotency_key = None;
            row.state = InstanceState::Completed;
            row.updated_at = later;
            match index % 4 {
                0 => row.metadata["release"]["release_id"] = json!(other_release_id),
                1 => row.state = InstanceState::Running,
                2 => row.created_at -= chrono::Duration::hours(1),
                _ => row.metadata["release"]["variant"] = json!("baseline"),
            }
            row
        })
        .collect();
    assert_eq!(
        srv.storage.create_instances_batch(&noise).await.unwrap(),
        1_004
    );

    let preview = client
        .get(format!("{base}/releases/{rid}/gate-preview"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(preview["release"]["state"], "canary");
    assert_eq!(preview["evaluation"]["gates"][0]["verdict"], "fail");
    assert_eq!(preview["evaluation"]["auto_rolled_back"], false);
    assert_eq!(preview["evaluation"]["baseline"]["total"], 3);
    assert_eq!(preview["evaluation"]["candidate"]["total"], 3);
    assert_eq!(preview["evaluation"]["candidate"]["failed"], 3);

    let path = format!("/releases/{rid}");
    let guarded = format!("{path}/evaluate/guarded");
    let (reviewed, _) = read_release_snapshot(&base, &client, &path).await;
    let edit = client
        .put(format!("{base}{path}/target/guarded"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"snapshot":reviewed,"target":{"percentage":30}}))
        .send()
        .await
        .unwrap();
    assert_eq!(edit.status(), StatusCode::OK);
    let (status, error) = post(&base, &client, &guarded, json!({"snapshot":reviewed})).await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["error"]["code"], "release_evaluation_not_applied");
    let (fresh, active) = read_release_snapshot(&base, &client, &path).await;
    assert_eq!(active["state"], "canary");

    // The current release still has a failing gate and can be rolled back.
    let (status, eval) = post(&base, &client, &guarded, json!({"snapshot":fresh})).await;
    assert_eq!(status, StatusCode::OK, "{eval}");
    assert_eq!(eval["gates"][0]["verdict"], "fail", "{eval}");
    assert_eq!(eval["auto_rolled_back"], true, "{eval}");
    assert_eq!(eval["release_state"], "rolled_back");

    // Evaluate again: idempotent, no second rollback decision.
    let (_, eval2) = post(
        &base,
        &client,
        &format!("/releases/{rid}/evaluate"),
        json!({}),
    )
    .await;
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
    assert!(
        stored.metadata.get("release").is_none(),
        "{:?}",
        stored.metadata
    );

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
        json!({"percent": 40}),
    )
    .await;

    // Same cohort key always lands on the same variant. The cohort key
    // must be the ONLY stable key on the request: a non-empty
    // idempotency_key takes precedence as the cohort key, so passing
    // distinct ones would make every instance its own cohort.
    let mut variants = Vec::new();
    for _ in 0..3 {
        let (_, v) = post(
            &base,
            &client,
            "/instances",
            json!({
                "sequence_id": fx.baseline_id, "tenant_id": "t1", "namespace": "default",
                "metadata": {"cohort_key": "customer-42"},
            }),
        )
        .await;
        let id = InstanceId::from_uuid(Uuid::parse_str(v["id"].as_str().unwrap()).unwrap());
        let stored = srv.storage.get_instance(id).await.unwrap().unwrap();
        variants.push(
            stored.metadata["release"]["variant"]
                .as_str()
                .unwrap()
                .to_string(),
        );
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
        json!({"percent": 10}),
    )
    .await;
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
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/canary"),
        json!({"percent": 10}),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    // Promote from draft → 409.
    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/promote"),
        json!({}),
    )
    .await;
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

#[tokio::test]
async fn failed_validation_returns_release_to_draft() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let fx = publish_versions(&base, &client).await;
    let release = create_release(&base, &client, &fx, json!([])).await;
    let rid = release["id"].as_str().unwrap().to_string();

    // Delete the candidate so validation fails AFTER the Draft→Validating
    // CAS (the failure mode that used to strand the release in Validating).
    srv.storage
        .delete_sequence(orch8_types::ids::SequenceId::from_uuid(fx.candidate_id))
        .await
        .unwrap();

    let (status, _) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({}),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // The release must be back in draft — not stranded in validating.
    let resp = client
        .get(format!("{base}/releases/{rid}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let value: Value = resp.json().await.unwrap();
    assert_eq!(value["state"], "draft", "{value}");

    // And it is re-validatable: skip-validation works from draft.
    let (status, report) = post(
        &base,
        &client,
        &format!("/releases/{rid}/validate"),
        json!({"skip": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{report}");
    assert_eq!(report["release_state"], "ready");
}
