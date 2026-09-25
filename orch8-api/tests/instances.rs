//! E2E tests for the Instances API.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::InstanceStore;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "test-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
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

#[tokio::test]
async fn create_instance_and_get_by_id_round_trip() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "key": "val" }, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["sequence_id"], seq_id.to_string());
    assert_eq!(fetched["state"], "scheduled");
}

#[tokio::test]
async fn create_instance_with_invalid_sequence_id_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let body = json!({
        "sequence_id": Uuid::now_v7(),
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn create_instance_with_idempotency_key_returns_same_id_on_retry() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "idem-123",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let first_id = created["id"].as_str().unwrap();

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let second: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(second["id"].as_str().unwrap(), first_id);
    assert_eq!(second["deduplicated"], true);
}

#[tokio::test]
async fn batch_create_instances_returns_correct_count() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } }
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let result: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(result["count"], 3);
}

/// Stab#13: batch endpoint previously skipped the empty-tenant /
/// empty-namespace validation that single-create enforces, letting blank
/// strings leak into the default-tenant view and bypass isolation.
#[tokio::test]
async fn batch_create_rejects_empty_tenant_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // No X-Tenant-Id header (so enforce_tenant_create passes through);
    // an empty tenant_id in the body must be rejected.
    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "", "namespace": "ns1", "context": { "data": {}, "config": {}, "audit": [] } },
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    // Empty `tenant_id` is rejected during TenantId deserialization → 422.
    assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
async fn batch_create_rejects_empty_namespace() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "instances": [
            { "sequence_id": seq_id, "tenant_id": "t1", "namespace": "", "context": { "data": {}, "config": {}, "audit": [] } },
        ]
    });

    let resp = client
        .post(format!("{}/instances/batch", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_instances_filters_by_state() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&state=scheduled",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let list = body["items"].as_array().unwrap();
    assert_eq!(list.len(), 1);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&state=running",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let list = body["items"].as_array().unwrap();
    assert_eq!(list.len(), 0);
}

#[tokio::test]
async fn update_instance_state_and_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": { "a": 1 }, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    // Update state to paused.
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "paused" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Update context.
    let resp = client
        .patch(format!("{}/instances/{inst_id}/context", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "context": { "data": { "b": 2 }, "config": {}, "audit": [] } }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["state"], "paused");
}

#[tokio::test]
async fn get_instance_returns_404_for_unknown_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/instances/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invalid_state_transition_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    // Try to transition from Scheduled -> Completed (invalid).
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "completed" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn create_instance_with_budget_echoes_budget_on_get() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] },
        "budget": {
            "max_input_tokens": 1000,
            "max_total_tokens": 5000,
            "max_steps": 10
        }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["budget"]["max_input_tokens"], 1000);
    assert_eq!(fetched["budget"]["max_total_tokens"], 5000);
    assert_eq!(fetched["budget"]["max_steps"], 10);
    // Unset limit is omitted from the serialized budget.
    assert!(fetched["budget"].get("max_output_tokens").is_none());
}

#[tokio::test]
async fn create_instance_without_budget_omits_budget_field() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    let inst_id = created["id"].as_str().unwrap();

    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let fetched: serde_json::Value = resp.json().await.unwrap();
    // skip_serializing_if = Option::is_none — the key is absent entirely.
    assert!(fetched.get("budget").is_none());
}

#[tokio::test]
async fn list_instances_filters_by_metadata_key_value() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // Two instances differing only in a metadata value.
    for env in ["prod", "staging"] {
        let body = json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "metadata": { "env": env, "team": "core" },
            "context": { "data": {}, "config": {}, "audit": [] }
        });
        let resp = client
            .post(format!("{}/instances", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Filter by metadata.env=prod -> exactly one.
    let resp = client
        .get(format!("{}/instances?metadata.env=prod", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let page: serde_json::Value = resp.json().await.unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1, "exactly one prod instance");
    assert_eq!(items[0]["metadata"]["env"], "prod");

    // Two-key filter that matches both keys on the prod row -> still one.
    let resp = client
        .get(format!(
            "{}/instances?metadata.env=prod&metadata.team=core",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let page: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);

    // A value that matches nothing -> empty.
    let resp = client
        .get(format!("{}/instances?metadata.env=qa", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let page: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn batch_action_dry_run_and_apply_with_metadata_filter() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // Two instances tagged grp=a, one grp=b.
    for grp in ["a", "a", "b"] {
        let body = json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "metadata": { "grp": grp },
            "context": { "data": {}, "config": {}, "audit": [] }
        });
        let resp = client
            .post(format!("{}/instances", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // Dry-run cancel scoped to grp=a -> matched 2, applied 0.
    let action = json!({
        "filter": { "tenant_id": "t1", "metadata": { "grp": "a" } },
        "action": "cancel",
        "dry_run": true
    });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&action)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(out["matched"], 2);
    assert_eq!(out["applied"], 0);
    assert_eq!(out["dry_run"], true);

    // Real cancel -> applied 2 (signals enqueued to active instances).
    let action = json!({
        "filter": { "tenant_id": "t1", "metadata": { "grp": "a" } },
        "action": "cancel"
    });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&action)
        .send()
        .await
        .unwrap();
    let out: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(out["matched"], 2);
    assert_eq!(out["applied"], 2);
    assert_eq!(out["failed"], 0);
}

#[tokio::test]
async fn batch_action_requires_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let action = json!({ "filter": {}, "action": "pause" });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .json(&action)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_action_signal_requires_signal_type() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let action = json!({ "filter": { "tenant_id": "t1" }, "action": "signal" });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&action)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn batch_action_pause_resume_and_signal_apply_to_active() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // Two active (just-created) instances.
    for _ in 0..2 {
        let body = json!({
            "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1",
            "metadata": { "grp": "pr" },
            "context": { "data": {}, "config": {}, "audit": [] }
        });
        let resp = client
            .post(format!("{}/instances", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
    }

    // pause, resume, then a custom signal all enqueue to the 2 active instances.
    for action in [
        json!({ "filter": { "tenant_id": "t1", "metadata": { "grp": "pr" } }, "action": "pause" }),
        json!({ "filter": { "tenant_id": "t1", "metadata": { "grp": "pr" } }, "action": "resume" }),
        json!({ "filter": { "tenant_id": "t1", "metadata": { "grp": "pr" } }, "action": "signal", "signal_type": "nudge" }),
    ] {
        let resp = client
            .post(format!("{}/instances/batch-action", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&action)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let out: serde_json::Value = resp.json().await.unwrap();
        assert_eq!(out["matched"], 2, "action {} matched", action["action"]);
        assert_eq!(out["applied"], 2, "action {} applied", action["action"]);
        assert_eq!(out["skipped"], 0);
        assert_eq!(out["failed"], 0);
    }
}

#[tokio::test]
async fn batch_action_retry_skips_non_failed_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // Active instances are not Failed → retry matches but skips every one.
    for _ in 0..2 {
        let body = json!({
            "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1",
            "metadata": { "grp": "rt" },
            "context": { "data": {}, "config": {}, "audit": [] }
        });
        client
            .post(format!("{}/instances", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    let action =
        json!({ "filter": { "tenant_id": "t1", "metadata": { "grp": "rt" } }, "action": "retry" });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&action)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(out["matched"], 2);
    assert_eq!(out["applied"], 0, "non-failed instances are not retried");
    assert_eq!(out["skipped"], 2);
}

#[tokio::test]
async fn batch_action_honors_limit_cap() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    for _ in 0..3 {
        let body = json!({
            "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1",
            "metadata": { "grp": "cap" },
            "context": { "data": {}, "config": {}, "audit": [] }
        });
        client
            .post(format!("{}/instances", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
    }

    // limit=1 caps the matched set regardless of how many match the filter.
    let action = json!({
        "filter": { "tenant_id": "t1", "metadata": { "grp": "cap" } },
        "action": "pause",
        "limit": 1,
        "dry_run": true
    });
    let resp = client
        .post(format!("{}/instances/batch-action", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&action)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let out: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(out["matched"], 1, "limit caps the matched set");
}

#[tokio::test]
async fn get_children_returns_child_instances() {
    use orch8_types::context::{ExecutionContext, RuntimeContext};
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};

    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;

    // Parent instance via the API.
    let body = json!({
        "sequence_id": seq_id, "tenant_id": "t1", "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    let parent_id: String = resp.json::<serde_json::Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let parent_uuid = uuid::Uuid::parse_str(&parent_id).unwrap();

    // Seed two children directly in storage (the engine sets parent_instance_id
    // for sub-sequences; here we link them explicitly).
    let mk_child = || TaskInstance {
        id: InstanceId::new(),
        sequence_id: SequenceId::from_uuid(seq_id),
        tenant_id: TenantId::unchecked("t1"),
        namespace: Namespace::new("ns1"),
        state: InstanceState::Scheduled,
        next_fire_at: None,
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({}),
        context: ExecutionContext {
            data: json!({}),
            config: json!({}),
            audit: vec![],
            runtime: RuntimeContext::default(),
        },
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: None,
        session_id: None,
        parent_instance_id: Some(InstanceId::from_uuid(parent_uuid)),
        budget: None,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    srv.storage.create_instance(&mk_child()).await.unwrap();
    srv.storage.create_instance(&mk_child()).await.unwrap();

    let resp = client
        .get(format!("{}/instances/{parent_id}/children", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let children: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(children.as_array().unwrap().len(), 2);
    assert_eq!(children[0]["parent_instance_id"], parent_id);
}

#[tokio::test]
async fn get_children_unknown_parent_returns_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/instances/{}/children",
            srv.base_url,
            uuid::Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}
