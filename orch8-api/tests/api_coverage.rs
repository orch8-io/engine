//! Coverage tests for the orch8 API layer.
//!
//! 100 tests covering: instance lifecycle, sequence CRUD, worker tasks,
//! approvals, credentials, error handling, and auth/tenant scoping.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::WorkerStore;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

// ─── Helpers ───────────────────────────────────────────────────────────────

fn mk_sequence_body(id: Uuid, tenant: &str, namespace: &str, name: &str) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": namespace,
        "name": name,
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

async fn create_seq(client: &reqwest::Client, base: &str, tenant: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&mk_sequence_body(seq_id, tenant, "ns1", "test-seq"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create_seq failed");
    seq_id
}

async fn create_inst(client: &reqwest::Client, base: &str, seq_id: Uuid, tenant: &str) -> String {
    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": tenant,
        "namespace": "ns1",
        "context": { "data": { "x": 1 }, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create_inst failed");
    let v: serde_json::Value = resp.json().await.unwrap();
    v["id"].as_str().unwrap().to_string()
}

async fn seed_worker_task(
    srv: &orch8_api::test_harness::TestServer,
    instance_id: Uuid,
    handler: &str,
    queue: Option<&str>,
) -> Uuid {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};
    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: orch8_types::ids::InstanceId::from_uuid(instance_id),
        block_id: orch8_types::ids::BlockId::new("s1"),
        handler_name: handler.into(),
        queue_name: queue.map(String::from),
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    };
    srv.storage.create_worker_task(&task).await.unwrap();
    task.id
}

// ════════════════════════════════════════════════════════════════════════════
// Instance Lifecycle (tests 1-30)
// ════════════════════════════════════════════════════════════════════════════

// 1: Create instance with valid params returns 201
#[tokio::test]
async fn t01_create_instance_valid_params() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

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
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v["id"].is_string());
}

// 2: Create instance with priority
#[tokio::test]
async fn t02_create_instance_with_priority() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "priority": "High",
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

// 3: Create instance with concurrency key
#[tokio::test]
async fn t03_create_instance_with_concurrency_key() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "concurrency_key": "user-42",
        "max_concurrency": 5,
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

// 4: Create instance with scheduled fire time
#[tokio::test]
async fn t04_create_instance_with_next_fire_at() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let future = chrono::Utc::now() + chrono::Duration::hours(1);
    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "next_fire_at": future.to_rfc3339(),
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

// 5: Create instance with metadata
#[tokio::test]
async fn t05_create_instance_with_metadata() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "metadata": { "env": "staging", "version": "1.2.3" },
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

    let v: serde_json::Value = resp.json().await.unwrap();
    let inst_id = v["id"].as_str().unwrap();
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let fetched: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(fetched["metadata"]["env"], "staging");
}

// 6: Create instance with empty tenant_id returns 400
#[tokio::test]
async fn t06_create_instance_empty_tenant_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// 7: Create instance with empty namespace returns 400
#[tokio::test]
async fn t07_create_instance_empty_namespace_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// 8: Create instance with missing sequence returns 404
#[tokio::test]
async fn t08_create_instance_missing_sequence_returns_404() {
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

// 9: Create instance with whitespace-only tenant returns 400
#[tokio::test]
async fn t09_create_instance_whitespace_tenant_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "   ",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// 10: Create instance with whitespace-only namespace returns 400
#[tokio::test]
async fn t10_create_instance_whitespace_namespace_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "   ",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// 11: Idempotency key deduplicates on second call
#[tokio::test]
async fn t11_idempotency_key_deduplicates() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "idem-unique-1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let resp1 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);
    let v1: serde_json::Value = resp1.json().await.unwrap();

    let resp2 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let v2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(v1["id"], v2["id"]);
    assert_eq!(v2["deduplicated"], true);
}

// 12: Different idempotency keys create separate instances
#[tokio::test]
async fn t12_different_idempotency_keys_create_separate() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body1 = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "key-a",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let body2 = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "key-b",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp1 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);
    let v1: serde_json::Value = resp1.json().await.unwrap();

    let resp2 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::CREATED);
    let v2: serde_json::Value = resp2.json().await.unwrap();
    assert_ne!(v1["id"], v2["id"]);
}

// 13: Empty idempotency key triggers storage conflict on second insert
// (the API skips dedup lookup for empty keys but the DB unique index
// on (tenant_id, idempotency_key) WHERE NOT NULL still fires)
#[tokio::test]
async fn t13_empty_idempotency_key_conflict_on_second() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp1 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);

    // Second call with same empty key hits unique constraint
    let resp2 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::CONFLICT);
}

// 14: Null idempotency key does not deduplicate
#[tokio::test]
async fn t14_null_idempotency_key_no_dedup() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;

    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp1 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);
    let v1: serde_json::Value = resp1.json().await.unwrap();

    let resp2 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::CREATED);
    let v2: serde_json::Value = resp2.json().await.unwrap();
    assert_ne!(v1["id"], v2["id"]);
}

// 15: Idempotency key scoped to tenant (different tenants, same key = different instances)
#[tokio::test]
async fn t15_idempotency_key_scoped_per_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id_t1 = create_seq(&client, &srv.base_url, "t1").await;
    // Create a separate sequence for t2
    let seq_id_t2 = Uuid::now_v7();
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&mk_sequence_body(seq_id_t2, "t2", "ns1", "test-seq-t2"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let body_t1 = json!({
        "sequence_id": seq_id_t1,
        "tenant_id": "t1",
        "namespace": "ns1",
        "idempotency_key": "shared-key",
        "context": { "data": {}, "config": {}, "audit": [] }
    });
    let body_t2 = json!({
        "sequence_id": seq_id_t2,
        "tenant_id": "t2",
        "namespace": "ns1",
        "idempotency_key": "shared-key",
        "context": { "data": {}, "config": {}, "audit": [] }
    });

    let resp1 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body_t1)
        .send()
        .await
        .unwrap();
    assert_eq!(resp1.status(), StatusCode::CREATED);

    let resp2 = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&body_t2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), StatusCode::CREATED);
}

// 16: List instances returns empty when no instances
#[tokio::test]
async fn t16_list_instances_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/instances?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
    assert_eq!(v["has_more"], false);
}

// 17: List instances filters by state
#[tokio::test]
async fn t17_list_instances_filter_by_state() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    create_inst(&client, &srv.base_url, seq_id, "t1").await;

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
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 1);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&state=completed",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

// 18: List instances filters by tenant isolation
#[tokio::test]
async fn t18_list_instances_tenant_isolation() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    create_inst(&client, &srv.base_url, seq_id, "t1").await;

    // Different tenant sees nothing
    let resp = client
        .get(format!("{}/instances?tenant_id=t2", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

// 19: List instances with pagination (limit)
#[tokio::test]
async fn t19_list_instances_pagination_limit() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    for _ in 0..3 {
        create_inst(&client, &srv.base_url, seq_id, "t1").await;
    }

    let resp = client
        .get(format!("{}/instances?tenant_id=t1&limit=2", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["has_more"], true);
}

// 20: List instances with namespace filter
#[tokio::test]
async fn t20_list_instances_filter_by_namespace() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    create_inst(&client, &srv.base_url, seq_id, "t1").await;

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&namespace=ns1",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 1);

    let resp = client
        .get(format!(
            "{}/instances?tenant_id=t1&namespace=nonexistent",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

// 21-30: Get instance, update state, retry — kept identical to original
// (already passing, no changes needed)

#[tokio::test]
async fn t21_get_instance_found() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["id"], inst_id);
    assert_eq!(v["state"], "scheduled");
}

#[tokio::test]
async fn t22_get_instance_not_found() {
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
async fn t23_get_instance_cross_tenant_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t24_get_instance_contains_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["context"]["data"]["x"], 1);
}

#[tokio::test]
async fn t25_get_instance_has_sequence_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["sequence_id"], seq_id.to_string());
}

#[tokio::test]
async fn t26_update_state_scheduled_to_paused() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "paused" }))
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
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["state"], "paused");
}

#[tokio::test]
async fn t27_update_state_paused_to_scheduled() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "paused" }))
        .send()
        .await
        .unwrap();
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "scheduled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t28_update_state_scheduled_to_cancelled() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "cancelled" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t29_invalid_state_transition_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
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
async fn t30_retry_from_failed_resets_to_scheduled() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "running" }))
        .send()
        .await
        .unwrap();
    client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "state": "failed" }))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/instances/{inst_id}/retry", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["state"], "scheduled");
}

// ════════════════════════════════════════════════════════════════════════════
// Sequence CRUD (tests 31-50)
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t31_create_sequence_valid() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id, "t1", "ns1", "my-seq"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["id"], seq_id.to_string());
}

#[tokio::test]
async fn t32_create_sequence_multiple_blocks() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "multi-block",
        "version": 1, "deprecated": false,
        "blocks": [
            { "type": "step", "id": "a", "handler": "noop", "params": {}, "cancellable": true },
            { "type": "step", "id": "b", "handler": "log", "params": {}, "cancellable": true }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn t33_create_sequence_dup_block_ids_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "dup-blocks",
        "version": 1, "deprecated": false,
        "blocks": [
            { "type": "step", "id": "dup", "handler": "noop", "params": {}, "cancellable": true },
            { "type": "step", "id": "dup", "handler": "log", "params": {}, "cancellable": true }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t34_create_sequence_unknown_handler_warnings() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "warn-seq",
        "version": 1, "deprecated": false,
        "blocks": [{ "type": "step", "id": "s1", "handler": "totally_unknown_xyz", "params": {}, "cancellable": true }],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v.get("warnings").is_some());
}

#[tokio::test]
async fn t35_create_sequence_with_status() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let body = json!({
        "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "staged-seq",
        "version": 1, "deprecated": false, "status": "staging",
        "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }],
        "created_at": chrono::Utc::now().to_rfc3339()
    });
    let resp = client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
}

#[tokio::test]
async fn t36_list_sequences_returns_created() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .get(format!("{}/sequences?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(!v["items"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn t37_list_sequences_filter_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .get(format!("{}/sequences?tenant_id=t2", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn t38_list_sequences_with_limit() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    for i in 0..3 {
        let seq_id = Uuid::now_v7();
        client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&mk_sequence_body(seq_id, "t1", "ns1", &format!("seq-{i}")))
            .send()
            .await
            .unwrap();
    }
    let resp = client
        .get(format!("{}/sequences?tenant_id=t1&limit=2", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 2);
    assert_eq!(v["has_more"], true);
}

#[tokio::test]
async fn t39_list_sequences_filter_namespace() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = Uuid::now_v7();
    client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id, "t1", "special-ns", "ns-seq"))
        .send()
        .await
        .unwrap();
    let resp = client
        .get(format!(
            "{}/sequences?tenant_id=t1&namespace=special-ns",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(!v["items"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn t40_list_sequences_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/sequences?tenant_id=nonexistent", srv.base_url))
        .header("X-Tenant-Id", "nonexistent")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn t41_get_sequence_by_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t42_get_sequence_by_name() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=test-seq",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["name"], "test-seq");
}

#[tokio::test]
async fn t43_get_sequence_by_name_not_found() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=nonexist",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t44_get_sequence_by_id_not_found() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/sequences/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t45_get_sequence_cross_tenant_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t46_deprecate_sequence() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .post(format!("{}/sequences/{seq_id}/deprecate", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn t47_list_sequence_versions() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    for ver in 1..=2 {
        let body = json!({ "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "versioned", "version": ver, "deprecated": false, "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }], "created_at": chrono::Utc::now().to_rfc3339() });
        client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
    }
    let resp = client
        .get(format!(
            "{}/sequences/versions?tenant_id=t1&namespace=ns1&name=versioned",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(v.len(), 2);
}

#[tokio::test]
async fn t48_delete_sequence() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let resp = client
        .delete(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client
        .get(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t49_delete_sequence_with_active_instances_409() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .delete(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn t50_get_sequence_by_name_with_version() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    for ver in 1..=2 {
        let body = json!({ "id": Uuid::now_v7(), "tenant_id": "t1", "namespace": "ns1", "name": "ver-target", "version": ver, "deprecated": false, "blocks": [{ "type": "step", "id": "s1", "handler": "noop", "params": {}, "cancellable": true }], "created_at": chrono::Utc::now().to_rfc3339() });
        client
            .post(format!("{}/sequences", srv.base_url))
            .header("X-Tenant-Id", "t1")
            .json(&body)
            .send()
            .await
            .unwrap();
    }
    let resp = client
        .get(format!(
            "{}/sequences/by-name?tenant_id=t1&namespace=ns1&name=ver-target&version=1",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["version"], 1);
}

// ════════════════════════════════════════════════════════════════════════════
// Worker Tasks (tests 51-70)
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t51_poll_tasks_returns_task() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    seed_worker_task(&srv, inst_uuid, "handler_a", None).await;
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_a","worker_id":"w1","limit":10}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn t52_poll_tasks_no_match_returns_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"nonexistent_handler","worker_id":"w1","limit":10}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 0);
}

#[tokio::test]
async fn t53_poll_tasks_respects_limit() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    seed_worker_task(&srv, inst_uuid, "handler_b", None).await;
    seed_worker_task(&srv, inst_uuid, "handler_b", None).await;
    seed_worker_task(&srv, inst_uuid, "handler_b", None).await;
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_b","worker_id":"w1","limit":2}))
        .send()
        .await
        .unwrap();
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(tasks.len() <= 2);
}

#[tokio::test]
async fn t54_poll_tasks_from_named_queue() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    seed_worker_task(&srv, inst_uuid, "handler_c", Some("my-queue")).await;
    let resp = client.post(format!("{}/workers/tasks/poll/queue", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"queue_name":"my-queue","handler_name":"handler_c","worker_id":"w1","limit":10})).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn t55_poll_tasks_wrong_queue_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    seed_worker_task(&srv, inst_uuid, "handler_c", Some("q-alpha")).await;
    let resp = client
        .post(format!("{}/workers/tasks/poll/queue", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(
            &json!({"queue_name":"q-beta","handler_name":"handler_c","worker_id":"w1","limit":10}),
        )
        .send()
        .await
        .unwrap();
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 0);
}

#[tokio::test]
async fn t56_complete_task_returns_200() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_d", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_d","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","output":{"result":"done"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t57_complete_task_complex_output() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_e", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_e","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","output":{"items":[1,2,3],"nested":{"key":"val"}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t58_complete_task_not_found_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/workers/tasks/{}/complete",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","output":{}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t59_complete_task_merges_context() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_f", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_f","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","output":{"new_key":"new_val"}}))
        .send()
        .await
        .unwrap();
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["context"]["data"]["new_key"], "new_val");
}

#[tokio::test]
async fn t60_complete_task_transitions_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_g", None).await;
    client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"state":"running"}))
        .send()
        .await
        .unwrap();
    client
        .patch(format!("{}/instances/{inst_id}/state", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"state":"waiting"}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_g","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","output":{}}))
        .send()
        .await
        .unwrap();
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["state"], "scheduled");
}

#[tokio::test]
async fn t61_fail_task_returns_200() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_h", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_h","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","message":"timeout exceeded","retryable":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t62_fail_task_non_retryable_fails_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_i", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_i","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    client
        .post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","message":"permanent error","retryable":false}))
        .send()
        .await
        .unwrap();
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["state"], "failed");
}

#[tokio::test]
async fn t63_fail_task_not_found_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/workers/tasks/{}/fail",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","message":"oops","retryable":false}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t64_fail_task_retryable() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_j", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_j","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1","message":"transient error","retryable":true}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t65_fail_task_with_detailed_message() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_k", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_k","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client.post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"worker_id":"w1","message":"Connection refused: host=db.internal port=5432","retryable":false})).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t66_heartbeat_returns_200() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_l", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_l","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    let resp = client
        .post(format!(
            "{}/workers/tasks/{task_id}/heartbeat",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t67_heartbeat_multiple_times() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    let task_id = seed_worker_task(&srv, inst_uuid, "handler_m", None).await;
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"handler_name":"handler_m","worker_id":"w1","limit":1}))
        .send()
        .await
        .unwrap();
    for _ in 0..3 {
        let resp = client
            .post(format!(
                "{}/workers/tasks/{task_id}/heartbeat",
                srv.base_url
            ))
            .header("X-Tenant-Id", "t1")
            .json(&json!({"worker_id":"w1"}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn t68_heartbeat_not_found_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/workers/tasks/{}/heartbeat",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"worker_id":"w1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t69_list_worker_tasks() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let inst_uuid = Uuid::parse_str(&inst_id).unwrap();
    seed_worker_task(&srv, inst_uuid, "handler_n", None).await;
    let resp = client
        .get(format!(
            "{}/workers/tasks?handler_name=handler_n",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn t70_worker_task_stats() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/workers/tasks/stats", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ════════════════════════════════════════════════════════════════════════════
// Approvals (tests 71-80)
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t71_list_approvals_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
    assert_eq!(v["total"], 0);
}

#[tokio::test]
async fn t72_list_approvals_tenant_filter() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals?tenant_id=t2", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["total"], 0);
}

#[tokio::test]
async fn t73_list_approvals_pagination() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/approvals?tenant_id=t1&offset=0&limit=10",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t74_list_approvals_namespace_filter() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!(
            "{}/approvals?tenant_id=t1&namespace=ns1",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["total"], 0);
}

#[tokio::test]
async fn t75_list_approvals_response_structure() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v.get("items").is_some());
    assert!(v.get("total").is_some());
}

#[tokio::test]
async fn t76_signal_instance_not_found() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!(
            "{}/instances/{}/signals",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"signal_type":"pause","payload":{}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t77_approvals_default_limit() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t78_approvals_no_tenant_header() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn t79_approvals_large_offset_empty() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals?offset=999999", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn t80_approvals_limit_zero() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/approvals?limit=0", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

// ════════════════════════════════════════════════════════════════════════════
// Credentials (tests 81-90)
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t81_create_credential() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-test-1","name":"Test API Key","kind":"api_key","value":"sk-secret-123","tenant_id":"t1"})).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["id"], "cred-test-1");
    assert!(v.get("value").is_none());
}

#[tokio::test]
async fn t82_get_credential_strips_secret() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-get-1","name":"My Key","kind":"api_key","value":"super-secret","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .get(format!("{}/credentials/cred-get-1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["id"], "cred-get-1");
    assert!(v.get("value").is_none());
}

#[tokio::test]
async fn t83_list_credentials() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-list-1","name":"Listed Key","kind":"api_key","value":"val","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .get(format!("{}/credentials?tenant_id=t1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(!v.is_empty());
}

#[tokio::test]
async fn t84_create_credential_invalid_id_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"invalid id with spaces!","name":"Bad","kind":"api_key","value":"val","tenant_id":"t1"})).send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t85_create_credential_empty_fields_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/credentials", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"id":"","name":"","kind":"api_key","value":"","tenant_id":"t1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t86_update_credential() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-upd-1","name":"Original","kind":"api_key","value":"orig-val","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .patch(format!("{}/credentials/cred-upd-1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"name":"Updated Name"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(v["name"], "Updated Name");
}

#[tokio::test]
async fn t87_update_credential_value() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-upd-val-1","name":"Key","kind":"api_key","value":"old-secret","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .patch(format!("{}/credentials/cred-upd-val-1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"value":"new-secret"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v.get("value").is_none());
}

#[tokio::test]
async fn t88_delete_credential() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-del-1","name":"To Delete","kind":"api_key","value":"val","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .delete(format!("{}/credentials/cred-del-1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = client
        .get(format!("{}/credentials/cred-del-1", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t89_get_credential_not_found() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/credentials/nonexist", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t90_credential_cross_tenant_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    client.post(format!("{}/credentials", srv.base_url)).header("X-Tenant-Id", "t1").json(&json!({"id":"cred-cross-1","name":"Cross","kind":"api_key","value":"val","tenant_id":"t1"})).send().await.unwrap();
    let resp = client
        .get(format!("{}/credentials/cred-cross-1", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ════════════════════════════════════════════════════════════════════════════
// Error Handling (tests 91-100)
// ════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn t91_not_found_error_body() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/instances/{}", srv.base_url, Uuid::now_v7()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
async fn t92_invalid_argument_error_body() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let body = json!({"sequence_id":seq_id,"tenant_id":"t1","namespace":"  ","context":{"data":{},"config":{},"audit":[]}});
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v["error"].as_str().unwrap().contains("invalid argument"));
}

#[tokio::test]
async fn t93_conflict_error_409() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .delete(format!("{}/sequences/{seq_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
    let v: serde_json::Value = resp.json().await.unwrap();
    assert!(v["error"].as_str().unwrap().contains("conflict"));
}

#[tokio::test]
async fn t94_forbidden_error_403() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_t1 = create_seq(&client, &srv.base_url, "t1").await;
    let seq_t2 = Uuid::now_v7();
    client
        .post(format!("{}/sequences", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .json(&mk_sequence_body(seq_t2, "t2", "ns1", "t2-seq"))
        .send()
        .await
        .unwrap();
    let inst_id = create_inst(&client, &srv.base_url, seq_t1, "t1").await;
    let resp = client
        .post(format!("{}/sequences/migrate-instance", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"instance_id":inst_id,"target_sequence_id":seq_t2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn t95_invalid_json_returns_error() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .header("content-type", "application/json")
        .body("{ invalid json }")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status() == StatusCode::UNPROCESSABLE_ENTITY
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn t96_tenant_header_body_mismatch_403() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let body = json!({"sequence_id":seq_id,"tenant_id":"t2","namespace":"ns1","context":{"data":{},"config":{},"audit":[]}});
    let resp = client
        .post(format!("{}/instances", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn t97_update_state_unknown_instance_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .patch(format!(
            "{}/instances/{}/state",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"state":"paused"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t98_update_context_unknown_instance_404() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let resp = client
        .patch(format!(
            "{}/instances/{}/context",
            srv.base_url,
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"context":{"data":{},"config":{},"audit":[]}}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn t99_retry_non_failed_returns_400() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let inst_id = create_inst(&client, &srv.base_url, seq_id, "t1").await;
    let resp = client
        .post(format!("{}/instances/{inst_id}/retry", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn t100_versioned_api_prefix() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_seq(&client, &srv.base_url, "t1").await;
    let body = json!({"sequence_id":seq_id,"tenant_id":"t1","namespace":"ns1","context":{"data":{},"config":{},"audit":[]}});
    let resp = client
        .post(format!("{}/instances", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let v: serde_json::Value = resp.json().await.unwrap();
    let inst_id = v["id"].as_str().unwrap();
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}
