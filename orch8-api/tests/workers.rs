//! E2E tests for the Workers API.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::WorkerStore;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "worker-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "external_handler",
                "params": {},
                "cancellable": true,
                "queue_name": "q1"
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

async fn create_instance(client: &reqwest::Client, base_url: &str, seq_id: Uuid) -> Uuid {
    let body = json!({
        "sequence_id": seq_id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "context": { "data": {}, "config": {}, "audit": [] }
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
    Uuid::parse_str(created["id"].as_str().unwrap()).unwrap()
}

/// Create a worker task directly via storage so we can test the API surface
/// without spinning up the full engine scheduler.
async fn seed_worker_task(srv: &orch8_api::test_harness::TestServer, instance_id: Uuid) -> Uuid {
    use orch8_types::worker::{WorkerTask, WorkerTaskState};
    let task = WorkerTask {
        id: Uuid::now_v7(),
        instance_id: orch8_types::ids::InstanceId::from_uuid(instance_id),
        block_id: orch8_types::ids::BlockId::new("s1"),
        handler_name: "external_handler".into(),
        queue_name: Some("q1".into()),
        params: json!({}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        state: WorkerTaskState::Pending,
        worker_id: None,
        claimed_at: None,
        heartbeat_at: None,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: chrono::Utc::now(),
    };
    srv.storage.create_worker_task(&task).await.unwrap();
    task.id
}

#[tokio::test]
async fn poll_tasks_returns_claimed_task() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 10
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["id"], task_id.to_string());
    // The claim API returns the task as it was selected (pending) even though
    // the DB row is updated to claimed immediately after. Verify via storage.
    let task = srv.storage.get_worker_task(task_id).await.unwrap().unwrap();
    assert_eq!(task.state.to_string(), "claimed");
}

#[tokio::test]
async fn complete_task_transitions_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    // Claim the task first.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Complete it.
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "output": { "result": "ok" }
        }))
        .send()
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        let text = resp.text().await.unwrap();
        panic!("complete_task failed: {text}");
    }

    // Task should now be completed.
    let resp = client
        .get(format!(
            "{}/workers/tasks?state=completed&handler_name=external_handler",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    if resp.status() != StatusCode::OK {
        let text = resp.text().await.unwrap();
        panic!("list tasks failed: {text}");
    }
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
}

#[tokio::test]
async fn fail_task_with_retryable_false_fails_instance() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    // Claim.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Fail permanently.
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "message": "boom",
            "retryable": false
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Instance should be failed.
    let resp = client
        .get(format!("{}/instances/{inst_id}", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let inst: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(inst["state"], "failed");
}

#[tokio::test]
async fn heartbeat_extends_task_claim() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    // Claim.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Heartbeat.
    let resp = client
        .post(format!(
            "{}/workers/tasks/{task_id}/heartbeat",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "worker_id": "worker-1" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Persist resumable progress using the sequence returned by the prior
    // heartbeat/poll contract.
    let resp = client
        .post(format!(
            "{}/workers/tasks/{task_id}/heartbeat",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "checkpoint_seq": 0,
            "checkpoint": { "page": 7, "cursor": "abc" }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["checkpoint_seq"], 1);

    let stored = srv.storage.get_worker_task(task_id).await.unwrap().unwrap();
    assert_eq!(
        stored.resume_checkpoint,
        Some(json!({"page": 7, "cursor": "abc"}))
    );
    assert_eq!(stored.checkpoint_seq, 1);

    // Replaying an old checkpoint version cannot clobber progress.
    let stale = client
        .post(format!(
            "{}/workers/tasks/{task_id}/heartbeat",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "checkpoint_seq": 0,
            "checkpoint": { "page": 999 }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn poll_from_named_queue_isolates_tasks() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    let resp = client
        .post(format!("{}/workers/tasks/poll/queue", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "queue_name": "q1",
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["id"], task_id.to_string());

    // Different queue returns nothing.
    let resp = client
        .post(format!("{}/workers/tasks/poll/queue", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "queue_name": "q2",
            "handler_name": "external_handler",
            "worker_id": "worker-1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 0);
}

#[tokio::test]
async fn poll_registers_worker_even_with_no_tasks() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Poll for a handler that has no pending tasks — the worker must still
    // appear on the registry (liveness is poll recency, not task claims).
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .json(&json!({
            "handler_name": "idle_handler",
            "worker_id": "idle-worker",
            "limit": 1,
            "version": "1.2.3"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/workers", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let workers: Vec<serde_json::Value> = resp.json().await.unwrap();
    let w = workers
        .iter()
        .find(|w| w["worker_id"] == "idle-worker")
        .expect("idle worker registered");
    assert_eq!(w["alive"], true);
    assert_eq!(w["version"], "1.2.3");
    assert_eq!(w["in_flight"], 0);
    assert!(
        w["handlers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|h| h == "idle_handler")
    );
}

#[tokio::test]
async fn get_workers_reports_in_flight_and_queue() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    seed_worker_task(&srv, inst_id).await;

    // Claim via queue-scoped poll so queue_name lands on the registration.
    let resp = client
        .post(format!("{}/workers/tasks/poll/queue", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "queue_name": "q1",
            "handler_name": "external_handler",
            "worker_id": "queue-worker",
            "limit": 10
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1);

    let resp = client
        .get(format!("{}/workers", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let workers: Vec<serde_json::Value> = resp.json().await.unwrap();
    let w = workers
        .iter()
        .find(|w| w["worker_id"] == "queue-worker")
        .expect("queue worker registered");
    assert_eq!(w["in_flight"], 1);
    assert!(w["queues"].as_array().unwrap().iter().any(|q| q == "q1"));
}

#[tokio::test]
async fn get_workers_scopes_by_tenant() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Worker registered under tenant t1 (tenant-scoped poll).
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "handler_name": "h_t1",
            "worker_id": "tenant1-worker",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // A t2-scoped caller must not see t1's worker.
    let resp = client
        .get(format!("{}/workers", srv.base_url))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let workers: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(
        !workers.iter().any(|w| w["worker_id"] == "tenant1-worker"),
        "t1 worker must be invisible to t2"
    );

    // The owning tenant sees it.
    let resp = client
        .get(format!("{}/workers", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let workers: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert!(workers.iter().any(|w| w["worker_id"] == "tenant1-worker"));
}

#[tokio::test]
async fn get_handlers_lists_builtin_and_external() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .json(&json!({
            "handler_name": "my_external_handler",
            "worker_id": "w1",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let resp = client
        .get(format!("{}/handlers", srv.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let catalog: serde_json::Value = resp.json().await.unwrap();
    let builtin = catalog["builtin"].as_array().unwrap();
    assert!(builtin.iter().any(|h| h == "noop"));
    assert!(builtin.iter().any(|h| h == "http_request"));
    let external = catalog["external"].as_array().unwrap();
    assert!(external.iter().any(|h| h == "my_external_handler"));
}

#[tokio::test]
async fn version_pin_blocks_old_worker_and_allows_new() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    seed_worker_task(&srv, inst_id).await;

    // Pin external_handler to >= 2.0.0 for tenant t1.
    let resp = client
        .post(format!("{}/workers/version-pins", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "tenant_id": "t1", "handler_name": "external_handler", "min_version": "2.0.0" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // A 1.x worker is blocked → no tasks.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "handler_name": "external_handler", "worker_id": "old", "limit": 10, "version": "1.9.0" }))
        .send()
        .await
        .unwrap();
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 0, "old worker must be blocked by the pin");

    // A worker with no version is also blocked.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "handler_name": "external_handler", "worker_id": "nover", "limit": 10 }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<Vec<serde_json::Value>>().await.unwrap().len(),
        0
    );

    // A 2.1 worker satisfies the pin and claims the task.
    let resp = client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "handler_name": "external_handler", "worker_id": "new", "limit": 10, "version": "2.1.0" }))
        .send()
        .await
        .unwrap();
    let tasks: Vec<serde_json::Value> = resp.json().await.unwrap();
    assert_eq!(tasks.len(), 1, "new worker must satisfy the pin");
}

#[tokio::test]
async fn version_pin_crud() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    client
        .post(format!("{}/workers/version-pins", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "tenant_id": "t1", "handler_name": "h", "min_version": "1.0.0" }))
        .send()
        .await
        .unwrap();

    // Upsert overwrites min_version.
    client
        .post(format!("{}/workers/version-pins", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "tenant_id": "t1", "handler_name": "h", "min_version": "3.0.0" }))
        .send()
        .await
        .unwrap();

    let resp = client
        .get(format!(
            "{}/workers/version-pins?tenant_id=t1",
            srv.base_url
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let pins: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(pins.as_array().unwrap().len(), 1);
    assert_eq!(pins[0]["min_version"], "3.0.0");

    let resp = client
        .delete(format!("{}/workers/version-pins/t1/h", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let resp = client
        .get(format!("{}/workers/version-pins", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.json::<serde_json::Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn worker_reported_logs_persist_and_list() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    // Claim the task first so completion is accepted.
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "handler_name": "external_handler", "worker_id": "worker-1", "limit": 1 }))
        .send()
        .await
        .unwrap();

    // Complete the task with attached logs.
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/complete", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "output": { "ok": true },
            "logs": [
                { "ts": "2026-01-01T00:00:00Z", "level": "info", "message": "started" },
                { "ts": "2026-01-01T00:00:01Z", "level": "warn", "message": "slow upstream" }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The logs are retrievable, oldest first, annotated with the block id.
    let resp = client
        .get(format!("{}/instances/{inst_id}/logs", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let logs: serde_json::Value = resp.json().await.unwrap();
    let arr = logs.as_array().unwrap();
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["message"], "started");
    assert_eq!(arr[0]["level"], "info");
    assert_eq!(arr[0]["block_id"], "s1");
    assert_eq!(arr[1]["message"], "slow upstream");
}

#[tokio::test]
async fn worker_reported_logs_persist_on_fail_path() {
    // The fail endpoint shares `persist_reported_logs` with complete — a worker
    // that dies should still leave its breadcrumbs in the instance log.
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let task_id = seed_worker_task(&srv, inst_id).await;

    // Claim the task first so the failure is accepted.
    client
        .post(format!("{}/workers/tasks/poll", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({ "handler_name": "external_handler", "worker_id": "worker-1", "limit": 1 }))
        .send()
        .await
        .unwrap();

    // Fail permanently, attaching diagnostic logs.
    let resp = client
        .post(format!("{}/workers/tasks/{task_id}/fail", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "worker_id": "worker-1",
            "message": "boom",
            "retryable": false,
            "logs": [
                { "ts": "2026-01-01T00:00:00Z", "level": "error", "message": "connection refused" }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The failure logs are retrievable on the instance.
    let resp = client
        .get(format!("{}/instances/{inst_id}/logs", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let logs: serde_json::Value = resp.json().await.unwrap();
    let arr = logs.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["message"], "connection refused");
    assert_eq!(arr[0]["level"], "error");
    assert_eq!(arr[0]["block_id"], "s1");
}

#[tokio::test]
async fn instance_logs_empty_when_none() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq_id = create_sequence(&client, &srv.base_url).await;
    let inst_id = create_instance(&client, &srv.base_url, seq_id).await;
    let resp = client
        .get(format!("{}/instances/{inst_id}/logs", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        0
    );
}
