//! E2E tests for the Workers API.

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::StorageBackend;
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
        instance_id: orch8_types::ids::InstanceId(instance_id),
        block_id: orch8_types::ids::BlockId("s1".into()),
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
