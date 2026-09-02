use chrono::Utc;
use orch8_api::test_harness::{TestServer, spawn_test_server_with_artifacts};
use orch8_storage::{ContinuityStore, OutputStore, ResourceStore, WorkerStore};
use orch8_types::continuity::CapsuleRequirements;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::worker::{WorkerTask, WorkerTaskState};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

async fn poll_tasks(response: reqwest::Response) -> Vec<Value> {
    let body: Value = response.json().await.unwrap();
    body["tasks"].as_array().unwrap().clone()
}

fn sequence_body(id: Uuid, tenant: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "distributed-tests",
        "name": format!("distributed-sequence-{id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{
            "type": "step", "id": "device-file", "handler": "device_file",
            "params": {}, "cancellable": true
        }],
        "interceptors": null,
        "created_at": Utc::now().to_rfc3339()
    })
}

async fn create_instance(server: &TestServer, client: &Client, tenant: &str) -> Uuid {
    let sequence_id = Uuid::now_v7();
    let response = client
        .post(format!("{}/sequences", server.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(&sequence_body(sequence_id, tenant))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let response = client
        .post(format!("{}/instances", server.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": sequence_id,
            "tenant_id": tenant,
            "namespace": "distributed-tests",
            "context": { "data": {}, "config": {}, "audit": [] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}

fn worker_task(
    instance: Uuid,
    block_id: &str,
    requirements: CapsuleRequirements,
    queue_name: Option<&str>,
    state: WorkerTaskState,
    worker_id: Option<&str>,
    claim_epoch: u64,
) -> WorkerTask {
    let now = Utc::now();
    WorkerTask {
        id: Uuid::now_v7(),
        instance_id: InstanceId::from_uuid(instance),
        block_id: BlockId::new(block_id),
        handler_name: "device_file".into(),
        queue_name: queue_name.map(String::from),
        requirements,
        params: json!({"prompt": "select a private file"}),
        context: json!({}),
        attempt: 0,
        timeout_ms: None,
        state,
        worker_id: worker_id.map(String::from),
        claimed_at: worker_id.map(|_| now),
        heartbeat_at: worker_id.map(|_| now),
        claim_epoch,
        resume_checkpoint: None,
        checkpoint_seq: 0,
        completed_at: None,
        output: None,
        error_message: None,
        error_retryable: None,
        created_at: now,
    }
}

fn runtime_capabilities(runtime_id: Uuid, handler: &str, region: &str) -> Value {
    let now = Utc::now();
    json!({
        "runtime_id": runtime_id,
        "kind": "mobile",
        "trust": "registered",
        "handlers": [handler],
        "plugins": ["chrome"],
        "regions": [region],
        "hardware": ["secure_enclave"],
        "connectivity": "wifi",
        "observed_at": now.to_rfc3339(),
        "expires_at": (now + chrono::Duration::minutes(4)).to_rfc3339()
    })
}

fn norway_requirements() -> CapsuleRequirements {
    CapsuleRequirements {
        handlers: vec!["device_file".into()],
        plugins: vec!["chrome".into()],
        regions: vec!["norway".into()],
        hardware: vec!["secure_enclave".into()],
        requires_network: true,
        requires_human_ui: true,
        ..Default::default()
    }
}

async fn seed_claimed_task(
    server: &TestServer,
    client: &Client,
    tenant: &str,
    worker_id: &str,
    claim_epoch: u64,
) -> WorkerTask {
    let instance = create_instance(server, client, tenant).await;
    let task = worker_task(
        instance,
        "device-file",
        CapsuleRequirements::default(),
        None,
        WorkerTaskState::Claimed,
        Some(worker_id),
        claim_epoch,
    );
    server.storage.create_worker_task(&task).await.unwrap();
    task
}

#[tokio::test]
async fn capability_poll_claims_only_compatible_queue_task_and_records_runtime() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let instance = create_instance(&server, &client, tenant).await;
    let matching = worker_task(
        instance,
        "norway-file",
        norway_requirements(),
        Some("trusted-devices"),
        WorkerTaskState::Pending,
        None,
        0,
    );
    let mut wrong_requirements = norway_requirements();
    wrong_requirements.regions = vec!["sweden".into()];
    let incompatible = worker_task(
        instance,
        "sweden-file",
        wrong_requirements,
        Some("trusted-devices"),
        WorkerTaskState::Pending,
        None,
        0,
    );
    server.storage.create_worker_task(&matching).await.unwrap();
    server
        .storage
        .create_worker_task(&incompatible)
        .await
        .unwrap();
    let runtime_id = Uuid::now_v7();

    let response = client
        .post(format!("{}/workers/tasks/poll/queue", server.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "queue_name": "trusted-devices",
            "handler_name": "device_file",
            "worker_id": runtime_id,
            "limit": 10,
            "capabilities": runtime_capabilities(runtime_id, "device_file", "norway")
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let tasks = poll_tasks(response).await;
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0]["id"], matching.id.to_string());
    assert_eq!(tasks[0]["claim_epoch"], 1);
    assert_eq!(
        server
            .storage
            .get_worker_task(incompatible.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerTaskState::Pending
    );
    let advertisements = server
        .storage
        .list_runtime_capabilities(&TenantId::unchecked(tenant), Utc::now(), 10)
        .await
        .unwrap();
    assert_eq!(advertisements.len(), 1);
    assert_eq!(
        advertisements[0].runtime_id.to_string(),
        runtime_id.to_string()
    );
}

#[tokio::test]
async fn legacy_poll_cannot_claim_capability_constrained_task() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let instance = create_instance(&server, &client, tenant).await;
    let constrained = worker_task(
        instance,
        "constrained",
        norway_requirements(),
        None,
        WorkerTaskState::Pending,
        None,
        0,
    );
    server
        .storage
        .create_worker_task(&constrained)
        .await
        .unwrap();

    let response = client
        .post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "handler_name": "device_file",
            "worker_id": "legacy-worker",
            "limit": 1
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(poll_tasks(response).await.is_empty());
    assert_eq!(
        server
            .storage
            .get_worker_task(constrained.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerTaskState::Pending
    );
}

#[tokio::test]
async fn capability_poll_rejects_worker_runtime_identity_mismatch() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let advertised_runtime = Uuid::now_v7();

    let response = client
        .post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("X-Tenant-Id", "device-tenant")
        .json(&json!({
            "handler_name": "device_file",
            "worker_id": Uuid::now_v7(),
            "capabilities": runtime_capabilities(advertised_runtime, "device_file", "norway")
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("runtime_id"));
}

#[tokio::test]
async fn capability_poll_rejects_advertisement_without_requested_handler() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let runtime_id = Uuid::now_v7();

    let response = client
        .post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("X-Tenant-Id", "device-tenant")
        .json(&json!({
            "handler_name": "device_file",
            "worker_id": runtime_id,
            "capabilities": runtime_capabilities(runtime_id, "another_handler", "norway")
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("handlers"));
}

#[tokio::test]
async fn capability_poll_rejects_expired_advertisement() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let runtime_id = Uuid::now_v7();
    let mut capabilities = runtime_capabilities(runtime_id, "device_file", "norway");
    capabilities["expires_at"] = json!((Utc::now() - chrono::Duration::seconds(1)).to_rfc3339());

    let response = client
        .post(format!("{}/workers/tasks/poll", server.v1_url()))
        .header("X-Tenant-Id", "device-tenant")
        .json(&json!({
            "handler_name": "device_file",
            "worker_id": runtime_id,
            "capabilities": capabilities
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("expiry"));
}

#[tokio::test]
async fn leased_artifact_upload_is_tenant_scoped_and_idempotent() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let instance = create_instance(&server, &client, tenant).await;
    let task_id = Uuid::now_v7();
    let worker_id = Uuid::now_v7().to_string();
    server
        .storage
        .create_worker_task(&WorkerTask {
            id: task_id,
            instance_id: InstanceId::from_uuid(instance),
            block_id: BlockId::new("device-file"),
            handler_name: "device_file".into(),
            queue_name: None,
            requirements: orch8_types::continuity::CapsuleRequirements::default(),
            params: json!({}),
            context: json!({}),
            attempt: 0,
            timeout_ms: None,
            state: WorkerTaskState::Claimed,
            worker_id: Some(worker_id.clone()),
            claimed_at: Some(Utc::now()),
            heartbeat_at: Some(Utc::now()),
            claim_epoch: 7,
            resume_checkpoint: None,
            checkpoint_seq: 0,
            completed_at: None,
            output: None,
            error_message: None,
            error_retryable: None,
            created_at: Utc::now(),
        })
        .await
        .unwrap();
    let upload_id = Uuid::now_v7();
    let url = format!(
        "{}/workers/tasks/{task_id}/artifacts/{upload_id}?worker_id={worker_id}&claim_epoch=7&file_name=private.txt",
        server.v1_url()
    );

    let upload = |bytes: &'static str, tenant: &'static str| {
        client
            .post(&url)
            .header("X-Tenant-Id", tenant)
            .header("Content-Type", "text/plain")
            .body(bytes)
            .send()
    };
    let response = upload("private bytes", tenant).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let first = response.json::<Value>().await.unwrap();
    assert_eq!(first["upload_id"], upload_id.to_string());
    assert_eq!(first["size"], 13);

    let response = upload("private bytes", tenant).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.json::<Value>().await.unwrap()["artifact"]["key"],
        first["artifact"]["key"]
    );

    let response = upload("changed", tenant).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);

    let response = upload("private bytes", "other-tenant").await.unwrap();
    assert!(matches!(
        response.status(),
        StatusCode::FORBIDDEN | StatusCode::NOT_FOUND
    ));
}

#[tokio::test]
async fn artifact_upload_rejects_sha256_mismatch_without_storing_bytes() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let worker_id = Uuid::now_v7().to_string();
    let task = seed_claimed_task(&server, &client, tenant, &worker_id, 3).await;
    let upload_id = Uuid::now_v7();

    let response = client
        .post(format!(
            "{}/workers/tasks/{}/artifacts/{upload_id}?worker_id={worker_id}&claim_epoch=3&sha256={}",
            server.v1_url(),
            task.id,
            "0".repeat(64)
        ))
        .header("X-Tenant-Id", tenant)
        .body("private bytes")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(response.text().await.unwrap().contains("sha256 mismatch"));
    assert!(
        server
            .storage
            .get_artifact(&format!("{}/{upload_id}", task.instance_id))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn artifact_upload_obeys_the_global_ten_mebibyte_limit() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let worker_id = Uuid::now_v7().to_string();
    let task = seed_claimed_task(&server, &client, tenant, &worker_id, 3).await;
    let upload_id = Uuid::now_v7();

    let response = client
        .post(format!(
            "{}/workers/tasks/{}/artifacts/{upload_id}?worker_id={worker_id}&claim_epoch=3",
            server.v1_url(),
            task.id
        ))
        .header("X-Tenant-Id", tenant)
        .header(reqwest::header::EXPECT, "100-continue")
        .body(vec![0_u8; 10 * 1024 * 1024 + 1])
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(
        server
            .storage
            .get_artifact(&format!("{}/{upload_id}", task.instance_id))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn artifact_upload_rejects_stale_claim_epoch_without_storing_bytes() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let worker_id = Uuid::now_v7().to_string();
    let task = seed_claimed_task(&server, &client, tenant, &worker_id, 9).await;
    let upload_id = Uuid::now_v7();

    let response = client
        .post(format!(
            "{}/workers/tasks/{}/artifacts/{upload_id}?worker_id={worker_id}&claim_epoch=8",
            server.v1_url(),
            task.id
        ))
        .header("X-Tenant-Id", tenant)
        .body("private bytes")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(
        server
            .storage
            .get_artifact(&format!("{}/{upload_id}", task.instance_id))
            .await
            .unwrap()
            .is_none()
    );
    let attempts = server
        .storage
        .list_worker_task_attempt_events(task.id, 10)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].event.as_str(), "stale_mutation_rejected");
}

#[tokio::test]
async fn uploaded_receipt_can_complete_task_and_resume_server_workflow() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let tenant = "device-tenant";
    let worker_id = Uuid::now_v7().to_string();
    let task = seed_claimed_task(&server, &client, tenant, &worker_id, 4).await;
    let upload_id = Uuid::now_v7();
    let upload = client
        .post(format!(
            "{}/workers/tasks/{}/artifacts/{upload_id}?worker_id={worker_id}&claim_epoch=4&file_name=private.txt",
            server.v1_url(),
            task.id
        ))
        .header("X-Tenant-Id", tenant)
        .header("Content-Type", "text/plain")
        .body("private bytes")
        .send()
        .await
        .unwrap();
    assert_eq!(upload.status(), StatusCode::CREATED);
    let receipt = upload.json::<Value>().await.unwrap();
    let output = json!({
        "artifact": receipt["artifact"].clone(),
        "sha256": receipt["sha256"].clone()
    });

    let response = client
        .post(format!(
            "{}/workers/tasks/{}/complete",
            server.v1_url(),
            task.id
        ))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "worker_id": worker_id,
            "claim_epoch": 4,
            "output": output.clone()
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let completed = server
        .storage
        .get_worker_task(task.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, WorkerTaskState::Completed);
    assert_eq!(completed.output, Some(output.clone()));
    let stored_output = server
        .storage
        .get_block_output(task.instance_id, &task.block_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored_output.output, output);
}
