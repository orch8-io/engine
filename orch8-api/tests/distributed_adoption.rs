#![allow(clippy::too_many_lines)] // Each scenario verifies a complete HTTP flow with durable fixtures.

use chrono::Utc;
use orch8_storage::{ContinuityStore, InstanceStore, SequenceStore, WorkerStore};
use orch8_types::continuity::{ContinuityExecution, EffectReceipt, RuntimeCapabilities};
use orch8_types::ids::BlockId;
use orch8_types::instance::TaskInstance;
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::sequence::SequenceDefinition;
use orch8_types::worker::{WorkerRegistration, WorkerTask, WorkerTaskAttemptEvent};
use serde_json::{Value, json};
use uuid::Uuid;

const PRIVATE: &str = "fixture-secret-must-never-appear";

#[tokio::test]
async fn timeline_and_effect_review_are_tenant_scoped_and_redacted() {
    let server = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let now = Utc::now();
    let sequence: SequenceDefinition = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "tenant_id": "tenant-a",
        "namespace": "default",
        "name": "scope",
        "version": 1,
        "created_at": now,
        "blocks": [{ "type": "step", "handler": "private_compute", "id": "work" }],
    }))
    .unwrap();
    server.storage.create_sequence(&sequence).await.unwrap();
    let instance: TaskInstance = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "sequence_id": sequence.id,
        "tenant_id": sequence.tenant_id,
        "namespace": sequence.namespace,
        "state": "waiting",
        "priority": "Normal",
        "timezone": "UTC",
        "metadata": {},
        "context": { "data": {"private": PRIVATE} },
        "created_at": now,
        "updated_at": now,
    }))
    .unwrap();
    server.storage.create_instance(&instance).await.unwrap();
    let execution: ContinuityExecution = serde_json::from_value(json!({
        "continuity_id": Uuid::now_v7(),
        "tenant_id": sequence.tenant_id,
        "current_instance_id": instance.id,
        "owner_runtime_id": Uuid::now_v7(),
        "epoch": 1,
        "state": "owned",
        "updated_at": now,
    }))
    .unwrap();
    server
        .storage
        .create_continuity_execution(&execution)
        .await
        .unwrap();
    let task: WorkerTask = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "instance_id": instance.id,
        "block_id": "work",
        "handler_name": "private_compute",
        "queue_name": "  routed:EU / batch  ",
        "params": { "private": PRIVATE },
        "context": { "private": PRIVATE },
        "attempt": 0,
        "state": "failed",
        "error_message": PRIVATE,
        "created_at": now,
    }))
    .unwrap();
    server.storage.create_worker_task(&task).await.unwrap();
    let mut unnamed = task.clone();
    unnamed.id = Uuid::now_v7();
    unnamed.block_id = BlockId::new("other");
    unnamed.queue_name = None;
    server.storage.create_worker_task(&unnamed).await.unwrap();
    let attempt: WorkerTaskAttemptEvent = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "task_id": task.id,
        "claim_epoch": 1,
        "worker_id": PRIVATE,
        "event": "failed",
        "reason": PRIVATE,
        "created_at": now,
    }))
    .unwrap();
    server
        .storage
        .record_worker_task_attempt_event(&attempt)
        .await
        .unwrap();
    let effect: EffectReceipt = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "tenant_id": sequence.tenant_id,
        "continuity_id": execution.continuity_id,
        "epoch": execution.epoch,
        "instance_id": instance.id,
        "block_id": "work",
        "kind": "worker",
        "state": "unknown",
        "destination_fingerprint": PRIVATE,
        "idempotency_key": PRIVATE,
        "request_sha256": PRIVATE,
        "attempt": 0,
        "created_at": now,
        "updated_at": now,
    }))
    .unwrap();
    server.storage.create_effect_receipt(&effect).await.unwrap();

    let timeline = format!(
        "{}/instances/{}/runtime-timeline",
        server.v1_url(),
        instance.id
    );
    let trace: Value = client
        .get(&timeline)
        .header("x-tenant-id", "tenant-a")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let kinds: Vec<_> = trace["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"runtime_owner"));
    let queued = |id: Uuid| {
        let id = id.to_string();
        trace["events"]
            .as_array()
            .unwrap()
            .iter()
            .find(|event| event["kind"] == "task_queued" && event["task_id"] == id)
            .unwrap()
    };
    assert_eq!(queued(task.id)["queue_name"], "  routed:EU / batch  ");
    assert_eq!(queued(unnamed.id).get("queue_name"), Some(&Value::Null));
    assert!(kinds.contains(&"worker_attempt"));
    assert!(!trace.to_string().contains(PRIVATE));
    let foreign = client
        .get(&timeline)
        .header("x-tenant-id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), reqwest::StatusCode::NOT_FOUND);

    for index in 1..=100 {
        let mut subsequent = effect.clone();
        subsequent.id = orch8_types::continuity::EffectId::from_uuid(Uuid::from_u128(
            effect.id.as_uuid().as_u128() + index,
        ));
        subsequent.idempotency_key = Some(format!("fixture-effect-{index}"));
        server
            .storage
            .create_effect_receipt(&subsequent)
            .await
            .unwrap();
    }
    let queue = format!("{}/continuity/effects/review", server.v1_url());
    let own: Value = client
        .get(&queue)
        .header("x-tenant-id", "tenant-a")
        .query(&[("tenant_id", "tenant-a")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own["items"][0]["id"], json!(effect.id));
    assert!(!own.to_string().contains(PRIVATE));
    assert_eq!(own["items"].as_array().unwrap().len(), 100);
    let cursor = own["next_cursor"].as_str().unwrap();
    let second: Value = client
        .get(&queue)
        .header("x-tenant-id", "tenant-a")
        .query(&[("tenant_id", "tenant-a"), ("after", cursor)])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let final_id = Uuid::from_u128(effect.id.as_uuid().as_u128() + 100);
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert_eq!(second["items"][0]["id"], json!(final_id));
    assert_eq!(second["has_more"], false);
    assert_eq!(second["next_cursor"], Value::Null);

    let other: Value = client
        .get(&queue)
        .header("x-tenant-id", "tenant-b")
        .query(&[("tenant_id", "tenant-b")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(other["items"], json!([]));

    let settle = format!(
        "{}/continuity/effects/{}/resolve",
        server.v1_url(),
        effect.id
    );
    client.post(&settle).header("x-tenant-id", "tenant-a")
        .json(&json!({ "tenant_id": "tenant-a", "state": "verified", "provider_receipt_id": "verified-receipt" }))
        .send().await.unwrap().error_for_status().unwrap();
    let settled: Value = client
        .get(&queue)
        .header("x-tenant-id", "tenant-a")
        .query(&[("tenant_id", "tenant-a")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let remaining = settled["items"].as_array().unwrap();
    assert_eq!(remaining.len(), 100);
    assert!(remaining.iter().all(|item| item["id"] != json!(effect.id)));
    assert_eq!(settled["has_more"], false);
    assert_eq!(settled["next_cursor"], Value::Null);
}

#[tokio::test]
async fn reports_live_workers_and_canaries_without_relaxing_residency() {
    let server = orch8_api::test_harness::spawn_test_server().await;
    let client = reqwest::Client::new();
    let sequence: SequenceDefinition = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "tenant_id": "tenant-a",
        "namespace": "default",
        "name": "private-work",
        "version": 1,
        "created_at": Utc::now(),
        "placement": { "residency": "eu" },
        "blocks": [{
            "type": "step",
            "id": "private",
            "handler": "private_compute",
            "placement": { "labels": { "site": "private" } }
        }]
    }))
    .unwrap();
    server.storage.create_sequence(&sequence).await.unwrap();
    let now = Utc::now();
    server
        .storage
        .create_queue_routing_rule(&QueueRoutingRule {
            id: Uuid::now_v7(),
            tenant_id: sequence.tenant_id.to_string(),
            handler_name: "private_compute".into(),
            match_queue: None,
            queue_override: "private-computes".into(),
            priority: 1,
            enabled: true,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
    let matching = Uuid::now_v7();
    for (id, residency, version, expires_at) in [
        (matching, "eu", "2.0.0", now + chrono::Duration::minutes(1)),
        (
            Uuid::now_v7(),
            "us",
            "2.0.0",
            now + chrono::Duration::minutes(1),
        ),
        (
            Uuid::now_v7(),
            "eu",
            "3.0.0",
            now - chrono::Duration::minutes(1),
        ),
    ] {
        let caps: RuntimeCapabilities = serde_json::from_value(json!({
            "runtime_id": id,
            "kind": "server",
            "trust": "registered",
            "handlers": ["private_compute"],
            "labels": { "site": "private", "residency": residency },
            "observed_at": now,
            "expires_at": expires_at,
        }))
        .unwrap();
        server
            .storage
            .upsert_runtime_capabilities(&sequence.tenant_id, &caps)
            .await
            .unwrap();
        server
            .storage
            .upsert_worker_registration(&WorkerRegistration {
                worker_id: id.to_string(),
                handler_name: "private_compute".into(),
                queue_name: Some("private-computes".into()),
                version: Some(version.into()),
                tenant_id: Some(sequence.tenant_id.to_string()),
                last_seen_at: now,
                available_poll_slots: None,
            })
            .await
            .unwrap();
    }
    let url = format!("{}/placement/plan/{}", server.v1_url(), sequence.id);
    let report: Value = client
        .get(&url)
        .header("x-tenant-id", "tenant-a")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["ready"], true);
    assert_eq!(report["steps"][0]["eligible_workers"], json!([matching]));
    let report: Value = client
        .get(&url)
        .header("x-tenant-id", "tenant-a")
        .query(&[("recipe", "gpu"), ("worker_version", "2.0.0")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["ready"], false);
    assert_eq!(
        report["steps"][0]["matched_policies"],
        json!(["preview recipe gpu"])
    );
    let report: Value = client
        .get(&url)
        .header("x-tenant-id", "tenant-a")
        .query(&[("worker_version", "3.0.0")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(report["ready"], false);
    assert_eq!(report["steps"][0]["eligible_workers"], json!([]));
    let response = client
        .get(&url)
        .header("x-tenant-id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn enrollment_issues_expiring_worker_only_credentials() {
    use orch8_storage::sqlite::SqliteStorage;
    use std::sync::Arc;

    let root = "test-admin-key";
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let server = orch8_api::test_harness::spawn_test_server_on(storage, Some(root)).await;
    let client = reqwest::Client::new();
    let keys = format!("{}/api-keys", server.v1_url());
    let expires_at = Utc::now() + chrono::Duration::hours(24);
    let enrollment: Value = client
        .post(&keys)
        .header("x-api-key", root)
        .json(&json!({
            "tenant_id": "private-pilot",
            "name": "pilot worker",
            "capabilities": ["worker"],
            "expires_at": expires_at,
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let worker_key = enrollment["secret"].as_str().unwrap();
    assert_eq!(enrollment["capabilities"], json!(["worker"]));
    assert_eq!(enrollment["expires_at"], json!(expires_at));
    let poll = format!("{}/workers/tasks/poll", server.v1_url());
    let own = client
        .post(&poll)
        .header("x-api-key", worker_key)
        .json(&json!({"worker_id": "pilot", "handler_name": "private_compute"}))
        .send()
        .await
        .unwrap();
    assert_eq!(own.status(), reqwest::StatusCode::OK);
    let foreign = client
        .post(&poll)
        .header("x-api-key", worker_key)
        .header("x-tenant-id", "foreign")
        .json(&json!({"worker_id": "pilot", "handler_name": "private_compute"}))
        .send()
        .await
        .unwrap();
    assert_eq!(foreign.status(), reqwest::StatusCode::FORBIDDEN);
    let escalated = client
        .post(&keys)
        .header("x-api-key", worker_key)
        .json(&json!({"tenant_id": "private-pilot", "name": "unauthorized operator"}))
        .send()
        .await
        .unwrap();
    assert_eq!(escalated.status(), reqwest::StatusCode::FORBIDDEN);
    let listing: Value = client
        .get(&keys)
        .header("x-api-key", root)
        .query(&[("tenant_id", "private-pilot")])
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!listing.to_string().contains(worker_key));
}

#[tokio::test]
async fn placement_inventory_counts_are_scoped_and_samples_are_bounded() {
    use orch8_storage::sqlite::SqliteStorage;
    use orch8_storage::worker_inventory::{WorkerObservation, WorkerTransport};
    use orch8_types::ids::TenantId;
    use std::sync::Arc;

    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let server = orch8_api::test_harness::spawn_test_server_on(storage, None).await;
    let sequence: orch8_types::sequence::SequenceDefinition = serde_json::from_value(json!({
        "id": Uuid::now_v7(),
        "tenant_id": "tenant-a",
        "namespace": "placement",
        "name": "sampled-transport-s",
        "version": 1,
        "deprecated": false,
        "created_at": Utc::now(),
        "blocks": [
            {"type":"step", "id":"one", "handler":"bounded-readiness"},
            {"type":"step", "id":"two", "handler":"bounded-readiness"}
        ],
    }))
    .unwrap();
    server.storage.create_sequence(&sequence).await.unwrap();
    let handlers = vec!["bounded-readiness".to_owned()];
    let record = async |tenant: &TenantId, start: u32, end: u32| {
        for index in start..end {
            server
                .storage
                .record_worker_observation(&WorkerObservation {
                    tenant: Some(tenant),
                    worker_id: &format!("worker-{index:04}"),
                    handlers: &handlers,
                    queue_name: None,
                    all_queues: true,
                    version: Some("2"),
                    transport: WorkerTransport::Http,
                    runtime_id: None,
                    shared_runtime: None,
                    available_poll_slots: Some(1),
                    draining: false,
                })
                .await
                .unwrap();
        }
    };
    let client = reqwest::Client::new();
    let url = format!("{}/placement/plan/{}", server.v1_url(), sequence.id);
    let plan = async || {
        client
            .get(&url)
            .header("x-tenant-id", "tenant-a")
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .text()
            .await
            .unwrap()
    };
    record(&sequence.tenant_id, 0, 55).await;
    let before = plan().await;
    record(&TenantId::unchecked("tenant-b"), 0, 1010).await;
    let after = plan().await;
    assert_eq!(
        before, after,
        "unrelated tenant rows do not change response bytes"
    );
    let report: Value = serde_json::from_str(&after).unwrap();
    assert_eq!(report["ready"], true);
    for step in report["steps"].as_array().unwrap() {
        assert_eq!(step["worker_inventory"]["completeness"], "complete");
        assert_eq!(step["worker_inventory"]["eligible_count"], 55);
        assert_eq!(step["eligible_workers"].as_array().unwrap().len(), 50);
        assert_eq!(step["available_poll_slots"], 55);
    }

    record(&sequence.tenant_id, 55, 1001).await;
    let limited: Value = serde_json::from_str(&plan().await).unwrap();
    assert_eq!(limited["ready"], false);
    assert_eq!(limited["inventory_complete"], false);
    for step in limited["steps"].as_array().unwrap() {
        assert_eq!(
            step["worker_inventory"]["completeness"],
            "observation_limit_reached"
        );
        assert_eq!(step["worker_inventory"]["eligible_count"], 1000);
        assert_eq!(step["eligible_workers"].as_array().unwrap().len(), 50);
        assert!(step["available_poll_slots"].is_null());
    }
}
