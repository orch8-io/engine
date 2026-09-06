use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;

#[tokio::test]
async fn approvals_endpoint_returns_empty_when_no_waiting_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();

    let resp = client
        .get(format!("{}/approvals", srv.base_url))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["items"], serde_json::json!([]));
    assert_eq!(body["total"], 0);
    assert_eq!(body["scanned_count"], 0);
    assert!(body["next_offset"].is_null());
}

#[tokio::test]
async fn approvals_pagination_advances_by_scanned_instances_even_without_decisions() {
    use chrono::Utc;
    use orch8_storage::InstanceStore;
    use orch8_types::context::ExecutionContext;
    use orch8_types::ids::{InstanceId, Namespace, SequenceId, TenantId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let sequence_id = SequenceId::new();
    let now = Utc::now();
    let response = client.post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", "tenant-a")
        .json(&serde_json::json!({"id": sequence_id, "tenant_id": "tenant-a", "namespace": "default", "name": "pagination", "version": 1, "blocks": [{"type": "step", "id": "step", "handler": "noop", "params": {}}], "created_at": now}))
        .send().await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    for index in 0..1001 {
        srv.storage
            .create_instance(&TaskInstance {
                id: InstanceId::new(),
                sequence_id,
                tenant_id: TenantId::unchecked("tenant-a"),
                namespace: Namespace::new("default"),
                state: InstanceState::Waiting,
                next_fire_at: None,
                priority: Priority::Normal,
                timezone: "UTC".into(),
                metadata: serde_json::json!({}),
                context: ExecutionContext::default(),
                concurrency_key: None,
                max_concurrency: None,
                idempotency_key: None,
                session_id: None,
                parent_instance_id: None,
                budget: None,
                created_at: now + chrono::Duration::seconds(index),
                updated_at: now,
            })
            .await
            .unwrap();
    }
    for (offset, limit, scanned, next) in [
        (0, 2, 2, Some(2)),
        (0, 1000, 1000, Some(1000)),
        (1000, 1000, 1, None),
    ] {
        let body: serde_json::Value = client
            .get(format!(
                "{}/approvals?limit={limit}&offset={offset}",
                srv.v1_url()
            ))
            .header("X-Tenant-Id", "tenant-a")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(body["items"], serde_json::json!([]));
        assert_eq!(body["scanned_count"], scanned);
        assert_eq!(body["next_offset"], serde_json::json!(next));
    }
}
