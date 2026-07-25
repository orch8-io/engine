use chrono::Utc;
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::AdminStore;
use orch8_types::audit::AuditLogEntry;
use orch8_types::ids::{InstanceId, TenantId};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

async fn seed_changes(
    server: &TestServer,
    client: &reqwest::Client,
    base: &str,
) -> [AuditLogEntry; 2] {
    let sequence_id = Uuid::now_v7();
    let sequence = json!({
        "id": sequence_id,
        "tenant_id": "tenant-a",
        "namespace": "default",
        "name": "change-feed",
        "version": 1,
        "blocks": [{"type": "step", "id": "noop", "handler": "noop", "params": {}}],
        "created_at": Utc::now().to_rfc3339(),
    });
    assert_eq!(
        client
            .post(format!("{base}/sequences"))
            .header("X-Tenant-Id", "tenant-a")
            .json(&sequence)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED
    );
    let instance: Value = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "sequence_id": sequence_id,
            "tenant_id": "tenant-a",
            "namespace": "default"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let instance_id =
        InstanceId::from_uuid(Uuid::parse_str(instance["id"].as_str().unwrap()).unwrap());
    let created_at = Utc::now();
    let mut entries = ["first", "second"].map(|event_type| AuditLogEntry {
        id: Uuid::now_v7(),
        instance_id,
        tenant_id: TenantId::unchecked("tenant-a"),
        event_type: event_type.into(),
        from_state: None,
        to_state: None,
        block_id: None,
        details: json!({}),
        created_at,
    });
    entries.sort_by_key(|entry| entry.id);
    for entry in &entries {
        server.storage.append_audit_log(entry).await.unwrap();
    }
    entries
}

#[tokio::test]
async fn change_feed_resumes_without_same_timestamp_gaps_or_duplicates() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = server.v1_url();
    let entries = seed_changes(&server, &client, &base).await;

    let first: Value = client
        .get(format!("{base}/changes?tenant_id=ignored&limit=1"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(first["changes"][0]["id"], entries[0].id.to_string());
    assert_eq!(first["has_more"], true);
    let cursor = first["next_cursor"].as_str().unwrap();
    let second: Value = client
        .get(format!("{base}/changes?cursor={cursor}&limit=10"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(second["changes"].as_array().unwrap().len(), 1);
    assert_eq!(second["changes"][0]["id"], entries[1].id.to_string());
    assert_eq!(second["has_more"], false);

    let mut stream = client
        .get(format!("{base}/changes/stream?limit=1"))
        .header("X-Tenant-Id", "tenant-a")
        .header("Last-Event-ID", cursor)
        .send()
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    let chunk = tokio::time::timeout(std::time::Duration::from_secs(2), stream.chunk())
        .await
        .expect("change SSE event timeout")
        .unwrap()
        .expect("change SSE event");
    let event = String::from_utf8(chunk.to_vec()).unwrap();
    assert!(event.contains("event: change"));
    assert!(event.contains(&entries[1].id.to_string()));

    assert_eq!(
        client
            .get(format!("{base}/changes?cursor=invalid"))
            .header("X-Tenant-Id", "tenant-a")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    assert_eq!(
        client
            .get(format!("{base}/changes"))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
}
