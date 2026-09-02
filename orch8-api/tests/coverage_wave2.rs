//! Second-wave endpoint coverage for plugin, session, and audit contracts.

use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::AdminStore;
use orch8_types::audit::AuditLogEntry;
use orch8_types::ids::{InstanceId, TenantId};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

fn sequence_body(id: Uuid, tenant: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "coverage-wave-2",
        "name": format!("sequence-{id}"),
        "version": 1,
        "deprecated": false,
        "blocks": [{"type": "step", "id": "step-1", "handler": "noop", "params": {}}],
        "created_at": chrono::Utc::now()
    })
}

async fn create_instance(server: &TestServer, client: &Client, tenant: &str) -> Uuid {
    let base = server.v1_url();
    let sequence_id = Uuid::now_v7();
    let response = client
        .post(format!("{base}/sequences"))
        .header("X-Tenant-Id", tenant)
        .json(&sequence_body(sequence_id, tenant))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({
            "sequence_id": sequence_id,
            "tenant_id": tenant,
            "namespace": "coverage-wave-2",
            "context": {"data": {}, "config": {}, "audit": []}
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

#[tokio::test]
async fn plugin_crud_round_trip_preserves_mutable_fields() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let name = format!("plugin-{}", Uuid::now_v7());

    let response = client
        .post(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "name": name,
            "plugin_type": "wasm",
            "source": "/plugins/original.wasm",
            "tenant_id": "tenant-a",
            "config": {"memory_mb": 64},
            "description": "original"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = client
        .patch(format!("{base}/plugins/{name}"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "source": "/plugins/revised.wasm",
            "enabled": false,
            "config": {"memory_mb": 128},
            "description": "revised"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let plugin: Value = response.json().await.unwrap();
    assert_eq!(plugin["source"], "/plugins/revised.wasm");
    assert_eq!(plugin["enabled"], false);
    assert_eq!(plugin["config"]["memory_mb"], 128);
    assert_eq!(plugin["description"], "revised");

    let listed: Value = client
        .get(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed.as_array().unwrap().len(), 1);

    let response = client
        .delete(format!("{base}/plugins/{name}"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    let response = client
        .get(format!("{base}/plugins/{name}"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn plugins_are_tenant_isolated() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let name = format!("private-{}", Uuid::now_v7());
    let response = client
        .post(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "name": name,
            "plugin_type": "grpc",
            "source": "https://plugins.example.test/service",
            "tenant_id": "tenant-a"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let response = client
        .get(format!("{base}/plugins/{name}"))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let listed: Value = client
        .get(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(listed.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn plugin_creation_rejects_missing_and_oversized_fields() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();

    let missing = client
        .post(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "name": "",
            "plugin_type": "wasm",
            "source": "",
            "tenant_id": "tenant-a"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::BAD_REQUEST);

    let oversized = client
        .post(format!("{base}/plugins"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "name": "n".repeat(256),
            "plugin_type": "wasm",
            "source": "s".repeat(2049),
            "tenant_id": "tenant-a"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn session_lifecycle_round_trip_updates_data_and_state() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let key = format!("onboarding-{}", Uuid::now_v7());

    let response = client
        .post(format!("{base}/sessions"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({
            "tenant_id": "tenant-a",
            "session_key": key,
            "data": {"step": 1}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let created: Value = response.json().await.unwrap();
    let id = created["id"].as_str().unwrap();
    assert_eq!(created["state"], "active");

    let response = client
        .patch(format!("{base}/sessions/{id}/data"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({"data": {"step": 2, "complete": true}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .patch(format!("{base}/sessions/{id}/state"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({"state": "completed"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let fetched: Value = client
        .get(format!("{base}/sessions/by-key/tenant-a/{key}"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(fetched["state"], "completed");
    assert_eq!(fetched["data"], json!({"step": 2, "complete": true}));

    let instances: Value = client
        .get(format!("{base}/sessions/{id}/instances"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(instances.as_array().unwrap().is_empty());
}

#[tokio::test]
async fn sessions_enforce_tenant_isolation_and_key_bounds() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let response = client
        .post(format!("{base}/sessions"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({"tenant_id": "tenant-a", "session_key": "private-session"}))
        .send()
        .await
        .unwrap();
    let id = response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let hidden = client
        .get(format!("{base}/sessions/{id}"))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

    let empty_key = client
        .post(format!("{base}/sessions"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({"tenant_id": "tenant-a", "session_key": ""}))
        .send()
        .await
        .unwrap();
    assert_eq!(empty_key.status(), StatusCode::BAD_REQUEST);

    let oversized_key = client
        .post(format!("{base}/sessions"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({"tenant_id": "tenant-a", "session_key": "k".repeat(513)}))
        .send()
        .await
        .unwrap();
    assert_eq!(oversized_key.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn audit_endpoint_returns_persisted_entries_and_hides_foreign_instances() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;
    server
        .storage
        .append_audit_log(&AuditLogEntry {
            id: Uuid::now_v7(),
            instance_id: InstanceId::from_uuid(instance_id),
            tenant_id: TenantId::unchecked("tenant-a"),
            event_type: "coverage_event".into(),
            from_state: Some("scheduled".into()),
            to_state: Some("running".into()),
            block_id: Some("step-1".into()),
            details: json!({"source": "integration-test"}),
            created_at: chrono::Utc::now(),
        })
        .await
        .unwrap();

    let response = client
        .get(format!("{base}/instances/{instance_id}/audit"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let entries: Value = response.json().await.unwrap();
    assert_eq!(entries.as_array().unwrap().len(), 1);
    assert_eq!(entries[0]["event_type"], "coverage_event");
    assert_eq!(entries[0]["details"]["source"], "integration-test");

    let hidden = client
        .get(format!("{base}/instances/{instance_id}/audit"))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(hidden.status(), StatusCode::NOT_FOUND);

    let missing = client
        .get(format!("{base}/instances/{}/audit", Uuid::now_v7()))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}
