//! End-to-end coverage for the instance checkpoint HTTP contract.

use orch8_api::test_harness::spawn_test_server;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use uuid::Uuid;

fn sequence_body(id: Uuid, tenant: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "checkpoint-tests",
        "name": "checkpoint-sequence",
        "version": 1,
        "deprecated": false,
        "blocks": [{
            "type": "step",
            "id": "step-1",
            "handler": "noop",
            "params": {},
            "cancellable": true
        }],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

async fn create_instance(client: &Client, base: &str, tenant: &str) -> Uuid {
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
            "namespace": "checkpoint-tests",
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

async fn save_checkpoint(
    client: &Client,
    base: &str,
    tenant: &str,
    instance_id: Uuid,
    checkpoint_data: Value,
) -> Uuid {
    let response = client
        .post(format!("{base}/instances/{instance_id}/checkpoints"))
        .header("X-Tenant-Id", tenant)
        .json(&json!({ "checkpoint_data": checkpoint_data }))
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

async fn assert_checkpoint_routes_not_found(
    client: &Client,
    base: &str,
    tenant: &str,
    instance_id: Uuid,
) {
    for (method, suffix, body) in [
        ("GET", "checkpoints", None),
        ("GET", "checkpoints/latest", None),
        (
            "POST",
            "checkpoints",
            Some(json!({ "checkpoint_data": {} })),
        ),
        ("POST", "checkpoints/prune", Some(json!({ "keep": 0 }))),
    ] {
        let mut request = match method {
            "GET" => client.get(format!("{base}/instances/{instance_id}/{suffix}")),
            "POST" => client.post(format!("{base}/instances/{instance_id}/{suffix}")),
            _ => unreachable!(),
        }
        .header("X-Tenant-Id", tenant);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "{method} {suffix}"
        );
    }
}

#[tokio::test]
async fn checkpoint_lifecycle_preserves_data_order_and_prunes_old_history() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&client, &base, "tenant-a").await;

    let first_id = save_checkpoint(
        &client,
        &base,
        "tenant-a",
        instance_id,
        json!({ "cursor": 1, "nested": { "ready": false } }),
    )
    .await;
    let second_id = save_checkpoint(
        &client,
        &base,
        "tenant-a",
        instance_id,
        json!({ "cursor": 2, "nested": { "ready": true } }),
    )
    .await;
    let third_id = save_checkpoint(
        &client,
        &base,
        "tenant-a",
        instance_id,
        json!({ "cursor": 3, "items": ["a", "b"] }),
    )
    .await;
    assert_ne!(first_id, second_id);
    assert_ne!(second_id, third_id);

    let response = client
        .get(format!("{base}/instances/{instance_id}/checkpoints"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let checkpoints = response.json::<Vec<Value>>().await.unwrap();
    assert_eq!(checkpoints.len(), 3);
    assert_eq!(checkpoints[0]["id"], third_id.to_string());
    assert_eq!(
        checkpoints[0]["checkpoint_data"]["items"],
        json!(["a", "b"])
    );
    assert_eq!(checkpoints[2]["id"], first_id.to_string());

    let response = client
        .get(format!("{base}/instances/{instance_id}/checkpoints/latest"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let latest = response.json::<Value>().await.unwrap();
    assert_eq!(latest["id"], third_id.to_string());
    assert_eq!(latest["instance_id"], instance_id.to_string());
    assert_eq!(latest["checkpoint_data"]["cursor"], 3);

    let response = client
        .post(format!("{base}/instances/{instance_id}/checkpoints/prune"))
        .header("X-Tenant-Id", "tenant-a")
        .json(&json!({ "keep": 1 }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap()["count"], 2);

    let remaining = client
        .get(format!("{base}/instances/{instance_id}/checkpoints"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json::<Vec<Value>>()
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0]["id"], third_id.to_string());
}

#[tokio::test]
async fn latest_checkpoint_is_not_found_until_one_is_saved() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&client, &base, "tenant-a").await;

    let response = client
        .get(format!("{base}/instances/{instance_id}/checkpoints/latest"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn checkpoint_routes_hide_instances_from_other_tenants() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&client, &base, "tenant-a").await;
    let checkpoint_id = save_checkpoint(
        &client,
        &base,
        "tenant-a",
        instance_id,
        json!({ "private": true }),
    )
    .await;

    assert_checkpoint_routes_not_found(&client, &base, "tenant-b", instance_id).await;

    let checkpoints = client
        .get(format!("{base}/instances/{instance_id}/checkpoints"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap()
        .json::<Vec<Value>>()
        .await
        .unwrap();
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0]["id"], checkpoint_id.to_string());
}

#[tokio::test]
async fn checkpoint_routes_return_not_found_for_unknown_instance() {
    let server = spawn_test_server().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = Uuid::now_v7();

    assert_checkpoint_routes_not_found(&client, &base, "tenant-a", instance_id).await;
}
