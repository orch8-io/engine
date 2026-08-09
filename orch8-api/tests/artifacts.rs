//! End-to-end coverage for tenant-scoped artifact listing and downloads.

use bytes::Bytes;
use orch8_api::test_harness::{TestServer, spawn_test_server_with_artifacts};
use orch8_storage::ResourceStore;
use orch8_types::ids::InstanceId;
use reqwest::{Client, StatusCode, header};
use serde_json::{Value, json};
use uuid::Uuid;

fn sequence_body(id: Uuid, tenant: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": tenant,
        "namespace": "artifact-tests",
        "name": format!("artifact-sequence-{id}"),
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
            "namespace": "artifact-tests",
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

#[tokio::test]
async fn list_and_download_artifact_preserve_metadata_bytes_and_safe_headers() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;
    let artifact = server
        .storage
        .put_artifact(
            InstanceId::from_uuid(instance_id),
            "text/plain",
            Bytes::from_static(b"durable output"),
        )
        .await
        .unwrap();

    let response = client
        .get(format!("{base}/instances/{instance_id}/artifacts"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.json::<Value>().await.unwrap();
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["key"], artifact.key);
    assert_eq!(body["items"][0]["size"], 14);
    assert_eq!(body["items"][0]["uri"], artifact.uri);

    let response = client
        .get(format!("{base}/artifacts/{}", artifact.key))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_TYPE],
        "application/octet-stream"
    );
    assert_eq!(
        response.headers()[header::CONTENT_DISPOSITION],
        "attachment"
    );
    assert_eq!(
        response.headers()[header::X_CONTENT_TYPE_OPTIONS],
        "nosniff"
    );
    assert_eq!(
        response.bytes().await.unwrap(),
        b"durable output".as_slice()
    );
}

#[tokio::test]
async fn download_accepts_a_valid_content_type_but_still_forces_attachment() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;
    let artifact = server
        .storage
        .put_artifact(
            InstanceId::from_uuid(instance_id),
            "application/json",
            Bytes::from_static(br#"{"safe":true}"#),
        )
        .await
        .unwrap();

    let response = client
        .get(format!(
            "{base}/artifacts/{}?content_type=application%2Fjson",
            artifact.key
        ))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
    assert_eq!(
        response.headers()[header::CONTENT_DISPOSITION],
        "attachment"
    );
}

#[tokio::test]
async fn artifact_routes_hide_data_from_other_tenants() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;
    let artifact = server
        .storage
        .put_artifact(
            InstanceId::from_uuid(instance_id),
            "text/plain",
            Bytes::from_static(b"private"),
        )
        .await
        .unwrap();

    let list = client
        .get(format!("{base}/instances/{instance_id}/artifacts"))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::NOT_FOUND);

    let download = client
        .get(format!("{base}/artifacts/{}", artifact.key))
        .header("X-Tenant-Id", "tenant-b")
        .send()
        .await
        .unwrap();
    assert_eq!(download.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn artifact_download_rejects_malformed_and_missing_keys() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;

    let malformed = client
        .get(format!("{base}/artifacts/not-an-instance/artifact-id"))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(malformed.status(), StatusCode::NOT_FOUND);

    let missing = client
        .get(format!("{base}/artifacts/{instance_id}/{}", Uuid::now_v7()))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn artifact_download_rejects_header_injection_in_content_type() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let base = server.v1_url();
    let instance_id = create_instance(&server, &client, "tenant-a").await;
    let artifact = server
        .storage
        .put_artifact(
            InstanceId::from_uuid(instance_id),
            "text/plain",
            Bytes::from_static(b"safe"),
        )
        .await
        .unwrap();

    let response = client
        .get(format!(
            "{base}/artifacts/{}?content_type=text%2Fplain%0D%0AX-Evil%3Ayes",
            artifact.key
        ))
        .header("X-Tenant-Id", "tenant-a")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}
