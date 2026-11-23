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

#[tokio::test]
async fn media_upload_replays_range_and_checks_final_digest_before_acknowledgement() {
    let server = spawn_test_server_with_artifacts().await;
    let client = Client::new();
    let instance = create_instance(&server, &client, "tenant-a").await;
    let base = format!(
        "{}/instances/{instance}/media-uploads/{}",
        server.v1_url(),
        Uuid::now_v7()
    );
    let manifest = json!({
        "length": 5,
        "chunk_bytes": 3,
        "sha256": "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
    });
    assert_eq!(
        client
            .put(&base)
            .header("X-Tenant-Id", "tenant-a")
            .json(&manifest)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(
        client
            .put(&base)
            .header("X-Tenant-Id", "tenant-b")
            .json(&manifest)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let append = |offset, body: &'static str| {
        client
            .put(format!("{base}/ranges/{offset}"))
            .header("X-Tenant-Id", "tenant-a")
            .body(body)
            .send()
    };

    assert_eq!(
        append(3, "lo").await.unwrap().status(),
        StatusCode::CONFLICT
    );
    for _ in 0..2 {
        let reply = append(0, "hel").await.unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        assert_eq!(reply.json::<Value>().await.unwrap()["offset"], 3);
    }
    assert_eq!(
        append(0, "hex").await.unwrap().status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        append(3, "xx").await.unwrap().status(),
        StatusCode::BAD_REQUEST
    );
    let read = |offset| {
        client
            .get(format!("{base}/ranges/{offset}"))
            .header("X-Tenant-Id", "tenant-a")
            .send()
    };
    assert_eq!(read(0).await.unwrap().status(), StatusCode::CONFLICT);
    for _ in 0..2 {
        let reply = append(3, "lo").await.unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        assert_eq!(reply.json::<Value>().await.unwrap()["offset"], 5);
    }
    assert_eq!(
        append(3, "xx").await.unwrap().status(),
        StatusCode::CONFLICT
    );
    assert_eq!(
        read(0).await.unwrap().bytes().await.unwrap(),
        b"hel".as_slice()
    );
    assert_eq!(
        read(3).await.unwrap().bytes().await.unwrap(),
        b"lo".as_slice()
    );
}
