use orch8_api::test_harness::spawn_test_server;
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn create(client: &reqwest::Client, base: &str, data: Value) -> String {
    let sequence: Value = client.post(format!("{base}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"id":uuid::Uuid::now_v7(),"created_at":chrono::Utc::now().to_rfc3339(),"deprecated":false,"tenant_id":"t1","namespace":"ns1","name":"context-patch-test","version":1,"blocks":[{"type":"step","id":"s1","handler":"noop","params":{}}]}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    let instance: Value = client.post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"sequence_id":sequence["id"],"tenant_id":"t1","namespace":"ns1","context":{"data":data,"config":{"retained":"config"},"runtime":{"total_steps_executed":7}}}))
        .send().await.unwrap().error_for_status().unwrap().json().await.unwrap();
    instance["id"].as_str().unwrap().to_owned()
}
async fn read(client: &reqwest::Client, base: &str, id: &str) -> Value {
    client
        .get(format!("{base}/instances/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}
#[tokio::test]
async fn data_patch_preserves_other_fields_and_context_sections() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(
        &client,
        &server.base_url,
        json!({"input":"keep","setting":"old"}),
    )
    .await;
    let before = read(&client, &server.base_url, &id).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"setting":"new","other":42}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let after = read(&client, &server.base_url, &id).await;
    assert_eq!(
        after["context"]["data"],
        json!({"input":"keep","setting":"new","other":42})
    );
    for section in ["config", "runtime", "audit"] {
        assert_eq!(after["context"][section], before["context"][section]);
    }
}
#[tokio::test]
async fn data_patch_rejects_invalid_shape_and_tenant_without_changes() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    for (tenant, body) in [
        ("t2", json!({"patch":{"keep":false}})),
        ("t1", json!({"patch":[],"runtime":{}})),
        ("t1", json!({"patch":{},"context":{}})),
    ] {
        let response = client
            .patch(format!("{}/instances/{id}/context/data", server.base_url))
            .header("X-Tenant-Id", tenant)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(!response.status().is_success());
    }
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!({"keep":true})
    );
}
#[tokio::test]
async fn data_patch_does_not_replace_non_object_data() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!([1, 2])).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"key":42}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!([1, 2])
    );
}
#[tokio::test]
async fn concurrent_accepted_patches_are_not_lost() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..12 {
        let client = client.clone();
        let url = format!("{}/instances/{id}/context/data", server.base_url);
        tasks.spawn(async move {
            let key = format!("key{index}");
            let response = client
                .patch(url)
                .header("X-Tenant-Id", "t1")
                .json(&json!({"patch":{key.clone():index}}))
                .send()
                .await
                .unwrap();
            (key, index, response.status())
        });
    }
    let mut accepted = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (key, index, status) = result.unwrap();
        assert!(status == StatusCode::OK || status == StatusCode::CONFLICT);
        if status == StatusCode::OK {
            accepted.push((key, index));
        }
    }
    assert!(!accepted.is_empty());
    let after = read(&client, &server.base_url, &id).await;
    for (key, index) in accepted {
        assert_eq!(after["context"]["data"][key], index);
    }
    assert_eq!(after["context"]["data"]["keep"], true);
}

#[tokio::test]
async fn oversized_merge_leaves_context_unchanged() {
    let server = orch8_api::test_harness::spawn_test_server_with_context_limit(4096).await;
    let client = reqwest::Client::new();
    let id = create(&client, &server.base_url, json!({"keep":true})).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"large":"x".repeat(5000)}}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        read(&client, &server.base_url, &id).await["context"]["data"],
        json!({"keep":true})
    );
}

#[tokio::test]
async fn data_patch_removes_only_requested_keys_and_patch_values_win() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let id = create(
        &client,
        &server.base_url,
        json!({"removed":1,"keep":2,"both":3}),
    )
    .await;
    let before = read(&client, &server.base_url, &id).await;
    let response = client
        .patch(format!("{}/instances/{id}/context/data", server.base_url))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"patch":{"both":4},"remove_keys":["removed","both","absent"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let after = read(&client, &server.base_url, &id).await;
    assert_eq!(after["context"]["data"], json!({"keep":2,"both":4}));
    for section in ["config", "runtime", "audit"] {
        assert_eq!(after["context"][section], before["context"][section]);
    }
}
