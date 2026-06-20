use orch8_api::test_harness::spawn_test_server;
use orch8_storage::AdminStore;

#[tokio::test]
async fn rollback_policy_crud() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create a policy
    let create_resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&serde_json::json!({
            "sequence_name": "test-seq",
            "error_rate_threshold": 0.1,
            "time_window_secs": 600
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 201);
    let body: serde_json::Value = create_resp.json().await.unwrap();
    assert_eq!(body["sequence_name"], "test-seq");
    assert_eq!(body["error_rate_threshold"], 0.1);
    assert_eq!(body["time_window_secs"], 600);

    // Get the policy
    let get_resp = client
        .get(format!("{}/rollback-policies/test-seq", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), 200);
    let body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(body["sequence_name"], "test-seq");

    // List policies
    let list_resp = client
        .get(format!("{}/rollback-policies", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(list_resp.status(), 200);
    let body: serde_json::Value = list_resp.json().await.unwrap();
    let arr = body.as_array().unwrap();
    assert_eq!(arr.len(), 1);

    // Delete the policy
    let del_resp = client
        .delete(format!("{}/rollback-policies/test-seq", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(del_resp.status(), 204);

    // Verify deletion
    let get_resp = client
        .get(format!("{}/rollback-policies/test-seq", server.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.status(), 404);
}

#[tokio::test]
async fn rollback_policy_tenant_isolation() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();

    // Create policy for tenant-a
    let resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&serde_json::json!({
            "tenant_id": "tenant-a",
            "sequence_name": "seq-1",
            "error_rate_threshold": 0.1,
            "time_window_secs": 300
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Create policy for tenant-b with same sequence name
    let resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&serde_json::json!({
            "tenant_id": "tenant-b",
            "sequence_name": "seq-1",
            "error_rate_threshold": 0.2,
            "time_window_secs": 600
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // List for tenant-a
    let resp = client
        .get(format!(
            "{}/rollback-policies?tenant_id=tenant-a",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body.as_array().unwrap().len(), 1);

    // tenant-b should not see tenant-a's policy when fetching specific seq
    let resp = client
        .get(format!(
            "{}/rollback-policies/seq-1?tenant_id=tenant-b",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["error_rate_threshold"], 0.2);

    // Delete tenant-a's policy
    let resp = client
        .delete(format!(
            "{}/rollback-policies/seq-1?tenant_id=tenant-a",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // tenant-b's policy should still exist
    let resp = client
        .get(format!(
            "{}/rollback-policies/seq-1?tenant_id=tenant-b",
            server.base_url
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
}

#[tokio::test]
async fn auto_rollback_triggered_on_error_threshold() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq = "error-prone-seq";

    // Create a very sensitive rollback policy (0% threshold, 1-hour window)
    let resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&serde_json::json!({
            "sequence_name": seq,
            "error_rate_threshold": 0.0,
            "time_window_secs": 3600
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Ingest a telemetry event for this sequence (to establish a denominator)
    let resp = client
        .post(format!("{}/telemetry/mobile", server.base_url))
        .json(&serde_json::json!({
            "events": [
                {
                    "event_type": "InstanceCompleted",
                    "payload": serde_json::json!({"sequence_name": seq}).to_string(),
                    "timestamp": chrono::Utc::now().to_rfc3339(),
                    "device": {
                        "device_id": "d1",
                        "os_name": "iOS",
                        "os_version": "17",
                        "app_version": "1.0",
                        "sdk_version": "0.1"
                    }
                }
            ]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Ingest an error for this sequence — this should trigger rollback
    // because error_rate > 0.0 threshold
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&serde_json::json!({
            "error_type": "RuntimeError",
            "message": "step failed",
            "device": {
                "device_id": "d1",
                "os_name": "iOS",
                "os_version": "17",
                "app_version": "1.0",
                "sdk_version": "0.1"
            },
            "sequence_name": seq
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Verify rollback history was recorded
    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert_eq!(
        history.len(),
        1,
        "rollback history should contain exactly one entry"
    );
    let entry = &history[0];
    assert_eq!(entry.tenant_id, "default");
    assert_eq!(entry.sequence_name, seq);
    assert!(entry.error_rate > 0.0, "error rate should be positive");
    assert!((entry.threshold - 0.0).abs() < f64::EPSILON);
    assert_eq!(entry.reason, "error_rate_threshold_breach");
}

#[tokio::test]
async fn auto_rollback_not_triggered_when_under_threshold() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();
    let seq = "healthy-seq";

    // Create a policy with 50% threshold
    let resp = client
        .post(format!("{}/rollback-policies", server.base_url))
        .json(&serde_json::json!({
            "sequence_name": seq,
            "error_rate_threshold": 0.5,
            "time_window_secs": 3600
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    // Ingest 1 error and 2 success events → error rate = 1/3 ≈ 33% < 50%
    for _ in 0..2 {
        let resp = client
            .post(format!("{}/telemetry/mobile", server.base_url))
            .json(&serde_json::json!({
                "events": [
                    {
                        "event_type": "InstanceCompleted",
                        "payload": serde_json::json!({"sequence_name": seq}).to_string(),
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                        "device": {
                            "device_id": "d1",
                            "os_name": "iOS",
                            "os_version": "17",
                            "app_version": "1.0",
                            "sdk_version": "0.1"
                        }
                    }
                ]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 202);
    }

    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&serde_json::json!({
            "error_type": "RuntimeError",
            "message": "step failed",
            "device": {
                "device_id": "d1",
                "os_name": "iOS",
                "os_version": "17",
                "app_version": "1.0",
                "sdk_version": "0.1"
            },
            "sequence_name": seq
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    // Verify no rollback history was recorded
    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert!(history.is_empty(), "rollback history should be empty");
}

#[tokio::test]
async fn auto_rollback_not_triggered_without_policy() {
    let server = spawn_test_server().await;
    let client = reqwest::Client::new();

    // No policy created — ingest error for unprotected sequence
    let resp = client
        .post(format!("{}/telemetry/mobile/errors", server.base_url))
        .json(&serde_json::json!({
            "error_type": "RuntimeError",
            "message": "step failed",
            "device": {
                "device_id": "d1",
                "os_name": "iOS",
                "os_version": "17",
                "app_version": "1.0",
                "sdk_version": "0.1"
            },
            "sequence_name": "unprotected-seq"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 202);

    let history = server
        .storage
        .list_rollback_history(None, None, 100)
        .await
        .unwrap();
    assert!(history.is_empty());
}
