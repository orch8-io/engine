//! E2E tests for the background jobs API (`/jobs`).

use orch8_api::test_harness::spawn_test_server;
use orch8_storage::{InstanceStore, SequenceStore, SignalStore};
use orch8_types::ids::{InstanceId, Namespace, TenantId};
use orch8_types::instance::InstanceState;
use reqwest::StatusCode;
use serde_json::{Value, json};

async fn post_job(
    client: &reqwest::Client,
    base: &str,
    tenant: &str,
    body: Value,
) -> (StatusCode, Value) {
    let resp = client
        .post(format!("{base}/jobs"))
        .header("X-Tenant-Id", tenant)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = resp.status();
    let body = resp.json().await.unwrap_or(Value::Null);
    (status, body)
}

#[tokio::test]
async fn enqueue_returns_contract_shape_and_get_round_trips() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();

    let (status, job) = post_job(
        &client,
        &base,
        "t1",
        json!({
            "handler": "send_email",
            "payload": {"to": "a@example.com"},
            "priority": "high",
            "metadata": {"source": "signup"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{job}");
    for key in [
        "id",
        "instance_id",
        "handler",
        "status",
        "created_at",
        "run_at",
    ] {
        assert!(job.get(key).is_some(), "missing {key} in {job}");
    }
    assert_eq!(job["id"], job["instance_id"]);
    assert_eq!(job["handler"], "send_email");
    assert_eq!(job["status"], "scheduled");

    let id = job["id"].as_str().unwrap().to_string();
    let got: Value = client
        .get(format!("{base}/jobs/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(got["status"], "scheduled");
    assert_eq!(got["attempts"], 0);

    // The backing instance is a normal instance with the payload as data.
    let inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(id.parse().unwrap()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(inst.context.data, json!({"to": "a@example.com"}));
    assert_eq!(inst.metadata["source"], "signup");
    assert_eq!(inst.metadata["_job"]["handler"], "send_email");
    assert_eq!(inst.priority, orch8_types::instance::Priority::High);
    let seq = srv
        .storage
        .get_sequence_by_name(
            &TenantId::unchecked("t1"),
            &Namespace::new("default"),
            "_job.send_email",
            None,
        )
        .await
        .unwrap()
        .expect("system sequence auto-created");
    assert_eq!(seq.id, inst.sequence_id);

    // Also reachable through the instance API.
    let resp = client
        .get(format!("{base}/instances/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn idempotency_key_replays_with_200_and_same_id() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let body = json!({"handler": "h", "payload": {}, "idempotency_key": "order-1"});
    let (s1, j1) = post_job(&client, &base, "t1", body.clone()).await;
    let (s2, j2) = post_job(&client, &base, "t1", body.clone()).await;
    assert_eq!(s1, StatusCode::CREATED);
    assert_eq!(s2, StatusCode::OK);
    assert_eq!(j1["id"], j2["id"]);
    // Same key in another tenant is independent.
    let (s3, j3) = post_job(&client, &base, "t2", body).await;
    assert_eq!(s3, StatusCode::CREATED);
    assert_ne!(j1["id"], j3["id"]);
}

#[tokio::test]
async fn delay_and_run_at_set_the_schedule() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (status, job) = post_job(
        &client,
        &base,
        "t1",
        json!({"handler": "h", "delay_ms": 3_600_000}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let run_at: chrono::DateTime<chrono::Utc> = job["run_at"].as_str().unwrap().parse().unwrap();
    assert!(run_at > chrono::Utc::now() + chrono::Duration::minutes(59));

    let (status, job) = post_job(
        &client,
        &base,
        "t1",
        json!({"handler": "h", "run_at": "2031-05-01T12:00:00Z"}),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(
            job["id"].as_str().unwrap().parse().unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inst.next_fire_at.unwrap().to_rfc3339(),
        "2031-05-01T12:00:00+00:00"
    );
}

#[tokio::test]
async fn invalid_requests_are_rejected() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    for body in [
        json!({"handler": ""}),
        json!({"handler": "h", "payload": "not-an-object"}),
        json!({"handler": "h", "delay_ms": 1, "run_at": "2031-01-01T00:00:00Z"}),
        json!({"handler": "h", "retry": {"max_attempts": 0, "initial_backoff_ms": 10}}),
    ] {
        let (status, _) = post_job(&client, &base, "t1", body.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    }
    // Unknown priority / unknown field fail JSON deserialization (4xx).
    for body in [
        json!({"handler": "h", "priority": "urgent"}),
        json!({"handler": "h", "bogus": true}),
    ] {
        let (status, _) = post_job(&client, &base, "t1", body.clone()).await;
        assert!(status.is_client_error(), "{body} -> {status}");
    }
}

#[tokio::test]
async fn tenant_isolation_on_get_list_and_cancel() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, job) = post_job(&client, &base, "t1", json!({"handler": "h"})).await;
    let id = job["id"].as_str().unwrap();

    let resp = client
        .get(format!("{base}/jobs/{id}"))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let resp = client
        .delete(format!("{base}/jobs/{id}"))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    let page: Value = client
        .get(format!("{base}/jobs"))
        .header("X-Tenant-Id", "t2")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 0);

    // Body tenant conflicting with the header is forbidden.
    let (status, _) = post_job(
        &client,
        &base,
        "t1",
        json!({"handler": "h", "tenant_id": "t2"}),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn non_job_instances_are_not_jobs() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    // Create a regular sequence + instance through the instance API.
    let (_, job) = post_job(&client, &base, "t1", json!({"handler": "h"})).await;
    let job_inst = srv
        .storage
        .get_instance(InstanceId::from_uuid(
            job["id"].as_str().unwrap().parse().unwrap(),
        ))
        .await
        .unwrap()
        .unwrap();
    let resp: Value = client
        .post(format!("{base}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": job_inst.sequence_id,
            "tenant_id": "t1",
            "namespace": "default"
        }))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let plain_id = resp["id"].as_str().unwrap();
    let resp = client
        .get(format!("{base}/jobs/{plain_id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let page: Value = client
        .get(format!("{base}/jobs"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn list_filters_and_keyset_pagination() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let mut ids = Vec::new();
    for i in 0..5 {
        let (_, j) = post_job(
            &client,
            &base,
            "t1",
            json!({"handler": "a", "payload": {"i": i}}),
        )
        .await;
        ids.push(j["id"].as_str().unwrap().to_string());
    }
    post_job(&client, &base, "t1", json!({"handler": "b"})).await;

    // Page through handler=a two at a time, newest first.
    let mut seen = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..5 {
        let mut url = format!("{base}/jobs?handler=a&limit=2");
        if let Some(c) = &cursor {
            url = format!("{url}&cursor={c}");
        }
        let page: Value = client
            .get(url)
            .header("X-Tenant-Id", "t1")
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        for item in page["items"].as_array().unwrap() {
            assert_eq!(item["handler"], "a");
            seen.push(item["id"].as_str().unwrap().to_string());
        }
        if page["has_more"] == true {
            cursor = Some(page["next_cursor"].as_str().unwrap().to_string());
        } else {
            assert!(page.get("next_cursor").is_none());
            break;
        }
    }
    let mut expected = ids.clone();
    expected.reverse();
    assert_eq!(
        seen, expected,
        "keyset pages must be newest-first without gaps"
    );

    // Status filter: cancel one, then filter.
    let resp = client
        .delete(format!("{base}/jobs/{}", ids[0]))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let cancelled: Value = resp.json().await.unwrap();
    assert_eq!(cancelled["status"], "cancelled");

    let page: Value = client
        .get(format!("{base}/jobs?status=cancelled"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], ids[0].as_str());

    let resp = client
        .get(format!("{base}/jobs?status=exploded"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let resp = client
        .get(format!("{base}/jobs?cursor=nope"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn failed_vs_dead_lettered_status_filter() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, plain) = post_job(&client, &base, "t1", json!({"handler": "h"})).await;
    let (_, retried) = post_job(
        &client,
        &base,
        "t1",
        json!({"handler": "h", "retry": {"max_attempts": 3, "initial_backoff_ms": 100}}),
    )
    .await;
    // Simulate both reaching the DLQ.
    for j in [&plain, &retried] {
        let id = InstanceId::from_uuid(j["id"].as_str().unwrap().parse().unwrap());
        srv.storage
            .update_instance_state(id, InstanceState::Failed, None)
            .await
            .unwrap();
    }
    let get = |status: &'static str| {
        let client = client.clone();
        let base = base.clone();
        async move {
            let page: Value = client
                .get(format!("{base}/jobs?status={status}"))
                .header("X-Tenant-Id", "t1")
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            page["items"].as_array().unwrap().clone()
        }
    };
    let failed = get("failed").await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["id"], plain["id"]);
    assert_eq!(failed[0]["status"], "failed");
    let dlq = get("dead_lettered").await;
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0]["id"], retried["id"]);
    assert_eq!(dlq[0]["status"], "dead_lettered");
}

#[tokio::test]
async fn cancel_running_job_sends_signal_and_terminal_job_conflicts() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let base = srv.v1_url();
    let (_, job) = post_job(&client, &base, "t1", json!({"handler": "h"})).await;
    let id = job["id"].as_str().unwrap();
    let iid = InstanceId::from_uuid(id.parse().unwrap());
    srv.storage
        .update_instance_state(iid, InstanceState::Running, None)
        .await
        .unwrap();

    let resp = client
        .delete(format!("{base}/jobs/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::ACCEPTED);
    let pending = srv.storage.get_pending_signals(iid).await.unwrap();
    assert!(
        pending
            .iter()
            .any(|s| s.signal_type == orch8_types::signal::SignalType::Cancel)
    );

    srv.storage
        .update_instance_state(iid, InstanceState::Completed, None)
        .await
        .unwrap();
    let resp = client
        .delete(format!("{base}/jobs/{id}"))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn payload_over_context_limit_is_rejected() {
    let srv = orch8_api::test_harness::spawn_test_server_with_context_limit(64).await;
    let client = reqwest::Client::new();
    let (status, _) = post_job(
        &client,
        &srv.v1_url(),
        "t1",
        json!({"handler": "h", "payload": {"blob": "x".repeat(500)}}),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
}
