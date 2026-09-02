use axum::http::Method;
use tempfile::NamedTempFile;

use super::*;
use crate::commands::test_support::mock_api;

#[tokio::test]
async fn create_sends_file_context_to_the_instances_endpoint() {
    use std::io::Write;

    let server = mock_api().await;
    let base = &server.base;
    let sequence_id = Uuid::new_v4();
    let mut context = NamedTempFile::new().unwrap();
    write!(context, "{{\"customer\":\"acme\",\"attempt\":2}}").unwrap();

    run(
        &Client::new(),
        base,
        InstanceCmd::Create {
            sequence_id,
            namespace: "billing".into(),
            context: Some(format!("@{}", context.path().display())),
        },
        OutputFormat::Json,
        Some("tenant-a"),
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].uri, "/instances");
    assert_eq!(requests[0].body["sequence_id"], sequence_id.to_string());
    assert_eq!(requests[0].body["tenant_id"], "tenant-a");
    assert_eq!(requests[0].body["namespace"], "billing");
    assert_eq!(requests[0].body["context"]["attempt"], 2);
}

#[tokio::test]
async fn list_encodes_every_filter_as_query_parameters() {
    let server = mock_api().await;
    let base = &server.base;
    let sequence_id = Uuid::new_v4();

    run(
        &Client::new(),
        base,
        InstanceCmd::List {
            tenant_id: Some("tenant a".into()),
            namespace: Some("billing/eu".into()),
            state: Some("running".into()),
            sequence_id: Some(sequence_id),
            limit: 17,
        },
        OutputFormat::Json,
        None,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests[0].method, Method::GET);
    let url = reqwest::Url::parse(&format!("{base}{}", requests[0].uri)).unwrap();
    let params: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
    assert_eq!(params.get("limit").map(String::as_str), Some("17"));
    assert_eq!(
        params.get("tenant_id").map(String::as_str),
        Some("tenant a")
    );
    assert_eq!(
        params.get("namespace").map(String::as_str),
        Some("billing/eu")
    );
    assert_eq!(params.get("state").map(String::as_str), Some("running"));
    assert_eq!(params.get("sequence_id"), Some(&sequence_id.to_string()));
}

#[tokio::test]
async fn state_mutations_use_the_expected_methods_paths_and_bodies() {
    let server = mock_api().await;
    let base = &server.base;
    let client = Client::new();
    let id = Uuid::new_v4();

    run(
        &client,
        base,
        InstanceCmd::SetState {
            id,
            state: "paused".into(),
        },
        OutputFormat::Json,
        None,
    )
    .await
    .unwrap();
    run(
        &client,
        base,
        InstanceCmd::Retry { id },
        OutputFormat::Json,
        None,
    )
    .await
    .unwrap();
    run(
        &client,
        base,
        InstanceCmd::BulkState {
            state: "cancelled".into(),
            tenant_id: Some("tenant-a".into()),
            namespace: None,
            states: Some("scheduled, running".into()),
        },
        OutputFormat::Json,
        None,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests[0].method, Method::PATCH);
    assert_eq!(requests[0].uri, format!("/instances/{id}/state"));
    assert_eq!(requests[0].body, serde_json::json!({"state": "paused"}));
    assert_eq!(requests[1].method, Method::POST);
    assert_eq!(requests[1].uri, format!("/instances/{id}/retry"));
    assert_eq!(requests[2].method, Method::PATCH);
    assert_eq!(requests[2].uri, "/instances/bulk/state");
    assert_eq!(requests[2].body["state"], "cancelled");
    assert_eq!(
        requests[2].body["filter"]["states"],
        serde_json::json!(["scheduled", "running"])
    );
}

#[tokio::test]
async fn invalid_inline_context_fails_before_any_request_is_sent() {
    let error = run(
        &Client::new(),
        "http://127.0.0.1:1",
        InstanceCmd::Create {
            sequence_id: Uuid::new_v4(),
            namespace: "default".into(),
            context: Some("{not-json".into()),
        },
        OutputFormat::Json,
        Some("tenant-a"),
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("invalid JSON context"));
}
