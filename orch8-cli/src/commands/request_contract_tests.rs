use std::io::Write;

use axum::http::{Method, StatusCode};
use reqwest::Client;
use serde_json::json;
use tempfile::NamedTempFile;
use uuid::Uuid;

use super::checkpoint::{self, CheckpointCmd};
use super::cron::{self, CronCmd};
use super::inspect_cmd::{self, InspectCmd};
use super::signal;
use super::test_support::{mock_api, mock_api_with_responses};
use crate::OutputFormat;

#[tokio::test]
async fn signal_sends_the_typed_json_payload() {
    let server = mock_api().await;
    let instance_id = Uuid::new_v4();

    signal::run(
        &Client::new(),
        &server.base,
        instance_id,
        "approval".into(),
        Some(r#"{"approved":true}"#.into()),
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].uri, format!("/instances/{instance_id}/signals"));
    assert_eq!(requests[0].body["signal_type"], "approval");
    assert_eq!(requests[0].body["payload"], json!({"approved": true}));
}

#[tokio::test]
async fn signal_rejects_invalid_payload_before_network_io() {
    let error = signal::run(
        &Client::new(),
        "http://127.0.0.1:1",
        Uuid::new_v4(),
        "approval".into(),
        Some("{not-json".into()),
        OutputFormat::Json,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("invalid JSON payload"));
}

#[tokio::test]
async fn checkpoint_commands_use_the_expected_contract() {
    let server = mock_api().await;
    let client = Client::new();
    let instance_id = Uuid::new_v4();

    checkpoint::run(
        &client,
        &server.base,
        CheckpointCmd::List { instance_id },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    checkpoint::run(
        &client,
        &server.base,
        CheckpointCmd::Latest { instance_id },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    checkpoint::run(
        &client,
        &server.base,
        CheckpointCmd::Prune {
            instance_id,
            keep: 7,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(
        requests[0].uri,
        format!("/instances/{instance_id}/checkpoints")
    );
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(
        requests[1].uri,
        format!("/instances/{instance_id}/checkpoints/latest")
    );
    assert_eq!(requests[2].method, Method::POST);
    assert_eq!(
        requests[2].uri,
        format!("/instances/{instance_id}/checkpoints/prune")
    );
    assert_eq!(requests[2].body, json!({"keep": 7}));
}

#[tokio::test]
async fn cron_commands_encode_filters_and_resource_paths() {
    let server = mock_api().await;
    let client = Client::new();
    let id = Uuid::new_v4();

    cron::run(
        &client,
        &server.base,
        CronCmd::List {
            tenant_id: Some("tenant a".into()),
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    cron::run(
        &client,
        &server.base,
        CronCmd::Get { id },
        OutputFormat::Json,
    )
    .await
    .unwrap();
    cron::run(
        &client,
        &server.base,
        CronCmd::Delete { id },
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    let list_url = reqwest::Url::parse(&format!("{}{}", server.base, requests[0].uri)).unwrap();
    assert_eq!(
        list_url.query_pairs().next().unwrap().1.as_ref(),
        "tenant a"
    );
    assert_eq!(requests[1].uri, format!("/cron/{id}"));
    assert_eq!(requests[2].method, Method::DELETE);
    assert_eq!(requests[2].uri, format!("/cron/{id}"));
}

#[tokio::test]
async fn cron_table_mode_renders_populated_empty_and_non_array_responses() {
    let populated = json!([{
        "id": Uuid::new_v4(),
        "tenant_id": "tenant-a",
        "cron_expr": "0 * * * *",
        "enabled": true,
        "next_fire_at": "2030-01-01T00:00:00Z"
    }]);
    let server = mock_api_with_responses(vec![
        (StatusCode::OK, populated.to_string()),
        (StatusCode::OK, "[]".into()),
        (StatusCode::OK, json!({"items": []}).to_string()),
    ])
    .await;
    let client = Client::new();

    cron::run(
        &client,
        &server.base,
        CronCmd::List { tenant_id: None },
        OutputFormat::Table,
    )
    .await
    .unwrap();
    cron::run(
        &client,
        &server.base,
        CronCmd::List { tenant_id: None },
        OutputFormat::Table,
    )
    .await
    .unwrap();
    cron::run(
        &client,
        &server.base,
        CronCmd::List { tenant_id: None },
        OutputFormat::Table,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].uri, "/cron");
    assert_eq!(requests[1].uri, "/cron");
    assert_eq!(requests[2].uri, "/cron");
}

#[tokio::test]
async fn cron_delete_surfaces_server_status_and_error_body() {
    let server = mock_api_with_responses(vec![(
        StatusCode::CONFLICT,
        json!({"error": "schedule is active"}).to_string(),
    )])
    .await;
    let id = Uuid::new_v4();

    let error = cron::run(
        &Client::new(),
        &server.base,
        CronCmd::Delete { id },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("409 Conflict"));
    assert!(error.to_string().contains("schedule is active"));
}

#[tokio::test]
async fn inspect_instance_encodes_the_historical_block_boundary() {
    let server = mock_api().await;
    let instance = Uuid::new_v4();

    inspect_cmd::run(
        &Client::new(),
        &server.base,
        InspectCmd::Template {
            block: "render".into(),
            instance: Some(instance),
            at_block: Some("step&before=unsafe".into()),
            sequence_file: None,
            context: None,
            outputs: None,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    let url = reqwest::Url::parse(&format!("{}{}", server.base, requests[0].uri)).unwrap();
    assert_eq!(
        url.path(),
        format!("/instances/{instance}/blocks/render/resolved-input")
    );
    assert_eq!(
        url.query_pairs().next().unwrap().1.as_ref(),
        "step&before=unsafe"
    );
}

#[tokio::test]
async fn inspect_draft_reads_sequence_and_fixture_files_into_the_request() {
    let server = mock_api().await;
    let mut sequence = NamedTempFile::new().unwrap();
    let mut context = NamedTempFile::new().unwrap();
    write!(sequence, r#"{{"name":"draft","blocks":[]}}"#).unwrap();
    write!(context, r#"{{"customer":"acme"}}"#).unwrap();

    inspect_cmd::run(
        &Client::new(),
        &server.base,
        InspectCmd::Template {
            block: "render".into(),
            instance: None,
            at_block: None,
            sequence_file: Some(sequence.path().to_path_buf()),
            context: Some(format!("@{}", context.path().display())),
            outputs: Some(r#"{"fetch":{"status":200}}"#.into()),
        },
        OutputFormat::Json,
    )
    .await
    .unwrap();

    let requests = server.log.snapshot();
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(requests[0].uri, "/sequences/inspect-template");
    assert_eq!(requests[0].body["block_id"], "render");
    assert_eq!(requests[0].body["context_data"]["customer"], "acme");
    assert_eq!(requests[0].body["outputs"]["fetch"]["status"], 200);
}

#[tokio::test]
async fn inspect_table_mode_renders_every_trace_evidence_shape() {
    let trace = json!({
        "block_id": "render",
        "entries": [
            {
                "status": "resolved",
                "param_path": "message",
                "expression": "steps.fetch.body",
                "value": {"ok": true},
                "result_type": "object",
                "source": "steps.fetch.body",
                "fallback_used": true,
                "coerced_to_string": true,
                "error": "fallback selected"
            },
            {
                "status": "resolved",
                "param_path": "plain",
                "expression": "data.name",
                "source": "data.name",
                "fallback_used": false,
                "coerced_to_string": false
            }
        ],
        "resolved_params": {"message": "rendered"}
    });
    let server = mock_api_with_responses(vec![(StatusCode::OK, trace.to_string())]).await;

    inspect_cmd::run(
        &Client::new(),
        &server.base,
        InspectCmd::Template {
            block: "render".into(),
            instance: Some(Uuid::new_v4()),
            at_block: None,
            sequence_file: None,
            context: None,
            outputs: None,
        },
        OutputFormat::Table,
    )
    .await
    .unwrap();

    assert_eq!(server.log.snapshot().len(), 1);
}

#[tokio::test]
async fn inspect_table_mode_handles_an_empty_trace() {
    let trace = json!({"block_id": "render", "entries": []});
    let server = mock_api_with_responses(vec![(StatusCode::OK, trace.to_string())]).await;

    inspect_cmd::run(
        &Client::new(),
        &server.base,
        InspectCmd::Template {
            block: "render".into(),
            instance: Some(Uuid::new_v4()),
            at_block: None,
            sequence_file: None,
            context: None,
            outputs: None,
        },
        OutputFormat::Table,
    )
    .await
    .unwrap();

    assert_eq!(server.log.snapshot().len(), 1);
}

#[tokio::test]
async fn inspect_rejects_server_errors_and_missing_input_sources() {
    let server = mock_api_with_responses(vec![(
        StatusCode::UNPROCESSABLE_ENTITY,
        json!({"error": "unknown block"}).to_string(),
    )])
    .await;
    let response_error = inspect_cmd::run(
        &Client::new(),
        &server.base,
        InspectCmd::Template {
            block: "missing".into(),
            instance: Some(Uuid::new_v4()),
            at_block: None,
            sequence_file: None,
            context: None,
            outputs: None,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(
        response_error
            .to_string()
            .contains("422 Unprocessable Entity")
    );

    let input_error = inspect_cmd::run(
        &Client::new(),
        "http://127.0.0.1:1",
        InspectCmd::Template {
            block: "render".into(),
            instance: None,
            at_block: None,
            sequence_file: None,
            context: None,
            outputs: None,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(input_error.to_string().contains("pass --instance"));
}

#[tokio::test]
async fn inspect_rejects_invalid_sequence_and_fixture_json_before_network_io() {
    let mut invalid_sequence = NamedTempFile::new().unwrap();
    write!(invalid_sequence, "{{not-json").unwrap();
    let sequence_error = inspect_cmd::run(
        &Client::new(),
        "http://127.0.0.1:1",
        InspectCmd::Template {
            block: "render".into(),
            instance: None,
            at_block: None,
            sequence_file: Some(invalid_sequence.path().to_path_buf()),
            context: None,
            outputs: None,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(sequence_error.to_string().contains("invalid JSON in"));

    let mut sequence = NamedTempFile::new().unwrap();
    write!(sequence, r#"{{"name":"draft","blocks":[]}}"#).unwrap();
    let fixture_error = inspect_cmd::run(
        &Client::new(),
        "http://127.0.0.1:1",
        InspectCmd::Template {
            block: "render".into(),
            instance: None,
            at_block: None,
            sequence_file: Some(sequence.path().to_path_buf()),
            context: Some("{not-json".into()),
            outputs: None,
        },
        OutputFormat::Json,
    )
    .await
    .unwrap_err();
    assert!(fixture_error.to_string().contains("invalid JSON argument"));
}
