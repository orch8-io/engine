//! Protocol-level tests for the optional Activepieces sidecar.

use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::Response;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::ExecutionContext;
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use tokio::sync::Mutex;

use super::*;

#[derive(Clone, Default)]
struct RequestCapture(Arc<Mutex<Option<Value>>>);

struct MockSidecar {
    url: String,
    capture: RequestCapture,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockSidecar {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn capture_request(
    State((capture, status, response_body)): State<(RequestCapture, StatusCode, String)>,
    request: Request,
) -> Response {
    let bytes = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    *capture.0.lock().await = Some(serde_json::from_slice(&bytes).unwrap());
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(response_body))
        .unwrap()
}

async fn spawn_sidecar(status: StatusCode, response_body: &str) -> MockSidecar {
    let capture = RequestCapture::default();
    let app = Router::new().fallback(capture_request).with_state((
        capture.clone(),
        status,
        response_body.to_string(),
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    MockSidecar {
        url: format!("http://{address}/execute"),
        capture,
        task,
    }
}

async fn context(params: Value, dry_run: bool) -> StepContext {
    let mut execution_context = ExecutionContext::default();
    execution_context.runtime.dry_run = dry_run;
    StepContext {
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("tenant-a"),
        block_id: BlockId::new("send-message"),
        params,
        context: Arc::new(execution_context),
        attempt: 3,
        storage: Arc::new(SqliteStorage::in_memory().await.unwrap()),
        wait_for_input: None,
    }
}

#[tokio::test]
async fn success_envelope_returns_output_and_sends_only_the_sidecar_contract() {
    let sidecar = spawn_sidecar(
        StatusCode::OK,
        r#"{"ok":true,"output":{"message_id":"m-1"}}"#,
    )
    .await;
    let ctx = context(
        json!({
            "auth": {"token": "secret"},
            "props": {"channel": "alerts"},
            "_internal": "must-not-leak"
        }),
        false,
    )
    .await;
    let instance_id = ctx.instance_id.to_string();

    let output = handle_ap_at(ctx, "ap://slack.send_message", &sidecar.url)
        .await
        .unwrap();

    assert_eq!(output, json!({"message_id": "m-1"}));
    let request = sidecar.capture.0.lock().await.clone().unwrap();
    assert_eq!(request["piece"], "slack");
    assert_eq!(request["action"], "send_message");
    assert_eq!(request["auth"]["token"], "secret");
    assert_eq!(request["props"]["channel"], "alerts");
    assert_eq!(request["instance_id"], instance_id);
    assert_eq!(request["attempt"], 3);
    assert!(request.get("_internal").is_none());
}

#[tokio::test]
async fn structured_permanent_error_preserves_message_and_details() {
    let sidecar = spawn_sidecar(
        StatusCode::OK,
        r#"{"ok":false,"error":{"type":"permanent","message":"bad auth","details":{"field":"token"}}}"#,
    )
    .await;

    let result = handle_ap_at(
        context(json!({}), false).await,
        "ap://slack.send_message",
        &sidecar.url,
    )
    .await;

    assert!(matches!(
        result,
        Err(StepError::Permanent { message, details: Some(details) })
            if message == "activepieces: bad auth" && details["field"] == "token"
    ));
}

#[tokio::test]
async fn malformed_server_error_is_retryable_with_response_evidence() {
    let sidecar = spawn_sidecar(StatusCode::BAD_GATEWAY, "upstream unavailable").await;

    let result = handle_ap_at(
        context(json!({}), false).await,
        "ap://slack.send_message",
        &sidecar.url,
    )
    .await;

    assert!(matches!(
        result,
        Err(StepError::Retryable { details: Some(details), .. })
            if details["status"] == 502 && details["body"] == "upstream unavailable"
    ));
}

#[tokio::test]
async fn malformed_client_error_is_permanent() {
    let sidecar = spawn_sidecar(StatusCode::BAD_REQUEST, "invalid props").await;

    let result = handle_ap_at(
        context(json!({}), false).await,
        "ap://slack.send_message",
        &sidecar.url,
    )
    .await;

    assert!(matches!(result, Err(StepError::Permanent { .. })));
}

#[tokio::test]
async fn successful_non_envelope_body_is_preserved_as_raw_output() {
    let sidecar = spawn_sidecar(StatusCode::OK, "plain result").await;

    let output = handle_ap_at(
        context(json!({}), false).await,
        "ap://slack.send_message",
        &sidecar.url,
    )
    .await
    .unwrap();

    assert_eq!(output, json!({"raw": "plain result"}));
}

#[tokio::test]
async fn dry_run_validates_handler_without_contacting_the_sidecar() {
    let output = handle_ap_at(
        context(json!({}), true).await,
        "ap://slack.send_message",
        "http://127.0.0.1:1/execute",
    )
    .await
    .unwrap();

    assert_eq!(output["dry_run"], true);
    assert_eq!(output["would"]["piece"], "slack");
    assert_eq!(output["would"]["action"], "send_message");
}
