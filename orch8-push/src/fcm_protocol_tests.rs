//! Protocol-level FCM tests with an in-process OAuth and message endpoint.

use std::collections::VecDeque;
use std::sync::{Arc, LazyLock};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use rsa::pkcs8::{EncodePrivateKey, LineEnding};
use tokio::sync::Mutex;

use super::*;

#[derive(Clone, Debug)]
struct Delivery {
    authorization: String,
    body: serde_json::Value,
}

type ScriptedResponse = (StatusCode, &'static str);

static TEST_RSA_KEY: LazyLock<String> = LazyLock::new(|| {
    rsa::RsaPrivateKey::new(&mut rand_08::rngs::OsRng, 2048)
        .unwrap()
        .to_pkcs8_pem(LineEnding::LF)
        .unwrap()
        .to_string()
});

#[derive(Clone, Default)]
struct MockFcmState {
    token_bodies: Arc<Mutex<Vec<String>>>,
    token_responses: Arc<Mutex<VecDeque<ScriptedResponse>>>,
    deliveries: Arc<Mutex<Vec<Delivery>>>,
    responses: Arc<Mutex<VecDeque<ScriptedResponse>>>,
}

struct MockFcm {
    endpoint: String,
    state: MockFcmState,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockFcm {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle_request(State(state): State<MockFcmState>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();
    let bytes = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    if path == "/token" {
        let request_number = {
            let mut token_bodies = state.token_bodies.lock().await;
            token_bodies.push(String::from_utf8(bytes.to_vec()).unwrap());
            token_bodies.len()
        };
        if let Some((status, body)) = state.token_responses.lock().await.pop_front() {
            return json_response(status, body);
        }
        let token = format!("access-token-{request_number}");
        return json_response(
            StatusCode::OK,
            &serde_json::json!({"access_token": token, "expires_in": 3600}).to_string(),
        );
    }

    state.deliveries.lock().await.push(Delivery {
        authorization: header_value(&headers, "authorization"),
        body: serde_json::from_slice(&bytes).unwrap(),
    });
    let (status, body) = state
        .responses
        .lock()
        .await
        .pop_front()
        .expect("every message request has a scripted response");
    json_response(status, body)
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

fn json_response(status: StatusCode, body: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap()
}

async fn spawn_mock_fcm(responses: Vec<ScriptedResponse>) -> MockFcm {
    spawn_mock_fcm_with_tokens(Vec::new(), responses).await
}

async fn spawn_mock_fcm_with_tokens(
    token_responses: Vec<ScriptedResponse>,
    responses: Vec<ScriptedResponse>,
) -> MockFcm {
    let state = MockFcmState {
        token_responses: Arc::new(Mutex::new(token_responses.into())),
        responses: Arc::new(Mutex::new(responses.into())),
        ..MockFcmState::default()
    };
    let app = Router::new()
        .fallback(handle_request)
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    MockFcm {
        endpoint,
        state,
        task,
    }
}

fn provider(endpoint: &str) -> FcmProvider {
    FcmProvider {
        client: reqwest::Client::new(),
        project_id: "test-project".into(),
        message_base_url: endpoint.into(),
        service_account: ServiceAccount {
            client_email: "fcm@test-project.iam.gserviceaccount.com".into(),
            private_key: TEST_RSA_KEY.clone(),
            token_uri: format!("{endpoint}/token"),
        },
        cached_token: Mutex::new(None),
    }
}

fn signed_metadata() -> SignedWakeMetadata {
    let now = chrono::Utc::now();
    SignedWakeMetadata::sign(
        "tenant-a",
        "device-a",
        "command-a",
        "wake-key-1",
        &ed25519_dalek::SigningKey::from_bytes(&[7_u8; 32]),
        now,
        now + chrono::Duration::minutes(5),
    )
    .unwrap()
}

#[tokio::test]
async fn successful_delivery_exchanges_a_jwt_and_sends_the_data_payload() {
    let mock = spawn_mock_fcm(vec![
        (StatusCode::OK, r#"{"name":"message-1"}"#),
        (StatusCode::OK, r#"{"name":"message-2"}"#),
    ])
    .await;
    let provider = provider(&mock.endpoint);

    provider
        .send_silent_push("device-token", "android")
        .await
        .unwrap();
    provider
        .send_silent_push("second-device", "android")
        .await
        .unwrap();

    let token_bodies = mock.state.token_bodies.lock().await;
    assert_eq!(token_bodies.len(), 1);
    assert!(token_bodies[0].contains("grant_type="));
    assert!(token_bodies[0].contains("assertion="));
    let deliveries = mock.state.deliveries.lock().await;
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].authorization, "Bearer access-token-1");
    assert_eq!(deliveries[1].authorization, "Bearer access-token-1");
    assert_eq!(deliveries[0].body["message"]["token"], "device-token");
    assert_eq!(deliveries[0].body["message"]["data"]["type"], "sync");
}

#[tokio::test]
async fn rejected_access_token_is_refreshed_before_retrying_delivery() {
    let mock = spawn_mock_fcm(vec![
        (StatusCode::UNAUTHORIZED, r#"{"error":"expired"}"#),
        (StatusCode::OK, r#"{"name":"message-2"}"#),
    ])
    .await;

    provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await
        .unwrap();

    assert_eq!(mock.state.token_bodies.lock().await.len(), 2);
    let deliveries = mock.state.deliveries.lock().await;
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].authorization, "Bearer access-token-1");
    assert_eq!(deliveries[1].authorization, "Bearer access-token-2");
}

#[tokio::test]
async fn unregistered_device_is_returned_without_retrying() {
    let body = r#"{"error":{"details":[{"errorCode":"UNREGISTERED"}]}}"#;
    let mock = spawn_mock_fcm(vec![(StatusCode::NOT_FOUND, body)]).await;

    let result = provider(&mock.endpoint)
        .send_silent_push("dead-token", "android")
        .await;

    assert!(matches!(result, Err(PushError::InvalidToken)));
    assert_eq!(mock.state.deliveries.lock().await.len(), 1);
}

#[tokio::test]
async fn transient_server_failure_reuses_the_cached_token_and_retries() {
    let mock = spawn_mock_fcm(vec![
        (StatusCode::SERVICE_UNAVAILABLE, r#"{"error":"busy"}"#),
        (StatusCode::OK, r#"{"name":"message-3"}"#),
    ])
    .await;

    provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await
        .unwrap();

    assert_eq!(mock.state.token_bodies.lock().await.len(), 1);
    let deliveries = mock.state.deliveries.lock().await;
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].authorization, deliveries[1].authorization);
}

#[tokio::test]
async fn token_endpoint_failures_are_retried_then_reported() {
    let mock = spawn_mock_fcm_with_tokens(
        vec![
            (StatusCode::SERVICE_UNAVAILABLE, "oauth busy"),
            (StatusCode::BAD_GATEWAY, "oauth unavailable"),
            (StatusCode::INTERNAL_SERVER_ERROR, "oauth failed"),
        ],
        Vec::new(),
    )
    .await;

    let result = provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await;

    assert!(matches!(
        result,
        Err(PushError::Retryable(message))
            if message.contains("after 3 attempts") && message.contains("oauth failed")
    ));
    assert_eq!(mock.state.token_bodies.lock().await.len(), 3);
    assert!(mock.state.deliveries.lock().await.is_empty());
}

#[tokio::test]
async fn malformed_token_responses_are_retried_then_reported() {
    let mock = spawn_mock_fcm_with_tokens(
        vec![
            (StatusCode::OK, "not-json"),
            (StatusCode::OK, "still-not-json"),
            (StatusCode::OK, "invalid-again"),
        ],
        Vec::new(),
    )
    .await;

    let result = provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await;

    assert!(matches!(
        result,
        Err(PushError::Retryable(message)) if message.contains("FCM token parse failed")
    ));
    assert_eq!(mock.state.token_bodies.lock().await.len(), 3);
}

#[tokio::test]
async fn token_response_without_expiry_is_cached_for_later_delivery() {
    let mock = spawn_mock_fcm_with_tokens(
        vec![(StatusCode::OK, r#"{"access_token":"no-expiry"}"#)],
        vec![
            (StatusCode::OK, r#"{"name":"message-1"}"#),
            (StatusCode::OK, r#"{"name":"message-2"}"#),
        ],
    )
    .await;
    let provider = provider(&mock.endpoint);

    provider
        .send_silent_push("device-one", "android")
        .await
        .unwrap();
    provider
        .send_silent_push("device-two", "android")
        .await
        .unwrap();

    assert_eq!(mock.state.token_bodies.lock().await.len(), 1);
}

#[tokio::test]
async fn permanent_message_error_preserves_a_bounded_vendor_body() {
    let mock = spawn_mock_fcm(vec![(StatusCode::BAD_REQUEST, "invalid payload")]).await;

    let result = provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await;

    assert!(matches!(
        result,
        Err(PushError::Permanent(message))
            if message.contains("400 Bad Request") && message.contains("invalid payload")
    ));
    assert_eq!(mock.state.deliveries.lock().await.len(), 1);
}

#[tokio::test]
async fn repeated_auth_rejection_exhausts_delivery_attempts() {
    let mock = spawn_mock_fcm(vec![
        (StatusCode::UNAUTHORIZED, "expired-1"),
        (StatusCode::UNAUTHORIZED, "expired-2"),
        (StatusCode::UNAUTHORIZED, "expired-3"),
    ])
    .await;

    let result = provider(&mock.endpoint)
        .send_silent_push("device-token", "android")
        .await;

    assert!(matches!(
        result,
        Err(PushError::Retryable(message))
            if message.contains("after 3 attempts") && message.contains("access token rejected")
    ));
    assert_eq!(mock.state.token_bodies.lock().await.len(), 3);
    assert_eq!(mock.state.deliveries.lock().await.len(), 3);
}

#[tokio::test]
async fn invalid_signing_key_fails_fast_without_network_io() {
    let mock = spawn_mock_fcm(Vec::new()).await;
    let mut provider = provider(&mock.endpoint);
    provider.service_account.private_key = "not-a-private-key".into();

    let result = provider.send_silent_push("device-token", "android").await;

    assert!(matches!(result, Err(PushError::Config(_))));
    assert!(mock.state.token_bodies.lock().await.is_empty());
    assert!(mock.state.deliveries.lock().await.is_empty());
}

#[tokio::test]
async fn signed_wake_uses_the_same_delivery_path_and_embeds_metadata() {
    let mock = spawn_mock_fcm(vec![(StatusCode::OK, r#"{"name":"signed"}"#)]).await;
    let metadata = signed_metadata();

    provider(&mock.endpoint)
        .send_signed_wake("device-token", "android", &metadata)
        .await
        .unwrap();

    let deliveries = mock.state.deliveries.lock().await;
    let encoded = deliveries[0].body["message"]["data"]["orch8"]
        .as_str()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<SignedWakeMetadata>(encoded).unwrap(),
        metadata
    );
}
