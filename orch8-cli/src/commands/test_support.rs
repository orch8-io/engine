use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use serde_json::Value;
use tokio::net::TcpListener;

#[derive(Clone, Debug)]
pub(crate) struct RecordedRequest {
    pub method: Method,
    pub uri: String,
    pub body: Value,
}

#[derive(Clone, Default)]
pub(crate) struct RequestLog(Arc<Mutex<Vec<RecordedRequest>>>);

impl RequestLog {
    pub fn snapshot(&self) -> Vec<RecordedRequest> {
        self.0.lock().unwrap().clone()
    }
}

type MockResponse = (StatusCode, String);

#[derive(Clone, Default)]
struct MockState {
    log: RequestLog,
    responses: Arc<Mutex<VecDeque<MockResponse>>>,
}

pub(crate) struct MockApi {
    pub base: String,
    pub log: RequestLog,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for MockApi {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn record_request(State(state): State<MockState>, request: Request) -> Response {
    let method = request.method().clone();
    let uri = request.uri().to_string();
    let bytes = to_bytes(request.into_body(), 1024 * 1024).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    state
        .log
        .0
        .lock()
        .unwrap()
        .push(RecordedRequest { method, uri, body });
    let (status, response_body) = state
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .unwrap_or((StatusCode::OK, "[]".into()));
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Body::from(response_body))
        .unwrap()
}

pub(crate) async fn mock_api() -> MockApi {
    mock_api_with_responses(Vec::new()).await
}

pub(crate) async fn mock_api_with_responses(responses: Vec<MockResponse>) -> MockApi {
    let log = RequestLog::default();
    let state = MockState {
        log: log.clone(),
        responses: Arc::new(Mutex::new(responses.into())),
    };
    let app = Router::new().fallback(record_request).with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    MockApi {
        base: format!("http://{address}"),
        log,
        task,
    }
}
