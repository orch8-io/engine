//! Conditional-write (compare-and-swap) contract shared by every CDN backend.
//!
//! Rule under test: `upload_if_match(path, bytes, expected)` writes only when
//! the object's current `ETag` equals `expected` (`None` = the object must not
//! exist) and otherwise returns `CdnError::Conflict` without changing the
//! object. The registry's head update relies on this, so the in-memory fake
//! must behave exactly like S3. Every contract runs against
//! `MemoryCdnBackend` and against `S3CdnBackend` talking to a stateful
//! in-process S3 that honours `If-Match` / `If-None-Match`. Outcomes are
//! observed only through the public trait (`get_etag`), never internal state.

use std::collections::HashMap;
use std::sync::Arc;

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::Response;
use axum::routing::any;
use tokio::sync::Mutex;

use super::*;

const PATH: &str = "tenant-a/registry/acme/index.json";

/// Minimal stateful S3: PUT (optionally conditional), HEAD and DELETE on
/// object keys, with the `ETag` of an object being the quoted SHA-256 of its
/// bytes. One mutex makes each request atomic, as S3 conditional writes are.
#[derive(Clone, Default)]
struct FakeS3State {
    objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
}

fn etag_of(bytes: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}

fn empty(status: StatusCode) -> Response {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = status;
    response
}

fn with_etag(status: StatusCode, etag: &str) -> Response {
    let mut response = empty(status);
    response
        .headers_mut()
        .insert("etag", HeaderValue::from_str(etag).unwrap());
    response
}

async fn fake_s3_handler(State(state): State<FakeS3State>, request: Request) -> Response {
    let key = request.uri().path().to_string();
    let method = request.method().clone();
    let if_match = request
        .headers()
        .get("if-match")
        .map(|value| value.to_str().unwrap().to_string());
    let if_none_match = request
        .headers()
        .get("if-none-match")
        .map(|value| value.to_str().unwrap().to_string());
    let body = to_bytes(request.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    let mut objects = state.objects.lock().await;
    let current = objects.get(&key).map(|bytes| etag_of(bytes));
    match method {
        Method::HEAD => current.map_or_else(
            || empty(StatusCode::NOT_FOUND),
            |etag| with_etag(StatusCode::OK, &etag),
        ),
        Method::DELETE => {
            objects.remove(&key);
            empty(StatusCode::NO_CONTENT)
        }
        Method::PUT => {
            let precondition_holds = match (&if_match, &if_none_match) {
                (Some(expected), _) => current.as_deref() == Some(expected.as_str()),
                (None, Some(star)) if star == "*" => current.is_none(),
                _ => true,
            };
            if !precondition_holds {
                return empty(StatusCode::PRECONDITION_FAILED);
            }
            let etag = etag_of(&body);
            objects.insert(key, body);
            with_etag(StatusCode::OK, &etag)
        }
        _ => empty(StatusCode::METHOD_NOT_ALLOWED),
    }
}

/// Aborts the fake S3 server when the test ends.
struct FakeS3Server(tokio::task::JoinHandle<()>);

impl Drop for FakeS3Server {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn spawn_fake_s3() -> (S3CdnBackend, FakeS3Server) {
    let app = Router::new()
        .fallback(any(fake_s3_handler))
        .with_state(FakeS3State::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let backend = S3CdnBackend::new(
        endpoint,
        "bucket".into(),
        "us-east-1".into(),
        "AKID".into(),
        "secret".into(),
    );
    (backend, FakeS3Server(task))
}

/// Run `check` against a fresh in-memory backend and a fresh fake-S3 backend.
macro_rules! for_each_backend {
    ($check:ident) => {{
        $check("memory", Arc::new(MemoryCdnBackend::new())).await;
        let (s3, _server) = spawn_fake_s3().await;
        $check("s3", Arc::new(s3)).await;
    }};
}

async fn put_unconditionally(backend: &dyn CdnBackend, path: &str, bytes: &[u8]) -> String {
    backend
        .upload(path, bytes.to_vec(), None, None)
        .await
        .unwrap();
    backend.get_etag(path).await.unwrap().unwrap()
}

async fn assert_conflict(
    backend: &dyn CdnBackend,
    case: &str,
    bytes: &[u8],
    expected: Option<&str>,
) {
    let result = backend
        .upload_if_match(PATH, bytes.to_vec(), None, None, expected)
        .await;
    assert!(
        matches!(result, Err(CdnError::Conflict)),
        "{case}: expected Conflict, got {result:?}"
    );
}

async fn check_create_when_absent(case: &str, backend: Arc<dyn CdnBackend>) {
    let etag = backend
        .upload_if_match(PATH, b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        Some(etag),
        "{case}: the returned ETag is the one later reads observe"
    );
}

#[tokio::test]
async fn cas_with_no_expected_etag_creates_an_absent_object() {
    for_each_backend!(check_create_when_absent);
}

async fn check_create_conflicts_when_present(case: &str, backend: Arc<dyn CdnBackend>) {
    let existing = put_unconditionally(backend.as_ref(), PATH, b"v1").await;
    assert_conflict(backend.as_ref(), case, b"v2", None).await;
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        Some(existing),
        "{case}: losing writer must not replace the object"
    );
}

#[tokio::test]
async fn cas_with_no_expected_etag_conflicts_when_the_object_exists() {
    for_each_backend!(check_create_conflicts_when_present);
}

async fn check_expected_etag_conflicts_when_absent(case: &str, backend: Arc<dyn CdnBackend>) {
    assert_conflict(backend.as_ref(), case, b"v1", Some("\"anything\"")).await;
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        None,
        "{case}: no object may be created"
    );
}

#[tokio::test]
async fn cas_with_an_expected_etag_conflicts_when_the_object_is_absent() {
    for_each_backend!(check_expected_etag_conflicts_when_absent);
}

async fn check_stale_etag_conflicts(case: &str, backend: Arc<dyn CdnBackend>) {
    let current = put_unconditionally(backend.as_ref(), PATH, b"v1").await;
    assert_conflict(backend.as_ref(), case, b"v2", Some(&etag_of(b"stale"))).await;
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        Some(current),
        "{case}: a conflict leaves the stored object untouched"
    );
}

#[tokio::test]
async fn cas_with_a_stale_etag_conflicts_and_leaves_the_object_untouched() {
    for_each_backend!(check_stale_etag_conflicts);
}

async fn check_matching_etag_swaps(case: &str, backend: Arc<dyn CdnBackend>) {
    let old = put_unconditionally(backend.as_ref(), PATH, b"v1").await;
    let new = backend
        .upload_if_match(PATH, b"v2".to_vec(), None, None, Some(&old))
        .await
        .unwrap();
    assert_ne!(new, old, "{case}");
    assert_eq!(backend.get_etag(PATH).await.unwrap(), Some(new), "{case}");
}

#[tokio::test]
async fn cas_with_the_current_etag_replaces_the_object() {
    for_each_backend!(check_matching_etag_swaps);
}

async fn check_retired_etag_cannot_be_replayed(case: &str, backend: Arc<dyn CdnBackend>) {
    let etag_a = backend
        .upload_if_match(PATH, b"a".to_vec(), None, None, None)
        .await
        .unwrap();
    let etag_b = backend
        .upload_if_match(PATH, b"b".to_vec(), None, None, Some(&etag_a))
        .await
        .unwrap();
    assert_conflict(backend.as_ref(), case, b"c", Some(&etag_a)).await;
    let etag_c = backend
        .upload_if_match(PATH, b"c".to_vec(), None, None, Some(&etag_b))
        .await
        .unwrap();
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        Some(etag_c),
        "{case}"
    );
}

#[tokio::test]
async fn cas_chain_advances_and_a_retired_etag_cannot_be_replayed() {
    for_each_backend!(check_retired_etag_cannot_be_replayed);
}

async fn check_preconditions_are_per_path(case: &str, backend: Arc<dyn CdnBackend>) {
    let other = put_unconditionally(backend.as_ref(), "a/index.json", b"a").await;
    backend
        .upload_if_match(PATH, b"b".to_vec(), None, None, None)
        .await
        .unwrap_or_else(|error| panic!("{case}: absent path must accept create: {error}"));
    assert_eq!(
        backend.get_etag("a/index.json").await.unwrap(),
        Some(other),
        "{case}: the other path is untouched"
    );
}

#[tokio::test]
async fn cas_preconditions_are_scoped_to_one_path() {
    for_each_backend!(check_preconditions_are_per_path);
}

async fn check_unconditional_write_invalidates_etag(case: &str, backend: Arc<dyn CdnBackend>) {
    let etag = backend
        .upload_if_match(PATH, b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    put_unconditionally(backend.as_ref(), PATH, b"v2").await;
    assert_conflict(backend.as_ref(), case, b"v3", Some(&etag)).await;
}

#[tokio::test]
async fn unconditional_upload_invalidates_a_previously_returned_etag() {
    for_each_backend!(check_unconditional_write_invalidates_etag);
}

async fn check_delete_resets_to_absent(case: &str, backend: Arc<dyn CdnBackend>) {
    let etag = backend
        .upload_if_match(PATH, b"v1".to_vec(), None, None, None)
        .await
        .unwrap();
    backend.delete(PATH).await.unwrap();
    assert_conflict(backend.as_ref(), case, b"v2", Some(&etag)).await;
    let recreated = backend
        .upload_if_match(PATH, b"v2".to_vec(), None, None, None)
        .await
        .unwrap();
    assert_eq!(
        backend.get_etag(PATH).await.unwrap(),
        Some(recreated),
        "{case}"
    );
}

#[tokio::test]
async fn deleted_object_rejects_its_old_etag_and_accepts_a_fresh_create() {
    for_each_backend!(check_delete_resets_to_absent);
}

async fn check_concurrent_creates_have_one_winner(case: &str, backend: Arc<dyn CdnBackend>) {
    let handles: Vec<_> = (0..8_u8)
        .map(|attempt| {
            let backend = Arc::clone(&backend);
            tokio::spawn(async move {
                backend
                    .upload_if_match(PATH, vec![attempt], None, None, None)
                    .await
            })
        })
        .collect();
    let mut winners = Vec::new();
    let mut conflicts = 0;
    for handle in handles {
        match handle.await.unwrap() {
            Ok(etag) => winners.push(etag),
            Err(CdnError::Conflict) => conflicts += 1,
            Err(other) => panic!("{case}: unexpected error: {other}"),
        }
    }
    assert_eq!((winners.len(), conflicts), (1, 7), "{case}");
    assert_eq!(
        backend.get_etag(PATH).await.unwrap().as_ref(),
        winners.first(),
        "{case}: the stored object is the winner's"
    );
}

#[tokio::test]
async fn concurrent_creates_admit_exactly_one_winner() {
    for_each_backend!(check_concurrent_creates_have_one_winner);
}

/// Backend that only implements the required methods and inherits the
/// default `upload_if_match`.
struct UnconditionalOnlyBackend;

#[async_trait::async_trait]
impl CdnBackend for UnconditionalOnlyBackend {
    async fn upload(
        &self,
        _path: &str,
        _bytes: Vec<u8>,
        _content_type: Option<&str>,
        _cache_control: Option<&str>,
    ) -> Result<(), CdnError> {
        panic!("the default conditional write must never fall back to an unconditional upload");
    }

    async fn delete(&self, _path: &str) -> Result<(), CdnError> {
        Ok(())
    }

    async fn get_etag(&self, _path: &str) -> Result<Option<String>, CdnError> {
        Ok(None)
    }
}

#[tokio::test]
async fn backend_without_cas_reports_unsupported_instead_of_writing_unconditionally() {
    let result = UnconditionalOnlyBackend
        .upload_if_match(PATH, b"v1".to_vec(), None, None, None)
        .await;

    assert!(matches!(
        result,
        Err(CdnError::ConditionalWritesUnsupported)
    ));
}
