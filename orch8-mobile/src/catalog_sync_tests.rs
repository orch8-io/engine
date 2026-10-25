use super::*;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer, SigningKey};
use orch8_publisher::{ManifestGenerator, ManifestSequence};
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::catalog::{CatalogAccess, CatalogAudience, CatalogIdentityFormat};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
struct Request {
    path: String,
    auth: Option<String>,
    etag: Option<String>,
    body: Value,
}
#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    gate: Option<Arc<Semaphore>>,
}
impl Reply {
    fn ok(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            headers: vec![],
            body,
            gate: None,
        }
    }
}
type Route = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;
struct Server {
    base: String,
    requests: Arc<Mutex<Vec<Request>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new(route: Route) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let requests: Arc<Mutex<Vec<Request>>> = Arc::default();
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    incoming = listener.accept() => {
                        let Ok((mut socket, _)) = incoming else { break; };
                        let route = Arc::clone(&route);
                        let recorded = Arc::clone(&recorded);
                        connections.spawn(async move {
                            let mut bytes = Vec::new();
                            let mut chunk = [0; 2048];
                            let divider = loop {
                                if let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") { break at + 4; }
                                let count = socket.read(&mut chunk).await.unwrap_or(0);
                                if count == 0 || bytes.len() + count > 32 * 1024 { return; }
                                bytes.extend_from_slice(&chunk[..count]);
                            };
                            let header = String::from_utf8_lossy(&bytes[..divider]).into_owned();
                            let field = |name: &str| header.lines().filter_map(|line| line.split_once(':')).find(|(key, _)| key.eq_ignore_ascii_case(name)).map(|(_, value)| value.trim().to_owned());
                            let length = field("content-length").and_then(|value| value.parse::<usize>().ok()).unwrap_or(0);
                            if length > 16 * 1024 { return; }
                            while bytes.len() < divider + length {
                                let count = socket.read(&mut chunk).await.unwrap_or(0);
                                if count == 0 { return; }
                                bytes.extend_from_slice(&chunk[..count]);
                            }
                            let request = Request { path:header.lines().next().unwrap().split_whitespace().nth(1).unwrap().into(), auth:field("authorization"), etag:field("if-none-match"), body:serde_json::from_slice(&bytes[divider..divider+length]).unwrap_or(Value::Null) };
                            recorded.lock().unwrap().push(request.clone());
                            let reply = route(&request);
                            if let Some(gate) = reply.gate { let _ = gate.acquire().await.map(tokio::sync::SemaphorePermit::forget); }
                            let mut header = format!("HTTP/1.1 {} Test\r\nContent-Length: {}\r\nConnection: close\r\n", reply.status, reply.body.len());
                            for (name, value) in reply.headers { let _ = write!(header,"{name}: {value}\r\n"); }
                            header.push_str("\r\n");
                            let _ = socket.write_all(header.as_bytes()).await;
                            let _ = socket.write_all(&reply.body).await;
                        });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {}
                }
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }
    fn recorded(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
    async fn saw(&self, path: &str) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !self.recorded().iter().any(|request| request.path == path) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[derive(Default)]
struct Script {
    replies: HashMap<String, Reply>,
    identity_mode: String,
    identity_gate: Option<Arc<Semaphore>>,
    previous_identity: Option<Vec<u8>>,
}
struct Fixture {
    sqlite: Arc<SqliteStorage>,
    storage: Arc<MobileStorage>,
    server: Server,
    script: Arc<Mutex<Script>>,
    signer: SigningKey,
    publisher: ManifestGenerator,
    coordinator: Arc<CatalogSyncCoordinator>,
}
impl Fixture {
    async fn new() -> Self {
        Self::with_storage(Arc::new(SqliteStorage::in_memory().await.unwrap())).await
    }
    async fn with_storage(sqlite: Arc<SqliteStorage>) -> Self {
        let script: Arc<Mutex<Script>> = Arc::default();
        let state = Arc::clone(&script);
        let server = Server::new(Arc::new(move |request| {
            let mut state = state.lock().unwrap();
            if request.path == "/identity" {
                let mut reply = Reply::ok(vec![]);
                let now = chrono::Utc::now();
                let subject = request
                    .auth
                    .as_deref()
                    .and_then(|token| token.strip_prefix("Bearer "))
                    .unwrap_or("")
                    .split('/')
                    .next()
                    .unwrap_or("");
                let mut response = CatalogIdentityResponse {
                    format: CatalogIdentityFormat::V1,
                    nonce: request.body["nonce"].as_str().unwrap_or("").into(),
                    audience: audience(subject, request.body["catalog"].as_str().unwrap_or("")),
                    issued_at: now,
                    expires_at: now + chrono::Duration::minutes(1),
                };
                response.audience.source = request.body["source"].as_str().unwrap_or("").into();
                match state.identity_mode.as_str() {
                    "nonce" => response.nonce = "another-request".into(),
                    "source" => response.audience.source = "another-source".into(),
                    "catalog" => response.audience.catalog = "another-catalog".into(),
                    "expired" => {
                        response.issued_at = now - chrono::Duration::minutes(2);
                        response.expires_at = now - chrono::Duration::minutes(1);
                    }
                    "future" => {
                        response.issued_at = now + chrono::Duration::hours(1);
                        response.expires_at = response.issued_at + chrono::Duration::minutes(1);
                    }
                    "long" => response.expires_at = now + chrono::Duration::hours(1),
                    "redirect" => {
                        reply.status = 302;
                        reply.headers.push(("Location".into(), "/stolen".into()));
                    }
                    "unauthorized" => reply.status = 401,
                    _ => {}
                }
                reply.body = if state.identity_mode == "large" {
                    let mut bytes = serde_json::to_vec(&response).unwrap();
                    bytes.resize(16 * 1024 + 1, b' ');
                    bytes
                } else if state.identity_mode == "replay" {
                    state.previous_identity.clone().unwrap()
                } else {
                    serde_json::to_vec(&response).unwrap()
                };
                state.previous_identity = Some(reply.body.clone());
                reply.gate = state.identity_gate.clone();
                return reply;
            }
            let mut reply = state.replies.get(&request.path).cloned().unwrap_or(Reply {
                status: 404,
                headers: vec![],
                body: vec![],
                gate: None,
            });
            if reply
                .headers
                .iter()
                .any(|(name, value)| name == "ETag" && request.etag.as_ref() == Some(value))
            {
                reply.status = 304;
                reply.body.clear();
            }
            reply
        }))
        .await;
        let signer = SigningKey::from_bytes(&[31; 32]);
        let publisher = ManifestGenerator::new(signer.clone(), "same-key".into());
        Self {
            storage: Arc::new(MobileStorage::new(Arc::clone(&sqlite))),
            sqlite,
            server,
            script,
            signer,
            publisher,
            coordinator: CatalogSyncCoordinator::new(),
        }
    }
    async fn scope(
        &self,
        subject: &str,
        catalog: &str,
    ) -> (crate::storage::scopes::AccountScope, CatalogScope) {
        let account = self
            .storage
            .open_account_scope(subject, "issuer", subject)
            .await
            .unwrap();
        let scope = self
            .storage
            .open_catalog_scope(
                &account,
                catalog,
                "source",
                &format!(
                    "ed25519:{}",
                    BASE64.encode(self.signer.verifying_key().as_bytes())
                ),
            )
            .await
            .unwrap();
        self.storage
            .bind_catalog_transport_for_test(
                &scope,
                &CatalogTransport {
                    manifest_url: format!(
                        "{}/catalog/{subject}/{catalog}/manifest",
                        self.server.base
                    ),
                    identity_url: format!("{}/identity", self.server.base),
                },
            )
            .await
            .unwrap();
        (account, scope)
    }
    async fn syncer(&self, scope: &CatalogScope) -> Arc<CatalogSynchronizer> {
        CatalogSynchronizer::build(
            Arc::clone(&self.storage),
            self.sqlite.clone(),
            scope.clone(),
            "0.7.1".into(),
            true,
        )
        .await
        .unwrap()
    }
    async fn sync(&self, scope: &CatalogScope, token: &str) -> Result<SyncResult, MobileError> {
        let token = token.to_owned();
        self.coordinator
            .sync(
                self.syncer(scope).await,
                HashSet::new(),
                CancellationToken::new(),
                move |_| std::future::ready(Ok(token)),
            )
            .await
    }
    fn publish(&self, subject: &str, catalog: &str, present: bool, required: &[&str]) -> i64 {
        self.publish_definition(subject, catalog, present, required, &definition())
    }
    fn publish_definition(
        &self,
        subject: &str,
        catalog: &str,
        present: bool,
        required: &[&str],
        body: &str,
    ) -> i64 {
        let prefix = format!("/catalog/{subject}/{catalog}");
        let entries = if present {
            vec![ManifestSequence {
                name: "flow".into(),
                version: 1,
                url: "flow.json".into(),
                signing_key_id: "same-key".into(),
                sha256: hex::encode(Sha256::digest(body.as_bytes())),
                required_handlers: required.iter().map(|value| (*value).into()).collect(),
                min_sdk_version: "0.7.1".into(),
            }]
        } else {
            vec![]
        };
        let signed = self
            .publisher
            .generate_catalog(audience(subject, catalog), entries, vec![], vec![])
            .unwrap();
        let version = signed.body.manifest.manifest_version;
        let mut script = self.script.lock().unwrap();
        let mut manifest =
            Reply::ok(format!("{}\n{}", signed.signature_b64, signed.canonical_json).into_bytes());
        manifest
            .headers
            .push(("ETag".into(), format!("\"{subject}-{catalog}-{version}\"")));
        script
            .replies
            .insert(format!("{prefix}/manifest"), manifest);
        script.replies.insert(
            format!("{prefix}/flow.json"),
            Reply::ok(body.as_bytes().to_vec()),
        );
        script.replies.insert(
            format!("{prefix}/flow.sig"),
            Reply::ok(
                BASE64
                    .encode(self.signer.sign(body.as_bytes()).to_bytes())
                    .into_bytes(),
            ),
        );
        version
    }
}
fn audience(subject: &str, catalog: &str) -> CatalogAudience {
    CatalogAudience {
        source: "source".into(),
        catalog: catalog.into(),
        access: CatalogAccess::Principal {
            issuer: "issuer".into(),
            subject: subject.into(),
        },
    }
}
fn definition() -> String {
    json!({"id":"11111111-1111-4111-8111-111111111111","tenant_id":"remote","namespace":"remote","name":"flow","version":1,"blocks":[],"created_at":"2026-01-01T00:00:00Z"}).to_string()
}

fn edit_manifest(fixture: &Fixture, subject: &str, catalog: &str, edit: impl FnOnce(&mut Value)) {
    let mut script = fixture.script.lock().unwrap();
    let reply = script
        .replies
        .get_mut(&format!("/catalog/{subject}/{catalog}/manifest"))
        .unwrap();
    let divider = reply.body.iter().position(|byte| *byte == b'\n').unwrap();
    let mut document: Value = serde_json::from_slice(&reply.body[divider + 1..]).unwrap();
    edit(&mut document);
    let body = orch8_publisher::manifest::canonical_json(&document).unwrap();
    reply.body = format!(
        "{}\n{body}",
        BASE64.encode(fixture.signer.sign(body.as_bytes()).to_bytes())
    )
    .into_bytes();
}

#[tokio::test]
async fn failed_download_does_not_allow_published_entry_equivocation_or_rollback() {
    for downgrade in [false, true] {
        let fixture = Fixture::new().await;
        let (_, scope) = fixture.scope("alice", "one").await;
        let original = fixture.publish("alice", "one", true, &[]);
        if downgrade {
            edit_manifest(&fixture, "alice", "one", |document| {
                document["manifest"]["sequences"][0]["version"] = 2.into();
            });
        }
        fixture
            .script
            .lock()
            .unwrap()
            .replies
            .get_mut("/catalog/alice/one/flow.json")
            .unwrap()
            .status = 404;
        assert_eq!(fixture.sync(&scope, "alice/v1").await.unwrap().skipped, 1);
        assert_eq!(
            fixture
                .storage
                .catalog_manifest_trust(&scope)
                .await
                .unwrap()
                .accepted_version,
            Some(original)
        );
        fixture.publish("alice", "one", true, &[]);
        if !downgrade {
            edit_manifest(&fixture, "alice", "one", |document| {
                document["manifest"]["sequences"][0]["sha256"] = "f".repeat(64).into();
            });
        }
        let before = fixture.server.recorded().len();
        assert!(fixture.sync(&scope, "alice/v2").await.is_err());
        assert_eq!(
            fixture
                .storage
                .catalog_manifest_trust(&scope)
                .await
                .unwrap()
                .accepted_version,
            Some(original),
            "entry identity must be durable before a download succeeds"
        );
        assert_eq!(
            fixture.server.recorded().len(),
            before + 2,
            "refuse the new manifest before another entry request"
        );
    }
}

#[tokio::test]
async fn four_catalogs_sync_and_revalidate_without_shared_metadata_or_workflow_ids() {
    let fixture = Fixture::new().await;
    fixture
        .storage
        .set_sync_metadata("manifest_etag", "legacy-secret-validator")
        .await
        .unwrap();
    let mut ids = HashSet::new();
    for subject in ["alice", "bob"] {
        for catalog in ["one", "two"] {
            let (_, scope) = fixture.scope(subject, catalog).await;
            let version = fixture.publish(subject, catalog, true, &[]);
            assert_eq!(
                fixture
                    .sync(&scope, &format!("{subject}/v1"))
                    .await
                    .unwrap()
                    .added,
                1
            );
            let workflow = fixture
                .storage
                .catalog_workflow(&scope, "flow", None)
                .await
                .unwrap()
                .unwrap();
            assert!(ids.insert(workflow.id));
            assert_eq!(
                fixture
                    .storage
                    .catalog_manifest_trust(&scope)
                    .await
                    .unwrap()
                    .applied_version,
                Some(version)
            );
            assert_eq!(
                fixture
                    .sync(&scope, &format!("{subject}/v2"))
                    .await
                    .unwrap()
                    .added,
                0
            );
        }
    }
    let requests = fixture.server.recorded();
    let identities: Vec<_> = requests
        .iter()
        .filter(|request| request.path == "/identity")
        .collect();
    assert_eq!(identities.len(), 8);
    let nonces: HashSet<_> = identities
        .iter()
        .map(|request| request.body["nonce"].as_str().unwrap())
        .collect();
    assert_eq!(nonces.len(), 8);
    assert!(
        requests
            .iter()
            .all(|request| request.etag.as_deref() != Some("legacy-secret-validator"))
    );
    assert!(
        fixture
            .storage
            .manifest_trust()
            .await
            .unwrap()
            .accepted_version
            .is_none()
    );
}

#[tokio::test]
async fn foreign_current_credential_is_refused_before_private_manifest_dispatch() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    let error = fixture
        .sync(&scope, "bob/private-token")
        .await
        .err()
        .unwrap()
        .to_string();
    assert!(!error.contains("private-token"));
    assert_eq!(
        fixture
            .server
            .recorded()
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        vec!["/identity"]
    );
    assert!(
        fixture
            .storage
            .catalog_manifest_trust(&scope)
            .await
            .unwrap()
            .accepted_version
            .is_none()
    );
    fixture.sync(&scope, "alice/rotated-token").await.unwrap();
    assert!(
        fixture
            .storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn bad_identity_nonce_origin_catalog_time_redirect_or_size_never_fetches_content() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    for mode in [
        "nonce",
        "source",
        "catalog",
        "expired",
        "future",
        "long",
        "redirect",
        "unauthorized",
        "large",
    ] {
        fixture.script.lock().unwrap().identity_mode = mode.into();
        assert!(
            fixture.sync(&scope, "alice/private-token").await.is_err(),
            "{mode}"
        );
    }
    assert!(
        fixture
            .server
            .recorded()
            .iter()
            .all(|request| request.path == "/identity")
    );
    fixture.script.lock().unwrap().identity_mode.clear();
    fixture.sync(&scope, "alice/v1").await.unwrap();
    let count = fixture.server.recorded().len();
    fixture.script.lock().unwrap().identity_mode = "replay".into();
    assert!(fixture.sync(&scope, "alice/v2").await.is_err());
    assert_eq!(
        fixture.server.recorded().len(),
        count + 1,
        "a previous nonce cannot authorize a new credential snapshot"
    );
}

#[tokio::test]
async fn withdrawal_and_changed_handler_evaluation_are_catalog_scoped() {
    let fixture = Fixture::new().await;
    let (_, a) = fixture.scope("alice", "one").await;
    let (_, b) = fixture.scope("bob", "one").await;
    fixture.publish("alice", "one", true, &[]);
    fixture.publish("bob", "one", true, &[]);
    fixture.sync(&a, "alice/v1").await.unwrap();
    fixture.sync(&b, "bob/v1").await.unwrap();
    fixture.publish("alice", "one", false, &[]);
    assert_eq!(fixture.sync(&a, "alice/v1").await.unwrap().removed, 1);
    assert!(
        fixture
            .storage
            .catalog_workflow(&a, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .storage
            .catalog_workflow(&b, "flow", None)
            .await
            .unwrap()
            .is_some()
    );
    fixture.publish("alice", "one", true, &["new-handler"]);
    assert_eq!(fixture.sync(&a, "alice/v1").await.unwrap().skipped, 1);
    let result = fixture
        .coordinator
        .sync(
            fixture.syncer(&a).await,
            HashSet::from(["new-handler".into()]),
            CancellationToken::new(),
            |_| std::future::ready(Ok("alice/v1".into())),
        )
        .await
        .unwrap();
    assert_eq!(
        result.added, 1,
        "an environment change must bypass an already-applied ETag"
    );
}

#[tokio::test]
async fn expired_identity_cancels_a_private_retry_wait_without_redispatch() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    {
        let mut script = fixture.script.lock().unwrap();
        let reply = script
            .replies
            .get_mut("/catalog/alice/one/manifest")
            .unwrap();
        reply.status = 503;
        reply.headers = vec![("Retry-After".into(), "120".into())];
    }
    let coordinator = Arc::clone(&fixture.coordinator);
    let syncer = fixture.syncer(&scope).await;
    let mut caller = tokio::spawn(async move {
        coordinator
            .sync(syncer, HashSet::new(), CancellationToken::new(), |_| {
                std::future::ready(Ok("alice/v1".into()))
            })
            .await
    });
    fixture.server.saw("/catalog/alice/one/manifest").await;
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    let outcome = tokio::time::timeout(Duration::from_secs(2), &mut caller).await;
    let settled = outcome.is_ok();
    if !settled {
        caller.abort();
        let _ = caller.await;
    }
    tokio::time::resume();
    assert!(
        settled,
        "private retry sleeps must stop at the verified identity's monotonic deadline"
    );
    assert!(matches!(outcome, Ok(Ok(Err(MobileError::Shutdown)))));
    assert_eq!(
        fixture
            .server
            .recorded()
            .iter()
            .filter(|request| request.path == "/catalog/alice/one/manifest")
            .count(),
        1
    );
}

#[tokio::test]
async fn one_unavailable_entry_does_not_starve_a_valid_entry_or_advance_the_cursor() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    let mut good: Value = serde_json::from_str(&definition()).unwrap();
    good["name"] = "good".into();
    good["id"] = "22222222-2222-4222-8222-222222222222".into();
    let good = good.to_string();
    edit_manifest(&fixture, "alice", "one", |document| {
        let mut entry = document["manifest"]["sequences"][0].clone();
        entry["name"] = "good".into();
        entry["url"] = "good.json".into();
        entry["sha256"] = hex::encode(Sha256::digest(good.as_bytes())).into();
        document["manifest"]["sequences"]
            .as_array_mut()
            .unwrap()
            .push(entry);
    });
    {
        let mut script = fixture.script.lock().unwrap();
        script
            .replies
            .get_mut("/catalog/alice/one/flow.json")
            .unwrap()
            .status = 404;
        script.replies.insert(
            "/catalog/alice/one/good.json".into(),
            Reply::ok(good.as_bytes().to_vec()),
        );
        script.replies.insert(
            "/catalog/alice/one/good.sig".into(),
            Reply::ok(
                BASE64
                    .encode(fixture.signer.sign(good.as_bytes()).to_bytes())
                    .into_bytes(),
            ),
        );
    }
    let result = fixture.sync(&scope, "alice/v1").await.unwrap();
    assert_eq!(result.added, 1);
    assert_eq!(result.skipped, 1);
    assert!(
        fixture
            .storage
            .catalog_workflow(&scope, "good", None)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        fixture
            .storage
            .catalog_manifest_trust(&scope)
            .await
            .unwrap()
            .pending()
    );
    assert!(
        fixture
            .storage
            .catalog_metadata(&scope, "manifest_etag")
            .await
            .unwrap()
            .is_none()
    );
    let prepared = fixture
        .storage
        .prepare_catalog_run(
            &scope,
            crate::storage::scopes::runs::CatalogRunRequest {
                workflow: "good",
                version: None,
                input_json: "{}",
                dedup_key: Some("good-operation"),
            },
            admission_limits(),
        )
        .await
        .unwrap();
    assert_eq!(
        prepared.workflow, "good",
        "pending downloads must not block reconciled, authorized cached work"
    );
}

#[tokio::test]
async fn another_account_progresses_while_a_retired_predecessor_retains_its_slot() {
    let fixture = Fixture::new().await;
    let (account, a) = fixture.scope("alice", "one").await;
    let (_, b) = fixture.scope("bob", "one").await;
    fixture.publish("alice", "one", true, &[]);
    fixture.publish("bob", "one", true, &[]);
    let gate = Arc::new(Semaphore::new(0));
    fixture.script.lock().unwrap().identity_gate = Some(Arc::clone(&gate));
    let syncer = fixture.syncer(&a).await;
    let coordinator = Arc::clone(&fixture.coordinator);
    let old = tokio::spawn(async move {
        coordinator
            .sync(syncer, HashSet::new(), CancellationToken::new(), |_| {
                std::future::ready(Ok("alice/v1".into()))
            })
            .await
    });
    fixture.server.saw("/identity").await;
    fixture.script.lock().unwrap().identity_gate = None;
    fixture
        .storage
        .retire_account_scope(&account)
        .await
        .unwrap();
    let (_, reopened) = fixture.scope("alice", "one").await;
    assert!(
        matches!(
            fixture.sync(&reopened, "alice/v2").await,
            Err(MobileError::ResourceLimit { .. })
        ),
        "reopen must not bypass the predecessor's physical run slot"
    );
    assert_eq!(fixture.sync(&b, "bob/v1").await.unwrap().added, 1);
    gate.add_permits(1);
    assert!(old.await.unwrap().is_err());
    assert!(
        !fixture
            .server
            .recorded()
            .iter()
            .any(|request| request.path == "/catalog/alice/one/manifest"),
        "retirement must fence the late identity response"
    );
    assert_eq!(fixture.sync(&reopened, "alice/v2").await.unwrap().added, 1);
}

#[tokio::test]
async fn two_accounts_bound_global_admission_before_a_third_provider_runs() {
    let fixture = Fixture::new().await;
    let mut tasks = Vec::new();
    let gate = Arc::new(Semaphore::new(0));
    fixture.script.lock().unwrap().identity_gate = Some(Arc::clone(&gate));
    for subject in ["alice", "bob"] {
        let (_, scope) = fixture.scope(subject, "one").await;
        fixture.publish(subject, "one", false, &[]);
        let syncer = fixture.syncer(&scope).await;
        let coordinator = Arc::clone(&fixture.coordinator);
        let token = format!("{subject}/v1");
        tasks.push(tokio::spawn(async move {
            coordinator
                .sync(
                    syncer,
                    HashSet::new(),
                    CancellationToken::new(),
                    move |_| std::future::ready(Ok(token)),
                )
                .await
        }));
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture
            .server
            .recorded()
            .iter()
            .filter(|request| request.path == "/identity")
            .count()
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, third) = fixture.scope("charlie", "one").await;
    let result = fixture
        .coordinator
        .sync(
            fixture.syncer(&third).await,
            HashSet::new(),
            CancellationToken::new(),
            |_| async {
                panic!("provider must not run outside sync admission");
            },
        )
        .await;
    gate.add_permits(2);
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    assert!(matches!(result, Err(MobileError::ResourceLimit { .. })));
}

#[tokio::test]
async fn cancelled_caller_keeps_admission_until_scoped_metadata_commit_settles() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sync.db");
    let path = path.to_str().unwrap();
    let fixture =
        Fixture::with_storage(Arc::new(SqliteStorage::file_mobile(path).await.unwrap())).await;
    let reader = MobileStorage::new(Arc::new(SqliteStorage::file_mobile(path).await.unwrap()));
    let (_, scope) = fixture.scope("alice", "one").await;
    let version = fixture.publish("alice", "one", false, &[]);
    let syncer = fixture.syncer(&scope).await;
    let (entered, release) = fixture.storage.pause_next_metadata_commit();
    let coordinator = Arc::clone(&fixture.coordinator);
    let first = Arc::clone(&syncer);
    let caller = tokio::spawn(async move {
        coordinator
            .sync(first, HashSet::new(), CancellationToken::new(), |_| {
                std::future::ready(Ok("alice/v1".into()))
            })
            .await
    });
    if tokio::time::timeout(Duration::from_secs(5), entered)
        .await
        .is_err()
    {
        release.add_permits(1);
        caller.abort();
        panic!("catalog metadata write did not reach its barrier");
    }
    caller.abort();
    let _ = caller.await;
    let concurrent = fixture
        .coordinator
        .sync(
            syncer,
            HashSet::new(),
            CancellationToken::new(),
            |_| async {
                panic!("cancelled caller still owns the write slot");
            },
        )
        .await;
    let observed = reader
        .catalog_metadata(&scope, "last_manifest_version")
        .await;
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.coordinator.global.available_permits() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(concurrent, Err(MobileError::ResourceLimit { .. })));
    assert!(
        observed.unwrap().is_none(),
        "uncommitted applied cursor must not leak"
    );
    assert_eq!(
        reader
            .catalog_metadata(&scope, "last_manifest_version")
            .await
            .unwrap(),
        Some(version.to_string())
    );
}

#[tokio::test]
async fn foreign_manifest_never_downloads_entries_and_cdn_never_receives_bearer() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    edit_manifest(&fixture, "alice", "one", |document| {
        document["audience"]["access"]["subject"] = "bob".into();
    });
    assert!(fixture.sync(&scope, "alice/private-token").await.is_err());
    assert_eq!(fixture.server.recorded().len(), 2);
    let signer = fixture.signer.clone();
    let cdn = Server::new(Arc::new(move |request| match request.path.as_str() {
        "/flow.json" => Reply::ok(definition().as_bytes().to_vec()),
        "/flow.sig" => Reply::ok(
            BASE64
                .encode(signer.sign(definition().as_bytes()).to_bytes())
                .into_bytes(),
        ),
        _ => Reply {
            status: 404,
            headers: vec![],
            body: vec![],
            gate: None,
        },
    }))
    .await;
    fixture.publish("alice", "one", true, &[]);
    edit_manifest(&fixture, "alice", "one", |document| {
        document["manifest"]["sequences"][0]["url"] = format!("{}/flow.json", cdn.base).into();
    });
    assert_eq!(
        fixture
            .sync(&scope, "alice/private-token")
            .await
            .unwrap()
            .added,
        1
    );
    let requests = cdn.recorded();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|request| request.auth.is_none()));
}

#[tokio::test]
async fn endpoint_binding_and_per_catalog_validators_survive_full_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("reopen.db");
    let path = path.to_str().unwrap();
    let fixture =
        Fixture::with_storage(Arc::new(SqliteStorage::file_mobile(path).await.unwrap())).await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    fixture.sync(&scope, "alice/v1").await.unwrap();
    let original = fixture
        .storage
        .catalog_workflow(&scope, "flow", None)
        .await
        .unwrap()
        .unwrap()
        .id;
    fixture.sqlite.pool().close().await;
    let sqlite = Arc::new(SqliteStorage::file_mobile(path).await.unwrap());
    let storage = Arc::new(MobileStorage::new(Arc::clone(&sqlite)));
    let account = storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    let scope = storage
        .open_catalog_scope(
            &account,
            "one",
            "source",
            &format!(
                "ed25519:{}",
                BASE64.encode(fixture.signer.verifying_key().as_bytes())
            ),
        )
        .await
        .unwrap();
    let syncer = CatalogSynchronizer::build(
        Arc::clone(&storage),
        sqlite,
        scope.clone(),
        "0.7.1".into(),
        true,
    )
    .await
    .unwrap();
    fixture
        .coordinator
        .sync(syncer, HashSet::new(), CancellationToken::new(), |_| {
            std::future::ready(Ok("alice/v2".into()))
        })
        .await
        .unwrap();
    let request = fixture
        .server
        .recorded()
        .into_iter()
        .rev()
        .find(|request| request.path.ends_with("/manifest"))
        .unwrap();
    assert!(
        request.etag.is_some(),
        "eligible validator must survive database reopen and token rotation"
    );
    assert_eq!(
        storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .unwrap()
            .id,
        original
    );
    assert!(
        storage
            .bind_catalog_transport(
                &scope,
                &CatalogTransport {
                    manifest_url: "https://other.example/manifest".into(),
                    identity_url: "https://other.example/identity".into()
                }
            )
            .await
            .is_err()
    );
}

#[tokio::test]
async fn oversized_scoped_collections_are_refused_before_trust_changes() {
    for collection in ["sequences", "signing_keys", "removed"] {
        let fixture = Fixture::new().await;
        let (_, scope) = fixture.scope("alice", "one").await;
        fixture.publish("alice", "one", true, &[]);
        edit_manifest(&fixture, "alice", "one", |document| {
            let original = document["manifest"][collection]
                .as_array()
                .unwrap()
                .first()
                .cloned();
            let entries = (0..1001)
                .map(|index| match collection {
                    "sequences" => {
                        let mut entry = original.clone().unwrap();
                        entry["name"] = format!("flow-{index}").into();
                        entry
                    }
                    "signing_keys" => {
                        let mut key = original.clone().unwrap();
                        if index != 0 {
                            key["key_id"] = format!("key-{index}").into();
                        }
                        key
                    }
                    _ => json!({"name":format!("removed-{index}"),"removed_at":chrono::Utc::now()}),
                })
                .collect::<Vec<_>>();
            document["manifest"][collection] = entries.into();
        });
        let bytes = fixture.script.lock().unwrap().replies["/catalog/alice/one/manifest"]
            .body
            .clone();
        assert!(
            fixture
                .storage
                .accept_signed_catalog_manifest(&scope, &bytes)
                .await
                .is_err(),
            "{collection}"
        );
        assert!(
            fixture
                .storage
                .catalog_manifest_trust(&scope)
                .await
                .unwrap()
                .accepted_version
                .is_none()
        );
        assert!(
            fixture
                .storage
                .catalog_signing_keys(&scope)
                .await
                .unwrap()
                .is_empty()
        );
    }
}

fn admission_limits() -> crate::storage::scopes::runs::CatalogRunLimits {
    crate::storage::scopes::runs::CatalogRunLimits {
        global_records: 16,
        account_records: 8,
        catalog_records: 4,
        global_input_bytes: 1024 * 1024,
        account_input_bytes: 512 * 1024,
        catalog_input_bytes: 256 * 1024,
    }
}

#[tokio::test]
async fn accepted_withdrawal_fences_new_admission_before_removal_reconciliation() {
    let fixture = Fixture::new().await;
    let (_, scope) = fixture.scope("alice", "one").await;
    fixture.publish("alice", "one", true, &[]);
    fixture.sync(&scope, "alice/v1").await.unwrap();
    let request = |key| crate::storage::scopes::runs::CatalogRunRequest {
        workflow: "flow",
        version: None,
        input_json: "{}",
        dedup_key: Some(key),
    };
    let old = fixture
        .storage
        .prepare_catalog_run(&scope, request("old"), admission_limits())
        .await
        .unwrap();
    fixture.publish("alice", "one", false, &[]);
    let bytes = fixture.script.lock().unwrap().replies["/catalog/alice/one/manifest"]
        .body
        .clone();
    fixture
        .storage
        .accept_signed_catalog_manifest(&scope, &bytes)
        .await
        .unwrap();
    assert!(
        fixture
            .storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_some(),
        "exercise the accepted-but-not-yet-reconciled window"
    );
    assert!(
        fixture
            .storage
            .prepare_catalog_run(&scope, request("new"), admission_limits())
            .await
            .is_err()
    );
    assert_eq!(
        fixture
            .storage
            .prepare_catalog_run(&scope, request("old"), admission_limits())
            .await
            .unwrap()
            .id,
        old.id
    );
    assert_eq!(fixture.sync(&scope, "alice/v2").await.unwrap().removed, 1);
    assert!(
        fixture
            .storage
            .catalog_workflow(&scope, "flow", None)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .storage
            .prepare_catalog_run(&scope, request("new"), admission_limits())
            .await
            .is_err()
    );
}

#[test]
fn public_sdk_sync_authenticates_four_catalogs_before_private_content_and_dispatches_their_runs() {
    use crate::{
        MobileAccountIdentity, MobileCatalogRunRequest, MobileEngine, MobileEngineConfig,
        TokenProvider,
    };
    struct Token(String);
    impl TokenProvider for Token {
        fn current_token(&self) -> String {
            self.0.clone()
        }
        fn refresh_token(&self) -> Result<String, MobileError> {
            Ok(self.0.clone())
        }
    }
    let engine = MobileEngine::new(
        ":memory:".into(),
        MobileEngineConfig {
            telemetry_enabled: false,
            operation_timeout_ms: 3000,
            ..Default::default()
        },
    )
    .unwrap();
    let fixture = engine
        .runtime
        .block_on(Fixture::with_storage(engine.sqlite.clone()));
    let mut retained = Vec::new();
    for subject in ["alice", "bob"] {
        let account = engine
            .clone()
            .open_account(MobileAccountIdentity {
                account_key: subject.into(),
                issuer: "issuer".into(),
                subject: subject.into(),
            })
            .unwrap();
        for catalog_name in ["one", "two"] {
            let (_, scope) = engine
                .runtime
                .block_on(fixture.scope(subject, catalog_name));
            let syncer = engine.runtime.block_on(fixture.syncer(&scope));
            let catalog = crate::scoped_sdk::catalog_for_transport_test(&account, scope, syncer);
            let mut body: Value = serde_json::from_str(&definition()).unwrap();
            body["blocks"] = json!([{"type":"step","id":"work","handler":"noop","params":{}}]);
            fixture.publish_definition(subject, catalog_name, true, &[], &body.to_string());
            assert_eq!(
                catalog
                    .sync(Arc::new(Token(format!("{subject}/v1"))))
                    .unwrap()
                    .added,
                1
            );
            let before = fixture.server.recorded().len();
            let wrong = if subject == "alice" { "bob" } else { "alice" };
            assert!(
                catalog
                    .sync(Arc::new(Token(format!("{wrong}/v1"))))
                    .is_err()
            );
            let requests = fixture.server.recorded();
            assert_eq!(requests.len(), before + 1);
            assert_eq!(requests[before].path, "/identity");
            let run = catalog
                .prepare_run(MobileCatalogRunRequest {
                    workflow: "flow".into(),
                    version: None,
                    input_json: "{}".into(),
                    operation_key: "same-key".into(),
                })
                .unwrap();
            run.start().unwrap();
            for _ in 0..30 {
                engine.tick_once().unwrap();
                if run.snapshot().unwrap().unwrap().state == crate::InstanceStateKind::Completed {
                    break;
                }
            }
            assert_eq!(
                run.snapshot().unwrap().unwrap().state,
                crate::InstanceStateKind::Completed
            );
            assert_eq!(
                catalog.history(None, 8).unwrap().items[0].instance_id,
                run.instance_id()
            );
            retained.push((account.clone(), catalog, run));
        }
    }
    assert_eq!(retained.len(), 4);
    engine.shutdown();
}

mod sdk_token_test_support {
    use crate::{MobileError, TokenProvider};
    use std::sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    pub(super) struct Gate(pub(super) Arc<(Mutex<bool>, Condvar)>);
    impl Drop for Gate {
        fn drop(&mut self) {
            let (lock, wake) = &*self.0;
            *lock.lock().unwrap() = true;
            wake.notify_all();
        }
    }
    pub(super) struct Blocked {
        pub(super) gate: Arc<(Mutex<bool>, Condvar)>,
        pub(super) entered: std::sync::mpsc::Sender<()>,
        pub(super) block_drop: bool,
    }
    impl Blocked {
        fn wait(&self) {
            let _ = self.entered.send(());
            let (lock, wake) = &*self.gate;
            let mut ready = lock.lock().unwrap();
            while !*ready {
                ready = wake.wait(ready).unwrap();
            }
        }
    }
    impl Drop for Blocked {
        fn drop(&mut self) {
            if self.block_drop {
                self.wait();
            }
        }
    }
    impl TokenProvider for Blocked {
        fn current_token(&self) -> String {
            if !self.block_drop {
                self.wait();
            }
            "alice/v1".into()
        }
        fn refresh_token(&self) -> Result<String, MobileError> {
            Ok("alice/v1".into())
        }
    }
    pub(super) struct Token {
        pub(super) value: &'static str,
        pub(super) calls: Arc<AtomicUsize>,
    }
    impl TokenProvider for Token {
        fn current_token(&self) -> String {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.value.into()
        }
        fn refresh_token(&self) -> Result<String, MobileError> {
            Ok(self.value.into())
        }
    }
}

#[test]
fn public_sdk_hung_token_keeps_its_account_charge_across_close_and_reopen_while_another_account_syncs()
 {
    use crate::{MobileAccountIdentity, MobileEngine, MobileEngineConfig};
    use sdk_token_test_support::{Blocked, Gate, Token};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for block_drop in [false, true] {
        let engine = MobileEngine::new(
            ":memory:".into(),
            MobileEngineConfig {
                telemetry_enabled: false,
                operation_timeout_ms: 3000,
                ..Default::default()
            },
        )
        .unwrap();
        let fixture = engine
            .runtime
            .block_on(Fixture::with_storage(engine.sqlite.clone()));
        let identity = |name: &str| MobileAccountIdentity {
            account_key: name.into(),
            issuer: "issuer".into(),
            subject: name.into(),
        };
        let alice = engine.clone().open_account(identity("alice")).unwrap();
        let (_, scope) = engine.runtime.block_on(fixture.scope("alice", "one"));
        let catalog = crate::scoped_sdk::catalog_for_transport_test(
            &alice,
            scope.clone(),
            engine.runtime.block_on(fixture.syncer(&scope)),
        );
        fixture.publish("alice", "one", true, &[]);
        let gate = Gate(Arc::new((Mutex::new(false), std::sync::Condvar::new())));
        let (entered, active) = std::sync::mpsc::channel();
        let (done, result) = std::sync::mpsc::channel();
        let caller = std::thread::spawn({
            let catalog = catalog.clone();
            let gate = gate.0.clone();
            move || {
                done.send(catalog.sync(Arc::new(Blocked {
                    gate,
                    entered,
                    block_drop,
                })))
                .unwrap();
            }
        });
        active.recv_timeout(Duration::from_secs(3)).unwrap();
        catalog.close_view();
        assert!(matches!(
            result.recv_timeout(Duration::from_secs(2)).unwrap(),
            Err(MobileError::Shutdown)
        ));
        caller.join().unwrap();
        alice.retire().unwrap();
        let fresh = engine.clone().open_account(identity("alice")).unwrap();
        let (_, scope) = engine.runtime.block_on(fixture.scope("alice", "one"));
        let reopened = crate::scoped_sdk::catalog_for_transport_test(
            &fresh,
            scope.clone(),
            engine.runtime.block_on(fixture.syncer(&scope)),
        );
        let alice_calls = Arc::new(AtomicUsize::new(0));
        let retry = reopened.sync(Arc::new(Token {
            value: "alice/v2",
            calls: alice_calls.clone(),
        }));
        let bob = engine.clone().open_account(identity("bob")).unwrap();
        let (_, scope) = engine.runtime.block_on(fixture.scope("bob", "one"));
        let other = crate::scoped_sdk::catalog_for_transport_test(
            &bob,
            scope.clone(),
            engine.runtime.block_on(fixture.syncer(&scope)),
        );
        fixture.publish("bob", "one", true, &[]);
        let bob_result = other.sync(Arc::new(Token {
            value: "bob/v1",
            calls: Arc::new(AtomicUsize::new(0)),
        }));
        drop(gate);
        assert!(
            matches!(retry, Err(MobileError::ResourceLimit { .. })),
            "old physical token call must retain account admission"
        );
        assert_eq!(alice_calls.load(Ordering::SeqCst), 0);
        assert_eq!(bob_result.unwrap().added, 1);
        engine.shutdown();
    }
}
