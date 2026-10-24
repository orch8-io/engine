use super::*;
use ed25519_dalek::Signer;
use orch8_storage::sqlite::SqliteStorage;
use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone)]
struct Reply {
    status: &'static str,
    headers: Vec<(&'static str, String)>,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct Request {
    path: String,
    authorization: Option<String>,
}

struct RecordingServer {
    base: String,
    replies: Arc<Mutex<HashMap<String, VecDeque<Reply>>>>,
    requests: Arc<Mutex<Vec<Request>>>,
    task: tokio::task::JoinHandle<()>,
}

impl RecordingServer {
    async fn new() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let replies: Arc<Mutex<HashMap<String, VecDeque<Reply>>>> = Arc::default();
        let routes = Arc::clone(&replies);
        let requests: Arc<Mutex<Vec<Request>>> = Arc::default();
        let recorded = Arc::clone(&requests);
        let task = tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut bytes = Vec::new();
                let mut chunk = [0; 1024];
                while bytes.len() < 32 * 1024 && !bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let text = String::from_utf8_lossy(&bytes);
                let path = text
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let authorization = text
                    .lines()
                    .filter_map(|line| line.split_once(':'))
                    .find(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                    .map(|(_, value)| value.trim().to_owned());
                recorded.lock().unwrap().push(Request {
                    path: path.clone(),
                    authorization,
                });
                let reply = routes
                    .lock()
                    .unwrap()
                    .get_mut(&path)
                    .and_then(|script| {
                        if script.len() > 1 {
                            script.pop_front()
                        } else {
                            script.front().cloned()
                        }
                    })
                    .unwrap_or(Reply {
                        status: "404 Not Found",
                        headers: vec![],
                        body: vec![],
                    });
                let mut header = format!(
                    "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
                    reply.status,
                    reply.body.len()
                );
                for (name, value) in reply.headers {
                    write!(header, "{name}: {value}\r\n").unwrap();
                }
                header.push_str("\r\n");
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(&reply.body).await.unwrap();
                socket.shutdown().await.unwrap();
            }
        });
        Self {
            base: format!("http://127.0.0.1:{port}"),
            replies,
            requests,
            task,
        }
    }

    fn script(&self, path: &str, replies: Vec<Reply>) {
        assert!(!replies.is_empty());
        self.replies
            .lock()
            .unwrap()
            .insert(path.into(), replies.into());
    }

    fn serve(&self, path: &str, body: Vec<u8>) {
        self.script(
            path,
            vec![Reply {
                status: "200 OK",
                headers: vec![],
                body,
            }],
        );
    }

    fn redirect(&self, path: &str, location: String) {
        self.script(
            path,
            vec![Reply {
                status: "307 Temporary Redirect",
                headers: vec![("Location", location)],
                body: vec![],
            }],
        );
    }

    fn recorded(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for RecordingServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Fixture {
    orch: SyncOrchestrator,
    origin: RecordingServer,
    entry: RecordingServer,
    key: ed25519_dalek::SigningKey,
    sequence: Vec<u8>,
}

impl Fixture {
    async fn new() -> Self {
        let sqlite = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let key = ed25519_dalek::SigningKey::from_bytes(&[9; 32]);
        let orch = SyncOrchestrator::new(
            Arc::new(MobileStorage::new(sqlite.clone())),
            sqlite,
            RootKey {
                pubkey: key.verifying_key(),
            },
            "0.7.1".into(),
            50,
        )
        .with_insecure_urls_for_tests();
        let sequence =
            serde_json::to_vec(&super::tests::make_test_sequence("origin-flow", Utc::now()))
                .unwrap();
        Self {
            orch,
            origin: RecordingServer::new().await,
            entry: RecordingServer::new().await,
            key,
            sequence,
        }
    }

    fn manifest(&self, entry_url: &str) -> Vec<u8> {
        let body = serde_json::to_vec(&serde_json::json!({
            "signing_keys": [{"key_id": "k1", "public_key": BASE64.encode(self.key.verifying_key().as_bytes())}],
            "sequences": [{
                "name": "origin-flow", "version": 1, "url": entry_url, "signing_key_id": "k1",
                "sha256": to_hex(&Sha256::digest(&self.sequence)), "required_handlers": [], "min_sdk_version": "0.0.0",
            }],
            "removed": [], "manifest_version": 1, "generated_at": Utc::now(),
        })).unwrap();
        super::tests::signed_manifest_bytes(&self.key, &body)
    }

    fn serve_entry(&self, server: &RecordingServer, query: &str) {
        server.serve(&format!("/seq.json{query}"), self.sequence.clone());
        server.serve(
            &format!("/seq.sig{query}"),
            BASE64
                .encode(self.key.sign(&self.sequence).to_bytes())
                .into_bytes(),
        );
    }
}

#[tokio::test]
async fn signed_cross_origin_entries_cannot_receive_the_manifest_bearer() {
    for protocol_relative in [false, true] {
        let fixture = Fixture::new().await;
        let entry_url = format!("{}/seq.json", fixture.entry.base);
        let entry_url = if protocol_relative {
            entry_url.strip_prefix("http:").unwrap().to_owned()
        } else {
            entry_url
        };
        fixture
            .origin
            .serve("/manifest", fixture.manifest(&entry_url));
        fixture.serve_entry(&fixture.entry, "");
        let result = fixture
            .orch
            .sync(
                &format!("{}/manifest", fixture.origin.base),
                &SyncAuth::Bearer("manifest_secret".into()),
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            (result.added, result.signature_failures),
            (1, 0),
            "valid cross-origin content must remain usable"
        );
        let origin = fixture.origin.recorded();
        assert_eq!(origin.len(), 1);
        assert_eq!(
            origin[0].authorization.as_deref(),
            Some("Bearer manifest_secret")
        );
        let entry = fixture.entry.recorded();
        assert_eq!(
            entry.len(),
            2,
            "sequence and detached signature must both be verified"
        );
        assert!(
            entry.iter().all(|request| request.authorization.is_none()),
            "manifest credential crossed an origin boundary: {entry:?}"
        );
    }
}

#[tokio::test]
async fn same_origin_entries_and_signatures_keep_the_manifest_bearer() {
    for form in ["relative", "absolute", "protocol-relative"] {
        let fixture = Fixture::new().await;
        let entry_url = match form {
            "relative" => "nested/../seq.json".to_owned(),
            "absolute" => format!("{}/seq.json", fixture.origin.base),
            _ => format!(
                "{}/seq.json",
                fixture.origin.base.strip_prefix("http:").unwrap()
            ),
        };
        fixture
            .origin
            .serve("/manifest", fixture.manifest(&entry_url));
        fixture.serve_entry(&fixture.origin, "");
        let result = fixture
            .orch
            .sync(
                &format!("{}/manifest", fixture.origin.base),
                &SyncAuth::Bearer("manifest_secret".into()),
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!((result.added, result.signature_failures), (1, 0));
        let requests = fixture.origin.recorded();
        assert_eq!(
            requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
            ["/manifest", "/seq.json", "/seq.sig"]
        );
        assert!(
            requests
                .iter()
                .all(|r| r.authorization.as_deref() == Some("Bearer manifest_secret"))
        );
        assert!(fixture.entry.recorded().is_empty());
    }
}

#[tokio::test]
async fn signed_url_mode_preserves_each_downloads_own_query_without_bearer() {
    for cross_origin in [false, true] {
        let fixture = Fixture::new().await;
        let server = if cross_origin {
            &fixture.entry
        } else {
            &fixture.origin
        };
        let entry_url = format!("{}/seq.json?entry_token=entry_secret", server.base);
        fixture.origin.serve(
            "/manifest?manifest_token=manifest_secret",
            fixture.manifest(&entry_url),
        );
        fixture.serve_entry(server, "?entry_token=entry_secret");
        let result = fixture
            .orch
            .sync(
                &format!(
                    "{}/manifest?manifest_token=manifest_secret",
                    fixture.origin.base
                ),
                &SyncAuth::UrlToken,
                &HashSet::new(),
            )
            .await
            .unwrap();
        assert_eq!((result.added, result.signature_failures), (1, 0));
        let origin = fixture.origin.recorded();
        assert_eq!(origin[0].path, "/manifest?manifest_token=manifest_secret");
        let all = origin
            .into_iter()
            .chain(fixture.entry.recorded())
            .collect::<Vec<_>>();
        assert_eq!(all.len(), 3);
        assert!(all.iter().all(|r| r.authorization.is_none()));
        assert_eq!(all[1].path, "/seq.json?entry_token=entry_secret");
        assert_eq!(all[2].path, "/seq.sig?entry_token=entry_secret");
    }
}

#[tokio::test]
async fn cross_origin_retry_does_not_gain_the_manifest_bearer() {
    let fixture = Fixture::new().await;
    fixture.origin.serve(
        "/manifest",
        fixture.manifest(&format!("{}/seq.json", fixture.entry.base)),
    );
    fixture.serve_entry(&fixture.entry, "");
    fixture.entry.script(
        "/seq.json",
        vec![
            Reply {
                status: "503 Service Unavailable",
                headers: vec![("Retry-After", "0".into())],
                body: vec![],
            },
            Reply {
                status: "200 OK",
                headers: vec![],
                body: fixture.sequence.clone(),
            },
        ],
    );
    let result = fixture
        .orch
        .sync(
            &format!("{}/manifest", fixture.origin.base),
            &SyncAuth::Bearer("manifest_secret".into()),
            &HashSet::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.added, 1);
    let requests = fixture.entry.recorded();
    assert_eq!(
        requests.iter().map(|r| r.path.as_str()).collect::<Vec<_>>(),
        ["/seq.json", "/seq.json", "/seq.sig"]
    );
    assert!(requests.iter().all(|r| r.authorization.is_none()));
}

#[tokio::test]
async fn manifest_entry_and_signature_redirects_remain_disabled() {
    for phase in ["manifest", "sequence", "signature"] {
        let fixture = Fixture::new().await;
        let manifest = fixture.manifest("seq.json");
        fixture.origin.serve("/manifest", manifest.clone());
        fixture.serve_entry(&fixture.origin, "");
        fixture.serve_entry(&fixture.entry, "");
        fixture.entry.serve("/landing", manifest);
        let (path, target) = match phase {
            "manifest" => ("/manifest", "/landing"),
            "sequence" => ("/seq.json", "/seq.json"),
            _ => ("/seq.sig", "/seq.sig"),
        };
        fixture
            .origin
            .redirect(path, format!("{}{target}", fixture.entry.base));
        let result = fixture
            .orch
            .sync(
                &format!("{}/manifest", fixture.origin.base),
                &SyncAuth::Bearer("manifest_secret".into()),
                &HashSet::new(),
            )
            .await;
        if phase == "manifest" {
            assert!(result.is_err());
        } else {
            assert_eq!(result.unwrap().added, 0);
        }
        assert!(
            fixture.entry.recorded().is_empty(),
            "{phase} redirect must not be followed"
        );
        assert!(
            fixture
                .origin
                .recorded()
                .iter()
                .all(|r| r.authorization.as_deref() == Some("Bearer manifest_secret"))
        );
    }
}

#[tokio::test]
async fn shared_physical_server_does_not_merge_distinct_url_origins() {
    let fixture = Fixture::new().await;
    let root = reqwest::Url::parse(&fixture.origin.base).unwrap();
    let port = root.port().unwrap();
    let client = orch8_engine::outbound::build(
        orch8_engine::outbound::builder(orch8_engine::outbound::Profile::TokenEndpoint)
            .resolve("entry.example", ([127, 0, 0, 1], port).into()),
    );
    fixture.orch.set_http_client_for_test(client);
    fixture.origin.serve(
        "/manifest",
        fixture.manifest(&format!("http://entry.example:{port}/seq.json")),
    );
    fixture.serve_entry(&fixture.origin, "");
    let result = fixture
        .orch
        .sync(
            &format!("{}/manifest", fixture.origin.base),
            &SyncAuth::Bearer("manifest_secret".into()),
            &HashSet::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.added, 1);
    let requests = fixture.origin.recorded();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0].authorization.as_deref(),
        Some("Bearer manifest_secret")
    );
    assert!(requests[1..].iter().all(|r| r.authorization.is_none()));
}

#[tokio::test]
async fn unverified_manifest_does_not_authorize_entry_requests() {
    let fixture = Fixture::new().await;
    let mut manifest = fixture.manifest(&format!("{}/seq.json", fixture.entry.base));
    manifest[0] = if manifest[0] == b'A' { b'B' } else { b'A' };
    fixture.origin.serve("/manifest", manifest);
    fixture.serve_entry(&fixture.entry, "");
    let error = fixture
        .orch
        .sync(
            &format!("{}/manifest", fixture.origin.base),
            &SyncAuth::Bearer("manifest_secret".into()),
            &HashSet::new(),
        )
        .await
        .err()
        .unwrap();
    assert!(error.to_string().contains("signature"));
    assert_eq!(fixture.origin.recorded().len(), 1);
    assert!(fixture.entry.recorded().is_empty());
}

#[path = "sync_auth_scope_tests.rs"]
mod scope_tests;
