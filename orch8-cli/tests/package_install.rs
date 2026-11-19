use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{Method, StatusCode};
use axum::response::{IntoResponse, Response};
use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_publisher::package::{PackageManifest, PackageRequirements, build_package};
use orch8_storage::SequenceStore;
use orch8_types::ids::{Namespace, TenantId};
use serde_json::{Value, json};

struct Install {
    dir: tempfile::TempDir,
    server: TestServer,
    base: String,
    proxy: Option<tokio::task::JoinHandle<()>>,
}

struct Proxy {
    client: reqwest::Client,
    upstream: String,
    creates: AtomicUsize,
    fault: Fault,
}

#[derive(Clone, Copy)]
enum Fault {
    RejectSecondCreate,
    LoseFirstCreateResponse,
}

async fn forward(State(proxy): State<Arc<Proxy>>, request: Request) -> Response {
    let (parts, body) = request.into_parts();
    let is_create = parts.method == Method::POST && parts.uri.path() == "/api/v1/sequences";
    let create = if is_create {
        proxy.creates.fetch_add(1, Ordering::Relaxed) + 1
    } else {
        0
    };
    if create == 2 && matches!(proxy.fault, Fault::RejectSecondCreate) {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let response = proxy
        .client
        .request(parts.method, format!("{}{}", proxy.upstream, parts.uri))
        .header("content-type", "application/json")
        .body(to_bytes(body, 1024 * 1024).await.unwrap())
        .send()
        .await
        .unwrap();
    let status = response.status();
    if create == 1 && matches!(proxy.fault, Fault::LoseFirstCreateResponse) {
        assert_eq!(status, StatusCode::CREATED);
        return (status, Body::empty()).into_response();
    }
    (status, response.bytes().await.unwrap()).into_response()
}

impl Install {
    async fn new(fault: Option<Fault>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let server = spawn_test_server().await;
        let (base, proxy) = if let Some(fault) = fault {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let app = axum::Router::new()
                .fallback(forward)
                .with_state(Arc::new(Proxy {
                    client: reqwest::Client::new(),
                    upstream: server.base_url.clone(),
                    creates: AtomicUsize::new(0),
                    fault,
                }));
            let handle = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
            (format!("http://{addr}/api/v1"), Some(handle))
        } else {
            (server.v1_url(), None)
        };
        Self {
            dir,
            server,
            base,
            proxy,
        }
    }

    fn package(&self, files: &[(&str, Value)]) {
        let manifest = PackageManifest {
            name: "acme/billing".into(),
            version: "1.0.0".into(),
            description: "billing workflows".into(),
            publisher: "acme".into(),
            requirements: PackageRequirements::default(),
            created_at: "2026-07-25T00:00:00Z".parse().unwrap(),
        };
        let files = files
            .iter()
            .map(|(path, doc)| ((*path).into(), doc.to_string()))
            .collect::<BTreeMap<_, _>>();
        let package = build_package(
            manifest,
            files,
            &ed25519_dalek::SigningKey::from_bytes(&[5; 32]),
        )
        .unwrap();
        std::fs::write(
            self.dir.path().join("test.orch8pkg"),
            serde_json::to_vec(&package).unwrap(),
        )
        .unwrap();
    }

    async fn run(&self) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_orch8"));
        command.env_clear().current_dir(self.dir.path());
        for name in ["PATH", "TMPDIR", "HOME", "SystemRoot"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command.env("ORCH8_CONTEXTS_FILE", self.dir.path().join("contexts.json"));
        command.args([
            "--url",
            &self.base,
            "package",
            "install",
            "test.orch8pkg",
            "--tenant-id",
            "demo",
            "--allow-untrusted",
        ]);
        tokio::task::spawn_blocking(move || command.output().unwrap())
            .await
            .unwrap()
    }

    async fn sequence(&self, name: &str) -> Option<orch8::SequenceDefinition> {
        self.server
            .storage
            .get_sequence_by_name(
                &TenantId::new("demo").unwrap(),
                &Namespace::new("pkg.acme.billing"),
                name,
                Some(1),
            )
            .await
            .unwrap()
    }

    fn lockfile(&self) -> std::path::PathBuf {
        self.dir.path().join(Path::new("orch8-packages.lock"))
    }
}

impl Drop for Install {
    fn drop(&mut self) {
        if let Some(proxy) = &self.proxy {
            proxy.abort();
        }
    }
}

fn sequence(name: &str, id: u128) -> Value {
    json!({
        "id": uuid::Uuid::from_u128(id), "tenant_id": "demo", "namespace": "default",
        "name": name, "version": 1, "created_at": "2026-07-25T00:00:00Z",
        "blocks": [{"type": "step", "id": "charge", "handler": "charge_card"}]
    })
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[tokio::test]
async fn retry_resumes_after_a_later_create_failure() {
    let install = Install::new(Some(Fault::RejectSecondCreate)).await;
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("sequences/b.json", sequence("b", 2)),
    ]);
    let output = install.run().await;
    assert!(!output.status.success());
    let first_id = install.sequence("a").await.unwrap().id;
    assert!(install.sequence("b").await.is_none());
    assert!(!install.lockfile().exists());

    assert_success(&install.run().await);
    assert_eq!(install.sequence("a").await.unwrap().id, first_id);
    assert!(install.sequence("b").await.is_some());
    assert!(install.lockfile().exists());

    assert_success(&install.run().await);
    assert_eq!(install.sequence("a").await.unwrap().id, first_id);
}

#[tokio::test]
async fn retry_reconciles_a_committed_create_with_a_missing_response() {
    let install = Install::new(Some(Fault::LoseFirstCreateResponse)).await;
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("sequences/b.json", sequence("b", 2)),
    ]);
    let _ = install.run().await;
    let first_id = install.sequence("a").await.unwrap().id;

    assert_success(&install.run().await);
    assert_eq!(install.sequence("a").await.unwrap().id, first_id);
    assert!(install.sequence("b").await.is_some());
    assert!(install.lockfile().exists());
}

#[tokio::test]
async fn retry_refuses_a_changed_definition_even_with_the_original_id() {
    let install = Install::new(Some(Fault::RejectSecondCreate)).await;
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("sequences/b.json", sequence("b", 2)),
    ]);
    assert!(!install.run().await.status.success());
    let first_id = install.sequence("a").await.unwrap().id;
    install
        .server
        .storage
        .deprecate_sequence(first_id)
        .await
        .unwrap();

    assert!(!install.run().await.status.success());
    let existing = install.sequence("a").await.unwrap();
    assert_eq!(existing.id, first_id);
    assert!(existing.deprecated);
    assert!(install.sequence("b").await.is_none());
}

#[tokio::test]
async fn conflicts_are_checked_for_the_entire_package_before_creating_any_sequence() {
    let install = Install::new(None).await;
    let existing_id = uuid::Uuid::from_u128(2);
    let mut existing = sequence("b", 2);
    existing["namespace"] = json!("pkg.acme.billing");
    let create = reqwest::Client::new()
        .post(format!("{}/sequences", install.base))
        .json(&existing)
        .send()
        .await
        .unwrap();
    assert_eq!(create.status(), StatusCode::CREATED);
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("sequences/b.json", sequence("b", 2)),
    ]);

    assert!(!install.run().await.status.success());
    assert!(install.sequence("a").await.is_none());
    assert_eq!(
        install.sequence("b").await.unwrap().id.into_uuid(),
        existing_id
    );
    assert!(!install.lockfile().exists());
}

#[tokio::test]
async fn invalid_later_definition_is_rejected_before_creating_any_sequence() {
    let install = Install::new(None).await;
    let mut invalid = sequence("b", 2);
    invalid["input_schema"] = json!("malformed schema");
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("sequences/b.json", invalid),
    ]);

    assert!(!install.run().await.status.success());
    assert!(install.sequence("a").await.is_none());
}

fn parent() -> Value {
    let mut parent = sequence("parent", 2);
    parent["blocks"] =
        json!([{"type": "sub_sequence", "id": "call", "sequence_name": "child", "version": 1}]);
    parent
}

fn contract() -> Value {
    json!({"cases":[{"name":"runs the pinned child", "input":{},
        "mocks":[{"handler":"charge_card","type":"success","output":{"receipt":true}}],
        "expect":{"terminal_state":"completed","call_counts":[{"handler":"charge_card","min":1,"max":1}]}}]})
}

#[tokio::test]
async fn packaged_contract_executes_the_pinned_child() {
    let install = Install::new(None).await;
    install.package(&[
        ("sequences/parent.json", parent()),
        ("sequences/child.json", sequence("child", 1)),
        ("contracts/parent.contracts.json", contract()),
    ]);

    assert_success(&install.run().await);
    assert!(install.sequence("parent").await.is_some());
    assert!(install.sequence("child").await.is_some());
}

#[tokio::test]
async fn missing_packaged_child_blocks_installation() {
    let install = Install::new(None).await;
    install.package(&[
        ("sequences/parent.json", parent()),
        ("contracts/parent.contracts.json", contract()),
    ]);

    let output = install.run().await;
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("child"));
    assert!(install.sequence("parent").await.is_none());
}

#[tokio::test]
async fn orphaned_contract_blocks_installation() {
    let install = Install::new(None).await;
    install.package(&[
        ("sequences/a.json", sequence("a", 1)),
        ("contracts/absent.contracts.json", contract()),
    ]);

    assert!(!install.run().await.status.success());
    assert!(install.sequence("a").await.is_none());
}
