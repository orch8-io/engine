//! Harness for the cross-crate end-to-end suites.
//!
//! [`Cloud`] runs the real control plane in-process: the `orch8-api` router
//! with API-key auth enabled (root key + per-tenant keys), the real
//! scheduler (`orch8_engine::Engine`, including the worker-lease reaper),
//! and the gRPC service — all on loopback TCP ports, over `SQLite` or
//! Postgres. Every Postgres cloud gets its own schema, so suites never see
//! each other's rows and the scheduler never picks up foreign instances.
//!
//! [`Link`] is a loopback TCP proxy standing in for a device's network
//! path: taking it offline refuses new connections and severs open ones,
//! exactly what a phone losing connectivity looks like to the server.
//!
//! [`Phone`] wraps a real `orch8_mobile::MobileEngine` with its own `SQLite`
//! file; [`Desktop`] is a desktop-kind runtime node built from the embedded
//! `orch8` engine plus the lease protocol over HTTP.
#![allow(clippy::missing_panics_doc)]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use orch8_api::test_harness::{BackendTestServer, TestServerOptions, spawn_test_server_with};
use orch8_engine::handlers::HandlerRegistry;
use orch8_storage::StorageBackend;
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::EffectReceipt;
use orch8_types::filter::Pagination;
use orch8_types::ids::{InstanceId, TenantId};
use orch8_types::worker::{WorkerTask, WorkerTaskAttemptEvent};
use orch8_types::worker_filter::WorkerTaskFilter;
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Root API key of every [`Cloud`].
pub const ROOT_KEY: &str = "orch8-e2e-root-key-0123456789abcdef";

/// Storage backend a [`Cloud`] runs on.
#[derive(Debug, Clone)]
pub enum Backend {
    Sqlite,
    /// Postgres at this URL (an isolated schema is created per cloud).
    Postgres(String),
}

impl Backend {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres(_) => "postgres",
        }
    }
}

/// `SQLite` always; Postgres too when `DATABASE_URL` is set.
#[must_use]
pub fn backends() -> Vec<Backend> {
    let mut out = vec![Backend::Sqlite];
    match std::env::var("DATABASE_URL") {
        Ok(url) if !url.is_empty() => out.push(Backend::Postgres(url)),
        _ => eprintln!("DATABASE_URL not set: running the sqlite leg only"),
    }
    out
}

/// Poll `probe` every 25 ms until it yields `Some`, or panic after `limit`.
pub fn wait_for<T>(what: &str, limit: Duration, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(value) = probe() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {limit:?} waiting for {what}"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
}

enum RawPool {
    Sqlite(sqlx::SqlitePool),
    Postgres(sqlx::PgPool),
}

/// The in-process control plane (API + scheduler + reaper + gRPC).
pub struct Cloud {
    rt: tokio::runtime::Runtime,
    pub backend: &'static str,
    pub storage: Arc<dyn StorageBackend>,
    server: Option<BackendTestServer>,
    pub grpc_addr: SocketAddr,
    engine_cancel: CancellationToken,
    http: reqwest::Client,
    /// Tenant every helper acts in (unique per cloud).
    pub tenant: String,
    raw: RawPool,
    /// Isolated Postgres schema (dropped with the cloud).
    pg_schema: Option<(sqlx::PgPool, String)>,
}

/// Knobs of a [`Cloud`] beyond the defaults.
#[derive(Debug, Clone)]
pub struct CloudOptions {
    pub scheduler: orch8_types::config::SchedulerConfig,
    /// Authenticate gRPC exactly like `orch8-server` (root key + per-tenant
    /// keys). Off by default: older suites call gRPC without a key.
    pub grpc_auth: bool,
}

impl Default for CloudOptions {
    fn default() -> Self {
        Self {
            scheduler: orch8_types::config::SchedulerConfig {
                tick_interval_ms: 50,
                worker_reaper_tick_secs: 1,
                worker_reaper_stale_secs: 60,
                ..orch8_types::config::SchedulerConfig::default()
            },
            grpc_auth: false,
        }
    }
}

impl Cloud {
    /// Start a control plane whose scheduler serves the handlers `register`
    /// adds (plus every builtin).
    pub fn start(backend: &Backend, register: impl FnOnce(&mut HandlerRegistry)) -> Self {
        Self::start_with(backend, register, CloudOptions::default())
    }

    /// [`Self::start`] with explicit [`CloudOptions`].
    pub fn start_with(
        backend: &Backend,
        register: impl FnOnce(&mut HandlerRegistry),
        options: CloudOptions,
    ) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("cloud runtime");
        let (storage, raw, pg_schema) = rt.block_on(open_storage(backend));

        let mut handlers = HandlerRegistry::new();
        orch8_engine::handlers::builtin::register_builtins(&mut handlers);
        register(&mut handlers);

        let server = rt.block_on(spawn_test_server_with(
            Arc::clone(&storage),
            TestServerOptions {
                root_api_key: Some(ROOT_KEY.into()),
                mobile_sync_enabled: true,
                ..TestServerOptions::default()
            },
        ));

        let grpc_addr = rt.block_on(spawn_grpc(Arc::clone(&storage), options.grpc_auth));

        let engine_cancel = CancellationToken::new();
        let config = options.scheduler;
        let engine = orch8_engine::Engine::new(
            Arc::clone(&storage),
            config,
            handlers,
            engine_cancel.clone(),
        );
        rt.spawn(async move {
            if let Err(error) = engine.run().await {
                tracing::error!(%error, "e2e scheduler stopped");
            }
        });

        Self {
            rt,
            backend: backend.name(),
            storage,
            server: Some(server),
            grpc_addr,
            engine_cancel,
            http: reqwest::Client::new(),
            tenant: format!("e2e-{}", Uuid::now_v7().simple()),
            raw,
            pg_schema,
        }
    }

    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }

    #[must_use]
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    /// `http://127.0.0.1:<port>/api/v1`.
    #[must_use]
    pub fn v1(&self) -> String {
        self.server.as_ref().expect("server running").v1_url()
    }

    /// Socket address of the HTTP API.
    #[must_use]
    pub fn http_addr(&self) -> SocketAddr {
        self.server
            .as_ref()
            .expect("server running")
            .base_url
            .trim_start_matches("http://")
            .parse()
            .expect("loopback address")
    }

    /// Call the API with `key` (as `x-api-key`) in this cloud's tenant.
    pub fn call(&self, key: &str, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
        let url = format!("{}{}", self.v1(), path);
        self.rt.block_on(async {
            let request = match method {
                "GET" => self.http.get(&url),
                "DELETE" => self.http.delete(&url),
                "PUT" => self.http.put(&url),
                _ => self.http.post(&url),
            }
            .header("x-api-key", key)
            .header("X-Tenant-Id", &self.tenant);
            let request = match body {
                Some(body) => request.json(body),
                None => request,
            };
            let response = request.send().await.expect("request");
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            (
                status,
                serde_json::from_str(&text).unwrap_or(Value::String(text)),
            )
        })
    }

    /// [`Self::call`] with the root key.
    pub fn root(&self, method: &str, path: &str, body: Option<&Value>) -> (u16, Value) {
        self.call(ROOT_KEY, method, path, body)
    }

    /// Mint a tenant-scoped API key with these capabilities.
    pub fn mint_key(&self, capabilities: &[&str]) -> String {
        let (status, body) = self.root(
            "POST",
            "/api-keys",
            Some(&json!({
                "tenant_id": self.tenant,
                "name": "e2e",
                "capabilities": capabilities,
            })),
        );
        assert_eq!(status, 201, "mint key: {body}");
        body["secret"].as_str().expect("secret").to_owned()
    }

    /// Store a sequence of `blocks`; returns its id.
    pub fn create_sequence(&self, name: &str, blocks: &Value) -> Uuid {
        let id = Uuid::now_v7();
        let (status, body) = self.root(
            "POST",
            "/sequences",
            Some(&json!({
                "id": id, "tenant_id": self.tenant, "namespace": "e2e",
                "name": format!("{name}-{}", Uuid::now_v7().simple()), "version": 1,
                "deprecated": false, "blocks": blocks, "interceptors": null,
                "created_at": chrono::Utc::now().to_rfc3339(),
            })),
        );
        assert_eq!(status, 201, "create sequence: {body}");
        id
    }

    /// Start an instance of `sequence` with `data` as `context.data`.
    pub fn create_instance(&self, sequence: Uuid, data: &Value) -> Uuid {
        let (status, body) = self.root(
            "POST",
            "/instances",
            Some(&json!({
                "sequence_id": sequence, "tenant_id": self.tenant, "namespace": "e2e",
                "context": {"data": data, "config": {}, "audit": []},
            })),
        );
        assert_eq!(status, 201, "create instance: {body}");
        body["id"].as_str().expect("id").parse().expect("uuid")
    }

    pub fn instance(&self, id: Uuid) -> Value {
        let (status, body) = self.root("GET", &format!("/instances/{id}"), None);
        assert_eq!(status, 200, "get instance: {body}");
        body
    }

    /// Wait until the instance reaches `state`; returns its JSON.
    pub fn wait_state(&self, id: Uuid, state: &str, limit: Duration) -> Value {
        let mut last = Value::Null;
        let found = wait_for_opt(limit, || {
            last = self.instance(id);
            (last["state"] == state).then(|| last.clone())
        });
        found.unwrap_or_else(|| panic!("instance {id} never reached {state}; last: {last}"))
    }

    /// Block outputs via `GET /instances/{id}/outputs`, oldest first.
    pub fn outputs(&self, id: Uuid) -> Vec<Value> {
        let (status, body) = self.root("GET", &format!("/instances/{id}/outputs"), None);
        assert_eq!(status, 200, "outputs: {body}");
        let mut outputs = body
            .as_array()
            .cloned()
            .or_else(|| body["items"].as_array().cloned())
            .unwrap_or_default();
        outputs.sort_by(|a, b| {
            a["created_at"]
                .as_str()
                .unwrap_or_default()
                .cmp(b["created_at"].as_str().unwrap_or_default())
        });
        outputs
    }

    /// Outputs of one block, oldest first (the `__retry__` markers excluded).
    pub fn block_outputs(&self, id: Uuid, block: &str) -> Vec<Value> {
        self.outputs(id)
            .into_iter()
            .filter(|output| output["block_id"] == block && output["output_ref"] != "__retry__")
            .collect()
    }

    /// Every worker task of an instance (all states).
    pub fn tasks(&self, instance: Uuid) -> Vec<WorkerTask> {
        self.rt
            .block_on(self.storage.list_worker_tasks(
                &WorkerTaskFilter {
                    instance_id: Some(InstanceId::from_uuid(instance)),
                    ..WorkerTaskFilter::default()
                },
                &Pagination {
                    sort_ascending: true,
                    ..Pagination::default()
                },
            ))
            .expect("list worker tasks")
    }

    pub fn task(&self, id: Uuid) -> Option<WorkerTask> {
        self.rt
            .block_on(self.storage.get_worker_task(id))
            .expect("get worker task")
    }

    pub fn attempt_events(&self, task: Uuid) -> Vec<WorkerTaskAttemptEvent> {
        self.rt
            .block_on(self.storage.list_worker_task_attempt_events(task, 100))
            .expect("attempt events")
    }

    /// Effect receipts of an instance, oldest attempt first.
    pub fn receipts(&self, instance: Uuid) -> Vec<EffectReceipt> {
        let tenant = TenantId::unchecked(&self.tenant);
        let instance = InstanceId::from_uuid(instance);
        self.rt.block_on(async {
            let Some(execution) = self
                .storage
                .get_continuity_execution_touching_instance(&tenant, instance)
                .await
                .expect("continuity lookup")
            else {
                return Vec::new();
            };
            let mut receipts: Vec<_> = self
                .storage
                .list_effect_receipts(&tenant, execution.continuity_id, 10_000)
                .await
                .expect("receipts")
                .into_iter()
                .filter(|receipt| receipt.instance_id == instance)
                .collect();
            receipts.sort_by_key(|receipt| (receipt.block_id.as_str().to_owned(), receipt.attempt));
            receipts
        })
    }

    /// Receipts of one block.
    pub fn block_receipts(&self, instance: Uuid, block: &str) -> Vec<EffectReceipt> {
        self.receipts(instance)
            .into_iter()
            .filter(|receipt| receipt.block_id.as_str() == block)
            .collect()
    }

    /// Age a claimed task's lease by an hour (the device "stalled" past
    /// `lease_secs`); the real reaper then reclaims it on its next tick.
    pub fn expire_lease(&self, task: Uuid) {
        self.rt.block_on(async {
            match &self.raw {
                RawPool::Postgres(pool) => {
                    sqlx::query(
                        "UPDATE worker_tasks SET heartbeat_at = NOW() - INTERVAL '1 hour', \
                         claimed_at = NOW() - INTERVAL '1 hour' WHERE id = $1",
                    )
                    .bind(task)
                    .execute(pool)
                    .await
                    .expect("age lease");
                }
                RawPool::Sqlite(pool) => {
                    let past = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
                    sqlx::query(
                        "UPDATE worker_tasks SET heartbeat_at = ?1, claimed_at = ?1 WHERE id = ?2",
                    )
                    .bind(past)
                    .bind(task.to_string())
                    .execute(pool)
                    .await
                    .expect("age lease");
                }
            }
        });
    }
}

impl Cloud {
    /// Every table of this cloud's database whose rows contain `needle`
    /// anywhere (any text or binary column; Postgres rows are rendered as
    /// text). Used to prove a value never reached the control plane.
    pub fn tables_containing(&self, needle: &str) -> Vec<String> {
        use sqlx::Row as _;
        self.rt.block_on(async {
            let mut hits = Vec::new();
            match &self.raw {
                RawPool::Sqlite(pool) => {
                    let tables: Vec<String> =
                        sqlx::query_scalar("SELECT name FROM sqlite_master WHERE type = 'table'")
                            .fetch_all(pool)
                            .await
                            .expect("list tables");
                    for table in tables {
                        let rows = sqlx::query(&format!("SELECT * FROM \"{table}\""))
                            .fetch_all(pool)
                            .await
                            .expect("scan table");
                        let found = rows.iter().any(|row| {
                            (0..row.len()).any(|i| {
                                row.try_get::<Option<String>, _>(i)
                                    .ok()
                                    .flatten()
                                    .is_some_and(|text| text.contains(needle))
                                    || row
                                        .try_get::<Option<Vec<u8>>, _>(i)
                                        .ok()
                                        .flatten()
                                        .is_some_and(|bytes| {
                                            String::from_utf8_lossy(&bytes).contains(needle)
                                        })
                            })
                        });
                        if found {
                            hits.push(table);
                        }
                    }
                }
                RawPool::Postgres(pool) => {
                    let schema = self
                        .pg_schema
                        .as_ref()
                        .map(|(_, schema)| schema.clone())
                        .expect("postgres schema");
                    let tables: Vec<String> = sqlx::query_scalar(
                        "SELECT table_name::text FROM information_schema.tables \
                         WHERE table_schema = $1 AND table_type = 'BASE TABLE'",
                    )
                    .bind(&schema)
                    .fetch_all(pool)
                    .await
                    .expect("list tables");
                    for table in tables {
                        let count: i64 = sqlx::query_scalar(&format!(
                            "SELECT COUNT(*) FROM \"{schema}\".\"{table}\" t \
                             WHERE strpos(t::text, $1) > 0"
                        ))
                        .bind(needle)
                        .fetch_one(pool)
                        .await
                        .expect("scan table");
                        if count > 0 {
                            hits.push(table);
                        }
                    }
                }
            }
            hits
        })
    }

    /// Runtime capability advertisements of this cloud's tenant (including
    /// expired ones' latest state as long as they are listed).
    pub fn runtimes(&self) -> Vec<orch8_types::continuity::RuntimeCapabilities> {
        let tenant = TenantId::unchecked(&self.tenant);
        self.rt
            .block_on(self.storage.list_runtime_capabilities(
                &tenant,
                chrono::Utc::now() - chrono::Duration::hours(1),
                1_000,
            ))
            .expect("runtimes")
    }
}

// ---------------------------------------------------------------------------
// TlsFront: the managed engine's public HTTPS/gRPC endpoint
// ---------------------------------------------------------------------------

/// Directory of the committed test TLS material (a throwaway CA and a
/// `localhost` / `127.0.0.1` server certificate; not secret).
#[must_use]
pub fn tls_fixture(name: &str) -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tls")
        .join(name)
}

/// A TLS front end standing in for the managed engine's public endpoint:
/// one `https://127.0.0.1:<port>` that routes by ALPN — `h2` to the gRPC
/// service, anything else (HTTP/1.1) to the REST API — like an ingress that
/// serves both on one host name.
pub struct TlsFront {
    pub endpoint: String,
    stop: CancellationToken,
}

impl TlsFront {
    #[must_use]
    pub fn start(cloud: &Cloud) -> Self {
        use tokio_rustls::rustls;
        use tokio_rustls::rustls::pki_types::pem::PemObject as _;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(tls_fixture("server.pem"))
                .expect("server.pem")
                .collect::<Result<_, _>>()
                .expect("server certificate");
        let key = PrivateKeyDer::from_pem_file(tls_fixture("server.key")).expect("server.key");
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("tls versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("tls config");
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = cloud
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind tls front");
        let port = listener.local_addr().expect("tls addr").port();
        let (grpc, http) = (cloud.grpc_addr, cloud.http_addr());
        let stop = CancellationToken::new();
        let stop_task = stop.clone();
        cloud.handle().spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = stop_task.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((tcp, _)) = accepted else { continue };
                let acceptor = acceptor.clone();
                tokio::spawn(async move {
                    let Ok(mut tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let upstream = if tls.get_ref().1.alpn_protocol() == Some(b"h2") {
                        grpc
                    } else {
                        http
                    };
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };
                    let _ = tokio::io::copy_bidirectional(&mut tls, &mut outbound).await;
                });
            }
        });
        Self {
            endpoint: format!("https://127.0.0.1:{port}"),
            stop,
        }
    }
}

impl Drop for TlsFront {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

// ---------------------------------------------------------------------------
// RoutedFront: a shared load balancer that routes by a header
// ---------------------------------------------------------------------------

/// One request seen by a [`RoutedFront`].
#[derive(Debug, Clone)]
pub struct RoutedHit {
    /// `"grpc"` or `"rest"`.
    pub listener: &'static str,
    pub path: String,
    /// Whether it carried the routing header with this engine's id (and was
    /// forwarded); otherwise it was refused with 404.
    pub routed: bool,
}

/// A stand-in for a load balancer shared by many engines (Fly's shared app):
/// it terminates TLS, inspects HTTP, and forwards a request to this engine
/// only when it carries `<header>: <instance>`; anything else gets 404, as
/// if no engine matched. gRPC (`h2`, forwarded as h2c) and REST (HTTP/1.1)
/// listen on **separate ports**, so a token needs both `endpoint` (gRPC) and
/// `api_url` (REST).
pub struct RoutedFront {
    /// `https://127.0.0.1:<port>` serving gRPC (join token `endpoint`).
    pub grpc_endpoint: String,
    /// `https://127.0.0.1:<port>/api/v1` (join token `api_url`).
    pub api_url: String,
    hits: Arc<Mutex<Vec<RoutedHit>>>,
    stop: CancellationToken,
}

type FrontClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    hyper::body::Incoming,
>;

impl RoutedFront {
    #[must_use]
    pub fn start(cloud: &Cloud, header: &'static str, instance: &'static str) -> Self {
        let hits = Arc::new(Mutex::new(Vec::new()));
        let stop = CancellationToken::new();
        let grpc_port = Self::listen(
            cloud,
            "grpc",
            cloud.grpc_addr,
            true,
            (header, instance),
            Arc::clone(&hits),
            stop.clone(),
        );
        let rest_port = Self::listen(
            cloud,
            "rest",
            cloud.http_addr(),
            false,
            (header, instance),
            Arc::clone(&hits),
            stop.clone(),
        );
        Self {
            grpc_endpoint: format!("https://127.0.0.1:{grpc_port}"),
            api_url: format!("https://127.0.0.1:{rest_port}/api/v1"),
            hits,
            stop,
        }
    }

    /// Every request seen so far.
    #[must_use]
    pub fn hits(&self) -> Vec<RoutedHit> {
        self.hits.lock().expect("hits").clone()
    }

    fn tls_acceptor(alpn: &[u8]) -> tokio_rustls::TlsAcceptor {
        use tokio_rustls::rustls;
        use tokio_rustls::rustls::pki_types::pem::PemObject as _;
        use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

        let certs: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(tls_fixture("server.pem"))
                .expect("server.pem")
                .collect::<Result<_, _>>()
                .expect("server certificate");
        let key = PrivateKeyDer::from_pem_file(tls_fixture("server.key")).expect("server.key");
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("tls versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("tls config");
        config.alpn_protocols = vec![alpn.to_vec()];
        tokio_rustls::TlsAcceptor::from(Arc::new(config))
    }

    #[allow(clippy::too_many_arguments)]
    fn listen(
        cloud: &Cloud,
        name: &'static str,
        upstream: SocketAddr,
        h2: bool,
        route: (&'static str, &'static str),
        hits: Arc<Mutex<Vec<RoutedHit>>>,
        stop: CancellationToken,
    ) -> u16 {
        let acceptor = Self::tls_acceptor(if h2 { b"h2" } else { b"http/1.1" });
        let listener = cloud
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind routed front");
        let port = listener.local_addr().expect("front addr").port();
        let client: FrontClient = {
            let mut builder =
                hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new());
            builder.http2_only(h2);
            builder.build_http()
        };
        cloud.handle().spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = stop.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((tcp, _)) = accepted else { continue };
                let (acceptor, client, hits) =
                    (acceptor.clone(), client.clone(), Arc::clone(&hits));
                tokio::spawn(async move {
                    let Ok(tls) = acceptor.accept(tcp).await else {
                        return;
                    };
                    let service = hyper::service::service_fn(move |request| {
                        Self::forward(
                            name,
                            upstream,
                            route,
                            client.clone(),
                            Arc::clone(&hits),
                            request,
                        )
                    });
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection(hyper_util::rt::TokioIo::new(tls), service)
                    .await;
                });
            }
        });
        port
    }

    async fn forward(
        name: &'static str,
        upstream: SocketAddr,
        (header, instance): (&'static str, &'static str),
        client: FrontClient,
        hits: Arc<Mutex<Vec<RoutedHit>>>,
        mut request: hyper::Request<hyper::body::Incoming>,
    ) -> Result<hyper::Response<axum::body::Body>, std::convert::Infallible> {
        let routed = request
            .headers()
            .get(header)
            .and_then(|value| value.to_str().ok())
            == Some(instance);
        hits.lock().expect("hits").push(RoutedHit {
            listener: name,
            path: request.uri().path().to_owned(),
            routed,
        });
        let status = |code: u16| {
            hyper::Response::builder()
                .status(code)
                .body(axum::body::Body::empty())
                .expect("response")
        };
        if !routed {
            return Ok(status(404));
        }
        let path = request
            .uri()
            .path_and_query()
            .map_or("/", hyper::http::uri::PathAndQuery::as_str)
            .to_owned();
        let Ok(uri) = format!("http://{upstream}{path}").parse() else {
            return Ok(status(400));
        };
        *request.uri_mut() = uri;
        match client.request(request).await {
            Ok(response) => Ok(response.map(axum::body::Body::new)),
            Err(_) => Ok(status(502)),
        }
    }
}

impl Drop for RoutedFront {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

// ---------------------------------------------------------------------------
// ExecutorProcess: a real `orch8-server` remote executor (no database)
// ---------------------------------------------------------------------------

/// Path of the `orch8-server` binary: `ORCH8_SERVER_BIN`, else the binary
/// next to this test's profile directory (run `cargo build -p orch8-server`
/// first), else build it.
#[must_use]
pub fn server_binary() -> std::path::PathBuf {
    static BIN: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
    BIN.get_or_init(|| {
        if let Ok(path) = std::env::var("ORCH8_SERVER_BIN") {
            return path.into();
        }
        // <target>/<profile>/deps/<test binary> → <target>/<profile>/orch8-server
        let exe = std::env::current_exe().expect("test binary path");
        let profile_dir = exe
            .parent()
            .and_then(std::path::Path::parent)
            .expect("target profile dir");
        let bin = profile_dir.join("orch8-server");
        if !bin.exists() {
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
            let status = std::process::Command::new(cargo)
                .args(["build", "-q", "-p", "orch8-server", "--bin", "orch8-server"])
                .status()
                .expect("run cargo build");
            assert!(status.success(), "building orch8-server failed");
        }
        assert!(bin.exists(), "{} not found", bin.display());
        bin
    })
    .clone()
}

/// A remote executor process started from a join token, with no database.
pub struct ExecutorProcess {
    pub name: String,
    child: std::process::Child,
    pub log: std::path::PathBuf,
}

impl ExecutorProcess {
    /// `orch8-server` with only `ORCH8_JOIN_TOKEN` (+ `env`) set: no config
    /// file, no database, no API or encryption key.
    #[must_use]
    pub fn spawn(name: &str, token: &str, env: &[(&str, String)], dir: &std::path::Path) -> Self {
        let log = dir.join(format!("{name}.log"));
        let file = std::fs::File::create(&log).expect("executor log");
        let health = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .expect("free port");
        let mut command = std::process::Command::new(server_binary());
        command
            .args(["--config", dir.join("absent.toml").to_str().expect("utf-8")])
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOSTNAME", name)
            .env("ORCH8_JOIN_TOKEN", token)
            .env("ORCH8_HTTP_ADDR", health.to_string())
            .env("RUST_LOG", "info,orch8_engine=debug,orch8_grpc=debug")
            .stdin(std::process::Stdio::null())
            .stdout(file.try_clone().expect("log"))
            .stderr(file);
        for (key, value) in env {
            command.env(key, value);
        }
        let child = command.spawn().expect("spawn executor");
        Self {
            name: name.to_owned(),
            child,
            log,
        }
    }

    #[must_use]
    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// `SIGKILL`: the process vanishes without releasing anything.
    pub fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }

    /// Wait up to `limit` for the process to exit on its own.
    pub fn wait_exit(&mut self, limit: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + limit;
        loop {
            if let Ok(Some(status)) = self.child.try_wait() {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[must_use]
    pub fn log_text(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for ExecutorProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        if std::thread::panicking() {
            let log = self.log_text();
            let tail: Vec<&str> = log.lines().rev().take(80).collect();
            eprintln!("---- executor {} log (tail) ----", self.name);
            for line in tail.into_iter().rev() {
                eprintln!("{line}");
            }
        }
    }
}

impl Drop for Cloud {
    fn drop(&mut self) {
        self.engine_cancel.cancel();
        if let Some(server) = self.server.take() {
            server.shutdown.cancel();
            drop(server);
        }
        if let Some((admin, schema)) = self.pg_schema.take() {
            self.rt.block_on(async {
                // Let the scheduler release its connections first.
                tokio::time::sleep(Duration::from_millis(200)).await;
                let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"))
                    .execute(&admin)
                    .await;
            });
        }
    }
}

fn wait_for_opt<T>(limit: Duration, mut probe: impl FnMut() -> Option<T>) -> Option<T> {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(value) = probe() {
            return Some(value);
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

async fn open_storage(
    backend: &Backend,
) -> (
    Arc<dyn StorageBackend>,
    RawPool,
    Option<(sqlx::PgPool, String)>,
) {
    match backend {
        Backend::Sqlite => {
            let sqlite = SqliteStorage::in_memory().await.expect("sqlite");
            let raw = RawPool::Sqlite(sqlite.pool().clone());
            (Arc::new(sqlite), raw, None)
        }
        Backend::Postgres(url) => {
            let admin = sqlx::PgPool::connect(url).await.expect("connect postgres");
            let schema = format!("e2e_{}", Uuid::now_v7().simple());
            sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
                .execute(&admin)
                .await
                .expect("create schema");
            let pg = PostgresStorage::new(url, 16, Some(&schema))
                .await
                .expect("postgres storage");
            pg.run_migrations().await.expect("migrations");
            let raw = RawPool::Postgres(pg.pool().clone());
            (Arc::new(pg), raw, Some((admin, schema)))
        }
    }
}

async fn spawn_grpc(storage: Arc<dyn StorageBackend>, auth: bool) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind grpc");
    let addr = listener.local_addr().expect("grpc addr");
    let service = orch8_grpc::service::Orch8GrpcService::new(Arc::clone(&storage));
    let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
    if auth {
        let layer = orch8_grpc::auth::GrpcAuthLayer::new(
            storage,
            Some(orch8_types::auth::precompute_secret_digest(ROOT_KEY)),
            false,
        );
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .layer(layer)
                .add_service(orch8_grpc::Orch8ServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
    } else {
        tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(orch8_grpc::Orch8ServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await;
        });
    }
    addr
}

// ---------------------------------------------------------------------------
// Link: a device's network path
// ---------------------------------------------------------------------------

/// Loopback TCP proxy in front of the API. Offline: new connections are
/// dropped on accept and every open connection is severed.
pub struct Link {
    /// API base through the link (`http://127.0.0.1:<port>/api/v1`).
    pub base: String,
    online: Arc<AtomicBool>,
    generation: Arc<Mutex<CancellationToken>>,
    stop: CancellationToken,
}

impl Link {
    #[must_use]
    pub fn start(cloud: &Cloud) -> Self {
        let upstream = cloud.http_addr();
        let online = Arc::new(AtomicBool::new(true));
        let generation = Arc::new(Mutex::new(CancellationToken::new()));
        let stop = CancellationToken::new();
        let listener = cloud
            .block_on(tokio::net::TcpListener::bind("127.0.0.1:0"))
            .expect("bind link");
        let port = listener.local_addr().expect("link addr").port();
        let (online_task, generation_task, stop_task) =
            (Arc::clone(&online), Arc::clone(&generation), stop.clone());
        cloud.handle().spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = stop_task.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((mut inbound, _)) = accepted else {
                    continue;
                };
                if !online_task.load(Ordering::Acquire) {
                    drop(inbound);
                    continue;
                }
                let cut = generation_task
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone();
                tokio::spawn(async move {
                    let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await else {
                        return;
                    };
                    tokio::select! {
                        () = cut.cancelled() => {}
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                    }
                });
            }
        });
        Self {
            base: format!("http://127.0.0.1:{port}/api/v1"),
            online,
            generation,
            stop,
        }
    }

    /// Go offline (refuse and sever) or back online.
    pub fn set_online(&self, online: bool) {
        self.online.store(online, Ordering::Release);
        if !online {
            let mut generation = self
                .generation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            generation.cancel();
            *generation = CancellationToken::new();
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        self.stop.cancel();
        self.set_online(false);
    }
}

// ---------------------------------------------------------------------------
// Gate: pause a native handler at a precise point
// ---------------------------------------------------------------------------

/// A latch a native handler waits on; the test observes when the handler
/// reached it and decides when it may continue.
#[derive(Default)]
pub struct Gate {
    state: Mutex<GateState>,
    changed: Condvar,
}

#[derive(Default)]
struct GateState {
    open: bool,
    arrivals: u32,
}

impl Gate {
    #[must_use]
    pub fn open() -> Arc<Self> {
        let gate = Self::default();
        gate.state.lock().expect("gate").open = true;
        Arc::new(gate)
    }

    #[must_use]
    pub fn closed() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Called by the handler: record arrival, then block while closed.
    pub fn pass(&self) {
        let mut state = self.state.lock().expect("gate");
        state.arrivals += 1;
        self.changed.notify_all();
        while !state.open {
            state = self.changed.wait(state).expect("gate");
        }
    }

    pub fn release(&self) {
        self.state.lock().expect("gate").open = true;
        self.changed.notify_all();
    }

    /// Block until `count` handler calls reached the gate.
    pub fn wait_arrivals(&self, count: u32, limit: Duration) {
        let deadline = Instant::now() + limit;
        let mut state = self.state.lock().expect("gate");
        while state.arrivals < count {
            let left = deadline.saturating_duration_since(Instant::now());
            assert!(!left.is_zero(), "handler never reached the gate");
            state = self.changed.wait_timeout(state, left).expect("gate").0;
        }
    }
}

// ---------------------------------------------------------------------------
// Phone: a real MobileEngine runtime node
// ---------------------------------------------------------------------------

/// Every invocation of a phone's native handler: `(effect_id, attempt,
/// task_id, params)` as the handler saw them.
pub type Ledger = Arc<Mutex<Vec<Value>>>;

/// Native `StepHandler` that records its input, waits at `gate`, and signs.
pub struct SignHandler {
    pub ledger: Ledger,
    pub gate: Arc<Gate>,
}

impl orch8_mobile::StepHandler for SignHandler {
    fn execute(
        &self,
        _step_name: String,
        input: String,
    ) -> Result<String, orch8_mobile::HandlerError> {
        let params: Value = serde_json::from_str(&input).unwrap_or(Value::Null);
        self.ledger.lock().expect("ledger").push(params.clone());
        self.gate.pass();
        let effect_id = params["__orch8"]["effect_id"].as_str().unwrap_or("none");
        Ok(json!({
            "signature": format!("sig-{effect_id}"),
            "signed_effect_id": effect_id,
            "doc": params["doc"],
        })
        .to_string())
    }
}

/// A phone: its own `SQLite` file, device id and credential, reaching the
/// control plane through `link`.
pub struct Phone {
    pub engine: Arc<orch8_mobile::MobileEngine>,
    pub db_path: String,
    pub device_id: String,
    pub key: String,
    pub api_base: String,
}

impl Phone {
    /// Open (or reopen) the phone's engine and register `handler` under
    /// `name`. Does not join the mesh yet.
    pub fn open(
        db_path: &str,
        device_id: &str,
        key: &str,
        api_base: &str,
        name: &str,
        handler: Arc<dyn orch8_mobile::StepHandler>,
    ) -> Self {
        let engine = orch8_mobile::MobileEngine::new(
            db_path.to_owned(),
            orch8_mobile::MobileEngineConfig {
                device_id: device_id.to_owned(),
                sync_api_key: key.to_owned(),
                handler_timeout_ms: 60_000,
                ..orch8_mobile::MobileEngineConfig::default()
            },
        )
        .expect("mobile engine");
        engine
            .register_handler(name.to_owned(), handler)
            .expect("register handler");
        Self {
            engine,
            db_path: db_path.to_owned(),
            device_id: device_id.to_owned(),
            key: key.to_owned(),
            api_base: api_base.to_owned(),
        }
    }

    #[must_use]
    pub fn runtime_id(&self) -> String {
        self.engine.node_runtime_id().expect("runtime id")
    }

    /// `register_node` against the control plane, then `start_worker`.
    pub fn join(&self, idle_poll_ms: u64) {
        let registration = self
            .engine
            .register_node(orch8_mobile::NodeCapabilities {
                api_base_url: Some(self.api_base.clone()),
                platform: Some("ios".into()),
                ..orch8_mobile::NodeCapabilities::default()
            })
            .expect("register node");
        assert_eq!(registration.runtime_id, self.runtime_id());
        self.engine
            .start_worker(orch8_mobile::WorkerOptions {
                max_concurrent_tasks: 1,
                idle_poll_interval_ms: idle_poll_ms,
                version: Some("e2e".into()),
            })
            .expect("start worker");
    }
}

/// Number of ledger entries.
#[must_use]
pub fn ledger_len(ledger: &Ledger) -> usize {
    ledger.lock().expect("ledger").len()
}

/// Effect ids the phone handler saw, in call order.
#[must_use]
pub fn ledger_effects(ledger: &Ledger) -> Vec<String> {
    ledger
        .lock()
        .expect("ledger")
        .iter()
        .map(|params| {
            params["__orch8"]["effect_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned()
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Desktop: a desktop-kind runtime node (embedded engine + lease protocol)
// ---------------------------------------------------------------------------

/// A desktop runtime node: polls the server mailbox as kind `desktop`, runs
/// delegated sub-sequences on an embedded `orch8` engine (its own `SQLite`),
/// and reports through the fenced lease protocol.
pub struct Desktop {
    pub runtime_id: Uuid,
    key: String,
    http: reqwest::Client,
    pub engine: orch8::Engine,
    /// Sub-sequence runs executed locally (delegation id → local instance).
    pub runs: Mutex<Vec<(String, orch8_types::ids::InstanceId)>>,
}

/// Outcome of one lease call as the desktop saw it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Accepted,
    /// Network error (the link is down): retry later.
    Unreachable,
    Status(StatusCode),
}

impl Desktop {
    /// Build the node; `register` adds the handlers its local engine serves.
    pub async fn start(
        key: &str,
        db_path: &str,
        register: impl FnOnce(orch8::EngineBuilder) -> orch8::EngineBuilder,
    ) -> Self {
        let builder = orch8::Engine::builder().storage(orch8::Storage::sqlite(db_path));
        let engine = register(builder).build().await.expect("desktop engine");
        engine.start();
        Self {
            runtime_id: Uuid::now_v7(),
            key: key.to_owned(),
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("desktop client"),
            engine,
            runs: Mutex::new(Vec::new()),
        }
    }

    /// Capability advertisement (kind `desktop`, serves `orch8.delegation`
    /// and the sub-sequence's handlers).
    #[must_use]
    pub fn capabilities(&self, handlers: &[&str]) -> Value {
        let now = chrono::Utc::now();
        let mut all: Vec<&str> = vec![orch8_engine::delegation::DELEGATION_HANDLER];
        all.extend_from_slice(handlers);
        json!({
            "runtime_id": self.runtime_id, "kind": "desktop", "trust": "registered",
            "handlers": all, "offline_capable": true, "connectivity": "ethernet",
            "observed_at": now.to_rfc3339(),
            "expires_at": (now + chrono::Duration::seconds(240)).to_rfc3339(),
        })
    }

    async fn post(&self, base: &str, path: &str, body: &Value) -> Result<(StatusCode, Value), ()> {
        let response = self
            .http
            .post(format!("{base}{path}"))
            .header("x-api-key", &self.key)
            .json(body)
            .send()
            .await
            .map_err(|_| ())?;
        let status = response.status();
        let body = response.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// `GET` through `base` with the desktop's credential.
    pub async fn get(&self, base: &str, path: &str) -> Result<(StatusCode, Value), ()> {
        let response = self
            .http
            .get(format!("{base}{path}"))
            .header("x-api-key", &self.key)
            .send()
            .await
            .map_err(|_| ())?;
        let status = response.status();
        let body = response.json().await.unwrap_or(Value::Null);
        Ok((status, body))
    }

    /// `POST /runtimes/register` through `base`.
    pub async fn register(&self, base: &str, tenant: &str, handlers: &[&str]) -> Delivery {
        match self
            .post(
                base,
                "/runtimes/register",
                &json!({"tenant_id": tenant, "capabilities": self.capabilities(handlers)}),
            )
            .await
        {
            Ok((status, _)) if status.is_success() => Delivery::Accepted,
            Ok((status, _)) => Delivery::Status(status),
            Err(()) => Delivery::Unreachable,
        }
    }

    /// Poll the mailbox once; `Ok(tasks)` or the delivery failure.
    pub async fn poll(&self, base: &str, handlers: &[&str]) -> Result<Vec<Value>, Delivery> {
        match self
            .post(
                base,
                "/workers/tasks/poll",
                &json!({
                    "handler_name": orch8_engine::delegation::DELEGATION_HANDLER,
                    "worker_id": self.runtime_id.to_string(),
                    "limit": 1,
                    "capabilities": self.capabilities(handlers),
                }),
            )
            .await
        {
            Ok((status, body)) if status.is_success() => {
                Ok(body["tasks"].as_array().cloned().unwrap_or_default())
            }
            Ok((status, _)) => Err(Delivery::Status(status)),
            Err(()) => Err(Delivery::Unreachable),
        }
    }

    /// Heartbeat / complete / fail for a claimed task.
    pub async fn lease_call(&self, base: &str, task: &Value, verb: &str, extra: Value) -> Delivery {
        let mut body = json!({
            "worker_id": self.runtime_id.to_string(),
            "claim_epoch": task["claim_epoch"],
        });
        if let (Some(target), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
            target.extend(extra.clone());
        }
        let id = task["id"].as_str().unwrap_or_default();
        match self
            .post(base, &format!("/workers/tasks/{id}/{verb}"), &body)
            .await
        {
            Ok((status, _)) if status.is_success() => Delivery::Accepted,
            Ok((status, _)) => Delivery::Status(status),
            Err(()) => Delivery::Unreachable,
        }
    }

    /// Run a delegated sub-sequence (its definition, fetched earlier)
    /// locally with the delegation's explicit input, and wait for a terminal
    /// state. Needs no network. Returns the output to report.
    pub async fn run_delegation(&self, sequence: Value, task: &Value) -> Value {
        let definition: orch8_types::sequence::SequenceDefinition =
            serde_json::from_value(sequence).expect("sub-sequence");
        let sequence_id = self
            .engine
            .upsert_sequence(definition)
            .await
            .expect("upsert sub-sequence");
        let input = task["params"]["input"].clone();
        let local = self
            .engine
            .create_instance(
                sequence_id,
                orch8::CreateInstanceOptions {
                    context: orch8::ExecutionContext {
                        data: input,
                        ..orch8::ExecutionContext::default()
                    },
                    ..orch8::CreateInstanceOptions::default()
                },
            )
            .await
            .expect("start sub-sequence");
        self.runs.lock().expect("runs").push((
            task["params"]["delegation_id"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
            local,
        ));
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let instance = self
                .engine
                .get_instance(local)
                .await
                .expect("local instance");
            if instance.state.is_terminal() {
                let outputs: serde_json::Map<String, Value> = self
                    .engine
                    .block_outputs(local)
                    .await
                    .expect("local outputs")
                    .into_iter()
                    .map(|output| (output.block_id.as_str().to_owned(), output.output))
                    .collect();
                return json!({
                    "local_instance_id": local,
                    "state": instance.state.to_string(),
                    "outputs": outputs,
                });
            }
            assert!(Instant::now() < deadline, "delegated run never finished");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
