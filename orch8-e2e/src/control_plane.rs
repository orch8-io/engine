//! Real `orch8-server` control-plane **processes** on a shared database
//! (T3-HAR-01/02/03, T1 HX-6b / HX-7): crash with `SIGKILL` and restart on
//! the same database, or run two schedulers against one database.
//!
//! ```ignore
//! let plane = ControlPlane::new(&backend);          // fresh SQLite file / PG schema
//! let mut a = plane.spawn("node-a", &[("ORCH8_STALE_INSTANCE_THRESHOLD_SECS", "3".into())]);
//! let id = plane.create_instance(&a, plane.create_sequence(&a, "s", &blocks), &json!({}));
//! a.kill();                                         // SIGKILL: no drain, no lease release
//! a.restart();                                      // same port, same database, same env
//! plane.wait_state(&a, id, "completed", LONG);
//! let b = plane.spawn("node-b", &[]);               // a second scheduler on the same DB
//! ```
//!
//! Unlike [`crate::Cloud`] (everything in this process), each node here is
//! the release-shaped binary with its own runtime, pools and leases, so a
//! kill really loses in-memory state. The test talks to nodes only over
//! HTTP. Every node gets `ORCH8_API_KEY` = [`crate::ROOT_KEY`], an encryption
//! key, and short engine ticks; per-test env overrides are applied last.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sqlx::AssertSqlSafe;
use uuid::Uuid;

use crate::{Backend, ROOT_KEY, server_binary};

/// Encryption key every control-plane node shares (they must agree to read
/// each other's rows).
pub const NODE_ENCRYPTION_KEY: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// How long [`ControlPlane::spawn`] waits for `/health/ready`.
pub const READY_TIMEOUT: Duration = Duration::from_secs(60);

enum Db {
    Sqlite {
        url: String,
        _dir: tempfile::TempDir,
    },
    Postgres {
        url: String,
        schema: String,
    },
}

/// One database shared by every node spawned from it, plus an HTTP client
/// and a tenant. Dropping it drops the Postgres schema / `SQLite` directory
/// (kill the nodes first; [`ServerProcess`] does on drop).
pub struct ControlPlane {
    db: Db,
    rt: tokio::runtime::Runtime,
    http: reqwest::Client,
    /// Tenant every helper acts in (unique per plane).
    pub tenant: String,
    pub backend: &'static str,
    logs: tempfile::TempDir,
}

impl ControlPlane {
    /// A fresh database: a `SQLite` file in a temp dir, or a new schema in
    /// the Postgres database at the backend's URL.
    #[must_use]
    pub fn new(backend: &Backend) -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("control-plane runtime");
        let db = match backend {
            Backend::Sqlite => {
                let dir = tempfile::tempdir().expect("sqlite dir");
                // `SqliteStorage::file` takes a path (it prefixes `sqlite:`).
                let path = dir.path().join("control-plane.db");
                Db::Sqlite {
                    url: path.display().to_string(),
                    _dir: dir,
                }
            }
            Backend::Postgres(url) => {
                let schema = format!("cp_{}", Uuid::now_v7().simple());
                rt.block_on(async {
                    let admin = sqlx::PgPool::connect(url).await.expect("connect postgres");
                    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA \"{schema}\"")))
                        .execute(&admin)
                        .await
                        .expect("create schema");
                    admin.close().await;
                });
                Db::Postgres {
                    url: url.clone(),
                    schema,
                }
            }
        };
        Self {
            db,
            rt,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("client"),
            tenant: format!("cp-{}", Uuid::now_v7().simple()),
            backend: backend.name(),
            logs: tempfile::tempdir().expect("log dir"),
        }
    }

    /// Database env every node of this plane gets.
    fn db_env(&self) -> Vec<(&'static str, String)> {
        match &self.db {
            Db::Sqlite { url, .. } => vec![
                ("ORCH8_STORAGE_BACKEND", "sqlite".into()),
                ("ORCH8_DATABASE_URL", url.clone()),
            ],
            Db::Postgres { url, schema } => vec![
                ("ORCH8_STORAGE_BACKEND", "postgres".into()),
                ("ORCH8_DATABASE_URL", url.clone()),
                ("ORCH8_DATABASE_SEARCH_PATH", schema.clone()),
            ],
        }
    }

    /// Start a node named `name` (also its `HOSTNAME`, i.e. its cluster node
    /// name) and wait until it is ready. `env` overrides the defaults.
    #[must_use]
    pub fn spawn(&self, name: &str, env: &[(&str, String)]) -> ServerProcess {
        self.spawn_at(name, env, free_addrs())
    }

    fn spawn_at(
        &self,
        name: &str,
        env: &[(&str, String)],
        addrs: (SocketAddr, SocketAddr),
    ) -> ServerProcess {
        let mut all: Vec<(String, String)> = vec![
            ("ORCH8_API_KEY".into(), ROOT_KEY.into()),
            ("ORCH8_ENCRYPTION_KEY".into(), NODE_ENCRYPTION_KEY.into()),
            ("ORCH8_RUN_MIGRATIONS".into(), "true".into()),
            ("ORCH8_TICK_INTERVAL_MS".into(), "50".into()),
            ("ORCH8_WORKER_REAPER_TICK_SECS".into(), "1".into()),
            ("ORCH8_NODE_REAPER_TICK_SECS".into(), "1".into()),
            ("ORCH8_CRON_TICK_SECS".into(), "1".into()),
            ("ORCH8_LOG_LEVEL".into(), "info".into()),
        ];
        all.extend(self.db_env().into_iter().map(|(k, v)| (k.to_owned(), v)));
        all.extend(env.iter().map(|(k, v)| ((*k).to_owned(), v.clone())));
        let mut process = ServerProcess::spawn(name, all, self.logs.path().to_path_buf(), addrs);
        process.wait_initial_ready(&self.rt, &self.http);
        process
    }

    /// Run `future` on this plane's runtime.
    pub fn block_on<F: std::future::Future>(&self, future: F) -> F::Output {
        self.rt.block_on(future)
    }

    /// Call `node`'s API with the root key in this plane's tenant. A
    /// connection error (node down) is status `0`.
    pub fn call(
        &self,
        node: &ServerProcess,
        method: &str,
        path: &str,
        body: Option<&Value>,
    ) -> (u16, Value) {
        let url = format!("{}{path}", node.v1());
        self.rt.block_on(async {
            let request = match method {
                "GET" => self.http.get(&url),
                "DELETE" => self.http.delete(&url),
                "PUT" => self.http.put(&url),
                _ => self.http.post(&url),
            }
            .header("x-api-key", ROOT_KEY)
            .header("X-Tenant-Id", &self.tenant);
            let request = match body {
                Some(body) => request.json(body),
                None => request,
            };
            let Ok(response) = request.send().await else {
                return (0, Value::Null);
            };
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            (
                status,
                serde_json::from_str(&text).unwrap_or(Value::String(text)),
            )
        })
    }

    /// Store a sequence of `blocks` through `node`; returns its id.
    pub fn create_sequence(&self, node: &ServerProcess, name: &str, blocks: &Value) -> Uuid {
        let id = Uuid::now_v7();
        let (status, body) = self.call(
            node,
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

    /// Start an instance of `sequence` through `node`.
    pub fn create_instance(&self, node: &ServerProcess, sequence: Uuid, data: &Value) -> Uuid {
        let (status, body) = self.call(
            node,
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

    pub fn instance(&self, node: &ServerProcess, id: Uuid) -> Value {
        let (status, body) = self.call(node, "GET", &format!("/instances/{id}"), None);
        assert_eq!(status, 200, "get instance: {body}");
        body
    }

    /// Poll until instance `id` is in `state`; panics with the last row.
    pub fn wait_state(
        &self,
        node: &ServerProcess,
        id: Uuid,
        state: &str,
        limit: Duration,
    ) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let last = self.instance(node, id);
            if last["state"] == state {
                return last;
            }
            assert!(
                Instant::now() < deadline,
                "instance {id} never reached {state} on {}; last: {last}",
                node.name
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Outputs of `block` (retry markers excluded).
    pub fn block_outputs(&self, node: &ServerProcess, id: Uuid, block: &str) -> Vec<Value> {
        let (status, body) = self.call(node, "GET", &format!("/instances/{id}/outputs"), None);
        assert_eq!(status, 200, "outputs: {body}");
        body.as_array()
            .cloned()
            .or_else(|| body["items"].as_array().cloned())
            .unwrap_or_default()
            .into_iter()
            .filter(|o| o["block_id"] == block && o["output_ref"] != "__retry__")
            .collect()
    }

    /// `POST /workers/tasks/poll` for `handler` as `worker`.
    pub fn poll_tasks(&self, node: &ServerProcess, handler: &str, worker: &str) -> Vec<Value> {
        let (status, body) = self.call(
            node,
            "POST",
            "/workers/tasks/poll",
            Some(&json!({"handler_name": handler, "worker_id": worker, "limit": 10})),
        );
        assert_eq!(status, 200, "poll: {body}");
        body["tasks"].as_array().cloned().unwrap_or_default()
    }

    /// Complete a claimed `task` as `worker` with `output`; returns the status.
    pub fn complete_task(
        &self,
        node: &ServerProcess,
        task: &Value,
        worker: &str,
        output: &Value,
    ) -> u16 {
        let id = task["id"].as_str().expect("task id");
        self.call(
            node,
            "POST",
            &format!("/workers/tasks/{id}/complete"),
            Some(
                &json!({"worker_id": worker, "claim_epoch": task["claim_epoch"], "output": output}),
            ),
        )
        .0
    }
}

impl Drop for ControlPlane {
    fn drop(&mut self) {
        if let Db::Postgres { url, schema } = &self.db {
            let (url, schema) = (url.clone(), schema.clone());
            self.rt.block_on(async move {
                if let Ok(admin) = sqlx::PgPool::connect(&url).await {
                    let _ = sqlx::query(AssertSqlSafe(format!(
                        "DROP SCHEMA IF EXISTS \"{schema}\" CASCADE"
                    )))
                    .execute(&admin)
                    .await;
                    admin.close().await;
                }
            });
        }
    }
}

/// A running `orch8-server` control-plane process. See the module docs.
pub struct ServerProcess {
    pub name: String,
    pub http_addr: SocketAddr,
    pub grpc_addr: SocketAddr,
    env: Vec<(String, String)>,
    log_dir: PathBuf,
    child: Option<std::process::Child>,
    /// Incremented per (re)start, so each run logs to its own file.
    generation: u32,
}

// Hold both reservations together so HTTP and gRPC cannot select the same port.
// Another process can still win after these listeners drop; initial startup retries
// only that explicit bind failure, before callers have seen either address.
fn free_addrs() -> (SocketAddr, SocketAddr) {
    let http = std::net::TcpListener::bind("127.0.0.1:0").expect("HTTP reservation");
    let grpc = std::net::TcpListener::bind("127.0.0.1:0").expect("gRPC reservation");
    (
        http.local_addr().expect("HTTP address"),
        grpc.local_addr().expect("gRPC address"),
    )
}

impl ServerProcess {
    fn spawn(
        name: &str,
        env: Vec<(String, String)>,
        log_dir: PathBuf,
        addrs: (SocketAddr, SocketAddr),
    ) -> Self {
        let mut process = Self {
            name: name.to_owned(),
            http_addr: addrs.0,
            grpc_addr: addrs.1,
            env,
            log_dir,
            child: None,
            generation: 0,
        };
        process.start();
        process
    }

    fn wait_initial_ready(&mut self, rt: &tokio::runtime::Runtime, http: &reqwest::Client) {
        for attempt in 0..4 {
            match self.wait_ready(rt, http) {
                Ok(()) => return,
                Err(error) => {
                    let log = std::fs::read_to_string(self.log_path()).unwrap_or_default();
                    let collision = log.contains("Address already in use")
                        && (log.contains("Failed to bind HTTP listener")
                            || log.contains("Failed to bind gRPC listener"));
                    assert!(attempt < 3 && collision, "{error}");
                    self.kill();
                    (self.http_addr, self.grpc_addr) = free_addrs();
                    self.start();
                }
            }
        }
    }

    fn start(&mut self) {
        self.generation += 1;
        let file = std::fs::File::create(self.log_path()).expect("node log");
        let mut command = std::process::Command::new(server_binary());
        command
            .args([
                "--config",
                self.log_dir.join("absent.toml").to_str().expect("utf-8"),
            ])
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .env("HOSTNAME", &self.name)
            .env("ORCH8_HTTP_ADDR", self.http_addr.to_string())
            .env("ORCH8_GRPC_ADDR", self.grpc_addr.to_string())
            .stdin(std::process::Stdio::null())
            .stdout(file.try_clone().expect("log"))
            .stderr(file);
        // Under cargo-llvm-cov, let the server write its own profile too.
        if let Ok(profile) = std::env::var("LLVM_PROFILE_FILE") {
            command.env("LLVM_PROFILE_FILE", profile);
        }
        for (key, value) in &self.env {
            command.env(key, value);
        }
        self.child = Some(command.spawn().expect("spawn orch8-server"));
    }

    fn log_path(&self) -> PathBuf {
        self.log_dir
            .join(format!("{}.{}.log", self.name, self.generation))
    }

    /// `http://127.0.0.1:<port>/api/v1`.
    #[must_use]
    pub fn v1(&self) -> String {
        format!("http://{}/api/v1", self.http_addr)
    }

    /// OS pid of the current run.
    #[must_use]
    pub fn pid(&self) -> Option<u32> {
        self.child.as_ref().map(std::process::Child::id)
    }

    fn wait_ready(
        &mut self,
        rt: &tokio::runtime::Runtime,
        http: &reqwest::Client,
    ) -> Result<(), String> {
        let url = format!("http://{}/health/ready", self.http_addr);
        let bound_marker = format!("Health endpoints: http://{}/health/live", self.http_addr);
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .expect("running child")
                .try_wait()
                .expect("child status")
            {
                return Err(format!(
                    "node {} exited {status} before readiness; log tail:\n{}",
                    self.name,
                    self.log_tail(60)
                ));
            }
            // A foreign server can answer 200 on a stolen port. Only probe after
            // this generation has logged its own successful HTTP bind.
            let bound = std::fs::read_to_string(self.log_path())
                .unwrap_or_default()
                .contains(&bound_marker);
            let ready = bound
                && rt.block_on(async {
                    http.get(&url)
                        .send()
                        .await
                        .is_ok_and(|r| r.status().is_success())
                });
            if ready
                && self
                    .child
                    .as_mut()
                    .expect("running child")
                    .try_wait()
                    .expect("child status")
                    .is_none()
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "node {} not ready after {READY_TIMEOUT:?}; log tail:\n{}",
                    self.name,
                    self.log_tail(60)
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// `SIGKILL` the process and reap it: no graceful drain, no lease
    /// release, nothing flushed — a crash.
    pub fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Start again (same name, ports, env and database) and wait until
    /// ready. Call [`Self::kill`] first for a crash-restart; this kills a
    /// still-running process the same way.
    pub fn restart(&mut self) {
        self.kill();
        self.start();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        let http = reqwest::Client::new();
        self.wait_ready(&rt, &http)
            .unwrap_or_else(|error| panic!("{error}"));
    }

    /// Last `lines` lines of the current run's log.
    #[must_use]
    pub fn log_tail(&self, lines: usize) -> String {
        let text = std::fs::read_to_string(self.log_path()).unwrap_or_default();
        let tail: Vec<&str> = text.lines().rev().take(lines).collect();
        tail.into_iter().rev().collect::<Vec<_>>().join("\n")
    }

    /// The whole log of every run of this node, oldest run first.
    #[must_use]
    pub fn all_logs(&self) -> String {
        (1..=self.generation)
            .map(|g| {
                std::fs::read_to_string(self.log_dir.join(format!("{}.{g}.log", self.name)))
                    .unwrap_or_default()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        self.kill();
        if std::thread::panicking() {
            eprintln!("---- node {} log (tail) ----", self.name);
            eprintln!("{}", self.log_tail(80));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_engine::testing::http::MockHttp;

    #[test]
    fn initial_bind_collision_never_accepts_foreign_health() {
        for listener in ["HTTP", "gRPC"] {
            let plane = ControlPlane::new(&Backend::Sqlite);
            let foreign = plane.block_on(MockHttp::start(Vec::new()));
            let mut addrs = free_addrs();
            if listener == "HTTP" {
                addrs.0 = foreign.addr();
            } else {
                addrs.1 = foreign.addr();
            }
            let node = plane.spawn_at("occupied-port", &[], addrs);
            assert_ne!(node.http_addr, foreign.addr());
            assert_ne!(node.grpc_addr, foreign.addr());
            assert!((2..=4).contains(&node.generation));
            assert_eq!(
                foreign.request_count(),
                0,
                "foreign health must never be probed"
            );
            assert!(
                node.all_logs()
                    .contains(&format!("Failed to bind {listener} listener"))
            );
            assert_eq!(plane.call(&node, "GET", "/sequences", None).0, 200);
        }
    }
}
