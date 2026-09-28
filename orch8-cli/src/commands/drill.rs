//! `orch8 drill kill-executor`: a local, measured failure drill.
//!
//! Topology (all on loopback, isolated temp directory):
//! - **control node** (this process): `SQLite` storage, full HTTP API, the
//!   engine scheduler and lease reaper, receipt signing key;
//! - **two executor processes** (`orch8 drill executor-process`, separate OS
//!   processes) that serve the workload's side-effecting handlers over the
//!   external worker protocol (poll, heartbeat, complete) — the same
//!   transport a hybrid executor's workers use;
//! - **a provider** (loopback HTTP) standing in for the external system: it
//!   records every delivery and dedupes on the idempotency key the engine
//!   carries in each receipt.
//!
//! The drill SIGKILLs one executor while it holds claimed tasks, then lets
//! the real lease reaper and retry policy recover. Everything reported is
//! measured from the ledger, the provider log, and wall clocks; the command
//! exits non-zero when an invariant fails.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result, bail};
use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::continuity::{EffectReceipt, EffectState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::TenantId;
use orch8_types::instance::InstanceState;
use orch8_types::worker::WorkerTaskState;
use orch8_types::worker_filter::WorkerTaskFilter;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::OutputFormat;

const TENANT: &str = "drill";
const HANDLERS: [&str; 2] = ["drill_charge", "drill_ship"];
const LEASE_SECS: u64 = 3;
const REAPER_TICK_SECS: u64 = 1;

#[derive(Debug, clap::Subcommand)]
pub enum DrillCmd {
    /// Kill an executor mid-flight and prove recovery from the effect ledger.
    KillExecutor(KillExecutorArgs),
    /// Internal: one executor process of the drill.
    #[command(hide = true)]
    ExecutorProcess(ExecutorProcessArgs),
}

#[derive(Debug, clap::Args)]
pub struct KillExecutorArgs {
    /// Workflow instances in the workload (2 side-effecting steps each).
    #[arg(long, default_value_t = 24, value_parser = clap::value_parser!(u32).range(2..=500))]
    pub instances: u32,
    /// Simulated provider latency per step (ms); the kill lands inside it.
    #[arg(long, default_value_t = 400)]
    pub work_ms: u64,
    /// Give up (and fail) if the workload has not finished by then.
    #[arg(long, default_value_t = 120)]
    pub timeout_secs: u64,
    /// Keep the drill directory (databases, logs, receipt bundle).
    #[arg(long)]
    pub keep: bool,
}

#[derive(Debug, clap::Args)]
pub struct ExecutorProcessArgs {
    #[arg(long)]
    pub api: String,
    #[arg(long)]
    pub provider: String,
    #[arg(long)]
    pub worker_id: String,
    #[arg(long, default_value_t = 4)]
    pub concurrency: usize,
    #[arg(long, default_value_t = 400)]
    pub work_ms: u64,
}

pub async fn run(cmd: DrillCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        DrillCmd::ExecutorProcess(args) => executor_process(args).await,
        DrillCmd::KillExecutor(args) => {
            let report = kill_executor(&args).await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => print_report(&report),
            }
            if !report.passed {
                bail!("drill FAILED: at least one invariant does not hold");
            }
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Provider (the "external system")
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Delivery {
    idempotency_key: String,
    instance_id: String,
    block_id: String,
    attempt: u32,
    task_id: String,
    worker_id: String,
    #[serde(default)]
    duplicate: bool,
    #[serde(default = "Utc::now")]
    at: DateTime<Utc>,
}

#[derive(Default)]
struct Provider {
    deliveries: Vec<Delivery>,
    applied: BTreeMap<String, String>,
}

type SharedProvider = Arc<Mutex<Provider>>;

async fn provider_deliver(
    State(provider): State<SharedProvider>,
    Json(mut delivery): Json<Delivery>,
) -> Json<Value> {
    let mut provider = provider
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    delivery.at = Utc::now();
    let receipt = if let Some(existing) = provider.applied.get(&delivery.idempotency_key) {
        delivery.duplicate = true;
        existing.clone()
    } else {
        let receipt = format!("pr_{}", provider.applied.len() + 1);
        provider
            .applied
            .insert(delivery.idempotency_key.clone(), receipt.clone());
        receipt
    };
    let duplicate = delivery.duplicate;
    provider.deliveries.push(delivery);
    Json(json!({"provider_receipt_id": receipt, "deduplicated": duplicate}))
}

// ---------------------------------------------------------------------------
// Executor process (child)
// ---------------------------------------------------------------------------

async fn executor_process(args: ExecutorProcessArgs) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let slots = Arc::new(tokio::sync::Semaphore::new(args.concurrency.max(1)));
    let args = Arc::new(args);
    loop {
        let mut claimed_any = false;
        for handler in HANDLERS {
            let free = slots.available_permits();
            if free == 0 {
                break;
            }
            let response = client
                .post(format!("{}/workers/tasks/poll", args.api))
                .header("x-tenant-id", TENANT)
                .json(&json!({"handler_name": handler, "worker_id": args.worker_id, "limit": free}))
                .send()
                .await;
            let Ok(response) = response else { continue };
            let Ok(body) = response.json::<Value>().await else {
                continue;
            };
            for task in body["tasks"].as_array().cloned().unwrap_or_default() {
                claimed_any = true;
                let Ok(permit) = Arc::clone(&slots).acquire_owned().await else {
                    continue;
                };
                let client = client.clone();
                let args = Arc::clone(&args);
                tokio::spawn(async move {
                    let _permit = permit;
                    let _ = run_task(&client, &args, &task).await;
                });
            }
        }
        if !claimed_any {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

async fn run_task(
    client: &reqwest::Client,
    args: &ExecutorProcessArgs,
    task: &Value,
) -> Result<()> {
    let task_id = task["id"].as_str().context("task id")?.to_owned();
    let claim_epoch = task["claim_epoch"].as_u64().unwrap_or(0);
    let heartbeat = {
        let client = client.clone();
        let url = format!("{}/workers/tasks/{task_id}/heartbeat", args.api);
        let worker_id = args.worker_id.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(800)).await;
                let _ = client
                    .post(&url)
                    .header("x-tenant-id", TENANT)
                    .json(&json!({"worker_id": worker_id, "claim_epoch": claim_epoch}))
                    .send()
                    .await;
            }
        })
    };
    tokio::time::sleep(Duration::from_millis(args.work_ms / 2)).await;
    let provider_reply: Value = client
        .post(&args.provider)
        .json(&json!({
            "idempotency_key": task["params"]["idempotency_key"].as_str().unwrap_or_default(),
            "instance_id": task["instance_id"],
            "block_id": task["block_id"],
            "attempt": task["attempt"],
            "task_id": task_id,
            "worker_id": args.worker_id,
        }))
        .send()
        .await?
        .json()
        .await?;
    tokio::time::sleep(Duration::from_millis(args.work_ms / 2)).await;
    let result = client
        .post(format!("{}/workers/tasks/{task_id}/complete", args.api))
        .header("x-tenant-id", TENANT)
        .json(&json!({
            "worker_id": args.worker_id,
            "claim_epoch": claim_epoch,
            "output": provider_reply,
        }))
        .send()
        .await;
    heartbeat.abort();
    result?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Control node + orchestration (parent)
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Invariant {
    pub name: &'static str,
    pub passed: bool,
    pub detail: String,
}

#[derive(Debug, Default, Serialize)]
pub struct LedgerSummary {
    pub receipts: usize,
    pub by_state_before_reconcile: BTreeMap<String, usize>,
    pub ambiguous_attempts: usize,
    pub reconciled_verified: usize,
    pub reconciled_abandoned: usize,
    pub duplicate_attempt_receipts: usize,
    pub steps_with_multiple_committed: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct ProviderSummary {
    pub deliveries: usize,
    pub distinct_effects_applied: usize,
    pub redeliveries_deduplicated: usize,
    pub redeliveries_without_ledger_flag: usize,
}

#[derive(Debug, Default, Serialize)]
pub struct BundleSummary {
    pub path: String,
    pub records: u64,
    pub signing_key_id: String,
    pub valid: bool,
    pub unresolved: u64,
}

#[derive(Debug, Default, Serialize)]
pub struct DrillReport {
    pub scenario: &'static str,
    pub passed: bool,
    pub claim: &'static str,
    pub instances: u32,
    pub steps_per_instance: usize,
    pub completed: usize,
    pub not_completed: usize,
    pub executors: usize,
    pub killed_executor: String,
    pub killed_at: Option<DateTime<Utc>>,
    pub tasks_in_flight_on_killed_executor: usize,
    pub deliveries_unacknowledged_at_kill: usize,
    pub lease_secs: u64,
    pub reaper_tick_secs: u64,
    pub detection_ms: Option<u64>,
    pub time_to_recovery_ms: Option<u64>,
    pub total_ms: u64,
    pub ledger: LedgerSummary,
    pub provider: ProviderSummary,
    pub receipts_bundle: BundleSummary,
    pub invariants: Vec<Invariant>,
    pub artifacts_dir: Option<String>,
}

struct Control {
    storage: Arc<dyn StorageBackend>,
    api: String,
    shutdown: CancellationToken,
}

async fn start_control(db: &std::path::Path) -> Result<Control> {
    let storage: Arc<dyn StorageBackend> = Arc::new(
        SqliteStorage::file(db.to_str().context("non-UTF-8 path")?)
            .await
            .context("open control database")?,
    );
    let shutdown = CancellationToken::new();
    let mut master = [0_u8; 32];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut master);
    let master_hex = master.iter().fold(String::with_capacity(64), |mut out, b| {
        use std::fmt::Write as _;
        let _ = write!(out, "{b:02x}");
        out
    });
    let crypto =
        orch8_api::ContinuityCrypto::from_master_key(&master_hex).map_err(anyhow::Error::msg)?;
    let app_state = orch8_api::AppState {
        storage: storage.clone(),
        shutdown: shutdown.clone(),
        max_context_bytes: 1_048_576,
        externalization_mode: orch8_types::config::ExternalizationMode::default(),
        worker_lease_secs: LEASE_SECS,
        worker_heartbeat_interval_secs: 1,
        circuit_breakers: None,
        stream_limiter: Arc::new(tokio::sync::Semaphore::new(
            orch8_api::DEFAULT_MAX_CONCURRENT_STREAMS,
        )),
        publisher: None,
        push_provider: Arc::new(orch8_push::NoopPushProvider),
        mobile_sync_enabled: false,
        entitlements: orch8_api::entitlements::unlimited_provider(),
        builtin_handlers: Arc::new(orch8_api::builtin_handler_names()),
        engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        continuity_crypto: Some(Arc::new(crypto)),
        continuity_trusted_signing_keys: Arc::new(BTreeMap::new()),
        federation_peers: Arc::new(Vec::new()),
        continuity_lab_enabled: false,
        browser_sessions: Arc::new(orch8_api::browser_sessions::BrowserSessionSigner::for_root(
            None,
        )),
        browser_output_max_bytes: orch8_api::DEFAULT_BROWSER_OUTPUT_MAX_BYTES,
        embedded: std::sync::Arc::default(),
    };
    let auth_storage = storage.clone();
    let app = orch8_api::build_router(app_state)
        .layer(axum::middleware::from_fn(move |req, next| {
            let storage = auth_storage.clone();
            async move { orch8_api::auth::api_key_middleware(storage, None, req, next).await }
        }))
        .layer(axum::middleware::from_fn(|req, next| async move {
            orch8_api::auth::tenant_middleware(false, req, next).await
        }));
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let api = format!("http://{}/api/v1", listener.local_addr()?);
    let http_shutdown = shutdown.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { http_shutdown.cancelled().await })
            .await;
    });

    let config = orch8_types::config::SchedulerConfig {
        tick_interval_ms: 50,
        worker_reaper_tick_secs: REAPER_TICK_SECS,
        worker_reaper_stale_secs: LEASE_SECS,
        ..orch8_types::config::SchedulerConfig::default()
    };
    let mut handlers = orch8_engine::handlers::HandlerRegistry::new();
    orch8_engine::handlers::builtin::register_builtins(&mut handlers);
    let engine = orch8_engine::Engine::new(storage.clone(), config, handlers, shutdown.clone());
    tokio::spawn(async move {
        if let Err(error) = engine.run().await {
            tracing::error!(%error, "drill control engine stopped");
        }
    });
    Ok(Control {
        storage,
        api,
        shutdown,
    })
}

fn workload_sequence() -> Result<orch8_types::sequence::SequenceDefinition> {
    let retry = json!({"max_attempts": 5, "initial_backoff": 100, "max_backoff": 500,
                       "backoff_multiplier": 2.0});
    Ok(serde_json::from_value(json!({
        "id": uuid::Uuid::now_v7(), "tenant_id": TENANT, "namespace": "default",
        "name": "drill-order", "version": 1, "created_at": Utc::now(),
        "blocks": [
            {"type": "step", "id": "charge", "handler": "drill_charge", "retry": retry,
             "params": {"idempotency_key": "{{context.data.order_id}}:charge", "amount": 42}},
            {"type": "step", "id": "ship", "handler": "drill_ship", "retry": retry,
             "params": {"idempotency_key": "{{context.data.order_id}}:ship"}}
        ]
    }))?)
}

async fn tasks_of(
    storage: &dyn StorageBackend,
    worker_id: Option<&str>,
    states: Option<Vec<WorkerTaskState>>,
) -> Result<Vec<orch8_types::worker::WorkerTask>> {
    Ok(storage
        .list_worker_tasks(
            &WorkerTaskFilter {
                worker_id: worker_id.map(ToOwned::to_owned),
                states,
                ..WorkerTaskFilter::default()
            },
            &Pagination {
                offset: 0,
                limit: 1_000,
                sort_ascending: true,
            },
        )
        .await?)
}

fn spawn_executor(
    api: &str,
    provider: &str,
    worker_id: &str,
    work_ms: u64,
    log_dir: &std::path::Path,
) -> Result<tokio::process::Child> {
    let exe = std::env::current_exe().context("locate the orch8 binary")?;
    let log = std::fs::File::create(log_dir.join(format!("{worker_id}.log")))?;
    tokio::process::Command::new(exe)
        .args([
            "drill",
            "executor-process",
            "--api",
            api,
            "--provider",
            provider,
            "--worker-id",
            worker_id,
            "--work-ms",
            &work_ms.to_string(),
        ])
        .stdin(std::process::Stdio::null())
        .stdout(log.try_clone()?)
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .context("spawn executor process")
}

fn elapsed_ms(since: Instant) -> u64 {
    u64::try_from(since.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[allow(clippy::too_many_lines)]
pub async fn kill_executor(args: &KillExecutorArgs) -> Result<DrillReport> {
    let started = Instant::now();
    let dir = tempfile::Builder::new().prefix("orch8-drill-").tempdir()?;
    let control = start_control(&dir.path().join("control.db")).await?;
    let storage = control.storage.clone();
    let tenant = TenantId::new(TENANT).map_err(anyhow::Error::msg)?;

    let provider: SharedProvider = Arc::default();
    let provider_listener = TcpListener::bind("127.0.0.1:0").await?;
    let provider_url = format!("http://{}/deliver", provider_listener.local_addr()?);
    let provider_app = Router::new()
        .route("/deliver", post(provider_deliver))
        .with_state(provider.clone());
    let provider_shutdown = control.shutdown.clone();
    tokio::spawn(async move {
        let _ = axum::serve(provider_listener, provider_app)
            .with_graceful_shutdown(async move { provider_shutdown.cancelled().await })
            .await;
    });

    let sequence = workload_sequence()?;
    storage.create_sequence(&sequence).await?;
    let client = crate::build_client(None, Some(TENANT))?;
    let mut instance_ids = Vec::new();
    for n in 0..args.instances {
        let response = client
            .post(format!("{}/instances", control.api))
            .json(&json!({
                "sequence_id": sequence.id, "tenant_id": TENANT, "namespace": "default",
                "context": {"data": {"order_id": format!("order-{n:04}")}, "config": {}, "audit": []},
            }))
            .send()
            .await?;
        let status = response.status();
        let body: Value = response.json().await?;
        if !status.is_success() {
            bail!("create drill instance failed ({status}): {body}");
        }
        instance_ids.push(body["id"].as_str().context("instance id")?.to_owned());
    }

    let victim_id = "drill-executor-1";
    let survivor_id = "drill-executor-2";
    let mut victim = spawn_executor(
        &control.api,
        &provider_url,
        victim_id,
        args.work_ms,
        dir.path(),
    )?;
    let _survivor = spawn_executor(
        &control.api,
        &provider_url,
        survivor_id,
        args.work_ms,
        dir.path(),
    )?;

    let deadline = Instant::now() + Duration::from_secs(args.timeout_secs);
    let mut report = DrillReport {
        scenario: "kill-executor",
        claim: "at-most-once dispatch evidence (not exactly-once)",
        instances: args.instances,
        steps_per_instance: HANDLERS.len(),
        executors: 2,
        killed_executor: victim_id.into(),
        lease_secs: LEASE_SECS,
        reaper_tick_secs: REAPER_TICK_SECS,
        ..DrillReport::default()
    };

    // Kill once the victim holds claimed work and the workload is under way.
    let mut orphaned: Vec<orch8_types::worker::WorkerTask> = Vec::new();
    let mut eligible_since: Option<Instant> = None;
    while Instant::now() < deadline {
        let completed = tasks_of(
            storage.as_ref(),
            None,
            Some(vec![WorkerTaskState::Completed]),
        )
        .await?
        .len();
        let claimed = tasks_of(
            storage.as_ref(),
            Some(victim_id),
            Some(vec![WorkerTaskState::Claimed]),
        )
        .await?;
        // Prefer the hardest moment: a claimed task whose effect already
        // reached the provider but was not yet acknowledged to the engine.
        let delivered_in_flight = {
            let provider = provider
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            claimed.iter().any(|t| {
                let id = t.id.to_string();
                provider.deliveries.iter().any(|d| d.task_id == id)
            })
        };
        let eligible = completed >= 2 && !claimed.is_empty();
        if eligible && eligible_since.is_none() {
            eligible_since = Some(Instant::now());
        }
        let waited_long_enough =
            eligible_since.is_some_and(|since| since.elapsed() > Duration::from_secs(3));
        if eligible && (delivered_in_flight || waited_long_enough) {
            victim.start_kill().context("kill executor")?;
            let _ = victim.wait().await;
            report.killed_at = Some(Utc::now());
            orphaned = tasks_of(
                storage.as_ref(),
                Some(victim_id),
                Some(vec![WorkerTaskState::Claimed]),
            )
            .await?;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let killed = Instant::now();
    report.tasks_in_flight_on_killed_executor = orphaned.len();
    {
        let provider = provider
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let orphan_ids: BTreeSet<String> = orphaned.iter().map(|t| t.id.to_string()).collect();
        report.deliveries_unacknowledged_at_kill = provider
            .deliveries
            .iter()
            .filter(|d| orphan_ids.contains(&d.task_id))
            .count();
    }

    // Recovery: every orphaned step completes on the surviving executor.
    let orphan_steps: BTreeSet<(String, String)> = orphaned
        .iter()
        .map(|t| (t.instance_id.to_string(), t.block_id.to_string()))
        .collect();
    let orphan_ids: BTreeSet<uuid::Uuid> = orphaned.iter().map(|t| t.id).collect();
    while report.killed_at.is_some() && Instant::now() < deadline {
        let all = tasks_of(storage.as_ref(), None, None).await?;
        if report.detection_ms.is_none()
            && orphan_ids.iter().any(|id| {
                all.iter()
                    .find(|t| t.id == *id)
                    .is_none_or(|t| t.state != WorkerTaskState::Claimed)
            })
        {
            report.detection_ms = Some(elapsed_ms(killed));
        }
        let recovered = orphan_steps.iter().all(|(instance, block)| {
            all.iter().any(|t| {
                t.state == WorkerTaskState::Completed
                    && t.worker_id.as_deref() == Some(survivor_id)
                    && &t.instance_id.to_string() == instance
                    && &t.block_id.to_string() == block
            })
        });
        if recovered {
            report.time_to_recovery_ms = Some(elapsed_ms(killed));
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Let the whole workload finish.
    let filter = InstanceFilter {
        tenant_id: Some(tenant.clone()),
        ..InstanceFilter::default()
    };
    let page = Pagination {
        offset: 0,
        limit: 1_000,
        sort_ascending: true,
    };
    let mut instances = storage.list_instances(&filter, &page).await?;
    while Instant::now() < deadline && instances.iter().any(|i| !i.state.is_terminal()) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        instances = storage.list_instances(&filter, &page).await?;
    }
    report.completed = instances
        .iter()
        .filter(|i| i.state == InstanceState::Completed)
        .count();
    report.not_completed = instances.len() - report.completed;

    // Ledger before reconciliation.
    let mut receipts: Vec<EffectReceipt> = Vec::new();
    for instance in &instances {
        receipts.extend(
            storage
                .list_instance_effect_receipts(&tenant, instance.id, 10_000)
                .await?,
        );
    }
    let deliveries = provider
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .deliveries
        .clone();
    summarize(&mut report, &receipts, &deliveries);

    // Verifier: resolve ambiguous receipts against the provider log.
    for receipt in receipts.iter().filter(|r| r.state == EffectState::Unknown) {
        let delivered = deliveries.iter().find(|d| {
            d.instance_id == receipt.instance_id.to_string()
                && d.block_id == receipt.block_id.to_string()
                && d.attempt == receipt.attempt
        });
        let (state, receipt_id) = match delivered {
            Some(_) => ("verified", delivered.map(|d| d.idempotency_key.clone())),
            None => ("abandoned", None),
        };
        let response = client
            .post(format!(
                "{}/continuity/effects/{}/resolve",
                control.api, receipt.id
            ))
            .json(&json!({"tenant_id": TENANT, "state": state, "provider_receipt_id": receipt_id}))
            .send()
            .await?;
        if response.status().is_success() {
            if state == "verified" {
                report.ledger.reconciled_verified += 1;
            } else {
                report.ledger.reconciled_abandoned += 1;
            }
        }
    }

    // Signed evidence for the whole run.
    let from = (Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let to = (Utc::now() + chrono::Duration::minutes(5)).to_rfc3339();
    let bundle = client
        .get(format!("{}/receipts/export", control.api))
        .query(&[("from", from.as_str()), ("to", to.as_str())])
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?;
    let bundle_path = dir.path().join("receipts.jsonl");
    std::fs::write(&bundle_path, &bundle)?;
    report.receipts_bundle.path = bundle_path.display().to_string();
    match orch8_engine::receipt_bundle::verify_bundle(&bundle, None) {
        Ok(verified) => {
            report.receipts_bundle.valid = true;
            report.receipts_bundle.records = verified.records;
            report.receipts_bundle.unresolved = verified.unresolved;
            report.receipts_bundle.signing_key_id = verified.signing_key_id;
        }
        Err(error) => report.receipts_bundle.signing_key_id = format!("INVALID: {error}"),
    }

    control.shutdown.cancel();
    report.total_ms = elapsed_ms(started);
    report.invariants = invariants(&report);
    report.passed = report.invariants.iter().all(|i| i.passed);
    if args.keep || !report.passed {
        report.artifacts_dir = Some(dir.keep().display().to_string());
    } else {
        report.receipts_bundle.path = "(discarded; re-run with --keep to retain)".into();
    }
    Ok(report)
}

fn summarize(report: &mut DrillReport, receipts: &[EffectReceipt], deliveries: &[Delivery]) {
    let ledger = &mut report.ledger;
    ledger.receipts = receipts.len();
    let mut per_attempt: BTreeMap<(String, String, u32), usize> = BTreeMap::new();
    let mut committed_per_step: BTreeMap<(String, String), usize> = BTreeMap::new();
    for receipt in receipts {
        let state = serde_json::to_value(receipt.state)
            .ok()
            .and_then(|v| v.as_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        *ledger.by_state_before_reconcile.entry(state).or_default() += 1;
        let step = (
            receipt.instance_id.to_string(),
            receipt.block_id.to_string(),
        );
        *per_attempt
            .entry((step.0.clone(), step.1.clone(), receipt.attempt))
            .or_default() += 1;
        if receipt.state == EffectState::Committed {
            *committed_per_step.entry(step).or_default() += 1;
        }
    }
    ledger.ambiguous_attempts = receipts
        .iter()
        .filter(|r| matches!(r.state, EffectState::Unknown | EffectState::Dispatched))
        .count();
    ledger.duplicate_attempt_receipts = per_attempt.values().filter(|n| **n > 1).count();
    ledger.steps_with_multiple_committed = committed_per_step.values().filter(|n| **n > 1).count();

    let provider = &mut report.provider;
    provider.deliveries = deliveries.len();
    provider.distinct_effects_applied = deliveries
        .iter()
        .map(|d| d.idempotency_key.as_str())
        .collect::<BTreeSet<_>>()
        .len();
    provider.redeliveries_deduplicated = deliveries.iter().filter(|d| d.duplicate).count();
    // A redelivery is only acceptable when every earlier delivering attempt
    // of that step is flagged ambiguous in the ledger (never silent).
    provider.redeliveries_without_ledger_flag = deliveries
        .iter()
        .filter(|d| d.duplicate)
        .filter(|redelivery| {
            deliveries
                .iter()
                .filter(|d| {
                    d.idempotency_key == redelivery.idempotency_key
                        && d.attempt < redelivery.attempt
                })
                .any(|earlier| {
                    !receipts.iter().any(|r| {
                        r.instance_id.to_string() == earlier.instance_id
                            && r.block_id.to_string() == earlier.block_id
                            && r.attempt == earlier.attempt
                            && matches!(r.state, EffectState::Unknown | EffectState::Dispatched)
                    })
                })
        })
        .count();
}

fn invariants(report: &DrillReport) -> Vec<Invariant> {
    let expected_effects = report.instances as usize * report.steps_per_instance;
    vec![
        Invariant {
            name: "executor_killed_mid_flight",
            passed: report.killed_at.is_some() && report.tasks_in_flight_on_killed_executor > 0,
            detail: format!(
                "{} claimed task(s) on {} at SIGKILL ({} already delivered to the provider)",
                report.tasks_in_flight_on_killed_executor,
                report.killed_executor,
                report.deliveries_unacknowledged_at_kill
            ),
        },
        Invariant {
            name: "all_instances_completed",
            passed: report.not_completed == 0 && report.completed == report.instances as usize,
            detail: format!("{}/{} completed", report.completed, report.instances),
        },
        Invariant {
            name: "recovered",
            passed: report.time_to_recovery_ms.is_some(),
            detail: report.time_to_recovery_ms.map_or_else(
                || "orphaned steps never completed on the surviving executor".into(),
                |ms| format!("orphaned steps completed on the survivor {ms} ms after SIGKILL"),
            ),
        },
        Invariant {
            name: "no_effect_recorded_twice",
            passed: report.ledger.duplicate_attempt_receipts == 0
                && report.ledger.steps_with_multiple_committed == 0,
            detail: format!(
                "{} attempt(s) with >1 receipt, {} step(s) with >1 committed receipt",
                report.ledger.duplicate_attempt_receipts,
                report.ledger.steps_with_multiple_committed
            ),
        },
        Invariant {
            name: "no_silent_redelivery",
            passed: report.provider.redeliveries_without_ledger_flag == 0,
            detail: format!(
                "{} redelivery(ies), all preceded by an attempt the ledger marked unknown: {}",
                report.provider.redeliveries_deduplicated,
                report.provider.redeliveries_without_ledger_flag == 0
            ),
        },
        Invariant {
            name: "each_effect_applied_once_at_provider",
            passed: report.provider.distinct_effects_applied == expected_effects,
            detail: format!(
                "{} distinct idempotency keys applied, {} expected",
                report.provider.distinct_effects_applied, expected_effects
            ),
        },
        Invariant {
            name: "signed_receipts_verify",
            passed: report.receipts_bundle.valid && report.receipts_bundle.unresolved == 0,
            detail: format!(
                "{} receipt(s), {} unresolved after reconciliation, key {}",
                report.receipts_bundle.records,
                report.receipts_bundle.unresolved,
                report.receipts_bundle.signing_key_id
            ),
        },
    ]
}

fn print_report(report: &DrillReport) {
    println!(
        "Drill: kill-executor  {}",
        if report.passed { "PASSED" } else { "FAILED" }
    );
    println!(
        "  workload:        {} instances x {} side-effecting steps, 2 executor processes",
        report.instances, report.steps_per_instance
    );
    println!(
        "  SIGKILL:         {} with {} claimed task(s), {} delivered but unacknowledged",
        report.killed_executor,
        report.tasks_in_flight_on_killed_executor,
        report.deliveries_unacknowledged_at_kill
    );
    let ms = |v: Option<u64>| v.map_or_else(|| "n/a".to_owned(), |v| format!("{v} ms"));
    println!(
        "  detection:       {} (lease {} s, reaper tick {} s)",
        ms(report.detection_ms),
        report.lease_secs,
        report.reaper_tick_secs
    );
    println!("  time to recovery: {}", ms(report.time_to_recovery_ms));
    println!(
        "  completed:       {}/{}  (total {} ms)",
        report.completed, report.instances, report.total_ms
    );
    println!(
        "  ledger:          {} receipts {:?}; {} ambiguous -> {} verified, {} abandoned",
        report.ledger.receipts,
        report.ledger.by_state_before_reconcile,
        report.ledger.ambiguous_attempts,
        report.ledger.reconciled_verified,
        report.ledger.reconciled_abandoned
    );
    println!(
        "  provider:        {} deliveries, {} distinct effects, {} redeliveries deduplicated",
        report.provider.deliveries,
        report.provider.distinct_effects_applied,
        report.provider.redeliveries_deduplicated
    );
    println!(
        "  evidence:        {} ({} receipts, signed by {})",
        report.receipts_bundle.path,
        report.receipts_bundle.records,
        report.receipts_bundle.signing_key_id
    );
    println!("  claim:           {}", report.claim);
    for invariant in &report.invariants {
        println!(
            "  [{}] {:<37} {}",
            if invariant.passed { "ok" } else { "FAIL" },
            invariant.name,
            invariant.detail
        );
    }
    if let Some(dir) = &report.artifacts_dir {
        println!("  artifacts:       {dir}");
    }
}
