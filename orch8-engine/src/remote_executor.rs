//! Hybrid remote executor: run leased steps inside the customer's network.
//!
//! In the hybrid model the engine (scheduler, API, database) runs in Orch8
//! Cloud and executors run in the customer's VPC. An executor holds **no
//! database**: it dials out to the engine, advertises what it can run
//! (handlers, labels such as `residency=eu`, regions, and the *names* of the
//! credentials it holds), claims leased tasks through the existing worker
//! protocol ([`LeaseTransport`]: the gRPC worker stream or HTTP polling),
//! runs the built-in handler locally, and reports the outcome fenced by the
//! task's `claim_epoch`.
//!
//! What stays on the executor:
//!
//! - **Credentials.** A hard-placed step's `credentials://<id>` references
//!   arrive unresolved (the engine defers them, see
//!   [`crate::step_placement::defers_credentials`]) and are resolved here
//!   from [`LocalCredentials`]. The secret value never travels to, and is
//!   never stored by, the engine.
//! - **Network access and side effects.** The handler runs here, so calls
//!   to internal APIs originate from the customer network.
//! - **Large outputs (optional).** With a BYOK vault configured, top-level
//!   output fields above a threshold are sealed into the customer bucket and
//!   only references are reported ([`seal_output`]); later steps on an
//!   executor open them again ([`open_vault_references`]).
//!
//! What the engine still sees: the step params and context it rendered and
//! sent (minus credential values), and every output that is not sealed.
//!
//! Draining (SIGTERM, a managed-control `drain`): the executor advertises
//! `draining` (withdrawing placement capability), stops claiming, lets
//! in-flight tasks finish for a bounded window, then releases the rest back
//! to the engine (`release {started: true}`: the effect is marked `unknown`
//! and the step's retry policy applies). A killed executor simply stops
//! heartbeating; the engine's lease reaper reclaims its tasks.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_storage::StorageBackend;
use orch8_storage::encrypting::{ExternalPayloadVault, VAULT_REF_KEY, is_vault_reference};
use orch8_types::continuity::{
    RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use orch8_types::error::StepError;
use orch8_types::ids::{InstanceId, TenantId};

use crate::credentials::LocalCredentials;
use crate::handlers::HandlerRegistry;
use crate::remote_worker::{HttpLeaseClient, LeaseResponse, RemoteTask, heartbeat_interval};

/// Lifetime of one capability advertisement sent by an executor.
const CAPABILITY_TTL: Duration = Duration::from_secs(120);
/// Delivery attempts for complete/fail before leaving the task to the
/// engine's lease reaper.
const DELIVERY_ATTEMPTS: u32 = 4;
/// Longest back-off after failed claims.
const MAX_CLAIM_BACKOFF: Duration = Duration::from_secs(15);
/// After the drain window: how long released tasks get to report.
const RELEASE_GRACE: Duration = Duration::from_secs(10);
/// Field added to a vault reference sealed by an executor: the ref key its
/// AEAD associated data is bound to, so another executor can open it.
pub const VAULT_REF_KEY_FIELD: &str = "ref";

// ---------------------------------------------------------------------------
// Identity and capabilities
// ---------------------------------------------------------------------------

/// Who this executor is and what it advertises. Never contains secrets:
/// `credentials` holds credential *ids* only.
#[derive(Debug, Clone)]
pub struct ExecutorIdentity {
    /// This replica's runtime id ([`replica_runtime_id`]).
    pub runtime_id: RuntimeId,
    /// Human-readable worker name (`<worker_id_prefix>-<hostname>` from the
    /// join token), advertised as the hardware fact `host:<name>`.
    pub host: String,
    pub tenant_id: String,
    pub labels: BTreeMap<String, String>,
    pub regions: Vec<String>,
    pub handlers: Vec<String>,
    pub credentials: Vec<String>,
}

impl ExecutorIdentity {
    /// The lease identity (`worker_id`) of every claim: the runtime id, so
    /// capability matching and sticky affinity refer to the same runtime.
    #[must_use]
    pub fn worker_id(&self) -> String {
        self.runtime_id.to_string()
    }

    /// A freshly stamped capability advertisement.
    #[must_use]
    pub fn capabilities(&self, draining: bool) -> RuntimeCapabilities {
        let now = chrono::Utc::now();
        let ttl = chrono::Duration::from_std(CAPABILITY_TTL).unwrap_or(chrono::Duration::MAX);
        RuntimeCapabilities {
            runtime_id: self.runtime_id,
            kind: RuntimeKind::Server,
            trust: RuntimeTrustLevel::Registered,
            handlers: self.handlers.clone(),
            plugins: Vec::new(),
            credentials: self.credentials.clone(),
            regions: self.regions.clone(),
            hardware: if self.host.is_empty() {
                Vec::new()
            } else {
                vec![format!("host:{}", self.host)]
            },
            offline_capable: false,
            connectivity: Some(RuntimeConnectivity::Ethernet),
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining,
            capsule_signing_public_key: None,
            labels: self.labels.clone(),
            observed_at: now,
            expires_at: now + ttl,
        }
    }

    /// Structural checks the control plane would otherwise reject at
    /// registration (label bounds, at least one handler).
    ///
    /// # Errors
    /// A human-readable description of the first violation.
    pub fn validate(&self) -> Result<(), String> {
        if self.handlers.is_empty() {
            return Err("the executor serves no handlers".into());
        }
        orch8_types::placement::validate_runtime_labels(&self.labels)
    }
}

/// Runtime id of one executor replica: derived deterministically from the
/// join token's runtime id and the replica's worker name, so every replica
/// of one token has its own capability row (draining one pod never
/// withdraws its siblings) and keeps it across restarts.
#[must_use]
pub fn replica_runtime_id(token_runtime_id: uuid::Uuid, worker_name: &str) -> RuntimeId {
    use sha2::Digest as _;
    let digest = sha2::Sha256::new()
        .chain_update(b"orch8-executor-replica-v1\0")
        .chain_update(token_runtime_id.as_bytes())
        .chain_update(worker_name.as_bytes())
        .finalize();
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    RuntimeId::from_uuid(uuid::Builder::from_random_bytes(bytes).into_uuid())
}

/// A registry holding the named built-ins (validated against
/// [`orch8_types::sequence::REMOTE_EXECUTABLE_BUILTINS`] by the caller).
/// Empty `names` = every remote-executable built-in compiled in.
#[must_use]
pub fn executor_registry(names: &[String]) -> HandlerRegistry {
    let mut registry = HandlerRegistry::new();
    crate::handlers::builtin::register_builtins(&mut registry);
    registry.retain_handlers(|name| {
        orch8_types::sequence::REMOTE_EXECUTABLE_BUILTINS.contains(&name)
            && (names.is_empty() || names.iter().any(|n| n == name))
    });
    registry
}

// ---------------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------------

/// A claimed task plus the transport session that delivered it.
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    pub task: RemoteTask,
    /// Transport session generation (gRPC routes a mutation through the
    /// stream only for tasks of the live session). `0` for HTTP.
    pub session: u64,
}

/// One claim round.
#[derive(Debug, Default)]
pub struct Claimed {
    pub tasks: Vec<ClaimedTask>,
    /// Server back-off hint while idle.
    pub poll_after: Option<Duration>,
    /// Server heartbeat cadence for these tasks.
    pub heartbeat: Option<Duration>,
}

/// The worker lease protocol as a remote executor uses it. Implemented over
/// HTTP polling ([`HttpLeaseTransport`]) and the negotiated gRPC worker
/// stream (`orch8_grpc::worker_client`).
#[async_trait]
pub trait LeaseTransport: Send + Sync + 'static {
    /// Short transport name for logs and reports.
    fn name(&self) -> &'static str;
    /// Refresh the capability lease (liveness, facts, `draining`).
    async fn advertise(&self, capabilities: &RuntimeCapabilities) -> Result<(), String>;
    /// Claim up to `capacity` tasks of `handlers` as `capabilities`.
    async fn claim(
        &self,
        handlers: &[String],
        capacity: u32,
        capabilities: &RuntimeCapabilities,
    ) -> Result<Claimed, String>;
    async fn heartbeat(&self, task: &ClaimedTask) -> LeaseResponse;
    async fn complete(&self, task: &ClaimedTask, output: &Value) -> LeaseResponse;
    async fn fail(&self, task: &ClaimedTask, message: &str, retryable: bool) -> LeaseResponse;
    /// Give a claim back. `started: true` marks the attempt's effect
    /// `unknown` on the engine (it may have happened).
    async fn release(&self, task: &ClaimedTask, started: bool) -> LeaseResponse;
}

/// HTTP polling transport (`/workers/tasks/poll` with capabilities,
/// `/runtimes/register` for capability refreshes).
pub struct HttpLeaseTransport {
    client: HttpLeaseClient,
    tenant_id: String,
    version: String,
    cursor: AtomicUsize,
}

impl HttpLeaseTransport {
    #[must_use]
    pub fn new(client: HttpLeaseClient, tenant_id: String, version: String) -> Self {
        Self {
            client,
            tenant_id,
            version,
            cursor: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl LeaseTransport for HttpLeaseTransport {
    fn name(&self) -> &'static str {
        "http"
    }

    async fn advertise(&self, capabilities: &RuntimeCapabilities) -> Result<(), String> {
        self.client
            .register_runtime(&self.tenant_id, capabilities)
            .await
    }

    async fn claim(
        &self,
        handlers: &[String],
        capacity: u32,
        capabilities: &RuntimeCapabilities,
    ) -> Result<Claimed, String> {
        let mut claimed = Claimed::default();
        if handlers.is_empty() {
            return Ok(claimed);
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed);
        let mut remaining = capacity;
        for offset in 0..handlers.len() {
            if remaining == 0 {
                break;
            }
            let handler = &handlers[(start + offset) % handlers.len()];
            let response = self
                .client
                .poll(handler, remaining, Some(&self.version), capabilities)
                .await?;
            if let Some(ms) = response.poll_after_ms.filter(|ms| *ms > 0) {
                let hint = Duration::from_millis(ms.min(60_000));
                claimed.poll_after = Some(claimed.poll_after.map_or(hint, |cur| cur.min(hint)));
            }
            let heartbeat =
                heartbeat_interval(response.heartbeat_interval_secs, response.lease_secs, None);
            claimed.heartbeat = Some(claimed.heartbeat.map_or(heartbeat, |h| h.min(heartbeat)));
            for task in response.tasks {
                remaining = remaining.saturating_sub(1);
                claimed.tasks.push(ClaimedTask { task, session: 0 });
            }
        }
        Ok(claimed)
    }

    async fn heartbeat(&self, task: &ClaimedTask) -> LeaseResponse {
        self.client
            .heartbeat(task.task.id, task.task.claim_epoch)
            .await
    }

    async fn complete(&self, task: &ClaimedTask, output: &Value) -> LeaseResponse {
        self.client
            .complete(task.task.id, task.task.claim_epoch, output)
            .await
    }

    async fn fail(&self, task: &ClaimedTask, message: &str, retryable: bool) -> LeaseResponse {
        self.client
            .fail(task.task.id, task.task.claim_epoch, message, retryable)
            .await
    }

    async fn release(&self, task: &ClaimedTask, started: bool) -> LeaseResponse {
        match self
            .client
            .release(task.task.id, task.task.claim_epoch, started)
            .await
        {
            // Pre-contract server: a retryable failure requeues the task.
            LeaseResponse::Unsupported => {
                self.client
                    .fail(
                        task.task.id,
                        task.task.claim_epoch,
                        "released by a draining executor",
                        true,
                    )
                    .await
            }
            other => other,
        }
    }
}

// ---------------------------------------------------------------------------
// BYOK: sealing outputs, opening inputs
// ---------------------------------------------------------------------------

/// Seal the large parts of a step output into the customer's vault.
///
/// An object output keeps its shape: each top-level field whose JSON is
/// larger than `threshold_bytes` (every field when `0`) is replaced by a
/// vault reference; small fields (status codes, ids) stay readable to the
/// engine and to templates. Any other output is sealed whole when it is over
/// the threshold. Each reference carries the ref key its ciphertext is
/// bound to (`ref`), so any executor with the same vault can open it.
///
/// # Errors
/// Vault failures (bucket or KMS unreachable). The caller fails the task
/// retryably rather than send plaintext.
pub async fn seal_output(
    vault: &dyn ExternalPayloadVault,
    instance_id: InstanceId,
    block_id: &str,
    output: Value,
    threshold_bytes: u64,
) -> Result<Value, String> {
    let over = |value: &Value| -> bool {
        threshold_bytes == 0
            || serde_json::to_vec(value).map_or(true, |bytes| {
                u64::try_from(bytes.len()).unwrap_or(u64::MAX) > threshold_bytes
            })
    };
    match output {
        Value::Object(map) => {
            let mut sealed = serde_json::Map::with_capacity(map.len());
            for (field, value) in map {
                if !is_vault_reference(&value) && over(&value) {
                    let ref_key = format!("remote-output/{block_id}/{field}");
                    let reference = seal_one(vault, instance_id, &ref_key, &value).await?;
                    sealed.insert(field, reference);
                } else {
                    sealed.insert(field, value);
                }
            }
            Ok(Value::Object(sealed))
        }
        other if !is_vault_reference(&other) && over(&other) => {
            let ref_key = format!("remote-output/{block_id}");
            seal_one(vault, instance_id, &ref_key, &other).await
        }
        other => Ok(other),
    }
}

async fn seal_one(
    vault: &dyn ExternalPayloadVault,
    instance_id: InstanceId,
    ref_key: &str,
    value: &Value,
) -> Result<Value, String> {
    let mut reference = vault
        .seal(instance_id, ref_key, value)
        .await
        .map_err(|e| format!("BYOK vault seal failed: {e}"))?;
    if let Some(inner) = reference
        .get_mut(VAULT_REF_KEY)
        .and_then(Value::as_object_mut)
    {
        inner.insert(VAULT_REF_KEY_FIELD.into(), Value::String(ref_key.into()));
    }
    Ok(reference)
}

/// Replace every executor-sealed vault reference in `value` (one carrying
/// its `ref` key) with the plaintext, so a handler sees the data a previous
/// executor step produced. References without a `ref` key (sealed by an
/// engine node) are left untouched.
///
/// # Errors
/// A reference that cannot be opened (wrong instance, revoked key, missing
/// object).
pub async fn open_vault_references(
    vault: &dyn ExternalPayloadVault,
    instance_id: InstanceId,
    value: &mut Value,
) -> Result<(), String> {
    if is_vault_reference(value) {
        let ref_key = value
            .get(VAULT_REF_KEY)
            .and_then(|inner| inner.get(VAULT_REF_KEY_FIELD))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if let Some(ref_key) = ref_key {
            *value = vault
                .open(instance_id, &ref_key, value)
                .await
                .map_err(|e| format!("BYOK vault open failed for `{ref_key}`: {e}"))?;
        }
        return Ok(());
    }
    match value {
        Value::Array(items) => {
            for item in items {
                Box::pin(open_vault_references(vault, instance_id, item)).await?;
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                Box::pin(open_vault_references(vault, instance_id, item)).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Executor
// ---------------------------------------------------------------------------

/// Tunables of a [`RemoteExecutor`].
#[derive(Debug, Clone)]
pub struct RemoteExecutorSettings {
    pub max_concurrent_tasks: u32,
    pub poll_interval: Duration,
    pub drain_timeout: Duration,
    /// Capability refresh cadence (must stay well under the engine's
    /// capability lease; the gRPC session lease is 45 s).
    pub advertise_interval: Duration,
    /// BYOK sealing threshold for output fields (see [`seal_output`]).
    pub externalize_bytes: u64,
    /// Cap on the heartbeat cadence (`None` = the engine's hint).
    pub max_heartbeat: Option<Duration>,
}

impl Default for RemoteExecutorSettings {
    fn default() -> Self {
        Self {
            max_concurrent_tasks: 16,
            poll_interval: Duration::from_millis(500),
            drain_timeout: Duration::from_secs(25),
            advertise_interval: Duration::from_secs(15),
            externalize_bytes: 64 * 1024,
            max_heartbeat: None,
        }
    }
}

/// Counters of one executor run.
#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExecutorStats {
    pub claimed: u64,
    pub completed: u64,
    pub failed: u64,
    /// Given back (drain, timeout, surplus) with `release`.
    pub released: u64,
    /// Lease lost to the engine mid-execution (reclaimed, stale epoch).
    pub lost: u64,
}

#[derive(Default)]
struct Counters {
    claimed: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    released: AtomicU64,
    lost: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> ExecutorStats {
        ExecutorStats {
            claimed: self.claimed.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            released: self.released.load(Ordering::Relaxed),
            lost: self.lost.load(Ordering::Relaxed),
        }
    }
}

enum Outcome {
    Complete(Value),
    Fail { message: String, retryable: bool },
}

/// Everything a remote executor needs; see the module docs.
pub struct RemoteExecutorParts {
    pub identity: ExecutorIdentity,
    pub settings: RemoteExecutorSettings,
    pub transport: Arc<dyn LeaseTransport>,
    pub registry: HandlerRegistry,
    pub credentials: LocalCredentials,
    pub vault: Option<Arc<dyn ExternalPayloadVault>>,
    /// Process-local scratch store handed to handlers (LLM response cache,
    /// usage events). Never the engine's database; discarded at exit.
    pub scratch: Arc<dyn StorageBackend>,
}

/// A running remote executor.
pub struct RemoteExecutor {
    identity: ExecutorIdentity,
    tenant: TenantId,
    settings: RemoteExecutorSettings,
    transport: Arc<dyn LeaseTransport>,
    registry: HandlerRegistry,
    credentials: LocalCredentials,
    vault: Option<Arc<dyn ExternalPayloadVault>>,
    scratch: Arc<dyn StorageBackend>,
    counters: Counters,
    slots: Arc<Semaphore>,
    running: StdMutex<HashMap<uuid::Uuid, CancellationToken>>,
    wake: Notify,
}

impl RemoteExecutor {
    /// # Errors
    /// Invalid identity (no handlers, label bounds) or a handler the
    /// registry does not hold.
    pub fn new(parts: RemoteExecutorParts) -> Result<Arc<Self>, String> {
        parts.identity.validate()?;
        let missing: Vec<&String> = parts
            .identity
            .handlers
            .iter()
            .filter(|h| !parts.registry.contains(h))
            .collect();
        if !missing.is_empty() {
            return Err(format!(
                "advertised handlers are not available: {missing:?}"
            ));
        }
        let slots = parts.settings.max_concurrent_tasks.clamp(1, 256) as usize;
        Ok(Arc::new(Self {
            tenant: TenantId::unchecked(parts.identity.tenant_id.clone()),
            identity: parts.identity,
            settings: parts.settings,
            transport: parts.transport,
            registry: parts.registry,
            credentials: parts.credentials,
            vault: parts.vault,
            scratch: parts.scratch,
            counters: Counters::default(),
            slots: Arc::new(Semaphore::new(slots)),
            running: StdMutex::new(HashMap::new()),
            wake: Notify::new(),
        }))
    }

    #[must_use]
    pub fn identity(&self) -> &ExecutorIdentity {
        &self.identity
    }

    #[must_use]
    pub fn stats(&self) -> ExecutorStats {
        self.counters.snapshot()
    }

    /// Tasks currently executing.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    async fn advertise(&self, draining: bool) {
        if let Err(error) = self
            .transport
            .advertise(&self.identity.capabilities(draining))
            .await
        {
            warn!(transport = self.transport.name(), %error, "capability advertisement failed");
        }
    }

    /// Claim and run tasks until `shutdown` fires, then drain: withdraw
    /// capability, finish in-flight work within the drain window, release
    /// the rest. Returns the run's counters.
    pub async fn run(self: Arc<Self>, shutdown: CancellationToken) -> ExecutorStats {
        info!(
            transport = self.transport.name(),
            runtime_id = %self.identity.runtime_id,
            host = %self.identity.host,
            handlers = ?self.identity.handlers,
            labels = ?self.identity.labels,
            credentials = self.identity.credentials.len(),
            "remote executor started"
        );
        self.advertise(false).await;
        let mut next_advertise = tokio::time::Instant::now() + self.settings.advertise_interval;
        let mut tasks: JoinSet<()> = JoinSet::new();
        let mut heartbeat = heartbeat_interval(None, None, None);
        let mut backoff = self.settings.poll_interval;
        loop {
            if shutdown.is_cancelled() {
                break;
            }
            while tasks.try_join_next().is_some() {}
            let free = self.slots.available_permits();
            let mut wait = self.settings.poll_interval;
            if free > 0 {
                let capacity = u32::try_from(free).unwrap_or(u32::MAX);
                match self
                    .transport
                    .claim(
                        &self.identity.handlers,
                        capacity,
                        &self.identity.capabilities(false),
                    )
                    .await
                {
                    Ok(claimed) => {
                        backoff = self.settings.poll_interval;
                        if let Some(h) = claimed.heartbeat {
                            heartbeat = h;
                        }
                        if claimed.tasks.is_empty() {
                            if let Some(hint) = claimed.poll_after {
                                wait = wait.max(hint.min(Duration::from_secs(5)));
                            }
                        } else {
                            wait = Duration::ZERO;
                        }
                        for task in claimed.tasks {
                            self.spawn_task(&mut tasks, task, heartbeat);
                        }
                    }
                    Err(error) => {
                        warn!(transport = self.transport.name(), %error, "claim failed");
                        backoff = backoff.saturating_mul(2).min(MAX_CLAIM_BACKOFF);
                        wait = backoff;
                    }
                }
            }
            if tokio::time::Instant::now() >= next_advertise {
                self.advertise(false).await;
                next_advertise = tokio::time::Instant::now() + self.settings.advertise_interval;
            }
            if wait.is_zero() && self.slots.available_permits() > 0 {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                () = shutdown.cancelled() => break,
                () = self.wake.notified() => {}
                () = tokio::time::sleep(wait) => {}
            }
        }
        self.drain(tasks).await
    }

    async fn drain(&self, mut tasks: JoinSet<()>) -> ExecutorStats {
        info!(
            in_flight = self.in_flight(),
            drain_timeout_secs = self.settings.drain_timeout.as_secs(),
            "remote executor draining: capability withdrawn, no new claims"
        );
        self.advertise(true).await;
        let deadline = tokio::time::Instant::now() + self.settings.drain_timeout;
        loop {
            if tasks.is_empty() {
                break;
            }
            tokio::select! {
                _ = tasks.join_next() => {}
                () = tokio::time::sleep_until(deadline) => {
                    let running: Vec<CancellationToken> = self
                        .running
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .values()
                        .cloned()
                        .collect();
                    warn!(tasks = running.len(), "drain window elapsed; releasing in-flight tasks");
                    for token in running {
                        token.cancel();
                    }
                    break;
                }
            }
        }
        let _ = tokio::time::timeout(RELEASE_GRACE, async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        let stats = self.stats();
        info!(?stats, "remote executor stopped");
        stats
    }

    fn spawn_task(
        self: &Arc<Self>,
        tasks: &mut JoinSet<()>,
        task: ClaimedTask,
        heartbeat: Duration,
    ) {
        self.counters.claimed.fetch_add(1, Ordering::Relaxed);
        let permit = Arc::clone(&self.slots).try_acquire_owned();
        let this = Arc::clone(self);
        tasks.spawn(async move {
            let Ok(permit) = permit else {
                // More tasks than free slots: hand the surplus back untouched.
                this.release(&task, false).await;
                return;
            };
            this.execute(task, permit, heartbeat).await;
            this.wake.notify_one();
        });
    }

    async fn release(&self, task: &ClaimedTask, started: bool) {
        match self.transport.release(task, started).await {
            LeaseResponse::Accepted => {
                self.counters.released.fetch_add(1, Ordering::Relaxed);
            }
            LeaseResponse::LostOwnership => {
                self.counters.lost.fetch_add(1, Ordering::Relaxed);
            }
            other => {
                warn!(task_id = %task.task.id, response = ?other,
                    "release not accepted; the engine's lease reaper will reclaim the task");
            }
        }
    }

    async fn execute(&self, task: ClaimedTask, permit: OwnedSemaphorePermit, heartbeat: Duration) {
        let _permit = permit;
        let id = task.task.id;
        let cancel = CancellationToken::new();
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(id, cancel.clone());
        self.execute_inner(&task, heartbeat, &cancel).await;
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&id);
    }

    /// Params and context as the handler must see them: local credentials
    /// resolved, executor-sealed vault references opened.
    async fn prepare(&self, task: &RemoteTask) -> Result<(Value, Value), String> {
        let mut params = task.params.clone();
        self.credentials
            .resolve_in_value(&mut params)
            .map_err(|e| e.to_string())?;
        let mut context = task.context.clone();
        if let Some(vault) = &self.vault {
            let instance = InstanceId::from_uuid(task.instance_id);
            open_vault_references(vault.as_ref(), instance, &mut params).await?;
            open_vault_references(vault.as_ref(), instance, &mut context).await?;
        }
        Ok((params, context))
    }

    async fn execute_inner(
        &self,
        claimed: &ClaimedTask,
        heartbeat: Duration,
        cancel: &CancellationToken,
    ) {
        let task = &claimed.task;
        let mut heartbeat = task.lease_secs.map_or(heartbeat, |lease| {
            heartbeat.min(heartbeat_interval(None, None, Some(lease)))
        });
        if let Some(cap) = self.settings.max_heartbeat {
            heartbeat = heartbeat.min(cap).max(Duration::from_millis(100));
        }
        let (params, context) = match self.prepare(task).await {
            Ok(prepared) => prepared,
            Err(message) => {
                self.settle(
                    claimed,
                    Outcome::Fail {
                        message,
                        retryable: false,
                    },
                )
                .await;
                return;
            }
        };
        let Some(handler) = self.registry.get(&task.handler_name) else {
            let message = format!(
                "handler '{}' is not served by this executor",
                task.handler_name
            );
            self.settle(
                claimed,
                Outcome::Fail {
                    message,
                    retryable: false,
                },
            )
            .await;
            return;
        };
        let mut with_context = task.clone();
        with_context.context = context;
        let ctx = crate::remote_worker::step_context(
            &with_context,
            self.tenant.clone(),
            params,
            Arc::clone(&self.scratch),
        );
        let timeout = task
            .timeout_ms
            .and_then(|ms| u64::try_from(ms).ok())
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis);
        let future = handler(ctx);
        let invocation = async move {
            match timeout {
                Some(limit) => tokio::time::timeout(limit, future).await.ok(),
                None => Some(future.await),
            }
        };
        tokio::pin!(invocation);
        let mut ticker =
            tokio::time::interval_at(tokio::time::Instant::now() + heartbeat, heartbeat);
        let result = loop {
            tokio::select! {
                result = &mut invocation => break result,
                _ = ticker.tick() => {
                    if self.transport.heartbeat(claimed).await == LeaseResponse::LostOwnership {
                        warn!(task_id = %task.id, "lease lost while executing; abandoning task");
                        self.counters.lost.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
                () = cancel.cancelled() => {
                    // Drain window elapsed: the handler future is dropped
                    // (stopped) and its effect may or may not have happened.
                    self.release(claimed, true).await;
                    return;
                }
            }
        };
        let Some(result) = result else {
            // Timed out: the outcome is ambiguous, not a failure. Release as
            // started so the engine marks the effect unknown and retries.
            warn!(task_id = %task.id, "task timed out on the executor; releasing it as started");
            self.release(claimed, true).await;
            return;
        };
        let outcome = self.outcome(task, result).await;
        self.settle(claimed, outcome).await;
    }

    /// The report for a handler result: output (sealed into the vault when
    /// one is configured) or failure.
    async fn outcome(&self, task: &RemoteTask, result: Result<Value, StepError>) -> Outcome {
        match result {
            Ok(output) => match &self.vault {
                Some(vault) => match seal_output(
                    vault.as_ref(),
                    InstanceId::from_uuid(task.instance_id),
                    &task.block_id,
                    output,
                    self.settings.externalize_bytes,
                )
                .await
                {
                    Ok(sealed) => Outcome::Complete(sealed),
                    // Never fall back to sending plaintext.
                    Err(message) => Outcome::Fail {
                        message,
                        retryable: true,
                    },
                },
                None => Outcome::Complete(output),
            },
            Err(StepError::Retryable { message, .. }) => Outcome::Fail {
                message,
                retryable: true,
            },
            Err(StepError::Permanent { message, .. }) => Outcome::Fail {
                message,
                retryable: false,
            },
        }
    }

    async fn settle(&self, claimed: &ClaimedTask, outcome: Outcome) {
        let mut backoff = Duration::from_millis(250);
        for attempt in 1..=DELIVERY_ATTEMPTS {
            let response = match &outcome {
                Outcome::Complete(output) => self.transport.complete(claimed, output).await,
                Outcome::Fail { message, retryable } => {
                    self.transport.fail(claimed, message, *retryable).await
                }
            };
            match response {
                LeaseResponse::Accepted => {
                    match outcome {
                        Outcome::Complete(_) => &self.counters.completed,
                        Outcome::Fail { .. } => &self.counters.failed,
                    }
                    .fetch_add(1, Ordering::Relaxed);
                    return;
                }
                LeaseResponse::LostOwnership => {
                    warn!(task_id = %claimed.task.id, "outcome rejected: the lease moved on");
                    self.counters.lost.fetch_add(1, Ordering::Relaxed);
                    return;
                }
                LeaseResponse::Rejected | LeaseResponse::Unsupported => {
                    warn!(task_id = %claimed.task.id, "the engine rejected the task outcome");
                    return;
                }
                LeaseResponse::Retry if attempt < DELIVERY_ATTEMPTS => {
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                }
                LeaseResponse::Retry => {}
            }
        }
        debug!(task_id = %claimed.task.id,
            "outcome undeliverable; the engine's lease reaper will reclaim the task");
    }
}

#[cfg(test)]
#[path = "remote_executor_tests.rs"]
mod tests;
