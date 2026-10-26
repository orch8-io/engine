//! Device-mesh delegation from phone-local parents (Feature 29).
//!
//! A workflow running on this phone's own engine can hand a
//! capability-specific **sub-step** or **sub-sequence** to another registered
//! runtime (a desktop, an edge box) through the server mailbox, park, and
//! resume with the result — surviving disconnects and app kills on both
//! sides, sharing no mutable execution state.
//!
//! ## What gets delegated
//!
//! The local scheduler already refuses to run a step whose `$runtime`
//! places it elsewhere: it parks the instance (`waiting`) and leaves a
//! local worker task, with a local effect receipt, in the phone's database.
//! The [`DelegationPump`] picks up every such task whose placement excludes
//! this phone — `$runtime.runtime_id` naming another runtime, or
//! `$runtime.runtime_kinds` without `mobile` (the destination is then
//! chosen among live registrations of those kinds that serve the handlers):
//!
//! * handler `orch8.delegation` — a **sub-sequence**: `params.sequence_id`
//!   names a sequence stored on the server, `params.input` is its explicit
//!   input;
//! * any other handler — an **isolated step**: the pump publishes (once, with
//!   a deterministic id) a one-step sequence running that handler on
//!   `{{context.data.params}}` and delegates it with `{"params": <params>}`.
//!
//! [`crate::MobileEngine::delegate`] is the explicit variant for hosts that
//! want to delegate from app code: same protocol, no parked step; the host
//! reads the outcome with `delegation_status`.
//!
//! ## Protocol (all through the existing control-plane endpoints)
//!
//! 1. `POST /continuity/executions {hosted_by_runtime: true}` registers the
//!    local parent's continuity identity (owner = this phone, epoch `e`);
//! 2. `POST /continuity/grants` mints a destination-bound one-time grant;
//! 3. `POST /continuity/delegations/claim` consumes it: the server anchors a
//!    mailbox task targeted at the destination on a delegation proxy;
//! 4. `GET /continuity/delegations/{id}` is polled until the destination's
//!    outcome is integrated (`completed` / `failed`).
//!
//! **Why polling and not the sync `commands` channel:** the outcome is a
//! durable server record keyed by the delegation id, so a poll is naturally
//! idempotent and loses nothing across disconnects or kills — the phone
//! just asks again. It rides the node client the worker already uses
//! (same credential, same `api_base_url`), needs no device registration or
//! HTTPS `sync_url`, and adds no server→device fan-out; a `step_result`
//! command would need its own at-least-once ack bookkeeping and could arrive
//! before the phone had even persisted the delegation.
//!
//! ## Crash and duplicate safety
//!
//! Each delegation is a row of `mobile_delegations`, inserted in the same
//! local transaction that claims the parked local task for the pump
//! (`worker_id = orch8.delegation-pump`), before any network call. Every
//! later step is persisted before the next network call, and every call is
//! idempotent or recoverable: an execution registration is re-answered for
//! the same owner, a claim whose response was lost is confirmed by reading
//! the delegation (and otherwise retried under a fresh delegation id), and
//! the resume is fenced twice — the parent must still be owned by this
//! phone at the delegation's epoch, and the local task must still be held
//! by the pump's claim epoch — so a duplicate result delivery or a restart
//! mid-resume completes the step exactly once and commits its local receipt
//! once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use sqlx::{Row, SqlitePool};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::delegation::DELEGATION_HANDLER;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{RuntimeCapabilities, RuntimeKind, RuntimeTrustLevel};
use orch8_types::worker::{WorkerClaim, WorkerTask};

use crate::error::MobileError;
use crate::node::NodeClient;

#[path = "delegation_scan.rs"]
mod scan;

const MAX_RETRY_HINTS: usize = 10_000;

/// Worker id under which the pump holds parked local tasks.
pub(crate) const PUMP_WORKER_ID: &str = "orch8.delegation-pump";
/// Delegations examined per pump pass.
const PUMP_BATCH: i64 = 50;
/// A delegation whose outcome cannot be read this long after it expired is
/// given up (the proxy was purged or the server lost it).
const RESULT_GRACE: chrono::Duration = chrono::Duration::minutes(10);
/// Resolved/abandoned delegations (input, result, grant) are kept this long
/// for `delegation_status`, then deleted (MC-09).
pub(crate) const FINISHED_RETENTION: chrono::Duration = chrono::Duration::days(30);
/// How long the live-runtime list is reused while placing delegations (MC-08).
const RUNTIMES_CACHE_TTL: Duration = Duration::from_secs(15);
/// Retry delay ceiling for a delegation that cannot be placed (MC-08).
const PLACEMENT_RETRY_CAP: Duration = Duration::from_secs(5 * 60);
/// Outcome polls stay at the base cadence for this many misses, then back off.
const POLL_FREE_MISSES: u32 = 5;
/// Outcome-poll ceiling once backed off.
const POLL_RETRY_CAP: Duration = Duration::from_secs(30);

/// Exponential delay `base * 2^attempts` capped at `cap`, scaled by `jitter`
/// (expected in `0.75..=1.25`; a parameter so the schedule is testable).
pub(crate) fn retry_delay(attempts: u32, base: Duration, cap: Duration, jitter: f64) -> Duration {
    let factor = 2f64.powi(i32::try_from(attempts.min(20)).unwrap_or(20));
    base.min(cap)
        .mul_f64(factor)
        .min(cap)
        .mul_f64(jitter.clamp(0.0, 2.0))
}

/// Per-delegation retry schedule (MC-08): a record whose last attempt failed
/// or found nothing is skipped by passes until its `next_at`.
#[derive(Default)]
struct RetryGate {
    entries: StdMutex<HashMap<String, (u32, Instant)>>,
}

impl RetryGate {
    fn due(&self, id: &str, now: Instant) -> bool {
        self.lock()
            .get(id)
            .is_none_or(|(_, next_at)| now >= *next_at)
    }

    fn attempts(&self, id: &str) -> u32 {
        self.lock().get(id).map_or(0, |(attempts, _)| *attempts)
    }

    /// Record a miss and schedule the next try `delay` from `now`.
    fn miss(&self, id: &str, now: Instant, delay: Duration) {
        let mut entries = self.lock();
        if !entries.contains_key(id) && entries.len() >= MAX_RETRY_HINTS {
            let oldest = entries
                .iter()
                .min_by_key(|(_, (_, next))| *next)
                .map(|(id, _)| id.clone());
            if let Some(oldest) = oldest {
                entries.remove(&oldest);
            }
        }
        let attempts = entries.get(id).map_or(0, |(attempts, _)| *attempts);
        entries.insert(id.to_owned(), (attempts.saturating_add(1), now + delay));
    }

    /// Forget a record after its durable state leaves the open set.
    fn forget(&self, id: &str) {
        self.lock().remove(id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, (u32, Instant)>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Delete delegations that finished (or were abandoned) before `cutoff`.
/// Returns the number removed.
pub(crate) async fn purge_finished(
    pool: &SqlitePool,
    cutoff: chrono::DateTime<chrono::Utc>,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        "DELETE FROM mobile_delegations \
         WHERE state IN ('resolved', 'abandoned') AND updated_at < ?",
    )
    .bind(cutoff.to_rfc3339())
    .execute(pool)
    .await?;
    Ok(result.rows_affected())
}

/// Options for `start_delegation`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct DelegationOptions {
    /// Tenant of the node credential (the control plane scopes every
    /// continuity call to it).
    pub tenant_id: String,
    /// How often pending delegations are advanced and their outcomes read
    /// (default 2 s). Push wake-ups advance them immediately.
    #[uniffi(default = 2000)]
    pub poll_interval_ms: u64,
    /// Lifetime of each grant and delegation (default 600 s, max 86400). A
    /// destination that has not reported by then fails the delegation, and
    /// the parked step follows its retry policy.
    #[uniffi(default = 600)]
    pub ttl_secs: u32,
}

/// An explicit delegation requested by the host (`delegate`).
#[derive(Debug, Clone, uniffi::Record)]
pub struct DelegateRequest {
    /// Local parent instance the delegation belongs to (must exist).
    pub instance_id: String,
    /// Destination runtime id (a live registration of the same tenant).
    pub destination_runtime_id: String,
    /// Server-side sequence the destination runs.
    pub sub_sequence_id: String,
    /// Explicit input (JSON object) handed to the sub-sequence.
    #[uniffi(default = "{}")]
    pub input_json: String,
}

/// Where a delegation stands, as recorded on this device.
#[derive(Debug, Clone, uniffi::Record)]
pub struct DelegationStatus {
    pub delegation_id: String,
    /// `preparing` (not yet accepted by the control plane), `delegated`
    /// (in the destination's mailbox or running there), `completed`,
    /// `failed`, or `abandoned` (never placed before its deadline).
    pub state: String,
    pub local_instance_id: String,
    /// The parked local step, for delegations made by a sequence.
    pub block_id: Option<String>,
    pub destination_runtime_id: Option<String>,
    /// The destination's reported output (JSON), once completed.
    pub output_json: Option<String>,
    pub error: Option<String>,
}

/// Counters exposed through `delegation_stats`.
#[derive(Debug, Clone, Default, uniffi::Record)]
pub struct DelegationStats {
    pub running: bool,
    /// Delegations accepted by the control plane.
    pub delegated: u64,
    pub completed: u64,
    pub failed: u64,
    pub abandoned: u64,
    /// Parked local steps resumed with an outcome (exactly once each).
    pub resumed: u64,
}

pub(crate) async fn init_tables(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS mobile_delegations (
            delegation_id          TEXT PRIMARY KEY,
            local_task_id          TEXT UNIQUE,
            local_instance_id      TEXT NOT NULL,
            block_id               TEXT,
            claim_epoch            INTEGER,
            kind                   TEXT NOT NULL,
            destination_runtime_id TEXT,
            runtime_kinds          TEXT,
            sub_sequence_id        TEXT,
            handler                TEXT,
            input                  TEXT NOT NULL,
            tenant_id              TEXT,
            continuity_id          TEXT,
            parent_epoch           INTEGER,
            grant_json             TEXT,
            expires_at             TEXT,
            state                  TEXT NOT NULL,
            outcome                TEXT,
            result                 TEXT,
            last_error             TEXT,
            deadline_at            TEXT NOT NULL,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_mobile_delegations_state ON mobile_delegations (state)",
    )
    .execute(pool)
    .await?;
    scan::init(pool).await?;
    Ok(())
}

/// How a delegation's outcome is delivered locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// A parked `orch8.delegation` step: resumed with the destination output.
    SubSequence,
    /// A parked isolated step: resumed with that step's output.
    Step,
    /// Requested by the host; the outcome is only recorded.
    Explicit,
}

impl Kind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::SubSequence => "sub_sequence",
            Self::Step => "step",
            Self::Explicit => "explicit",
        }
    }
}

/// One persisted delegation.
#[derive(Debug, Clone)]
struct Record {
    delegation_id: String,
    local_task_id: Option<String>,
    local_instance_id: String,
    block_id: Option<String>,
    claim_epoch: Option<u64>,
    kind: Kind,
    destination_runtime_id: Option<String>,
    runtime_kinds: Vec<RuntimeKind>,
    sub_sequence_id: Option<String>,
    handler: Option<String>,
    input: Value,
    tenant_id: Option<String>,
    continuity_id: Option<String>,
    parent_epoch: Option<u64>,
    grant: Option<Value>,
    expires_at: Option<String>,
    state: String,
    outcome: Option<String>,
    result: Option<Value>,
    last_error: Option<String>,
    deadline_at: chrono::DateTime<chrono::Utc>,
}

fn corrupt_record(field: &'static str) -> sqlx::Error {
    sqlx::Error::Decode(Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        format!("invalid delegation {field}"),
    )))
}

impl Record {
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Self, sqlx::Error> {
        let json = |column: &'static str| -> Result<Option<Value>, sqlx::Error> {
            row.try_get::<Option<String>, _>(column)?
                .map(|text| serde_json::from_str(&text).map_err(|_| corrupt_record(column)))
                .transpose()
        };
        let epoch = |column: &'static str| -> Result<Option<u64>, sqlx::Error> {
            row.try_get::<Option<i64>, _>(column)?
                .map(|value| u64::try_from(value).map_err(|_| corrupt_record(column)))
                .transpose()
        };
        let deadline: String = row.try_get("deadline_at")?;
        let record = Self {
            delegation_id: row.try_get("delegation_id")?,
            local_task_id: row.try_get("local_task_id")?,
            local_instance_id: row.try_get("local_instance_id")?,
            block_id: row.try_get("block_id")?,
            claim_epoch: epoch("claim_epoch")?,
            kind: match row.try_get::<String, _>("kind")?.as_str() {
                "sub_sequence" => Kind::SubSequence,
                "step" => Kind::Step,
                "explicit" => Kind::Explicit,
                _ => return Err(corrupt_record("kind")),
            },
            destination_runtime_id: row.try_get("destination_runtime_id")?,
            runtime_kinds: json("runtime_kinds")?
                .map(|kinds| {
                    serde_json::from_value(kinds).map_err(|_| corrupt_record("runtime_kinds"))
                })
                .transpose()?
                .unwrap_or_default(),
            sub_sequence_id: row.try_get("sub_sequence_id")?,
            handler: row.try_get("handler")?,
            input: json("input")?.ok_or_else(|| corrupt_record("input"))?,
            tenant_id: row.try_get("tenant_id")?,
            continuity_id: row.try_get("continuity_id")?,
            parent_epoch: epoch("parent_epoch")?,
            grant: json("grant_json")?,
            expires_at: row.try_get("expires_at")?,
            state: row.try_get("state")?,
            outcome: row.try_get("outcome")?,
            result: json("result")?,
            last_error: row.try_get("last_error")?,
            deadline_at: chrono::DateTime::parse_from_rfc3339(&deadline)
                .map_err(|_| corrupt_record("deadline_at"))?
                .with_timezone(&chrono::Utc),
        };
        if record.state == "claimed"
            && (record.parent_epoch.is_none() || record.continuity_id.is_none())
        {
            return Err(corrupt_record("claimed parent identity"));
        }
        if record.kind != Kind::Explicit
            && (record.local_task_id.is_none()
                || record.claim_epoch.is_none()
                || record.block_id.is_none())
        {
            return Err(corrupt_record("parked task identity"));
        }
        Ok(record)
    }

    fn status(&self) -> DelegationStatus {
        let state = match (self.state.as_str(), self.outcome.as_deref()) {
            ("resolved", Some(outcome)) => outcome.to_owned(),
            ("abandoned", _) => "abandoned".to_owned(),
            ("claimed", _) => "delegated".to_owned(),
            _ => "preparing".to_owned(),
        };
        let result = self.result.as_ref();
        DelegationStatus {
            delegation_id: self.delegation_id.clone(),
            state,
            local_instance_id: self.local_instance_id.clone(),
            block_id: self.block_id.clone(),
            destination_runtime_id: self.destination_runtime_id.clone(),
            output_json: result
                .and_then(|result| result.get("output"))
                .map(Value::to_string),
            error: result
                .and_then(|result| result["error"].as_str().map(str::to_owned))
                .or_else(|| {
                    (self.state == "abandoned")
                        .then(|| self.last_error.clone())
                        .flatten()
                }),
        }
    }
}

fn now_text() -> String {
    chrono::Utc::now().to_rfc3339()
}

fn storage_err(error: &sqlx::Error) -> MobileError {
    MobileError::Storage {
        detail: error.to_string(),
    }
}

/// Everything the pump needs; shared with the engine handle.
pub(crate) struct DelegationPump {
    client: Arc<NodeClient>,
    storage: Arc<dyn StorageBackend>,
    pool: SqlitePool,
    options: DelegationOptions,
    wake: Notify,
    stop: CancellationToken,
    scheduler_work: Arc<Notify>,
    /// Per-delegation retry schedule (MC-08).
    gate: RetryGate,
    /// Set by `wake()` (push, new work): the next pass ignores the schedule.
    force_pass: AtomicBool,
    /// Briefly cached `GET runtimes` answer (MC-08).
    runtimes_cache: StdMutex<Option<(Instant, Vec<RuntimeCapabilities>)>>,
    delegated: AtomicU64,
    completed: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    resumed: AtomicU64,
}

impl DelegationPump {
    pub(crate) fn new(
        client: Arc<NodeClient>,
        storage: Arc<dyn StorageBackend>,
        pool: SqlitePool,
        options: DelegationOptions,
        scheduler_work: Arc<Notify>,
    ) -> Result<Arc<Self>, MobileError> {
        if options.tenant_id.trim().is_empty() {
            return Err(MobileError::InvalidInput {
                detail: "delegation requires the credential's tenant_id".into(),
            });
        }
        if !(1..=86_400).contains(&options.ttl_secs) {
            return Err(MobileError::InvalidInput {
                detail: "delegation ttl_secs must be between 1 and 86400".into(),
            });
        }
        Ok(Arc::new(Self {
            client,
            storage,
            pool,
            options,
            wake: Notify::new(),
            stop: CancellationToken::new(),
            scheduler_work,
            gate: RetryGate::default(),
            force_pass: AtomicBool::new(false),
            runtimes_cache: StdMutex::new(None),
            delegated: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            abandoned: AtomicU64::new(0),
            resumed: AtomicU64::new(0),
        }))
    }

    pub(crate) fn spawn(self: &Arc<Self>, handle: &tokio::runtime::Handle) {
        let pump = Arc::clone(self);
        handle.spawn(async move {
            let interval = Duration::from_millis(pump.options.poll_interval_ms.max(50));
            loop {
                pump.run_once().await;
                tokio::select! {
                    () = pump.stop.cancelled() => break,
                    () = pump.wake.notified() => {}
                    () = tokio::time::sleep(interval) => {}
                }
            }
            debug!("delegation pump stopped");
        });
    }

    pub(crate) fn wake(&self) {
        self.force_pass.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }

    pub(crate) const fn ttl_secs(&self) -> u32 {
        self.options.ttl_secs
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.stop.is_cancelled()
    }

    pub(crate) fn stats(&self) -> DelegationStats {
        DelegationStats {
            running: !self.is_stopped(),
            delegated: self.delegated.load(Ordering::Relaxed),
            completed: self.completed.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            resumed: self.resumed.load(Ordering::Relaxed),
        }
    }

    /// One pass: park newly delegable local steps, advance delegations that
    /// the control plane has not accepted yet, and read outcomes.
    pub(crate) async fn run_once(&self) {
        if let Err(error) = self.discover().await {
            warn!(%error, "delegation discovery failed");
        }
        let records = match self.load_open().await {
            Ok(records) => records,
            Err(error) => {
                warn!(%error, "failed to read pending delegations");
                return;
            }
        };
        let force = self.force_pass.swap(false, Ordering::AcqRel);
        for record in &records {
            if self.stop.is_cancelled() {
                break;
            }
            let id = record.delegation_id.clone();
            let claimed = record.state == "claimed";
            if !force && !self.gate.due(&id, Instant::now()) {
                continue;
            }
            let outcome = match record.state.as_str() {
                "prepared" => self.advance(record.clone()).await.map(|()| false),
                "claimed" => self.poll_outcome(record.clone()).await,
                _ => Ok(false),
            };
            if let Err(error) = outcome {
                debug!(%error, "delegation step deferred");
                self.schedule_retry(&id, claimed);
            } else if matches!(outcome, Ok(true)) {
                // Still waiting on the destination; terminal transitions
                // explicitly forget their retry hint.
                self.schedule_retry(&id, true);
            }
        }
    }

    /// Back off a delegation that did not make progress: placement retries
    /// grow to [`PLACEMENT_RETRY_CAP`]; outcome polls keep the base cadence
    /// for [`POLL_FREE_MISSES`] misses, then grow to [`POLL_RETRY_CAP`].
    fn schedule_retry(&self, id: &str, polling: bool) {
        let base = Duration::from_millis(self.options.poll_interval_ms.max(50));
        let attempts = self.gate.attempts(id);
        let jitter = 0.75 + rand::random::<f64>() * 0.5;
        let delay = if polling {
            let exponent = attempts.saturating_add(1).saturating_sub(POLL_FREE_MISSES);
            retry_delay(exponent, base, POLL_RETRY_CAP, jitter)
        } else {
            retry_delay(
                attempts.saturating_add(1),
                base,
                PLACEMENT_RETRY_CAP,
                jitter,
            )
        };
        self.gate.miss(id, Instant::now(), delay);
    }

    // ------------------------------------------------------------------
    // Discovery: parked local steps placed off this runtime
    // ------------------------------------------------------------------

    async fn discover(&self) -> Result<(), MobileError> {
        let own = self.client.runtime_id();
        let candidates = scan::next_page(&self.pool, &scan::DISCOVERY, PUMP_BATCH)
            .await
            .map_err(|error| storage_err(&error))?;
        for row in candidates {
            let id: String = row.try_get("id").map_err(|error| storage_err(&error))?;
            let requirements: String = row
                .try_get("requirements")
                .map_err(|error| storage_err(&error))?;
            let Ok(requirements) =
                serde_json::from_str::<orch8_types::continuity::CapsuleRequirements>(&requirements)
            else {
                continue;
            };
            let placed_elsewhere = match requirements.runtime_id {
                Some(target) => target != own,
                None => {
                    !requirements.runtime_kinds.is_empty()
                        && !requirements.runtime_kinds.contains(&RuntimeKind::Mobile)
                }
            };
            if !placed_elsewhere {
                continue;
            }
            let Ok(uuid) = uuid::Uuid::parse_str(&id) else {
                continue;
            };
            let task = match self.storage.get_worker_task(uuid).await {
                Ok(Some(task)) => task,
                Ok(None) => continue,
                Err(error) => {
                    warn!(task_id = %uuid, %error, "invalid delegation task retained; continuing discovery");
                    continue;
                }
            };
            if let Err(error) = self.park(&task).await {
                warn!(task_id = %task.id, %error, "failed to take a placed step for delegation");
            }
        }
        Ok(())
    }

    /// Claim the parked local task for the pump and record its delegation in
    /// one local transaction, before anything touches the network.
    async fn park(&self, task: &WorkerTask) -> Result<(), MobileError> {
        let (kind, sub_sequence_id, input) = if task.handler_name == DELEGATION_HANDLER {
            let Some(sequence) = task.params.get("sequence_id").and_then(Value::as_str) else {
                return self
                    .reject_unclaimed(task, "an orch8.delegation step needs params.sequence_id")
                    .await;
            };
            (
                Kind::SubSequence,
                Some(sequence.to_owned()),
                task.params
                    .get("input")
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            )
        } else {
            let mut params = task.params.clone();
            if let Some(object) = params.as_object_mut() {
                object.remove("__orch8");
            }
            (Kind::Step, None, json!({ "params": params }))
        };
        let now = chrono::Utc::now();
        let deadline = now + chrono::Duration::seconds(i64::from(self.options.ttl_secs));
        let delegation_id = uuid::Uuid::now_v7().to_string();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|error| storage_err(&error))?;
        let claimed = sqlx::query(
            "UPDATE worker_tasks SET state = 'claimed', worker_id = ?1, claimed_at = ?2, \
             heartbeat_at = ?2, claim_epoch = claim_epoch + 1 \
             WHERE id = ?3 AND state = 'pending' AND awaiting_dispatch = 0",
        )
        .bind(PUMP_WORKER_ID)
        .bind(now.to_rfc3339())
        .bind(task.id.to_string())
        .execute(&mut *tx)
        .await
        .map_err(|error| storage_err(&error))?
        .rows_affected();
        if claimed != 1 {
            return Ok(());
        }
        let claim_epoch: i64 =
            sqlx::query_scalar("SELECT claim_epoch FROM worker_tasks WHERE id = ?")
                .bind(task.id.to_string())
                .fetch_one(&mut *tx)
                .await
                .map_err(|error| storage_err(&error))?;
        sqlx::query(
            "INSERT INTO mobile_delegations (delegation_id, local_task_id, local_instance_id, \
             block_id, claim_epoch, kind, destination_runtime_id, runtime_kinds, \
             sub_sequence_id, handler, input, state, deadline_at, created_at, updated_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'prepared', ?12, ?13, ?13)",
        )
        .bind(&delegation_id)
        .bind(task.id.to_string())
        .bind(task.instance_id.to_string())
        .bind(task.block_id.as_str())
        .bind(claim_epoch)
        .bind(kind.as_str())
        .bind(task.requirements.runtime_id.map(|id| id.to_string()))
        .bind(serde_json::to_string(&task.requirements.runtime_kinds)?)
        .bind(sub_sequence_id)
        .bind(&task.handler_name)
        .bind(input.to_string())
        .bind(deadline.to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(&mut *tx)
        .await
        .map_err(|error| storage_err(&error))?;
        tx.commit().await.map_err(|error| storage_err(&error))?;
        info!(
            task_id = %task.id,
            instance_id = %task.instance_id,
            block_id = %task.block_id,
            delegation_id,
            "local step parked for delegation"
        );
        Ok(())
    }

    /// A placed step that can never be delegated: claim it and fail it
    /// permanently (the step definition itself is wrong).
    async fn reject_unclaimed(&self, task: &WorkerTask, message: &str) -> Result<(), MobileError> {
        let claimed = sqlx::query(
            "UPDATE worker_tasks SET state = 'claimed', worker_id = ?1, claim_epoch = claim_epoch + 1 \
             WHERE id = ?2 AND state = 'pending'",
        )
        .bind(PUMP_WORKER_ID)
        .bind(task.id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|error| storage_err(&error))?
        .rows_affected();
        if claimed != 1 {
            return Ok(());
        }
        let Some(task) = self.storage.get_worker_task(task.id).await? else {
            return Ok(());
        };
        let Some(instance) = self.storage.get_instance(task.instance_id).await? else {
            return Ok(());
        };
        let claim = WorkerClaim::new(PUMP_WORKER_ID.to_owned(), task.claim_epoch);
        orch8_engine::worker_lease::fail_worker_task(
            self.storage.as_ref(),
            &instance,
            &task,
            &claim,
            message,
            false,
        )
        .await?;
        self.scheduler_work.notify_one();
        Ok(())
    }

    /// Record an explicit delegation requested by the host.
    pub(crate) async fn record_explicit(
        pool: &SqlitePool,
        request: &DelegateRequest,
        input: &Value,
        ttl_secs: u32,
    ) -> Result<String, MobileError> {
        let now = chrono::Utc::now();
        let delegation_id = uuid::Uuid::now_v7().to_string();
        sqlx::query(
            "INSERT INTO mobile_delegations (delegation_id, local_instance_id, kind, \
             destination_runtime_id, sub_sequence_id, input, state, deadline_at, created_at, \
             updated_at) VALUES (?1, ?2, 'explicit', ?3, ?4, ?5, 'prepared', ?6, ?7, ?7)",
        )
        .bind(&delegation_id)
        .bind(&request.instance_id)
        .bind(&request.destination_runtime_id)
        .bind(&request.sub_sequence_id)
        .bind(input.to_string())
        .bind((now + chrono::Duration::seconds(i64::from(ttl_secs))).to_rfc3339())
        .bind(now.to_rfc3339())
        .execute(pool)
        .await
        .map_err(|error| storage_err(&error))?;
        Ok(delegation_id)
    }

    pub(crate) async fn status(
        pool: &SqlitePool,
        delegation_id: &str,
    ) -> Result<Option<DelegationStatus>, MobileError> {
        let row = sqlx::query("SELECT * FROM mobile_delegations WHERE delegation_id = ?")
            .bind(delegation_id)
            .fetch_optional(pool)
            .await
            .map_err(|error| storage_err(&error))?;
        row.map(|row| Record::from_row(&row).map(|record| record.status()))
            .transpose()
            .map_err(|error| storage_err(&error))
    }

    pub(crate) async fn list(pool: &SqlitePool) -> Result<Vec<DelegationStatus>, MobileError> {
        let rows = sqlx::query("SELECT * FROM mobile_delegations ORDER BY created_at")
            .fetch_all(pool)
            .await
            .map_err(|error| storage_err(&error))?;
        rows.iter()
            .map(|row| Record::from_row(row).map(|record| record.status()))
            .collect::<Result<_, _>>()
            .map_err(|error| storage_err(&error))
    }

    async fn load_open(&self) -> Result<Vec<Record>, MobileError> {
        let rows = scan::next_page(&self.pool, &scan::OPEN, PUMP_BATCH)
            .await
            .map_err(|error| storage_err(&error))?;
        let mut records = Vec::with_capacity(rows.len());
        for row in rows {
            match Record::from_row(&row) {
                Ok(record) => records.push(record),
                Err(error) => {
                    let id: &str = row.try_get("delegation_id").unwrap_or("unknown");
                    warn!(delegation_id = id, %error, "invalid delegation record retained; continuing scan");
                }
            }
        }
        Ok(records)
    }

    async fn update(
        &self,
        delegation_id: &str,
        // `'static` keeps the interpolated SET clause a compile-time literal.
        sets: &'static str,
        binds: Vec<Option<String>>,
    ) -> Result<(), MobileError> {
        let sql =
            format!("UPDATE mobile_delegations SET {sets}, updated_at = ? WHERE delegation_id = ?");
        let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
        for bind in binds {
            query = query.bind(bind);
        }
        query
            .bind(now_text())
            .bind(delegation_id)
            .execute(&self.pool)
            .await
            .map_err(|error| storage_err(&error))?;
        Ok(())
    }

    async fn note_error(&self, record: &Record, error: &str) -> Result<(), MobileError> {
        debug!(delegation_id = %record.delegation_id, error, "delegation not placed yet");
        self.schedule_retry(&record.delegation_id, false);
        self.update(
            &record.delegation_id,
            "last_error = ?",
            vec![Some(error.to_owned())],
        )
        .await
    }

    // ------------------------------------------------------------------
    // Placement: execution identity, grant, claim
    // ------------------------------------------------------------------

    fn tenant(&self) -> &str {
        &self.options.tenant_id
    }

    // One linear placement protocol; each stage persists before the next
    // network call, so splitting it would only scatter that ordering.
    #[allow(clippy::too_many_lines)]
    async fn advance(&self, mut record: Record) -> Result<(), MobileError> {
        if chrono::Utc::now() >= record.deadline_at {
            let reason = format!(
                "delegation could not be placed before its deadline: {}",
                record
                    .last_error
                    .as_deref()
                    .unwrap_or("control plane unreachable")
            );
            return self.give_up(&record, &reason).await;
        }

        // 1. Destination: named, or chosen among live runtimes of the kinds.
        if record.destination_runtime_id.is_none() {
            match self.choose_destination(&record).await? {
                Some(destination) => {
                    self.update(
                        &record.delegation_id,
                        "destination_runtime_id = ?",
                        vec![Some(destination.clone())],
                    )
                    .await?;
                    record.destination_runtime_id = Some(destination);
                }
                None => {
                    return self
                        .note_error(
                            &record,
                            "no live runtime of the placed kinds serves this step",
                        )
                        .await;
                }
            }
        }

        // 2. Sub-sequence: an isolated step runs as a published one-step
        //    sequence with a deterministic id.
        if record.sub_sequence_id.is_none() {
            let sequence = self.ensure_step_sequence(&record).await?;
            self.update(
                &record.delegation_id,
                "sub_sequence_id = ?",
                vec![Some(sequence.clone())],
            )
            .await?;
            record.sub_sequence_id = Some(sequence);
        }

        // 3. The local parent's continuity identity on the control plane.
        if record.continuity_id.is_none() {
            let (status, body) = self
                .client
                .post_json(
                    "continuity/executions",
                    &json!({
                        "tenant_id": self.tenant(),
                        "instance_id": record.local_instance_id,
                        "runtime_id": self.client.runtime_id(),
                        "hosted_by_runtime": true,
                    }),
                )
                .await?;
            if !(200..300).contains(&status) {
                return self
                    .reject_or_retry(&record, status, &format!("register parent: {body}"))
                    .await;
            }
            let continuity = body["continuity_id"].as_str().map(str::to_owned);
            let epoch = body["epoch"].as_u64();
            let tenant = body["tenant_id"].as_str().map(str::to_owned);
            self.update(
                &record.delegation_id,
                "continuity_id = ?, parent_epoch = ?, tenant_id = ?",
                vec![
                    continuity.clone(),
                    epoch.map(|epoch| epoch.to_string()),
                    tenant.clone(),
                ],
            )
            .await?;
            record.continuity_id = continuity;
            record.parent_epoch = epoch;
            record.tenant_id = tenant;
        }

        // 4. A destination-bound one-time grant (reused until consumed).
        if record.grant.is_none() {
            let (status, body) = self
                .client
                .post_json(
                    "continuity/grants",
                    &json!({
                        "tenant_id": self.tenant(),
                        "continuity_id": record.continuity_id,
                        "destination_runtime_id": record.destination_runtime_id,
                        "allowed_actions": ["accept"],
                        "ttl_seconds": self.options.ttl_secs,
                    }),
                )
                .await?;
            if !(200..300).contains(&status) {
                return self
                    .reject_or_retry(&record, status, &format!("grant: {body}"))
                    .await;
            }
            let expires_at = body["signed_grant"]["grant"]["expires_at"]
                .as_str()
                .map(str::to_owned);
            self.update(
                &record.delegation_id,
                "grant_json = ?, expires_at = ?",
                vec![Some(body.to_string()), expires_at.clone()],
            )
            .await?;
            record.grant = Some(body);
            record.expires_at = expires_at;
        }

        // 5. The claim. Its response can be lost; the delegation is then
        //    confirmed by reading it back.
        let grant = record.grant.clone().unwrap_or(Value::Null);
        let tenant = record
            .tenant_id
            .clone()
            .unwrap_or_else(|| self.tenant().to_owned());
        // An isolated step's one-step sequence is published by the control
        // plane with the claim (a device session cannot author sequences).
        let step = match (&record.handler, record.kind) {
            (Some(handler), Kind::Step) => json!({
                "handler": handler,
                "block_id": record.block_id.clone().unwrap_or_else(|| "step".into()),
            }),
            _ => Value::Null,
        };
        let (status, body) = self
            .client
            .post_json(
                "continuity/delegations/claim",
                &json!({
                    "tenant_id": tenant,
                    "delegation": {
                        "id": record.delegation_id,
                        "tenant_id": tenant,
                        "parent_continuity_id": record.continuity_id,
                        "parent_epoch": record.parent_epoch,
                        "source_runtime_id": self.client.runtime_id(),
                        "destination_runtime_id": record.destination_runtime_id,
                        "sub_sequence_id": record.sub_sequence_id,
                        "grant_id": grant["signed_grant"]["grant"]["id"],
                        "expires_at": record.expires_at,
                    },
                    "signed_grant": grant["signed_grant"],
                    "token": grant["token"],
                    "input": record.input,
                    "step": step,
                }),
            )
            .await?;
        if (200..300).contains(&status) {
            return self.mark_claimed(&record).await;
        }
        if status == 409 {
            let (read, _) = self.read_delegation(&record.delegation_id).await?;
            if read == 200 {
                return self.mark_claimed(&record).await;
            }
            // Not placed (destination unreachable, grant already spent by a
            // claim that never enqueued): retry under a fresh identity; the
            // old grant expires unused.
            let fresh = uuid::Uuid::now_v7().to_string();
            sqlx::query(
                "UPDATE mobile_delegations SET delegation_id = ?, grant_json = NULL, \
                 expires_at = NULL, last_error = ?, updated_at = ? WHERE delegation_id = ?",
            )
            .bind(&fresh)
            .bind(format!("claim: {body}"))
            .bind(now_text())
            .bind(&record.delegation_id)
            .execute(&self.pool)
            .await
            .map_err(|error| storage_err(&error))?;
            return Ok(());
        }
        self.reject_or_retry(&record, status, &format!("claim: {body}"))
            .await
    }

    async fn mark_claimed(&self, record: &Record) -> Result<(), MobileError> {
        self.update(
            &record.delegation_id,
            "state = 'claimed', last_error = ?",
            vec![None],
        )
        .await?;
        self.gate.forget(&record.delegation_id);
        self.delegated.fetch_add(1, Ordering::Relaxed);
        info!(
            delegation_id = %record.delegation_id,
            destination = record.destination_runtime_id.as_deref().unwrap_or(""),
            "delegation placed in the destination's mailbox"
        );
        Ok(())
    }

    /// Transient answers (unreachable/expired registrations, 5xx, 429) are
    /// retried until the deadline; a definitive rejection gives up now.
    async fn reject_or_retry(
        &self,
        record: &Record,
        status: u16,
        detail: &str,
    ) -> Result<(), MobileError> {
        let transient = matches!(status, 408 | 409 | 425 | 429 | 500..=599);
        let message = format!("HTTP {status}: {detail}");
        if transient {
            self.note_error(record, &message).await
        } else {
            self.give_up(record, &format!("delegation rejected: {message}"))
                .await
        }
    }

    async fn live_runtimes(&self) -> Result<Option<Vec<RuntimeCapabilities>>, MobileError> {
        if let Some((fetched_at, runtimes)) = self
            .runtimes_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            && fetched_at.elapsed() < RUNTIMES_CACHE_TTL
        {
            return Ok(Some(runtimes.clone()));
        }
        let (status, body) = self
            .client
            .get_json(&format!("runtimes?tenant_id={}", encode(self.tenant())))
            .await?;
        if status != 200 {
            return Ok(None);
        }
        let runtimes: Vec<RuntimeCapabilities> = serde_json::from_value(body).unwrap_or_default();
        *self
            .runtimes_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some((Instant::now(), runtimes.clone()));
        Ok(Some(runtimes))
    }

    async fn choose_destination(&self, record: &Record) -> Result<Option<String>, MobileError> {
        let Some(runtimes) = self.live_runtimes().await? else {
            return Ok(None);
        };
        let needed: Vec<String> = match (&record.sub_sequence_id, &record.handler) {
            (None, Some(handler)) => vec![handler.clone()],
            _ => Vec::new(),
        };
        let now = chrono::Utc::now();
        let own = self.client.runtime_id();
        let mut candidates: Vec<&RuntimeCapabilities> = runtimes
            .iter()
            .filter(|runtime| {
                runtime.runtime_id != own
                    && record.runtime_kinds.contains(&runtime.kind)
                    && runtime.trust >= RuntimeTrustLevel::Registered
                    && !runtime.draining
                    && runtime.expires_at > now
                    && runtime
                        .handlers
                        .iter()
                        .any(|handler| handler == DELEGATION_HANDLER)
                    && needed
                        .iter()
                        .all(|handler| runtime.handlers.contains(handler))
            })
            .collect();
        candidates.sort_by_key(|runtime| runtime.runtime_id.to_string());
        Ok(candidates
            .first()
            .map(|runtime| runtime.runtime_id.to_string()))
    }

    /// The deterministic id of the one-step sequence an isolated step runs
    /// as. With a device session the control plane publishes it when the
    /// delegation is claimed; a legacy API key publishes it here
    /// (idempotently), which also works against servers that predate
    /// control-plane publishing.
    async fn ensure_step_sequence(&self, record: &Record) -> Result<String, MobileError> {
        let handler = record.handler.clone().unwrap_or_default();
        let block = record.block_id.clone().unwrap_or_else(|| "step".into());
        let id = orch8_engine::delegation::step_sequence_id(self.tenant(), &handler, &block);
        if self.client.is_device_session() {
            return Ok(id.to_string());
        }
        let (status, body) = self
            .client
            .post_json(
                "sequences",
                &orch8_engine::delegation::step_sequence_document(self.tenant(), &handler, &block),
            )
            .await?;
        if (200..300).contains(&status) || status == 409 {
            let (read, _) = self.client.get_json(&format!("sequences/{id}")).await?;
            if read == 200 {
                return Ok(id.to_string());
            }
        }
        Err(MobileError::Engine {
            detail: format!("publish delegated step sequence: HTTP {status}: {body}"),
        })
    }

    // ------------------------------------------------------------------
    // Outcome: read, fence, resume exactly once
    // ------------------------------------------------------------------

    async fn read_delegation(&self, delegation_id: &str) -> Result<(u16, Value), MobileError> {
        self.client
            .get_json(&format!(
                "continuity/delegations/{delegation_id}?tenant_id={}",
                encode(self.tenant())
            ))
            .await
    }

    async fn poll_outcome(&self, record: Record) -> Result<bool, MobileError> {
        let expected_epoch = record
            .parent_epoch
            .ok_or_else(|| storage_err(&corrupt_record("parent_epoch")))?;
        let continuity = record
            .continuity_id
            .as_deref()
            .ok_or_else(|| storage_err(&corrupt_record("continuity_id")))?;
        let (status, body) = self.read_delegation(&record.delegation_id).await?;
        if status != 200 {
            let expired = record
                .expires_at
                .as_deref()
                .and_then(|at| chrono::DateTime::parse_from_rfc3339(at).ok())
                .is_some_and(|at| {
                    chrono::Utc::now() > at.with_timezone(&chrono::Utc) + RESULT_GRACE
                });
            if status == 404 && expired {
                self.resolve(
                    &record,
                    Err(("delegation outcome was lost".into(), true)),
                    &Value::Null,
                )
                .await?;
                return Ok(false);
            }
            return Ok(true);
        }
        if body["delegation"]["id"].as_str() != Some(record.delegation_id.as_str())
            || body["delegation"]["parent_continuity_id"].as_str() != Some(continuity)
        {
            return Err(MobileError::Engine {
                detail: "control plane returned an unrelated delegation record".into(),
            });
        }
        let delivered = match body["status"].as_str() {
            Some("completed") => Ok(()),
            Some("failed") => Err(()),
            _ => return Ok(true),
        };
        let result = body["result"].clone();
        // Fence: the parent must still be this phone's, at the epoch the
        // delegation was granted under.
        let own = self.client.runtime_id().to_string();
        let fenced = body["delegation"]["source_runtime_id"].as_str() != Some(own.as_str())
            || body["parent_owner_runtime_id"].as_str() != Some(own.as_str())
            || body["delegation"]["parent_epoch"].as_u64() != Some(expected_epoch)
            || body["parent_epoch_now"].as_u64() != Some(expected_epoch);
        let outcome = if fenced {
            Err((
                "delegation fenced: the parent's ownership moved since it was delegated".to_owned(),
                false,
            ))
        } else {
            match delivered {
                Ok(()) => Ok(output_for(&record, &result)),
                Err(()) => Err((
                    format!(
                        "delegated to {} and failed: {}",
                        result["runtime_id"].as_str().unwrap_or("destination"),
                        result["error"].as_str().unwrap_or("unknown error")
                    ),
                    true,
                )),
            }
        };
        self.resolve(&record, outcome, &result).await?;
        Ok(false)
    }

    /// Deliver an outcome locally and close the record. Safe to repeat.
    async fn resolve(
        &self,
        record: &Record,
        outcome: Result<Value, (String, bool)>,
        result: &Value,
    ) -> Result<(), MobileError> {
        if record.kind != Kind::Explicit {
            let (Some(task), Some(epoch)) = (record.local_task_id.as_deref(), record.claim_epoch)
            else {
                return Ok(());
            };
            let task_id = uuid::Uuid::parse_str(task).map_err(|error| MobileError::Storage {
                detail: format!("corrupt delegation task id: {error}"),
            })?;
            let claim = WorkerClaim::new(PUMP_WORKER_ID.to_owned(), epoch);
            let resumed = orch8_engine::delegation::resume_local_parent(
                self.storage.as_ref(),
                task_id,
                &claim,
                &record.delegation_id,
                result,
                match &outcome {
                    Ok(output) => Ok(output),
                    Err((message, retryable)) => Err((message.as_str(), *retryable)),
                },
            )
            .await?;
            if resumed {
                self.resumed.fetch_add(1, Ordering::Relaxed);
                self.scheduler_work.notify_one();
            }
        }
        let label = if outcome.is_ok() {
            "completed"
        } else {
            "failed"
        };
        let result = if result.is_null() {
            outcome.as_ref().err().map_or(
                Value::Null,
                |(message, _)| json!({"status": "failed", "error": message}),
            )
        } else {
            result.clone()
        };
        self.update(
            &record.delegation_id,
            "state = 'resolved', outcome = ?, result = ?",
            vec![Some(label.to_owned()), Some(result.to_string())],
        )
        .await?;
        self.gate.forget(&record.delegation_id);
        if outcome.is_ok() {
            self.completed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        info!(delegation_id = %record.delegation_id, outcome = label, "delegation resolved");
        Ok(())
    }

    /// The delegation could not be placed: the parked step fails retryably
    /// (its retry policy decides) and the record is closed.
    async fn give_up(&self, record: &Record, reason: &str) -> Result<(), MobileError> {
        warn!(delegation_id = %record.delegation_id, reason, "delegation abandoned");
        if let (Some(task), Some(epoch)) = (record.local_task_id.as_deref(), record.claim_epoch)
            && let Ok(task_id) = uuid::Uuid::parse_str(task)
        {
            let claim = WorkerClaim::new(PUMP_WORKER_ID.to_owned(), epoch);
            if orch8_engine::delegation::resume_local_parent(
                self.storage.as_ref(),
                task_id,
                &claim,
                &record.delegation_id,
                &json!({"status": "abandoned", "error": reason}),
                Err((reason, true)),
            )
            .await?
            {
                self.scheduler_work.notify_one();
            }
        }
        self.update(
            &record.delegation_id,
            "state = 'abandoned', last_error = ?",
            vec![Some(reason.to_owned())],
        )
        .await?;
        self.gate.forget(&record.delegation_id);
        self.abandoned.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// The local step output for a delivered delegation: an isolated step gets
/// its own block's output when the destination reported block outputs (the
/// orch8 runtime's `{"outputs": {<block>: …}}` report), otherwise the
/// destination's output as reported.
fn output_for(record: &Record, result: &Value) -> Value {
    let reported = result.get("output").cloned().unwrap_or(Value::Null);
    if record.kind == Kind::Step
        && let Some(block) = record.block_id.as_deref()
        && let Some(own) = reported
            .get("outputs")
            .and_then(|outputs| outputs.get(block))
    {
        return own.clone();
    }
    reported
}

/// Deterministic id of the one-step sequence an isolated step publishes.
/// Percent-encode a query value.
fn encode(value: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(byte));
        } else {
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_sequence_ids_are_deterministic_and_scoped() {
        use orch8_engine::delegation::step_sequence_id;
        let a = step_sequence_id("t", "scan", "step");
        assert_eq!(a, step_sequence_id("t", "scan", "step"));
        assert_ne!(a, step_sequence_id("u", "scan", "step"));
        assert_ne!(a, step_sequence_id("t", "scan2", "step"));
        assert_ne!(
            step_sequence_id("t", "ab", "c"),
            step_sequence_id("t", "a", "bc"),
            "parts are delimited"
        );
    }

    #[test]
    fn mc08_retry_delay_grows_exponentially_with_jitter_and_a_cap() {
        let base = Duration::from_secs(2);
        let cap = Duration::from_secs(300);
        assert_eq!(retry_delay(1, base, cap, 1.0), Duration::from_secs(4));
        assert_eq!(retry_delay(3, base, cap, 1.0), Duration::from_secs(16));
        assert_eq!(retry_delay(30, base, cap, 1.0), cap, "capped");
        assert_eq!(retry_delay(1, base, cap, 0.75), Duration::from_secs(3));
        assert_eq!(retry_delay(1, base, cap, 1.25), Duration::from_secs(5));
        assert_eq!(
            retry_delay(u32::MAX, base, cap, 1.0),
            cap,
            "huge attempt counts must not overflow"
        );
    }

    #[test]
    fn mc08_a_failed_delegation_is_skipped_until_its_backoff_elapses() {
        let gate = RetryGate::default();
        let t0 = Instant::now();
        assert!(gate.due("d1", t0), "unknown records are always due");

        gate.miss("d1", t0, Duration::from_secs(4));
        assert_eq!(gate.attempts("d1"), 1);
        assert!(!gate.due("d1", t0 + Duration::from_secs(2)));
        assert!(gate.due("d1", t0 + Duration::from_secs(4)));
        assert!(gate.due("d2", t0), "other records are unaffected");

        gate.miss("d1", t0, Duration::from_secs(8));
        assert_eq!(gate.attempts("d1"), 2);

        gate.forget("d1");
        assert_eq!(gate.attempts("d1"), 0, "closed records are forgotten");
    }

    #[tokio::test]
    async fn mc09_finished_delegations_are_purged_after_retention_only() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        init_tables(&pool).await.unwrap();
        let now = chrono::Utc::now();
        let old = (now - chrono::Duration::days(31)).to_rfc3339();
        let recent = (now - chrono::Duration::days(2)).to_rfc3339();
        for (id, state, updated) in [
            ("old-resolved", "resolved", &old),
            ("old-abandoned", "abandoned", &old),
            ("old-open", "claimed", &old),
            ("recent-resolved", "resolved", &recent),
        ] {
            sqlx::query(
                "INSERT INTO mobile_delegations (delegation_id, local_instance_id, kind, input, \
                 state, deadline_at, created_at, updated_at) VALUES (?1, 'i', 'explicit', '{}', ?2, ?3, ?3, ?3)",
            )
            .bind(id)
            .bind(state)
            .bind(updated)
            .execute(&pool)
            .await
            .unwrap();
        }

        let removed = purge_finished(&pool, now - FINISHED_RETENTION)
            .await
            .unwrap();
        assert_eq!(removed, 2);
        let left: Vec<String> = sqlx::query_scalar(
            "SELECT delegation_id FROM mobile_delegations ORDER BY delegation_id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(left, vec!["old-open", "recent-resolved"]);
    }

    #[test]
    fn query_values_are_percent_encoded() {
        assert_eq!(encode("tenant a/b"), "tenant%20a%2Fb");
        assert_eq!(encode("e2e-01"), "e2e-01");
    }

    fn record(kind: Kind) -> Record {
        Record {
            delegation_id: "d".into(),
            local_task_id: None,
            local_instance_id: "i".into(),
            block_id: Some("scan".into()),
            claim_epoch: None,
            kind,
            destination_runtime_id: None,
            runtime_kinds: Vec::new(),
            sub_sequence_id: None,
            handler: None,
            input: json!({}),
            tenant_id: None,
            continuity_id: None,
            parent_epoch: None,
            grant: None,
            expires_at: None,
            state: "claimed".into(),
            outcome: None,
            result: None,
            last_error: None,
            deadline_at: chrono::Utc::now(),
        }
    }

    #[test]
    fn isolated_steps_unwrap_their_block_output() {
        let result = json!({"output": {"outputs": {"scan": {"text": "ok"}}, "state": "completed"}});
        assert_eq!(
            output_for(&record(Kind::Step), &result),
            json!({"text": "ok"})
        );
        assert_eq!(
            output_for(&record(Kind::SubSequence), &result),
            result["output"],
            "a sub-sequence resumes with the whole report"
        );
        let bare = json!({"output": {"text": "bare"}});
        assert_eq!(
            output_for(&record(Kind::Step), &bare),
            json!({"text": "bare"})
        );
    }

    #[test]
    fn status_maps_record_states() {
        let mut open = record(Kind::Explicit);
        assert_eq!(open.status().state, "delegated");
        open.state = "prepared".into();
        assert_eq!(open.status().state, "preparing");
        open.state = "resolved".into();
        open.outcome = Some("completed".into());
        open.result = Some(json!({"status": "completed", "output": {"n": 1}}));
        let status = open.status();
        assert_eq!(status.state, "completed");
        assert_eq!(status.output_json.as_deref(), Some("{\"n\":1}"));
        open.state = "abandoned".into();
        open.result = None;
        open.last_error = Some("no destination".into());
        let status = open.status();
        assert_eq!(status.state, "abandoned");
        assert_eq!(status.error.as_deref(), Some("no destination"));
    }
}

#[cfg(test)]
#[path = "delegation_scan_tests.rs"]
mod scan_tests;
