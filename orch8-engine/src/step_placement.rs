//! Placement policies at dispatch: residency, capability labels, sticky
//! affinity, tenant policies, rate budgets, and the autoscaling backlog
//! metrics (see `docs/PLACEMENT.md`).
//!
//! Everything compiles into the existing `$runtime` capability requirements
//! of the worker task, so the single claim predicate
//! ([`orch8_types::worker::claim_allowed`]) enforces placement for HTTP
//! polls, queue polls, and the gRPC stream on every storage backend.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use moka::future::Cache;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{CapsuleRequirements, RuntimeCapabilities};
use orch8_types::ids::{InstanceId, SequenceId, TenantId};
use orch8_types::instance::TaskInstance;
use orch8_types::placement::{
    Affinity, PLACEMENT_METADATA_KEY, PLACEMENT_UNSATISFIED, Placement, PlacementPolicies,
    PlacementPreference, PlacementTarget, PriorityLane, RateBudgetCheck, ResolvedPlacement,
};
use orch8_types::sequence::StepDef;
use orch8_types::worker::{RUNTIME_REQUIREMENTS_PARAM, WorkerTask, WorkerTaskState};

use crate::error::EngineError;

/// How long a node trusts its cached copy of a tenant's policies. A `PUT`
/// invalidates the local node immediately; other nodes converge within this.
pub const POLICY_CACHE_TTL: Duration = Duration::from_secs(5);

static POLICY_CACHE: LazyLock<Cache<TenantId, Arc<PlacementPolicies>>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(POLICY_CACHE_TTL)
        .build()
});

/// `(sequence name, sequence-level placement)` by id. Sequence versions are
/// immutable, so a longer TTL is safe.
/// `(sequence name, sequence-level placement)`.
type SequencePlacement = Arc<(String, Option<Placement>)>;

static SEQUENCE_CACHE: LazyLock<Cache<SequenceId, SequencePlacement>> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(Duration::from_secs(60))
        .build()
});

/// Drop this node's cached policies for `tenant_id` (called after a `PUT`).
pub async fn invalidate_policies(tenant_id: &TenantId) {
    POLICY_CACHE.invalidate(tenant_id).await;
}

async fn tenant_policies(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
) -> Result<Arc<PlacementPolicies>, EngineError> {
    if let Some(cached) = POLICY_CACHE.get(tenant_id).await {
        return Ok(cached);
    }
    let policies = Arc::new(storage.get_placement_policies(tenant_id).await?);
    POLICY_CACHE
        .insert(tenant_id.clone(), Arc::clone(&policies))
        .await;
    Ok(policies)
}

async fn sequence_placement(
    storage: &dyn StorageBackend,
    sequence_id: SequenceId,
) -> Result<SequencePlacement, EngineError> {
    if let Some(cached) = SEQUENCE_CACHE.get(&sequence_id).await {
        return Ok(cached);
    }
    let entry = Arc::new(
        storage
            .get_sequence(sequence_id)
            .await?
            .map(|sequence| (sequence.name, sequence.placement))
            .unwrap_or_default(),
    );
    SEQUENCE_CACHE.insert(sequence_id, Arc::clone(&entry)).await;
    Ok(entry)
}

fn instance_tags(instance: &TaskInstance) -> Vec<&str> {
    instance
        .metadata
        .get("tags")
        .and_then(serde_json::Value::as_array)
        .map(|tags| tags.iter().filter_map(serde_json::Value::as_str).collect())
        .unwrap_or_default()
}

/// Worker id of the instance's most recently completed worker task (the
/// sticky-affinity target).
async fn last_worker(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<Option<String>, EngineError> {
    let filter = orch8_types::worker_filter::WorkerTaskFilter {
        instance_id: Some(instance_id),
        states: Some(vec![WorkerTaskState::Completed]),
        ..Default::default()
    };
    let pagination = orch8_types::filter::Pagination {
        offset: 0,
        limit: 1,
        sort_ascending: false,
    };
    Ok(storage
        .list_worker_tasks(&filter, &pagination)
        .await?
        .into_iter()
        .next()
        .and_then(|task| task.worker_id))
}

/// The step's effective placement (step + sequence + tenant policies),
/// without touching params. `Ok(None)` when nothing applies — including every
/// built-in that manipulates engine state
/// ([`orch8_types::sequence::is_engine_only_builtin`]): those always run on
/// the engine node. `Err(message)` is a contradictory placement.
async fn resolve_step_placement(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
) -> Result<Result<Option<ResolvedPlacement>, String>, EngineError> {
    if orch8_types::sequence::is_engine_only_builtin(&step_def.handler) {
        return Ok(Ok(None));
    }
    let sequence = sequence_placement(storage, instance.sequence_id).await?;
    let policies = tenant_policies(storage, &instance.tenant_id).await?;
    if step_def.placement.is_none() && sequence.1.is_none() && policies.items.is_empty() {
        return Ok(Ok(None));
    }
    let tags = instance_tags(instance);
    let target = PlacementTarget {
        sequence_name: &sequence.0,
        handler: &step_def.handler,
        tags: &tags,
    };
    match orch8_types::placement::resolve(
        step_def.placement.as_ref(),
        sequence.1.as_ref(),
        &policies.items,
        &target,
    ) {
        Ok(resolved) if resolved.is_empty() => Ok(Ok(None)),
        Ok(resolved) => Ok(Ok(Some(resolved))),
        Err(message) => Ok(Err(message)),
    }
}

/// Whether the engine must leave this step's `credentials://` references
/// unresolved for the executor (hybrid mode).
///
/// A step with hard placement (region, labels, residency — from the step,
/// its sequence, or a tenant policy) is always dispatched to remote
/// runtimes. Its credential references are resolved **on the claiming
/// executor** from the executor's local credentials, so secrets never pass
/// through (or are stored by) the control plane; the task requires the
/// referenced ids as runtime credential facts. Every other step keeps
/// engine-side resolution. A contradictory placement returns `false`: the
/// step is rejected before dispatch anyway.
pub async fn defers_credentials(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
) -> Result<bool, EngineError> {
    if crate::handlers::PluginKind::detect(&step_def.handler).is_some() {
        return Ok(false);
    }
    Ok(matches!(
        resolve_step_placement(storage, instance, step_def).await?,
        Ok(Some(resolved)) if resolved.has_hard_constraints()
    ))
}

/// Resolve the step's effective placement (step + sequence + tenant
/// policies) and merge it into `params.$runtime`.
///
/// Returns the resolved placement (`None` when nothing applies, in which
/// case `params` is untouched). `Err(message)` is a permanent dispatch
/// rejection (contradictory placement), reported like any other invalid
/// `$runtime`.
pub async fn apply_step_placement(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
    params: &mut serde_json::Value,
    now: DateTime<Utc>,
) -> Result<Result<Option<ResolvedPlacement>, String>, EngineError> {
    let resolved = match resolve_step_placement(storage, instance, step_def).await? {
        Ok(Some(resolved)) => resolved,
        other => return Ok(other),
    };
    // An unparsable `$runtime` is rejected by the regular dispatch path with
    // its precise message; leave it alone here.
    let Ok(mut requirements) = orch8_types::worker::peek_runtime_requirements(params) else {
        return Ok(Ok(None));
    };
    if let Err(message) =
        orch8_types::placement::apply_hard_constraints(&resolved, &mut requirements)
    {
        return Ok(Err(message));
    }
    if resolved.has_hard_constraints() {
        // Hybrid: a hard-placed step's `credentials://` references are
        // resolved on the executor that claims it (see
        // [`defers_credentials`]), so only an executor that holds every
        // referenced credential may claim the task.
        for id in orch8_types::worker::credential_reference_ids(params) {
            if !requirements.credentials.contains(&id) {
                requirements.credentials.push(id);
            }
        }
    }
    let affinity_worker = if resolved.affinity == Affinity::Instance {
        last_worker(storage, instance.id).await?
    } else {
        None
    };
    if affinity_worker.is_some() || !resolved.prefer_labels.is_empty() {
        let wait_ms = i64::try_from(resolved.affinity_wait_ms).unwrap_or(i64::MAX);
        requirements.prefer = Some(PlacementPreference {
            worker_id: affinity_worker,
            labels: resolved.prefer_labels.clone(),
            until_ms: now.timestamp_millis().saturating_add(wait_ms),
        });
    }
    if params.is_null() {
        *params = serde_json::Value::Object(serde_json::Map::new());
    }
    let Some(object) = params.as_object_mut() else {
        return Ok(Err(
            "placement requires object step params (got a non-object value)".to_string(),
        ));
    };
    object.insert(
        RUNTIME_REQUIREMENTS_PARAM.to_string(),
        serde_json::to_value(&requirements)
            .map_err(orch8_types::error::StorageError::Serialization)?,
    );
    Ok(Ok(Some(resolved)))
}

fn eligible_ignoring_preference(
    requirements: &CapsuleRequirements,
    carries_credentials: bool,
    candidate: &RuntimeCapabilities,
    now: DateTime<Utc>,
) -> bool {
    let mut hard = requirements.clone();
    hard.prefer = None;
    orch8_types::worker::claim_allowed(&hard, carries_credentials, candidate, now)
}

/// After a placed task was enqueued: when no live runtime satisfies its hard
/// placement, record the visible `placement_unsatisfied` reason on the
/// instance (metadata + audit event + counter). The task stays pending — it
/// is never handed to a non-matching runtime.
pub async fn record_placement_status(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    task: &WorkerTask,
) {
    if !task.requirements.has_placement_facts() {
        return;
    }
    let now = Utc::now();
    let candidates = match storage
        .list_runtime_capabilities(&instance.tenant_id, now, 1_000)
        .await
    {
        Ok(candidates) => candidates,
        Err(error) => {
            tracing::warn!(%error, "placement status: runtime lookup failed");
            return;
        }
    };
    if candidates.iter().any(|candidate| {
        eligible_ignoring_preference(&task.requirements, task.carries_credentials, candidate, now)
    }) {
        return;
    }
    let detail = serde_json::json!({
        "status": PLACEMENT_UNSATISFIED,
        "block_id": task.block_id.as_str(),
        "task_id": task.id,
        "since": now.to_rfc3339(),
        "regions": task.requirements.regions,
        "labels": task.requirements.labels,
        "residency": task.requirements.residency,
    });
    tracing::warn!(
        instance_id = %instance.id,
        block_id = %task.block_id,
        handler = %task.handler_name,
        "no live runtime satisfies the step placement; the task waits (placement_unsatisfied)"
    );
    crate::metrics::inc(crate::metrics::PLACEMENT_UNSATISFIED_TOTAL);
    if let Err(error) = storage
        .merge_instance_metadata(
            instance.id,
            &serde_json::json!({ PLACEMENT_METADATA_KEY: detail.clone() }),
        )
        .await
    {
        tracing::warn!(%error, "placement status: metadata write failed");
    }
    crate::lifecycle::audit_event(
        storage,
        instance.id,
        &instance.tenant_id,
        PLACEMENT_UNSATISFIED,
        Some(task.block_id.as_str()),
        detail,
    )
    .await;
}

/// Record on the instance that a placed task was claimed (clears a previous
/// `placement_unsatisfied`). Called by the worker API after a claim.
pub async fn record_placement_claimed(storage: &dyn StorageBackend, task: &WorkerTask) {
    if !task.requirements.has_placement_facts() && task.requirements.prefer.is_none() {
        return;
    }
    let patch = serde_json::json!({
        PLACEMENT_METADATA_KEY: {
            "status": "placed",
            "block_id": task.block_id.as_str(),
            "task_id": task.id,
            "worker_id": task.worker_id,
            "at": Utc::now().to_rfc3339(),
        }
    });
    if let Err(error) = storage
        .merge_instance_metadata(task.instance_id, &patch)
        .await
    {
        tracing::warn!(%error, task_id = %task.id, "placement status: claim write failed");
    }
}

/// Take one token from the step's global rate budget. `Some(retry_after)`
/// defers the instance; an unconfigured budget does not gate.
pub async fn rate_budget_retry_at(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    step_def: &StepDef,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, EngineError> {
    let Some(key) = &step_def.rate_budget else {
        return Ok(None);
    };
    match storage
        .take_rate_budget_token(&instance.tenant_id, key, now)
        .await?
    {
        RateBudgetCheck::Allowed => Ok(None),
        RateBudgetCheck::Unconfigured => {
            tracing::debug!(
                instance_id = %instance.id,
                rate_budget = %key,
                "rate budget not configured; step not gated"
            );
            Ok(None)
        }
        RateBudgetCheck::Deferred { retry_after } => {
            tracing::info!(
                instance_id = %instance.id,
                block_id = %step_def.id,
                rate_budget = %key,
                retry_after = %retry_after,
                "rate budget exhausted, deferring instance"
            );
            crate::metrics::inc(crate::metrics::RATE_BUDGET_DEFERRED);
            Ok(Some(retry_after))
        }
    }
}

/// Labels of one `orch8_queue_depth` series.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DepthLabels {
    pub capability: String,
    pub region: String,
    pub priority_lane: &'static str,
}

/// One scrape of the pending worker-task backlog.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BacklogSnapshot {
    /// `orch8_queue_depth{capability,region,priority_lane}`.
    pub depth: BTreeMap<DepthLabels, u64>,
    /// `orch8_placement_unsatisfied{capability,region}`: pending tasks with
    /// hard placement that no live runtime satisfies.
    pub unsatisfied: BTreeMap<(String, String), u64>,
}

/// How often every node refreshes the backlog gauges.
pub const BACKLOG_METRICS_INTERVAL: Duration = Duration::from_secs(15);

/// Maximum backlog groups read per scrape (bounds query cost and label
/// cardinality).
pub const BACKLOG_GROUP_LIMIT: u32 = 2_000;

/// Read the pending backlog and aggregate it into metric series.
pub async fn backlog_snapshot(
    storage: &dyn StorageBackend,
) -> Result<BacklogSnapshot, EngineError> {
    let rows = storage
        .pending_worker_task_depth(BACKLOG_GROUP_LIMIT)
        .await?;
    let now = Utc::now();
    let mut snapshot = BacklogSnapshot::default();
    let mut runtimes: HashMap<String, Vec<RuntimeCapabilities>> = HashMap::new();
    for row in rows {
        let region = orch8_types::placement::region_label(&row.requirements);
        let labels = DepthLabels {
            capability: row.handler_name.clone(),
            region: region.clone(),
            priority_lane: PriorityLane::for_priority(row.priority).as_str(),
        };
        *snapshot.depth.entry(labels).or_default() += row.count;
        if !row.requirements.has_placement_facts() {
            continue;
        }
        if !runtimes.contains_key(&row.tenant_id) {
            let live = storage
                .list_runtime_capabilities(&TenantId::unchecked(row.tenant_id.clone()), now, 1_000)
                .await?;
            runtimes.insert(row.tenant_id.clone(), live);
        }
        let satisfied = runtimes[&row.tenant_id].iter().any(|candidate| {
            eligible_ignoring_preference(&row.requirements, false, candidate, now)
        });
        if !satisfied {
            *snapshot
                .unsatisfied
                .entry((row.handler_name, region))
                .or_default() += row.count;
        }
    }
    Ok(snapshot)
}

/// Publish a snapshot as gauges. Series present in `previous` but absent
/// now are set to 0 so drained queues scale down.
#[allow(clippy::cast_precision_loss)]
pub fn publish_backlog(snapshot: &BacklogSnapshot, previous: &BacklogSnapshot) {
    for labels in previous.depth.keys() {
        if !snapshot.depth.contains_key(labels) {
            set_depth(labels, 0.0);
        }
    }
    for (labels, count) in &snapshot.depth {
        set_depth(labels, *count as f64);
    }
    for key in previous.unsatisfied.keys() {
        if !snapshot.unsatisfied.contains_key(key) {
            set_unsatisfied(key, 0.0);
        }
    }
    for (key, count) in &snapshot.unsatisfied {
        set_unsatisfied(key, *count as f64);
    }
}

fn set_depth(labels: &DepthLabels, value: f64) {
    metrics::gauge!(
        crate::metrics::QUEUE_DEPTH,
        "capability" => labels.capability.clone(),
        "region" => labels.region.clone(),
        "priority_lane" => labels.priority_lane,
    )
    .set(value);
}

fn set_unsatisfied((capability, region): &(String, String), value: f64) {
    metrics::gauge!(
        crate::metrics::PLACEMENT_UNSATISFIED,
        "capability" => capability.clone(),
        "region" => region.clone(),
    )
    .set(value);
}

/// Periodically publish the backlog gauges until `cancel` fires. Every node
/// reports the same cluster-wide backlog: aggregate with `max()`, not
/// `sum()`, in autoscaler queries.
pub async fn run_backlog_metrics_loop(
    storage: Arc<dyn StorageBackend>,
    interval: Duration,
    cancel: tokio_util::sync::CancellationToken,
) {
    let mut previous = BacklogSnapshot::default();
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {}
        }
        match backlog_snapshot(storage.as_ref()).await {
            Ok(snapshot) => {
                publish_backlog(&snapshot, &previous);
                previous = snapshot;
            }
            Err(error) => tracing::warn!(%error, "queue backlog metrics scrape failed"),
        }
    }
}
