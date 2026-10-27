//! Crash-safe classification for externally visible step effects.
//!
//! The guard deliberately provides at-most-once safety, not exactly-once
//! semantics. Once dispatch is durably recorded, any ambiguous outcome blocks
//! automatic retry until an operator or verifier resolves the receipt.

use chrono::Utc;
use orch8_publisher::manifest::canonical_json;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, EffectDispatchOutcome, EffectId, EffectKind, EffectReceipt,
    EffectState, ExecutionEpoch, OwnershipState, RuntimeId,
};
use orch8_types::continuity_advanced::{InvariantRule, WorkflowInvariant};
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use uuid::Uuid;

use crate::dataflow::hex_sha256;
use crate::error::EngineError;

/// Single-use capability to move a prepared native effect across the durable
/// dispatch boundary. Its fields are private and it is never deserialized.
#[derive(Clone)]
pub struct EffectAdmission {
    inner: Arc<Admission>,
}

struct Admission {
    storage: Arc<dyn StorageBackend>,
    receipt: EffectReceipt,
    fence: orch8_storage::EffectDispatchFence,
    started: AtomicBool,
    admitted: AtomicBool,
    failure: Mutex<Option<EngineError>>,
}

impl EffectAdmission {
    /// Read the prepared effect's identity only for its original invocation.
    /// Reading neither admits dispatch nor consumes the transition capability.
    pub fn effect_id_for(
        &self,
        tenant: &TenantId,
        instance: InstanceId,
        block: &BlockId,
        attempt: u32,
    ) -> Option<EffectId> {
        let receipt = &self.inner.receipt;
        (receipt.tenant_id == *tenant
            && self.inner.fence.instance_id == instance
            && receipt.block_id == *block
            && receipt.attempt == attempt)
            .then_some(receipt.id)
    }

    /// Call only after native preflight/cancellation checks and before host code.
    /// Once this starts, interruption is an ambiguous durable dispatch window.
    pub async fn admit(&self) -> Result<(), orch8_types::error::StepError> {
        if self.inner.started.swap(true, Ordering::AcqRel) {
            return Err(orch8_types::error::StepError::ClaimLost);
        }
        let mut guard = EffectGuard {
            storage: self.inner.storage.as_ref(),
            receipt: self.inner.receipt.clone(),
            fence: self.inner.fence,
        };
        if let Err(error) = guard.dispatch().await {
            *self
                .inner
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
            return Err(orch8_types::error::StepError::ClaimLost);
        }
        self.inner.admitted.store(true, Ordering::Release);
        Ok(())
    }

    /// Refuse an owned preflight before dispatch while preserving its typed
    /// scheduler outcome. Consumes this capability: later admission is refused.
    /// Once dispatch has started, rejection cannot relabel its ambiguous outcome.
    pub fn reject_preflight(&self, error: EngineError) -> orch8_types::error::StepError {
        if self
            .inner
            .started
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            *self
                .inner
                .failure
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(error);
        }
        orch8_types::error::StepError::ClaimLost
    }

    pub(crate) fn is_admitted(&self) -> bool {
        self.inner.admitted.load(Ordering::Acquire)
    }

    pub(crate) fn take_failure(&self) -> Option<EngineError> {
        self.inner
            .failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
    }
}

pub(crate) struct EffectGuard<'a> {
    storage: &'a dyn StorageBackend,
    receipt: EffectReceipt,
    fence: orch8_storage::EffectDispatchFence,
}

impl<'a> EffectGuard<'a> {
    pub(crate) async fn begin(
        storage: &'a dyn StorageBackend,
        tenant_id: &TenantId,
        instance_id: InstanceId,
        block_id: &BlockId,
        handler: &str,
        params: &Value,
        attempt: u32,
    ) -> Result<Option<Self>, EngineError> {
        Self::begin_with_claim(
            storage,
            tenant_id,
            instance_id,
            block_id,
            handler,
            params,
            attempt,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // original claim is part of durable effect admission
    pub(crate) async fn begin_with_claim(
        storage: &'a dyn StorageBackend,
        tenant_id: &TenantId,
        instance_id: InstanceId,
        block_id: &BlockId,
        handler: &str,
        params: &Value,
        attempt: u32,
        claim_token: Option<chrono::DateTime<Utc>>,
    ) -> Result<Option<Self>, EngineError> {
        if !crate::release_diff::handler_has_side_effects(handler) {
            return Ok(None);
        }
        let Some(mut guard) = Self::prepare_with_claim(
            storage,
            tenant_id,
            instance_id,
            block_id,
            handler,
            params,
            attempt,
            claim_token,
        )
        .await?
        else {
            return Ok(None);
        };
        guard.dispatch().await?;
        Ok(Some(guard))
    }

    pub(crate) async fn begin_step(
        storage: &'a dyn StorageBackend,
        instance: &orch8_types::instance::TaskInstance,
        step: &orch8_types::sequence::StepDef,
        params: &Value,
        attempt: u32,
        dry_run: bool,
    ) -> Result<Option<Self>, EngineError> {
        if dry_run {
            return Ok(None);
        }
        Self::begin_with_claim(
            storage,
            &instance.tenant_id,
            instance.id,
            &step.id,
            &step.handler,
            params,
            attempt,
            instance.claimed_at,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn prepare(
        storage: &'a dyn StorageBackend,
        tenant_id: &TenantId,
        instance_id: InstanceId,
        block_id: &BlockId,
        handler: &str,
        params: &Value,
        attempt: u32,
    ) -> Result<Option<Self>, EngineError> {
        Self::prepare_with_claim(
            storage,
            tenant_id,
            instance_id,
            block_id,
            handler,
            params,
            attempt,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)] // original claim is part of durable effect admission
    pub(crate) async fn prepare_with_claim(
        storage: &'a dyn StorageBackend,
        tenant_id: &TenantId,
        instance_id: InstanceId,
        block_id: &BlockId,
        handler: &str,
        params: &Value,
        attempt: u32,
        claim_token: Option<chrono::DateTime<Utc>>,
    ) -> Result<Option<Self>, EngineError> {
        let execution = ensure_effect_scope(storage, tenant_id, instance_id).await?;
        if execution.state != OwnershipState::Owned || execution.current_instance_id != instance_id
        {
            return Err(EngineError::ClaimLost { instance_id });
        }

        if let Some(mut unresolved) = storage
            .find_unresolved_effect_receipt(
                tenant_id,
                execution.continuity_id,
                instance_id,
                block_id,
                attempt,
            )
            .await?
        {
            if unresolved.state == EffectState::Dispatched {
                let expected = unresolved.state;
                unresolved
                    .transition(EffectState::Unknown, Utc::now())
                    .map_err(|error| EngineError::EffectTransition(error.to_string()))?;
                let _ = storage
                    .cas_effect_receipt(tenant_id, unresolved.id, expected, &unresolved)
                    .await?;
            }
            return Err(blocked(&unresolved));
        }

        let legacy_id = legacy_effect_id(
            execution.continuity_id,
            execution.epoch,
            instance_id,
            block_id,
            attempt,
        );
        let now = Utc::now();
        let mut candidate = EffectReceipt {
            id: EffectId::new(),
            tenant_id: tenant_id.clone(),
            continuity_id: execution.continuity_id,
            epoch: execution.epoch,
            instance_id,
            block_id: block_id.clone(),
            kind: effect_kind(handler),
            state: EffectState::Planned,
            destination_fingerprint: destination_fingerprint(handler, params)?,
            idempotency_key: params
                .get("idempotency_key")
                .and_then(Value::as_str)
                .filter(|value| value.len() <= 256)
                .map(ToOwned::to_owned),
            request_sha256: request_hash(handler, params)?,
            provider_receipt_id: None,
            attempt,
            created_at: now,
            updated_at: now,
        };
        let receipt = storage.ensure_effect_attempt(&candidate, legacy_id).await?;
        candidate.id = receipt.id;
        if !same_effect_identity(&receipt, &candidate) {
            return Err(EngineError::InvalidConfig(
                "effect attempt contains different effect evidence".into(),
            ));
        }
        let mut guard = Self {
            storage,
            receipt,
            fence: orch8_storage::EffectDispatchFence {
                instance_id,
                epoch: execution.epoch,
                owner_runtime_id: execution.owner_runtime_id,
                claim_token,
            },
        };
        match guard.receipt.state {
            EffectState::Planned => guard.advance(EffectState::Prepared).await?,
            EffectState::Prepared => {}
            EffectState::Dispatched => {
                guard.advance(EffectState::Unknown).await?;
                return Err(blocked(&guard.receipt));
            }
            _ => return Err(blocked(&guard.receipt)),
        }
        Ok(Some(guard))
    }

    pub(crate) fn admission(&self, storage: Arc<dyn StorageBackend>) -> EffectAdmission {
        EffectAdmission {
            inner: Arc::new(Admission {
                storage,
                receipt: self.receipt.clone(),
                fence: self.fence,
                started: AtomicBool::new(false),
                admitted: AtomicBool::new(false),
                failure: Mutex::new(None),
            }),
        }
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), EngineError> {
        let current = self
            .storage
            .get_effect_receipt(&self.receipt.tenant_id, self.receipt.id)
            .await?
            .ok_or_else(|| {
                EngineError::InvalidConfig("native effect receipt disappeared".into())
            })?;
        if !same_effect_identity(&current, &self.receipt) {
            return Err(EngineError::InvalidConfig(
                "native effect identity changed".into(),
            ));
        }
        self.receipt = current;
        Ok(())
    }

    /// Durable provider id of this active attempt. External worker tasks
    /// store it so settlement never has to recompute it.
    pub(crate) fn effect_id(&self) -> EffectId {
        self.receipt.id
    }

    pub(crate) async fn commit(mut self, output: &Value) -> Result<(), EngineError> {
        self.receipt.provider_receipt_id = provider_receipt_id(output);
        self.advance(EffectState::Committed).await
    }

    pub(crate) async fn mark_unknown(mut self) -> Result<(), EngineError> {
        // Native work cancelled before durable admission is still safely prepared.
        if self.receipt.state == EffectState::Prepared {
            return Ok(());
        }
        self.advance(EffectState::Unknown).await
    }

    pub(crate) async fn dispatch(&mut self) -> Result<(), EngineError> {
        let guards =
            load_effect_commit_guards(self.storage, &self.receipt, self.fence.instance_id).await?;
        let mut updated = self.receipt.clone();
        updated
            .transition(EffectState::Dispatched, Utc::now())
            .map_err(|error| EngineError::EffectTransition(error.to_string()))?;
        match self
            .storage
            .dispatch_effect_receipt_owned(
                &updated.tenant_id,
                &updated,
                &self.fence,
                !guards.is_empty(),
            )
            .await?
        {
            EffectDispatchOutcome::Dispatched => {
                self.receipt = updated;
                if !guards.is_empty() {
                    record_effect_guard_results(
                        self.storage,
                        &guards,
                        &self.receipt,
                        self.fence.epoch,
                        false,
                    )
                    .await?;
                }
                Ok(())
            }
            EffectDispatchOutcome::Duplicate => {
                let Some(invariant) = guards.first() else {
                    return Err(EngineError::InvalidConfig(
                        "unguarded effect dispatch returned a duplicate verdict".into(),
                    ));
                };
                record_effect_guard_results(
                    self.storage,
                    &guards,
                    &updated,
                    self.fence.epoch,
                    true,
                )
                .await?;
                Err(EngineError::InvariantViolation {
                    invariant_id: invariant.id,
                    block_id: updated.block_id,
                })
            }
            EffectDispatchOutcome::Stale => {
                let current = self
                    .storage
                    .get_effect_receipt(&updated.tenant_id, updated.id)
                    .await?
                    .unwrap_or(self.receipt.clone());
                if matches!(current.state, EffectState::Planned | EffectState::Prepared) {
                    Err(EngineError::ClaimLost {
                        instance_id: self.fence.instance_id,
                    })
                } else {
                    Err(blocked(&current))
                }
            }
        }
    }

    async fn advance(&mut self, next: EffectState) -> Result<(), EngineError> {
        let expected = self.receipt.state;
        let mut updated = self.receipt.clone();
        updated
            .transition(next, Utc::now())
            .map_err(|error| EngineError::EffectTransition(error.to_string()))?;
        if !self
            .storage
            .cas_effect_receipt(&updated.tenant_id, updated.id, expected, &updated)
            .await?
        {
            let current = self
                .storage
                .get_effect_receipt(&updated.tenant_id, updated.id)
                .await?
                .unwrap_or(self.receipt.clone());
            return Err(blocked(&current));
        }
        self.receipt = updated;
        Ok(())
    }
}

/// Every side-effecting instance needs a durable receipt namespace, including
/// ordinary local runs that were never explicitly enrolled in portable
/// continuity. Deriving both IDs from the instance makes concurrent first
/// effects converge on one scope across processes and runtimes.
pub async fn ensure_effect_scope(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    instance_id: InstanceId,
) -> Result<ContinuityExecution, EngineError> {
    let now = Utc::now();
    let candidate = ContinuityExecution {
        continuity_id: ContinuityId::from_uuid(deterministic_scope_uuid(
            b"orch8-effect-scope-v1\0",
            instance_id,
        )),
        tenant_id: tenant_id.clone(),
        current_instance_id: instance_id,
        owner_runtime_id: RuntimeId::from_uuid(deterministic_scope_uuid(
            b"orch8-effect-runtime-v1\0",
            instance_id,
        )),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    Ok(storage.ensure_continuity_execution(&candidate).await?)
}

fn deterministic_scope_uuid(domain: &[u8], instance_id: InstanceId) -> Uuid {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(instance_id.into_uuid().as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Uuid::from_bytes(bytes)
}

async fn load_effect_commit_guards(
    storage: &dyn StorageBackend,
    receipt: &EffectReceipt,
    current_instance: InstanceId,
) -> Result<Vec<WorkflowInvariant>, EngineError> {
    let Some(instance) = storage.get_instance(current_instance).await? else {
        return Ok(Vec::new());
    };
    let Some(sequence) = storage.get_sequence(instance.sequence_id).await? else {
        return Ok(Vec::new());
    };
    let invariants = storage
        .load_effect_guards(
            &receipt.tenant_id,
            receipt.continuity_id,
            sequence.id,
            sequence.version,
            Some(receipt.kind),
        )
        .await?;
    Ok(invariants
        .into_iter()
        .filter(|invariant| {
            invariant.commit_guard
                && matches!(
                    invariant.rule,
                    InvariantRule::EffectAtMostOnce { kind } if kind == receipt.kind
                )
        })
        .collect())
}

async fn record_effect_guard_results(
    storage: &dyn StorageBackend,
    guards: &[WorkflowInvariant],
    candidate: &EffectReceipt,
    current_epoch: ExecutionEpoch,
    include_candidate: bool,
) -> Result<(), EngineError> {
    let mut receipts = storage
        .list_effect_receipts(&candidate.tenant_id, candidate.continuity_id, 10_000)
        .await?;
    if include_candidate {
        receipts.retain(|receipt| receipt.id != candidate.id);
        receipts.push(candidate.clone());
    }
    let output_paths = std::collections::BTreeSet::new();
    let evidence = crate::continuity_advanced::InvariantEvidence {
        receipts: &receipts,
        terminal_state: None,
        budget_breached: None,
        output_paths: &output_paths,
    };
    let now = Utc::now();
    for invariant in guards {
        let result = crate::continuity_advanced::evaluate_invariant(
            invariant,
            candidate.continuity_id,
            current_epoch,
            &evidence,
            now,
        );
        let _ = storage
            .append_invariant_result(&candidate.tenant_id, &result)
            .await?;
    }
    Ok(())
}

/// How an external worker task's effect receipt is settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerEffectSettlement {
    /// The node reported success: `dispatched` (or `unknown`, e.g. after a
    /// racing lease resolution) becomes `committed`.
    Committed,
    /// The outcome is ambiguous (worker failure, lease expiry after start,
    /// release after start): `dispatched` becomes `unknown`.
    Unknown,
    /// No node ever started the task (it expired while still `pending`):
    /// the effect provably did not happen, so the receipt is `abandoned`.
    Abandoned,
}

/// Commit the receipt created when an external worker task was dispatched.
/// Legacy and non-continuity tasks are intentionally a no-op.
pub async fn commit_external_worker_effect(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
    output: &Value,
) -> Result<(), EngineError> {
    settle_worker_task_effect(
        storage,
        tenant_id,
        task,
        WorkerEffectSettlement::Committed,
        Some(output),
    )
    .await
}

/// Mark an external worker effect uncertain before applying retry policy.
pub async fn mark_external_worker_effect_unknown(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
) -> Result<(), EngineError> {
    settle_worker_task_effect(
        storage,
        tenant_id,
        task,
        WorkerEffectSettlement::Unknown,
        None,
    )
    .await
}

/// The id of the effect receipt bound to a worker task: the `effect_id`
/// stored on the task when its attempt was dispatched. The id is never
/// recomputed (a recomputation uses the *current* owner epoch and misses
/// after a handoff). Rows written before the column existed carry no id;
/// for those the attempt's still-open receipt is looked up by its recorded
/// identity (instance, block, attempt) instead.
async fn bound_effect_receipt_id(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
) -> Result<Option<EffectId>, EngineError> {
    if let Some(id) = task.effect_id {
        return Ok(Some(id));
    }
    let Some(execution) = storage
        .get_continuity_execution_touching_instance(tenant_id, task.instance_id)
        .await?
    else {
        return Ok(None);
    };
    Ok(storage
        .find_unresolved_effect_receipt(
            tenant_id,
            execution.continuity_id,
            task.instance_id,
            &task.block_id,
            u32::from(task.attempt),
        )
        .await?
        .map(|receipt| receipt.id))
}

/// Settle the effect receipt bound to a worker task (see
/// `bound_effect_receipt_id`). Idempotent: an already-settled receipt is
/// left alone.
pub async fn settle_worker_task_effect(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
    settlement: WorkerEffectSettlement,
    output: Option<&Value>,
) -> Result<(), EngineError> {
    let Some(id) = bound_effect_receipt_id(storage, tenant_id, task).await? else {
        return Ok(());
    };
    let Some(mut receipt) = storage.get_effect_receipt(tenant_id, id).await? else {
        return Ok(());
    };
    let path: &[EffectState] = match (settlement, receipt.state) {
        (WorkerEffectSettlement::Committed, EffectState::Dispatched | EffectState::Unknown) => {
            &[EffectState::Committed]
        }
        (WorkerEffectSettlement::Unknown, EffectState::Dispatched) => &[EffectState::Unknown],
        (WorkerEffectSettlement::Abandoned, EffectState::Dispatched) => {
            &[EffectState::Unknown, EffectState::Abandoned]
        }
        (WorkerEffectSettlement::Abandoned, EffectState::Unknown) => &[EffectState::Abandoned],
        (_, state) if state.is_resolved() || state == EffectState::Unknown => return Ok(()),
        _ => return Err(blocked(&receipt)),
    };
    for next in path {
        let expected = receipt.state;
        if *next == EffectState::Committed {
            receipt.provider_receipt_id = output.and_then(provider_receipt_id);
        }
        receipt
            .transition(*next, Utc::now())
            .map_err(|error| EngineError::EffectTransition(error.to_string()))?;
        if !storage
            .cas_effect_receipt(tenant_id, id, expected, &receipt)
            .await?
        {
            let current = storage
                .get_effect_receipt(tenant_id, id)
                .await?
                .unwrap_or(receipt);
            return if current.state == *next || current.state.is_resolved() {
                Ok(())
            } else {
                Err(blocked(&current))
            };
        }
    }
    Ok(())
}

/// The effect receipt a stranded retry row (see
/// `worker_lease::finalize_stranded_dispatches`) must be bound to: the
/// attempt's still-open receipt when a dispatcher already created one (an
/// older node's dispatch records the receipt but cannot bind it to the row),
/// otherwise a freshly dispatched receipt for this attempt. `None` for pure
/// handlers, which carry no receipt.
pub(crate) async fn stranded_attempt_effect_id(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
) -> Result<Option<EffectId>, EngineError> {
    if !crate::release_diff::handler_has_side_effects(&task.handler_name) {
        return Ok(None);
    }
    let execution = ensure_effect_scope(storage, tenant_id, task.instance_id).await?;
    if let Some(open) = storage
        .find_unresolved_effect_receipt(
            tenant_id,
            execution.continuity_id,
            task.instance_id,
            &task.block_id,
            u32::from(task.attempt),
        )
        .await?
    {
        return Ok(Some(open.id));
    }
    Ok(EffectGuard::begin(
        storage,
        tenant_id,
        task.instance_id,
        &task.block_id,
        &task.handler_name,
        &task.params,
        u32::from(task.attempt),
    )
    .await?
    .as_ref()
    .map(EffectGuard::effect_id))
}

/// Whether a worker task's effect may already have happened: it has a
/// receipt that is still `dispatched` or `unknown`. Tasks without one
/// (pure/idempotent built-ins, dry runs) can simply be requeued.
pub async fn worker_task_effect_is_ambiguous(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
) -> Result<bool, EngineError> {
    if task.effect_id.is_none()
        && !crate::release_diff::handler_has_side_effects(&task.handler_name)
    {
        return Ok(false);
    }
    let Some(id) = bound_effect_receipt_id(storage, tenant_id, task).await? else {
        return Ok(false);
    };
    Ok(storage
        .get_effect_receipt(tenant_id, id)
        .await?
        .is_some_and(|receipt| {
            matches!(
                receipt.state,
                EffectState::Dispatched | EffectState::Unknown
            )
        }))
}

fn blocked(receipt: &EffectReceipt) -> EngineError {
    EngineError::EffectBlocked {
        effect_id: receipt.id,
        block_id: receipt.block_id.clone(),
        state: receipt.state,
    }
}

pub(crate) fn legacy_effect_id(
    continuity_id: orch8_types::continuity::ContinuityId,
    epoch: orch8_types::continuity::ExecutionEpoch,
    instance_id: InstanceId,
    block_id: &BlockId,
    attempt: u32,
) -> EffectId {
    let mut hasher = Sha256::new();
    hasher.update(b"orch8-effect-v1\0");
    hasher.update(continuity_id.as_uuid().as_bytes());
    hasher.update(epoch.get().to_be_bytes());
    hasher.update(instance_id.into_uuid().as_bytes());
    hasher.update(block_id.as_str().as_bytes());
    hasher.update(attempt.to_be_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    EffectId::from_uuid(Uuid::from_bytes(bytes))
}

fn request_hash(handler: &str, params: &Value) -> Result<String, EngineError> {
    let canonical = canonical_json(&serde_json::json!({
        "handler": handler,
        "params": params,
    }))
    .map_err(|error| EngineError::Serialization(error.to_string()))?;
    Ok(hex_sha256(canonical.as_bytes()))
}

fn destination_fingerprint(handler: &str, params: &Value) -> Result<String, EngineError> {
    const DESTINATION_KEYS: &[&str] = &[
        "url", "provider", "model", "topic", "queue", "resource", "endpoint",
    ];
    let selected = params
        .as_object()
        .map_or_else(serde_json::Map::new, |object| {
            DESTINATION_KEYS
                .iter()
                .filter_map(|key| {
                    object
                        .get(*key)
                        .map(|value| ((*key).to_owned(), value.clone()))
                })
                .collect()
        });
    let canonical = canonical_json(&serde_json::json!({
        "handler": handler,
        "destination": selected,
    }))
    .map_err(|error| EngineError::Serialization(error.to_string()))?;
    Ok(hex_sha256(canonical.as_bytes()))
}

fn effect_kind(handler: &str) -> EffectKind {
    match handler {
        "http_request" => EffectKind::Http,
        "llm_call" | "agent" | "embed" => EffectKind::Model,
        "emit_event" | "send_signal" | "email" | "notify" => EffectKind::Message,
        "memory_store" | "memory_delete" | "blob_put" => EffectKind::Storage,
        _ if !orch8_types::sequence::BUILTIN_HANDLER_NAMES.contains(&handler) => EffectKind::Worker,
        _ => EffectKind::Custom,
    }
}

fn provider_receipt_id(output: &Value) -> Option<String> {
    ["provider_receipt_id", "receipt_id", "request_id"]
        .into_iter()
        .find_map(|key| output.get(key).and_then(Value::as_str))
        .filter(|value| value.len() <= 256)
        .map(ToOwned::to_owned)
}

fn same_effect_identity(left: &EffectReceipt, right: &EffectReceipt) -> bool {
    left.id == right.id
        && left.tenant_id == right.tenant_id
        && left.continuity_id == right.continuity_id
        && left.block_id == right.block_id
        && left.attempt == right.attempt
        && left.request_sha256 == right.request_sha256
        && left.kind == right.kind
        && left.destination_fingerprint == right.destination_fingerprint
        && left.idempotency_key == right.idempotency_key
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use chrono::Utc;
    use orch8_storage::{
        ContinuityStore, InstanceStore, InvariantStore, SequenceStore, sqlite::SqliteStorage,
    };
    use orch8_types::context::ExecutionContext;
    use orch8_types::continuity::{
        ContinuityExecution, ContinuityId, ExecutionEpoch, OwnershipState, RuntimeId,
    };
    use orch8_types::continuity_advanced::{
        EvidenceStatus, InvariantId, InvariantRule, WorkflowInvariant,
    };
    use orch8_types::ids::{Namespace, SequenceId};
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::{BlockDefinition, SequenceDefinition, SequenceStatus, StepDef};

    use super::*;
    use crate::handlers::HandlerRegistry;
    use crate::handlers::step::{StepExecParams, execute_step_dry};

    async fn continuity_storage() -> (SqliteStorage, TaskInstance, ContinuityExecution) {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("tenant-effect"),
            namespace: Namespace::new("default"),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&instance).await.unwrap();
        let execution = ContinuityExecution {
            continuity_id: ContinuityId::new(),
            tenant_id: instance.tenant_id.clone(),
            current_instance_id: instance.id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: now,
        };
        storage
            .create_continuity_execution(&execution)
            .await
            .unwrap();
        (storage, instance, execution)
    }

    async fn ordinary_storage() -> (SqliteStorage, TaskInstance) {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("tenant-effect"),
            namespace: Namespace::new("default"),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&instance).await.unwrap();
        (storage, instance)
    }

    async fn add_sequence_and_effect_guard(
        storage: &SqliteStorage,
        instance: &TaskInstance,
        kind: EffectKind,
    ) -> InvariantId {
        storage
            .create_sequence(&SequenceDefinition {
                embed: None,
                sub_tenant: None,
                schema: None,
                schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
                id: instance.sequence_id,
                tenant_id: instance.tenant_id.clone(),
                namespace: instance.namespace.clone(),
                name: "guarded-effects".into(),
                version: 1,
                deprecated: false,
                status: SequenceStatus::Production,
                blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                    id: BlockId::new("placeholder"),
                    handler: "noop".into(),
                    params: serde_json::json!({}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    rate_budget: None,
                    placement: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                    output_schema: None,
                    when: None,
                    compensation: None,
                }))],
                interceptors: None,
                input_schema: None,
                sla: None,
                on_failure: None,
                on_cancel: None,
                placement: None,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        let id = InvariantId::new();
        storage
            .create_workflow_invariant(&WorkflowInvariant {
                id,
                tenant_id: instance.tenant_id.clone(),
                sequence_id: instance.sequence_id,
                sequence_version: Some(1),
                name: "charge only once".into(),
                rule: InvariantRule::EffectAtMostOnce { kind },
                commit_guard: true,
                enabled: true,
                created_at: Utc::now(),
            })
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn successful_effect_is_durably_committed_with_provider_evidence() {
        let (storage, instance, execution) = continuity_storage().await;
        let block = BlockId::new("charge");
        let guard = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &block,
            "http_request",
            &serde_json::json!({
                "url": "https://payments.example/charge",
                "idempotency_key": "order-7"
            }),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        guard
            .commit(&serde_json::json!({"receipt_id": "provider-42"}))
            .await
            .unwrap();

        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Committed);
        assert_eq!(
            receipts[0].provider_receipt_id.as_deref(),
            Some("provider-42")
        );
        assert_eq!(receipts[0].idempotency_key.as_deref(), Some("order-7"));
        assert_eq!(receipts[0].request_sha256.len(), 64);
        assert_eq!(receipts[0].destination_fingerprint.len(), 64);
    }

    #[tokio::test]
    async fn ordinary_instance_gets_a_universal_effect_ledger() {
        let (storage, instance) = ordinary_storage().await;
        assert!(
            storage
                .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
                .await
                .unwrap()
                .is_none()
        );

        let guard = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &BlockId::new("charge"),
            "custom_charge",
            &serde_json::json!({"amount": 100}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        guard
            .commit(&serde_json::json!({"receipt_id": "provider-ordinary"}))
            .await
            .unwrap();

        let execution = storage
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .expect("side-effecting execution must have a durable ledger scope");
        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Committed);
    }

    #[tokio::test]
    async fn concurrent_first_effects_converge_on_one_ledger_scope() {
        let (storage, instance) = ordinary_storage().await;
        let left_block = BlockId::new("left");
        let right_block = BlockId::new("right");
        let left_params = serde_json::json!({"value": 1});
        let right_params = serde_json::json!({"value": 2});
        let left = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &left_block,
            "custom_left",
            &left_params,
            0,
        );
        let right = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &right_block,
            "custom_right",
            &right_params,
            0,
        );
        let (left, right) = tokio::join!(left, right);
        left.unwrap()
            .unwrap()
            .commit(&serde_json::json!({}))
            .await
            .unwrap();
        right
            .unwrap()
            .unwrap()
            .commit(&serde_json::json!({}))
            .await
            .unwrap();

        let execution = storage
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .unwrap();
        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 2);
        assert!(
            receipts
                .iter()
                .all(|receipt| receipt.state == EffectState::Committed)
        );
    }

    #[tokio::test]
    async fn pure_step_does_not_create_a_ledger_scope() {
        let (storage, instance) = ordinary_storage().await;
        let guard = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &BlockId::new("transform"),
            "transform",
            &serde_json::json!({}),
            0,
        )
        .await
        .unwrap();
        assert!(guard.is_none());
        assert!(
            storage
                .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn abandoned_dispatch_is_classified_unknown_and_blocks_redispatch() {
        let (storage, instance, execution) = continuity_storage().await;
        let block = BlockId::new("charge");
        let first = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &block,
            "http_request",
            &serde_json::json!({"url": "https://payments.example/charge"}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        drop(first); // models process death after the durable dispatch boundary

        let retry = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &block,
            "http_request",
            &serde_json::json!({"url": "https://payments.example/charge"}),
            0,
        )
        .await;
        assert!(matches!(
            retry,
            Err(EngineError::EffectBlocked {
                state: EffectState::Unknown,
                ..
            })
        ));
        let unresolved = storage
            .find_unresolved_effect_receipt(
                &instance.tenant_id,
                execution.continuity_id,
                instance.id,
                &block,
                0,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(unresolved.state, EffectState::Unknown);
    }

    #[tokio::test]
    async fn pure_handlers_do_not_create_receipts() {
        let (storage, instance, execution) = continuity_storage().await;
        let guard = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &BlockId::new("transform"),
            "transform",
            &serde_json::json!({}),
            0,
        )
        .await
        .unwrap();
        assert!(guard.is_none());
        assert!(
            storage
                .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn registered_side_effect_handler_is_guarded_in_the_real_dispatch_path() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let mut handlers = HandlerRegistry::new();
        handlers.register("custom_charge", move |_context| {
            observed.fetch_add(1, Ordering::SeqCst);
            async {
                Err(orch8_types::error::StepError::Retryable {
                    message: "provider connection closed".into(),
                    details: None,
                })
            }
        });
        let make_exec = || StepExecParams {
            claim_token: None,
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: BlockId::new("charge"),
            handler_name: "custom_charge".into(),
            params: serde_json::json!({"amount": 100}),
            context: ExecutionContext::default(),
            attempt: 0,
            timeout: None,
            externalize_threshold: 0,
            wait_for_input: None,
            cache_key: None,
            output_schema: None,
        };

        let first = execute_step_dry(&storage, &handlers, make_exec()).await;
        assert!(matches!(first, Err(EngineError::StepFailed { .. })));
        let second = execute_step_dry(&storage, &handlers, make_exec()).await;
        assert!(matches!(
            second,
            Err(EngineError::EffectBlocked {
                state: EffectState::Unknown,
                ..
            })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let execution = storage
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .expect("real dispatch must create a ledger scope");
        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Unknown);
    }

    #[tokio::test]
    async fn commit_guard_blocks_duplicate_before_dispatch_and_deduplicates_violation() {
        let (storage, instance, execution) = continuity_storage().await;
        let invariant_id =
            add_sequence_and_effect_guard(&storage, &instance, EffectKind::Worker).await;
        let params = serde_json::json!({"provider": "payments", "amount": 100});
        let first = EffectGuard::begin(
            &storage,
            &instance.tenant_id,
            instance.id,
            &BlockId::new("charge-a"),
            "custom_charge",
            &params,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        first
            .commit(&serde_json::json!({"receipt_id": "provider-1"}))
            .await
            .unwrap();

        for block in ["charge-b", "charge-c"] {
            let duplicate = EffectGuard::begin(
                &storage,
                &instance.tenant_id,
                instance.id,
                &BlockId::new(block),
                "custom_charge",
                &params,
                0,
            )
            .await;
            assert!(matches!(
                duplicate,
                Err(EngineError::InvariantViolation {
                    invariant_id: observed,
                    ..
                }) if observed == invariant_id
            ));
        }

        let results = storage
            .list_invariant_results(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(
            results
                .iter()
                .filter(|result| result.status == EvidenceStatus::Fail)
                .count(),
            1,
            "repeated duplicate evidence must produce one violation"
        );
        assert!(results.iter().any(|result| {
            result.invariant_id == invariant_id && result.status == EvidenceStatus::Pass
        }));
        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(
            receipts
                .iter()
                .filter(|receipt| {
                    matches!(
                        receipt.state,
                        EffectState::Dispatched | EffectState::Committed
                    )
                })
                .count(),
            1
        );
    }
    fn native_execution(instance: &orch8_types::instance::TaskInstance) -> StepExecParams {
        StepExecParams {
            claim_token: None,
            instance_id: instance.id,
            tenant_id: instance.tenant_id.clone(),
            block_id: BlockId::new("native"),
            handler_name: "native".into(),
            params: serde_json::json!({}),
            context: ExecutionContext::default(),
            attempt: 0,
            timeout: None,
            externalize_threshold: 0,
            wait_for_input: None,
            cache_key: None,
            output_schema: None,
        }
    }

    #[tokio::test]
    async fn unadmitted_caller_cannot_settle_another_callers_native_dispatch() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let first_entered = Arc::new(tokio::sync::Notify::new());
        let first_release = Arc::new(tokio::sync::Notify::new());
        let second_entered = Arc::new(tokio::sync::Notify::new());
        let second_release = Arc::new(tokio::sync::Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut handlers = HandlerRegistry::new();
        let (first_ready, first_gate, second_ready, second_gate, call_count) = (
            first_entered.clone(),
            first_release.clone(),
            second_entered.clone(),
            second_release.clone(),
            calls.clone(),
        );
        handlers.register_with_deferred_effect_admission("native", move |ctx| {
            let (first_ready, first_gate, second_ready, second_gate, call_count) = (
                first_ready.clone(),
                first_gate.clone(),
                second_ready.clone(),
                second_gate.clone(),
                call_count.clone(),
            );
            async move {
                if call_count.fetch_add(1, Ordering::SeqCst) == 0 {
                    first_ready.notify_one();
                    first_gate.notified().await;
                    Err(orch8_types::error::StepError::Retryable {
                        message: "preflight failed before admission".into(),
                        details: None,
                    })
                } else {
                    ctx.effect_admission.as_ref().unwrap().admit().await?;
                    second_ready.notify_one();
                    second_gate.notified().await;
                    Ok(serde_json::json!({}))
                }
            }
        });
        let handlers = Arc::new(handlers);
        let exec = native_execution(&instance);
        let test_storage = storage.clone();
        let test_handlers = handlers.clone();
        let first =
            tokio::spawn(
                async move { execute_step_dry(&test_storage, &test_handlers, exec).await },
            );
        tokio::time::timeout(std::time::Duration::from_secs(5), first_entered.notified())
            .await
            .unwrap();
        let exec = native_execution(&instance);
        let test_storage = storage.clone();
        let test_handlers = handlers.clone();
        let second =
            tokio::spawn(
                async move { execute_step_dry(&test_storage, &test_handlers, exec).await },
            );
        tokio::time::timeout(std::time::Duration::from_secs(5), second_entered.notified())
            .await
            .unwrap();
        first_release.notify_one();
        assert!(first.await.unwrap().is_err());
        let execution = storage
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .unwrap();
        let receipts = storage
            .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(receipts.len(), 1);
        assert_eq!(receipts[0].state, EffectState::Dispatched);
        second_release.notify_one();
        second.await.unwrap().unwrap();
        assert_eq!(
            storage
                .list_effect_receipts(&instance.tenant_id, execution.continuity_id, 10)
                .await
                .unwrap()[0]
                .state,
            EffectState::Committed
        );
    }
    #[tokio::test]
    async fn native_callback_identity_is_bound_to_the_original_effect_without_admitting_it() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let block = BlockId::new("native");
        let guard = EffectGuard::prepare(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &block,
            "native",
            &serde_json::json!({}),
            2,
        )
        .await
        .unwrap()
        .unwrap();
        let admission = guard.admission(storage.clone());
        assert_eq!(
            admission.effect_id_for(&instance.tenant_id, instance.id, &block, 2),
            Some(guard.receipt.id)
        );
        for (tenant, id, candidate_block, attempt) in [
            (
                TenantId::unchecked("foreign"),
                instance.id,
                block.clone(),
                2,
            ),
            (
                instance.tenant_id.clone(),
                InstanceId::new(),
                block.clone(),
                2,
            ),
            (
                instance.tenant_id.clone(),
                instance.id,
                BlockId::new("foreign"),
                2,
            ),
            (instance.tenant_id.clone(), instance.id, block, 3),
        ] {
            assert_eq!(
                admission.effect_id_for(&tenant, id, &candidate_block, attempt),
                None
            );
        }
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, guard.receipt.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Prepared
        );
        admission.admit().await.unwrap();
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, guard.receipt.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Dispatched
        );
    }

    #[tokio::test]
    async fn cloned_native_admission_is_single_use() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let guard = EffectGuard::prepare(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &BlockId::new("native"),
            "native",
            &serde_json::json!({}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        let admission = guard.admission(storage.clone());
        let clone = admission.clone();
        admission.admit().await.unwrap();
        assert!(matches!(
            clone.admit().await,
            Err(orch8_types::error::StepError::ClaimLost)
        ));
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, guard.receipt.id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Dispatched
        );
    }

    #[tokio::test]
    async fn native_mock_uses_normal_guard_without_calling_native_adapter() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let calls = Arc::new(AtomicUsize::new(0));
        let n = calls.clone();
        let mut handlers = HandlerRegistry::new();
        handlers.register_with_deferred_effect_admission("native", move |_ctx| {
            n.fetch_add(1, Ordering::SeqCst);
            async { Ok(serde_json::json!({"real":true})) }
        });
        handlers.set_mock("native", serde_json::json!({"mock":true}));
        let result = execute_step_dry(
            &storage,
            &handlers,
            StepExecParams {
                claim_token: None,
                instance_id: instance.id,
                tenant_id: instance.tenant_id.clone(),
                block_id: BlockId::new("native"),
                handler_name: "native".into(),
                params: serde_json::json!({}),
                context: ExecutionContext::default(),
                attempt: 0,
                timeout: None,
                externalize_threshold: 0,
                wait_for_input: None,
                cache_key: None,
                output_schema: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(result.output, serde_json::json!({"mock":true}));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn preflight_rejection_consumes_admission_and_cannot_commit_ignored_control_error() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let mut handlers = HandlerRegistry::new();
        handlers.register_with_deferred_effect_admission("native", |ctx| async move {
            let admission = ctx.effect_admission.unwrap();
            assert!(matches!(
                admission.reject_preflight(EngineError::AdmissionBusy {
                    instance_id: ctx.instance_id,
                }),
                orch8_types::error::StepError::ClaimLost
            ));
            assert!(matches!(
                admission.admit().await,
                Err(orch8_types::error::StepError::ClaimLost)
            ));
            // Even a buggy adapter ignoring the control error cannot commit it.
            Ok(serde_json::json!({"ignored":true}))
        });
        assert!(matches!(
            execute_step_dry(&storage, &handlers, native_execution(&instance)).await,
            Err(EngineError::AdmissionBusy { .. })
        ));
        let ledger = storage
            .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
            .await
            .unwrap()
            .unwrap();
        let rows = storage
            .list_effect_receipts(&instance.tenant_id, ledger.continuity_id, 10)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].state, EffectState::Prepared);
    }

    #[tokio::test]
    async fn preflight_rejection_cannot_relabel_started_dispatch() {
        let (storage, instance) = ordinary_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let exec = native_execution(&instance);
        let mut guard = EffectGuard::prepare(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &exec.block_id,
            "native",
            &exec.params,
            0,
        )
        .await
        .unwrap()
        .unwrap();
        let admission = guard.admission(storage.clone());
        admission.admit().await.unwrap();
        assert!(matches!(
            admission.reject_preflight(EngineError::AdmissionBusy {
                instance_id: instance.id
            }),
            orch8_types::error::StepError::ClaimLost
        ));
        assert!(admission.take_failure().is_none());
        assert!(admission.is_admitted());
        guard.refresh().await.unwrap();
        assert_eq!(guard.receipt.state, EffectState::Dispatched);
    }
    #[tokio::test]
    async fn prepared_native_admission_cannot_dispatch_after_pause() {
        let (storage, instance, _) = continuity_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let guard = EffectGuard::prepare(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &BlockId::new("native"),
            "native",
            &serde_json::json!({}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        let id = guard.effect_id();
        let admission = guard.admission(storage.clone());
        storage
            .update_instance_state(instance.id, InstanceState::Paused, None)
            .await
            .unwrap();
        assert!(
            admission.admit().await.is_err(),
            "prepared callback dispatched after pause"
        );
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Prepared
        );
    }

    #[tokio::test]
    async fn prepared_native_admission_cannot_dispatch_after_export_fence() {
        let (storage, instance, execution) = continuity_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let guard = EffectGuard::prepare(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &BlockId::new("native"),
            "native",
            &serde_json::json!({}),
            0,
        )
        .await
        .unwrap()
        .unwrap();
        let id = guard.effect_id();
        let admission = guard.admission(storage.clone());
        let mut transferring = execution.clone();
        transferring.state = OwnershipState::Transferring;
        assert!(
            storage
                .cas_continuity_owner(
                    &instance.tenant_id,
                    execution.continuity_id,
                    execution.epoch,
                    execution.owner_runtime_id,
                    &transferring
                )
                .await
                .unwrap()
        );
        assert!(
            admission.admit().await.is_err(),
            "prepared callback dispatched after continuity export"
        );
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Prepared
        );
    }
    #[tokio::test]
    async fn prepared_native_admission_keeps_original_claim_after_replacement() {
        let (storage, mut instance, _) = continuity_storage().await;
        let storage: Arc<dyn StorageBackend> = Arc::new(storage);
        let now = Utc::now();
        storage
            .update_instance_state(instance.id, InstanceState::Scheduled, Some(now))
            .await
            .unwrap();
        instance = storage
            .claim_due_instances(now, 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        let old = instance.claimed_at.unwrap();
        let guard = EffectGuard::prepare_with_claim(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &BlockId::new("native"),
            "native",
            &serde_json::json!({}),
            0,
            Some(old),
        )
        .await
        .unwrap()
        .unwrap();
        let id = guard.effect_id();
        let admission = guard.admission(storage.clone());
        storage
            .update_instance_state(instance.id, InstanceState::Scheduled, Some(now))
            .await
            .unwrap();
        let replacement = storage
            .claim_due_instances(now + chrono::Duration::seconds(1), 1, 0)
            .await
            .unwrap()
            .pop()
            .unwrap();
        assert_ne!(replacement.claimed_at, Some(old));
        assert!(admission.admit().await.is_err());
        assert_eq!(
            storage
                .get_effect_receipt(&instance.tenant_id, id)
                .await
                .unwrap()
                .unwrap()
                .state,
            EffectState::Prepared
        );
        // Fresh work can reuse the safely prepared receipt under its own claim.
        let mut recovered = EffectGuard::prepare_with_claim(
            storage.as_ref(),
            &instance.tenant_id,
            instance.id,
            &BlockId::new("native"),
            "native",
            &serde_json::json!({}),
            0,
            replacement.claimed_at,
        )
        .await
        .unwrap()
        .unwrap();
        recovered.dispatch().await.unwrap();
    }
}
