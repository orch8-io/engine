//! Crash-safe classification for externally visible step effects.
//!
//! The guard deliberately provides at-most-once safety, not exactly-once
//! semantics. Once dispatch is durably recorded, any ambiguous outcome blocks
//! automatic retry until an operator or verifier resolves the receipt.

use chrono::Utc;
use orch8_publisher::manifest::canonical_json;
use orch8_storage::StorageBackend;
use orch8_types::continuity::{
    EffectDispatchOutcome, EffectId, EffectKind, EffectReceipt, EffectState,
};
use orch8_types::continuity_advanced::{InvariantRule, WorkflowInvariant};
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use uuid::Uuid;

use crate::error::EngineError;

pub(crate) struct EffectGuard<'a> {
    storage: &'a dyn StorageBackend,
    receipt: EffectReceipt,
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
        if !crate::release_diff::handler_has_side_effects(handler) {
            return Ok(None);
        }
        let Some(execution) = storage
            .get_continuity_execution_by_instance(tenant_id, instance_id)
            .await?
        else {
            return Ok(None);
        };

        if let Some(mut unresolved) = storage
            .find_unresolved_effect_receipt(
                tenant_id,
                execution.continuity_id,
                instance_id,
                block_id,
            )
            .await?
        {
            if unresolved.state == EffectState::Dispatched {
                let expected = unresolved.state;
                unresolved
                    .transition(EffectState::Unknown, Utc::now())
                    .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
                let _ = storage
                    .cas_effect_receipt(tenant_id, unresolved.id, expected, &unresolved)
                    .await?;
            }
            return Err(blocked(&unresolved));
        }

        let id = deterministic_effect_id(
            execution.continuity_id,
            execution.epoch,
            instance_id,
            block_id,
            attempt,
        );
        let now = Utc::now();
        let candidate = EffectReceipt {
            id,
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
        let receipt = storage.ensure_effect_receipt(&candidate).await?;
        if !same_effect_identity(&receipt, &candidate) {
            return Err(EngineError::InvalidConfig(
                "deterministic effect id collided with different effect evidence".into(),
            ));
        }
        let mut guard = Self { storage, receipt };
        match guard.receipt.state {
            EffectState::Planned => guard.advance(EffectState::Prepared).await?,
            EffectState::Prepared => {}
            _ => return Err(blocked(&guard.receipt)),
        }
        guard.dispatch().await?;
        Ok(Some(guard))
    }

    pub(crate) async fn commit(mut self, output: &Value) -> Result<(), EngineError> {
        self.receipt.provider_receipt_id = provider_receipt_id(output);
        self.advance(EffectState::Committed).await
    }

    pub(crate) async fn mark_unknown(mut self) -> Result<(), EngineError> {
        self.advance(EffectState::Unknown).await
    }

    async fn dispatch(&mut self) -> Result<(), EngineError> {
        let guards = load_effect_commit_guards(self.storage, &self.receipt).await?;
        if guards.is_empty() {
            return self.advance(EffectState::Dispatched).await;
        }
        let mut updated = self.receipt.clone();
        updated
            .transition(EffectState::Dispatched, Utc::now())
            .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
        match self
            .storage
            .dispatch_effect_receipt_at_most_once(&updated.tenant_id, &updated)
            .await?
        {
            EffectDispatchOutcome::Dispatched => {
                self.receipt = updated;
                record_effect_guard_results(self.storage, &guards, &self.receipt, false).await?;
                Ok(())
            }
            EffectDispatchOutcome::Duplicate => {
                record_effect_guard_results(self.storage, &guards, &updated, true).await?;
                Err(EngineError::InvariantViolation {
                    invariant_id: guards[0].id,
                    block_id: updated.block_id,
                })
            }
            EffectDispatchOutcome::Stale => {
                let current = self
                    .storage
                    .get_effect_receipt(&updated.tenant_id, updated.id)
                    .await?
                    .unwrap_or(self.receipt.clone());
                Err(blocked(&current))
            }
        }
    }

    async fn advance(&mut self, next: EffectState) -> Result<(), EngineError> {
        let expected = self.receipt.state;
        let mut updated = self.receipt.clone();
        updated
            .transition(next, Utc::now())
            .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
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

async fn load_effect_commit_guards(
    storage: &dyn StorageBackend,
    receipt: &EffectReceipt,
) -> Result<Vec<WorkflowInvariant>, EngineError> {
    let Some(instance) = storage.get_instance(receipt.instance_id).await? else {
        return Ok(Vec::new());
    };
    let Some(sequence) = storage.get_sequence(instance.sequence_id).await? else {
        return Ok(Vec::new());
    };
    let invariants = storage
        .list_workflow_invariants(&receipt.tenant_id, sequence.id, sequence.version, 10_000)
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
            candidate.epoch,
            &evidence,
            now,
        );
        let _ = storage
            .append_invariant_result(&candidate.tenant_id, &result)
            .await?;
    }
    Ok(())
}

/// Commit the receipt created when an external worker task was dispatched.
/// Legacy and non-continuity tasks are intentionally a no-op.
pub async fn commit_external_worker_effect(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
    output: &Value,
) -> Result<(), EngineError> {
    settle_external_worker_effect(storage, tenant_id, task, Some(output)).await
}

/// Mark an external worker effect uncertain before applying retry policy.
pub async fn mark_external_worker_effect_unknown(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
) -> Result<(), EngineError> {
    settle_external_worker_effect(storage, tenant_id, task, None).await
}

async fn settle_external_worker_effect(
    storage: &dyn StorageBackend,
    tenant_id: &TenantId,
    task: &orch8_types::worker::WorkerTask,
    output: Option<&Value>,
) -> Result<(), EngineError> {
    let Some(execution) = storage
        .get_continuity_execution_by_instance(tenant_id, task.instance_id)
        .await?
    else {
        return Ok(());
    };
    let id = deterministic_effect_id(
        execution.continuity_id,
        execution.epoch,
        task.instance_id,
        &task.block_id,
        u32::from(task.attempt),
    );
    let Some(mut receipt) = storage.get_effect_receipt(tenant_id, id).await? else {
        return Ok(());
    };
    if receipt.state != EffectState::Dispatched {
        return if matches!(receipt.state, EffectState::Committed | EffectState::Unknown) {
            Ok(())
        } else {
            Err(blocked(&receipt))
        };
    }
    let expected = receipt.state;
    let next = if output.is_some() {
        EffectState::Committed
    } else {
        EffectState::Unknown
    };
    receipt.provider_receipt_id = output.and_then(provider_receipt_id);
    receipt
        .transition(next, Utc::now())
        .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
    if !storage
        .cas_effect_receipt(tenant_id, id, expected, &receipt)
        .await?
    {
        let current = storage
            .get_effect_receipt(tenant_id, id)
            .await?
            .unwrap_or(receipt);
        return if current.state == next {
            Ok(())
        } else {
            Err(blocked(&current))
        };
    }
    Ok(())
}

fn blocked(receipt: &EffectReceipt) -> EngineError {
    EngineError::EffectBlocked {
        effect_id: receipt.id,
        block_id: receipt.block_id.clone(),
        state: receipt.state,
    }
}

fn deterministic_effect_id(
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
    .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
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
    .map_err(|error| EngineError::InvalidConfig(error.to_string()))?;
    Ok(hex_sha256(canonical.as_bytes()))
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(64);
    for byte in digest {
        write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
    }
    output
}

fn effect_kind(handler: &str) -> EffectKind {
    match handler {
        "http_request" => EffectKind::Http,
        "llm_call" | "agent" | "embed" => EffectKind::Model,
        "emit_event" | "send_signal" => EffectKind::Message,
        "memory_store" | "blob_put" => EffectKind::Storage,
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
        && left.epoch == right.epoch
        && left.instance_id == right.instance_id
        && left.block_id == right.block_id
        && left.attempt == right.attempt
        && left.request_sha256 == right.request_sha256
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

    async fn add_sequence_and_effect_guard(
        storage: &SqliteStorage,
        instance: &TaskInstance,
        kind: EffectKind,
    ) -> InvariantId {
        storage
            .create_sequence(&SequenceDefinition {
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
        let (storage, instance, execution) = continuity_storage().await;
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
}
