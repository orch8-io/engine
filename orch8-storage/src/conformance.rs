//! Reusable behavioral conformance suite for third-party storage backends.
//!
//! Run this only against an isolated test database. It creates a uniquely
//! scoped deprecated sequence and a terminal instance as durable audit
//! evidence; it never deletes rows because deletion could hide a backend's
//! referential-integrity defect.

use chrono::Utc;
use orch8_types::context::ExecutionContext;
use orch8_types::error::StorageError;
use orch8_types::ids::{BlockId, InstanceId, Namespace, SequenceId, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::sequence::{SequenceDefinition, SequenceStatus};
use orch8_types::signal::{Signal, SignalType};
use serde::Serialize;
use serde_json::json;
use thiserror::Error;
use uuid::Uuid;

use crate::StorageBackend;

/// Successful core conformance evidence and the durable rows it created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConformanceReport {
    pub scope: String,
    pub sequence_id: SequenceId,
    pub instance_id: InstanceId,
    pub checks: Vec<&'static str>,
}

#[derive(Debug, Error)]
pub enum ConformanceError {
    #[error("storage conformance check {check} failed: {source}")]
    Backend {
        check: &'static str,
        #[source]
        source: StorageError,
    },
    #[error("storage conformance check {check} violated its contract: {message}")]
    Violation {
        check: &'static str,
        message: String,
    },
}

/// Exercise the minimum cross-backend durability contract used by the
/// scheduler: immutable sequences, duplicate classification, instance
/// idempotency, state CAS, outputs, signals, tenant isolation, and terminal
/// cleanup evidence.
pub async fn run_core_conformance(
    storage: &dyn StorageBackend,
) -> Result<ConformanceReport, ConformanceError> {
    let run_id = Uuid::now_v7();
    let scope = format!("orch8-conformance-{run_id}");
    let tenant = TenantId::new(scope.clone()).map_err(|message| ConformanceError::Violation {
        check: "scope",
        message,
    })?;
    let sequence = conformance_sequence(&tenant, run_id);
    check_sequence(storage, &sequence).await?;
    let instance = conformance_instance(&tenant, sequence.id, run_id);
    backend("instance_create", storage.create_instance(&instance).await)?;
    check_instance_identity(storage, &tenant, &instance).await?;
    check_state_cas(storage, instance.id).await?;
    check_output(storage, instance.id).await?;
    check_signal(storage, instance.id).await?;
    finalize_evidence(storage, sequence.id, instance.id).await?;
    Ok(ConformanceReport {
        scope,
        sequence_id: sequence.id,
        instance_id: instance.id,
        checks: vec![
            "sequence_round_trip",
            "duplicate_classification",
            "instance_idempotency",
            "tenant_isolation",
            "instance_state_cas",
            "output_round_trip",
            "signal_delivery",
            "terminal_evidence",
        ],
    })
}

async fn check_sequence(
    storage: &dyn StorageBackend,
    sequence: &SequenceDefinition,
) -> Result<(), ConformanceError> {
    backend("sequence_create", storage.create_sequence(sequence).await)?;
    let loaded = backend("sequence_read", storage.get_sequence(sequence.id).await)?;
    require(
        "sequence_read",
        loaded.is_some_and(|value| {
            value.id == sequence.id
                && value.tenant_id == sequence.tenant_id
                && value.namespace == sequence.namespace
                && value.name == sequence.name
                && value.version == sequence.version
                && value.blocks.len() == sequence.blocks.len()
        }),
        "created immutable sequence did not round-trip",
    )?;
    require(
        "duplicate_classification",
        matches!(
            storage.create_sequence(sequence).await,
            Err(StorageError::Conflict(_))
        ),
        "duplicate sequence must return StorageError::Conflict",
    )
}

async fn check_instance_identity(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    instance: &TaskInstance,
) -> Result<(), ConformanceError> {
    let key = instance.idempotency_key.as_deref().unwrap_or_default();
    let by_key = backend(
        "instance_idempotency",
        storage.find_by_idempotency_key(tenant, key).await,
    )?;
    require(
        "instance_idempotency",
        by_key.as_ref().map(|value| value.id) == Some(instance.id),
        "tenant idempotency lookup did not return the created instance",
    )?;
    let other_tenant = TenantId::new("orch8-conformance-other").expect("static tenant is valid");
    let cross_tenant = backend(
        "tenant_isolation",
        storage.find_by_idempotency_key(&other_tenant, key).await,
    )?;
    require(
        "tenant_isolation",
        cross_tenant.is_none(),
        "idempotency lookup crossed the tenant boundary",
    )
}

async fn check_state_cas(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<(), ConformanceError> {
    let first_cas = backend(
        "instance_state_cas",
        storage
            .conditional_update_instance_state(
                instance_id,
                InstanceState::Scheduled,
                InstanceState::Running,
                None,
            )
            .await,
    )?;
    let stale_cas = backend(
        "instance_state_cas",
        storage
            .conditional_update_instance_state(
                instance_id,
                InstanceState::Scheduled,
                InstanceState::Completed,
                None,
            )
            .await,
    )?;
    require(
        "instance_state_cas",
        first_cas && !stale_cas,
        "exactly one compare-and-swap writer must win",
    )
}

async fn check_output(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<(), ConformanceError> {
    let block_id = BlockId::new("conformance");
    let output = BlockOutput {
        id: Uuid::now_v7(),
        instance_id,
        block_id: block_id.clone(),
        output: json!({"durable": true}),
        output_ref: None,
        output_size: 16,
        attempt: 0,
        created_at: Utc::now(),
    };
    backend(
        "output_round_trip",
        storage.save_block_output(&output).await,
    )?;
    let loaded_output = backend(
        "output_round_trip",
        storage.get_block_output(instance_id, &block_id).await,
    )?;
    require(
        "output_round_trip",
        loaded_output.as_ref().map(|value| &value.output) == Some(&output.output),
        "saved output did not round-trip",
    )
}

async fn check_signal(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
) -> Result<(), ConformanceError> {
    let signal = Signal {
        id: Uuid::now_v7(),
        instance_id,
        signal_type: SignalType::Custom("conformance".into()),
        payload: json!({"probe": true}),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    backend("signal_delivery", storage.enqueue_signal(&signal).await)?;
    let pending = backend(
        "signal_delivery",
        storage.get_pending_signals(instance_id).await,
    )?;
    require(
        "signal_delivery",
        pending.iter().any(|value| value.id == signal.id),
        "enqueued signal was not visible",
    )?;
    backend(
        "signal_delivery",
        storage.mark_signal_delivered(signal.id).await,
    )?;
    let pending = backend(
        "signal_delivery",
        storage.get_pending_signals(instance_id).await,
    )?;
    require(
        "signal_delivery",
        pending.iter().all(|value| value.id != signal.id),
        "delivered signal remained pending",
    )
}

async fn finalize_evidence(
    storage: &dyn StorageBackend,
    sequence_id: SequenceId,
    instance_id: InstanceId,
) -> Result<(), ConformanceError> {
    backend(
        "terminal_evidence",
        storage
            .update_instance_state(instance_id, InstanceState::Cancelled, None)
            .await,
    )?;
    backend(
        "terminal_evidence",
        storage.deprecate_sequence(sequence_id).await,
    )
}

fn backend<T>(check: &'static str, result: Result<T, StorageError>) -> Result<T, ConformanceError> {
    result.map_err(|source| ConformanceError::Backend { check, source })
}

fn require(check: &'static str, condition: bool, message: &str) -> Result<(), ConformanceError> {
    if condition {
        Ok(())
    } else {
        Err(ConformanceError::Violation {
            check,
            message: message.into(),
        })
    }
}

fn conformance_sequence(tenant: &TenantId, run_id: Uuid) -> SequenceDefinition {
    SequenceDefinition {
        id: SequenceId::new(),
        tenant_id: tenant.clone(),
        namespace: Namespace::new("conformance"),
        name: format!("storage-conformance-{run_id}"),
        version: 1,
        deprecated: false,
        blocks: Vec::new(),
        interceptors: None,
        input_schema: None,
        sla: None,
        on_failure: None,
        on_cancel: None,
        created_at: Utc::now(),
        status: SequenceStatus::Draft,
    }
}

fn conformance_instance(tenant: &TenantId, sequence_id: SequenceId, run_id: Uuid) -> TaskInstance {
    let now = Utc::now();
    TaskInstance {
        id: InstanceId::new(),
        sequence_id,
        tenant_id: tenant.clone(),
        namespace: Namespace::new("conformance"),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::Normal,
        timezone: "UTC".into(),
        metadata: json!({"orch8_storage_conformance": true}),
        context: ExecutionContext::default(),
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: Some(format!("storage-conformance-{run_id}")),
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InstanceStore;
    use crate::sqlite::SqliteStorage;

    #[tokio::test]
    async fn sqlite_passes_public_core_conformance() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let report = run_core_conformance(&storage).await.unwrap();
        assert_eq!(report.checks.len(), 8);
        assert_eq!(
            storage
                .get_instance(report.instance_id)
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Cancelled
        );
    }
}
