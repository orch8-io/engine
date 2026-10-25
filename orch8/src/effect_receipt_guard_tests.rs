//! `EffectContext::load` is the double-dispatch guard for live effects.
//!
//! Rule under test: a live (non-dry-run) effect handler runs only when the
//! instance has a continuity scope AND a durable receipt for this exact
//! attempt that is still `dispatched`. Any other durable state is a permanent
//! invariant failure — retrying cannot make an ambiguous effect safe to
//! repeat. Storage failures map to retryable/permanent by transience.

use super::*;
use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::continuity::{
    ContinuityExecution, ContinuityId, EffectId, EffectKind, ExecutionEpoch, OwnershipState,
    RuntimeId,
};
use orch8_types::ids::{BlockId, InstanceId, TenantId};

const TENANT: &str = "tenant_1";

fn step(storage: Arc<dyn StorageBackend>, instance_id: InstanceId, dry_run: bool) -> StepContext {
    StepContext {
        effect_admission: None,
        claim_token: None,
        instance_id,
        tenant_id: TenantId::unchecked(TENANT),
        block_id: BlockId::new("charge"),
        params: serde_json::json!({}),
        context: Arc::new(ExecutionContext {
            runtime: RuntimeContext {
                dry_run,
                ..RuntimeContext::default()
            },
            ..ExecutionContext::default()
        }),
        attempt: 1,
        storage,
        wait_for_input: None,
    }
}

/// Fresh store holding a continuity scope for a new instance.
async fn scoped_store() -> (Arc<dyn StorageBackend>, InstanceId, ContinuityId) {
    let storage: Arc<dyn StorageBackend> =
        Arc::new(SqliteStorage::in_memory().await.expect("in-memory sqlite"));
    let instance_id = InstanceId::new();
    let continuity_id = ContinuityId::new();
    storage
        .create_continuity_execution(&ContinuityExecution {
            continuity_id,
            tenant_id: TenantId::unchecked(TENANT),
            current_instance_id: instance_id,
            owner_runtime_id: RuntimeId::new(),
            epoch: ExecutionEpoch::initial(),
            state: OwnershipState::Owned,
            updated_at: chrono::Utc::now(),
        })
        .await
        .expect("continuity scope");
    (storage, instance_id, continuity_id)
}

fn receipt(
    continuity_id: ContinuityId,
    instance_id: InstanceId,
    state: EffectState,
) -> EffectReceipt {
    let now = chrono::Utc::now();
    EffectReceipt {
        id: EffectId::new(),
        tenant_id: TenantId::unchecked(TENANT),
        continuity_id,
        epoch: ExecutionEpoch::initial(),
        instance_id,
        block_id: BlockId::new("charge"),
        kind: EffectKind::Webhook,
        state,
        destination_fingerprint: "fp".into(),
        idempotency_key: None,
        request_sha256: "0".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    }
}

fn permanent_message(result: Result<EffectContext, StepError>) -> String {
    match result {
        Err(StepError::Permanent { message, .. }) => message,
        Err(other) => panic!("expected a permanent invariant, got {other:?}"),
        Ok(_) => panic!("expected a permanent invariant, got a live effect context"),
    }
}

#[tokio::test]
async fn live_effect_without_a_continuity_scope_is_a_permanent_invariant() {
    let storage: Arc<dyn StorageBackend> =
        Arc::new(SqliteStorage::in_memory().await.expect("in-memory sqlite"));

    let message =
        permanent_message(EffectContext::load(step(storage, InstanceId::new(), false)).await);

    assert_eq!(message, "effect continuity scope is missing");
}

#[tokio::test]
async fn live_effect_without_a_dispatched_receipt_is_a_permanent_invariant() {
    let (storage, instance_id, _) = scoped_store().await;

    let message = permanent_message(EffectContext::load(step(storage, instance_id, false)).await);

    assert_eq!(message, "dispatched effect receipt is missing");
}

#[tokio::test]
async fn live_effect_whose_receipt_is_no_longer_dispatched_is_refused() {
    // An `unknown` receipt (ambiguous outcome) must never be re-dispatched.
    let (storage, instance_id, continuity_id) = scoped_store().await;
    storage
        .create_effect_receipt(&receipt(continuity_id, instance_id, EffectState::Unknown))
        .await
        .expect("seed receipt");

    let message = permanent_message(EffectContext::load(step(storage, instance_id, false)).await);

    assert_eq!(message, "effect receipt is not in dispatched state");
}

#[tokio::test]
async fn live_effect_with_a_dispatched_receipt_exposes_its_id_as_the_idempotency_key() {
    let (storage, instance_id, continuity_id) = scoped_store().await;
    let seeded = receipt(continuity_id, instance_id, EffectState::Dispatched);
    storage
        .create_effect_receipt(&seeded)
        .await
        .expect("seed receipt");

    let context = EffectContext::load(step(storage, instance_id, false))
        .await
        .expect("live effect context");

    assert_eq!(
        context.dispatch_idempotency_key(),
        Some(seeded.id.into_uuid().to_string())
    );
}

#[test]
fn storage_failures_map_to_retryable_only_when_transient() {
    let cases = [
        (
            "connection loss",
            StorageError::Connection("socket closed".into()),
            true,
        ),
        (
            "query error",
            StorageError::Query("syntax error".into()),
            false,
        ),
    ];
    for (case, error, retryable) in cases {
        let mapped = storage_step_error(&error);
        let (is_retryable, message) = match mapped {
            StepError::Retryable { message, .. } => (true, message),
            StepError::Permanent { message, .. } => (false, message),
            StepError::ClaimLost => panic!("unexpected claim loss"),
        };
        assert_eq!(is_retryable, retryable, "case: {case}");
        assert!(
            message.contains(&error.to_string()),
            "case: {case}: mapped message must keep the storage detail: {message}"
        );
    }
}
