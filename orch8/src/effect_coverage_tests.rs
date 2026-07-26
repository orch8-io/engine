//! Coverage tests for durable effect-dispatch context semantics.
//!
//! Count contract: 12 independently named unit tests.

use super::*;
use std::sync::Arc;

use orch8_storage::sqlite::SqliteStorage;
use orch8_types::context::{ExecutionContext, RuntimeContext};
use orch8_types::continuity::{ContinuityId, EffectId, EffectKind, ExecutionEpoch};
use orch8_types::ids::{BlockId, InstanceId, TenantId};

async fn step(dry_run: bool) -> StepContext {
    let storage = SqliteStorage::in_memory().await.expect("in-memory sqlite");
    StepContext {
        instance_id: InstanceId::new(),
        tenant_id: TenantId::unchecked("tenant_1"),
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
        storage: Arc::new(storage),
        wait_for_input: None,
    }
}

fn receipt() -> EffectReceipt {
    let now = chrono::Utc::now();
    EffectReceipt {
        id: EffectId::new(),
        tenant_id: TenantId::unchecked("tenant_1"),
        continuity_id: ContinuityId::new(),
        epoch: ExecutionEpoch::initial(),
        instance_id: InstanceId::new(),
        block_id: BlockId::new("charge"),
        kind: EffectKind::Webhook,
        state: EffectState::Dispatched,
        destination_fingerprint: "fp".into(),
        idempotency_key: None,
        request_sha256: "0".repeat(64),
        provider_receipt_id: None,
        attempt: 1,
        created_at: now,
        updated_at: now,
    }
}

macro_rules! retry_mapping {
    ($name:ident, $error:expr, $retryable:expr) => {
        #[test]
        fn $name() {
            let mapped = storage_step_error(&$error);
            match mapped {
                StepError::Retryable { .. } => assert!($retryable),
                StepError::Permanent { .. } => assert!(!$retryable),
            }
        }
    };
}

retry_mapping!(
    coverage_effect_001_connection_loss_maps_to_retryable,
    StorageError::Connection("socket closed".into()),
    true
);
retry_mapping!(
    coverage_effect_002_pool_exhaustion_maps_to_retryable,
    StorageError::PoolExhausted,
    true
);
retry_mapping!(
    coverage_effect_003_transient_backend_error_maps_to_retryable,
    StorageError::Backend("throttled".into()),
    true
);
retry_mapping!(
    coverage_effect_004_unsupported_operation_maps_to_permanent,
    StorageError::Unsupported("no artifact store".into()),
    false
);
retry_mapping!(
    coverage_effect_005_query_error_maps_to_permanent,
    StorageError::Query("syntax error".into()),
    false
);

#[test]
fn coverage_effect_006_invariant_error_is_permanent_with_the_message() {
    let error = invariant_error("effect receipt is not in dispatched state");
    let StepError::Permanent { message, details } = error else {
        panic!("expected permanent invariant, got {error:?}");
    };
    assert_eq!(message, "effect receipt is not in dispatched state");
    assert!(details.is_none());
}

#[tokio::test]
async fn coverage_effect_007_dry_run_load_carries_no_receipt() {
    let context = EffectContext::load(step(true).await).await.unwrap();
    assert!(context.receipt().is_none());
    assert_eq!(context.dispatch_idempotency_key(), None);
}

#[tokio::test]
async fn coverage_effect_008_dispatch_key_is_the_durable_receipt_uuid() {
    let receipt = receipt();
    let expected = receipt.id.into_uuid().to_string();
    let context = EffectContext {
        step: step(false).await,
        receipt: Some(receipt),
    };
    assert_eq!(context.dispatch_idempotency_key(), Some(expected));
    assert_eq!(
        context.receipt().map(|value| value.state),
        Some(EffectState::Dispatched)
    );
}

#[tokio::test]
async fn coverage_effect_009_missing_continuity_scope_is_a_permanent_invariant() {
    // Live (non-dry-run) dispatch against storage with no continuity row must
    // fail permanently — retrying cannot invent the missing scope.
    let result = EffectContext::load(step(false).await).await;
    let Err(error) = result else {
        panic!("expected a continuity-scope invariant, got a live context");
    };
    let StepError::Permanent { message, .. } = error else {
        panic!("expected permanent invariant, got {error:?}");
    };
    assert!(message.contains("continuity scope"), "{message}");
}

#[tokio::test]
async fn coverage_effect_010_context_derefs_to_the_step_context() {
    let context = EffectContext {
        step: step(true).await,
        receipt: None,
    };
    // `Deref<Target = StepContext>` exposes the ordinary handler surface.
    assert_eq!(context.attempt, 1);
    assert_eq!(context.block_id, BlockId::new("charge"));
    assert_eq!(context.step().tenant_id, TenantId::unchecked("tenant_1"));
}

retry_mapping!(
    coverage_effect_011_conflict_error_maps_to_permanent,
    StorageError::Conflict("duplicate idempotency key".into()),
    false
);

#[test]
fn coverage_effect_012_mapped_error_message_keeps_the_storage_detail() {
    // The mapped StepError must retain the underlying storage failure so logs
    // and dead-letter records explain what actually went wrong.
    let mapped = storage_step_error(&StorageError::Connection("socket closed".into()));
    let StepError::Retryable { message, details } = mapped else {
        panic!("expected retryable mapping, got {mapped:?}");
    };
    assert!(message.contains("socket closed"), "{message}");
    assert!(details.is_none());
}
