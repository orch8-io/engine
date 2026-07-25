use std::ops::Deref;

use orch8_engine::handlers::StepContext;
use orch8_types::continuity::{EffectReceipt, EffectState};
use orch8_types::error::{StepError, StorageError};

/// Handler context for an externally visible, durably guarded effect.
///
/// The engine persists a receipt in `dispatched` state before constructing
/// this value. Pass [`Self::dispatch_idempotency_key`] to the external
/// provider so a repeated delivery can be deduplicated against the same
/// durable effect identity. Returning success commits the receipt; errors and
/// timeouts leave conservative evidence for recovery instead of blindly
/// repeating an ambiguous effect.
#[derive(Clone)]
pub struct EffectContext {
    step: StepContext,
    receipt: Option<EffectReceipt>,
}

impl EffectContext {
    pub(crate) async fn load(step: StepContext) -> Result<Self, StepError> {
        if step.is_dry_run() {
            return Ok(Self {
                step,
                receipt: None,
            });
        }

        let execution = step
            .storage
            .get_continuity_execution_by_instance(&step.tenant_id, step.instance_id)
            .await
            .map_err(|error| storage_step_error(&error))?
            .ok_or_else(|| invariant_error("effect continuity scope is missing"))?;
        let receipt = step
            .storage
            .find_unresolved_effect_receipt(
                &step.tenant_id,
                execution.continuity_id,
                step.instance_id,
                &step.block_id,
                step.attempt,
            )
            .await
            .map_err(|error| storage_step_error(&error))?
            .ok_or_else(|| invariant_error("dispatched effect receipt is missing"))?;
        if receipt.state != EffectState::Dispatched {
            return Err(invariant_error("effect receipt is not in dispatched state"));
        }

        Ok(Self {
            step,
            receipt: Some(receipt),
        })
    }

    /// Stable key for the current durable dispatch.
    ///
    /// Supply this value as the downstream provider's idempotency key. It is
    /// absent only during dry runs, where the handler must not perform the
    /// external effect.
    #[must_use]
    pub fn dispatch_idempotency_key(&self) -> Option<String> {
        self.receipt
            .as_ref()
            .map(|receipt| receipt.id.into_uuid().to_string())
    }

    /// Durable receipt visible to the handler before external dispatch.
    #[must_use]
    pub const fn receipt(&self) -> Option<&EffectReceipt> {
        self.receipt.as_ref()
    }

    /// Access the ordinary handler context explicitly.
    #[must_use]
    pub const fn step(&self) -> &StepContext {
        &self.step
    }
}

impl Deref for EffectContext {
    type Target = StepContext;

    fn deref(&self) -> &Self::Target {
        &self.step
    }
}

fn storage_step_error(error: &StorageError) -> StepError {
    if error.is_transient() {
        StepError::Retryable {
            message: format!("failed to load durable effect evidence: {error}"),
            details: None,
        }
    } else {
        StepError::Permanent {
            message: format!("failed to load durable effect evidence: {error}"),
            details: None,
        }
    }
}

fn invariant_error(message: &str) -> StepError {
    StepError::Permanent {
        message: message.to_string(),
        details: None,
    }
}
