//! Shared helpers for built-in coordination handlers
//! (`emit_event`, `send_signal`, `query_instance`).
//!
//! Extracted from per-handler copies of the same logic so each handler body
//! stays focused on its own behaviour.

use serde_json::{json, Value};
use tracing::warn;
use uuid::Uuid;

use orch8_types::{
    error::{StepError, StorageError},
    ids::{InstanceId, TenantId},
};

/// Build a `StepError::Permanent` with no details.
pub(crate) fn permanent(message: impl Into<String>) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: None,
    }
}

/// Build a `StepError::Permanent` with structured details.
#[allow(dead_code)]
pub(crate) fn permanent_with_details(message: impl Into<String>, details: Value) -> StepError {
    StepError::Permanent {
        message: message.into(),
        details: Some(details),
    }
}

/// Parse an `InstanceId` from a params object field.
///
/// Returns `StepError::Permanent` if the field is missing, not a string, or
/// isn't a valid UUID.
pub(crate) fn parse_instance_id(params: &Value, field: &str) -> Result<InstanceId, StepError> {
    let id_str = params
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| permanent(format!("missing '{field}' string param")))?;
    let uuid =
        Uuid::parse_str(id_str).map_err(|e| permanent(format!("invalid '{field}' uuid: {e}")))?;
    Ok(InstanceId(uuid))
}

/// Map a `StorageError` to the appropriate `StepError` retryability.
///
/// Connection/pool/query errors are treated as retryable (transient infra
/// issues). Everything else — serialization, constraint violations, terminal
/// targets, logic errors — is permanent. `TerminalTarget` in particular is
/// permanent by definition: the target will never leave the terminal state,
/// so retrying would only produce the same error.
pub(crate) fn map_storage_err(e: &StorageError) -> StepError {
    match e {
        StorageError::Connection(_) | StorageError::PoolExhausted | StorageError::Query(_) => {
            StepError::Retryable {
                message: format!("storage: {e}"),
                details: None,
            }
        }
        _ => StepError::Permanent {
            message: format!("storage: {e}"),
            details: None,
        },
    }
}

/// Cross-tenant guard shared by the three coordination handlers.
///
/// Logs a warning (so the denied attempt is observable in tracing) and returns
/// a `Permanent` error that carries caller/target tenants in `details` for
/// debugging without leaking target existence through the message.
pub(crate) fn check_same_tenant(
    caller: &TenantId,
    target: &TenantId,
    op: &str,
) -> Result<(), StepError> {
    if caller == target {
        return Ok(());
    }
    warn!(
        caller_tenant = %caller.0,
        target_tenant = %target.0,
        operation = %op,
        "{op}: cross-tenant {op} denied"
    );
    Err(StepError::Permanent {
        message: format!("cross-tenant {op} denied"),
        details: Some(json!({
            "caller_tenant": caller.0,
            "target_tenant": target.0,
        })),
    })
}
