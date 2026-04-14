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

#[cfg(test)]
mod tests {
    use super::*;

    // --- permanent() ---

    #[test]
    fn permanent_creates_permanent_error_with_no_details() {
        let err = permanent("something broke");
        match err {
            StepError::Permanent { message, details } => {
                assert_eq!(message, "something broke");
                assert!(details.is_none());
            }
            StepError::Retryable { .. } => panic!("expected Permanent"),
        }
    }

    #[test]
    fn permanent_accepts_string() {
        let err = permanent(String::from("owned string"));
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    // --- parse_instance_id() ---

    #[test]
    fn parse_instance_id_valid_uuid() {
        let id = uuid::Uuid::now_v7();
        let params = json!({"instance_id": id.to_string()});
        let result = parse_instance_id(&params, "instance_id").unwrap();
        assert_eq!(result.0, id);
    }

    #[test]
    fn parse_instance_id_missing_field() {
        let params = json!({"other": "value"});
        let err = parse_instance_id(&params, "instance_id").unwrap_err();
        match err {
            StepError::Permanent { message, .. } => {
                assert!(message.contains("missing 'instance_id' string param"));
            }
            StepError::Retryable { .. } => panic!("expected Permanent"),
        }
    }

    #[test]
    fn parse_instance_id_not_a_string() {
        let params = json!({"instance_id": 42});
        let err = parse_instance_id(&params, "instance_id").unwrap_err();
        match err {
            StepError::Permanent { message, .. } => {
                assert!(message.contains("missing 'instance_id' string param"));
            }
            StepError::Retryable { .. } => panic!("expected Permanent"),
        }
    }

    #[test]
    fn parse_instance_id_invalid_uuid() {
        let params = json!({"instance_id": "not-a-uuid"});
        let err = parse_instance_id(&params, "instance_id").unwrap_err();
        match err {
            StepError::Permanent { message, .. } => {
                assert!(message.contains("invalid 'instance_id' uuid"));
            }
            StepError::Retryable { .. } => panic!("expected Permanent"),
        }
    }

    #[test]
    fn parse_instance_id_custom_field_name() {
        let id = uuid::Uuid::now_v7();
        let params = json!({"target_id": id.to_string()});
        let result = parse_instance_id(&params, "target_id").unwrap();
        assert_eq!(result.0, id);
    }

    // --- map_storage_err() ---

    #[test]
    fn map_storage_err_connection_is_retryable() {
        let err = map_storage_err(&StorageError::Connection("down".into()));
        assert!(matches!(err, StepError::Retryable { .. }));
    }

    #[test]
    fn map_storage_err_pool_exhausted_is_retryable() {
        let err = map_storage_err(&StorageError::PoolExhausted);
        assert!(matches!(err, StepError::Retryable { .. }));
    }

    #[test]
    fn map_storage_err_query_is_retryable() {
        let err = map_storage_err(&StorageError::Query("timeout".into()));
        assert!(matches!(err, StepError::Retryable { .. }));
    }

    #[test]
    fn map_storage_err_not_found_is_permanent() {
        let err = map_storage_err(&StorageError::NotFound {
            entity: "instance",
            id: "x".into(),
        });
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn map_storage_err_conflict_is_permanent() {
        let err = map_storage_err(&StorageError::Conflict("dup".into()));
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn map_storage_err_terminal_target_is_permanent() {
        let err = map_storage_err(&StorageError::TerminalTarget {
            entity: "instance".into(),
            id: "abc".into(),
        });
        assert!(matches!(err, StepError::Permanent { .. }));
    }

    #[test]
    fn map_storage_err_message_contains_original() {
        let err = map_storage_err(&StorageError::Connection("host:5432".into()));
        if let StepError::Retryable { message, .. } = err {
            assert!(message.contains("host:5432"));
        }
    }

    // --- check_same_tenant() ---

    #[test]
    fn check_same_tenant_same_tenant_ok() {
        let t = TenantId("t1".into());
        assert!(check_same_tenant(&t, &t, "send_signal").is_ok());
    }

    #[test]
    fn check_same_tenant_different_tenants_error() {
        let caller = TenantId("t1".into());
        let target = TenantId("t2".into());
        let err = check_same_tenant(&caller, &target, "send_signal").unwrap_err();
        match err {
            StepError::Permanent { message, details } => {
                assert!(message.contains("cross-tenant send_signal denied"));
                let d = details.unwrap();
                assert_eq!(d["caller_tenant"], "t1");
                assert_eq!(d["target_tenant"], "t2");
            }
            StepError::Retryable { .. } => panic!("expected Permanent"),
        }
    }

    #[test]
    fn check_same_tenant_includes_op_in_message() {
        let caller = TenantId("a".into());
        let target = TenantId("b".into());
        let err = check_same_tenant(&caller, &target, "query_instance").unwrap_err();
        if let StepError::Permanent { message, .. } = err {
            assert!(message.contains("query_instance"));
        }
    }
}
