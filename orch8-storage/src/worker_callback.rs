//! Callback grants are sent only with issued claims and never form part of a task payload.

use chrono::{DateTime, Utc};
use orch8_types::{
    error::StorageError,
    ids::TenantId,
    worker::{CallbackParameter, WorkerClaim, WorkerTask},
};
use uuid::Uuid;

/// Exact callback authorization to recheck inside the completion transaction.
#[derive(Clone)]
pub struct WorkerCallbackBinding {
    pub task_id: Uuid,
    pub claim: WorkerClaim,
    pub tenant_id: TenantId,
    pub expires_at: DateTime<Utc>,
    pub(crate) token: String,
}

impl WorkerCallbackBinding {
    pub(crate) fn issue(
        task: &WorkerTask,
        tenant_id: TenantId,
    ) -> Result<Option<Self>, StorageError> {
        if !task.wait_for_callback {
            return Ok(None);
        }
        let invalid = || StorageError::Constraint("invalid callback worker claim".into());
        let expires_at = task
            .timeout_ms
            .and_then(chrono::Duration::try_milliseconds)
            .and_then(|timeout| task.created_at.checked_add_signed(timeout))
            .ok_or_else(invalid)?;
        let worker = task.worker_id.clone().ok_or_else(invalid)?;
        Ok(Some(Self {
            task_id: task.id,
            claim: WorkerClaim::new(worker, task.claim_epoch),
            tenant_id,
            expires_at,
            token: format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple()),
        }))
    }

    #[must_use]
    pub fn token(&self) -> &str {
        &self.token
    }

    /// Restore the actual parameter values from the server-owned insertion paths.
    pub fn materialize_delivery(&self, task: &mut WorkerTask) -> Result<(), StorageError> {
        let invalid =
            || StorageError::Constraint("callback delivery does not match its issued claim".into());
        if !task.wait_for_callback
            || task.id != self.task_id
            || task.claim_epoch != self.claim.claim_epoch
            || task.worker_id.as_deref() != Some(self.claim.worker_id.as_str())
        {
            return Err(invalid());
        }
        let paths = task
            .context
            .as_object_mut()
            .and_then(|context| context.remove(orch8_types::worker::ASL_CALLBACK_PATHS))
            .ok_or_else(invalid)?;
        let paths = serde_json::from_value::<Vec<CallbackParameter>>(paths)?;
        for parameter in paths {
            let (path, value) = match parameter {
                CallbackParameter::Token(path) => {
                    (path, serde_json::Value::String(self.token.clone()))
                }
                CallbackParameter::Intrinsic {
                    pointer,
                    expression,
                } => (
                    pointer,
                    expression
                        .evaluate_callback(&self.token)
                        .map_err(|_| invalid())?,
                ),
            };
            let slot = task.params.pointer_mut(&path).ok_or_else(invalid)?;
            *slot = value;
        }
        Ok(())
    }
}

#[must_use]
pub fn valid_token(token: &str) -> bool {
    token.len() == 64
        && token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
