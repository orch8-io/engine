//! Accepted completion evidence retained independently of hot task and native history.
use crate::worker_invocation::WorkerInvocation;
use chrono::{DateTime, Utc};
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
    worker::WorkerClaim,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;
use uuid::Uuid;

/// This is acceptance evidence, not proof that the workflow integrated the result.
/// The output is sealed by `EncryptingStorage` before it enters a backend.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerAcceptedOutcome {
    pub tenant_id: TenantId,
    pub task_id: Uuid,
    pub instance_id: InstanceId,
    pub claim: WorkerClaim,
    pub invocation: Option<WorkerInvocation>,
    pub output: Value,
    pub stored_output_sha256: String,
    pub accepted_at: DateTime<Utc>,
}
impl std::fmt::Debug for WorkerAcceptedOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerAcceptedOutcome")
            .field("task_id", &self.task_id)
            .field("instance_id", &self.instance_id)
            .field("accepted_at", &self.accepted_at)
            .finish_non_exhaustive()
    }
}
impl WorkerAcceptedOutcome {
    pub(crate) fn new(
        tenant_id: TenantId,
        task_id: Uuid,
        instance_id: InstanceId,
        claim: WorkerClaim,
        invocation: Option<WorkerInvocation>,
        output: Value,
    ) -> Self {
        let stored_output_sha256 = digest(&output);
        Self {
            tenant_id,
            task_id,
            instance_id,
            claim,
            invocation,
            output,
            stored_output_sha256,
            accepted_at: Utc::now(),
        }
    }
    pub(crate) fn validate(
        &self,
        tenant: &TenantId,
        task: Uuid,
        instance: InstanceId,
    ) -> Result<(), StorageError> {
        if self.tenant_id != *tenant
            || self.task_id != task
            || self.instance_id != instance
            || self.stored_output_sha256 != digest(&self.output)
        {
            return Err(StorageError::Constraint(
                "accepted worker outcome scope or payload mismatch".into(),
            ));
        }
        if let Some(invocation) = &self.invocation {
            invocation.validate_scope(tenant, task, instance)?;
        }
        Ok(())
    }
}
fn digest(output: &Value) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(orch8_types::ai::canonical_json(output).as_bytes()) {
        write!(result, "{byte:02x}").expect("writing into a String cannot fail");
    }
    result
}
