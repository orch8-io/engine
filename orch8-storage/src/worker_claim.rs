//! Immutable claimant identity, separate from reusable hot leases and result receipts.
use crate::{worker_admission::WorkerDispatchAdmission, worker_invocation::WorkerInvocation};
use orch8_types::{
    continuity::{RuntimeCapabilities, RuntimeId, RuntimeKind},
    error::StorageError,
    ids::{BlockId, InstanceId, TenantId},
    worker::{WorkerClaim, WorkerTask, WorkerTaskState},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Empty struct variants are deliberate: Serde unit variants discard unknown fields.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum WorkerClaimPrincipal {
    /// No authenticated principal was captured by this claim path.
    Legacy {},
    /// Explicitly unsecured transport, never authenticated evidence.
    Unsecured {},
    Root {},
    ApiKey {
        tenant_id: TenantId,
        key_id: String,
    },
    Runtime {
        tenant_id: TenantId,
        runtime_id: RuntimeId,
        runtime_kind: RuntimeKind,
        device_id: Option<String>,
    },
    Workload {
        tenant_id: TenantId,
        identity: String,
    },
}
fn identifier(value: &str, max: usize) -> bool {
    !value.trim().is_empty() && value.len() <= max && !value.contains('\0')
}
pub(crate) fn invalid() -> StorageError {
    StorageError::Constraint("invalid issued worker claim scope".into())
}
impl WorkerClaimPrincipal {
    pub fn validate(&self) -> Result<(), StorageError> {
        match self {
            Self::Legacy {} | Self::Unsecured {} | Self::Root {} => {}
            Self::ApiKey { tenant_id, key_id } => {
                if !identifier(tenant_id.as_str(), 128) || !identifier(key_id, 256) {
                    return Err(invalid());
                }
            }
            Self::Workload {
                tenant_id,
                identity,
            } => {
                if !identifier(tenant_id.as_str(), 128) || !identifier(identity, 128) {
                    return Err(invalid());
                }
            }
            Self::Runtime {
                tenant_id,
                runtime_kind,
                device_id,
                ..
            } => {
                if !identifier(tenant_id.as_str(), 128)
                    || match runtime_kind {
                        RuntimeKind::Browser => device_id.is_some(),
                        RuntimeKind::Mobile => {
                            device_id.as_ref().is_none_or(|id| !identifier(id, 256))
                        }
                        _ => true,
                    }
                {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
    pub fn tenant_id(&self) -> Option<&TenantId> {
        match self {
            Self::ApiKey { tenant_id, .. }
            | Self::Runtime { tenant_id, .. }
            | Self::Workload { tenant_id, .. } => Some(tenant_id),
            _ => None,
        }
    }
    pub fn has_authenticated_owner(&self) -> bool {
        !matches!(self, Self::Legacy {} | Self::Unsecured {})
    }
    pub(crate) fn accepts(&self, tenant: &TenantId, worker: &str) -> bool {
        self.tenant_id().is_none_or(|id| id == tenant)
            && match self {
                Self::Runtime { runtime_id, .. } => runtime_id.to_string() == worker,
                _ => true,
            }
    }
}
/// A validated selector and server-verified principal. Transport adapters must
/// derive the principal from authentication, never from worker request JSON.
#[derive(Clone)]
pub struct WorkerClaimRequest {
    pub(crate) principal: WorkerClaimPrincipal,
    pub(crate) handler_name: String,
    pub(crate) worker_id: String,
    pub(crate) tenant_id: Option<TenantId>,
    pub(crate) queue_name: Option<String>,
    pub(crate) capabilities: Option<RuntimeCapabilities>,
    pub(crate) limit: u32,
}
impl WorkerClaimRequest {
    pub fn new(
        principal: WorkerClaimPrincipal,
        handler_name: String,
        worker_id: String,
        tenant_id: Option<TenantId>,
        queue_name: Option<String>,
        capabilities: Option<RuntimeCapabilities>,
        limit: u32,
    ) -> Result<Self, StorageError> {
        let tenant_id = tenant_id.or_else(|| principal.tenant_id().cloned());
        let value = Self {
            principal,
            handler_name,
            worker_id,
            tenant_id,
            queue_name,
            capabilities,
            limit,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        self.principal.validate()?;
        if matches!(self.principal, WorkerClaimPrincipal::Legacy {})
            || !identifier(&self.handler_name, 256)
            || !identifier(&self.worker_id, 128)
            || self.limit > 1000
            || self
                .tenant_id
                .as_ref()
                .is_some_and(|tenant| !identifier(tenant.as_str(), 128))
            || self
                .queue_name
                .as_ref()
                .is_some_and(|queue| !identifier(queue, 256))
            || self
                .principal
                .tenant_id()
                .is_some_and(|id| Some(id) != self.tenant_id.as_ref())
        {
            return Err(invalid());
        }
        if let WorkerClaimPrincipal::Runtime {
            runtime_id,
            runtime_kind,
            ..
        } = &self.principal
            && (self.worker_id != runtime_id.to_string()
                || self.capabilities.as_ref().is_none_or(|caps| {
                    caps.runtime_id != *runtime_id || caps.kind != *runtime_kind
                }))
        {
            return Err(invalid());
        }
        Ok(())
    }
}
pub(crate) struct ClaimRuntime<'a> {
    pub capabilities: &'a RuntimeCapabilities,
    pub principal: Option<&'a WorkerClaimPrincipal>,
}

/// Contains no params, application context, checkpoint or credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerIssuedClaim {
    pub version: u8,
    pub task_id: Uuid,
    pub claim: WorkerClaim,
    pub tenant_id: TenantId,
    pub instance_id: InstanceId,
    pub block_id: BlockId,
    pub handler_name: String,
    pub queue_name: Option<String>,
    pub attempt: u16,
    pub principal: WorkerClaimPrincipal,
    pub invocation: Option<WorkerInvocation>,
    pub admission: Option<WorkerDispatchAdmission>,
    pub issued_at: chrono::DateTime<chrono::Utc>,
}
impl WorkerIssuedClaim {
    pub(crate) fn new(
        task: &WorkerTask,
        tenant_id: TenantId,
        principal: Option<&WorkerClaimPrincipal>,
        invocation: Option<WorkerInvocation>,
        admission: Option<WorkerDispatchAdmission>,
    ) -> Result<Self, StorageError> {
        if task.state != WorkerTaskState::Claimed {
            return Err(invalid());
        }
        let value = Self {
            version: 1,
            task_id: task.id,
            claim: WorkerClaim::new(
                task.worker_id.clone().ok_or_else(invalid)?,
                task.claim_epoch,
            ),
            tenant_id,
            instance_id: task.instance_id,
            block_id: task.block_id.clone(),
            handler_name: task.handler_name.clone(),
            queue_name: task.queue_name.clone(),
            attempt: task.attempt,
            principal: principal
                .cloned()
                .unwrap_or(WorkerClaimPrincipal::Legacy {}),
            invocation,
            admission,
            issued_at: task.claimed_at.ok_or_else(invalid)?,
        };
        value.validate()?;
        Ok(value)
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        self.principal.validate()?;
        if self.version != 1
            || self.claim.claim_epoch == 0
            || i64::try_from(self.claim.claim_epoch).is_err()
            || !identifier(&self.claim.worker_id, 128)
            || !self
                .principal
                .accepts(&self.tenant_id, &self.claim.worker_id)
        {
            return Err(invalid());
        }
        if let Some(source) = &self.invocation {
            source.validate_scope(&self.tenant_id, self.task_id, self.instance_id)?;
            if source.block_id != self.block_id
                || source.handler_name != self.handler_name
                || source.attempt != self.attempt
            {
                return Err(invalid());
            }
        }
        if self.admission.as_ref().is_some_and(|admission| {
            self.invocation.as_ref() != Some(&admission.invocation)
                || admission.effect.as_ref().map(|effect| effect.effect_id)
                    != admission.invocation.effect_id
        }) {
            return Err(invalid());
        }
        Ok(())
    }
    pub fn belongs_to(&self, principal: &WorkerClaimPrincipal) -> bool {
        self.validate().is_ok()
            && self.principal.has_authenticated_owner()
            && self.principal == *principal
    }
}
