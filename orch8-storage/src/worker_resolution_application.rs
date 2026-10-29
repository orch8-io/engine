//! Apply a retained worker failure without reinterpreting it as a new report.
use crate::{
    worker_invocation::WorkerInvocationTarget, worker_resolution::WorkerResolutionOutcome,
};
use orch8_types::{
    error::StorageError,
    ids::TenantId,
    worker::{WorkerAttemptEventKind, WorkerTask, WorkerTaskResolutionAction},
};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct WorkerResolutionRecovery {
    pub id: Uuid,
    pub tenant_id: TenantId,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerResolutionApplication {
    Applied,
    AlreadyApplied,
    Deferred,
    ReconciliationRequired,
}
#[derive(Clone)]
pub struct WorkerResolutionIntegration {
    pub(crate) report: WorkerResolutionOutcome,
    pub(crate) action: WorkerTaskResolutionAction,
}
impl WorkerResolutionIntegration {
    pub fn new(
        report: WorkerResolutionOutcome,
        action: WorkerTaskResolutionAction,
    ) -> Result<Self, StorageError> {
        let plan = Self { report, action };
        plan.validate()?;
        Ok(plan)
    }
    pub(crate) fn validate(&self) -> Result<(), StorageError> {
        let invalid =
            || StorageError::Constraint("invalid retained worker failure application".into());
        let report = &self.report;
        let source = report.invocation.as_ref().ok_or_else(invalid)?;
        if report.event != WorkerAttemptEventKind::Failed
            || report.action["kind"] != "fail_task_only"
            || !report.is_logical_reason()
            || !report.reason.is_string()
            || source.handler_name == "orch8.delegation"
            || matches!(source.target, WorkerInvocationTarget::Unbound { .. })
        {
            return Err(invalid());
        }
        source.validate_scope(&report.tenant_id, report.task_id, report.instance_id)?;
        match &self.action {
            WorkerTaskResolutionAction::Retry {
                retry_task,
                retry_output,
                node_id,
                ..
            } => {
                if !report.retryable || !source.accepts_node(*node_id) {
                    return Err(invalid());
                }
                source.validate_retry_task(retry_task)?;
                source.validate_retry_output(retry_output.as_deref())?;
            }
            WorkerTaskResolutionAction::FailNode { node_id, .. } => {
                if !source.accepts_node(*node_id) || node_id.is_none() {
                    return Err(invalid());
                }
            }
            WorkerTaskResolutionAction::FailInstance => {
                if !matches!(source.target, WorkerInvocationTarget::Flat { .. }) {
                    return Err(invalid());
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
    /// This reservation contains no certified original input. It remains
    /// unclaimable until normal dispatch renders and admits the next attempt.
    pub fn retry_source(report: &WorkerResolutionOutcome) -> Result<WorkerTask, StorageError> {
        let source = report
            .invocation
            .as_ref()
            .ok_or_else(|| StorageError::Constraint("missing retained worker invocation".into()))?;
        let context = orch8_types::context::ExecutionContext {
            runtime: orch8_types::context::RuntimeContext {
                run_id: source.run_id.clone(),
                dry_run: source.dry_run,
                ..Default::default()
            },
            ..Default::default()
        };
        let task: WorkerTask = serde_json::from_value(serde_json::json!({
            "id":report.task_id,"instance_id":report.instance_id,"block_id":source.block_id,
            "handler_name":source.handler_name,"attempt":source.attempt,"state":"failed",
            "context":context,"params":{},"effect_id":report.effect_id,"continuity_epoch":source.continuity_epoch,
            "claim_epoch":report.claim_epoch,"worker_id":report.worker_id,"created_at":report.resolved_at
        }))?;
        Ok(task)
    }
    pub(crate) fn action_summary(&self) -> serde_json::Value {
        match &self.action {
            WorkerTaskResolutionAction::Retry {
                retry_task,
                node_id,
                ..
            } => {
                serde_json::json!({"kind":"retry","task_id":retry_task.id,"attempt":retry_task.attempt,"node_id":node_id})
            }
            WorkerTaskResolutionAction::FailNode { node_id, .. } => {
                serde_json::json!({"kind":"fail_node","node_id":node_id})
            }
            WorkerTaskResolutionAction::FailInstance => serde_json::json!({"kind":"fail_instance"}),
            _ => serde_json::json!({"kind":"invalid"}),
        }
    }
}
