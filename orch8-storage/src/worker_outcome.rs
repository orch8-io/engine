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

/// Validated result metadata remains logical when its output is sealed.
/// Private fields prevent changing the result after deriving its receipt identity.
#[derive(Clone)]
pub struct WorkerCompletion {
    pub(crate) output: Value,
    pub(crate) logical_sha256: String,
    pub(crate) stored_sha256: String,
    pub(crate) stored_sealed: bool,
    pub(crate) provider_receipt_id: Option<String>,
    pub(crate) delegation: Option<WorkerDelegationCompletion>,
    pub(crate) reconciliation: Option<crate::worker_reconciliation::WorkerReconciliationApproval>,
}

/// Host-supplied application data travels with the original accepted result.
/// JSON text keeps the exact result representable in `PostgreSQL` JSONB.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct WorkerDelegationCompletion {
    pub(crate) body: Value,
    pub(crate) logical_sha256: String,
    pub(crate) stored_sha256: String,
}
impl WorkerDelegationCompletion {
    fn validate_stored(&self) -> Result<(), StorageError> {
        if digest(&self.body) != self.stored_sha256 {
            return Err(StorageError::Constraint(
                "delegation completion body changed".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn is_logical(&self) -> bool {
        digest(&self.body) == self.logical_sha256
    }
    pub(crate) fn with_stored_body(&self, body: Value) -> Self {
        Self {
            stored_sha256: digest(&body),
            body,
            logical_sha256: self.logical_sha256.clone(),
        }
    }
}
impl WorkerCompletion {
    #[must_use]
    pub fn new(output: Value) -> Self {
        let logical_sha256 = digest(&output);
        let provider_receipt_id = ["provider_receipt_id", "receipt_id", "request_id"]
            .into_iter()
            .find_map(|key| output.get(key).and_then(Value::as_str))
            .filter(|id| id.len() <= 256)
            .map(str::to_owned);
        Self {
            output,
            stored_sha256: logical_sha256.clone(),
            logical_sha256,
            stored_sealed: false,
            provider_receipt_id,
            delegation: None,
            reconciliation: None,
        }
    }
    /// A trusted operator explicitly accepts this exact legacy report.
    pub fn for_legacy_reconciliation(
        output: Value,
        approval: crate::worker_reconciliation::WorkerReconciliationApproval,
    ) -> Result<Self, StorageError> {
        approval.validate()?;
        let mut completion = Self::new(output);
        completion.reconciliation = Some(approval);
        Ok(completion)
    }
    /// Match a retry of this exact decision/result; unsupported metadata never matches.
    #[must_use]
    pub fn matches_reconciled_outcome(&self, outcome: &WorkerAcceptedOutcome) -> bool {
        self.reconciliation
            .as_ref()
            .zip(outcome.reconciliation.as_ref())
            .is_some_and(|(approval, decision)| {
                decision.validate(outcome).is_ok()
                    && *approval == decision.approval
                    && self.logical_sha256 == decision.logical_output_sha256
            })
    }
    pub(crate) fn with_stored_output(&self, output: Value) -> Self {
        Self {
            stored_sha256: digest(&output),
            output,
            logical_sha256: self.logical_sha256.clone(),
            stored_sealed: true,
            provider_receipt_id: self.provider_receipt_id.clone(),
            delegation: self.delegation.clone(),
            reconciliation: self.reconciliation.clone(),
        }
    }
    /// Only a hosting runtime uses this constructor; worker HTTP/gRPC reports
    /// cannot supply application metadata through their output objects.
    pub fn for_local_delegation(
        output: Value,
        id: &str,
        result: &Value,
    ) -> Result<Self, StorageError> {
        if id.is_empty() || id.len() > 256 {
            return Err(StorageError::Constraint(
                "invalid local delegation identity".into(),
            ));
        }
        let body = Value::String(serde_json::to_string(
            &serde_json::json!({"id":id,"result":result}),
        )?);
        let hash = digest(&body);
        let mut completion = Self::new(output);
        completion.delegation = Some(WorkerDelegationCompletion {
            body,
            logical_sha256: hash.clone(),
            stored_sha256: hash,
        });
        Ok(completion)
    }
    pub(crate) fn validate_stored(&self) -> Result<(), StorageError> {
        if let Some(approval) = &self.reconciliation {
            approval.validate()?;
        }
        if let Some(delegation) = &self.delegation {
            delegation.validate_stored()?;
            if !self.stored_sealed && !delegation.is_logical() {
                return Err(StorageError::Constraint(
                    "delegation completion is not logical".into(),
                ));
            }
        }
        if digest(&self.output) != self.stored_sha256
            || (!self.stored_sealed && self.logical_sha256 != self.stored_sha256)
        {
            return Err(StorageError::Constraint(
                "worker completion payload changed after preparation".into(),
            ));
        }
        Ok(())
    }
}
impl std::fmt::Debug for WorkerCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerCompletion")
            .field("logical_sha256", &self.logical_sha256)
            .finish_non_exhaustive()
    }
}

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
    /// Absent only on success records accepted before logical metadata was retained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logical_output_sha256: Option<String>,
    pub accepted_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) delegation: Option<WorkerDelegationCompletion>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconciliation: Option<crate::worker_reconciliation::WorkerReconciliationDecision>,
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
        completion: &WorkerCompletion,
    ) -> Self {
        let stored_output_sha256 = completion.stored_sha256.clone();
        Self {
            tenant_id,
            task_id,
            instance_id,
            claim,
            invocation,
            output: completion.output.clone(),
            logical_output_sha256: Some(completion.logical_sha256.clone()),
            stored_output_sha256,
            accepted_at: Utc::now(),
            delegation: completion.delegation.clone(),
            reconciliation: None,
        }
    }
    pub(crate) fn is_logical_output(&self) -> bool {
        self.logical_output_sha256
            .as_ref()
            .is_some_and(|hash| hash == &digest(&self.output))
    }
    pub(crate) fn validate(
        &self,
        tenant: &TenantId,
        task: Uuid,
        instance: InstanceId,
    ) -> Result<(), StorageError> {
        if let Some(delegation) = &self.delegation {
            delegation.validate_stored()?;
        }
        if self.tenant_id != *tenant
            || self.task_id != task
            || self.instance_id != instance
            || self.stored_output_sha256 != digest(&self.output)
        {
            return Err(StorageError::Constraint(
                "accepted worker outcome scope or payload mismatch".into(),
            ));
        }
        if let Some(decision) = &self.reconciliation {
            decision.validate(self)?;
        }
        if let Some(invocation) = &self.invocation {
            invocation.validate_scope(tenant, task, instance)?;
        }
        Ok(())
    }
}
pub(crate) fn digest(output: &Value) -> String {
    let mut result = String::with_capacity(64);
    for byte in Sha256::digest(orch8_types::ai::canonical_json(output).as_bytes()) {
        write!(result, "{byte:02x}").expect("writing into a String cannot fail");
    }
    result
}

/// Settle the effect pinned by an immutable binding, checking the logical
/// request proof for admitted work. Legacy binding strength is preserved.
/// A committed receipt without an accepted result needs reconciliation.
pub(crate) fn settled_receipt(
    invocation: &WorkerInvocation,
    proof: Option<&crate::worker_invocation::WorkerDispatchEffect>,
    mut receipt: orch8_types::continuity::EffectReceipt,
    completion: &WorkerCompletion,
) -> Result<orch8_types::continuity::EffectReceipt, StorageError> {
    use orch8_types::continuity::EffectState;
    let invalid = || {
        StorageError::Conflict(
            "worker completion effect identity or state requires reconciliation".into(),
        )
    };
    let owner = invocation.continuity.as_ref().ok_or_else(invalid)?;
    if invocation.effect_id != Some(receipt.id)
        || receipt.tenant_id != invocation.tenant_id
        || receipt.continuity_id != owner.continuity_id
        || !matches!(
            receipt.state,
            EffectState::Dispatched | EffectState::Unknown
        )
    {
        return Err(invalid());
    }
    // Legacy bindings retain the original receipt ID but do not certify an
    // admitted payload. Preserve that weaker classification in the journal.
    if let Some(proof) = proof
        && (receipt.id != proof.effect_id
            || receipt.kind != proof.kind
            || receipt.request_sha256 != proof.request_sha256
            || receipt.destination_fingerprint != proof.destination_fingerprint)
    {
        return Err(invalid());
    }
    receipt
        .provider_receipt_id
        .clone_from(&completion.provider_receipt_id);
    receipt
        .transition(EffectState::Committed, Utc::now())
        .map_err(|_| invalid())?;
    Ok(receipt)
}

/// Preserve number representations (including negative zero and integral
/// floats) across JSONB, without changing any established fingerprint algorithm.
pub(crate) fn encode_record(outcome: &WorkerAcceptedOutcome) -> Result<Value, StorageError> {
    let mut record = serde_json::to_value(outcome)?;
    let fields = record.as_object_mut().ok_or_else(|| {
        StorageError::Constraint("worker outcome record must be an object".into())
    })?;
    fields.insert(
        "output_encoding".into(),
        Value::String(
            if outcome.reconciliation.is_some() {
                "json_text_reconciled_v1"
            } else {
                "json_text_v1"
            }
            .into(),
        ),
    );
    fields.insert(
        "output".into(),
        Value::String(serde_json::to_string(&outcome.output)?),
    );
    Ok(record)
}

/// Read the original 134 representation as well as the versioned encoding.
/// Unknown codecs fail closed rather than interpreting encoded data as output.
pub(crate) fn decode_record(mut record: Value) -> Result<WorkerAcceptedOutcome, StorageError> {
    let reconciled = record
        .get("reconciliation")
        .is_some_and(|value| !value.is_null());
    if reconciled
        != (record.get("output_encoding").and_then(Value::as_str)
            == Some("json_text_reconciled_v1"))
    {
        return Err(StorageError::Constraint(
            "worker reconciliation requires its versioned reader".into(),
        ));
    }
    if let Some(encoding) = record.get("output_encoding") {
        if !matches!(
            encoding.as_str(),
            Some("json_text_v1" | "json_text_reconciled_v1")
        ) {
            return Err(StorageError::Constraint(
                "unsupported worker outcome output encoding".into(),
            ));
        }
        let body = record
            .get("output")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                StorageError::Constraint("worker outcome encoded output must be text".into())
            })?;
        let output = serde_json::from_str(body)?;
        let fields = record.as_object_mut().ok_or_else(|| {
            StorageError::Constraint("worker outcome record must be an object".into())
        })?;
        fields.insert("output".into(), output);
        fields.remove("output_encoding");
    }
    Ok(serde_json::from_value(record)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn record_codec_preserves_values_reads_legacy_and_rejects_unknown_encodings() {
        let outcome = WorkerAcceptedOutcome::new(
            TenantId::unchecked("codec-test"),
            Uuid::now_v7(),
            InstanceId::new(),
            WorkerClaim::new("worker", 1),
            None,
            &WorkerCompletion::new(serde_json::json!({"value":1e18,"zero":-0.0})),
        );
        let encoded = encode_record(&outcome).unwrap();
        assert_eq!(encoded["output_encoding"], "json_text_v1");
        assert!(encoded["output"].is_string());
        assert_eq!(decode_record(encoded.clone()).unwrap(), outcome);
        assert_eq!(
            decode_record(serde_json::to_value(&outcome).unwrap()).unwrap(),
            outcome
        );
        let mut unknown = encoded.clone();
        unknown["output_encoding"] = Value::String("future_unknown".into());
        assert!(decode_record(unknown).is_err());
        let mut malformed = encoded;
        malformed["output"] = Value::Null;
        assert!(decode_record(malformed).is_err());
    }
    #[test]
    fn reconciled_records_require_explicit_codec_scope_and_supported_decision_version() {
        use crate::{
            worker_claim::WorkerClaimPrincipal,
            worker_reconciliation::{WorkerReconciliationApproval, WorkerReconciliationDecision},
        };
        let tenant = TenantId::unchecked("reconciled-codec");
        let task = Uuid::now_v7();
        let claim = WorkerClaim::new("old-worker", 0);
        let approval = WorkerReconciliationApproval::new(
            WorkerClaimPrincipal::Root {},
            tenant.clone(),
            task,
            claim.clone(),
            Uuid::now_v7(),
            "backend-ack".into(),
        )
        .unwrap();
        let completion = WorkerCompletion::for_legacy_reconciliation(
            serde_json::json!({"secret":"PRIVATE_OUTPUT_CANARY\0λ","number":-0.0}),
            approval.clone(),
        )
        .unwrap();
        let mut outcome = WorkerAcceptedOutcome::new(
            tenant.clone(),
            task,
            InstanceId::new(),
            claim,
            None,
            &completion,
        );
        outcome.reconciliation =
            Some(WorkerReconciliationDecision::prepare(&approval, &outcome, None).unwrap());
        let encoded = encode_record(&outcome).unwrap();
        assert_eq!(encoded["output_encoding"], "json_text_reconciled_v1");
        assert_eq!(decode_record(encoded.clone()).unwrap(), outcome);
        assert!(
            !serde_json::to_string(&outcome.reconciliation)
                .unwrap()
                .contains("PRIVATE_OUTPUT_CANARY")
        );
        let mut downgraded = encoded.clone();
        downgraded["output_encoding"] = serde_json::json!("json_text_v1");
        assert!(decode_record(downgraded).is_err());
        let mut missing = encoded.clone();
        missing.as_object_mut().unwrap().remove("reconciliation");
        assert!(decode_record(missing).is_err());
        let mut future = encoded.clone();
        future["reconciliation"]["version"] = serde_json::json!(2);
        let future = decode_record(future).unwrap();
        assert!(future.validate(&tenant, task, outcome.instance_id).is_err());
        assert!(!completion.matches_reconciled_outcome(&future));
        let mut scoped = encoded;
        scoped["reconciliation"]["approval"]["actor"]["realm"] = serde_json::json!("future-realm");
        assert!(decode_record(scoped).is_err());
    }
}

/// Result acceptance and native application are separate durable facts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerOutcomeApplication {
    Applied,
    AlreadyApplied,
    Deferred,
    /// The context snapshot changed; reload and merge before retrying.
    ContextChanged,
    ReconciliationRequired,
}

/// Prepared only from durable accepted evidence and a fresh native snapshot.
/// Private fields prevent callers substituting an output or execution target.
#[derive(Clone)]
pub struct WorkerOutcomeIntegration {
    pub(crate) accepted: WorkerAcceptedOutcome,
    pub(crate) expected_context: orch8_types::context::ExecutionContext,
    pub(crate) context: orch8_types::context::ExecutionContext,
    pub(crate) output: orch8_types::output::BlockOutput,
    pub(crate) within_context_limit: bool,
}

/// Recover application independently of the hot task or the reporting worker.
/// A bounded retry reloads and remerges concurrent context changes.
pub async fn integrate_worker_outcome(
    storage: &dyn crate::StorageBackend,
    tenant: &TenantId,
    task: Uuid,
    claim: &WorkerClaim,
    max_context_bytes: u32,
) -> Result<WorkerOutcomeApplication, StorageError> {
    use WorkerOutcomeApplication::{ContextChanged, Deferred, ReconciliationRequired};
    for _ in 0..3 {
        let Some(accepted) = storage
            .get_worker_accepted_outcome(Some(tenant), task, claim)
            .await?
        else {
            return Ok(ReconciliationRequired);
        };
        let Some(instance) = storage.get_instance(accepted.instance_id).await? else {
            return Ok(Deferred);
        };
        if instance.tenant_id != *tenant {
            return Ok(ReconciliationRequired);
        }
        let mut context = instance.context.clone();
        if let Some(fields) = accepted.output.as_object() {
            if !context.data.is_object() {
                context.data = serde_json::json!({});
            }
            context
                .data
                .as_object_mut()
                .expect("object initialized")
                .extend(fields.clone());
        }
        if let Some(delegation) = &accepted.delegation {
            if !delegation.is_logical() {
                return Err(StorageError::Constraint(
                    "delegation completion is not decrypted".into(),
                ));
            }
            let body: Value = serde_json::from_str(delegation.body.as_str().ok_or_else(|| {
                StorageError::Constraint("invalid delegation completion body".into())
            })?)?;
            let id = body
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.len() <= 256)
                .ok_or_else(|| {
                    StorageError::Constraint("invalid delegation completion identity".into())
                })?;
            if !context.data.is_object() {
                context.data = serde_json::json!({});
            }
            let delegations = context
                .data
                .as_object_mut()
                .expect("object initialized")
                .entry("delegations")
                .or_insert_with(|| serde_json::json!({}));
            if !delegations.is_object() {
                *delegations = serde_json::json!({});
            }
            delegations
                .as_object_mut()
                .expect("object initialized")
                .insert(id.to_owned(), body["result"].clone());
        }
        let within_context_limit = context.check_size(max_context_bytes).is_ok();
        let Some(invocation) = accepted.invocation.as_ref() else {
            return Ok(ReconciliationRequired);
        };
        let output = orch8_types::output::BlockOutput {
            id: Uuid::now_v7(),
            instance_id: accepted.instance_id,
            block_id: invocation.block_id.clone(),
            output: accepted.output.clone(),
            output_ref: None,
            output_size: u32::try_from(serde_json::to_vec(&accepted.output)?.len())
                .map_err(|_| StorageError::Constraint("worker output too large".into()))?,
            attempt: invocation.attempt,
            created_at: Utc::now(),
        };
        let integration = WorkerOutcomeIntegration {
            accepted,
            expected_context: instance.context,
            context,
            output,
            within_context_limit,
        };
        let result = storage
            .apply_worker_outcome_integration(&integration)
            .await?;
        if result != ContextChanged {
            return Ok(result);
        }
    }
    Ok(Deferred)
}

/// Runtime fields are unsealed. Compare the full invocation, never a block alias.
pub(crate) fn integration_source_matches(
    invocation: &WorkerInvocation,
    instance: &orch8_types::instance::TaskInstance,
    owner: Option<&orch8_types::continuity::ContinuityExecution>,
) -> bool {
    use orch8_types::continuity::OwnershipState;
    invocation.instance_id == instance.id
        && invocation.tenant_id == instance.tenant_id
        && invocation.sequence_id == instance.sequence_id
        && invocation.run_id.as_ref().is_some_and(|id| !id.is_empty())
        && invocation.run_id == instance.context.runtime.run_id
        && invocation.dry_run == instance.context.runtime.dry_run
        && match (&invocation.continuity, owner) {
            (None, None) => invocation.continuity_epoch.is_none(),
            (Some(expected), Some(actual)) => {
                actual.state == OwnershipState::Owned
                    && actual.tenant_id == invocation.tenant_id
                    && actual.current_instance_id == invocation.instance_id
                    && actual.continuity_id == expected.continuity_id
                    && actual.epoch.get() == expected.epoch
                    && invocation.continuity_epoch == Some(expected.epoch)
                    && actual.owner_runtime_id == expected.owner_runtime_id
            }
            _ => false,
        }
}

/// Small recovery identity: no provider output is loaded during queue scans.
#[derive(Debug, Clone)]
pub struct WorkerOutcomeRecovery {
    pub task_id: Uuid,
    pub tenant_id: TenantId,
    pub claim: WorkerClaim,
}
