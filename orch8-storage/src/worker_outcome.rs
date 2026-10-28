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
        }
    }
    pub(crate) fn with_stored_output(&self, output: Value) -> Self {
        Self {
            stored_sha256: digest(&output),
            output,
            logical_sha256: self.logical_sha256.clone(),
            stored_sealed: true,
            provider_receipt_id: self.provider_receipt_id.clone(),
        }
    }
    pub(crate) fn validate_stored(&self) -> Result<(), StorageError> {
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
        Value::String("json_text_v1".into()),
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
    if let Some(encoding) = record.get("output_encoding") {
        if encoding.as_str() != Some("json_text_v1") {
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
}
