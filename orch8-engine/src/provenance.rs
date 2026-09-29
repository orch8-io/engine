//! Provenance recording shared by every completion surface (HTTP, gRPC, the
//! mobile delegation pump).
//!
//! Two pieces live here so the transports cannot drift apart:
//!
//! * [`append_provenance_digest`] appends one entry to an execution's
//!   hash-chained provenance log (signed when a signer is configured),
//!   retrying the optimistic head race a bounded number of times;
//! * [`record_worker_output_provenance`] records which runtime (kind + id)
//!   produced a remote step output — an audit event plus, for
//!   continuity-enrolled instances, a provenance entry — as evidence next to
//!   the output, never by mutating the output JSON.

use chrono::Utc;
use ed25519_dalek::SigningKey;
use orch8_storage::StorageBackend;
use orch8_types::continuity::ContinuityExecution;
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::worker::WorkerTask;
use serde_json::Value;

/// Signing identity for provenance entries (the engine's continuity key).
#[derive(Clone, Copy)]
pub struct ProvenanceSigner<'a> {
    pub key_id: &'a str,
    pub signing_key: &'a SigningKey,
}

impl std::fmt::Debug for ProvenanceSigner<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProvenanceSigner")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

/// Audit event type of a remote step output's provenance record.
pub const WORKER_OUTPUT_PROVENANCE_EVENT: &str = "worker_output_provenance";
/// Provenance-chain entry kind of a remote step output.
pub const REMOTE_STEP_OUTPUT_KIND: &str = "remote_step_output";

/// Append one entry (by payload digest) to `execution`'s provenance chain.
/// The chain head is re-read on every attempt; a concurrent append makes the
/// insert fail and the loop retries on the new head (at most eight times).
pub async fn append_provenance_digest(
    storage: &dyn StorageBackend,
    signer: Option<ProvenanceSigner<'_>>,
    execution: &ContinuityExecution,
    kind: &str,
    summary: &str,
    payload_sha256: &str,
) -> Result<(), StorageError> {
    const ATTEMPTS: u32 = 8;
    let mut attempt = 0;
    loop {
        let previous = storage
            .get_provenance_head(&execution.tenant_id, execution.continuity_id)
            .await?
            .map(|entry| entry.entry_sha256);
        let mut entry = crate::continuity::build_provenance_entry_with_summary(
            execution,
            kind,
            Some(summary.into()),
            payload_sha256,
            previous,
            Utc::now(),
        );
        if let Some(signer) = signer {
            entry = crate::continuity::sign_provenance_entry(
                entry,
                signer.key_id.to_owned(),
                signer.signing_key,
            );
        }
        match storage.append_provenance(&entry).await {
            Ok(()) => return Ok(()),
            Err(error) if attempt + 1 >= ATTEMPTS => return Err(error),
            Err(_) => {
                attempt += 1;
                tokio::task::yield_now().await;
            }
        }
    }
}

/// Record which runtime produced a remote step output: a
/// `worker_output_provenance` audit event (runtime kind + id, claim epoch,
/// effect id, output digest and size) and, when the instance belongs to a
/// continuity execution, a `remote_step_output` provenance entry.
///
/// Tasks claimed without a runtime kind (legacy capability-less polls) have
/// no provenance to record. Best-effort: failures are logged and never fail
/// the completion that triggered them.
pub async fn record_worker_output_provenance(
    storage: &dyn StorageBackend,
    signer: Option<ProvenanceSigner<'_>>,
    tenant_id: &TenantId,
    task: &WorkerTask,
    output: &Value,
) {
    let Some(kind) = task.claimed_runtime_kind else {
        return;
    };
    let encoded = serde_json::to_vec(output).unwrap_or_default();
    let output_sha256 = crate::dataflow::hex_sha256(&encoded);
    let runtime_id = task.worker_id.clone().unwrap_or_default();
    let details = serde_json::json!({
        "task_id": task.id,
        "runtime_kind": kind,
        "runtime_id": runtime_id,
        "claim_epoch": task.claim_epoch,
        "effect_id": task.effect_id,
        "output_sha256": output_sha256,
        "output_bytes": encoded.len(),
        "untrusted_page_data": kind == orch8_types::continuity::RuntimeKind::Browser,
    });
    let entry = orch8_types::audit::AuditLogEntry {
        id: uuid::Uuid::now_v7(),
        instance_id: task.instance_id,
        tenant_id: tenant_id.clone(),
        event_type: WORKER_OUTPUT_PROVENANCE_EVENT.into(),
        from_state: None,
        to_state: None,
        block_id: Some(task.block_id.as_str().to_owned()),
        details: details.clone(),
        created_at: Utc::now(),
    };
    if let Err(error) = storage.append_audit_log(&entry).await {
        tracing::warn!(task_id = %task.id, %error, "failed to record worker output provenance");
    }
    match storage
        .get_continuity_execution_by_instance(tenant_id, task.instance_id)
        .await
    {
        Ok(Some(execution)) => {
            let digest = crate::dataflow::hex_sha256(details.to_string().as_bytes());
            if let Err(error) = append_provenance_digest(
                storage,
                signer,
                &execution,
                REMOTE_STEP_OUTPUT_KIND,
                &format!(
                    "step {} output from {} runtime {runtime_id}",
                    task.block_id,
                    kind.as_str()
                ),
                &digest,
            )
            .await
            {
                tracing::warn!(task_id = %task.id, %error, "failed to append output provenance");
            }
        }
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(task_id = %task.id, %error, "provenance lookup failed");
        }
    }
}
