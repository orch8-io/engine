//! One-time decision validation runs inside the completion transaction.
use crate::{
    worker_claim::{WorkerClaimPrincipal, WorkerIssuedClaim},
    worker_outcome::WorkerAcceptedOutcome,
    worker_reconciliation::{WorkerReconciliationApproval, WorkerReconciliationDecision},
};
use orch8_types::{api_key::ApiCapability, error::StorageError};
use sqlx::Row;

pub(super) async fn decision_on(
    conn: &mut sqlx::SqliteConnection,
    approval: &WorkerReconciliationApproval,
    outcome: &WorkerAcceptedOutcome,
) -> Result<WorkerReconciliationDecision, StorageError> {
    approval.validate_outcome(outcome)?;
    // A stored key can be revoked or lose Operator after HTTP auth. Hold its
    // row lock through acceptance so revocation and this decision serialize.
    if let WorkerClaimPrincipal::ApiKey { tenant_id, key_id } = &approval.actor {
        let row=sqlx::query("SELECT capabilities_json,revoked,expires_at FROM api_keys WHERE id=?1 AND tenant_id=?2").bind(key_id).bind(tenant_id.as_str()).fetch_optional(&mut *conn).await?
            .ok_or_else(||StorageError::Conflict("reconciliation approver no longer exists".into()))?;
        let grants: Vec<ApiCapability> =
            serde_json::from_str(&row.get::<String, _>("capabilities_json"))?;
        let expires = row
            .get::<Option<String>, _>("expires_at")
            .as_deref()
            .map(super::helpers::parse_ts)
            .transpose()?;
        // Read time after acquiring the lock: a key may expire while SELECT
        // waits, without a row update to force PostgreSQL predicate rechecks.
        let now = chrono::Utc::now();
        if row.get::<bool, _>("revoked")
            || !grants.contains(&ApiCapability::Operator)
            || expires.is_some_and(|expiry| expiry <= now)
        {
            return Err(StorageError::Conflict(
                "reconciliation approver is no longer an active operator".into(),
            ));
        }
    }
    let row=sqlx::query("SELECT tenant_id,instance_id AS instance_scope,worker_id,record FROM worker_issued_claims WHERE task_id=?1 AND claim_epoch=?2").bind(outcome.task_id.to_string())
        .bind(i64::try_from(outcome.claim.claim_epoch).map_err(|_|StorageError::Constraint("reconciliation epoch exceeds SQL range".into()))?)
        .fetch_optional(&mut *conn).await?;
    let issued = if let Some(row) = row {
        let issued: WorkerIssuedClaim = serde_json::from_str(&row.get::<String, _>("record"))?;
        issued.validate()?;
        if issued.tenant_id.as_str() != row.get::<String, _>("tenant_id")
            || issued.instance_id.to_string() != row.get::<String, _>("instance_scope")
            || issued.claim.worker_id != row.get::<String, _>("worker_id")
        {
            return Err(StorageError::Constraint(
                "reconciliation issued-claim index mismatch".into(),
            ));
        }
        Some(issued)
    } else {
        None
    };
    WorkerReconciliationDecision::prepare(approval, outcome, issued.as_ref())
}
