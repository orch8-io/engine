//! Transaction-local generation capture and bounded immutable reads.
use crate::{delegation_claim::invalid, worker_claim::WorkerIssuedClaim};
use orch8_types::{
    delegation_definition::{
        claim::DelegationClaimContext,
        input::DelegationInputReference,
        protocol::DelegationResourceUsage,
        wire::{DefinitionBundleReference, DefinitionReadScope},
    },
    error::StorageError,
    ids::TenantId,
    worker::WorkerClaim,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn capture_on(
    conn: &mut sqlx::SqliteConnection,
    claim: &WorkerIssuedClaim,
) -> Result<DelegationClaimContext, StorageError> {
    let row=sqlx::query("SELECT CASE WHEN length(CAST(p.record AS BLOB))<=65536 THEN p.record END AS publication,m.bundle_sha256,m.wire_version,m.wire_sha256,m.wire_bytes,m.wire_definitions,m.wire_selectors,i.definition_expanded_bytes,i.input_wire_version,i.input_sha256,i.input_bytes,i.input_expanded_bytes FROM delegation_publications p JOIN delegation_definition_manifests m ON m.tenant_id=p.tenant_id AND m.task_id=p.task_id JOIN delegation_publication_inputs i ON i.tenant_id=p.tenant_id AND i.task_id=p.task_id WHERE p.tenant_id=?1 AND p.task_id=?2 AND i.version=1 AND i.request_sha256=json_extract(p.record,'$.request_sha256')")
        .bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).fetch_optional(&mut *conn).await?.ok_or_else(invalid)?;
    let publication: crate::delegation_publication::DelegationPublication = serde_json::from_str(
        &row.get::<Option<String>, _>("publication")
            .ok_or_else(invalid)?,
    )
    .map_err(|_| invalid())?;
    let number = |column: &str| u32::try_from(row.get::<i64, _>(column)).map_err(|_| invalid());
    let definition = DefinitionBundleReference {
        version: number("wire_version")?,
        sequence_sha256: publication.sequence_sha256.clone(),
        bundle_sha256: row.get("bundle_sha256"),
        payload_sha256: row.get("wire_sha256"),
        payload_bytes: number("wire_bytes")?,
        definition_count: number("wire_definitions")?,
        selector_count: number("wire_selectors")?,
    };
    let input = DelegationInputReference {
        version: number("input_wire_version")?,
        payload_sha256: row.get("input_sha256"),
        payload_bytes: number("input_bytes")?,
        expanded_bytes: number("input_expanded_bytes")?,
    };
    let usage = DelegationResourceUsage {
        definition_expanded_bytes: number("definition_expanded_bytes")?,
        input_wire_version: input.version,
        input_payload_bytes: input.payload_bytes,
        input_expanded_bytes: input.expanded_bytes,
    };
    let context = crate::delegation_claim::capture(claim, publication, definition, input, usage)?;
    let record = serde_json::to_string(&context)?;
    if record.len() > 16_384 {
        return Err(invalid());
    }
    let epoch = i64::try_from(context.scope.claim_epoch).map_err(|_| invalid())?;
    let inserted=sqlx::query("INSERT INTO worker_issued_delegation_contexts(task_id,claim_epoch,tenant_id,worker_id,record) VALUES(?1,?2,?3,?4,?5)")
        .bind(claim.task_id.to_string()).bind(epoch).bind(claim.tenant_id.as_str()).bind(&claim.claim.worker_id).bind(record).execute(&mut *conn).await?;
    if inserted.rows_affected() != 1 {
        return Err(invalid());
    }
    Ok(context)
}
pub(super) async fn get(
    store: &super::SqliteStorage,
    tenant: &TenantId,
    task_id: Uuid,
    claim: &WorkerClaim,
) -> Result<Option<DelegationClaimContext>, StorageError> {
    let Ok(epoch) = i64::try_from(claim.claim_epoch) else {
        return Ok(None);
    };
    let row=sqlx::query("SELECT CASE WHEN length(CAST(d.record AS BLOB))<=16384 THEN d.record END AS record FROM worker_issued_delegation_contexts d JOIN worker_issued_claims c ON c.task_id=d.task_id AND c.claim_epoch=d.claim_epoch AND c.tenant_id=d.tenant_id AND c.worker_id=d.worker_id WHERE d.tenant_id=?1 AND d.task_id=?2 AND d.claim_epoch=?3 AND d.worker_id=?4")
        .bind(tenant.as_str()).bind(task_id.to_string()).bind(epoch).bind(&claim.worker_id).fetch_optional(&store.pool).await?;
    let Some(row) = row else { return Ok(None) };
    let context: DelegationClaimContext =
        serde_json::from_str(&row.get::<Option<String>, _>("record").ok_or_else(invalid)?)
            .map_err(|_| invalid())?;
    context.validate_scope(&DefinitionReadScope {
        tenant_id: tenant.clone(),
        task_id,
        worker_id: claim.worker_id.clone(),
        claim_epoch: claim.claim_epoch,
    })?;
    Ok(Some(context))
}
