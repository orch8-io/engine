//! Exact input retention is evidence, never scheduler or provider authority.
use super::{
    CatalogScope, MobileStorage, StorageError, fence_catalog, invalid, observation,
    retained_receipt,
};
use orch8_types::delegation_definition::{
    claim::DelegationClaimContext,
    input::{DelegationInputEnvelope, InputLimits, VerifiedDelegationInput},
    wire::DefinitionPayloadEncoding,
};

pub(super) async fn bound(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    server: &str,
    context: &DelegationClaimContext,
) -> Result<String, StorageError> {
    context.validate()?;
    super::validate_request(server, &context.scope, &context.definition)?;
    fence_catalog(tx, scope).await?;
    let row:Option<(String,String,i64,String,String,String)>=sqlx::query_as("SELECT local_run_id,current_receipt_id,owner_epoch,worker_id,claim_epoch,request_sha256 FROM mobile_remote_run_bindings WHERE account_id=?1 AND catalog_id=?2 AND server=?3 AND remote_tenant=?4 AND task_id=?5")
        .bind(scope.account_id()).bind(scope.catalog_id()).bind(server).bind(context.scope.tenant_id.as_str()).bind(context.scope.task_id.to_string()).fetch_optional(&mut **tx).await?;
    let (id, receipt, owner, worker, epoch, request) =
        row.ok_or_else(|| invalid("logical input requires a bound run"))?;
    if owner != scope.epoch()
        || worker != context.scope.worker_id
        || epoch != context.scope.claim_epoch.to_string()
        || request != context.request_sha256
    {
        return Err(invalid("logical input claim ownership changed"));
    }
    let retained = retained_receipt(
        tx,
        scope,
        server,
        &context.scope,
        &context.definition,
        &receipt,
    )
    .await?;
    let expected = observation::Observation::new(
        owner,
        context.scope.clone(),
        context.definition.clone(),
        request,
        receipt,
    )?;
    observation::validate_prior(tx, &id, &expected, &retained.claim, retained.owner_epoch).await?;
    Ok(id)
}
pub(super) async fn read(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    context: &DelegationClaimContext,
) -> Result<Option<VerifiedDelegationInput>, StorageError> {
    // CASE bounds corrupt oversized fields before copying them out of SQLite.
    let row:Option<(i64,Option<String>,Option<String>,i64)>=sqlx::query_as("SELECT version,CASE WHEN length(CAST(reference_json AS BLOB))<=1024 THEN reference_json END,CASE WHEN length(CAST(payload_base64 AS BLOB))<=5592408 THEN payload_base64 END,expanded_bytes FROM mobile_remote_run_inputs WHERE local_run_id=?1").bind(id).fetch_optional(&mut **tx).await?;
    let Some((version, reference, payload, expanded)) = row else {
        return Ok(None);
    };
    if version != 1 || expanded != i64::from(context.input.expanded_bytes) {
        return Err(invalid("retained logical input codec or charge changed"));
    }
    let reference = serde_json::from_str(
        &reference.ok_or_else(|| invalid("retained logical input reference is oversized"))?,
    )
    .map_err(|_| invalid("invalid retained logical input reference"))?;
    let envelope = DelegationInputEnvelope {
        version: 1,
        scope: context.scope.clone(),
        request_sha256: context.request_sha256.clone(),
        reference,
        payload_encoding: DefinitionPayloadEncoding::JsonUtf8Base64,
        payload_base64: payload.ok_or_else(|| invalid("retained logical input is oversized"))?,
    };
    envelope
        .verify(
            &context.scope,
            &context.request_sha256,
            &context.input,
            InputLimits::default(),
        )
        .map(Some)
}
async fn capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    charge: i64,
) -> Result<(), StorageError> {
    // Include retired accounts/catalogs; closing a scope cannot reset its charge.
    for (account, catalog, bytes, count) in [
        (None, None, 128_i64 * 1024 * 1024, 512_i64),
        (Some(scope.account_id()), None, 64 * 1024 * 1024, 256),
        (
            Some(scope.account_id()),
            Some(scope.catalog_id()),
            32 * 1024 * 1024,
            128,
        ),
    ] {
        let (used,n):(i64,i64)=sqlx::query_as("SELECT COALESCE(SUM(length(CAST(i.payload_base64 AS BLOB))+i.expanded_bytes),0),COUNT(*) FROM mobile_remote_run_inputs i JOIN mobile_remote_run_bindings b ON b.local_run_id=i.local_run_id WHERE (?1 IS NULL OR b.account_id=?1) AND (?2 IS NULL OR b.catalog_id=?2)").bind(account).bind(catalog).fetch_one(&mut **tx).await?;
        if used.checked_add(charge).is_none_or(|n| n > bytes) || n >= count {
            return Err(invalid("scoped logical input capacity exhausted"));
        }
    }
    Ok(())
}
#[allow(dead_code)] // Connected with remote native admission, not legacy unscoped workers.
impl MobileStorage {
    /// Expected context is independently authenticated provenance. Cached bytes
    /// may survive a lease; callers must separately obtain live forward authority.
    pub(crate) async fn load_claim_input(
        &self,
        scope: &CatalogScope,
        server: &str,
        context: &DelegationClaimContext,
    ) -> Result<Option<VerifiedDelegationInput>, StorageError> {
        let mut tx = self.pool().begin().await?;
        let id = bound(&mut tx, scope, server, context).await?;
        let result = read(&mut tx, &id, context).await?;
        tx.commit().await?;
        Ok(result)
    }
    /// No overwrite, no repair on corruption, and no generation-specific copy.
    pub(crate) async fn install_claim_input(
        &self,
        scope: &CatalogScope,
        server: &str,
        context: &DelegationClaimContext,
        envelope: &DelegationInputEnvelope,
    ) -> Result<VerifiedDelegationInput, StorageError> {
        context.validate()?;
        let verified = envelope.verify(
            &context.scope,
            &context.request_sha256,
            &context.input,
            InputLimits::default(),
        )?;
        let mut tx = self.pool().begin().await?;
        let id = bound(&mut tx, scope, server, context).await?;
        if let Some(prior) = read(&mut tx, &id, context).await? {
            tx.commit().await?;
            return Ok(prior);
        }
        let charge = i64::try_from(envelope.payload_base64.len())
            .map_err(|_| invalid("logical input charge overflow"))?
            .checked_add(i64::from(context.input.expanded_bytes))
            .ok_or_else(|| invalid("logical input charge overflow"))?;
        capacity(&mut tx, scope, charge).await?;
        sqlx::query("INSERT INTO mobile_remote_run_inputs(local_run_id,version,reference_json,payload_base64,expanded_bytes) VALUES(?1,1,?2,?3,?4)").bind(&id).bind(serde_json::to_string(&context.input)?).bind(&envelope.payload_base64).bind(i64::from(context.input.expanded_bytes)).execute(&mut *tx).await?;
        // Detect suppressed inserts rather than publishing an unretained value.
        read(&mut tx, &id, context)
            .await?
            .ok_or_else(|| invalid("logical input was not retained"))?;
        tx.commit().await?;
        Ok(verified)
    }
}
#[cfg(test)]
mod tests;
