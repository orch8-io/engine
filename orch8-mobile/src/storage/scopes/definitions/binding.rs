//! Durable logical-run identity; never scheduler or provider admission.
use super::{
    CatalogScope, DefinitionBundleReference, DefinitionGraphLimits, DefinitionReadScope,
    InstalledClaimDefinitions, MobileStorage, StorageError, fence_catalog, invalid, read_existing,
};
use crate::storage::scopes::AccountScope;
use orch8_types::ids::InstanceId;

mod inputs;
mod materialize;
mod observation;

/// The latest observed claim, not proof of a live lease or permission to execute.
/// A native materializer must retain this run ID and independently fence every
/// forward attempt against current remote authority and physical host capacity.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct BoundClaimRun {
    pub(crate) local_run_id: InstanceId,
    pub(crate) definitions: InstalledClaimDefinitions,
    pub(crate) worker_id: String,
    pub(crate) claim_epoch: u64,
}

impl MobileStorage {
    /// Inspect retained, authenticated source mappings without downloading the
    /// wire envelope. Expected identity must still come from trusted context.
    #[allow(dead_code)] // Staged until claim negotiation and admission are wired.
    pub(crate) async fn load_claim_definitions(
        &self,
        scope: &CatalogScope,
        server: &str,
        claim: &DefinitionReadScope,
        reference: &DefinitionBundleReference,
    ) -> Result<Option<InstalledClaimDefinitions>, StorageError> {
        validate_request(server, claim, reference)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let found = read_existing(&mut tx, scope, server, claim, reference, None).await?;
        tx.commit().await?;
        Ok(found)
    }

    /// Bind one remote task to one local logical run across claim generations
    /// and sign-in epochs. `request_sha256` must be the publication request
    /// fingerprint from trusted provenance, covering dynamic input separately
    /// from the definition reference. This method does not validate raw task
    /// input against that fingerprint. No native instance or work is created.
    /// An existing run can reuse its verified graph without installing another
    /// claim-specific copy. Claim/reference/request identity must still come
    /// independently from trusted provenance, never inferred from the cache.
    #[allow(dead_code)] // Staged until claim negotiation and admission are wired.
    pub(crate) async fn bind_claim_run(
        &self,
        scope: &CatalogScope,
        server: &str,
        claim: &DefinitionReadScope,
        reference: &DefinitionBundleReference,
        request_sha256: &str,
    ) -> Result<BoundClaimRun, StorageError> {
        self.try_bind_claim_run(scope, server, claim, reference, request_sha256)
            .await?
            .ok_or_else(|| invalid("claim definitions have not been installed"))
    }

    /// `None` means this logical run has no installed definitions yet. Invalid
    /// retained state is an error, so callers must not silently replace it by
    /// downloading another envelope. All expected identity is trusted context.
    #[allow(dead_code)] // Staged until claim negotiation and admission are wired.
    pub(crate) async fn try_bind_claim_run(
        &self,
        scope: &CatalogScope,
        server: &str,
        claim: &DefinitionReadScope,
        reference: &DefinitionBundleReference,
        request_sha256: &str,
    ) -> Result<Option<BoundClaimRun>, StorageError> {
        validate_request(server, claim, reference)?;
        if request_sha256.len() != 64
            || !request_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid publication request fingerprint"));
        }
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let current = read_existing(&mut tx, scope, server, claim, reference, None).await?;
        let found: Option<(String,String,String,String,String,i64,String)> = sqlx::query_as("SELECT local_run_id,initial_receipt_id,current_receipt_id,worker_id,claim_epoch,owner_epoch,request_sha256 FROM mobile_remote_run_bindings WHERE account_id=?1 AND catalog_id=?2 AND server=?3 AND remote_tenant=?4 AND task_id=?5")
            .bind(scope.account_id()).bind(scope.catalog_id()).bind(server).bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).fetch_optional(&mut *tx).await?;
        let (id, definitions, receipt_id) = if let Some((
            id,
            initial,
            latest,
            worker,
            epoch,
            owner,
            request,
        )) = found
        {
            if request != request_sha256 {
                return Err(invalid("logical run publication request changed"));
            }
            let epoch = epoch_value(&epoch)?;
            if claim.claim_epoch < epoch
                || (claim.claim_epoch == epoch && claim.worker_id != worker)
            {
                return Err(invalid(
                    "claim generation is stale or has a different owner",
                ));
            }
            let latest =
                retained_receipt(&mut tx, scope, server, claim, reference, &latest).await?;
            let prior = observation::Observation::new(
                owner,
                DefinitionReadScope {
                    worker_id: worker,
                    claim_epoch: epoch,
                    ..claim.clone()
                },
                reference.clone(),
                request_sha256.to_owned(),
                latest.graph.receipt_id.clone(),
            )?;
            observation::validate_prior(&mut tx, &id, &prior, &latest.claim, latest.owner_epoch)
                .await?;
            // Keep the original physical graph even when the new generation
            // installed an equivalent graph under fresh private local IDs.
            let initial =
                retained_receipt(&mut tx, scope, server, claim, reference, &initial).await?;
            let receipt_id = current.as_ref().unwrap_or(&latest.graph).receipt_id.clone();
            sqlx::query("UPDATE mobile_remote_run_bindings SET current_receipt_id=?2,owner_epoch=?3,worker_id=?4,claim_epoch=?5 WHERE local_run_id=?1")
                .bind(&id).bind(&receipt_id).bind(scope.epoch()).bind(&claim.worker_id).bind(claim.claim_epoch.to_string()).execute(&mut *tx).await?;
            (id, initial.graph, receipt_id)
        } else {
            let Some(current) = current else {
                tx.commit().await?;
                return Ok(None);
            };
            let id = InstanceId::new().to_string();
            sqlx::query("INSERT INTO mobile_remote_run_bindings(local_run_id,account_id,catalog_id,server,remote_tenant,task_id,owner_epoch,worker_id,claim_epoch,initial_receipt_id,current_receipt_id,request_sha256) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,?11)")
                .bind(&id).bind(scope.account_id()).bind(scope.catalog_id()).bind(server).bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).bind(scope.epoch()).bind(&claim.worker_id).bind(claim.claim_epoch.to_string()).bind(&current.receipt_id).bind(request_sha256).execute(&mut *tx).await?;
            let receipt_id = current.receipt_id.clone();
            (id, current, receipt_id)
        };
        let local_id = id
            .parse::<uuid::Uuid>()
            .map_err(|_| invalid("invalid logical run ID"))?;
        if local_id.is_nil() {
            return Err(invalid("invalid logical run ID"));
        }
        let observed = observation::Observation::new(
            scope.epoch(),
            claim.clone(),
            reference.clone(),
            request_sha256.to_owned(),
            receipt_id,
        )?;
        observation::write(&mut tx, &id, &observed).await?;
        tx.commit().await?;
        Ok(Some(BoundClaimRun {
            local_run_id: InstanceId::from_uuid(local_id),
            definitions,
            worker_id: claim.worker_id.clone(),
            claim_epoch: claim.claim_epoch,
        }))
    }
}

fn validate_request(
    server: &str,
    claim: &DefinitionReadScope,
    reference: &DefinitionBundleReference,
) -> Result<(), StorageError> {
    super::super::transport::endpoint(server, false)?;
    super::super::validate(claim.tenant_id.as_str())?;
    super::super::validate(&claim.worker_id)?;
    claim.validate()?;
    reference.validate(DefinitionGraphLimits::default())
}
fn epoch_value(text: &str) -> Result<u64, StorageError> {
    let epoch = text
        .parse::<u64>()
        .map_err(|_| invalid("invalid retained claim epoch"))?;
    if epoch == 0 || text != epoch.to_string() {
        return Err(invalid("invalid retained claim epoch"));
    }
    Ok(epoch)
}
struct RetainedReceipt {
    graph: InstalledClaimDefinitions,
    claim: DefinitionReadScope,
    owner_epoch: i64,
}
async fn retained_receipt(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    server: &str,
    claim: &DefinitionReadScope,
    reference: &DefinitionBundleReference,
    receipt_id: &str,
) -> Result<RetainedReceipt, StorageError> {
    let metadata:Option<(i64,String,String)>=sqlx::query_as("SELECT owner_epoch,worker_id,claim_epoch FROM mobile_claim_definition_receipts WHERE id=?1 AND account_id=?2 AND catalog_id=?3 AND server=?4 AND remote_tenant=?5 AND task_id=?6")
        .bind(receipt_id).bind(scope.account_id()).bind(scope.catalog_id()).bind(server).bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).fetch_optional(&mut **tx).await?;
    let (owner_epoch, worker, epoch) = metadata
        .ok_or_else(|| invalid("logical run definition receipt is unavailable or foreign"))?;
    if owner_epoch <= 0 {
        return Err(invalid("invalid retained owner epoch"));
    }
    // The enclosing transaction already fenced the current account. Read old
    // retained evidence without treating an old epoch as a live session.
    let retained_scope = CatalogScope {
        account: AccountScope {
            id: scope.account_id().to_owned(),
            epoch: owner_epoch,
        },
        id: scope.catalog_id().to_owned(),
    };
    let retained_claim = DefinitionReadScope {
        worker_id: worker,
        claim_epoch: epoch_value(&epoch)?,
        ..claim.clone()
    };
    retained_claim.validate()?;
    super::super::validate(&retained_claim.worker_id)?;
    let graph = read_existing(
        tx,
        &retained_scope,
        server,
        &retained_claim,
        reference,
        None,
    )
    .await?
    .ok_or_else(|| invalid("logical run definition receipt is unavailable"))?;
    if graph.receipt_id != receipt_id {
        return Err(invalid("logical run definition receipt changed"));
    }
    Ok(RetainedReceipt {
        graph,
        claim: retained_claim,
        owner_epoch,
    })
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod observation_tests;
