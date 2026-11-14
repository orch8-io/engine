//! Explicit durable host binding. This is neither remote authentication nor native admission.
use super::{CatalogScope, fence_catalog, invalid};
use crate::storage::MobileStorage;
use orch8_types::{
    continuity::RuntimeId, delegation_definition::claim::DelegationClaimContext,
    error::StorageError, ids::TenantId,
};
use serde::{Deserialize, Serialize};

pub(crate) struct WorkerPeerDescriptor<'a> {
    pub(crate) peer_key: &'a str,
    pub(crate) api_base: &'a str,
    pub(crate) remote_tenant: &'a str,
    pub(crate) runtime_id: RuntimeId,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    version: u32,
    api_base: String,
    remote_tenant: TenantId,
    runtime_id: RuntimeId,
}
#[derive(Clone)]
pub(crate) struct CatalogWorkerPeer {
    scope: CatalogScope,
    key: String,
    record: Record,
}
impl std::fmt::Debug for CatalogWorkerPeer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CatalogWorkerPeer").finish_non_exhaustive()
    }
}
impl CatalogWorkerPeer {
    pub(crate) fn same_binding(&self, other: &Self) -> bool {
        self.scope == other.scope && self.key == other.key && self.record == other.record
    }
    pub(crate) fn scope(&self) -> &CatalogScope {
        &self.scope
    }
    pub(crate) fn api_base(&self) -> &str {
        &self.record.api_base
    }
    pub(crate) fn runtime_id(&self) -> RuntimeId {
        self.record.runtime_id
    }
    pub(crate) fn remote_tenant(&self) -> &TenantId {
        &self.record.remote_tenant
    }
    /// Only identity matching. The caller must separately check the current
    /// local account/catalog, original reporter, remote lease and native capacity.
    pub(crate) fn validate_claim_identity(
        &self,
        claim: &DelegationClaimContext,
    ) -> Result<(), StorageError> {
        claim.validate()?;
        if claim.scope.tenant_id != self.record.remote_tenant
            || claim.scope.worker_id != self.record.runtime_id.to_string()
        {
            return Err(invalid("claim does not belong to the bound worker peer"));
        }
        Ok(())
    }
}
fn normalized(descriptor: &WorkerPeerDescriptor<'_>) -> Result<Record, StorageError> {
    super::validate(descriptor.peer_key)?;
    if descriptor.peer_key.len() > 256 || descriptor.runtime_id.into_uuid().is_nil() {
        return Err(invalid("invalid worker peer descriptor"));
    }
    super::transport::endpoint(descriptor.api_base, false)?;
    let record = Record {
        version: 1,
        api_base: format!("{}/", descriptor.api_base.trim_end_matches('/')),
        remote_tenant: TenantId::new(descriptor.remote_tenant)
            .map_err(|_| invalid("invalid worker peer tenant"))?,
        runtime_id: descriptor.runtime_id,
    };
    if serde_json::to_vec(&record)?.len() > 8192 {
        return Err(invalid("worker peer descriptor exceeds bounds"));
    }
    Ok(record)
}
type Stored = (String, String, String, Option<String>);
async fn read(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    key: &str,
) -> Result<Option<Record>, StorageError> {
    let row:Option<Stored>=sqlx::query_as("SELECT api_base,remote_tenant,runtime_id,CASE WHEN length(CAST(record AS BLOB))<=8192 THEN record END FROM mobile_scoped_worker_peers WHERE account_id=?1 AND catalog_id=?2 AND peer_key=?3")
        .bind(scope.account_id()).bind(scope.catalog_id()).bind(key).fetch_optional(&mut **tx).await?;
    let Some((api, tenant, runtime, record)) = row else {
        return Ok(None);
    };
    let record: Record =
        serde_json::from_str(&record.ok_or_else(|| invalid("invalid retained worker peer"))?)
            .map_err(|_| invalid("invalid retained worker peer"))?;
    let expected = normalized(&WorkerPeerDescriptor {
        peer_key: key,
        api_base: &api,
        remote_tenant: &tenant,
        runtime_id: runtime
            .parse::<uuid::Uuid>()
            .map(RuntimeId::from_uuid)
            .map_err(|_| invalid("invalid retained worker peer"))?,
    })?;
    if record != expected {
        return Err(invalid("retained worker peer identity changed"));
    }
    Ok(Some(record))
}
async fn capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
) -> Result<(), StorageError> {
    let (global,account,catalog):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(account_id=?1),0),COALESCE(SUM(account_id=?1 AND catalog_id=?2),0) FROM mobile_scoped_worker_peers")
        .bind(scope.account_id()).bind(scope.catalog_id()).fetch_one(&mut **tx).await?;
    if global >= 128 || account >= 64 || catalog >= 16 {
        return Err(StorageError::QuotaExceeded(
            "scoped worker peer capacity is occupied".into(),
        ));
    }
    Ok(())
}
impl MobileStorage {
    /// The host supplies all fields, including a distinct backend worker identity.
    /// A task or payload response can never choose its local account/catalog.
    pub(crate) async fn bind_worker_peer(
        &self,
        scope: &CatalogScope,
        descriptor: WorkerPeerDescriptor<'_>,
    ) -> Result<CatalogWorkerPeer, StorageError> {
        let record = normalized(&descriptor)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        if let Some(prior) = read(&mut tx, scope, descriptor.peer_key).await? {
            if prior != record {
                return Err(invalid("worker peer binding cannot change"));
            }
        } else {
            capacity(&mut tx, scope).await?;
            let insert=sqlx::query("INSERT INTO mobile_scoped_worker_peers(account_id,catalog_id,peer_key,api_base,remote_tenant,runtime_id,record) VALUES(?1,?2,?3,?4,?5,?6,?7)")
                .bind(scope.account_id()).bind(scope.catalog_id()).bind(descriptor.peer_key).bind(&record.api_base).bind(record.remote_tenant.as_str()).bind(record.runtime_id.to_string()).bind(serde_json::to_string(&record)?).execute(&mut *tx).await;
            match insert {
                Ok(result) if result.rows_affected() == 1 => {}
                Ok(_) => return Err(invalid("worker peer binding was not retained")),
                Err(error)
                    if error
                        .as_database_error()
                        .is_some_and(sqlx::error::DatabaseError::is_unique_violation) =>
                {
                    return Err(invalid(
                        "worker identity already belongs to another local scope",
                    ));
                }
                Err(error) => return Err(error.into()),
            }
            if read(&mut tx, scope, descriptor.peer_key).await?.as_ref() != Some(&record) {
                return Err(invalid("worker peer binding changed during insertion"));
            }
        }
        tx.commit().await?;
        Ok(CatalogWorkerPeer {
            scope: scope.clone(),
            key: descriptor.peer_key.into(),
            record,
        })
    }
    pub(crate) async fn check_worker_peer(
        &self,
        peer: &CatalogWorkerPeer,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        check_in(&mut tx, peer).await?;
        tx.commit().await?;
        Ok(())
    }
}
#[cfg(test)]
mod tests;

mod devices;

pub(crate) async fn check_in(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    peer: &CatalogWorkerPeer,
) -> Result<(), StorageError> {
    fence_catalog(tx, &peer.scope).await?;
    if read(tx, &peer.scope, &peer.key).await?.as_ref() != Some(&peer.record) {
        return Err(invalid("worker peer is missing or changed"));
    }
    Ok(())
}
