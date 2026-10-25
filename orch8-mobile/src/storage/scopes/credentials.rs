//! Explicit catalog grants reference trusted account-owned credential records.
//! Secret creation/refresh remains with the configured storage backend, including
//! its encryption decorator. This table never writes or copies secret material.
use super::{CatalogScope, fence_catalog, invalid, validate, workflows::owner};
use crate::storage::MobileStorage;
use orch8_types::error::StorageError;

impl MobileStorage {
    pub(crate) async fn grant_catalog_credential(
        &self,
        scope: &CatalogScope,
        name: &str,
        id: &str,
    ) -> Result<(), StorageError> {
        validate(name)?;
        validate(id)?;
        if name.len() > 128 || name.contains('/') {
            return Err(invalid("invalid catalog credential alias"));
        }
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let (tenant, _) = owner(scope)?;
        let owned: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM credentials WHERE id=?1 AND tenant_id=?2)",
        )
        .bind(id)
        .bind(tenant.as_str())
        .fetch_one(&mut *tx)
        .await?;
        if !owned {
            return Err(invalid("catalog credential is not owned by this account"));
        }
        let exists:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mobile_catalog_credential_grants WHERE account_id=?1 AND catalog_id=?2 AND name=?3)").bind(&scope.account.id).bind(&scope.id).bind(name).fetch_one(&mut *tx).await?;
        if !exists {
            let (global,account,catalog):(i64,i64,i64)=sqlx::query_as("SELECT COUNT(*), COALESCE(SUM(account_id=?1),0), COALESCE(SUM(account_id=?1 AND catalog_id=?2),0) FROM mobile_catalog_credential_grants").bind(&scope.account.id).bind(&scope.id).fetch_one(&mut *tx).await?;
            if global >= 4096 || account >= 256 || catalog >= 64 {
                return Err(StorageError::QuotaExceeded(
                    "catalog credential grant allowance exhausted".into(),
                ));
            }
        }
        sqlx::query("INSERT INTO mobile_catalog_credential_grants(account_id,catalog_id,name,owner_epoch,credential_id) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(account_id,catalog_id,name) DO UPDATE SET owner_epoch=excluded.owner_epoch,credential_id=excluded.credential_id")
            .bind(&scope.account.id).bind(&scope.id).bind(name).bind(scope.account.epoch).bind(id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    pub(crate) async fn revoke_catalog_credential(
        &self,
        scope: &CatalogScope,
        name: &str,
    ) -> Result<bool, StorageError> {
        validate(name)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let changed=sqlx::query("DELETE FROM mobile_catalog_credential_grants WHERE account_id=?1 AND catalog_id=?2 AND name=?3")
            .bind(&scope.account.id).bind(&scope.id).bind(name).execute(&mut *tx).await?.rows_affected()!=0;
        tx.commit().await?;
        Ok(changed)
    }
}
