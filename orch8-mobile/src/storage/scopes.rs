//! Persistent ownership primitives, not an authentication or public SDK API.
//!
//! Descriptors must come from the host's trusted account context. Matching a
//! descriptor here does not verify a bearer token or a remote manifest audience.
use super::MobileStorage;
use orch8_types::error::StorageError;
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

#[derive(Clone)]
pub(crate) struct AccountScope {
    id: String,
    epoch: i64,
}

#[derive(Clone)]
pub(crate) struct CatalogScope {
    account: AccountScope,
    id: String,
}

fn invalid(message: &'static str) -> StorageError {
    StorageError::Conflict(message.into())
}

fn validate(value: &str) -> Result<(), StorageError> {
    if value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(invalid("invalid mobile scope descriptor"));
    }
    Ok(())
}

/// Taking `SQLite`'s writer reservation before checking the epoch serializes
/// retirement with mutations, even across independent pools/processes. A write
/// that wins this reservation may finish before retirement; a later one fails.
async fn fence(
    tx: &mut Transaction<'_, Sqlite>,
    account: &AccountScope,
) -> Result<(), StorageError> {
    let result = sqlx::query(
        "UPDATE mobile_accounts SET epoch = epoch WHERE id = ?1 AND epoch = ?2 AND active = 1",
    )
    .bind(&account.id)
    .bind(account.epoch)
    .execute(&mut **tx)
    .await?;
    if result.rows_affected() != 1 {
        return Err(invalid("mobile account scope is retired"));
    }
    Ok(())
}

impl MobileStorage {
    /// Reuse an active account, or reopen a retired account with a new epoch.
    /// Identity changes require a separate explicit migration, not token refresh.
    pub(crate) async fn open_account_scope(
        &self,
        account_key: &str,
        issuer: &str,
        subject: &str,
    ) -> Result<AccountScope, StorageError> {
        for value in [account_key, issuer, subject] {
            validate(value)?;
        }
        let mut tx = self.pool().begin().await?;
        sqlx::query("INSERT INTO mobile_accounts (id, account_key, principal_issuer, principal_subject, epoch, active) VALUES (?1, ?2, ?3, ?4, 1, 1) ON CONFLICT(account_key) DO NOTHING")
            .bind(Uuid::new_v4().to_string()).bind(account_key).bind(issuer).bind(subject)
            .execute(&mut *tx).await?;
        let (id, stored_issuer, stored_subject, epoch, active): (String, String, String, i64, bool) =
            sqlx::query_as("SELECT id, principal_issuer, principal_subject, epoch, active FROM mobile_accounts WHERE account_key = ?1")
                .bind(account_key).fetch_one(&mut *tx).await?;
        if stored_issuer != issuer || stored_subject != subject {
            return Err(invalid("mobile account principal binding mismatch"));
        }
        let epoch = if active {
            epoch
        } else {
            let next = epoch
                .checked_add(1)
                .ok_or_else(|| invalid("mobile account epoch exhausted"))?;
            sqlx::query("UPDATE mobile_accounts SET epoch = ?2, active = 1 WHERE id = ?1")
                .bind(&id)
                .bind(next)
                .execute(&mut *tx)
                .await?;
            next
        };
        tx.commit().await?;
        Ok(AccountScope { id, epoch })
    }

    /// Retire this exact session. Repeated/late retirement cannot close a newer
    /// session. No stored workflow/effect evidence is deleted here.
    pub(crate) async fn retire_account_scope(
        &self,
        account: &AccountScope,
    ) -> Result<(), StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_accounts SET active = 0 WHERE id = ?1 AND epoch = ?2 AND active = 1",
        )
        .bind(&account.id)
        .bind(account.epoch)
        .execute(self.pool())
        .await?;
        if result.rows_affected() != 1 {
            return Err(invalid("mobile account scope is retired"));
        }
        Ok(())
    }

    pub(crate) async fn open_catalog_scope(
        &self,
        account: &AccountScope,
        catalog_key: &str,
        source: &str,
        signing_authority: &str,
    ) -> Result<CatalogScope, StorageError> {
        for value in [catalog_key, source, signing_authority] {
            validate(value)?;
        }
        let mut tx = self.pool().begin().await?;
        fence(&mut tx, account).await?;
        sqlx::query("INSERT INTO mobile_catalogs (id, account_id, catalog_key, source, signing_authority) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(account_id, catalog_key) DO NOTHING")
            .bind(Uuid::new_v4().to_string()).bind(&account.id).bind(catalog_key).bind(source).bind(signing_authority)
            .execute(&mut *tx).await?;
        let (id, stored_source, stored_authority): (String, String, String) =
            sqlx::query_as("SELECT id, source, signing_authority FROM mobile_catalogs WHERE account_id = ?1 AND catalog_key = ?2")
                .bind(&account.id).bind(catalog_key).fetch_one(&mut *tx).await?;
        if stored_source != source || stored_authority != signing_authority {
            return Err(invalid("mobile catalog source binding mismatch"));
        }
        tx.commit().await?;
        Ok(CatalogScope {
            account: account.clone(),
            id,
        })
    }

    pub(crate) async fn catalog_metadata(
        &self,
        scope: &CatalogScope,
        key: &str,
    ) -> Result<Option<String>, StorageError> {
        validate(key)?;
        // One snapshot checks membership/epoch AND selects the value. A missing
        // value differs from a missing/retired owner; no preflight check race.
        let row: Option<(Option<String>,)> = sqlx::query_as(
            "SELECT (SELECT value FROM mobile_catalog_metadata m WHERE m.account_id = a.id AND m.catalog_id = c.id AND m.key = ?4) FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3",
        ).bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id).bind(key)
            .fetch_optional(self.pool()).await?;
        row.map(|(value,)| value)
            .ok_or_else(|| invalid("mobile catalog scope is retired or foreign"))
    }

    pub(crate) async fn update_catalog_metadata(
        &self,
        scope: &CatalogScope,
        values: &[(&str, Option<&str>)],
    ) -> Result<(), StorageError> {
        for (key, _) in values {
            validate(key)?;
        }
        let mut tx = self.pool().begin().await?;
        fence(&mut tx, &scope.account).await?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM mobile_catalogs WHERE account_id = ?1 AND id = ?2)",
        )
        .bind(&scope.account.id)
        .bind(&scope.id)
        .fetch_one(&mut *tx)
        .await?;
        if !exists {
            return Err(invalid("mobile catalog scope is retired or foreign"));
        }
        for (key, value) in values {
            if let Some(value) = value {
                sqlx::query("INSERT INTO mobile_catalog_metadata (account_id, catalog_id, key, value) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(account_id, catalog_id, key) DO UPDATE SET value = excluded.value")
                    .bind(&scope.account.id).bind(&scope.id).bind(key).bind(value)
                    .execute(&mut *tx).await?;
            } else {
                sqlx::query("DELETE FROM mobile_catalog_metadata WHERE account_id = ?1 AND catalog_id = ?2 AND key = ?3")
                    .bind(&scope.account.id).bind(&scope.id).bind(key).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
