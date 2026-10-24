//! Persistent ownership primitives, not an authentication or public SDK API.
//!
//! Descriptors must come from the host's trusted account context. Matching a
//! descriptor here does not verify a bearer token or a remote manifest audience.
use super::MobileStorage;
use super::manifest::{ACCEPTED_DIGEST, ACCEPTED_VERSION, ManifestTrust};
use orch8_types::error::StorageError;
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

mod signed;
mod workflows;

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

async fn fence_catalog(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
) -> Result<(), StorageError> {
    fence(tx, &scope.account).await?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM mobile_catalogs WHERE account_id = ?1 AND id = ?2)",
    )
    .bind(&scope.account.id)
    .bind(&scope.id)
    .fetch_one(&mut **tx)
    .await?;
    if !exists {
        return Err(invalid("mobile catalog scope is retired or foreign"));
    }
    Ok(())
}

async fn catalog_trust_in(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
) -> Result<ManifestTrust, StorageError> {
    let rows = sqlx::query_as("SELECT key, value FROM mobile_catalog_metadata WHERE account_id = ?1 AND catalog_id = ?2 AND key IN ('accepted_manifest_version', 'accepted_manifest_digest', 'last_manifest_version')")
        .bind(&scope.account.id).bind(&scope.id).fetch_all(&mut **tx).await?;
    ManifestTrust::from_rows(rows)
}

async fn write_metadata_in(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
    values: &[(&str, Option<&str>)],
) -> Result<(), StorageError> {
    for (key, value) in values {
        if let Some(value) = value {
            sqlx::query("INSERT INTO mobile_catalog_metadata (account_id, catalog_id, key, value) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(account_id, catalog_id, key) DO UPDATE SET value = excluded.value")
                .bind(&scope.account.id).bind(&scope.id).bind(key).bind(value).execute(&mut **tx).await?;
        } else {
            sqlx::query("DELETE FROM mobile_catalog_metadata WHERE account_id = ?1 AND catalog_id = ?2 AND key = ?3")
                .bind(&scope.account.id).bind(&scope.id).bind(key).execute(&mut **tx).await?;
        }
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
            if [
                ACCEPTED_VERSION,
                ACCEPTED_DIGEST,
                "last_manifest_version",
                "manifest_etag",
                "last_sync_ts",
                "sync_env_fingerprint",
            ]
            .contains(key)
            {
                return Err(invalid(
                    "manifest metadata requires an accepted-document operation",
                ));
            }
        }
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        write_metadata_in(&mut tx, scope, values).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Return keys only for this live catalog session. Empty key sets still
    /// check ownership; retired/foreign handles never masquerade as empty sets.
    pub(crate) async fn catalog_signing_keys(
        &self,
        scope: &CatalogScope,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let rows: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT k.key_id, k.public_key FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id LEFT JOIN mobile_catalog_signing_keys k ON k.account_id = a.id AND k.catalog_id = c.id WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3 ORDER BY k.key_id",
        ).bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id)
            .fetch_all(self.pool()).await?;
        if rows.is_empty() {
            return Err(invalid("mobile catalog scope is retired or foreign"));
        }
        Ok(rows
            .into_iter()
            .filter_map(|(id, key)| id.zip(key))
            .collect())
    }

    /// Seed key storage independently when testing its transaction/owner rules.
    /// Production acceptance always binds keys to a manifest version/digest.
    #[cfg(test)]
    async fn replace_catalog_signing_keys(
        &self,
        scope: &CatalogScope,
        keys: &[(&str, &str)],
    ) -> Result<(), StorageError> {
        self.replace_catalog_keys_in(scope, keys, None).await
    }

    /// Accept a verified document and its complete key set in one owner/epoch
    /// fenced transaction. Verification and audience binding are caller duties.
    async fn accept_catalog_manifest(
        &self,
        scope: &CatalogScope,
        keys: &[(&str, &str)],
        version: i64,
        digest: &str,
    ) -> Result<(), StorageError> {
        self.replace_catalog_keys_in(scope, keys, Some((version, digest)))
            .await
    }

    async fn replace_catalog_keys_in(
        &self,
        scope: &CatalogScope,
        keys: &[(&str, &str)],
        acceptance: Option<(i64, &str)>,
    ) -> Result<(), StorageError> {
        let mut seen = std::collections::HashSet::new();
        for (id, key) in keys {
            validate(id)?;
            validate(key)?;
            if !seen.insert(*id) {
                return Err(invalid("duplicate mobile catalog signing key ID"));
            }
        }
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        if let Some((version, digest)) = acceptance {
            catalog_trust_in(&mut tx, scope)
                .await?
                .check_candidate(version, digest)?;
        }
        sqlx::query(
            "DELETE FROM mobile_catalog_signing_keys WHERE account_id = ?1 AND catalog_id = ?2",
        )
        .bind(&scope.account.id)
        .bind(&scope.id)
        .execute(&mut *tx)
        .await?;
        for (id, key) in keys {
            sqlx::query("INSERT INTO mobile_catalog_signing_keys (account_id, catalog_id, key_id, public_key) VALUES (?1, ?2, ?3, ?4)")
                .bind(&scope.account.id).bind(&scope.id).bind(id).bind(key).execute(&mut *tx).await?;
        }
        if let Some((version, digest)) = acceptance {
            write_metadata_in(
                &mut tx,
                scope,
                &[
                    (ACCEPTED_VERSION, Some(&version.to_string())),
                    (ACCEPTED_DIGEST, Some(digest)),
                ],
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn catalog_manifest_trust(
        &self,
        scope: &CatalogScope,
    ) -> Result<ManifestTrust, StorageError> {
        let rows: Vec<(Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT m.key, m.value FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id LEFT JOIN mobile_catalog_metadata m ON m.account_id = a.id AND m.catalog_id = c.id AND m.key IN ('accepted_manifest_version', 'accepted_manifest_digest', 'last_manifest_version') WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3",
        ).bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id).fetch_all(self.pool()).await?;
        if rows.is_empty() {
            return Err(invalid("mobile catalog scope is retired or foreign"));
        }
        ManifestTrust::from_rows(
            rows.into_iter()
                .filter_map(|(key, value)| key.zip(value))
                .collect(),
        )
    }

    /// Finish only the currently accepted document. A late older completion
    /// cannot overwrite a newer catalog's replay fence or evaluation snapshot.
    pub(crate) async fn commit_catalog_evaluation(
        &self,
        scope: &CatalogScope,
        version: i64,
        digest: &str,
        etag: Option<&str>,
        fingerprint: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        let trust = catalog_trust_in(&mut tx, scope).await?;
        trust.require_current(version, digest)?;
        write_metadata_in(
            &mut tx,
            scope,
            &[
                ("last_manifest_version", Some(&version.to_string())),
                ("manifest_etag", etag),
                ("sync_env_fingerprint", Some(fingerprint)),
                ("last_sync_ts", Some(&chrono::Utc::now().to_rfc3339())),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
