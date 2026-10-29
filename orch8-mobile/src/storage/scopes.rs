//! Persistent ownership primitives, not an authentication or public SDK API.
//!
//! Descriptors must come from the host's trusted account context. Matching a
//! descriptor here does not verify a bearer token or a remote manifest audience.
use super::MobileStorage;
use super::manifest::{ACCEPTED_DIGEST, ACCEPTED_VERSION, ManifestTrust};
use orch8_types::error::StorageError;
use sqlx::{Sqlite, Transaction};
use uuid::Uuid;

mod credentials;
pub(crate) mod runs;
mod signed;
mod transport;
mod workflows;

pub(crate) use signed::AcceptedCatalogManifest;
pub(crate) use transport::{CatalogBinding, CatalogTransport};

struct CatalogAcceptance<'a> {
    version: i64,
    digest: &'a str,
    entries: &'a [(&'a str, i32, &'a str)],
    execution: Option<&'a [crate::sync::ManifestSequenceEntry]>,
}

const RECONCILED_VERSION: &str = "catalog_reconciled_version";
const RECONCILED_DIGEST: &str = "catalog_reconciled_digest";

async fn require_reconciled(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
) -> Result<(i64, String), StorageError> {
    let trust = catalog_trust_in(tx, scope).await?;
    if let Some((version, digest)) = trust.evaluated_identity() {
        return Ok((version, digest.to_owned()));
    }
    let version:Option<String>=sqlx::query_scalar("SELECT value FROM mobile_catalog_metadata WHERE account_id = ?1 AND catalog_id = ?2 AND key = 'catalog_reconciled_version'")
        .bind(&scope.account.id).bind(&scope.id).fetch_optional(&mut **tx).await?;
    let digest:Option<String>=sqlx::query_scalar("SELECT value FROM mobile_catalog_metadata WHERE account_id = ?1 AND catalog_id = ?2 AND key = 'catalog_reconciled_digest'")
        .bind(&scope.account.id).bind(&scope.id).fetch_optional(&mut **tx).await?;
    let version = version
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| invalid("catalog reconciliation is pending"))?;
    let digest = digest.ok_or_else(|| invalid("catalog reconciliation is pending"))?;
    trust.require_current(version, &digest)?;
    Ok((version, digest))
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct AccountScope {
    id: String,
    epoch: i64,
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub(crate) struct CatalogScope {
    account: AccountScope,
    id: String,
}

impl AccountScope {
    pub(crate) fn account_id(&self) -> &str {
        &self.id
    }
    pub(crate) fn epoch(&self) -> i64 {
        self.epoch
    }
}

impl CatalogScope {
    pub(crate) fn account_id(&self) -> &str {
        &self.account.id
    }
    pub(crate) fn catalog_id(&self) -> &str {
        &self.id
    }
    pub(crate) fn epoch(&self) -> i64 {
        self.account.epoch
    }
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
    /// Resolve only a proven running scoped request, in one `SQLite` snapshot.
    /// The native entry boundary revalidates the claim after routing/queueing.
    pub(crate) async fn catalog_callback_scope(
        &self,
        id: orch8_types::ids::InstanceId,
        tenant: &orch8_types::ids::TenantId,
        token: chrono::DateTime<chrono::Utc>,
    ) -> Result<CatalogScope, StorageError> {
        let owner: Option<(String,String,i64)> = sqlx::query_as(
            "SELECT r.account_id,r.catalog_id,r.owner_epoch FROM mobile_catalog_run_requests r
             JOIN task_instances i ON i.id=r.id JOIN mobile_accounts a ON a.id=r.account_id
             WHERE i.id=?1 AND i.tenant_id=?2 AND i.state='running' AND i.claimed_at=?3
             AND i.tenant_id='mobile-account-' || r.account_id AND i.namespace='catalog-' || r.catalog_id
             AND i.sequence_id=r.selected_sequence_id AND r.state='prepared'
             AND a.active=1 AND a.epoch=r.owner_epoch"
        ).bind(id.to_string()).bind(tenant.as_str()).bind(token.to_rfc3339()).fetch_optional(self.pool()).await?;
        let (account_id, id, epoch) =
            owner.ok_or_else(|| invalid("scoped native callback is not authorized"))?;
        Ok(CatalogScope {
            account: AccountScope {
                id: account_id,
                epoch,
            },
            id,
        })
    }

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
        let mut tx = self.pool().begin().await?;
        let result = sqlx::query(
            "UPDATE mobile_accounts SET active=0 WHERE id=?1 AND epoch=?2 AND active=1",
        )
        .bind(&account.id)
        .bind(account.epoch)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(invalid("mobile account scope is retired"));
        }
        // Retire logical work without deleting original requests or effect
        // evidence. Already-running host code retains its physical permit.
        sqlx::query("UPDATE task_instances SET state='cancelled',next_fire_at=NULL,claimed_at=NULL,updated_at=?3 WHERE id IN (SELECT id FROM mobile_catalog_run_requests WHERE account_id=?1 AND owner_epoch=?2) AND state IN ('scheduled','running','waiting','paused') AND EXISTS(SELECT 1 FROM mobile_catalog_run_requests r WHERE r.id=task_instances.id AND task_instances.tenant_id='mobile-account-' || r.account_id AND task_instances.namespace='catalog-' || r.catalog_id AND task_instances.sequence_id=r.selected_sequence_id)")
            .bind(&account.id).bind(account.epoch).bind(chrono::Utc::now().to_rfc3339()).execute(&mut *tx).await?;
        orch8_storage::sqlite::SqliteStorage::retire_mobile_capsule_cuts_in_transaction(
            &mut tx,
            &account.id,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Cleanup may release only an already-retired epoch, including after reopen.
    pub(crate) async fn catalog_epoch_retired(
        &self,
        scope: &CatalogScope,
    ) -> Result<bool, StorageError> {
        let retired: Option<bool> = sqlx::query_scalar(
            "SELECT (active = 0 OR epoch != ?2) FROM mobile_accounts WHERE id = ?1",
        )
        .bind(scope.account_id())
        .bind(scope.epoch())
        .fetch_optional(self.pool())
        .await?;
        retired.ok_or_else(|| invalid("mobile account scope is missing"))
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
                RECONCILED_VERSION,
                RECONCILED_DIGEST,
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
    #[cfg(test)]
    async fn accept_catalog_manifest(
        &self,
        scope: &CatalogScope,
        keys: &[(&str, &str)],
        version: i64,
        digest: &str,
    ) -> Result<(), StorageError> {
        self.replace_catalog_keys_in(
            scope,
            keys,
            Some(CatalogAcceptance {
                version,
                digest,
                entries: &[],
                execution: None,
            }),
        )
        .await
    }

    async fn replace_catalog_keys_in(
        &self,
        scope: &CatalogScope,
        keys: &[(&str, &str)],
        acceptance: Option<CatalogAcceptance<'_>>,
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
        if let Some(accepted) = &acceptance {
            catalog_trust_in(&mut tx, scope)
                .await?
                .check_candidate(accepted.version, accepted.digest)?;
            // Both lookups return one indexed row, never workflow bodies. A
            // failed first download must not permit later entry equivocation
            // or a per-workflow version rollback under a newer root document.
            for (name, version, digest) in accepted.entries {
                let known: Option<(i32, String)> = sqlx::query_as("SELECT version, body_digest FROM mobile_catalog_entry_history WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3 ORDER BY version DESC LIMIT 1")
                    .bind(&scope.account.id).bind(&scope.id).bind(name).fetch_optional(&mut *tx).await?;
                let installed: Option<(i32, String)> = sqlx::query_as("SELECT version, body_digest FROM mobile_catalog_workflows WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3 ORDER BY version DESC LIMIT 1")
                    .bind(&scope.account.id).bind(&scope.id).bind(name).fetch_optional(&mut *tx).await?;
                for (previous, hash) in known.into_iter().chain(installed) {
                    if previous > *version || (previous == *version && hash != *digest) {
                        return Err(invalid(
                            "catalog workflow version rollback or changed published identity",
                        ));
                    }
                }
                sqlx::query("INSERT INTO mobile_catalog_entry_history(account_id, catalog_id, name, version, body_digest) VALUES (?1, ?2, ?3, ?4, ?5) ON CONFLICT(account_id, catalog_id, name, version) DO NOTHING")
                    .bind(&scope.account.id).bind(&scope.id).bind(name).bind(version).bind(digest).execute(&mut *tx).await?;
            }
        }
        if let Some(accepted) = &acceptance
            && let Some(entries) = accepted.execution
        {
            for entry in entries {
                let mut required = entry.required_handlers.clone();
                required.sort();
                required.dedup();
                if entry.min_sdk_version.len() > 256 || required.len() > 256 {
                    return Err(invalid("catalog execution policy exceeds its limit"));
                }
                for name in &required {
                    validate(name)?;
                    if name.len() > 256 {
                        return Err(invalid("invalid catalog required handler"));
                    }
                }
                sqlx::query("INSERT INTO mobile_catalog_execution_policy(account_id,catalog_id,name,version,manifest_version,manifest_digest,min_sdk_version,required_handlers) VALUES (?1,?2,?3,?4,?5,?6,?7,?8) ON CONFLICT(account_id,catalog_id,name,version) DO UPDATE SET manifest_version=excluded.manifest_version,manifest_digest=excluded.manifest_digest,min_sdk_version=excluded.min_sdk_version,required_handlers=excluded.required_handlers")
                    .bind(&scope.account.id).bind(&scope.id).bind(&entry.name).bind(entry.version).bind(accepted.version).bind(accepted.digest)
                    .bind(&entry.min_sdk_version).bind(serde_json::to_string(&required)?).execute(&mut *tx).await?;
            }
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
        if let Some(accepted) = acceptance {
            write_metadata_in(
                &mut tx,
                scope,
                &[
                    (ACCEPTED_VERSION, Some(&accepted.version.to_string())),
                    (ACCEPTED_DIGEST, Some(accepted.digest)),
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
        #[cfg(test)]
        {
            let barrier = self.metadata_barrier.lock().unwrap().take();
            if let Some(barrier) = barrier {
                let _ = barrier.entered.send(());
                barrier.release.acquire().await.unwrap().forget();
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Called after the complete snapshot removal pass. This is independent
    /// of downloads/evaluation: one unavailable entry need not block other
    /// authorized cached workflows, while pending removals admit no new work.
    pub(crate) async fn mark_catalog_reconciled(
        &self,
        scope: &CatalogScope,
        version: i64,
        digest: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        catalog_trust_in(&mut tx, scope)
            .await?
            .require_current(version, digest)?;
        write_metadata_in(
            &mut tx,
            scope,
            &[
                (RECONCILED_VERSION, Some(&version.to_string())),
                (RECONCILED_DIGEST, Some(digest)),
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) mod test_support;
