//! Owner-qualified engine definitions. The scoped sync caller must verify the
//! original body/signature and manifest entry/audience before installation.
use super::{CatalogScope, catalog_trust_in, fence_catalog, invalid, validate};
use crate::storage::MobileStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};
use orch8_types::sequence::SequenceDefinition;
use sha2::{Digest, Sha256};
use sqlx::{Sqlite, Transaction};

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct WorkflowRemoval {
    pub(crate) deleted: u64,
    pub(crate) retained: u64,
}

fn owner(scope: &CatalogScope) -> Result<(TenantId, Namespace), StorageError> {
    let tenant = TenantId::new(format!("mobile-account-{}", scope.account.id))
        .map_err(|_| invalid("invalid catalog workflow owner"))?;
    Ok((tenant, Namespace::new(format!("catalog-{}", scope.id))))
}

fn sequence_id(raw: &str) -> Result<SequenceId, StorageError> {
    raw.parse()
        .map(SequenceId::from_uuid)
        .map_err(|_| invalid("invalid catalog workflow ID"))
}

async fn read_owner(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
) -> Result<(), StorageError> {
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM mobile_accounts a JOIN mobile_catalogs c ON c.account_id = a.id WHERE a.id = ?1 AND a.epoch = ?2 AND a.active = 1 AND c.id = ?3)")
        .bind(&scope.account.id).bind(scope.account.epoch).bind(&scope.id).fetch_one(&mut **tx).await?;
    if !valid {
        return Err(invalid("mobile catalog scope is retired or foreign"));
    }
    Ok(())
}

async fn checked_definition(
    tx: &mut Transaction<'_, Sqlite>,
    scope: &CatalogScope,
    id: SequenceId,
    name: &str,
    version: i32,
) -> Result<SequenceDefinition, StorageError> {
    let seq = SqliteStorage::get_sequence_in_transaction(tx, id)
        .await?
        .ok_or_else(|| invalid("catalog workflow definition is missing"))?;
    let (tenant, namespace) = owner(scope)?;
    if seq.tenant_id != tenant
        || seq.namespace != namespace
        || seq.name != name
        || seq.version != version
    {
        return Err(invalid("catalog workflow ownership mismatch"));
    }
    Ok(seq)
}

impl MobileStorage {
    /// The immutable source body determines published identity; local owner
    /// fields/IDs are assigned only after hashing and parsing those exact bytes.
    /// The accepted manifest fence prevents an older run's late storage writes.
    pub(super) async fn install_catalog_workflow(
        &self,
        scope: &CatalogScope,
        original_json: &str,
        manifest_version: i64,
        manifest_digest: &str,
    ) -> Result<SequenceId, StorageError> {
        if original_json.len() > crate::MAX_SEQUENCES_RESPONSE_BYTES {
            return Err(StorageError::QuotaExceeded(
                "catalog workflow exceeds the payload limit".into(),
            ));
        }
        let mut seq: SequenceDefinition = serde_json::from_str(original_json)
            .map_err(|_| invalid("invalid catalog workflow definition"))?;
        validate(&seq.name)?;
        if seq.version <= 0 {
            return Err(invalid("catalog workflow version must be positive"));
        }
        let source_id = seq.id.to_string();
        let digest = hex::encode(Sha256::digest(original_json.as_bytes()));
        let (tenant, namespace) = owner(scope)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        catalog_trust_in(&mut tx, scope)
            .await?
            .require_current(manifest_version, manifest_digest)?;
        let previous: Option<(String, String, Option<String>, Option<i64>)> = sqlx::query_as(
            "SELECT source_id, body_digest, sequence_id, withdrawn_at_version FROM mobile_catalog_workflows WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3 AND version = ?4",
        ).bind(&scope.account.id).bind(&scope.id).bind(&seq.name).bind(seq.version).fetch_optional(&mut *tx).await?;
        if let Some((old_source, old_digest, cached_id, withdrawn)) = previous {
            if old_source != source_id || old_digest != digest {
                return Err(invalid("published catalog workflow version is immutable"));
            }
            if withdrawn.is_some_and(|version| version >= manifest_version) {
                return Err(invalid(
                    "catalog workflow was withdrawn by this or a newer manifest",
                ));
            }
            if let Some(cached_id) = cached_id {
                let id = sequence_id(&cached_id)?;
                checked_definition(&mut tx, scope, id, &seq.name, seq.version).await?;
                if withdrawn.is_some() {
                    sqlx::query("UPDATE sequences SET deprecated = ?2 WHERE id = ?1")
                        .bind(&cached_id)
                        .bind(seq.deprecated)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("UPDATE mobile_catalog_workflows SET withdrawn_at_version = NULL WHERE sequence_id = ?1 AND account_id = ?2 AND catalog_id = ?3")
                        .bind(&cached_id).bind(&scope.account.id).bind(&scope.id).execute(&mut *tx).await?;
                }
                tx.commit().await?;
                return Ok(id);
            }
        }
        // A supplied UUID never selects a physical row in the shared engine.
        // The durable owner/name/version mapping makes retries reuse the local
        // ID while the definition is cached, including across database reopen.
        seq.id = SequenceId::new();
        seq.tenant_id = tenant;
        seq.namespace = namespace;
        seq.sub_tenant = None;
        seq.embed = None;
        SqliteStorage::create_sequence_in_transaction(&mut tx, &seq).await?;
        sqlx::query("INSERT INTO mobile_catalog_workflows (account_id, catalog_id, name, version, source_id, body_digest, sequence_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7) ON CONFLICT(account_id, catalog_id, name, version) DO UPDATE SET sequence_id = excluded.sequence_id, withdrawn_at_version = NULL, last_cached_at = datetime('now')")
            .bind(&scope.account.id).bind(&scope.id).bind(&seq.name).bind(seq.version).bind(source_id).bind(digest).bind(seq.id.to_string()).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(seq.id)
    }

    /// Active catalog lookup. Already-running instances keep their physical
    /// sequence ID and can finish against a retained deprecated definition.
    pub(crate) async fn catalog_workflow(
        &self,
        scope: &CatalogScope,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        validate(name)?;
        let mut tx = self.pool().begin().await?;
        read_owner(&mut tx, scope).await?;
        let row: Option<(String, i32)> = sqlx::query_as("SELECT w.sequence_id, w.version FROM mobile_catalog_workflows w JOIN sequences s ON s.id = w.sequence_id WHERE w.account_id = ?1 AND w.catalog_id = ?2 AND w.name = ?3 AND (?4 IS NULL OR w.version = ?4) AND w.withdrawn_at_version IS NULL AND s.deprecated = 0 ORDER BY w.version DESC LIMIT 1")
            .bind(&scope.account.id).bind(&scope.id).bind(name).bind(version).fetch_optional(&mut *tx).await?;
        let result = if let Some((id, version)) = row {
            Some(checked_definition(&mut tx, scope, sequence_id(&id)?, name, version).await?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }

    pub(crate) async fn catalog_workflow_by_id(
        &self,
        scope: &CatalogScope,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        let mut tx = self.pool().begin().await?;
        read_owner(&mut tx, scope).await?;
        let row: Option<(String, i32)> = sqlx::query_as("SELECT w.name, w.version FROM mobile_catalog_workflows w JOIN sequences s ON s.id = w.sequence_id WHERE w.account_id = ?1 AND w.catalog_id = ?2 AND w.sequence_id = ?3 AND w.withdrawn_at_version IS NULL AND s.deprecated = 0")
            .bind(&scope.account.id).bind(&scope.id).bind(id.to_string()).fetch_optional(&mut *tx).await?;
        let result = if let Some((name, version)) = row {
            Some(checked_definition(&mut tx, scope, id, &name, version).await?)
        } else {
            None
        };
        tx.commit().await?;
        Ok(result)
    }

    pub(crate) async fn remove_catalog_workflow(
        &self,
        scope: &CatalogScope,
        name: &str,
        manifest_version: i64,
        manifest_digest: &str,
    ) -> Result<WorkflowRemoval, StorageError> {
        validate(name)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        catalog_trust_in(&mut tx, scope)
            .await?
            .require_current(manifest_version, manifest_digest)?;
        let rows: Vec<(String, i32)> = sqlx::query_as("SELECT sequence_id, version FROM mobile_catalog_workflows WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3 AND sequence_id IS NOT NULL ORDER BY version")
            .bind(&scope.account.id).bind(&scope.id).bind(name).fetch_all(&mut *tx).await?;
        let mut result = WorkflowRemoval::default();
        for (raw_id, version) in rows {
            let id = sequence_id(&raw_id)?;
            checked_definition(&mut tx, scope, id, name, version).await?;
            let live: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_instances WHERE sequence_id = ?1 AND state NOT IN ('completed', 'failed', 'cancelled'))")
                .bind(&raw_id).fetch_one(&mut *tx).await?;
            if live {
                sqlx::query("UPDATE sequences SET deprecated = 1 WHERE id = ?1")
                    .bind(&raw_id)
                    .execute(&mut *tx)
                    .await?;
                result.retained += 1;
            } else {
                SqliteStorage::delete_sequence_in_transaction(&mut tx, id).await?;
                result.deleted += 1;
            }
        }
        sqlx::query("UPDATE mobile_catalog_workflows SET withdrawn_at_version = MAX(COALESCE(withdrawn_at_version, 0), ?4) WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3")
            .bind(&scope.account.id).bind(&scope.id).bind(name).bind(manifest_version).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Eviction removes only the cached definition; immutable published identity
    /// and withdrawal evidence remain. Nonterminal instances pin their version.
    pub(crate) async fn evict_catalog_workflow(
        &self,
        scope: &CatalogScope,
        name: &str,
        version: i32,
        manifest_version: i64,
        manifest_digest: &str,
    ) -> Result<bool, StorageError> {
        validate(name)?;
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        catalog_trust_in(&mut tx, scope)
            .await?
            .require_current(manifest_version, manifest_digest)?;
        let row: Option<(Option<String>,)> = sqlx::query_as("SELECT sequence_id FROM mobile_catalog_workflows WHERE account_id = ?1 AND catalog_id = ?2 AND name = ?3 AND version = ?4")
            .bind(&scope.account.id).bind(&scope.id).bind(name).bind(version).fetch_optional(&mut *tx).await?;
        let mut evicted = false;
        if let Some((Some(raw_id),)) = row {
            let id = sequence_id(&raw_id)?;
            checked_definition(&mut tx, scope, id, name, version).await?;
            SqliteStorage::delete_sequence_in_transaction(&mut tx, id).await?;
            evicted = true;
        }
        tx.commit().await?;
        Ok(evicted)
    }
}

#[cfg(test)]
mod tests;
