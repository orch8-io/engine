//! Durable acceptance is distinct from successful workflow evaluation. A retry
//! may finish an accepted document, but cannot restore an older trust set.
use super::MobileStorage;
use crate::{BundledCatalogDisposition, BundledCatalogInstall};
use chrono::Utc;
use orch8_types::error::StorageError;
use sqlx::{Sqlite, Transaction};
use std::collections::HashMap;

pub(super) const ACCEPTED_VERSION: &str = "accepted_manifest_version";
pub(super) const ACCEPTED_DIGEST: &str = "accepted_manifest_digest";

#[derive(Default)]
pub(crate) struct ManifestTrust {
    pub(crate) accepted_version: Option<i64>,
    pub(crate) applied_version: Option<i64>,
    accepted_digest: Option<String>,
}

fn conflict(message: &'static str) -> StorageError {
    StorageError::Conflict(message.into())
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

impl ManifestTrust {
    pub(super) fn from_rows(rows: Vec<(String, String)>) -> Result<Self, StorageError> {
        let mut rows: HashMap<_, _> = rows.into_iter().collect();
        let parse = |value: Option<String>| -> Result<Option<i64>, StorageError> {
            value
                .map(|value| {
                    value
                        .parse::<i64>()
                        .ok()
                        .filter(|version| *version >= 0)
                        .ok_or_else(|| conflict("invalid manifest version metadata"))
                })
                .transpose()
        };
        let accepted_version = parse(rows.remove(ACCEPTED_VERSION))?;
        let applied_version = parse(rows.remove("last_manifest_version"))?;
        let accepted_digest = rows.remove(ACCEPTED_DIGEST);
        if accepted_version.is_some() != accepted_digest.is_some()
            || accepted_digest
                .as_deref()
                .is_some_and(|digest| !valid_digest(digest))
            || matches!((accepted_version, applied_version), (Some(accepted), Some(applied)) if accepted < applied)
        {
            return Err(conflict("inconsistent manifest trust metadata"));
        }
        Ok(Self {
            accepted_version,
            applied_version,
            accepted_digest,
        })
    }

    pub(crate) fn floor(&self) -> Option<i64> {
        // Legacy databases have only an applied cursor. Never reset that fence
        // when adopting the new accepted-version record.
        self.accepted_version.or(self.applied_version)
    }

    pub(crate) fn pending(&self) -> bool {
        self.accepted_version.is_some_and(|accepted| {
            self.applied_version
                .is_none_or(|applied| accepted > applied)
        })
    }

    /// A completed evaluation also proves that removal reconciliation ran.
    /// This lets older fully evaluated scoped caches work offline without
    /// inventing a reconciliation marker for an unproven/pending document.
    pub(crate) fn evaluated_identity(&self) -> Option<(i64, &str)> {
        let version = self.accepted_version?;
        (self.applied_version == Some(version))
            .then_some((version, self.accepted_digest.as_deref()?))
    }

    pub(crate) fn check_candidate(&self, version: i64, digest: &str) -> Result<(), StorageError> {
        if version < 0 || !valid_digest(digest) {
            return Err(conflict("invalid manifest acceptance identity"));
        }
        if self.floor().is_some_and(|floor| version < floor) {
            return Err(conflict(
                "manifest is older than the durable acceptance fence",
            ));
        }
        if self.accepted_version == Some(version) && self.accepted_digest.as_deref() != Some(digest)
        {
            return Err(conflict(
                "manifest content changed without a version increase",
            ));
        }
        Ok(())
    }

    pub(super) fn require_current(&self, version: i64, digest: &str) -> Result<(), StorageError> {
        self.check_candidate(version, digest)?;
        if self.accepted_version != Some(version) {
            return Err(conflict(
                "manifest evaluation requires the current accepted document",
            ));
        }
        Ok(())
    }
}

pub(super) async fn legacy_trust_in(
    tx: &mut Transaction<'_, Sqlite>,
) -> Result<ManifestTrust, StorageError> {
    let rows = sqlx::query_as("SELECT key, value FROM sync_metadata WHERE key IN ('accepted_manifest_version', 'accepted_manifest_digest', 'last_manifest_version')")
        .fetch_all(&mut **tx).await?;
    ManifestTrust::from_rows(rows)
}

impl MobileStorage {
    pub(crate) async fn install_legacy_seed(
        &self,
        sequences: &[orch8_types::sequence::SequenceDefinition],
        limit: u32,
    ) -> Result<BundledCatalogInstall, StorageError> {
        let sequence_count = u32::try_from(sequences.len()).map_err(|_| {
            StorageError::Constraint("bundled catalog has too many sequences".into())
        })?;
        let mut tx = self.pool().begin_with("BEGIN IMMEDIATE").await?;
        let trust = legacy_trust_in(&mut tx).await?;
        if trust.accepted_version.is_some() || trust.applied_version.is_some() {
            return Ok(BundledCatalogInstall {
                disposition: BundledCatalogDisposition::ManifestOwned,
                installed: 0,
                skipped: sequence_count,
            });
        }

        let mut installed = 0;
        for sequence in sequences {
            let existing: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM sequences WHERE tenant_id = ?1
                 AND namespace = ?2 AND name = ?3)",
            )
            .bind(sequence.tenant_id.as_str())
            .bind(sequence.namespace.as_str())
            .bind(&sequence.name)
            .fetch_one(&mut *tx)
            .await?;
            if existing {
                continue;
            }
            let count: i64 = sqlx::query_scalar(
                "SELECT COUNT(DISTINCT name) FROM sequences WHERE tenant_id = ?1 AND namespace = ?2",
            )
            .bind(sequence.tenant_id.as_str())
            .bind(sequence.namespace.as_str())
            .fetch_one(&mut *tx)
            .await?;
            if count >= i64::from(limit) {
                return Err(StorageError::Constraint(
                    "bundled catalog exceeds the configured sequence count limit".into(),
                ));
            }
            orch8_storage::sqlite::SqliteStorage::create_sequence_in_transaction(&mut tx, sequence)
                .await?;
            installed += 1;
        }
        tx.commit().await?;
        Ok(BundledCatalogInstall {
            disposition: if installed == 0 {
                BundledCatalogDisposition::AlreadyPresent
            } else {
                BundledCatalogDisposition::Seeded
            },
            installed,
            skipped: sequence_count - installed,
        })
    }

    pub(crate) async fn manifest_trust(&self) -> Result<ManifestTrust, StorageError> {
        let rows = sqlx::query_as("SELECT key, value FROM sync_metadata WHERE key IN ('accepted_manifest_version', 'accepted_manifest_digest', 'last_manifest_version')")
            .fetch_all(self.pool()).await?;
        ManifestTrust::from_rows(rows)
    }

    /// Accept one verified, fully validated document atomically with its trust
    /// set, before workflow effects. The writer reservation precedes the durable
    /// comparison, including when no acceptance row exists yet.
    pub(crate) async fn accept_manifest_trust(
        &self,
        keys: &[(&str, &str)],
        version: i64,
        digest: &str,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool().begin().await?;
        sqlx::query(
            "UPDATE sync_metadata SET value = value WHERE key = 'accepted_manifest_version'",
        )
        .execute(&mut *tx)
        .await?;
        legacy_trust_in(&mut tx)
            .await?
            .check_candidate(version, digest)?;
        sqlx::query("DELETE FROM trusted_keys")
            .execute(&mut *tx)
            .await?;
        let now = Utc::now().to_rfc3339();
        for (id, key) in keys {
            sqlx::query(
                "INSERT INTO trusted_keys (key_id, public_key, trusted_since) VALUES (?1, ?2, ?3)",
            )
            .bind(id)
            .bind(key)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }
        for (key, value) in [
            (ACCEPTED_VERSION, version.to_string()),
            (ACCEPTED_DIGEST, digest.to_owned()),
        ] {
            sqlx::query("INSERT INTO sync_metadata (key, value, updated_at) VALUES (?1, ?2, ?3) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at")
                .bind(key).bind(value).bind(&now).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
