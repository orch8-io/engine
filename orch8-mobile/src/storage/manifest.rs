//! Durable acceptance is distinct from successful workflow evaluation. A retry
//! may finish an accepted document, but cannot restore an older trust set.
use super::MobileStorage;
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
