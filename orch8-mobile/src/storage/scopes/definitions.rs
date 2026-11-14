//! Downloaded graph receipts, separate from signed catalog sync and native runs.
//! Expected scope/reference/server are trusted host claim context, never values
//! copied from the response. Installation grants no execution or credential use.
use super::{CatalogScope, fence_catalog, invalid};
use crate::storage::MobileStorage;
use orch8_storage::sqlite::SqliteStorage;
use orch8_types::{
    delegation_definition::{
        DefinitionGraphLimits, DefinitionManifest,
        wire::{DefinitionBundleReference, DefinitionReadScope, DefinitionWireEnvelope},
    },
    error::StorageError,
    ids::{Namespace, SequenceId},
};
use std::collections::BTreeSet;

mod binding;
pub(crate) use binding::BoundClaimRun;
mod native;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct InstalledClaimDefinitions {
    pub(crate) receipt_id: String,
    pub(crate) root_id: SequenceId,
    pub(crate) namespace: Namespace,
}

impl MobileStorage {
    /// Staged internal boundary; claim-side negotiation and restart authority
    /// must be wired before a public SDK or provider execution path calls this.
    #[allow(dead_code)]
    pub(crate) async fn install_claim_definitions(
        &self,
        scope: &CatalogScope,
        server: &str,
        expected_scope: &DefinitionReadScope,
        expected_reference: &DefinitionBundleReference,
        envelope: &DefinitionWireEnvelope,
    ) -> Result<InstalledClaimDefinitions, StorageError> {
        super::transport::endpoint(server, false)?;
        super::validate(expected_scope.tenant_id.as_str())?;
        super::validate(&expected_scope.worker_id)?;
        let verified = envelope.verify(
            expected_scope,
            expected_reference,
            DefinitionGraphLimits::default(),
        )?;
        let manifest = verified.graph().manifest.clone();
        let mut tx = self.pool().begin().await?;
        fence_catalog(&mut tx, scope).await?;
        if let Some(existing) = read_existing(
            &mut tx,
            scope,
            server,
            expected_scope,
            expected_reference,
            Some(&manifest),
        )
        .await?
        {
            tx.commit().await?;
            return Ok(existing);
        }
        let (tenant, _) = super::workflows::owner(scope)?;
        let prepared = verified.prepare_local(&tenant)?;
        let receipt = InstalledClaimDefinitions {
            receipt_id: uuid::Uuid::new_v4().to_string(),
            root_id: prepared.root_id(),
            namespace: prepared.namespace().clone(),
        };
        let mut entries = Vec::with_capacity(prepared.entries().len());
        let mut bytes = 0_i64;
        for entry in prepared.into_entries() {
            let (source, definition) = entry.into_parts();
            SqliteStorage::create_sequence_in_transaction(&mut tx, &definition).await?;
            let fingerprint = native::fingerprint(&mut tx, definition.id).await?;
            bytes = bytes
                .checked_add(fingerprint.bytes)
                .ok_or_else(|| invalid("local definition size overflow"))?;
            entries.push((source, fingerprint.sha256, definition.id));
        }
        capacity(&mut tx, scope, bytes).await?;
        sqlx::query("INSERT INTO mobile_claim_definition_receipts(id,account_id,catalog_id,owner_epoch,server,remote_tenant,task_id,worker_id,claim_epoch,reference_json,namespace,root_id,definition_bytes) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)")
            .bind(&receipt.receipt_id).bind(scope.account_id()).bind(scope.catalog_id()).bind(scope.epoch()).bind(server).bind(expected_scope.tenant_id.as_str()).bind(expected_scope.task_id.to_string()).bind(&expected_scope.worker_id).bind(expected_scope.claim_epoch.to_string()).bind(serde_json::to_string(expected_reference)?).bind(receipt.namespace.as_str()).bind(receipt.root_id.to_string()).bind(bytes).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO mobile_claim_definition_codecs(receipt_id,version) VALUES (?1,1)")
            .bind(&receipt.receipt_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO mobile_claim_definition_manifests(receipt_id,manifest) VALUES (?1,?2)",
        )
        .bind(&receipt.receipt_id)
        .bind(serde_json::to_string(&manifest)?)
        .execute(&mut *tx)
        .await?;
        for (source, local_hash, sequence_id) in entries {
            sqlx::query("INSERT INTO mobile_claim_definition_entries(receipt_id,source_sha256,sequence_id,local_sha256) VALUES (?1,?2,?3,?4)")
                .bind(&receipt.receipt_id).bind(source).bind(sequence_id.to_string()).bind(local_hash).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(receipt)
    }
}

async fn read_existing(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    server: &str,
    claim: &DefinitionReadScope,
    expected: &DefinitionBundleReference,
    incoming_manifest: Option<&DefinitionManifest>,
) -> Result<Option<InstalledClaimDefinitions>, StorageError> {
    let found: Option<(String,String,String,String,i64)> = sqlx::query_as("SELECT id,reference_json,namespace,root_id,definition_bytes FROM mobile_claim_definition_receipts WHERE account_id=?1 AND catalog_id=?2 AND owner_epoch=?3 AND server=?4 AND remote_tenant=?5 AND task_id=?6 AND worker_id=?7 AND claim_epoch=?8")
        .bind(scope.account_id()).bind(scope.catalog_id()).bind(scope.epoch()).bind(server).bind(claim.tenant_id.as_str()).bind(claim.task_id.to_string()).bind(&claim.worker_id).bind(claim.claim_epoch.to_string()).fetch_optional(&mut **tx).await?;
    let Some((id, reference, namespace, root, stored_bytes)) = found else {
        return Ok(None);
    };
    let reference: DefinitionBundleReference = serde_json::from_str(&reference)
        .map_err(|_| invalid("invalid installed graph reference"))?;
    if reference != *expected {
        return Err(invalid("claim definition reference changed"));
    }
    let source_hashes = manifest_sources(tx, &id, expected, incoming_manifest).await?;
    let rows:Vec<(String,String,String)> = sqlx::query_as("SELECT source_sha256,sequence_id,local_sha256 FROM mobile_claim_definition_entries WHERE receipt_id=?1 LIMIT 257").bind(&id).fetch_all(&mut **tx).await?;
    if rows.len() != expected.definition_count as usize
        || rows
            .iter()
            .map(|(source, _, _)| source.clone())
            .collect::<BTreeSet<_>>()
            != source_hashes
    {
        return Err(invalid("installed graph definition count changed"));
    }
    let (tenant, _) = super::workflows::owner(scope)?;
    let root_id = root
        .parse::<uuid::Uuid>()
        .map(SequenceId::from_uuid)
        .map_err(|_| invalid("invalid installed root ID"))?;
    let mut root_found = false;
    let mut bytes = 0_i64;
    for (source, local, hash) in rows {
        let local = local
            .parse::<uuid::Uuid>()
            .map(SequenceId::from_uuid)
            .map_err(|_| invalid("invalid installed definition ID"))?;
        let fingerprint = native::fingerprint(tx, local).await?;
        if fingerprint.sha256 != hash {
            return Err(invalid("installed definition changed"));
        }
        let definition = SqliteStorage::get_sequence_in_transaction(tx, local)
            .await?
            .ok_or_else(|| invalid("installed definition is missing"))?;
        if definition.tenant_id != tenant
            || definition.namespace.as_str() != namespace
            || definition.sub_tenant.is_some()
            || definition.embed.is_some()
        {
            return Err(invalid("installed definition changed"));
        }
        bytes = bytes
            .checked_add(fingerprint.bytes)
            .ok_or_else(|| invalid("installed definition size overflow"))?;
        if source == expected.sequence_sha256 && local == root_id {
            root_found = true;
        }
    }
    if !root_found || bytes != stored_bytes {
        return Err(invalid("installed definition receipt is inconsistent"));
    }
    Ok(Some(InstalledClaimDefinitions {
        receipt_id: id,
        root_id,
        namespace: Namespace::new(namespace),
    }))
}

/// Authenticate the retained source set before loading native definitions.
async fn manifest_sources(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    expected: &DefinitionBundleReference,
    incoming_manifest: Option<&DefinitionManifest>,
) -> Result<BTreeSet<String>, StorageError> {
    let codec: Option<i64> = sqlx::query_scalar(
        "SELECT version FROM mobile_claim_definition_codecs WHERE receipt_id=?1",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    if codec != Some(1) {
        return Err(invalid(
            "native definition fingerprint codec is unavailable or unsupported",
        ));
    }
    let stored: Option<String> = sqlx::query_scalar(
        "SELECT manifest FROM mobile_claim_definition_manifests WHERE receipt_id=?1",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    let manifest = match (stored, incoming_manifest) {
        (Some(record), _) => DefinitionManifest::decode(&record)?,
        (None, Some(manifest)) => {
            // Repair a missing manifest only from a freshly verified graph.
            // The native codec is checked independently above.
            sqlx::query(
                "INSERT INTO mobile_claim_definition_manifests(receipt_id,manifest) VALUES (?1,?2)",
            )
            .bind(id)
            .bind(serde_json::to_string(manifest)?)
            .execute(&mut **tx)
            .await?;
            manifest.clone()
        }
        (None, None) => return Err(invalid("installed graph manifest is unavailable")),
    };
    if manifest.sha256()? != expected.bundle_sha256
        || manifest.root_sha256 != expected.sequence_sha256
        || manifest.bindings.len() != expected.selector_count as usize
    {
        return Err(invalid("installed graph manifest changed"));
    }
    manifest.document_hashes()
}

async fn capacity(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    scope: &CatalogScope,
    bytes: i64,
) -> Result<(), StorageError> {
    if !(1..=8 * 1024 * 1024).contains(&bytes) {
        return Err(StorageError::QuotaExceeded(
            "local graph exceeds supported size".into(),
        ));
    }
    let (global,account,catalog,total,account_bytes,catalog_bytes):(i64,i64,i64,i64,i64,i64)=sqlx::query_as("SELECT COUNT(*),COALESCE(SUM(account_id=?1),0),COALESCE(SUM(account_id=?1 AND catalog_id=?2),0),COALESCE(SUM(definition_bytes),0),COALESCE(SUM(CASE WHEN account_id=?1 THEN definition_bytes ELSE 0 END),0),COALESCE(SUM(CASE WHEN account_id=?1 AND catalog_id=?2 THEN definition_bytes ELSE 0 END),0) FROM mobile_claim_definition_receipts")
        .bind(scope.account_id()).bind(scope.catalog_id()).fetch_one(&mut **tx).await?;
    if global >= 128
        || account >= 64
        || catalog >= 32
        || total + bytes > 64 * 1024 * 1024
        || account_bytes + bytes > 32 * 1024 * 1024
        || catalog_bytes + bytes > 16 * 1024 * 1024
    {
        return Err(StorageError::QuotaExceeded(
            "local claim definition capacity is occupied".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod native_tests;
