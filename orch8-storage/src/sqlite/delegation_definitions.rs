//! Transaction-local catalog capture and immutable bundle storage.
use crate::delegation_definition::graph::{
    DefinitionGraphLimits, DefinitionManifest, DefinitionResolver, DefinitionSelector, capture,
};
use crate::delegation_definition::{PublishedDelegationDefinition, decode_document, invalid};
use crate::delegation_publication::{DelegationPublication, DelegationPublishIntent};
use async_trait::async_trait;
use orch8_types::delegation_definition::wire::{
    DefinitionBundleReference, EncodedDefinitionBundle, RetainedDefinitionDocument,
};
use orch8_types::{
    error::StorageError,
    ids::{Namespace, TenantId},
    sequence::SequenceDefinition,
};
use sqlx::Row;
use uuid::Uuid;

struct Catalog<'a>(&'a mut sqlx::SqliteConnection);
#[async_trait]
impl DefinitionResolver for Catalog<'_> {
    async fn resolve(
        &mut self,
        tenant: &TenantId,
        namespace: &Namespace,
        selector: &DefinitionSelector,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        let row=sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND ((?4 IS NOT NULL AND version=?4) OR (?4 IS NULL AND deprecated=0)) ORDER BY version DESC LIMIT 1").bind(tenant.as_str()).bind(namespace.as_str()).bind(&selector.name).bind(selector.version).fetch_optional(&mut *self.0).await?;
        row.map(|row| super::helpers::row_to_sequence(&row))
            .transpose()
    }
}

pub(super) async fn capture_on(
    conn: &mut sqlx::SqliteConnection,
    intent: &DelegationPublishIntent,
) -> Result<PublishedDelegationDefinition, StorageError> {
    let graph = capture(
        intent.sequence.clone(),
        &mut Catalog(conn),
        DefinitionGraphLimits::default(),
    )
    .await?;
    let digest = graph.sha256()?;
    for document in &graph.definitions {
        sqlx::query("INSERT OR IGNORE INTO delegation_definitions(tenant_id,sequence_sha256,sequence_id,version,definition) VALUES(?1,?2,?3,1,?4)").bind(intent.sequence.tenant_id.as_str()).bind(&document.sequence_sha256).bind(document.definition.id.to_string())
            .bind(serde_json::to_string(&document.definition)?).execute(&mut *conn).await?;
    }
    let record = serde_json::to_string(&graph.manifest)?;
    // Shared bodies may already exist with an earlier creation timestamp. Pin
    // the exact retained bytes, not a reserialization of today's catalog structs.
    let documents = records_on(conn, &intent.sequence.tenant_id, &graph.manifest).await?;
    let (wire, _) = EncodedDefinitionBundle::from_retained(
        &record,
        documents,
        DefinitionGraphLimits::default(),
    )?;
    let reference = wire.reference();
    sqlx::query("INSERT OR IGNORE INTO delegation_definition_manifests(tenant_id,task_id,bundle_sha256,manifest,wire_version,wire_sha256,wire_bytes,wire_definitions,wire_selectors) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)").bind(intent.sequence.tenant_id.as_str()).bind(intent.task.id.to_string()).bind(&digest).bind(&record)
        .bind(i64::from(reference.version)).bind(&reference.payload_sha256).bind(i64::from(reference.payload_bytes))
        .bind(i64::from(reference.definition_count)).bind(i64::from(reference.selector_count)).execute(&mut *conn).await?;
    let input = intent.input.reference();
    let stored_input = serde_json::to_string(&intent.stored_input)?;
    if stored_input.len() > 8 * 1024 * 1024 {
        return Err(invalid());
    }
    let inserted = sqlx::query("INSERT INTO delegation_publication_inputs(tenant_id,task_id,version,request_sha256,definition_expanded_bytes,input_wire_version,input_sha256,input_bytes,input_expanded_bytes,stored_input) VALUES(?1,?2,1,?3,?4,?5,?6,?7,?8,?9)")
        .bind(intent.sequence.tenant_id.as_str()).bind(intent.task.id.to_string()).bind(&intent.receipt.request_sha256)
        .bind(i64::from(wire.expanded_bytes())).bind(i64::from(input.version)).bind(&input.payload_sha256)
        .bind(i64::from(input.payload_bytes)).bind(i64::from(input.expanded_bytes)).bind(stored_input)
        .execute(&mut *conn).await?;
    if inserted.rows_affected() != 1 {
        return Err(invalid());
    }
    let stored = read_on(
        conn,
        &intent.sequence.tenant_id,
        intent.task.id,
        &intent.receipt,
    )
    .await?
    .ok_or_else(invalid)?;
    if stored.bundle_sha256 != digest || stored.wire().reference() != reference {
        return Err(invalid());
    }
    Ok(stored)
}

pub(super) async fn read_on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    task: Uuid,
    publication: &DelegationPublication,
) -> Result<Option<PublishedDelegationDefinition>, StorageError> {
    let row=sqlx::query("SELECT bundle_sha256,manifest,wire_version,wire_sha256,wire_bytes,wire_definitions,wire_selectors FROM delegation_definition_manifests WHERE tenant_id=?1 AND task_id=?2").bind(tenant.as_str()).bind(task.to_string()).fetch_optional(&mut *conn).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let record: String = row.get("manifest");
    let manifest = DefinitionManifest::decode(&record)?;
    let number = |column: &str| u32::try_from(row.get::<i64, _>(column)).map_err(|_| invalid());
    let reference = DefinitionBundleReference {
        version: number("wire_version")?,
        sequence_sha256: publication.sequence_sha256.clone(),
        bundle_sha256: row.get("bundle_sha256"),
        payload_sha256: row.get("wire_sha256"),
        payload_bytes: number("wire_bytes")?,
        definition_count: number("wire_definitions")?,
        selector_count: number("wire_selectors")?,
    };
    reference.validate(DefinitionGraphLimits::default())?;
    if manifest.sha256()? != reference.bundle_sha256
        || manifest.root_sha256 != reference.sequence_sha256
    {
        return Err(invalid());
    }
    let documents = records_on(conn, tenant, &manifest).await?;
    let (wire, verified) = EncodedDefinitionBundle::from_retained(
        &record,
        documents,
        DefinitionGraphLimits::default(),
    )?;
    if wire.reference() != &reference {
        return Err(invalid());
    }
    Ok(Some(PublishedDelegationDefinition::from_verified_graph(
        verified,
        wire,
        tenant,
        task,
        publication,
    )?))
}

async fn records_on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    manifest: &DefinitionManifest,
) -> Result<Vec<RetainedDefinitionDocument>, StorageError> {
    let hashes = manifest.document_hashes()?;
    // Bound aggregate body bytes in SQL before transferring them into memory.
    let rows=sqlx::query("WITH docs AS (SELECT sequence_sha256,sequence_id,version,definition FROM delegation_definitions WHERE tenant_id=?1 AND sequence_sha256 IN (SELECT value FROM json_each(?2))) SELECT * FROM docs WHERE (SELECT COALESCE(SUM(length(CAST(definition AS BLOB))),0) FROM docs)<=4194304").bind(tenant.as_str()).bind(serde_json::to_string(&hashes)?).fetch_all(&mut *conn).await?;
    if rows.len() != hashes.len() {
        return Err(invalid());
    }
    let mut records = Vec::with_capacity(rows.len());
    for row in rows {
        let hash: String = row.get("sequence_sha256");
        let definition_json: String = row.get("definition");
        decode_document(
            tenant,
            &hash,
            Uuid::parse_str(&row.get::<String, _>("sequence_id")).map_err(|_| invalid())?,
            row.get::<i64, _>("version"),
            &definition_json,
        )?;
        records.push(RetainedDefinitionDocument {
            sequence_sha256: hash,
            definition_json,
        });
    }
    Ok(records)
}
