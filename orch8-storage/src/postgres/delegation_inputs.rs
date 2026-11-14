//! Bounded reads of immutable publication input, with no mutable-data fallback.
use super::PostgresStorage;
use crate::delegation_input::{PublishedDelegationInput, invalid};
use orch8_types::{
    delegation_definition::input::DelegationInputReference, error::StorageError, ids::TenantId,
};
use sqlx::Row;
use uuid::Uuid;

pub(super) async fn read(
    store: &PostgresStorage,
    tenant: &TenantId,
    task: Uuid,
) -> Result<Option<PublishedDelegationInput>, StorageError> {
    let row = sqlx::query("SELECT i.version,i.request_sha256,i.definition_expanded_bytes,i.input_wire_version,i.input_sha256,i.input_bytes,i.input_expanded_bytes,CASE WHEN octet_length(i.stored_input)<=8388608 THEN i.stored_input END AS stored_input,CASE WHEN octet_length(p.record::text)<=65536 THEN p.record::text END AS publication FROM delegation_publication_inputs i JOIN delegation_publications p ON p.tenant_id=i.tenant_id AND p.task_id=i.task_id WHERE i.tenant_id=$1 AND i.task_id=$2")
        .bind(tenant.as_str()).bind(task).fetch_optional(&store.pool).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let number = |column: &str| u32::try_from(row.get::<i32, _>(column)).map_err(|_| invalid());
    if number("version")? != 1 {
        return Err(invalid());
    }
    PublishedDelegationInput::from_record(
        tenant,
        task,
        &row.get::<Option<String>, _>("publication")
            .ok_or_else(invalid)?,
        row.get("request_sha256"),
        DelegationInputReference {
            version: number("input_wire_version")?,
            payload_sha256: row.get("input_sha256"),
            payload_bytes: number("input_bytes")?,
            expanded_bytes: number("input_expanded_bytes")?,
        },
        number("definition_expanded_bytes")?,
        &row.get::<Option<String>, _>("stored_input")
            .ok_or_else(invalid)?,
    )
    .map(Some)
}
