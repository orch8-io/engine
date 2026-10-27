//! Bounded indexed pagination; cursor timestamps come from SQL columns.
use super::{SqliteStorage, continuity::decode};
use crate::{EffectReceiptCursor, EffectReceiptPage};
use orch8_types::{
    continuity::{ContinuityId, EffectId},
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;

pub(super) async fn page(
    store: &SqliteStorage,
    tenant: &TenantId,
    continuity: ContinuityId,
    after: Option<&EffectReceiptCursor>,
    limit: u32,
) -> Result<EffectReceiptPage, StorageError> {
    let limit = limit.min(1000);
    if limit == 0 {
        return Ok(EffectReceiptPage {
            receipts: Vec::new(),
            next: None,
        });
    }
    let mut query = sqlx::QueryBuilder::new(
        "SELECT id,created_at,record FROM effect_receipts WHERE tenant_id=",
    );
    query
        .push_bind(tenant.as_str())
        .push(" AND continuity_id=")
        .push_bind(continuity.to_string());
    if let Some(cursor) = after {
        let time = cursor.created_at.clone();
        query
            .push(" AND (created_at,id)>(")
            .push_bind(time)
            .push(",")
            .push_bind(cursor.id.to_string())
            .push(")");
    }
    query
        .push(" ORDER BY created_at,id LIMIT ")
        .push_bind(i64::from(limit));
    let rows = query.build().fetch_all(&store.pool).await?;
    let full = rows.len() == limit as usize;
    let mut receipts = Vec::with_capacity(rows.len());
    let mut last = None;
    for row in rows {
        let id = EffectId::from_uuid(
            row.try_get::<String, _>("id")?
                .parse::<uuid::Uuid>()
                .map_err(|error| StorageError::Query(error.to_string()))?,
        );
        let receipt = decode(&row.try_get::<String, _>("record")?)?;
        crate::effect_receipts::validate(&receipt, tenant, continuity, id)?;
        last = Some(EffectReceiptCursor {
            created_at: row.try_get::<String, _>("created_at")?,
            id,
        });
        receipts.push(receipt);
    }
    Ok(EffectReceiptPage {
        receipts,
        next: if full { last } else { None },
    })
}

pub(super) async fn audit(
    store: &SqliteStorage,
    tenant: &TenantId,
    continuity: ContinuityId,
    kind: orch8_types::continuity::EffectKind,
    candidate: Option<&orch8_types::continuity::EffectReceipt>,
) -> Result<crate::EffectAtMostOnceAudit, StorageError> {
    crate::effect_receipts::validate_audit_candidate(tenant, continuity, kind, candidate)?;
    let proposal = candidate
        .map(serde_json::to_string)
        .transpose()
        .map_err(StorageError::Serialization)?;
    // One statement: counts and witnesses describe the same SQLite snapshot.
    let row = sqlx::query(
        "WITH scoped AS (
            SELECT json_extract(record,'$.destination_fingerprint') AS destination,
                   json_extract(record,'$.request_sha256') AS request
            FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2
              AND json_extract(record,'$.kind')=?3
              AND json_extract(record,'$.state') IN ('dispatched','committed','unknown','verified')
              AND (?5 IS NULL OR id<>?5)
            UNION ALL SELECT json_extract(?4,'$.destination_fingerprint'),
                             json_extract(?4,'$.request_sha256') WHERE ?4 IS NOT NULL
         ), duplicates AS (
            SELECT destination,request FROM scoped GROUP BY destination,request HAVING COUNT(*)>1
         )
         SELECT (SELECT COUNT(*) FROM scoped) AS observed,
                (SELECT COUNT(*) FROM duplicates) AS duplicates,
                (SELECT COUNT(*) FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2
                 AND (json_extract(record,'$.state') IS NULL OR json_extract(record,'$.state') NOT IN ('planned','prepared','dispatched','committed','unknown','verified','compensated','abandoned'))) AS invalid,
                (SELECT json_group_array(json_object('destination_fingerprint',destination,'request_sha256',request))
                 FROM (SELECT destination,request FROM duplicates ORDER BY destination,request LIMIT 16)) AS witnesses"
    )
    .bind(tenant.as_str()).bind(continuity.to_string())
    .bind(super::continuity::state_name(kind)?).bind(proposal)
    .bind(candidate.map(|r| r.id.to_string()))
    .fetch_one(&store.pool).await?;
    let witnesses = serde_json::from_str(&row.get::<String, _>("witnesses"))
        .map_err(StorageError::Serialization)?;
    crate::effect_receipts::validate_known_states(row.get("invalid"))?;
    crate::effect_receipts::decode_audit(row.get("observed"), row.get("duplicates"), witnesses)
}

pub(super) async fn status(
    store: &SqliteStorage,
    tenant: &TenantId,
    continuity: ContinuityId,
) -> Result<crate::EffectLedgerStatus, StorageError> {
    let row = sqlx::query(
        "WITH scoped AS (
            SELECT id,created_at,record,json_extract(record,'$.state') AS effect_state
            FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2
         ), unresolved AS (
            SELECT * FROM scoped WHERE effect_state NOT IN ('committed','verified','compensated','abandoned')
         )
         SELECT (SELECT COUNT(*) FROM scoped) AS total,
                (SELECT COUNT(*) FROM unresolved) AS unresolved,
                (SELECT COUNT(*) FROM scoped WHERE effect_state='unknown') AS unknown,
                (SELECT COUNT(*) FROM scoped WHERE effect_state IS NULL OR effect_state NOT IN ('planned','prepared','dispatched','committed','unknown','verified','compensated','abandoned')) AS invalid,
                (SELECT json_group_array(json(record)) FROM (
                    SELECT record FROM unresolved ORDER BY created_at,id LIMIT 16)) AS sample,
                (SELECT json_group_array(json_extract(record,'$.request_sha256')) FROM (
                    SELECT record FROM scoped WHERE effect_state='unknown' ORDER BY created_at,id LIMIT 16)) AS unknown_sample"
    )
    .bind(tenant.as_str()).bind(continuity.to_string()).fetch_one(&store.pool).await?;
    crate::effect_receipts::validate_known_states(row.get("invalid"))?;
    crate::effect_receipts::decode_status(
        tenant,
        continuity,
        (row.get("total"), row.get("unresolved"), row.get("unknown")),
        serde_json::from_str(&row.get::<String, _>("sample"))?,
        serde_json::from_str(&row.get::<String, _>("unknown_sample"))?,
    )
}

/// A single SELECT retains one database snapshot through the whole stream;
/// paginated independent reads could skip state changes between pages.
pub(super) async fn compensation(
    store: &SqliteStorage,
    tenant: &TenantId,
    continuity: ContinuityId,
) -> Result<Vec<crate::CompensationEffect>, StorageError> {
    let mut connection = store.pool.acquire().await?;
    compensation_on(&mut connection, tenant, continuity).await
}

pub(super) async fn compensation_on(
    conn: &mut sqlx::SqliteConnection,
    tenant: &TenantId,
    continuity: ContinuityId,
) -> Result<Vec<crate::CompensationEffect>, StorageError> {
    use futures::TryStreamExt;
    let mut rows = sqlx::query(
        "SELECT id,record FROM effect_receipts WHERE tenant_id=?1 AND continuity_id=?2",
    )
    .bind(tenant.as_str())
    .bind(continuity.to_string())
    .fetch(&mut *conn);
    let mut effects = Vec::new();
    while let Some(row) = rows.try_next().await? {
        let id = EffectId::from_uuid(
            row.try_get::<String, _>("id")?
                .parse()
                .map_err(|error: uuid::Error| StorageError::Constraint(error.to_string()))?,
        );
        let receipt: orch8_types::continuity::EffectReceipt =
            decode(&row.get::<String, _>("record"))?;
        crate::effect_receipts::validate(&receipt, tenant, continuity, id)?;
        if crate::effect_receipts::compensation_relevant(receipt.state) {
            effects.push(crate::CompensationEffect::from(&receipt));
        }
    }
    Ok(effects)
}
