//! Bounded indexed pagination; cursor timestamps come from SQL columns.
use super::{PostgresStorage, continuity::decode};
use crate::{EffectReceiptCursor, EffectReceiptPage};
use orch8_types::{
    continuity::{ContinuityId, EffectId},
    error::StorageError,
    ids::TenantId,
};
use sqlx::Row;

pub(super) async fn page(
    store: &PostgresStorage,
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
        .push_bind(continuity.into_uuid());
    if let Some(cursor) = after {
        let time = chrono::DateTime::parse_from_rfc3339(&cursor.created_at)
            .map_err(|error| StorageError::Constraint(error.to_string()))?
            .with_timezone(&chrono::Utc);
        query
            .push(" AND (created_at,id)>(")
            .push_bind(time)
            .push(",")
            .push_bind(cursor.id.into_uuid())
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
        let id = EffectId::from_uuid(row.try_get::<uuid::Uuid, _>("id")?);
        let receipt = decode(row.try_get("record")?)?;
        crate::effect_receipts::validate(&receipt, tenant, continuity, id)?;
        last = Some(EffectReceiptCursor {
            created_at: row
                .try_get::<chrono::DateTime<chrono::Utc>, _>("created_at")?
                .to_rfc3339(),
            id,
        });
        receipts.push(receipt);
    }
    Ok(EffectReceiptPage {
        receipts,
        next: if full { last } else { None },
    })
}
