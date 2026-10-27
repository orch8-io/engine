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
