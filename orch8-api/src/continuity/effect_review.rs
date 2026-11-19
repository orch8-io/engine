use super::*;
use chrono::DateTime;
use orch8_types::continuity::EffectId;
use utoipa::ToSchema;

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EffectReviewQueue {
    items: Vec<EffectReviewItem>,
    has_more: bool,
    next_cursor: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub(crate) struct EffectReviewItem {
    id: orch8_types::continuity::EffectId,
    instance_id: InstanceId,
    continuity_id: ContinuityId,
    block_id: String,
    kind: orch8_types::continuity::EffectKind,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ReviewQuery {
    tenant_id: String,
    after: Option<String>,
}

fn parse_cursor(value: &str) -> Result<(DateTime<Utc>, EffectId), ApiError> {
    let invalid = || ApiError::InvalidArgument("invalid effect review cursor".into());
    let (timestamp, id) = value.split_once(',').ok_or_else(invalid)?;
    let timestamp = timestamp.parse().map_err(|_| invalid())?;
    let id = uuid::Uuid::parse_str(id)
        .map(EffectId::from_uuid)
        .map_err(|_| invalid())?;
    Ok((timestamp, id))
}

#[utoipa::path(get, path = "/continuity/effects/review", tag = "continuity",
    params(("tenant_id" = String, Query, description = "Tenant whose unknown effects need review"),
        ("after" = Option<String>, Query, description = "Continuation cursor returned by the preceding page")),
    responses((status = 200, description = "Oldest unknown effects first", body = EffectReviewQueue))
)]
pub(crate) async fn list_review(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ReviewQuery>,
) -> Result<Json<EffectReviewQueue>, ApiError> {
    let tenant_id = query_tenant(&tenant_ctx, &query.tenant_id)?;
    let after = query.after.as_deref().map(parse_cursor).transpose()?;
    let items = state
        .storage
        .list_unknown_effect_receipts(&tenant_id, 101, after)
        .await
        .map_err(|error| ApiError::from_storage(error, "effect review queue"))?;
    let has_more = items.len() > 100;
    let next_cursor = items
        .get(99)
        .filter(|_| has_more)
        .map(|effect| format!("{},{}", effect.updated_at.to_rfc3339(), effect.id));
    let items = items
        .into_iter()
        .take(100)
        .map(|effect| EffectReviewItem {
            id: effect.id,
            instance_id: effect.instance_id,
            continuity_id: effect.continuity_id,
            block_id: effect.block_id.to_string(),
            kind: effect.kind,
            updated_at: effect.updated_at,
        })
        .collect();
    Ok(Json(EffectReviewQueue {
        items,
        has_more,
        next_cursor,
    }))
}
