use axum::Extension;
use axum::extract::{Json, Path, State};
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::ToSchema;

use crate::{
    AppState,
    auth::{OptionalAdmin, OptionalTenant, PrincipalContext, principal_is_operator},
    error::ApiError,
};

#[derive(Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewedTriggerRemoval {
    expected_created_at: DateTime<Utc>,
    expected_updated_at: DateTime<Utc>,
}

#[utoipa::path(delete, path = "/triggers/{slug}/reviewed", tag = "triggers",
    params(("slug" = String, Path, description = "Exact trigger slug")),
    request_body = ReviewedTriggerRemoval,
    responses(
        (status = 204, description = "Reviewed trigger removed"),
        (status = 403, description = "Operator capability required"),
        (status = 404, description = "Trigger not found in the selected tenant"),
        (status = 409, description = "Trigger was edited or replaced"),
    )
)]
pub(crate) async fn remove_reviewed_trigger(
    State(state): State<AppState>,
    tenant: OptionalTenant,
    admin: OptionalAdmin,
    principal: Option<Extension<PrincipalContext>>,
    Path(slug): Path<String>,
    Json(review): Json<ReviewedTriggerRemoval>,
) -> Result<StatusCode, ApiError> {
    if admin.is_none() && !principal_is_operator(principal.as_ref().map(|principal| &principal.0)) {
        return Err(ApiError::Forbidden(
            "trigger removal requires Operator access".into(),
        ));
    }
    let Some(Extension(tenant)) = tenant else {
        return Err(ApiError::InvalidArgument(
            "trigger removal requires a tenant".into(),
        ));
    };
    let trigger = state
        .storage
        .get_trigger(Some(&tenant.tenant_id), &slug)
        .await
        .map_err(|error| ApiError::from_storage(error, "trigger"))?
        .filter(|trigger| trigger.tenant_id == tenant.tenant_id && trigger.slug == slug)
        .ok_or_else(|| ApiError::NotFound("trigger".into()))?;
    if trigger.created_at != review.expected_created_at
        || trigger.updated_at != review.expected_updated_at
        || !state
            .storage
            .delete_trigger_cas(&trigger)
            .await
            .map_err(|error| ApiError::from_storage(error, "trigger"))?
    {
        return Err(ApiError::Conflict(
            "trigger changed; inspect it again before removal".into(),
        ));
    }
    Ok(StatusCode::NO_CONTENT)
}
