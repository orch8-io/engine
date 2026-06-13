use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::ids::{Namespace, SequenceId, TenantId};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/cron", post(create_cron).get(list_cron))
        .route(
            "/cron/{id}",
            get(get_cron).put(update_cron).delete(delete_cron),
        )
        .route("/cron/{id}/next-fires", get(next_fires))
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct CreateCronRequest {
    tenant_id: TenantId,
    namespace: Namespace,
    sequence_id: SequenceId,
    cron_expr: String,
    #[serde(default = "default_tz")]
    timezone: String,
    #[serde(default)]
    metadata: serde_json::Value,
    #[serde(default = "default_true")]
    enabled: bool,
    /// Behavior when a fire is due while a previous run is still active:
    /// `allow` (default), `skip`, `buffer_one`, `cancel_previous`.
    #[serde(default)]
    overlap_policy: orch8_types::cron::OverlapPolicy,
}

fn default_tz() -> String {
    "UTC".into()
}

const fn default_true() -> bool {
    true
}

#[derive(Deserialize, ToSchema)]
pub(crate) struct UpdateCronRequest {
    cron_expr: Option<String>,
    timezone: Option<String>,
    enabled: Option<bool>,
    metadata: Option<serde_json::Value>,
    overlap_policy: Option<orch8_types::cron::OverlapPolicy>,
}

#[derive(Deserialize)]
pub(crate) struct ListCronQuery {
    tenant_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

fn default_limit() -> u32 {
    100
}

#[derive(Deserialize)]
pub(crate) struct NextFiresQuery {
    #[serde(default = "default_next_fires_n")]
    n: u32,
}

fn default_next_fires_n() -> u32 {
    5
}

/// Iteratively compute the next `n` fire instants (UTC) for a schedule,
/// starting strictly after `now`. Each instant is fed back as the new "after"
/// bound, so the DST-correct [`orch8_engine::cron::calculate_next_fire_after`] governs every step —
/// gap occurrences clamp, ambiguous occurrences fire once. Stops early if the
/// expression has no further occurrences.
fn compute_next_fires(
    schedule: &CronSchedule,
    now: chrono::DateTime<Utc>,
    n: u32,
) -> Vec<chrono::DateTime<Utc>> {
    let mut out = Vec::with_capacity(n as usize);
    let mut after = now;
    for _ in 0..n {
        let Some(next) = orch8_engine::cron::calculate_next_fire_after(schedule, after) else {
            break;
        };
        // Defensive: a non-advancing result would loop forever. `.after()` is
        // exclusive so this should never trip, but guard anyway.
        if next <= after {
            break;
        }
        out.push(next);
        after = next;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_tz_is_utc() {
        assert_eq!(default_tz(), "UTC");
    }

    #[test]
    fn default_true_is_true() {
        assert!(default_true());
    }
}

#[utoipa::path(post, path = "/cron", tag = "cron",
    request_body = CreateCronRequest,
    responses(
        (status = 201, description = "Cron schedule created", body = serde_json::Value),
        (status = 400, description = "Invalid cron expression"),
    )
)]
pub(crate) async fn create_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreateCronRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = crate::auth::enforce_tenant_create(&tenant_ctx, &req.tenant_id)?;

    orch8_engine::cron::validate_cron_expr(&req.cron_expr)
        .map_err(|e| ApiError::InvalidArgument(format!("invalid cron expression: {e}")))?;

    let now = Utc::now();
    let id = Uuid::now_v7();

    let schedule = CronSchedule {
        id,
        tenant_id,
        namespace: req.namespace,
        sequence_id: req.sequence_id,
        cron_expr: req.cron_expr,
        timezone: req.timezone,
        enabled: req.enabled,
        metadata: req.metadata,
        overlap_policy: req.overlap_policy,
        skipped_fires: 0,
        last_skipped_at: None,
        last_triggered_at: None,
        next_fire_at: None,
        created_at: now,
        updated_at: now,
    };

    let next_fire = orch8_engine::cron::calculate_next_fire(&schedule);
    let schedule = CronSchedule {
        next_fire_at: next_fire,
        ..schedule
    };

    state
        .storage
        .create_cron_schedule(&schedule)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": id,
            "next_fire_at": schedule.next_fire_at,
        })),
    ))
}

#[utoipa::path(get, path = "/cron/{id}", tag = "cron",
    params(("id" = Uuid, Path, description = "Cron schedule ID")),
    responses(
        (status = 200, description = "Cron schedule found", body = CronSchedule),
        (status = 404, description = "Cron schedule not found"),
    )
)]
pub(crate) async fn get_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &schedule.tenant_id,
        &format!("cron_schedule {id}"),
    )?;

    Ok(Json(schedule))
}

#[utoipa::path(get, path = "/cron/{id}/next-fires", tag = "cron",
    params(
        ("id" = Uuid, Path, description = "Cron schedule ID"),
        ("n" = Option<u32>, Query, description = "How many upcoming fires to preview (default 5, max 50)"),
    ),
    responses(
        (status = 200, description = "Upcoming fire instants (UTC, DST-correct)", body = serde_json::Value),
        (status = 404, description = "Cron schedule not found"),
    )
)]
pub(crate) async fn next_fires(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(q): Query<NextFiresQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &schedule.tenant_id,
        &format!("cron_schedule {id}"),
    )?;

    let n = q.n.clamp(1, 50);
    let fires = compute_next_fires(&schedule, Utc::now(), n);

    Ok(Json(serde_json::json!({
        "timezone": schedule.timezone,
        "fires": fires,
    })))
}

#[utoipa::path(get, path = "/cron", tag = "cron",
    params(("tenant_id" = Option<String>, Query, description = "Filter by tenant")),
    responses((status = 200, description = "List of cron schedules", body = Vec<CronSchedule>))
)]
pub(crate) async fn list_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListCronQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let schedules = state
        .storage
        .list_cron_schedules(tenant.as_ref(), q.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedules"))?;

    Ok(Json(schedules))
}

#[utoipa::path(put, path = "/cron/{id}", tag = "cron",
    params(("id" = Uuid, Path, description = "Cron schedule ID")),
    request_body = UpdateCronRequest,
    responses(
        (status = 200, description = "Cron schedule updated", body = CronSchedule),
        (status = 400, description = "Invalid cron expression"),
        (status = 404, description = "Cron schedule not found"),
    )
)]
pub(crate) async fn update_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateCronRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &schedule.tenant_id,
        &format!("cron_schedule {id}"),
    )?;

    if let Some(expr) = req.cron_expr {
        orch8_engine::cron::validate_cron_expr(&expr)
            .map_err(|e| ApiError::InvalidArgument(format!("invalid cron expression: {e}")))?;
        schedule.cron_expr = expr;
    }
    if let Some(tz) = req.timezone {
        schedule.timezone = tz;
    }
    if let Some(enabled) = req.enabled {
        schedule.enabled = enabled;
    }
    if let Some(metadata) = req.metadata {
        schedule.metadata = metadata;
    }
    if let Some(policy) = req.overlap_policy {
        schedule.overlap_policy = policy;
    }

    schedule.next_fire_at = orch8_engine::cron::calculate_next_fire(&schedule);

    state
        .storage
        .update_cron_schedule(&schedule)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(Json(schedule))
}

#[utoipa::path(delete, path = "/cron/{id}", tag = "cron",
    params(("id" = Uuid, Path, description = "Cron schedule ID")),
    responses((status = 204, description = "Cron schedule deleted"))
)]
pub(crate) async fn delete_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &schedule.tenant_id,
        &format!("cron_schedule {id}"),
    )?;

    state
        .storage
        .delete_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(StatusCode::NO_CONTENT)
}
