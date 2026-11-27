use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use orch8_types::cron::{CronInventoryFilter, CronSchedule};
use orch8_types::error::StorageError;
use orch8_types::ids::{Namespace, SequenceId, TenantId};

use crate::AppState;
use crate::error::ApiError;

/// Reject unparseable IANA timezone names at the API boundary: the engine's
/// scheduler silently falls back to UTC for an invalid tz, so accepting a
/// typo like `Amercia/New_York` here would quietly re-time the schedule.
fn validate_timezone(tz: &str) -> Result<(), ApiError> {
    tz.parse::<chrono_tz::Tz>().map_err(|_| {
        ApiError::InvalidArgument(format!(
            "invalid timezone: '{tz}' (expected an IANA name like 'America/New_York' or 'UTC')"
        ))
    })?;
    Ok(())
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/cron", post(create_cron).get(list_cron))
        .route(
            "/cron/{id}",
            get(get_cron).put(update_cron).delete(delete_cron),
        )
        .route(
            "/cron/{id}/reviewed",
            axum::routing::put(update_reviewed_cron).delete(delete_reviewed_cron),
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
    view: Option<CronListView>,
    cursor: Option<Uuid>,
    namespace: Option<String>,
    sequence_id: Option<Uuid>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum CronListView {
    Page,
}

#[derive(Serialize, ToSchema)]
pub(crate) struct CronSchedulePage {
    items: Vec<CronSchedule>,
    next_cursor: Option<Uuid>,
    has_more: bool,
    filter: CronInventoryFilter,
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

    #[test]
    fn validate_timezone_rejects_typos_accepts_iana_names() {
        assert!(validate_timezone("UTC").is_ok());
        assert!(validate_timezone("America/New_York").is_ok());
        assert!(matches!(
            validate_timezone("Amercia/New_York"),
            Err(ApiError::InvalidArgument(_))
        ));
        assert!(matches!(
            validate_timezone(""),
            Err(ApiError::InvalidArgument(_))
        ));
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
    validate_timezone(&req.timezone)?;

    // Verify the target sequence exists and belongs to the caller's tenant.
    let sequence = state
        .storage
        .get_sequence(req.sequence_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {}", req.sequence_id.into_uuid())))?;
    if sequence.tenant_id != tenant_id {
        return Err(ApiError::Forbidden(
            "sequence does not belong to the caller's tenant".into(),
        ));
    }

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
        (status = 200, description = "Cron schedule found", body = CronSchedule,
            headers(("X-Orch8-Cron-Revision" = String, description = "Configuration revision when reviewed writes are supported"))),
        (status = 404, description = "Cron schedule not found"),
    )
)]
pub(crate) async fn get_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let mut headers = HeaderMap::new();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    let (schedule, reviewed) = match state.storage.review_cron_schedule(id).await {
        Ok(schedule) => (scope_cron(schedule, &tenant_ctx, id)?, true),
        Err(StorageError::Unsupported(_)) => (scoped_cron(&state, &tenant_ctx, id).await?, false),
        Err(error) => return Err(ApiError::from_storage(error, "cron_schedule")),
    };
    let schedule = schedule.ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;
    if reviewed {
        let revision = schedule
            .configuration_revision()
            .map_err(|_| ApiError::Internal("Could not fingerprint cron configuration".into()))?;
        headers.insert(
            "x-orch8-cron-revision",
            HeaderValue::from_str(&revision)
                .map_err(|_| ApiError::Internal("Invalid cron configuration revision".into()))?,
        );
    }
    Ok((headers, Json(schedule)))
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

#[derive(Serialize, ToSchema)]
#[serde(untagged)]
enum CronScheduleList {
    Schedules(Vec<CronSchedule>),
    Page(CronSchedulePage),
}

#[utoipa::path(get, path = "/cron", tag = "cron",
    params(
        ("tenant_id" = Option<String>, Query, description = "Filter by tenant"),
        ("limit" = Option<u32>, Query, description = "Limit, default 100. Page view clamps to 1–200."),
        ("view" = Option<String>, Query, description = "Use page for a live window in descending UUID order."),
        ("cursor" = Option<Uuid>, Query, description = "Use the previous page's next_cursor."),
        ("namespace" = Option<String>, Query, description = "Exact namespace in the page view."),
        ("sequence_id" = Option<Uuid>, Query, description = "Exact stored sequence UUID in the page view."),
        ("enabled" = Option<bool>, Query, description = "Filter by enabled state in the page view."),
    ),
    responses((status = 200, description = "Cron schedules or, with view=page, a CronSchedulePage", body = inline(CronScheduleList)))
)]
pub(crate) async fn list_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(q): Query<ListCronQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, q.tenant_id.as_deref());
    let filter = CronInventoryFilter {
        namespace: q.namespace,
        sequence_id: q.sequence_id,
        enabled: q.enabled,
    };
    if q.view.is_some() {
        let limit = q.limit.clamp(1, 200);
        let mut items = state
            .storage
            .list_cron_schedules_page(tenant.as_ref(), q.cursor, &filter, limit + 1)
            .await
            .map_err(|e| ApiError::from_storage(e, "cron_schedules"))?;
        let has_more = items.len() > limit as usize;
        items.truncate(limit as usize);
        let next_cursor = items.last().filter(|_| has_more).map(|item| item.id);
        return Ok(Json(CronScheduleList::Page(CronSchedulePage {
            items,
            next_cursor,
            has_more,
            filter,
        })));
    }
    if q.cursor.is_some() || !filter.is_empty() {
        return Err(ApiError::InvalidArgument(
            "cron cursor and inventory filters require view=page".into(),
        ));
    }
    let schedules = state
        .storage
        .list_cron_schedules(tenant.as_ref(), q.limit)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedules"))?;

    Ok(Json(CronScheduleList::Schedules(schedules)))
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
    let mut schedule = scoped_cron(&state, &tenant_ctx, id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;
    apply_cron_update(&mut schedule, req)?;

    state
        .storage
        .update_cron_schedule(&schedule)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(Json(schedule))
}
fn apply_cron_update(schedule: &mut CronSchedule, req: UpdateCronRequest) -> Result<(), ApiError> {
    if let Some(expr) = req.cron_expr {
        orch8_engine::cron::validate_cron_expr(&expr)
            .map_err(|e| ApiError::InvalidArgument(format!("invalid cron expression: {e}")))?;
        schedule.cron_expr = expr;
    }
    if let Some(tz) = req.timezone {
        validate_timezone(&tz)?;
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

    schedule.next_fire_at = orch8_engine::cron::calculate_next_fire(schedule);

    Ok(())
}

fn scope_cron(
    schedule: Option<CronSchedule>,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<Option<CronSchedule>, ApiError> {
    if let Some(value) = &schedule {
        crate::auth::enforce_tenant_access(
            tenant_ctx,
            &value.tenant_id,
            &format!("cron_schedule {id}"),
        )?;
    }
    Ok(schedule)
}

async fn scoped_cron(
    state: &AppState,
    tenant_ctx: &crate::auth::OptionalTenant,
    id: Uuid,
) -> Result<Option<CronSchedule>, ApiError> {
    scope_cron(
        state
            .storage
            .get_cron_schedule(id)
            .await
            .map_err(|error| ApiError::from_storage(error, "cron_schedule"))?,
        tenant_ctx,
        id,
    )
}

fn cron_expected_revision(headers: &HeaderMap) -> Result<&str, ApiError> {
    let mut values = headers.get_all("x-orch8-cron-if-revision").iter();
    if let Some(value) = values.next().and_then(|value| value.to_str().ok())
        && values.next().is_none()
        && value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Ok(value);
    }
    Err(ApiError::InvalidArgument(
        "One valid reviewed cron revision is required".into(),
    ))
}

fn cron_guard(mut response: Response, outcome: &'static str) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert("x-orch8-cron-guard", HeaderValue::from_static(outcome));
    response
}

fn stale_cron_review() -> Response {
    cron_guard(
        ApiError::Conflict("Cron configuration changed. Inspect the schedule again.".into())
            .into_response(),
        "stale",
    )
}

#[utoipa::path(put, path = "/cron/{id}/reviewed", tag = "cron",
    params(
        ("id" = Uuid, Path, description = "Cron schedule ID"),
        ("X-Orch8-Cron-If-Revision" = String, Header, description = "Reviewed configuration revision"),
    ),
    request_body = UpdateCronRequest,
    responses(
        (status = 200, description = "Reviewed cron configuration updated", body = CronSchedule,
            headers(("X-Orch8-Cron-Guard" = String, description = "Applied when the reviewed configuration matched"))),
        (status = 400, description = "Invalid revision or configuration"),
        (status = 409, description = "The reviewed configuration changed",
            headers(("X-Orch8-Cron-Guard" = String, description = "Stale when the comparison rejected the write")))
    )
)]
pub(crate) async fn update_reviewed_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(req): Json<UpdateCronRequest>,
) -> Result<Response, ApiError> {
    let revision = cron_expected_revision(&headers)?;
    let Some(mut schedule) = scoped_cron(&state, &tenant_ctx, id).await? else {
        return Ok(stale_cron_review());
    };
    apply_cron_update(&mut schedule, req)?;
    let updated = state
        .storage
        .update_reviewed_cron_schedule(&schedule, revision)
        .await
        .map_err(|error| ApiError::from_storage(error, "cron_schedule"))?;
    Ok(match updated {
        Some(row) => cron_guard(Json(row).into_response(), "applied"),
        None => stale_cron_review(),
    })
}

#[utoipa::path(delete, path = "/cron/{id}/reviewed", tag = "cron",
    params(
        ("id" = Uuid, Path, description = "Cron schedule ID"),
        ("X-Orch8-Cron-If-Revision" = String, Header, description = "Reviewed configuration revision"),
    ),
    responses(
        (status = 204, description = "Reviewed cron configuration removed",
            headers(("X-Orch8-Cron-Guard" = String, description = "Applied when the reviewed configuration matched"))),
        (status = 400, description = "Invalid revision"),
        (status = 409, description = "The reviewed configuration changed",
            headers(("X-Orch8-Cron-Guard" = String, description = "Stale when the comparison rejected the write")))
    )
)]
pub(crate) async fn delete_reviewed_cron(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let revision = cron_expected_revision(&headers)?;
    let Some(schedule) = scoped_cron(&state, &tenant_ctx, id).await? else {
        return Ok(stale_cron_review());
    };
    let removed = state
        .storage
        .delete_reviewed_cron_schedule(id, &schedule.tenant_id, revision)
        .await
        .map_err(|error| ApiError::from_storage(error, "cron_schedule"))?;
    Ok(if removed {
        cron_guard(StatusCode::NO_CONTENT.into_response(), "applied")
    } else {
        stale_cron_review()
    })
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
    scoped_cron(&state, &tenant_ctx, id)
        .await?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    state
        .storage
        .delete_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(StatusCode::NO_CONTENT)
}
