use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::ids::{Namespace, SequenceId, TenantId};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/cron", post(create_cron).get(list_cron))
        .route("/cron/{id}", get(get_cron).put(update_cron).delete(delete_cron))
}

#[derive(Deserialize)]
struct CreateCronRequest {
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
}

fn default_tz() -> String {
    "UTC".into()
}

fn default_true() -> bool {
    true
}

#[derive(Deserialize)]
struct UpdateCronRequest {
    cron_expr: Option<String>,
    timezone: Option<String>,
    enabled: Option<bool>,
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ListCronQuery {
    tenant_id: Option<String>,
}

async fn create_cron(
    State(state): State<AppState>,
    Json(req): Json<CreateCronRequest>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate cron expression.
    orch8_engine::cron::validate_cron_expr(&req.cron_expr)
        .map_err(|e| ApiError::InvalidArgument(format!("invalid cron expression: {e}")))?;

    let now = Utc::now();
    let id = Uuid::new_v4();

    // Calculate initial next_fire_at.
    let schedule = CronSchedule {
        id,
        tenant_id: req.tenant_id,
        namespace: req.namespace,
        sequence_id: req.sequence_id,
        cron_expr: req.cron_expr,
        timezone: req.timezone,
        enabled: req.enabled,
        metadata: req.metadata,
        last_triggered_at: None,
        next_fire_at: None,
        created_at: now,
        updated_at: now,
    };

    // Compute next fire time from the cron expression.
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

async fn get_cron(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    let schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

    Ok(Json(schedule))
}

async fn list_cron(
    State(state): State<AppState>,
    Query(q): Query<ListCronQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant = q.tenant_id.map(TenantId);
    let schedules = state
        .storage
        .list_cron_schedules(tenant.as_ref())
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedules"))?;

    Ok(Json(schedules))
}

async fn update_cron(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(req): Json<UpdateCronRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let mut schedule = state
        .storage
        .get_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?
        .ok_or_else(|| ApiError::NotFound(format!("cron_schedule {id}")))?;

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

    // Recalculate next fire time.
    schedule.next_fire_at = orch8_engine::cron::calculate_next_fire(&schedule);

    state
        .storage
        .update_cron_schedule(&schedule)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(Json(schedule))
}

async fn delete_cron(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .delete_cron_schedule(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cron_schedule"))?;

    Ok(StatusCode::NO_CONTENT)
}
