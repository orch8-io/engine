use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use orch8_storage::{MobileApprovalRequest, MobileCommand, MobileDevice, MobileInstanceStatus};

use crate::error::ApiError;
use crate::AppState;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/mobile/sync", post(handle_sync))
        .route("/mobile/devices/register", post(register_device))
        .route("/mobile/devices", get(list_devices))
        .route("/mobile/approvals", get(list_approvals))
        .route("/mobile/approvals/{id}/resolve", post(resolve_approval))
        .route("/mobile/status", get(list_status))
        .route("/mobile/commands", post(create_command))
}

// ---------------------------------------------------------------------------
// POST /mobile/sync
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct SyncRequest {
    device_id: String,
    #[serde(default)]
    status_updates: Vec<StatusUpdatePayload>,
    #[serde(default)]
    approval_requests: Vec<ApprovalRequestPayload>,
    #[serde(default)]
    step_delegations: Vec<StepDelegationPayload>,
    #[serde(default)]
    command_acks: Vec<String>,
}

#[derive(Deserialize)]
struct StatusUpdatePayload {
    instance_id: String,
    sequence_name: Option<String>,
    state: String,
    current_step: Option<String>,
    handler: Option<String>,
    timestamp: String,
    context_summary: Option<serde_json::Value>,
    steps: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ApprovalRequestPayload {
    instance_id: String,
    block_id: String,
    sequence_name: Option<String>,
    prompt: Option<String>,
    choices: Option<serde_json::Value>,
    store_as: Option<String>,
    timeout_seconds: Option<i64>,
    metadata: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct StepDelegationPayload {
    request_id: String,
    instance_id: String,
    block_id: String,
    handler: String,
    params: serde_json::Value,
}

#[derive(Serialize)]
struct SyncResponse {
    commands: Vec<CommandPayload>,
    sync_interval_secs: u32,
}

#[derive(Serialize)]
struct CommandPayload {
    id: String,
    #[serde(rename = "type")]
    command_type: String,
    payload: serde_json::Value,
}

#[allow(clippy::too_many_lines)]
async fn handle_sync(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<SyncRequest>,
) -> Result<impl IntoResponse, ApiError> {
    const MAX_SYNC_ITEMS_PER_ARRAY: usize = 500;

    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .unwrap_or_default();
    if req.status_updates.len() > MAX_SYNC_ITEMS_PER_ARRAY
        || req.approval_requests.len() > MAX_SYNC_ITEMS_PER_ARRAY
        || req.step_delegations.len() > MAX_SYNC_ITEMS_PER_ARRAY
        || req.command_acks.len() > MAX_SYNC_ITEMS_PER_ARRAY
    {
        return Err(ApiError::InvalidArgument(format!(
            "sync arrays must each contain at most {MAX_SYNC_ITEMS_PER_ARRAY} items"
        )));
    }

    let storage = &state.storage;

    for update in &req.status_updates {
        let status = MobileInstanceStatus {
            device_id: req.device_id.clone(),
            instance_id: update.instance_id.clone(),
            sequence_name: update.sequence_name.clone(),
            state: update.state.clone(),
            current_step: update.current_step.clone(),
            handler: update.handler.clone(),
            context_summary: update
                .context_summary
                .as_ref()
                .map(std::string::ToString::to_string),
            steps: update.steps.as_ref().map(std::string::ToString::to_string),
            updated_at: update.timestamp.clone(),
        };
        storage
            .upsert_mobile_instance_status(&status)
            .await
            .map_err(|e| ApiError::from_storage(e, "mobile_instance_status"))?;
    }

    for approval in &req.approval_requests {
        let approval_req = MobileApprovalRequest {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: req.device_id.clone(),
            tenant_id: tenant_id.clone(),
            instance_id: approval.instance_id.clone(),
            block_id: approval.block_id.clone(),
            sequence_name: approval.sequence_name.clone(),
            prompt: approval.prompt.clone(),
            choices: approval
                .choices
                .as_ref()
                .map(std::string::ToString::to_string),
            store_as: approval.store_as.clone(),
            timeout_secs: approval.timeout_seconds,
            metadata: approval
                .metadata
                .as_ref()
                .map(std::string::ToString::to_string),
            state: "pending".into(),
            resolution: None,
            created_at: String::new(),
            resolved_at: None,
        };
        storage
            .insert_mobile_approval(&approval_req)
            .await
            .map_err(|e| ApiError::from_storage(e, "mobile_approval_requests"))?;
    }

    // Process step delegations — device asks server to resolve credentials
    // and execute steps that require secrets. Results come back as commands.
    for delegation in &req.step_delegations {
        let mut params = delegation.params.clone();
        // Resolve credentials:// references in the params
        if let Err(e) =
            orch8_engine::credentials::resolve_in_value(storage.as_ref(), &tenant_id, &mut params)
                .await
        {
            warn!(
                request_id = %delegation.request_id,
                error = %e,
                "credential resolution failed for delegated step"
            );
            let error_cmd = MobileCommand {
                id: uuid::Uuid::new_v4().to_string(),
                device_id: req.device_id.clone(),
                command_type: "step_result".into(),
                payload: serde_json::json!({
                    "request_id": delegation.request_id,
                    "instance_id": delegation.instance_id,
                    "block_id": delegation.block_id,
                    "success": false,
                    "error": format!("credential resolution failed: {e}"),
                })
                .to_string(),
                created_at: String::new(),
                acked_at: None,
            };
            storage
                .create_mobile_command(&error_cmd)
                .await
                .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;
            continue;
        }

        // Create a step_result command with the resolved params.
        // The mobile device will execute the step handler with these
        // resolved params (secrets are only in transit, never stored on device).
        let result_cmd = MobileCommand {
            id: uuid::Uuid::new_v4().to_string(),
            device_id: req.device_id.clone(),
            command_type: "step_result".into(),
            payload: serde_json::json!({
                "request_id": delegation.request_id,
                "instance_id": delegation.instance_id,
                "block_id": delegation.block_id,
                "handler": delegation.handler,
                "resolved_params": params,
                "success": true,
            })
            .to_string(),
            created_at: String::new(),
            acked_at: None,
        };
        storage
            .create_mobile_command(&result_cmd)
            .await
            .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;
    }

    if !req.command_acks.is_empty() {
        storage
            .ack_mobile_commands(&req.device_id, &req.command_acks)
            .await
            .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;
    }

    let pending = storage
        .fetch_pending_commands(&req.device_id, 50)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;

    storage
        .update_device_last_sync(&req.device_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_devices"))?;

    let sync_interval_secs = if pending.is_empty() { 30 } else { 5 };

    let commands: Vec<CommandPayload> = pending
        .into_iter()
        .map(|cmd| CommandPayload {
            id: cmd.id,
            command_type: cmd.command_type,
            payload: serde_json::from_str(&cmd.payload).unwrap_or(serde_json::Value::Null),
        })
        .collect();

    debug!(
        device_id = %req.device_id,
        status_count = req.status_updates.len(),
        approval_count = req.approval_requests.len(),
        commands_delivered = commands.len(),
        "mobile sync"
    );

    Ok(Json(SyncResponse {
        commands,
        sync_interval_secs,
    }))
}

// ---------------------------------------------------------------------------
// POST /mobile/devices/register
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RegisterDeviceRequest {
    device_id: String,
    push_token: Option<String>,
    platform: String,
    app_version: Option<String>,
}

async fn register_device(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<RegisterDeviceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .unwrap_or_default();

    let device = MobileDevice {
        device_id: req.device_id.clone(),
        tenant_id,
        push_token: req.push_token,
        platform: req.platform,
        app_version: req.app_version,
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    };

    state
        .storage
        .register_mobile_device(&device)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_devices"))?;

    debug!(device_id = %req.device_id, "mobile device registered");
    Ok(StatusCode::CREATED)
}

// ---------------------------------------------------------------------------
// GET /mobile/devices
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListDevicesQuery {
    tenant_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

async fn list_devices(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(params): Query<ListDevicesQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .or(params.tenant_id);

    let items = state
        .storage
        .list_mobile_devices(tenant_id.as_deref(), params.limit.min(500))
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_devices"))?;

    Ok(Json(serde_json::json!({
        "items": items,
        "total": items.len(),
    })))
}

// ---------------------------------------------------------------------------
// GET /mobile/approvals
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListApprovalsQuery {
    tenant_id: Option<String>,
    state: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

const fn default_limit() -> u32 {
    100
}

async fn list_approvals(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(params): Query<ListApprovalsQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .or(params.tenant_id);

    let items = state
        .storage
        .list_mobile_approvals(
            tenant_id.as_deref(),
            params.state.as_deref(),
            params.limit.min(500),
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_approval_requests"))?;

    Ok(Json(serde_json::json!({
        "items": items,
        "total": items.len(),
    })))
}

// ---------------------------------------------------------------------------
// POST /mobile/approvals/{id}/resolve
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ResolveApprovalRequest {
    output: serde_json::Value,
}

async fn resolve_approval(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<String>,
    Json(req): Json<ResolveApprovalRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .unwrap_or_default();

    // Fetch the approval first (read-only) to verify existence and tenant
    // ownership BEFORE performing the irreversible DB mutation. This prevents
    // a cross-tenant attacker from resolving approvals they don't own.
    let approval = state
        .storage
        .get_mobile_approval(&id)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_approval_requests"))?;

    let Some(ref approval) = approval else {
        return Err(ApiError::NotFound(format!(
            "approval {id} not found or already resolved"
        )));
    };

    if approval.state != "pending" {
        return Err(ApiError::NotFound(format!(
            "approval {id} not found or already resolved"
        )));
    }

    if !tenant_id.is_empty() && approval.tenant_id != tenant_id {
        return Err(ApiError::NotFound(format!(
            "approval {id} not found or already resolved"
        )));
    }

    // Tenant ownership verified — now perform the DB mutation.
    let resolution = req.output.to_string();

    let approval = state
        .storage
        .resolve_mobile_approval(&id, &resolution)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_approval_requests"))?;

    let Some(approval) = approval else {
        // Could happen if the approval was concurrently resolved between
        // the get and the update — treat as already resolved.
        return Err(ApiError::NotFound(format!(
            "approval {id} not found or already resolved"
        )));
    };

    let command_payload = serde_json::json!({
        "instance_id": approval.instance_id,
        "block_id": approval.block_id,
        "step_name": approval.block_id,
        "output": req.output,
    });

    let command = MobileCommand {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: approval.device_id.clone(),
        command_type: "complete_step".into(),
        payload: command_payload.to_string(),
        created_at: String::new(),
        acked_at: None,
    };

    state
        .storage
        .create_mobile_command(&command)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;

    // Fire-and-forget silent push
    let device = state
        .storage
        .get_mobile_device(&approval.device_id)
        .await
        .ok()
        .flatten();

    if let Some(device) = device {
        if let Some(token) = device.push_token {
            let push = state.push_provider.clone();
            let platform = device.platform.clone();
            tokio::spawn(async move {
                if let Err(e) = push.send_silent_push(&token, &platform).await {
                    warn!(device_id = %device.device_id, error = %e, "push notification failed");
                }
            });
        }
    }

    debug!(approval_id = %id, device_id = %approval.device_id, "approval resolved");
    Ok(StatusCode::OK)
}

// ---------------------------------------------------------------------------
// GET /mobile/status
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ListStatusQuery {
    tenant_id: Option<String>,
    device_id: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
}

async fn list_status(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(params): Query<ListStatusQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .or(params.tenant_id);

    let items = state
        .storage
        .list_mobile_instance_status(
            tenant_id.as_deref(),
            params.device_id.as_deref(),
            params.limit.min(500),
        )
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_instance_status"))?;

    Ok(Json(serde_json::json!({
        "items": items,
        "total": items.len(),
    })))
}

// ---------------------------------------------------------------------------
// POST /mobile/commands
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct CreateCommandRequest {
    device_id: String,
    command_type: String,
    payload: serde_json::Value,
}

async fn create_command(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Json(req): Json<CreateCommandRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let tenant_id = tenant_ctx
        .as_ref()
        .map(|axum::Extension(ctx)| ctx.tenant_id.to_string())
        .unwrap_or_default();

    // Verify the caller owns the target device BEFORE enqueuing anything —
    // without this check any tenant could inject commands (including
    // `complete_step`) into another tenant's device queue. Same fall-open
    // convention as `resolve_approval`: an empty tenant (insecure mode /
    // root key without X-Tenant-Id) skips the scope check.
    if !tenant_id.is_empty() {
        let device = state
            .storage
            .get_mobile_device(&req.device_id)
            .await
            .map_err(|e| ApiError::from_storage(e, "mobile_devices"))?;
        let owned = device.is_some_and(|d| d.tenant_id == tenant_id);
        if !owned {
            return Err(ApiError::NotFound(format!(
                "device {} not found",
                req.device_id
            )));
        }
    }

    let command = MobileCommand {
        id: uuid::Uuid::new_v4().to_string(),
        device_id: req.device_id.clone(),
        command_type: req.command_type,
        payload: req.payload.to_string(),
        created_at: String::new(),
        acked_at: None,
    };

    state
        .storage
        .create_mobile_command(&command)
        .await
        .map_err(|e| ApiError::from_storage(e, "mobile_commands"))?;

    // Fire-and-forget silent push
    let device = state
        .storage
        .get_mobile_device(&req.device_id)
        .await
        .ok()
        .flatten();

    if let Some(device) = device {
        if let Some(token) = device.push_token {
            let push = state.push_provider.clone();
            let platform = device.platform.clone();
            tokio::spawn(async move {
                if let Err(e) = push.send_silent_push(&token, &platform).await {
                    warn!(device_id = %device.device_id, error = %e, "push notification failed");
                }
            });
        }
    }

    Ok(StatusCode::CREATED)
}
