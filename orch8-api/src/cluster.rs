use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use orch8_types::cluster::ClusterNode;

use crate::AppState;
use crate::api_keys::require_admin;
use crate::auth::OptionalAdmin;
use crate::error::ApiError;

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/cluster/nodes", get(list_nodes))
        .route("/cluster/nodes/{id}/drain", post(drain_node))
}

#[utoipa::path(
    get,
    path = "/cluster/nodes",
    responses((status = 200, body = Vec<ClusterNode>))
)]
pub(crate) async fn list_nodes(
    State(state): State<AppState>,
    admin: OptionalAdmin,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    let nodes = state
        .storage
        .list_nodes()
        .await
        .map_err(|e| ApiError::from_storage(e, "cluster_node"))?;
    Ok(Json(nodes))
}

#[utoipa::path(
    post,
    path = "/cluster/nodes/{id}/drain",
    params(("id" = Uuid, Path, description = "Node ID")),
    responses((status = 200, description = "Drain signal sent"))
)]
pub(crate) async fn drain_node(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    require_admin(&admin)?;
    state
        .storage
        .drain_node(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cluster_node"))?;
    Ok(StatusCode::OK)
}
