use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use uuid::Uuid;

use orch8_types::cluster::ClusterNode;

use crate::error::ApiError;
use crate::AppState;

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
) -> Result<impl IntoResponse, ApiError> {
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
    Path(id): Path<Uuid>,
) -> Result<impl IntoResponse, ApiError> {
    state
        .storage
        .drain_node(id)
        .await
        .map_err(|e| ApiError::from_storage(e, "cluster_node"))?;
    Ok(StatusCode::OK)
}
