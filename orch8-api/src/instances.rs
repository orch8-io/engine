//! Instance HTTP routes: lifecycle, signals, outputs, bulk ops, checkpoints,
//! audit, and dynamic step injection.

use axum::routing::{get, patch, post};
use axum::Router;

use crate::AppState;

mod audit;
mod bulk;
mod checkpoints;
mod inject;
mod lifecycle;
mod outputs;
mod signals;
mod types;

// Re-exports for `crate::instances::*` compatibility (openapi.rs references
// handlers and public request types by this path). utoipa's derive macro
// also looks for `__path_<fn>` structs next to each handler, so those must
// be re-exported alongside the function.
pub(crate) use audit::{__path_list_audit_log, list_audit_log};
pub(crate) use bulk::{
    __path_bulk_reschedule, __path_bulk_update_state, __path_list_dlq, bulk_reschedule,
    bulk_update_state, list_dlq,
};
pub(crate) use checkpoints::{
    __path_get_latest_checkpoint, __path_list_checkpoints, __path_prune_checkpoints,
    __path_save_checkpoint, get_latest_checkpoint, list_checkpoints, prune_checkpoints,
    save_checkpoint,
};
pub use checkpoints::{PruneCheckpointsRequest, SaveCheckpointRequest};
pub use inject::InjectBlocksRequest;
pub(crate) use inject::{__path_inject_blocks, inject_blocks};
pub(crate) use lifecycle::{
    __path_create_instance, __path_create_instances_batch, __path_get_instance,
    __path_list_instances, __path_retry_instance, __path_update_context, __path_update_state,
    create_instance, create_instances_batch, get_instance, list_instances, retry_instance,
    update_context, update_state,
};
pub(crate) use outputs::{
    __path_get_execution_tree, __path_get_outputs, get_execution_tree, get_outputs,
};
pub(crate) use signals::{__path_send_signal, send_signal};

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/instances", post(create_instance).get(list_instances))
        .route("/instances/batch", post(create_instances_batch))
        .route("/instances/{id}", get(get_instance))
        .route("/instances/{id}/state", patch(update_state))
        .route("/instances/{id}/context", patch(update_context))
        .route("/instances/{id}/signals", post(send_signal))
        .route("/instances/{id}/outputs", get(get_outputs))
        .route("/instances/{id}/tree", get(get_execution_tree))
        .route("/instances/{id}/retry", post(retry_instance))
        .route(
            "/instances/{id}/checkpoints",
            get(list_checkpoints).post(save_checkpoint),
        )
        .route(
            "/instances/{id}/checkpoints/latest",
            get(get_latest_checkpoint),
        )
        .route("/instances/{id}/checkpoints/prune", post(prune_checkpoints))
        .route("/instances/{id}/audit", get(list_audit_log))
        .route("/instances/{id}/inject-blocks", post(inject_blocks))
        .route(
            "/instances/{id}/stream",
            get(crate::streaming::stream_instance),
        )
        .route("/instances/bulk/state", patch(bulk_update_state))
        .route("/instances/bulk/reschedule", patch(bulk_reschedule))
        .route("/instances/dlq", get(list_dlq))
}
