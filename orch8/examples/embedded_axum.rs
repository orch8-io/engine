//! Embedding the engine in an axum web service: HTTP requests create durable
//! workflow instances on a shared [`orch8::Engine`].
//!
//! ```sh
//! cargo run -p orch8 --example embedded_axum
//! curl -X POST localhost:3000/orders                # -> {"instance_id": "..."}
//! curl localhost:3000/orders/<instance_id>          # -> {"state": "completed", ...}
//! ```

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};

use orch8::{Engine, InstanceId, SequenceId, Storage};

fn sequence_json() -> serde_json::Value {
    serde_json::json!({
        "id": uuid::Uuid::now_v7(),
        "tenant_id": "default",
        "namespace": "default",
        "name": "fulfil-order",
        "version": 1,
        "blocks": [
            { "type": "step", "id": "reserve", "handler": "reserve_stock", "params": {} },
            { "type": "step", "id": "notify", "handler": "log", "params": { "message": "order fulfilled" } }
        ],
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

/// Shared application state: the engine (cheap to clone) plus the sequence
/// new orders should run.
#[derive(Clone)]
struct AppState {
    engine: Engine,
    sequence_id: SequenceId,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::builder()
        .storage(Storage::sqlite_in_memory())
        .handler("reserve_stock", |_ctx: orch8::StepContext| async move {
            Ok(serde_json::json!({ "reserved": true }))
        })
        .build()
        .await?;

    let sequence_id = engine
        .upsert_sequence(serde_json::from_value(sequence_json())?)
        .await?;

    engine.start();

    let app = Router::new()
        .route("/orders", post(create_order))
        .route("/orders/{id}", get(get_order))
        .with_state(AppState {
            engine: engine.clone(),
            sequence_id,
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://127.0.0.1:3000 (POST /orders, GET /orders/{{id}})");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    engine.shutdown().await;
    Ok(())
}

async fn create_order(
    State(state): State<AppState>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let id = state
        .engine
        .create_instance(state.sequence_id, orch8::CreateInstanceOptions::default())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(serde_json::json!({ "instance_id": id })))
}

async fn get_order(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let instance = state
        .engine
        .get_instance(InstanceId::from_uuid(id))
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;
    Ok(Json(serde_json::json!({
        "instance_id": instance.id,
        "state": instance.state,
        "context": instance.context.data,
        "updated_at": instance.updated_at,
    })))
}
