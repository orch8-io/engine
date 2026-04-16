//! SSE streaming endpoint for real-time instance progress.
//!
//! `GET /instances/{id}/stream` returns a Server-Sent Events stream that emits:
//! - `state` events when instance state changes
//! - `output` events when new block outputs appear
//! - `done` event when the instance reaches a terminal state
//!
//! The stream polls storage at a configurable interval (default 500ms) and sends
//! keepalive comments every 15s. Closes when the instance reaches a terminal state.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use serde::Deserialize;
use uuid::Uuid;

use orch8_types::ids::InstanceId;
use orch8_types::instance::InstanceState;

use crate::error::ApiError;
use crate::AppState;

#[derive(Deserialize)]
pub(crate) struct StreamQuery {
    /// Poll interval in milliseconds (default 500, min 100, max 5000).
    #[serde(default = "default_poll_ms")]
    pub poll_ms: u64,
}

fn default_poll_ms() -> u64 {
    500
}

fn is_terminal(state: InstanceState) -> bool {
    matches!(
        state,
        InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
    )
}

/// SSE stream for instance progress.
pub(crate) async fn stream_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let instance_id = InstanceId(id);

    // Fetch instance to enforce tenant access before starting the stream.
    let instance = state
        .storage
        .get_instance(instance_id)
        .await
        .map_err(|e| ApiError::from_storage(e, "instance"))?
        .ok_or_else(|| ApiError::NotFound(format!("instance {id}")))?;
    crate::auth::enforce_tenant_access(
        &tenant_ctx,
        &instance.tenant_id,
        &format!("instance {id}"),
    )?;

    let poll_interval = Duration::from_millis(query.poll_ms.clamp(100, 5000));

    // Channel depth: one slot per event type we may emit per tick (state, outputs batch, done).
    // Small buffer ensures a slow client applies backpressure to the producer.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);
    let shutdown = state.shutdown.clone();
    let storage = state.storage.clone();

    tokio::spawn(async move {
        let mut last_output_count: usize = 0;
        let mut last_state: Option<InstanceState> = None;
        let mut ticker = tokio::time::interval(poll_interval);
        // Skip the immediate first tick so we don't double-query right after the prefetch above.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                () = tx.closed() => break,
                _ = ticker.tick() => {}
            }

            // Fetch instance.
            let instance = match storage.get_instance(instance_id).await {
                Ok(Some(inst)) => inst,
                Ok(None) => {
                    let _ = tx
                        .send(Ok(Event::default()
                            .event("error")
                            .data(r#"{"error":"instance not found"}"#)))
                        .await;
                    break;
                }
                Err(e) => {
                    tracing::debug!(error = %e, instance_id = %instance_id.0, "stream: get_instance failed, will retry");
                    continue;
                }
            };

            // Emit state change.
            if last_state != Some(instance.state) {
                let event = Event::default().event("state").data(
                    serde_json::json!({
                        "instance_id": instance_id.0,
                        "state": instance.state.to_string(),
                    })
                    .to_string(),
                );
                if tx.send(Ok(event)).await.is_err() {
                    break;
                }
                last_state = Some(instance.state);
            }

            // Emit new block outputs.
            match storage.get_all_outputs(instance_id).await {
                Ok(outputs) => {
                    let new_count = outputs.len();
                    if new_count > last_output_count {
                        for output in outputs.iter().skip(last_output_count) {
                            // Serialisation of owned `BlockOutput` is infallible in practice —
                            // if it ever fails, drop the payload but keep the stream alive.
                            let payload =
                                serde_json::to_string(output).unwrap_or_else(|_| "{}".to_string());
                            let event = Event::default().event("output").data(payload);
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                        }
                        last_output_count = new_count;
                    }
                }
                Err(e) => {
                    tracing::debug!(error = %e, instance_id = %instance_id.0, "stream: get_all_outputs failed, will retry");
                }
            }

            // Close on terminal state.
            if is_terminal(instance.state) {
                let _ = tx
                    .send(Ok(Event::default().event("done").data(
                        serde_json::json!({"state": instance.state.to_string()}).to_string(),
                    )))
                    .await;
                break;
            }
        }
    });

    Ok(Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
