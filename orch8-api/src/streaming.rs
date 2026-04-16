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
        InstanceState::Completed
            | InstanceState::Failed
            | InstanceState::Cancelled
    )
}

/// SSE stream for instance progress.
pub(crate) async fn stream_instance(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Query(query): Query<StreamQuery>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let instance_id = InstanceId(id);
    let poll_interval = Duration::from_millis(query.poll_ms.clamp(100, 5000));

    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);

    tokio::spawn(async move {
        let mut last_output_count: usize = 0;
        let mut last_state: Option<String> = None;

        loop {
            tokio::time::sleep(poll_interval).await;

            // Fetch instance.
            let instance = match state.storage.get_instance(instance_id).await {
                Ok(Some(inst)) => inst,
                Ok(None) => {
                    let _ = tx
                        .send(Ok(Event::default()
                            .event("error")
                            .data(r#"{"error":"instance not found"}"#)))
                        .await;
                    break;
                }
                Err(_) => continue,
            };

            // Emit state change.
            let current_state = instance.state.to_string();
            if last_state.as_deref() != Some(&current_state) {
                let event = Event::default().event("state").data(
                    serde_json::json!({
                        "instance_id": instance_id.0.to_string(),
                        "state": &current_state,
                    })
                    .to_string(),
                );
                if tx.send(Ok(event)).await.is_err() {
                    break;
                }
                last_state = Some(current_state);
            }

            // Emit new block outputs.
            if let Ok(outputs) = state.storage.get_all_outputs(instance_id).await {
                let new_count = outputs.len();
                if new_count > last_output_count {
                    for output in outputs.iter().skip(last_output_count) {
                        let event = Event::default()
                            .event("output")
                            .data(serde_json::to_string(output).unwrap_or_default());
                        if tx.send(Ok(event)).await.is_err() {
                            return;
                        }
                    }
                    last_output_count = new_count;
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

    Sse::new(tokio_stream::wrappers::ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
