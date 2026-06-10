//! SSE streaming endpoint for real-time instance progress.
//!
//! `GET /instances/{id}/stream` returns a Server-Sent Events stream that emits:
//! - `state` events when instance state changes
//! - `output` events when new block outputs appear
//! - `llm_delta` events with incremental text from a streaming `llm_call`
//!   step (`stream: true`), forwarded live from the in-process
//!   [`orch8_engine::stream_bus`] — not polled from storage
//! - `done` event when the instance reaches a terminal state
//!
//! State/output changes poll storage at a configurable interval (default
//! 500ms); keepalive comments are sent every 15s. Closes when the instance
//! reaches a terminal state.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use serde::Deserialize;
use tokio::sync::broadcast;
use uuid::Uuid;

use orch8_engine::stream_bus::{stream_bus, StreamEvent};
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

const fn default_poll_ms() -> u64 {
    500
}

/// Await the next live event from the stream-bus subscription. With no
/// subscription left (`None`, after the channel closed) pends forever — the
/// caller guards the select arm with `delta_rx.is_some()`, so this branch is
/// then never polled.
async fn next_delta(
    rx: Option<&mut broadcast::Receiver<StreamEvent>>,
) -> Result<StreamEvent, broadcast::error::RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

const fn is_terminal(state: InstanceState) -> bool {
    matches!(
        state,
        InstanceState::Completed | InstanceState::Failed | InstanceState::Cancelled
    )
}

/// SSE stream for instance progress.
#[utoipa::path(get, path = "/instances/{id}/stream", tag = "instances",
    params(
        ("id" = uuid::Uuid, Path, description = "Instance ID"),
        ("poll_ms" = Option<u64>, Query, description = "Poll interval in ms (min 100ms)"),
    ),
    responses(
        (status = 200, description = "Server-Sent Events stream of instance state/output changes plus live llm_delta events from streaming llm_call steps", content_type = "text/event-stream"),
        (status = 404, description = "Instance not found"),
        (status = 503, description = "Too many concurrent streams"),
    )
)]
#[allow(clippy::too_many_lines)]
pub(crate) async fn stream_instance(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Path(id): Path<Uuid>,
    Query(query): Query<StreamQuery>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, ApiError> {
    let instance_id = InstanceId::from_uuid(id);

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

    // Perf#3: acquire a concurrency permit before spawning. Each live stream
    // owns a permit for its lifetime; when the semaphore is exhausted, reject
    // with 503 instead of silently spawning another polling task. Without
    // this gate a single client could open arbitrarily many streams and
    // exhaust tokio tasks + DB pool connections.
    let permit = state
        .stream_limiter
        .clone()
        .try_acquire_owned()
        .map_err(|_| {
            ApiError::Unavailable("too many concurrent streaming clients; retry later".into())
        })?;

    // Perf#2: exponential backoff on consecutive storage errors. The
    // previous shape `continue`d on error, which still waited for the
    // next poll tick but hammered the DB at the configured cadence
    // during an outage. On repeated failures we extend the wait,
    // capped at ~30s, and reset on success.
    #[allow(clippy::items_after_statements)]
    const MAX_BACKOFF: Duration = Duration::from_secs(30);

    let poll_interval = Duration::from_millis(query.poll_ms.clamp(100, 5000));

    // Channel depth: one slot per event type we may emit per tick (state, outputs batch, done).
    // Small buffer ensures a slow client applies backpressure to the producer.
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);
    let shutdown = state.shutdown.clone();
    let storage = state.storage.clone();

    // Subscribe to the in-process stream bus BEFORE the response starts, so
    // llm_delta events published after the client connects are never missed.
    // The engine runs in the same process (orch8-server); in API-only
    // deployments the channel simply never receives anything.
    let mut delta_rx = Some(stream_bus().subscribe(instance_id));

    tokio::spawn(async move {
        // Hold the permit for the lifetime of the stream task; dropped
        // automatically on return/panic, freeing a slot.
        let _permit = permit;

        let mut last_output_at: Option<chrono::DateTime<chrono::Utc>> = None;
        let mut last_state: Option<InstanceState> = None;
        let mut ticker = tokio::time::interval(poll_interval);
        // Skip the immediate first tick so we don't double-query right after the prefetch above.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        let mut consecutive_errors: u32 = 0;

        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => break,
                () = tx.closed() => break,
                _ = ticker.tick() => {}
                event = next_delta(delta_rx.as_mut()), if delta_rx.is_some() => {
                    match event {
                        Ok(ev) => {
                            // `StreamEvent` serialization is infallible (plain
                            // strings + tag); fall back to `{}` defensively.
                            let payload = serde_json::to_string(&ev)
                                .unwrap_or_else(|_| "{}".to_string());
                            let sse = Event::default().event("llm_delta").data(payload);
                            if tx.send(Ok(sse)).await.is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            // Best-effort live view: a slow client just loses
                            // deltas; the durable output event carries the
                            // full text anyway.
                            tracing::debug!(
                                skipped,
                                instance_id = %instance_id.into_uuid(),
                                "stream: client lagged behind llm_delta broadcast"
                            );
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            delta_rx = None;
                        }
                    }
                    // Deltas don't advance the storage poll; wait for the
                    // next tick before re-querying.
                    continue;
                }
            }

            // If the last iterations failed, wait an extra backoff period
            // BEFORE issuing the next query. Backoff doubles per failure
            // (capped), and sits on top of the normal poll interval.
            if consecutive_errors > 0 {
                // `poll_interval` is clamped to ≤ 5000ms so `as_millis()`
                // comfortably fits in a u64 before the multiplier applies.
                let base_ms = u64::try_from(poll_interval.as_millis()).unwrap_or(u64::MAX);
                let extra_ms = base_ms.saturating_mul(1u64 << consecutive_errors.min(8));
                let extra = Duration::from_millis(extra_ms).min(MAX_BACKOFF);
                tokio::select! {
                    biased;
                    () = shutdown.cancelled() => break,
                    () = tx.closed() => break,
                    () = tokio::time::sleep(extra) => {}
                }
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
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    tracing::debug!(error = %e, instance_id = %instance_id.into_uuid(), consecutive_errors, "stream: get_instance failed, will back off");
                    continue;
                }
            };

            // Emit state change.
            if last_state != Some(instance.state) {
                let event = Event::default().event("state").data(
                    serde_json::json!({
                        "instance_id": instance_id.into_uuid(),
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
            // Use `get_outputs_after_created_at` to avoid fetching the entire
            // history on every poll — critical for long-running instances that
            // accumulate thousands of outputs.
            match storage
                .get_outputs_after_created_at(instance_id, last_output_at)
                .await
            {
                Ok(outputs) => {
                    if !outputs.is_empty() {
                        for output in &outputs {
                            // Serialisation of owned `BlockOutput` is infallible in practice —
                            // if it ever fails, drop the payload but keep the stream alive.
                            let payload = serde_json::to_string(output).unwrap_or_else(|e| {
                                tracing::error!(
                                    instance_id = %instance_id.into_uuid(),
                                    output_id = %output.id,
                                    error = %e,
                                    "failed to serialize output in SSE stream"
                                );
                                "{}".to_string()
                            });
                            let event = Event::default().event("output").data(payload);
                            if tx.send(Ok(event)).await.is_err() {
                                return;
                            }
                            if last_output_at.is_none_or(|t| output.created_at > t) {
                                last_output_at = Some(output.created_at);
                            }
                        }
                    }
                    consecutive_errors = 0;
                }
                Err(e) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    tracing::debug!(error = %e, instance_id = %instance_id.into_uuid(), consecutive_errors, "stream: get_outputs_after_created_at failed, will back off");
                    continue;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_poll_ms_is_500() {
        assert_eq!(default_poll_ms(), 500);
    }

    #[test]
    fn is_terminal_for_completed() {
        assert!(is_terminal(InstanceState::Completed));
    }

    #[test]
    fn is_terminal_for_failed() {
        assert!(is_terminal(InstanceState::Failed));
    }

    #[test]
    fn is_terminal_for_cancelled() {
        assert!(is_terminal(InstanceState::Cancelled));
    }

    #[test]
    fn is_terminal_false_for_running() {
        assert!(!is_terminal(InstanceState::Running));
    }

    #[test]
    fn is_terminal_false_for_scheduled() {
        assert!(!is_terminal(InstanceState::Scheduled));
    }

    #[test]
    fn is_terminal_false_for_waiting() {
        assert!(!is_terminal(InstanceState::Waiting));
    }

    #[test]
    fn is_terminal_false_for_paused() {
        assert!(!is_terminal(InstanceState::Paused));
    }
}
