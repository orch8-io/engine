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
#[utoipa::path(get, path = "/instances/{id}/stream", tag = "instances",
    params(
        ("id" = uuid::Uuid, Path, description = "Instance ID"),
        ("poll_ms" = Option<u64>, Query, description = "Poll interval in ms (min 100ms)"),
    ),
    responses(
        (status = 200, description = "Server-Sent Events stream of instance state/tree/output changes", content_type = "text/event-stream"),
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

    // Perf#3: acquire a concurrency permit before spawning. Each live stream
    // owns a permit for its lifetime; when the semaphore is exhausted, reject
    // with 503 instead of silently spawning another polling task. Without
    // this gate a single client could open arbitrarily many streams and
    // exhaust tokio tasks + DB pool connections.
    let permit = std::sync::Arc::clone(&state.stream_limiter)
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

    tokio::spawn(async move {
        // Hold the permit for the lifetime of the stream task; dropped
        // automatically on return/panic, freeing a slot.
        let _permit = permit;

        let mut last_output_count: usize = 0;
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
                    tracing::debug!(error = %e, instance_id = %instance_id.0, consecutive_errors, "stream: get_instance failed, will back off");
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
                    consecutive_errors = 0;
                }
                Err(e) => {
                    consecutive_errors = consecutive_errors.saturating_add(1);
                    tracing::debug!(error = %e, instance_id = %instance_id.0, consecutive_errors, "stream: get_all_outputs failed, will back off");
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
