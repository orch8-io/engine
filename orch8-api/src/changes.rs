//! Tenant-scoped ordered state-change feed with opaque resumable cursors.

use std::convert::Infallible;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::get;
use axum::{Json, Router};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use orch8_types::audit::{AuditLogEntry, ChangeCursor};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::error::ApiError;

const DEFAULT_LIMIT: u32 = 100;
const MAX_LIMIT: u32 = 500;

#[derive(Debug, Deserialize)]
pub(crate) struct ChangeQuery {
    tenant_id: Option<String>,
    cursor: Option<String>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ChangePage {
    pub changes: Vec<AuditLogEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route("/changes", get(list_changes))
        .route("/changes/stream", get(stream_changes))
}

#[utoipa::path(get, path = "/changes", tag = "changes",
    params(
        ("tenant_id" = Option<String>, Query, description = "Required without X-Tenant-Id"),
        ("cursor" = Option<String>, Query, description = "Exclusive opaque cursor from next_cursor"),
        ("limit" = Option<u32>, Query, description = "Page size, capped at 500"),
    ),
    responses(
        (status = 200, description = "Ascending tenant state changes", body = ChangePage),
        (status = 400, description = "Missing tenant or malformed cursor"),
    )
)]
pub(crate) async fn list_changes(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ChangeQuery>,
) -> Result<Json<ChangePage>, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref())
        .ok_or_else(|| ApiError::InvalidArgument("tenant scope is required".into()))?;
    let cursor = query.cursor.as_deref().map(decode_cursor).transpose()?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let mut changes = state
        .storage
        .list_tenant_changes(&tenant, cursor, limit.saturating_add(1))
        .await
        .map_err(|error| ApiError::from_storage(error, "change_feed"))?;
    let has_more = changes.len() > limit as usize;
    changes.truncate(limit as usize);
    let next_cursor = changes.last().map(ChangeCursor::from).map(encode_cursor);
    Ok(Json(ChangePage {
        changes,
        next_cursor,
        has_more,
    }))
}

#[utoipa::path(get, path = "/changes/stream", tag = "changes",
    params(
        ("tenant_id" = Option<String>, Query, description = "Required without X-Tenant-Id"),
        ("cursor" = Option<String>, Query, description = "Overrides Last-Event-ID when supplied"),
        ("limit" = Option<u32>, Query, description = "Maximum changes fetched per poll"),
    ),
    responses(
        (status = 200, description = "Resumable tenant change stream (SSE)"),
        (status = 400, description = "Missing tenant or malformed cursor"),
        (status = 429, description = "Concurrent stream limit reached"),
    )
)]
pub(crate) async fn stream_changes(
    State(state): State<AppState>,
    tenant_ctx: crate::auth::OptionalTenant,
    Query(query): Query<ChangeQuery>,
    headers: HeaderMap,
) -> Result<impl axum::response::IntoResponse, ApiError> {
    let tenant = crate::auth::scoped_tenant_id(&tenant_ctx, query.tenant_id.as_deref())
        .ok_or_else(|| ApiError::InvalidArgument("tenant scope is required".into()))?;
    let header_cursor = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok());
    let mut cursor = query
        .cursor
        .as_deref()
        .or(header_cursor)
        .map(decode_cursor)
        .transpose()?;
    let limit = query.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let permit = crate::stream_limits::acquire(&state.stream_limiter, tenant.as_str())
        .map_err(|_| ApiError::RateLimited("concurrent change stream limit reached".into()))?;
    let storage = state.storage.clone();
    let shutdown = state.shutdown.clone();
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(128);
    tokio::spawn(async move {
        let _permit = permit;
        let producer = async {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                match storage.list_tenant_changes(&tenant, cursor, limit).await {
                    Ok(changes) if changes.is_empty() => {
                        tokio::select! {
                            () = shutdown.cancelled() => break,
                            () = tokio::time::sleep(Duration::from_millis(500)) => {}
                        }
                    }
                    Ok(changes) => {
                        for change in changes {
                            let next = ChangeCursor::from(&change);
                            let encoded = encode_cursor(next);
                            let Ok(event) = Event::default()
                                .event("change")
                                .id(encoded)
                                .json_data(&change)
                            else {
                                continue;
                            };
                            if sender.send(Ok(event)).await.is_err() {
                                return;
                            }
                            cursor = Some(next);
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, tenant = %tenant, "change stream storage read failed");
                        if sender
                            .send(Ok(Event::default().event("error").data(
                                "change feed temporarily unavailable; reconnect with Last-Event-ID",
                            )))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        };
        crate::streaming::run_until_stream_closed(&shutdown, &sender, producer).await;
    });
    Ok(
        Sse::new(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))),
    )
}

fn encode_cursor(cursor: ChangeCursor) -> String {
    URL_SAFE_NO_PAD
        .encode(serde_json::to_vec(&cursor).expect("serializing a ChangeCursor cannot fail"))
}

fn decode_cursor(encoded: &str) -> Result<ChangeCursor, ApiError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError::InvalidArgument("malformed change cursor".into()))?;
    serde_json::from_slice(&bytes)
        .map_err(|_| ApiError::InvalidArgument("malformed change cursor".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[tokio::test]
    async fn dropping_idle_change_stream_releases_its_slot() {
        use axum::response::IntoResponse as _;
        use std::sync::Arc;
        let slots = Arc::new(tokio::sync::Semaphore::new(1));
        let state = AppState {
            storage: Arc::new(
                orch8_storage::sqlite::SqliteStorage::in_memory()
                    .await
                    .unwrap(),
            ),
            shutdown: tokio_util::sync::CancellationToken::new(),
            max_context_bytes: 0,
            externalization_mode: orch8_types::config::ExternalizationMode::default(),
            worker_lease_secs: 60,
            worker_heartbeat_interval_secs: 15,
            circuit_breakers: None,
            stream_limiter: slots.clone(),
            publisher: None,
            push_provider: Arc::new(orch8_push::NoopPushProvider),
            mobile_sync_enabled: false,
            entitlements: crate::entitlements::unlimited_provider(),
            builtin_handlers: Arc::new(Vec::new()),
            engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            continuity_crypto: None,
            continuity_trusted_signing_keys: Arc::new(std::collections::BTreeMap::new()),
            federation_peers: Arc::new(Vec::new()),
            continuity_lab_enabled: false,
        };
        let response = stream_changes(
            State(state),
            None,
            Query(ChangeQuery {
                tenant_id: Some("idle-tenant".into()),
                cursor: None,
                limit: None,
            }),
            HeaderMap::new(),
        )
        .await
        .unwrap()
        .into_response();
        assert_eq!(slots.available_permits(), 0);
        drop(response);
        let permit = tokio::time::timeout(Duration::from_secs(5), slots.acquire())
            .await
            .expect("disconnected idle stream must release its slot")
            .unwrap();
        drop(permit);
    }

    #[test]
    fn opaque_cursor_round_trips_and_rejects_garbage() {
        let cursor = ChangeCursor {
            created_at: Utc::now(),
            id: Uuid::now_v7(),
        };
        assert_eq!(decode_cursor(&encode_cursor(cursor)).unwrap(), cursor);
        assert!(decode_cursor("not+url/base64").is_err());
    }
}
