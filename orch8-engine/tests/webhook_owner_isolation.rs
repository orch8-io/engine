//! The scheduler and durable outbox belong to their supplied backend.
use std::sync::Arc;

use axum::{Router, routing::post};
use chrono::Utc;
use orch8_engine::{handlers::HandlerRegistry, scheduler, webhooks};
use orch8_storage::{StorageBackend, sqlite::SqliteStorage};
use orch8_types::clock::SharedClock;
use orch8_types::config::{SchedulerConfig, WebhookConfig};
use orch8_types::ids::InstanceId;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn webhook_owner_scheduler_releases_its_backend_after_stop() {
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let weak = Arc::downgrade(&storage);
    let cancel = CancellationToken::new();
    cancel.cancel();
    scheduler::run_tick_loop(
        Arc::clone(&storage),
        Arc::new(HandlerRegistry::new()),
        &SchedulerConfig::default(),
        cancel,
    )
    .await
    .unwrap();
    drop(storage);
    assert!(
        weak.upgrade().is_none(),
        "a stopped scheduler must not install a permanent process-global owner of its database"
    );
}

#[tokio::test]
async fn webhook_owner_durable_attempt_is_recorded_in_its_backend() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/hook",
                post(|| async { axum::http::StatusCode::NO_CONTENT }),
            ),
        )
        .with_graceful_shutdown(stop.cancelled_owned())
        .await
        .unwrap();
    });
    let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let delivery_id = uuid::Uuid::now_v7();
    let event = webhooks::instance_event(
        "instance.completed",
        InstanceId::new(),
        serde_json::json!({}),
    );
    let entry = WebhookOutboxEntry {
        id: uuid::Uuid::now_v7(),
        url,
        event_type: event.event_type.clone(),
        instance_id: event.instance_id.map(InstanceId::into_uuid),
        payload: serde_json::to_value(event).unwrap(),
        attempts: 0,
        last_error: None,
        created_at: Utc::now(),
        delivery_id: Some(delivery_id),
        status: WebhookOutboxStatus::Pending,
        next_attempt_at: None,
        claimed_at: None,
    };
    storage.park_webhook(&entry).await.unwrap();
    assert_eq!(
        webhooks::outbox_tick(
            &storage,
            &WebhookConfig::default(),
            &SharedClock::default(),
            &CancellationToken::new()
        )
        .await,
        1
    );
    let attempts = storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    shutdown.cancel();
    server.await.unwrap();
    assert_eq!(
        attempts.len(),
        1,
        "attempt history must follow the explicitly supplied backend, independently of process defaults"
    );
    assert!(attempts[0].success);
    assert!(
        storage
            .get_webhook_outbox(entry.id)
            .await
            .unwrap()
            .is_none()
    );
}
