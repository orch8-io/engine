//! Manual redelivery cannot take over a scheduler-owned outbox row.
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use axum::{Router, routing::post};
use chrono::Utc;
use orch8_api::test_harness::spawn_test_server_on;
use orch8_storage::{WorkerStore, sqlite::SqliteStorage};
use orch8_types::ids::InstanceId;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

#[tokio::test]
async fn manual_redelivery_preserves_pending_and_claimed_rows() {
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let api = spawn_test_server_on(storage.clone(), Some("outbox-root")).await;
    let received = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&received);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let server_task = tokio::spawn(async move {
        let router = Router::new().route(
            "/hook",
            post(move || {
                let counter = Arc::clone(&counter);
                async move {
                    counter.fetch_add(1, Ordering::Relaxed);
                    axum::http::StatusCode::NO_CONTENT
                }
            }),
        );
        axum::serve(listener, router)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
    });
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let mut results = Vec::new();
    for status in [
        WebhookOutboxStatus::Pending,
        WebhookOutboxStatus::InFlight,
        WebhookOutboxStatus::Parked,
    ] {
        let event = orch8_engine::webhooks::instance_event(
            "instance.completed",
            InstanceId::new(),
            serde_json::json!({}),
        );
        let row = WebhookOutboxEntry {
            id: Uuid::now_v7(),
            url: url.clone(),
            event_type: event.event_type.clone(),
            instance_id: event.instance_id.map(InstanceId::into_uuid),
            payload: serde_json::to_value(event).unwrap(),
            attempts: 0,
            last_error: None,
            created_at: Utc::now(),
            delivery_id: Some(Uuid::now_v7()),
            status,
            next_attempt_at: None,
            claimed_at: (status == WebhookOutboxStatus::InFlight).then(Utc::now),
        };
        storage.park_webhook(&row).await.unwrap();
        let response = client
            .post(format!(
                "{}/webhooks/outbox/{}/redeliver",
                api.v1_url(),
                row.id
            ))
            .header("x-api-key", "outbox-root")
            .send()
            .await
            .unwrap();
        let retained = storage.get_webhook_outbox(row.id).await.unwrap();
        results.push((
            status,
            response.status().as_u16(),
            retained.as_ref().is_some_and(|saved| {
                saved.status == row.status
                    && saved.claimed_at == row.claimed_at
                    && saved.attempts == row.attempts
            }),
        ));
    }
    shutdown.cancel();
    server_task.await.unwrap();
    assert_eq!(
        results,
        vec![
            (WebhookOutboxStatus::Pending, 409, true),
            (WebhookOutboxStatus::InFlight, 409, true),
            (WebhookOutboxStatus::Parked, 200, false),
        ],
        "only parked rows belong to the manual-redelivery path"
    );
    assert_eq!(
        received.load(Ordering::Relaxed),
        1,
        "only the parked row should send an HTTP request"
    );
}
