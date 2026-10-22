//! API servers keep signing keys, alert credentials and attempt history local.
use std::fmt::Write as _;
use std::sync::Arc;

use axum::{Router, body::Bytes, http::HeaderMap, routing::post};
use chrono::Utc;
use hmac::{Hmac, KeyInit, Mac};
use orch8_api::test_harness::{
    BackendTestServer, spawn_test_server_on, spawn_test_server_with_webhooks_on,
};
use orch8_storage::{AdminStore, WorkerStore, sqlite::SqliteStorage};
use orch8_types::alert::AlertDestination;
use orch8_types::config::{SecretString, WebhookConfig};
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::ids::InstanceId;
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};
use serde_json::{Value, json};
use tokio::sync::{Mutex, OnceCell};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const ROOT_KEY: &str = "webhook-owner-root";

struct Receiver {
    url: String,
    requests: Arc<Mutex<Vec<(HeaderMap, Bytes)>>>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

async fn receiver() -> Receiver {
    receiver_with_status(axum::http::StatusCode::NO_CONTENT).await
}

async fn receiver_with_status(status: axum::http::StatusCode) -> Receiver {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let captured = Arc::clone(&requests);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    let cancel = CancellationToken::new();
    let stop = cancel.clone();
    let task = tokio::spawn(async move {
        let router = Router::new().route(
            "/hook",
            post(move |headers: HeaderMap, body: Bytes| {
                let captured = Arc::clone(&captured);
                async move {
                    captured.lock().await.push((headers, body));
                    status
                }
            }),
        );
        axum::serve(listener, router)
            .with_graceful_shutdown(stop.cancelled_owned())
            .await
            .unwrap();
    });
    Receiver {
        url,
        requests,
        cancel,
        task,
    }
}

async fn credential(storage: &SqliteStorage, value: &str) {
    let now = Utc::now();
    storage
        .create_credential(&CredentialDef {
            id: "pd".into(),
            tenant_id: "acme".into(),
            name: "Routing key".into(),
            kind: CredentialKind::default(),
            value: SecretString::from(value.to_owned()),
            expires_at: None,
            refresh_url: None,
            refresh_token: None,
            enabled: true,
            description: None,
            created_at: now,
            updated_at: now,
        })
        .await
        .unwrap();
}

async fn unrelated_default() -> Arc<SqliteStorage> {
    static LEGACY: OnceCell<Arc<SqliteStorage>> = OnceCell::const_new();
    Arc::clone(
        LEGACY
            .get_or_init(|| async {
                let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
                credential(&storage, "unrelated-routing-key").await;
                orch8_engine::webhooks::init_outbox(
                    storage.clone(),
                    WebhookConfig {
                        secret: Some(SecretString::from("unrelated-signing-key".to_owned())),
                        ..WebhookConfig::default()
                    },
                );
                storage
            })
            .await,
    )
}

fn entry(url: &str) -> WebhookOutboxEntry {
    let event =
        orch8_engine::webhooks::instance_event("instance.completed", InstanceId::new(), json!({}));
    WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: url.into(),
        event_type: event.event_type.clone(),
        instance_id: event.instance_id.map(InstanceId::into_uuid),
        payload: serde_json::to_value(event).unwrap(),
        attempts: 1,
        last_error: Some("old failure".into()),
        created_at: Utc::now(),
        delivery_id: None,
        status: WebhookOutboxStatus::Parked,
        next_attempt_at: None,
        claimed_at: None,
    }
}

async fn redeliver(server: &BackendTestServer, entry: &WebhookOutboxEntry) -> (Value, Uuid) {
    server.storage.park_webhook(entry).await.unwrap();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(5))
        .build()
        .unwrap();
    let path = format!("{}/webhooks/outbox/{}", server.v1_url(), entry.id);
    let preview = client
        .get(format!("{path}/redeliver-preview"))
        .header("x-api-key", ROOT_KEY)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    let result = client
        .post(format!("{path}/redeliver"))
        .header("x-api-key", ROOT_KEY)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    (
        preview,
        result["delivery_id"].as_str().unwrap().parse().unwrap(),
    )
}

// The legacy default is process-wide and its in-memory SQLite connection
// belongs to the runtime that opens it. Keep all scenarios in one runtime:
// independent #[tokio::test] runtimes would close that connection as soon as
// its first scenario finishes, destroying the shared in-memory schema.
#[tokio::test]
async fn webhook_owner_api_isolation() {
    webhook_owner_api_ignores_an_unrelated_process_default().await;
    webhook_owner_two_live_servers_keep_their_own_signing_keys().await;
    webhook_owner_alert_resolves_credentials_in_its_own_database().await;
    webhook_owner_explicit_legacy_default_still_supports_redelivery().await;
    webhook_owner_failed_redelivery_uses_its_retry_budget_and_keeps_the_row().await;
}

async fn webhook_owner_api_ignores_an_unrelated_process_default() {
    let unrelated = unrelated_default().await;
    let receiver = receiver().await;
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let server = spawn_test_server_on(storage.clone(), Some(ROOT_KEY)).await;
    let (preview, delivery_id) = redeliver(&server, &entry(&receiver.url)).await;
    let own_attempts = storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    let foreign_attempts = unrelated
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    let requests = receiver.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        (
            preview["will_be_signed"].as_bool().unwrap(),
            requests[0].0.contains_key("x-orch8-signature"),
            own_attempts.len(),
            foreign_attempts.len()
        ),
        (false, false, 1, 0),
        "API-only server uses its unsigned configuration and its own attempt history"
    );
}

fn assert_signature(secret: &str, headers: &HeaderMap, body: &[u8]) {
    let timestamp = headers["x-orch8-timestamp"].to_str().unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let mut expected = String::from("sha256=");
    for byte in mac.finalize().into_bytes() {
        write!(&mut expected, "{byte:02x}").unwrap();
    }
    assert_eq!(headers["x-orch8-signature"].to_str().unwrap(), expected);
}

async fn webhook_owner_two_live_servers_keep_their_own_signing_keys() {
    unrelated_default().await;
    let receiver = receiver().await;
    let mut servers = Vec::new();
    for secret in ["owner-b-signing-key", "owner-c-signing-key"] {
        let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let config = WebhookConfig {
            secret: Some(SecretString::from(secret.to_owned())),
            max_retries: 0,
            timeout_secs: 2,
            ..WebhookConfig::default()
        };
        let server =
            spawn_test_server_with_webhooks_on(storage.clone(), Some(ROOT_KEY), config).await;
        servers.push((storage, server, secret));
    }
    for (index, (storage, server, secret)) in servers.iter().enumerate() {
        let (preview, delivery_id) = redeliver(server, &entry(&receiver.url)).await;
        assert_eq!(preview["will_be_signed"], true);
        let requests = receiver.requests.lock().await;
        assert_eq!(requests.len(), index + 1);
        let (headers, body) = &requests[index];
        assert_signature(secret, headers, body);
        let attempts = storage
            .get_webhook_delivery_attempts(delivery_id)
            .await
            .unwrap();
        assert_eq!(attempts.len(), 1);
        assert!(attempts[0].signed);
    }
}

async fn webhook_owner_alert_resolves_credentials_in_its_own_database() {
    let unrelated = unrelated_default().await;
    let receiver = receiver().await;
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    credential(&storage, "owner-routing-key").await;
    let server = spawn_test_server_on(storage.clone(), Some(ROOT_KEY)).await;
    let mut row = entry(&receiver.url);
    let mut event: orch8_engine::webhooks::WebhookEvent =
        serde_json::from_value(row.payload).unwrap();
    event.data = json!({"_orch8_alert": {
        "tenant_id": "acme",
        "destination": AlertDestination::Pagerduty { routing_key_ref: "credentials://pd".into(), severity: "error".into() },
        "body": {"event_action": "trigger", "payload": {"summary": "test"}}
    }});
    row.payload = serde_json::to_value(event).unwrap();
    let (preview, delivery_id) = redeliver(&server, &row).await;
    assert_eq!(
        preview["will_be_signed"], false,
        "PagerDuty delivery has no webhook HMAC signature"
    );
    let requests = receiver.requests.lock().await;
    assert_eq!(requests.len(), 1);
    let body: Value = serde_json::from_slice(&requests[0].1).unwrap();
    assert_eq!(body["routing_key"], "owner-routing-key");
    let attempts = storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(!attempts[0].signed);
    assert!(
        unrelated
            .get_webhook_delivery_attempts(delivery_id)
            .await
            .unwrap()
            .is_empty()
    );
}

async fn webhook_owner_explicit_legacy_default_still_supports_redelivery() {
    let storage = unrelated_default().await;
    let receiver = receiver().await;
    let row = entry(&receiver.url);
    let delivery_id = orch8_engine::webhooks::redeliver(&row, &CancellationToken::new())
        .await
        .unwrap();
    let requests = receiver.requests.lock().await;
    assert_eq!(requests.len(), 1);
    assert_signature("unrelated-signing-key", &requests[0].0, &requests[0].1);
    let attempts = storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(attempts[0].signed);
}

#[test]
fn webhook_owner_preview_matches_payload_signing_policy() {
    let config = WebhookConfig {
        secret: Some(SecretString::from("ordinary-key".to_owned())),
        ..WebhookConfig::default()
    };
    let mut row = entry("https://hooks.example.test/a");
    assert!(orch8_engine::webhooks::redelivery_will_sign_with_config(
        &config, &row
    ));
    let destinations = [
        (
            AlertDestination::Webhook {
                url: row.url.clone(),
                secret_ref: "credentials://hook".into(),
            },
            true,
        ),
        (
            AlertDestination::Pagerduty {
                routing_key_ref: "credentials://pd".into(),
                severity: "error".into(),
            },
            false,
        ),
    ];
    for (destination, expected) in destinations {
        let mut event =
            orch8_engine::webhooks::instance_event("alert.fired", InstanceId::new(), json!({}));
        event.data =
            json!({"_orch8_alert": {"tenant_id":"acme", "destination": destination, "body": {}}});
        row.payload = serde_json::to_value(event).unwrap();
        assert_eq!(
            orch8_engine::webhooks::redelivery_will_sign_with_config(&config, &row),
            expected
        );
        assert_eq!(
            orch8_engine::webhooks::redelivery_will_sign_with_config(
                &WebhookConfig::default(),
                &row
            ),
            expected
        );
    }
    row.payload = json!({});
    assert!(!orch8_engine::webhooks::redelivery_will_sign_with_config(
        &config, &row
    ));
}

async fn webhook_owner_failed_redelivery_uses_its_retry_budget_and_keeps_the_row() {
    let unrelated = unrelated_default().await;
    let receiver = receiver_with_status(axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;
    let storage = Arc::new(SqliteStorage::in_memory().await.unwrap());
    let config = WebhookConfig {
        max_retries: 0,
        timeout_secs: 2,
        ..WebhookConfig::default()
    };
    let server = spawn_test_server_with_webhooks_on(storage.clone(), Some(ROOT_KEY), config).await;
    let row = entry(&receiver.url);
    storage.park_webhook(&row).await.unwrap();
    let result = reqwest::Client::new()
        .post(format!(
            "{}/webhooks/outbox/{}/redeliver",
            server.v1_url(),
            row.id
        ))
        .header("x-api-key", ROOT_KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(result.status(), reqwest::StatusCode::BAD_GATEWAY);
    let requests = receiver.requests.lock().await;
    assert_eq!(
        requests.len(),
        1,
        "another engine's retry budget must not be used"
    );
    let delivery_id = requests[0].0["x-orch8-delivery-id"]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    let attempts = storage
        .get_webhook_delivery_attempts(delivery_id)
        .await
        .unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(!attempts[0].success);
    assert!(storage.get_webhook_outbox(row.id).await.unwrap().is_some());
    assert!(
        unrelated
            .get_webhook_delivery_attempts(delivery_id)
            .await
            .unwrap()
            .is_empty()
    );
}
