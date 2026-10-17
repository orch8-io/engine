//! Regression tests for the delta code review (D-02, D-03). Runs on every
//! backend the harness opens (`SQLite` always, Postgres with `DATABASE_URL`).

use chrono::{Duration, Utc};

use orch8_storage::backend_test;
use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::webhook_outbox::{WebhookOutboxEntry, WebhookOutboxStatus};

fn oauth(id: &str) -> CredentialDef {
    let now = Utc::now();
    CredentialDef {
        id: id.into(),
        tenant_id: String::new(),
        name: id.into(),
        kind: CredentialKind::Oauth2,
        value: SecretString::new(r#"{"access_token":"a"}"#.into()),
        expires_at: Some(now),
        refresh_url: Some("https://token.example.invalid/oauth".into()),
        refresh_token: Some(SecretString::new("rt".into())),
        enabled: true,
        description: None,
        created_at: now,
        updated_at: now,
    }
}

fn six_hours(_: u32) -> chrono::TimeDelta {
    chrono::TimeDelta::hours(6)
}

async fn is_due(s: &dyn orch8_storage::StorageBackend, id: &str) -> bool {
    s.list_credentials_due_for_refresh(std::time::Duration::from_secs(300))
        .await
        .unwrap()
        .iter()
        .any(|c| c.id == id)
}

backend_test!(
    /// D-03: re-authorising a credential through the API must clear the
    /// refresh back-off, or it waits out the (up to 6 h) hold.
    async fn api_update_resets_refresh_backoff(b) {
        let id = format!("cred-{}", uuid::Uuid::new_v4());
        let s = b.storage.as_ref();
        s.create_credential(&oauth(&id)).await.unwrap();
        assert!(is_due(s, &id).await, "{}: due before any failure", b.name());
        s.record_credential_refresh_failure(&id, Utc::now(), six_hours)
            .await
            .unwrap();
        assert!(!is_due(s, &id).await, "{}: held by back-off", b.name());

        let stored = s.get_credential(None, &id).await.unwrap().unwrap();
        s.update_credential(&stored).await.unwrap();
        assert!(is_due(s, &id).await, "{}: update clears the hold", b.name());

        s.record_credential_refresh_failure(&id, Utc::now(), six_hours)
            .await
            .unwrap();
        assert!(!is_due(s, &id).await);
        let stored = s.get_credential(None, &id).await.unwrap().unwrap();
        assert!(
            s.update_credential_cas(&stored, stored.updated_at)
                .await
                .unwrap()
        );
        assert!(is_due(s, &id).await, "{}: update_cas clears the hold", b.name());
        // The failure counter restarted too.
        let n = s
            .record_credential_refresh_failure(&id, Utc::now(), six_hours)
            .await
            .unwrap();
        assert_eq!(n, 1, "{}", b.name());
    }
);

backend_test!(
    /// D-02: the bounded parked prune removes only old parked rows.
    async fn parked_outbox_prune_is_scoped(b) {
        let s = b.storage.as_ref();
        let entry = |status, age_days| WebhookOutboxEntry {
            id: uuid::Uuid::now_v7(),
            url: "https://a.example/hook".into(),
            event_type: "instance.completed".into(),
            instance_id: None,
            payload: serde_json::json!({}),
            attempts: 0,
            last_error: None,
            created_at: Utc::now() - Duration::days(age_days),
            delivery_id: Some(uuid::Uuid::now_v7()),
            status,
            next_attempt_at: None,
            claimed_at: None,
        };
        let old_parked = entry(WebhookOutboxStatus::Parked, 400);
        let fresh_parked = entry(WebhookOutboxStatus::Parked, 0);
        let old_pending = entry(WebhookOutboxStatus::Pending, 400);
        for e in [&old_parked, &fresh_parked, &old_pending] {
            s.park_webhook(e).await.unwrap();
        }
        let deleted = s
            .delete_parked_webhook_outbox_before(Utc::now() - Duration::days(30))
            .await
            .unwrap();
        assert!(deleted >= 1, "{}", b.name());
        assert!(s.get_webhook_outbox(old_parked.id).await.unwrap().is_none());
        assert!(s.get_webhook_outbox(fresh_parked.id).await.unwrap().is_some());
        assert!(s.get_webhook_outbox(old_pending.id).await.unwrap().is_some());
    }
);
