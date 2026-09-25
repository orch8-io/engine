//! Postgres regressions for the push wake outbox and mobile device upsert
//! (MOB-N2, STO-N11/MOB-N8, API-N6/STO-M4, MOB-N1 retention sweep).
//!
//! Gated on `DATABASE_URL`; skipped with a message when it is absent.

use chrono::{Duration, Utc};
use orch8_push::{PushOutboxStore, PushTerminalReason, WakeAttemptOutcome};
use orch8_storage::postgres::PostgresStorage;
use orch8_storage::{MobileDevice, MobileSyncStore};
use orch8_types::error::StorageError;
use uuid::Uuid;

async fn store() -> Option<PostgresStorage> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let storage = PostgresStorage::new(&url, 5, None)
        .await
        .expect("connect to DATABASE_URL");
    storage.run_migrations().await.expect("run migrations");
    Some(storage)
}

macro_rules! require_postgres {
    () => {
        match store().await {
            Some(s) => s,
            None => {
                eprintln!("skipping: DATABASE_URL not set");
                return;
            }
        }
    };
}

fn device(id: &str, tenant: &str, token: Option<&str>) -> MobileDevice {
    MobileDevice {
        device_id: id.into(),
        tenant_id: tenant.into(),
        push_token: token.map(Into::into),
        platform: "ios".into(),
        app_version: None,
        active: true,
        last_sync_at: None,
        registered_at: String::new(),
    }
}

#[tokio::test]
async fn claim_returns_target_in_one_statement_and_cleanup_matches_token() {
    let s = require_postgres!();
    let scope = Uuid::new_v4().to_string();
    s.register_mobile_device(&device(&scope, &scope, Some("token-a")))
        .await
        .unwrap();
    let now = Utc::now();
    s.enqueue_wake(&scope, &scope, "cmd", now).await.unwrap();
    let wakes = s
        .claim_due_wakes(now, now + Duration::seconds(30), 1000)
        .await
        .unwrap();
    let wake = wakes
        .into_iter()
        .find(|w| w.device_id == scope)
        .expect("our wake is claimed with its device target");
    assert_eq!(wake.push_token, "token-a");
    assert_eq!(wake.platform, "ios");

    // Device re-registers a fresh token while the wake is in flight.
    s.register_mobile_device(&device(&scope, &scope, Some("token-b")))
        .await
        .unwrap();
    s.record_wake_outcome(
        &wake,
        &WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::InvalidToken,
            error: "gone".into(),
        },
        now,
    )
    .await
    .unwrap();
    let d = s.get_mobile_device(&scope).await.unwrap().unwrap();
    assert!(d.active);
    assert_eq!(d.push_token.as_deref(), Some("token-b"));
}

#[tokio::test]
async fn device_upsert_refuses_cross_tenant_overwrite_postgres() {
    let s = require_postgres!();
    let id = Uuid::new_v4().to_string();
    s.register_mobile_device(&device(&id, "tenant-a", Some("victim")))
        .await
        .unwrap();
    let err = s
        .register_mobile_device(&device(&id, "tenant-b", Some("attacker")))
        .await
        .unwrap_err();
    assert!(matches!(err, StorageError::Conflict(_)));
    let d = s.get_mobile_device(&id).await.unwrap().unwrap();
    assert_eq!(d.tenant_id, "tenant-a");
    assert_eq!(d.push_token.as_deref(), Some("victim"));
    // Same-tenant re-registration still updates.
    s.register_mobile_device(&device(&id, "tenant-a", Some("rotated")))
        .await
        .unwrap();
    let d = s.get_mobile_device(&id).await.unwrap().unwrap();
    assert_eq!(d.push_token.as_deref(), Some("rotated"));
}

#[tokio::test]
async fn prune_wakes_deletes_only_old_non_leased_rows_postgres() {
    let s = require_postgres!();
    let scope = Uuid::new_v4().to_string();
    // No push token: rows can never be claimed, the growth MOB-N1 describes.
    s.register_mobile_device(&device(&scope, &scope, None))
        .await
        .unwrap();
    let old = Utc::now() - Duration::days(30);
    s.enqueue_wake(&scope, &scope, "old", old).await.unwrap();
    s.enqueue_wake(&scope, &scope, "fresh", Utc::now())
        .await
        .unwrap();
    s.prune_wakes(Utc::now() - Duration::days(7)).await.unwrap();
    let pool = sqlx::PgPool::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    let left: Vec<String> =
        sqlx::query_scalar("SELECT command_id FROM push_wake_outbox WHERE tenant_id=$1")
            .bind(&scope)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(left, vec!["fresh".to_string()]);
    pool.close().await;
}
