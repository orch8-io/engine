//! Postgres regressions for the push wake outbox and mobile device upsert
//! (MOB-N2, STO-N11/MOB-N8, API-N6/STO-M4, MOB-N1 retention sweep).
//!
//! Gated on `DATABASE_URL` via `orch8_storage::testing::backends` (skips
//! locally, fails under CI); each test gets its own schema.

use chrono::{Duration, Utc};
use orch8_push::{PushOutboxStore, PushTerminalReason, WakeAttemptOutcome};
use orch8_storage::testing::backends::TestPostgres;
use orch8_storage::{MobileDevice, MobileSyncStore};
use orch8_types::error::StorageError;
use uuid::Uuid;

async fn store() -> Option<TestPostgres> {
    TestPostgres::fresh().await
}

macro_rules! require_postgres {
    () => {
        match store().await {
            Some(s) => s,
            None => return,
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
    let pool = s.pool().clone();
    let left: Vec<String> =
        sqlx::query_scalar("SELECT command_id FROM push_wake_outbox WHERE tenant_id=$1")
            .bind(&scope)
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(left, vec!["fresh".to_string()]);
    pool.close().await;
}

#[tokio::test]
async fn command_ack_does_not_mark_another_tenants_wake_for_the_same_device_id_postgres() {
    let s = require_postgres!();
    let device_id = Uuid::new_v4().to_string();
    let tenant_a = format!("a-{device_id}");
    let tenant_b = format!("b-{device_id}");
    let now = Utc::now();
    s.enqueue_wake(&tenant_a, &device_id, "cmd-1", now)
        .await
        .unwrap();
    s.enqueue_wake(&tenant_b, &device_id, "cmd-1", now)
        .await
        .unwrap();

    let acked = s
        .record_command_acks(&tenant_a, &device_id, &["cmd-1".into()], now)
        .await
        .unwrap();

    let acked_tenants: Vec<String> = sqlx::query_scalar(
        "SELECT tenant_id FROM push_wake_outbox WHERE device_id=$1 AND command_acked_at IS NOT NULL",
    )
    .bind(&device_id)
    .fetch_all(s.pool())
    .await
    .unwrap();
    assert_eq!(acked, 1);
    assert_eq!(acked_tenants, vec![tenant_a]);
}
