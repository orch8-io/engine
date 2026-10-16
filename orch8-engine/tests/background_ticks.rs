//! Single-pass entry points of the background loops, driven on virtual
//! "now" instead of sleeping through the loops' intervals: `gc::gc_tick`
//! (here), `webhooks::outbox_tick` and `credentials::refresh_pass_at` (see
//! `http_mock.rs` for tests that need a receiver).

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;

use orch8_engine::gc::{GcOptions, gc_tick};
use orch8_storage::{DedupeScope, EmitDedupeOutcome, StorageBackend};
use orch8_types::ids::InstanceId;

mod common;

async fn seeded_dedupe_row(storage: &Arc<dyn StorageBackend>) -> (DedupeScope, &'static str) {
    let scope = DedupeScope::Parent(InstanceId::new());
    storage
        .record_or_get_emit_dedupe(&scope, "k", InstanceId::new())
        .await
        .unwrap();
    (scope, "k")
}

/// `Inserted` = the old dedupe row is gone, `AlreadyExists` = still there.
async fn probe(storage: &Arc<dyn StorageBackend>, scope: &DedupeScope, key: &str) -> bool {
    matches!(
        storage
            .record_or_get_emit_dedupe(scope, key, InstanceId::new())
            .await
            .unwrap(),
        EmitDedupeOutcome::AlreadyExists(_)
    )
}

#[tokio::test]
async fn gc_tick_past_the_dedupe_ttl_deletes_the_dedupe_row() {
    let storage = common::storage().await;
    let (scope, key) = seeded_dedupe_row(&storage).await;
    let ttl = Duration::from_secs(3600);
    let options = GcOptions {
        dedupe_ttl: ttl,
        ..GcOptions::default()
    };

    gc_tick(
        storage.as_ref(),
        &options,
        Utc::now() + chrono::Duration::hours(2),
    )
    .await;

    assert!(
        !probe(&storage, &scope, key).await,
        "expired row must be swept"
    );
}

#[tokio::test]
async fn gc_tick_within_the_dedupe_ttl_keeps_the_dedupe_row() {
    let storage = common::storage().await;
    let (scope, key) = seeded_dedupe_row(&storage).await;
    let options = GcOptions {
        dedupe_ttl: Duration::from_secs(3600),
        ..GcOptions::default()
    };

    gc_tick(
        storage.as_ref(),
        &options,
        Utc::now() + chrono::Duration::minutes(30),
    )
    .await;

    assert!(probe(&storage, &scope, key).await, "fresh row must survive");
}
