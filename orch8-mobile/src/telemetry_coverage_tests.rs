//! Telemetry coverage: buffer/batch sizing contract, batch preparation, and
//! the auto-flush attempt throttle.
//!
//! Count contract: 10 independently named unit tests.

use super::*;
use orch8_types::clock::{Clock, ManualClock};

fn device_ctx() -> DeviceContext {
    DeviceContext {
        device_id: "dev-1".to_string(),
        os_name: "iOS".to_string(),
        os_version: "17.0".to_string(),
        app_version: "1.0.0".to_string(),
        sdk_version: "0.4.0".to_string(),
    }
}

async fn manager_with_clock(clock: SharedClock) -> TelemetryManager {
    let sqlite = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .unwrap(),
    );
    let storage = Arc::new(MobileStorage::new(sqlite));
    TelemetryManager::new_with_clock(storage, true, device_ctx(), clock)
}

fn stored_event(id: i64, event_type: &str, payload: &str) -> crate::storage::TelemetryEvent {
    crate::storage::TelemetryEvent {
        id,
        event_type: event_type.to_string(),
        payload: payload.to_string(),
        created_at: "2026-07-25T12:00:00Z".to_string(),
    }
}

#[test]
fn coverage_telemetry_001_upload_batch_stays_below_offline_buffer_cap() {
    assert_eq!(MAX_BUFFER_SIZE, 1000);
    assert_eq!(MAX_UPLOAD_BATCH_SIZE, 500);
    assert!(MAX_UPLOAD_BATCH_SIZE < MAX_BUFFER_SIZE);
}

#[test]
fn coverage_telemetry_002_auto_flush_threshold_is_eighty_percent() {
    assert_eq!(AUTO_FLUSH_PCT, 80);
    assert_eq!(
        u64::from(MAX_BUFFER_SIZE) * u64::from(AUTO_FLUSH_PCT) / 100,
        800
    );
}

#[test]
fn coverage_telemetry_003_capacity_trim_targets_ninety_percent() {
    assert_eq!(CAPACITY_TRIM_PCT, 90);
    assert_eq!(
        u64::from(MAX_BUFFER_SIZE) * u64::from(CAPACITY_TRIM_PCT) / 100,
        900
    );
}

#[test]
fn coverage_telemetry_004_record_new_stamps_rfc3339_timestamp() {
    let record = TelemetryEventRecord::new("StepFinished", r#"{"ok":true}"#);

    assert_eq!(record.event_type, "StepFinished");
    assert_eq!(record.payload, r#"{"ok":true}"#);
    assert!(
        chrono::DateTime::parse_from_rfc3339(&record.timestamp).is_ok(),
        "timestamp must be RFC 3339: {}",
        record.timestamp
    );
}

#[tokio::test]
async fn coverage_telemetry_005_prepared_batch_handles_empty_event_list() {
    let mgr = manager_with_clock(SharedClock::default()).await;

    let prepared = mgr.prepare_batch(&[]).unwrap();

    assert!(prepared.ids.is_empty());
    let body: serde_json::Value = serde_json::from_str(&prepared.body).unwrap();
    assert_eq!(body["events"], serde_json::json!([]));
    assert_eq!(body["device"]["device_id"], "dev-1");
}

#[tokio::test]
async fn coverage_telemetry_006_prepared_batch_embeds_payload_verbatim() {
    let mgr = manager_with_clock(SharedClock::default()).await;
    let events = vec![stored_event(7, "Custom", r#"{"raw":[1,2,3]}"#)];

    let prepared = mgr.prepare_batch(&events).unwrap();

    assert_eq!(prepared.ids, [7]);
    let body: serde_json::Value = serde_json::from_str(&prepared.body).unwrap();
    assert_eq!(body["events"][0]["payload"], r#"{"raw":[1,2,3]}"#);
    assert_eq!(body["events"][0]["timestamp"], "2026-07-25T12:00:00Z");
}

#[tokio::test]
async fn coverage_telemetry_007_auto_flush_claim_throttles_attempts() {
    let start = chrono::Utc::now();
    let manual = Arc::new(ManualClock::new(start));
    let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
    let mgr = manager_with_clock(clock).await;

    assert!(mgr.claim_auto_flush_attempt(), "first claim succeeds");
    assert!(
        !mgr.claim_auto_flush_attempt(),
        "second claim inside the cooldown is refused"
    );

    manual.advance(chrono::Duration::seconds(AUTO_FLUSH_COOLDOWN_SECS - 1));
    assert!(!mgr.claim_auto_flush_attempt());

    manual.advance(chrono::Duration::seconds(1));
    assert!(
        mgr.claim_auto_flush_attempt(),
        "claim after cooldown succeeds"
    );
}

#[tokio::test]
async fn coverage_telemetry_008_below_threshold_scan_does_not_claim_attempt() {
    let start = chrono::Utc::now();
    let manual = Arc::new(ManualClock::new(start));
    let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
    let mgr = manager_with_clock(clock).await;

    let threshold = u64::from(MAX_BUFFER_SIZE) * u64::from(AUTO_FLUSH_PCT) / 100;
    mgr.maybe_auto_flush(threshold - 1).await;

    assert!(
        mgr.last_flush_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none(),
        "a below-threshold record must not spend an auto-flush attempt"
    );
}

#[tokio::test]
async fn coverage_telemetry_009_threshold_crossing_claims_once_per_cooldown() {
    let start = chrono::Utc::now();
    let manual = Arc::new(ManualClock::new(start));
    let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
    let mgr = manager_with_clock(clock).await;

    let threshold = u64::from(MAX_BUFFER_SIZE) * u64::from(AUTO_FLUSH_PCT) / 100;
    mgr.maybe_auto_flush(threshold).await;
    let claimed_at = *mgr
        .last_flush_attempt
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(claimed_at, Some(start));

    // No endpoint has ever flushed successfully, so no network attempt fires;
    // the claimed cooldown must still suppress a second crossing.
    mgr.maybe_auto_flush(threshold + 1).await;
    let still_claimed_at = *mgr
        .last_flush_attempt
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(still_claimed_at, Some(start));
}

#[test]
fn coverage_telemetry_010_storage_error_maps_to_mobile_variant() {
    let error = mobile_storage_error(orch8_types::error::StorageError::Query("boom".to_string()));

    match error {
        MobileError::Storage { message } => assert!(message.contains("boom")),
        other => panic!("expected MobileError::Storage, got {other:?}"),
    }
}
