//! Push outbox worker coverage: backoff schedule, limit clamps, outcome
//! classification, and the drain/claim/persist contract.
//!
//! Count contract: 31 independently named unit tests.

use std::sync::Mutex;

use super::*;

fn now() -> DateTime<Utc> {
    DateTime::from_timestamp(1_800_000_000, 0).unwrap()
}

fn claimed(attempts: u32) -> ClaimedWake {
    ClaimedWake {
        id: Uuid::nil(),
        tenant_id: "tenant-a".into(),
        device_id: "device-a".into(),
        command_id: "command-a".into(),
        push_token: "token-a".into(),
        platform: "android".into(),
        attempts,
        lease_until: now() + Duration::seconds(30),
    }
}

/// Minimal store used to construct a worker for pure `classify` assertions.
struct NullStore;

#[async_trait]
impl PushOutboxStore for NullStore {
    async fn enqueue_wake(
        &self,
        _tenant_id: &str,
        _device_id: &str,
        _command_id: &str,
        _created_at: DateTime<Utc>,
    ) -> Result<Uuid, String> {
        Ok(Uuid::nil())
    }

    async fn claim_due_wakes(
        &self,
        _now: DateTime<Utc>,
        _lease_until: DateTime<Utc>,
        _limit: u32,
    ) -> Result<Vec<ClaimedWake>, String> {
        Ok(Vec::new())
    }

    async fn record_wake_outcome(
        &self,
        _wake: &ClaimedWake,
        _outcome: &WakeAttemptOutcome,
        _recorded_at: DateTime<Utc>,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn record_command_acks(
        &self,
        _device_id: &str,
        _command_ids: &[String],
        _acked_at: DateTime<Utc>,
    ) -> Result<u64, String> {
        Ok(0)
    }
}

fn classify_worker(max_attempts: u32) -> PushOutboxWorker {
    PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(1, max_attempts)
}

macro_rules! retry_delay_case {
    ($name:ident, $attempts:expr, $seconds:expr) => {
        #[test]
        fn $name() {
            assert_eq!(retry_delay($attempts), Duration::seconds($seconds));
        }
    };
}

retry_delay_case!(coverage_outbox_001_first_retry_waits_five_seconds, 1, 5);
retry_delay_case!(coverage_outbox_002_second_retry_doubles_to_ten, 2, 10);
retry_delay_case!(coverage_outbox_003_third_retry_waits_twenty, 3, 20);
retry_delay_case!(coverage_outbox_004_fourth_retry_waits_forty, 4, 40);
retry_delay_case!(coverage_outbox_005_fifth_retry_waits_eighty, 5, 80);
retry_delay_case!(coverage_outbox_006_sixth_retry_waits_one_sixty, 6, 160);
retry_delay_case!(coverage_outbox_007_seventh_retry_waits_three_twenty, 7, 320);
retry_delay_case!(coverage_outbox_008_eighth_retry_waits_six_forty, 8, 640);
retry_delay_case!(coverage_outbox_009_ninth_retry_is_capped, 9, 1280);
retry_delay_case!(
    coverage_outbox_010_max_attempts_never_overflows,
    u32::MAX,
    1280
);

#[test]
fn coverage_outbox_011_retry_delay_saturates_beyond_the_table() {
    // Zero completed attempts still waits the base delay (no sub-base panic).
    assert_eq!(retry_delay(0), Duration::seconds(5));
    // Anything past the exponent cap stays at the 1280s ceiling.
    assert_eq!(retry_delay(10), Duration::seconds(1280));
    assert_eq!(retry_delay(100), Duration::seconds(1280));
}

#[test]
fn coverage_outbox_012_batch_size_zero_clamps_to_one() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(0, 8);
    assert_eq!(worker.batch_size, 1);
}

#[test]
fn coverage_outbox_013_batch_size_above_max_clamps_to_thousand() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(1_001, 8);
    assert_eq!(worker.batch_size, 1_000);
}

#[test]
fn coverage_outbox_014_batch_size_at_max_is_preserved() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(1_000, 8);
    assert_eq!(worker.batch_size, 1_000);
}

#[test]
fn coverage_outbox_015_max_attempts_zero_clamps_to_one() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(1, 0);
    assert_eq!(worker.max_attempts, 1);
}

#[test]
fn coverage_outbox_016_max_attempts_above_ceiling_clamps_to_32() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider))
        .with_limits(1, 33);
    assert_eq!(worker.max_attempts, 32);
}

#[test]
fn coverage_outbox_017_default_limits_are_applied() {
    let worker = PushOutboxWorker::new(Arc::new(NullStore), Arc::new(crate::NoopPushProvider));
    assert_eq!(worker.batch_size, 100);
    assert_eq!(worker.max_attempts, 8);
    assert_eq!(worker.lease_duration, Duration::seconds(30));
}

#[test]
fn coverage_outbox_018_successful_send_classifies_delivered() {
    let outcome = classify_worker(8).classify(&claimed(0), Ok(()), now());
    assert_eq!(outcome, WakeAttemptOutcome::Delivered);
}

#[test]
fn coverage_outbox_019_invalid_token_terminates_with_pinned_message() {
    let outcome = classify_worker(8).classify(&claimed(0), Err(PushError::InvalidToken), now());
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::InvalidToken,
            error: "device token rejected by provider".into(),
        }
    );
}

#[test]
fn coverage_outbox_020_retryable_retries_with_exact_backoff_and_error() {
    let outcome =
        classify_worker(8).classify(&claimed(0), Err(PushError::Retryable("busy".into())), now());
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Retry {
            next_attempt_at: now() + retry_delay(1),
            error: "busy".into(),
        }
    );
}

#[test]
fn coverage_outbox_021_delivery_error_retries_like_retryable() {
    let outcome = classify_worker(8).classify(
        &claimed(2),
        Err(PushError::Delivery("connection reset".into())),
        now(),
    );
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Retry {
            next_attempt_at: now() + retry_delay(3),
            error: "connection reset".into(),
        }
    );
}

#[test]
fn coverage_outbox_022_permanent_error_terminates_without_retry() {
    let outcome = classify_worker(8).classify(
        &claimed(0),
        Err(PushError::Permanent("bad payload".into())),
        now(),
    );
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::PermanentFailure,
            error: "bad payload".into(),
        }
    );
}

#[test]
fn coverage_outbox_023_config_error_terminates_as_misconfigured() {
    let outcome = classify_worker(8).classify(
        &claimed(0),
        Err(PushError::Config("no credentials".into())),
        now(),
    );
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::Misconfigured,
            error: "no credentials".into(),
        }
    );
}

#[test]
fn coverage_outbox_024_retry_limit_boundary_is_inclusive_and_preserves_error() {
    let worker = classify_worker(2);
    // Completing this attempt reaches 1 of 2 allowed: still retries.
    assert!(matches!(
        worker.classify(&claimed(0), Err(PushError::Retryable("x".into())), now()),
        WakeAttemptOutcome::Retry { .. }
    ));
    // Completing this attempt reaches the limit: the wake parks terminally.
    let outcome = worker.classify(
        &claimed(1),
        Err(PushError::Retryable("still busy".into())),
        now(),
    );
    assert_eq!(
        outcome,
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::RetryLimit,
            error: "still busy".into(),
        }
    );
}

#[derive(Default)]
struct DrainRecorder {
    wakes: Mutex<Vec<ClaimedWake>>,
    claim_error: Mutex<Option<String>>,
    outcome_error: Mutex<Option<String>>,
    claimed_now: Mutex<Option<DateTime<Utc>>>,
    claimed_lease_until: Mutex<Option<DateTime<Utc>>>,
    claimed_limit: Mutex<Option<u32>>,
    outcomes: Mutex<Vec<WakeAttemptOutcome>>,
}

#[async_trait]
impl PushOutboxStore for DrainRecorder {
    async fn enqueue_wake(
        &self,
        _tenant_id: &str,
        _device_id: &str,
        _command_id: &str,
        _created_at: DateTime<Utc>,
    ) -> Result<Uuid, String> {
        Ok(Uuid::nil())
    }

    async fn claim_due_wakes(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ClaimedWake>, String> {
        *self.claimed_now.lock().unwrap() = Some(now);
        *self.claimed_lease_until.lock().unwrap() = Some(lease_until);
        *self.claimed_limit.lock().unwrap() = Some(limit);
        if let Some(error) = self.claim_error.lock().unwrap().take() {
            return Err(error);
        }
        let mut wakes = self.wakes.lock().unwrap();
        let count = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .min(wakes.len());
        Ok(wakes.drain(..count).collect())
    }

    async fn record_wake_outcome(
        &self,
        _wake: &ClaimedWake,
        outcome: &WakeAttemptOutcome,
        _recorded_at: DateTime<Utc>,
    ) -> Result<(), String> {
        if let Some(error) = self.outcome_error.lock().unwrap().as_ref() {
            return Err(error.clone());
        }
        self.outcomes.lock().unwrap().push(outcome.clone());
        Ok(())
    }

    async fn record_command_acks(
        &self,
        _device_id: &str,
        command_ids: &[String],
        _acked_at: DateTime<Utc>,
    ) -> Result<u64, String> {
        u64::try_from(command_ids.len()).map_err(|error| error.to_string())
    }
}

#[derive(Default)]
struct SilentRecordingProvider(Mutex<Vec<(String, String)>>);

#[async_trait]
impl PushProvider for SilentRecordingProvider {
    async fn send_silent_push(&self, token: &str, platform: &str) -> Result<(), PushError> {
        self.0.lock().unwrap().push((token.into(), platform.into()));
        Ok(())
    }
}

#[tokio::test]
async fn coverage_outbox_025_drain_claims_with_now_lease_and_batch_limit() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0)]),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store.clone(), provider).with_limits(7, 8);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 1);
    assert_eq!(*store.claimed_now.lock().unwrap(), Some(now()));
    assert_eq!(
        *store.claimed_lease_until.lock().unwrap(),
        Some(now() + Duration::seconds(30))
    );
    assert_eq!(*store.claimed_limit.lock().unwrap(), Some(7));
}

#[tokio::test]
async fn coverage_outbox_026_empty_claim_returns_zero_without_provider_calls() {
    let store = Arc::new(DrainRecorder::default());
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store, provider.clone());

    assert_eq!(worker.drain_once(now()).await.unwrap(), 0);
    assert!(provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn coverage_outbox_027_claim_error_propagates_without_provider_calls() {
    let store = Arc::new(DrainRecorder {
        claim_error: Mutex::new(Some("storage down".into())),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store, provider.clone());

    assert_eq!(worker.drain_once(now()).await, Err("storage down".into()));
    assert!(provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn coverage_outbox_028_outcome_persistence_error_propagates() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0)]),
        outcome_error: Mutex::new(Some("db gone".into())),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store, provider);

    assert_eq!(worker.drain_once(now()).await, Err("db gone".into()));
}

#[tokio::test]
async fn coverage_outbox_029_unsigned_drain_sends_token_and_platform_per_wake() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0), claimed(0)]),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store.clone(), provider.clone());

    assert_eq!(worker.drain_once(now()).await.unwrap(), 2);
    assert_eq!(
        provider.0.lock().unwrap().as_slice(),
        [
            ("token-a".to_string(), "android".to_string()),
            ("token-a".to_string(), "android".to_string()),
        ]
    );
    assert_eq!(store.outcomes.lock().unwrap().len(), 2);
}

#[derive(Default)]
struct MetadataRecorder(Mutex<Vec<crate::SignedWakeMetadata>>);

#[async_trait]
impl PushProvider for MetadataRecorder {
    async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
        panic!("signed worker must not send unsigned wakes");
    }

    async fn send_signed_wake(
        &self,
        _token: &str,
        _platform: &str,
        metadata: &crate::SignedWakeMetadata,
    ) -> Result<(), PushError> {
        self.0.lock().unwrap().push(metadata.clone());
        Ok(())
    }
}

#[tokio::test]
async fn coverage_outbox_030_signed_drain_binds_five_minute_expiry_and_unique_nonces() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0), claimed(0)]),
        ..Default::default()
    });

    let provider = Arc::new(MetadataRecorder::default());
    let signing_key = SigningKey::from_bytes(&[9; 32]);
    let worker = PushOutboxWorker::new(store, provider.clone())
        .with_wake_signer("push-k9", signing_key.clone());

    assert_eq!(worker.drain_once(now()).await.unwrap(), 2);
    let sent = provider.0.lock().unwrap();
    assert_eq!(sent.len(), 2);
    assert_ne!(sent[0].nonce, sent[1].nonce);
    for metadata in sent.iter() {
        assert_eq!(metadata.key_id, "push-k9");
        assert_eq!(metadata.issued_at, now());
        assert_eq!(metadata.expires_at, now() + Duration::minutes(5));
        let mut nonces = crate::WakeNonceCache::new(8);
        assert_eq!(
            metadata.verify(
                "tenant-a",
                "device-a",
                now(),
                &signing_key.verifying_key(),
                &mut nonces,
            ),
            Ok(())
        );
    }
}

#[tokio::test]
async fn coverage_outbox_031_drain_stops_at_batch_limit_and_leaves_the_rest() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0), claimed(0), claimed(0)]),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store.clone(), provider.clone()).with_limits(2, 8);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 2);
    // Two wakes sent and recorded; the third stays queued for the next drain.
    assert_eq!(provider.0.lock().unwrap().len(), 2);
    assert_eq!(store.outcomes.lock().unwrap().len(), 2);
    assert_eq!(store.wakes.lock().unwrap().len(), 1);
}
