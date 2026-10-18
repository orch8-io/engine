//! Push outbox worker rules: retry backoff doubles from 5 s and is capped at
//! 1280 s; configured limits are clamped (batch 1..=1000, attempts 1..=32);
//! provider errors map to exactly one retry or terminal outcome; and each
//! drain leases one row at a time, persists every outcome, propagates storage
//! errors, and stops at the batch limit.

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
        _tenant_id: &str,
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

fn retryable(message: &str) -> Result<(), PushError> {
    Err(PushError::Retryable(message.into()))
}

#[test]
fn retry_backoff_doubles_from_five_seconds_and_caps_at_1280_seconds() {
    assert_eq!(retry_delay(0), Duration::seconds(5));
    assert_eq!(retry_delay(1), Duration::seconds(5));
    for attempts in 1..9 {
        assert_eq!(
            retry_delay(attempts + 1),
            retry_delay(attempts) * 2,
            "attempt {attempts} doubles"
        );
    }
    for attempts in [9, 10, 100, u32::MAX] {
        assert_eq!(
            retry_delay(attempts),
            Duration::seconds(1280),
            "attempt {attempts} is capped"
        );
    }
}

#[test]
fn provider_errors_map_to_one_retry_or_terminal_outcome() {
    let cases = [
        (
            "retryable",
            claimed(0),
            retryable("busy"),
            WakeAttemptOutcome::Retry {
                next_attempt_at: now() + retry_delay(1),
                error: "busy".into(),
            },
        ),
        (
            "delivery error retries like retryable",
            claimed(2),
            Err(PushError::Delivery("connection reset".into())),
            WakeAttemptOutcome::Retry {
                next_attempt_at: now() + retry_delay(3),
                error: "connection reset".into(),
            },
        ),
        (
            "permanent",
            claimed(0),
            Err(PushError::Permanent("bad payload".into())),
            WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::PermanentFailure,
                error: "bad payload".into(),
            },
        ),
        (
            "config",
            claimed(0),
            Err(PushError::Config("no credentials".into())),
            WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::Misconfigured,
                error: "no credentials".into(),
            },
        ),
    ];
    for (case, wake, result, expected) in cases {
        assert_eq!(
            classify_worker(8).classify(&wake, result, now()),
            expected,
            "{case}"
        );
    }
}

#[test]
fn retryable_failure_parks_once_completed_attempts_reach_the_limit() {
    let worker = classify_worker(2);

    assert!(matches!(
        worker.classify(&claimed(0), retryable("x"), now()),
        WakeAttemptOutcome::Retry { .. }
    ));
    assert_eq!(
        worker.classify(&claimed(1), retryable("still busy"), now()),
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::RetryLimit,
            error: "still busy".into(),
        }
    );
}

fn is_retry_limit(outcome: &WakeAttemptOutcome) -> bool {
    matches!(
        outcome,
        WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::RetryLimit,
            ..
        }
    )
}

#[test]
fn attempt_limit_is_clamped_to_between_one_and_thirty_two() {
    let cases = [
        ("0 clamps to 1: first failure parks", 0, 0, true),
        ("33 clamps to 32: 31st attempt retries", 33, 30, false),
        ("33 clamps to 32: 32nd attempt parks", 33, 31, true),
    ];
    for (case, configured, prior_attempts, parks) in cases {
        let outcome =
            classify_worker(configured).classify(&claimed(prior_attempts), retryable("x"), now());
        assert_eq!(is_retry_limit(&outcome), parks, "{case}: {outcome:?}");
    }
}

#[tokio::test]
async fn batch_size_is_clamped_to_between_one_and_one_thousand() {
    let cases = [(0_u32, 3_usize, 1_usize), (1_001, 1_001, 1_000)];
    for (configured, queued, drained) in cases {
        let store = Arc::new(DrainRecorder {
            wakes: Mutex::new(vec![claimed(0); queued]),
            ..Default::default()
        });
        let worker =
            PushOutboxWorker::new(store.clone(), Arc::new(SilentRecordingProvider::default()))
                .with_limits(configured, 8);

        assert_eq!(
            worker.drain_once(now()).await.unwrap(),
            drained,
            "batch size {configured}"
        );
        assert_eq!(
            store.wakes.lock().unwrap().len(),
            queued - drained,
            "batch size {configured} leaves the rest queued"
        );
    }
}

type ClaimCall = (DateTime<Utc>, DateTime<Utc>, u32);

#[derive(Default)]
struct DrainRecorder {
    wakes: Mutex<Vec<ClaimedWake>>,
    claim_error: Mutex<Option<String>>,
    outcome_error: Mutex<Option<String>>,
    claimed_now: Mutex<Option<DateTime<Utc>>>,
    claimed_lease_until: Mutex<Option<DateTime<Utc>>>,
    claimed_limit: Mutex<Option<u32>>,
    claims: Mutex<Vec<ClaimCall>>,
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
        self.claims.lock().unwrap().push((now, lease_until, limit));
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
        _tenant_id: &str,
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
async fn drain_claims_a_batch_for_the_free_slots_at_a_fixed_due_time() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0)]),
        ..Default::default()
    });
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store.clone(), provider).with_limits(7, 8);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 1);
    let claims = store.claims.lock().unwrap();
    // One claim asks for as many wakes as the batch allows (7 < 32 slots); it
    // returns fewer, so the queue is treated as drained.
    assert_eq!(claims.len(), 1);
    assert_eq!(claims[0].0, now());
    assert_eq!(claims[0].2, 7);
    // The lease must outlast the whole provider budget, or another node
    // could reclaim the wake while this one is still sending it.
    let provider_budget = Duration::from_std(worker.provider_timeout).unwrap();
    assert!(claims[0].1 - claims[0].0 > provider_budget);
}

#[tokio::test]
async fn empty_claim_drains_nothing_and_never_calls_the_provider() {
    let store = Arc::new(DrainRecorder::default());
    let provider = Arc::new(SilentRecordingProvider::default());
    let worker = PushOutboxWorker::new(store, provider.clone());

    assert_eq!(worker.drain_once(now()).await.unwrap(), 0);
    assert!(provider.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn claim_error_propagates_without_calling_the_provider() {
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
async fn outcome_persistence_error_propagates() {
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
async fn unsigned_drain_sends_each_wake_token_and_platform() {
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
async fn signed_drain_sends_verifiable_five_minute_wakes_with_unique_nonces() {
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
        assert!(metadata.issued_at >= now());
        assert!(metadata.issued_at < now() + Duration::seconds(1));
        assert_eq!(
            metadata.expires_at - metadata.issued_at,
            Duration::minutes(5)
        );
        let mut nonces = crate::WakeNonceCache::new(8);
        assert_eq!(
            metadata.verify(
                "tenant-a",
                "device-a",
                metadata.issued_at,
                &signing_key.verifying_key(),
                &mut nonces,
            ),
            Ok(())
        );
    }
}

#[tokio::test]
async fn drain_stops_at_the_batch_limit_and_leaves_the_rest_queued() {
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

struct SlowRecordingProvider;

#[async_trait]
impl PushProvider for SlowRecordingProvider {
    async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        Ok(())
    }
}

/// Tracks how many provider calls overlap.
#[derive(Default)]
struct OverlapProvider {
    now: std::sync::atomic::AtomicUsize,
    max: std::sync::atomic::AtomicUsize,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl PushProvider for OverlapProvider {
    async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
        use std::sync::atomic::Ordering::SeqCst;
        let now = self.now.fetch_add(1, SeqCst) + 1;
        self.max.fetch_max(now, SeqCst);
        self.calls.fetch_add(1, SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        self.now.fetch_sub(1, SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn drain_sends_a_claimed_batch_concurrently_up_to_the_configured_bound() {
    use std::sync::atomic::Ordering::SeqCst;
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0); 9]),
        ..Default::default()
    });
    let provider = Arc::new(OverlapProvider::default());
    let worker = PushOutboxWorker::new(store.clone(), provider.clone())
        .with_limits(100, 8)
        .with_concurrency(3);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 9);
    assert_eq!(provider.calls.load(SeqCst), 9);
    assert_eq!(
        provider.max.load(SeqCst),
        3,
        "bounded concurrency, not sequential"
    );
    assert_eq!(store.outcomes.lock().unwrap().len(), 9);
    // Slots are refilled as calls finish: the first claim fills the 3 slots,
    // later claims top up what freed.
    let claims = store.claims.lock().unwrap();
    assert_eq!(claims[0].2, 3);
    assert!(claims.iter().all(|claim| claim.0 == now()));
}

struct HangingForTokenProvider;

#[async_trait]
impl PushProvider for HangingForTokenProvider {
    async fn send_silent_push(&self, token: &str, _platform: &str) -> Result<(), PushError> {
        if token == "hang" {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[tokio::test]
async fn one_hung_provider_call_does_not_block_the_wakes_behind_it() {
    let mut hung = claimed(0);
    hung.push_token = "hang".into();
    let mut wakes = vec![hung];
    wakes.extend(vec![claimed(0); 5]);
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(wakes),
        ..Default::default()
    });
    let mut worker = PushOutboxWorker::new(store.clone(), Arc::new(HangingForTokenProvider))
        .with_limits(100, 8)
        .with_concurrency(2);
    worker.provider_timeout = std::time::Duration::from_millis(300);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 6);
    let outcomes = store.outcomes.lock().unwrap();
    assert_eq!(outcomes.len(), 6);
    // The five healthy wakes were delivered while the hung one was still
    // pending; the timed-out call is recorded last.
    assert!(
        outcomes[..5]
            .iter()
            .all(|o| matches!(o, WakeAttemptOutcome::Delivered))
    );
    assert!(matches!(outcomes[5], WakeAttemptOutcome::Retry { .. }));
}

#[tokio::test]
async fn dropping_the_drain_aborts_in_flight_calls_so_shutdown_is_prompt() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0); 4]),
        ..Default::default()
    });
    let mut hung = claimed(0);
    hung.push_token = "hang".into();
    store.wakes.lock().unwrap().insert(0, hung);
    // Default provider timeout is 75 s; cancellation must not wait for it.
    let worker = PushOutboxWorker::new(store.clone(), Arc::new(HangingForTokenProvider));
    let started = std::time::Instant::now();
    let result = tokio::time::timeout(
        std::time::Duration::from_millis(100),
        worker.drain_once(now()),
    )
    .await;
    assert!(result.is_err(), "drain is still waiting on the hung call");
    assert!(started.elapsed() < std::time::Duration::from_secs(5));
}

#[tokio::test]
async fn concurrency_is_clamped_to_between_one_and_two_fifty_six() {
    let store = Arc::new(DrainRecorder::default());
    let provider = Arc::new(SilentRecordingProvider::default());
    let low = PushOutboxWorker::new(store.clone(), provider.clone()).with_concurrency(0);
    let high = PushOutboxWorker::new(store, provider).with_concurrency(10_000);
    assert_eq!(low.concurrency, 1);
    assert_eq!(high.concurrency, 256);
}

#[tokio::test]
async fn drain_times_out_a_stalled_provider_before_its_lease_expires() {
    let store = Arc::new(DrainRecorder {
        wakes: Mutex::new(vec![claimed(0)]),
        ..Default::default()
    });
    let mut worker =
        PushOutboxWorker::new(store.clone(), Arc::new(SlowRecordingProvider)).with_limits(1, 8);
    worker.provider_timeout = std::time::Duration::from_millis(1);
    worker.lease_duration = Duration::seconds(1);

    assert_eq!(worker.drain_once(now()).await.unwrap(), 1);
    let outcomes = store.outcomes.lock().unwrap();
    assert!(matches!(
        outcomes.as_slice(),
        [WakeAttemptOutcome::Retry { error, .. }] if error == "push provider timed out"
    ));
    let claims = store.claims.lock().unwrap();
    assert!(claims[0].1 - claims[0].0 >= Duration::seconds(1));
}
