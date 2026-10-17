//! Durable push wake delivery state machine.
//!
//! Storage owns claiming and outcome persistence; this crate owns provider
//! dispatch, retry classification, bounded backoff, and terminal semantics.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use ed25519_dalek::SigningKey;
use uuid::Uuid;

use crate::{CollapsibleWake, PushError, PushProvider};

/// A leased wake returned by durable storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaimedWake {
    pub id: Uuid,
    pub tenant_id: String,
    pub device_id: String,
    pub command_id: String,
    pub push_token: String,
    pub platform: String,
    /// Attempts completed before this lease.
    pub attempts: u32,
    /// Fencing value used when persisting this claimant's outcome.
    pub lease_until: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushTerminalReason {
    InvalidToken,
    PermanentFailure,
    Misconfigured,
    RetryLimit,
}

/// Persisted result of one provider call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeAttemptOutcome {
    Delivered,
    Retry {
        next_attempt_at: DateTime<Utc>,
        error: String,
    },
    Terminal {
        reason: PushTerminalReason,
        error: String,
    },
}

/// Persistence boundary for a multi-node-safe push outbox.
#[async_trait]
pub trait PushOutboxStore: Send + Sync + 'static {
    /// Persist a wake correlated to a durable mobile command. Re-enqueueing
    /// the same tenant/device/command tuple must be idempotent.
    async fn enqueue_wake(
        &self,
        tenant_id: &str,
        device_id: &str,
        command_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<Uuid, String>;

    /// Persist an execution-scoped wake while terminally superseding older
    /// pending rows with the same collapse key. Backends should override this
    /// default transactionally; the default preserves correctness without
    /// collapse for third-party stores.
    async fn enqueue_collapsible_wake(&self, wake: &CollapsibleWake) -> Result<Uuid, String> {
        self.enqueue_wake(
            &wake.tenant_id,
            &wake.device_id,
            &wake.command_id,
            wake.created_at,
        )
        .await
    }

    /// Atomically lease due rows. Implementations must prevent concurrent
    /// workers from receiving the same wake until `lease_until` expires.
    async fn claim_due_wakes(
        &self,
        now: DateTime<Utc>,
        lease_until: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<ClaimedWake>, String>;

    /// Atomically persist the result and clear the lease. Retry outcomes
    /// return the row to the due queue; terminal outcomes never retry.
    async fn record_wake_outcome(
        &self,
        wake: &ClaimedWake,
        outcome: &WakeAttemptOutcome,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), String>;

    /// Correlate device acknowledgements with the wake records of
    /// `device_id` **within `tenant_id`**. Wake rows are unique per
    /// `(tenant, device, command)`, so an ack must never mark another
    /// tenant's row for a colliding device/command id.
    async fn record_command_acks(
        &self,
        tenant_id: &str,
        device_id: &str,
        command_ids: &[String],
        acked_at: DateTime<Utc>,
    ) -> Result<u64, String>;

    /// Retention sweep: delete wake rows created before `created_before` that
    /// are not currently leased (`in_flight`). Delivered/terminal rows are
    /// history; pending rows that old target devices that never became
    /// claimable (no token, inactive, provider unconfigured) — the command
    /// itself is still delivered by device polling. Returns rows deleted.
    /// The default is a no-op for third-party stores.
    async fn prune_wakes(&self, created_before: DateTime<Utc>) -> Result<u64, String> {
        let _ = created_before;
        Ok(0)
    }
}

/// Bounded outbox worker. Safe to run on every server node when the store's
/// claim operation provides leases/`SKIP LOCKED` semantics.
pub struct PushOutboxWorker {
    store: Arc<dyn PushOutboxStore>,
    provider: Arc<dyn PushProvider>,
    batch_size: u32,
    lease_duration: Duration,
    provider_timeout: std::time::Duration,
    max_attempts: u32,
    wake_signer: Option<(String, SigningKey)>,
    concurrency: u32,
}

/// Default number of provider calls one drain keeps in flight.
pub const DEFAULT_DELIVERY_CONCURRENCY: u32 = 32;

impl PushOutboxWorker {
    #[must_use]
    pub fn new(store: Arc<dyn PushOutboxStore>, provider: Arc<dyn PushProvider>) -> Self {
        Self {
            store,
            provider,
            batch_size: 100,
            // FCM can make three token requests and three delivery requests,
            // each with a 10-second HTTP timeout, plus short retry backoffs.
            lease_duration: Duration::seconds(90),
            provider_timeout: std::time::Duration::from_secs(75),
            max_attempts: 8,
            wake_signer: None,
            concurrency: DEFAULT_DELIVERY_CONCURRENCY,
        }
    }

    /// Bound on simultaneous provider calls within one drain (1..=256,
    /// default [`DEFAULT_DELIVERY_CONCURRENCY`]). Sequential delivery capped a
    /// node at roughly one wake per provider round-trip and let a single hung
    /// call stall everything behind it.
    #[must_use]
    pub fn with_concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = concurrency.clamp(1, 256);
        self
    }

    #[must_use]
    pub fn with_limits(mut self, batch_size: u32, max_attempts: u32) -> Self {
        self.batch_size = batch_size.clamp(1, 1_000);
        self.max_attempts = max_attempts.clamp(1, 32);
        self
    }

    #[must_use]
    pub fn with_wake_signer(mut self, key_id: impl Into<String>, key: SigningKey) -> Self {
        self.wake_signer = Some((key_id.into(), key));
        self
    }

    /// Delete wake rows older than `retention` (see
    /// [`PushOutboxStore::prune_wakes`]). Runs whether or not the provider
    /// is configured, so an unconfigured deployment's outbox stays bounded.
    pub async fn prune_once(&self, now: DateTime<Utc>, retention: Duration) -> Result<u64, String> {
        self.store.prune_wakes(now - retention).await
    }

    /// Claim and deliver one bounded batch, returning the number attempted.
    ///
    /// Up to `concurrency` provider calls run at once; a slot freed by a
    /// finished call is refilled with a fresh claim, so one hung call occupies
    /// one slot instead of blocking the whole queue. Dropping the returned
    /// future (shutdown) aborts in-flight calls; their leases simply expire
    /// and the wakes are re-driven.
    ///
    /// # Errors
    /// The first claim or outcome-persistence error; calls already in flight
    /// are still allowed to finish and persist before it is returned.
    pub async fn drain_once(&self, now: DateTime<Utc>) -> Result<usize, String> {
        if !self.provider.is_configured() {
            return Ok(0);
        }
        let started = std::time::Instant::now();
        let mut claimed_total: u32 = 0;
        let mut exhausted = false;
        let mut first_error: Option<String> = None;
        let mut in_flight: tokio::task::JoinSet<Result<(), String>> = tokio::task::JoinSet::new();
        loop {
            while !exhausted && first_error.is_none() {
                let free = self
                    .concurrency
                    .saturating_sub(u32::try_from(in_flight.len()).unwrap_or(u32::MAX));
                let want = free.min(self.batch_size.saturating_sub(claimed_total));
                if want == 0 {
                    break;
                }
                let elapsed = match Duration::from_std(started.elapsed()) {
                    Ok(elapsed) => elapsed,
                    Err(error) => {
                        first_error = Some(format!("push outbox clock overflow: {error}"));
                        break;
                    }
                };
                let attempt_at = now + elapsed;
                // Count claim latency against the provider budget as well,
                // leaving time to persist the result before the lease can be
                // reclaimed.
                let send_deadline = tokio::time::Instant::now() + self.provider_timeout;
                // Keep due eligibility fixed for this drain. A retry recorded
                // earlier in the batch must wait for a later drain.
                let wakes = match self
                    .store
                    .claim_due_wakes(now, attempt_at + self.lease_duration, want)
                    .await
                {
                    Ok(wakes) => wakes,
                    Err(error) => {
                        first_error = Some(error);
                        break;
                    }
                };
                // A short claim means the queue is empty or contended; stop
                // refilling and let the next tick continue.
                if u32::try_from(wakes.len()).unwrap_or(u32::MAX) < want {
                    exhausted = true;
                }
                claimed_total =
                    claimed_total.saturating_add(u32::try_from(wakes.len()).unwrap_or(u32::MAX));
                for wake in wakes {
                    let job = DeliveryJob {
                        store: Arc::clone(&self.store),
                        provider: Arc::clone(&self.provider),
                        wake_signer: self.wake_signer.clone(),
                        max_attempts: self.max_attempts,
                        now,
                        started,
                        attempt_at,
                        send_deadline,
                    };
                    in_flight.spawn(job.run(wake));
                }
            }
            match in_flight.join_next().await {
                None => break,
                Some(Ok(Ok(()))) => {}
                Some(Ok(Err(error))) => {
                    first_error.get_or_insert(error);
                }
                Some(Err(join_error)) => {
                    first_error.get_or_insert(format!("push delivery task failed: {join_error}"));
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(claimed_total as usize),
        }
    }

    #[cfg(test)]
    fn classify(
        &self,
        wake: &ClaimedWake,
        result: Result<(), PushError>,
        now: DateTime<Utc>,
    ) -> WakeAttemptOutcome {
        classify_outcome(self.max_attempts, wake, result, now)
    }
}

fn classify_outcome(
    max_attempts: u32,
    wake: &ClaimedWake,
    result: Result<(), PushError>,
    now: DateTime<Utc>,
) -> WakeAttemptOutcome {
    match result {
        Ok(()) => WakeAttemptOutcome::Delivered,
        Err(PushError::InvalidToken) => WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::InvalidToken,
            error: "device token rejected by provider".into(),
        },
        Err(PushError::Retryable(error) | PushError::Delivery(error)) => {
            let completed_attempts = wake.attempts.saturating_add(1);
            if completed_attempts >= max_attempts {
                WakeAttemptOutcome::Terminal {
                    reason: PushTerminalReason::RetryLimit,
                    error,
                }
            } else {
                WakeAttemptOutcome::Retry {
                    next_attempt_at: now + retry_delay(completed_attempts),
                    error,
                }
            }
        }
        Err(PushError::Permanent(error)) => WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::PermanentFailure,
            error,
        },
        Err(PushError::Config(error)) => WakeAttemptOutcome::Terminal {
            reason: PushTerminalReason::Misconfigured,
            error,
        },
    }
}

/// Everything one spawned delivery needs (owned, so it can run on its own task).
struct DeliveryJob {
    store: Arc<dyn PushOutboxStore>,
    provider: Arc<dyn PushProvider>,
    wake_signer: Option<(String, SigningKey)>,
    max_attempts: u32,
    now: DateTime<Utc>,
    started: std::time::Instant,
    attempt_at: DateTime<Utc>,
    send_deadline: tokio::time::Instant,
}

impl DeliveryJob {
    async fn run(self, wake: ClaimedWake) -> Result<(), String> {
        let send = async {
            if let Some((key_id, key)) = &self.wake_signer {
                match crate::SignedWakeMetadata::sign(
                    &wake.tenant_id,
                    &wake.device_id,
                    &wake.command_id,
                    key_id,
                    key,
                    self.attempt_at,
                    self.attempt_at + Duration::minutes(5),
                ) {
                    Ok(metadata) => {
                        self.provider
                            .send_signed_wake(&wake.push_token, &wake.platform, &metadata)
                            .await
                    }
                    Err(error) => Err(PushError::Permanent(error.to_string())),
                }
            } else {
                self.provider
                    .send_silent_push(&wake.push_token, &wake.platform)
                    .await
            }
        };
        let provider_result = tokio::time::timeout_at(self.send_deadline, send)
            .await
            .unwrap_or_else(|_| Err(PushError::Retryable("push provider timed out".into())));
        let completed_at = self.now
            + Duration::from_std(self.started.elapsed())
                .map_err(|error| format!("push outbox clock overflow: {error}"))?;
        let outcome = classify_outcome(self.max_attempts, &wake, provider_result, completed_at);
        self.store
            .record_wake_outcome(&wake, &outcome, completed_at)
            .await
    }
}

fn retry_delay(completed_attempts: u32) -> Duration {
    let exponent = completed_attempts.saturating_sub(1).min(8);
    Duration::seconds(5_i64.saturating_mul(1_i64 << exponent))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    struct FixedProvider(Mutex<Vec<Result<(), PushError>>>);

    #[async_trait]
    impl PushProvider for FixedProvider {
        async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
            self.0.lock().unwrap().remove(0)
        }
    }

    #[derive(Default)]
    struct SignedRecordingProvider(Mutex<Vec<crate::SignedWakeMetadata>>);

    #[async_trait]
    impl PushProvider for SignedRecordingProvider {
        async fn send_silent_push(&self, _token: &str, _platform: &str) -> Result<(), PushError> {
            Err(PushError::Permanent("unsigned wake rejected".into()))
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

    #[derive(Default)]
    struct RecordingStore {
        wakes: Mutex<Vec<ClaimedWake>>,
        outcomes: Mutex<Vec<WakeAttemptOutcome>>,
    }

    #[async_trait]
    impl PushOutboxStore for RecordingStore {
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
            limit: u32,
        ) -> Result<Vec<ClaimedWake>, String> {
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

    fn wake(attempts: u32) -> ClaimedWake {
        ClaimedWake {
            id: Uuid::nil(),
            tenant_id: "tenant-a".into(),
            device_id: "device-a".into(),
            command_id: "command-a".into(),
            push_token: "token".into(),
            platform: "ios".into(),
            attempts,
            lease_until: DateTime::from_timestamp(1_800_000_030, 0).unwrap(),
        }
    }

    #[tokio::test]
    async fn persists_delivery_retry_and_invalid_token_outcomes() {
        let store = Arc::new(RecordingStore {
            wakes: Mutex::new(vec![wake(0), wake(1), wake(0)]),
            ..Default::default()
        });
        let provider = Arc::new(FixedProvider(Mutex::new(vec![
            Ok(()),
            Err(PushError::Retryable("busy".into())),
            Err(PushError::InvalidToken),
        ])));
        let worker = PushOutboxWorker::new(store.clone(), provider);
        let now = DateTime::from_timestamp(1_800_000_000, 0).unwrap();

        assert_eq!(worker.drain_once(now).await.unwrap(), 3);
        let outcomes = store.outcomes.lock().unwrap();
        assert!(matches!(outcomes[0], WakeAttemptOutcome::Delivered));
        assert!(matches!(outcomes[1], WakeAttemptOutcome::Retry { .. }));
        assert!(matches!(
            outcomes[2],
            WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::InvalidToken,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn parks_retryable_failure_at_attempt_limit() {
        let store = Arc::new(RecordingStore {
            wakes: Mutex::new(vec![wake(7)]),
            ..Default::default()
        });
        let provider = Arc::new(FixedProvider(Mutex::new(vec![Err(PushError::Retryable(
            "still busy".into(),
        ))])));
        let worker = PushOutboxWorker::new(store.clone(), provider);

        worker.drain_once(Utc::now()).await.unwrap();
        assert!(matches!(
            store.outcomes.lock().unwrap()[0],
            WakeAttemptOutcome::Terminal {
                reason: PushTerminalReason::RetryLimit,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn unconfigured_provider_leaves_outbox_unclaimed() {
        let store = Arc::new(RecordingStore {
            wakes: Mutex::new(vec![wake(0)]),
            ..Default::default()
        });
        let worker = PushOutboxWorker::new(store.clone(), Arc::new(crate::NoopPushProvider));

        assert_eq!(worker.drain_once(Utc::now()).await.unwrap(), 0);
        assert_eq!(store.wakes.lock().unwrap().len(), 1);
        assert!(store.outcomes.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn configured_signer_sends_bound_metadata_instead_of_unsigned_wake() {
        let store = Arc::new(RecordingStore {
            wakes: Mutex::new(vec![wake(0)]),
            ..Default::default()
        });
        let provider = Arc::new(SignedRecordingProvider::default());
        let worker = PushOutboxWorker::new(store, provider.clone())
            .with_wake_signer("push-v1", SigningKey::from_bytes(&[8; 32]));
        worker.drain_once(Utc::now()).await.unwrap();
        let sent = provider.0.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].tenant_id, "tenant-a");
        assert_eq!(sent[0].device_id, "device-a");
        assert_eq!(sent[0].command_id, "command-a");
    }
}

#[cfg(test)]
#[path = "outbox_drain_tests.rs"]
mod outbox_drain_tests;
