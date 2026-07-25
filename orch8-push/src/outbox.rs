//! Durable push wake delivery state machine.
//!
//! Storage owns claiming and outcome persistence; this crate owns provider
//! dispatch, retry classification, bounded backoff, and terminal semantics.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use uuid::Uuid;

use crate::{PushError, PushProvider};

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
}

/// Bounded outbox worker. Safe to run on every server node when the store's
/// claim operation provides leases/`SKIP LOCKED` semantics.
pub struct PushOutboxWorker {
    store: Arc<dyn PushOutboxStore>,
    provider: Arc<dyn PushProvider>,
    batch_size: u32,
    lease_duration: Duration,
    max_attempts: u32,
}

impl PushOutboxWorker {
    #[must_use]
    pub fn new(store: Arc<dyn PushOutboxStore>, provider: Arc<dyn PushProvider>) -> Self {
        Self {
            store,
            provider,
            batch_size: 100,
            lease_duration: Duration::seconds(30),
            max_attempts: 8,
        }
    }

    #[must_use]
    pub fn with_limits(mut self, batch_size: u32, max_attempts: u32) -> Self {
        self.batch_size = batch_size.clamp(1, 1_000);
        self.max_attempts = max_attempts.clamp(1, 32);
        self
    }

    /// Claim and deliver one bounded batch, returning the number attempted.
    pub async fn drain_once(&self, now: DateTime<Utc>) -> Result<usize, String> {
        let wakes = self
            .store
            .claim_due_wakes(now, now + self.lease_duration, self.batch_size)
            .await?;
        for wake in &wakes {
            let provider_result = self
                .provider
                .send_silent_push(&wake.push_token, &wake.platform)
                .await;
            let outcome = self.classify(wake, provider_result, now);
            self.store.record_wake_outcome(wake, &outcome, now).await?;
        }
        Ok(wakes.len())
    }

    fn classify(
        &self,
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
                if completed_attempts >= self.max_attempts {
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
    struct RecordingStore {
        wakes: Mutex<Vec<ClaimedWake>>,
        outcomes: Mutex<Vec<WakeAttemptOutcome>>,
    }

    #[async_trait]
    impl PushOutboxStore for RecordingStore {
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
}
