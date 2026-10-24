//! Monotonic lease supervision shared by remote runtimes.
//!
//! Only an accepted heartbeat authorizes execution. Each accepted renewal is
//! measured from before its request, so network latency never extends local
//! authority. The watcher is polled alongside the owned operation, including
//! preparation and outcome delivery; a slow RPC cannot suspend that operation.

use std::future::Future;
use std::time::Duration;

use tokio::time::{Instant, sleep_until, timeout_at};

use super::{DEFAULT_LEASE_SECS, LeaseResponse};

/// Why local execution no longer has confirmed server authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseEnd {
    /// The server explicitly rejected this claim generation.
    LostOwnership,
    /// No successful renewal arrived before the confirmed deadline.
    Expired,
    /// The server rejected or does not support the ownership check.
    Unconfirmed,
}

/// Server-confirmed authority, bounded by a conservative monotonic deadline.
pub struct ConfirmedLease {
    duration: Duration,
    until: Instant,
}

impl ConfirmedLease {
    /// Check ownership before invoking a callback. Retry transient failures for
    /// at most one lease duration without starting any task work.
    ///
    /// # Errors
    /// A rejected, fenced, or unconfirmed claim must not start a callback.
    pub async fn acquire<F, R>(
        lease_secs: Option<u32>,
        cadence: Duration,
        mut renew: R,
    ) -> Result<Self, LeaseEnd>
    where
        F: Future<Output = LeaseResponse>,
        R: FnMut() -> F,
    {
        let duration = Duration::from_secs(
            lease_secs
                .filter(|seconds| *seconds > 0)
                .map_or(DEFAULT_LEASE_SECS, u64::from),
        );
        let limit = Instant::now() + duration;
        loop {
            let started = Instant::now();
            if started >= limit {
                return Err(LeaseEnd::Expired);
            }
            let response = timeout_at(limit, renew())
                .await
                .map_err(|_| LeaseEnd::Expired)?;
            if Instant::now() >= limit {
                return Err(LeaseEnd::Expired);
            }
            match response {
                LeaseResponse::Accepted => {
                    let until = started + duration;
                    if Instant::now() >= until {
                        return Err(LeaseEnd::Expired);
                    }
                    return Ok(Self { duration, until });
                }
                LeaseResponse::LostOwnership => return Err(LeaseEnd::LostOwnership),
                LeaseResponse::Rejected | LeaseResponse::Unsupported => {
                    return Err(LeaseEnd::Unconfirmed);
                }
                LeaseResponse::Retry => {
                    sleep_until((Instant::now() + bounded_cadence(cadence, duration)).min(limit))
                        .await;
                }
            }
        }
    }

    /// Run owned work while renewing its lease independently of its progress.
    /// On failure the operation is dropped before this method returns.
    ///
    /// # Errors
    /// Returns the reason execution lost authority. Callers must retain any
    /// durable outcome or started/unknown evidence needed for recovery.
    pub async fn run<T, O, F, R>(
        self,
        cadence: Duration,
        operation: O,
        renew: R,
    ) -> Result<T, LeaseEnd>
    where
        O: Future<Output = T>,
        F: Future<Output = LeaseResponse>,
        R: FnMut() -> F,
    {
        tokio::select! {
            biased;
            end = self.watch(cadence, renew) => Err(end),
            result = operation => Ok(result),
        }
    }

    async fn watch<F, R>(mut self, cadence: Duration, mut renew: R) -> LeaseEnd
    where
        F: Future<Output = LeaseResponse>,
        R: FnMut() -> F,
    {
        let cadence = bounded_cadence(cadence, self.duration);
        loop {
            sleep_until((Instant::now() + cadence).min(self.until)).await;
            let started = Instant::now();
            if started >= self.until {
                return LeaseEnd::Expired;
            }
            let Ok(response) = timeout_at(self.until, renew()).await else {
                return LeaseEnd::Expired;
            };
            // Tokio timeouts may poll a ready inner future before checking
            // their deadline. A late acknowledgement cannot revive authority.
            if Instant::now() >= self.until {
                return LeaseEnd::Expired;
            }
            match response {
                LeaseResponse::Accepted => self.until = started + self.duration,
                LeaseResponse::LostOwnership => return LeaseEnd::LostOwnership,
                LeaseResponse::Rejected | LeaseResponse::Unsupported => {
                    return LeaseEnd::Unconfirmed;
                }
                LeaseResponse::Retry => {}
            }
            if Instant::now() >= self.until {
                return LeaseEnd::Expired;
            }
        }
    }
}

fn bounded_cadence(cadence: Duration, duration: Duration) -> Duration {
    cadence.min(duration / 3).max(Duration::from_millis(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::pending;

    #[tokio::test(start_paused = true)]
    async fn slow_confirmation_does_not_move_the_deadline_to_response_time() {
        let start = Instant::now();
        let lease = ConfirmedLease::acquire(Some(1), Duration::from_millis(100), || async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            LeaseResponse::Accepted
        })
        .await
        .unwrap();
        let result = lease
            .run(Duration::from_millis(100), pending::<()>(), || {
                pending::<LeaseResponse>()
            })
            .await;
        assert_eq!(result, Err(LeaseEnd::Expired));
        assert!(
            start.elapsed() <= Duration::from_millis(1010),
            "elapsed: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_successful_renewal_recovers_after_a_transient_failure() {
        let start = Instant::now();
        let lease = ConfirmedLease::acquire(Some(1), Duration::from_millis(100), || async {
            LeaseResponse::Accepted
        })
        .await
        .unwrap();
        let mut attempts = 0;
        let result = lease
            .run(Duration::from_millis(100), pending::<()>(), || {
                attempts += 1;
                let response = if attempts == 2 {
                    LeaseResponse::Accepted
                } else {
                    LeaseResponse::Retry
                };
                async move { response }
            })
            .await;
        assert_eq!(result, Err(LeaseEnd::Expired));
        assert!(start.elapsed() >= Duration::from_millis(1200));
        assert!(
            start.elapsed() <= Duration::from_millis(1220),
            "elapsed: {:?}",
            start.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_second_grants_renew_before_expiry_even_with_a_long_server_cadence() {
        let lease = ConfirmedLease::acquire(Some(1), Duration::from_secs(1), || async {
            LeaseResponse::Accepted
        })
        .await
        .unwrap();
        let mut renewals = 0;
        let result = lease
            .run(
                Duration::from_secs(1),
                tokio::time::sleep(Duration::from_millis(2500)),
                || {
                    renewals += 1;
                    async { LeaseResponse::Accepted }
                },
            )
            .await;
        assert!(result.is_ok());
        assert!(renewals >= 7);
    }

    #[tokio::test]
    async fn late_ready_renewal_cannot_revive_an_expired_local_lease() {
        let lease = ConfirmedLease {
            duration: Duration::from_secs(1),
            until: Instant::now() + Duration::from_millis(100),
        };
        let result = lease
            .run(
                Duration::from_millis(10),
                async {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    true
                },
                || async {
                    // Deliberately block this test's single-thread runtime so the
                    // renewal becomes ready after the old deadline, before the timer
                    // can run. The operation must not resume on that late response.
                    std::thread::sleep(Duration::from_millis(110));
                    LeaseResponse::Accepted
                },
            )
            .await;
        assert_eq!(result, Err(LeaseEnd::Expired));
    }
}
