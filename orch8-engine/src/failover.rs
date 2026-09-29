//! Active-passive multi-region failover fence.
//!
//! A deployment opts in by giving every engine node a region
//! (`ORCH8_FAILOVER_REGION`). The database then holds a singleton
//! [`RegionFence`] naming the one active region and a monotonically
//! increasing epoch. Engine nodes:
//!
//! - **start only when their region is active** ([`wait_until_active`]); a
//!   standby node keeps serving health checks but never runs the scheduler;
//! - **fence themselves** ([`watch`]) as soon as the fence names another
//!   region or a newer epoch, and also when the fence cannot be read for
//!   longer than the blind window (fail closed: a node that cannot prove it
//!   is still active stops claiming work).
//!
//! Promotion ([`promote`]) is a compare-and-swap on the epoch, so two
//! operators (or a script retry) cannot both "win". Replicating the database
//! itself is the operator's `PostgreSQL` responsibility; see `docs/FAILOVER.md`
//! for the procedure and the RPO/RTO this mechanism can and cannot give.

use std::time::Duration;

use chrono::Utc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;
use orch8_types::federation::{RegionFence, is_valid_region};

/// Observed fence position for one region.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceStatus {
    /// This region holds the fence at `epoch`.
    Active { epoch: u64 },
    /// Another region holds it.
    Standby { active_region: String, epoch: u64 },
    /// No fence has been installed yet (`orch8 failover promote` first).
    Uninitialized,
}

/// Why [`watch`] stopped the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FenceLoss {
    /// Another region (or a newer epoch of this region) was promoted.
    Superseded { active_region: String, epoch: u64 },
    /// The fence could not be read for longer than the blind window.
    Unreachable,
    /// Graceful shutdown, not a fence event.
    Stopped,
}

/// Errors from [`promote`].
#[derive(Debug, thiserror::Error)]
pub enum PromoteError {
    #[error("invalid region name (use 1-64 of [a-z0-9-])")]
    InvalidRegion,
    #[error("fence epoch is {actual}, expected {expected}; re-read the fence and retry")]
    EpochMismatch { expected: u64, actual: u64 },
    #[error("fence changed concurrently; re-read the fence and retry")]
    Concurrent,
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Read the fence and classify it for `region`.
///
/// # Errors
/// Storage read failures.
pub async fn check(
    storage: &dyn StorageBackend,
    region: &str,
) -> Result<FenceStatus, StorageError> {
    Ok(match storage.get_region_fence().await? {
        None => FenceStatus::Uninitialized,
        Some(fence) if fence.active_region == region => FenceStatus::Active { epoch: fence.epoch },
        Some(fence) => FenceStatus::Standby {
            active_region: fence.active_region,
            epoch: fence.epoch,
        },
    })
}

/// Block until `region` is the active region. Returns the epoch the node
/// became active under, or `None` if `cancel` fired first.
pub async fn wait_until_active(
    storage: &dyn StorageBackend,
    region: &str,
    poll: Duration,
    cancel: &CancellationToken,
) -> Option<u64> {
    let mut logged = None;
    loop {
        match check(storage, region).await {
            Ok(FenceStatus::Active { epoch }) => {
                info!(
                    region,
                    epoch, "region fence: this region is active; starting engine"
                );
                return Some(epoch);
            }
            Ok(status) => {
                if logged.as_ref() != Some(&status) {
                    info!(
                        region,
                        ?status,
                        "region fence: standby — engine not started"
                    );
                    logged = Some(status);
                }
            }
            Err(error) => warn!(region, %error, "region fence: read failed while in standby"),
        }
        tokio::select! {
            () = cancel.cancelled() => return None,
            () = tokio::time::sleep(poll) => {}
        }
    }
}

/// Watch the fence while active. Cancels `engine` (the scheduler's
/// shutdown token) the moment this node is no longer provably active.
pub async fn watch(
    storage: &dyn StorageBackend,
    region: &str,
    acquired_epoch: u64,
    poll: Duration,
    blind_window: Duration,
    engine: &CancellationToken,
) -> FenceLoss {
    let mut last_confirmed = tokio::time::Instant::now();
    loop {
        tokio::select! {
            () = engine.cancelled() => return FenceLoss::Stopped,
            () = tokio::time::sleep(poll) => {}
        }
        match storage.get_region_fence().await {
            Ok(Some(fence)) if fence.active_region == region && fence.epoch == acquired_epoch => {
                last_confirmed = tokio::time::Instant::now();
            }
            Ok(Some(fence)) => {
                error!(
                    region,
                    acquired_epoch,
                    active_region = %fence.active_region,
                    epoch = fence.epoch,
                    "region fence: superseded — stopping the scheduler"
                );
                engine.cancel();
                return FenceLoss::Superseded {
                    active_region: fence.active_region,
                    epoch: fence.epoch,
                };
            }
            Ok(None) => {
                error!(
                    region,
                    "region fence: fence row disappeared — stopping the scheduler"
                );
                engine.cancel();
                return FenceLoss::Unreachable;
            }
            Err(read_error) => {
                if last_confirmed.elapsed() >= blind_window {
                    error!(region, %read_error, "region fence: unreadable beyond the blind window — stopping the scheduler");
                    engine.cancel();
                    return FenceLoss::Unreachable;
                }
                warn!(region, %read_error, "region fence: read failed; still inside the blind window");
            }
        }
    }
}

/// Make `region` the active region. `expected_epoch` pins the promotion to
/// the fence the operator inspected (recommended); `None` accepts whatever
/// epoch is current. Returns the installed fence.
///
/// # Errors
/// See [`PromoteError`].
pub async fn promote(
    storage: &dyn StorageBackend,
    region: &str,
    expected_epoch: Option<u64>,
    updated_by: &str,
    reason: &str,
) -> Result<RegionFence, PromoteError> {
    if !is_valid_region(region) {
        return Err(PromoteError::InvalidRegion);
    }
    let current = storage.get_region_fence().await?;
    if let (Some(expected), Some(fence)) = (expected_epoch, current.as_ref())
        && fence.epoch != expected
    {
        return Err(PromoteError::EpochMismatch {
            expected,
            actual: fence.epoch,
        });
    }
    let next = RegionFence {
        active_region: region.to_owned(),
        epoch: current.as_ref().map_or(1, |fence| fence.epoch + 1),
        updated_at: Utc::now(),
        updated_by: updated_by.chars().take(256).collect(),
        reason: reason.chars().take(1024).collect(),
    };
    if storage
        .advance_region_fence(current.map(|fence| fence.epoch), &next)
        .await?
    {
        Ok(next)
    } else {
        Err(PromoteError::Concurrent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use orch8_storage::sqlite::SqliteStorage;

    async fn store() -> Arc<dyn StorageBackend> {
        Arc::new(SqliteStorage::in_memory().await.unwrap())
    }

    #[tokio::test]
    async fn promotion_is_a_strict_epoch_cas() {
        let storage = store().await;
        assert_eq!(
            check(storage.as_ref(), "eu").await.unwrap(),
            FenceStatus::Uninitialized
        );
        let first = promote(storage.as_ref(), "eu", None, "ops", "initial")
            .await
            .unwrap();
        assert_eq!(first.epoch, 1);
        assert!(matches!(
            promote(storage.as_ref(), "us", Some(7), "ops", "stale view").await,
            Err(PromoteError::EpochMismatch {
                expected: 7,
                actual: 1
            })
        ));
        let second = promote(storage.as_ref(), "us", Some(1), "ops", "failover")
            .await
            .unwrap();
        assert_eq!(second.epoch, 2);
        assert_eq!(
            check(storage.as_ref(), "eu").await.unwrap(),
            FenceStatus::Standby {
                active_region: "us".into(),
                epoch: 2
            }
        );
        assert!(matches!(
            promote(storage.as_ref(), "EU", None, "ops", "").await,
            Err(PromoteError::InvalidRegion)
        ));
    }

    #[tokio::test]
    async fn stale_cas_cannot_overwrite_a_newer_fence() {
        let storage = store().await;
        promote(storage.as_ref(), "eu", None, "ops", "")
            .await
            .unwrap();
        let racing = RegionFence {
            active_region: "ap".into(),
            epoch: 2,
            updated_at: Utc::now(),
            updated_by: String::new(),
            reason: String::new(),
        };
        assert!(
            storage
                .advance_region_fence(Some(1), &racing)
                .await
                .unwrap()
        );
        // A second promoter that also read epoch 1 must lose.
        let loser = RegionFence {
            active_region: "us".into(),
            ..racing
        };
        assert!(!storage.advance_region_fence(Some(1), &loser).await.unwrap());
        // And a non-consecutive epoch is refused outright.
        let skip = RegionFence { epoch: 9, ..loser };
        assert!(!storage.advance_region_fence(Some(2), &skip).await.unwrap());
        assert_eq!(
            storage
                .get_region_fence()
                .await
                .unwrap()
                .unwrap()
                .active_region,
            "ap"
        );
    }

    #[tokio::test]
    async fn active_node_fences_itself_and_standby_takes_over() {
        let storage = store().await;
        promote(storage.as_ref(), "eu", None, "ops", "")
            .await
            .unwrap();

        let eu_engine = CancellationToken::new();
        let eu_epoch = wait_until_active(
            storage.as_ref(),
            "eu",
            Duration::from_millis(10),
            &eu_engine,
        )
        .await
        .unwrap();
        let watcher = {
            let storage = Arc::clone(&storage);
            let eu_engine = eu_engine.clone();
            tokio::spawn(async move {
                watch(
                    storage.as_ref(),
                    "eu",
                    eu_epoch,
                    Duration::from_millis(10),
                    Duration::from_secs(5),
                    &eu_engine,
                )
                .await
            })
        };
        let us_cancel = CancellationToken::new();
        let standby = {
            let storage = Arc::clone(&storage);
            let us_cancel = us_cancel.clone();
            tokio::spawn(async move {
                wait_until_active(
                    storage.as_ref(),
                    "us",
                    Duration::from_millis(10),
                    &us_cancel,
                )
                .await
            })
        };
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !standby.is_finished(),
            "standby must not start while eu is active"
        );
        assert!(!eu_engine.is_cancelled());

        promote(storage.as_ref(), "us", Some(eu_epoch), "ops", "eu outage")
            .await
            .unwrap();

        let loss = tokio::time::timeout(Duration::from_secs(2), watcher)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            loss,
            FenceLoss::Superseded {
                active_region: "us".into(),
                epoch: 2
            }
        );
        assert!(eu_engine.is_cancelled(), "old region scheduler is stopped");
        let us_epoch = tokio::time::timeout(Duration::from_secs(2), standby)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(us_epoch, Some(2));
    }
}
