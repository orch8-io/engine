use std::time::Duration;

use tracing::{info, warn};

use orch8_storage::StorageBackend;
use orch8_types::error::StorageError;

use crate::error::EngineError;

/// Recover instances that were Running when the engine crashed.
/// Resets them to Scheduled so they will be re-claimed on the next tick.
pub async fn recover_stale_instances(
    storage: &dyn StorageBackend,
    stale_threshold_secs: u64,
) -> Result<u64, EngineError> {
    let threshold = Duration::from_secs(stale_threshold_secs);

    match storage.recover_stale_instances(threshold).await {
        Ok(count) => {
            if count > 0 {
                warn!(
                    count = count,
                    threshold_secs = stale_threshold_secs,
                    "recovered stale instances after crash"
                );
            } else {
                info!("no stale instances found during recovery check");
            }
            Ok(count)
        }
        Err(StorageError::Connection(msg)) => {
            warn!(error = %msg, "storage unavailable during recovery, will retry on next tick");
            Ok(0)
        }
        Err(e) => Err(EngineError::Storage(e)),
    }
}
