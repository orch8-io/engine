//! Handler invocations that outlived their device-side timeout.
//!
//! A timeout on the device cannot stop an app-native handler: the foreign
//! `StepHandler::execute` call runs on a blocking thread and keeps going
//! (possibly performing its side effect) after the engine stopped waiting.
//! Such a *straggler* is tracked here until it actually returns, and the
//! remote worker does not claim new work for that handler meanwhile — a retry
//! of the same step must never run on this device concurrently with the
//! attempt it replaces. A handler that never returns is quarantined for at
//! most [`QUARANTINE_MAX`]; after that the worker claims for it again (and
//! logs that the earlier invocation is still outstanding).

use std::collections::HashMap;
use std::sync::Mutex as StdMutex;
use std::time::{Duration, Instant};

use tracing::warn;

/// Longest a timed-out invocation blocks new claims for its handler.
pub(crate) const QUARANTINE_MAX: Duration = Duration::from_secs(15 * 60);

/// `details` marker on the `StepError` produced by a device-side timeout.
pub(crate) const DEVICE_TIMEOUT_DETAIL: &str = "device_timeout";

#[derive(Default)]
pub(crate) struct Stragglers {
    running: StdMutex<HashMap<String, Straggling>>,
}

struct Straggling {
    count: u32,
    since: Instant,
}

impl Stragglers {
    /// An invocation of `handler` timed out but is still running.
    pub(crate) fn begin(&self, handler: &str) {
        let mut running = self.lock();
        let entry = running.entry(handler.to_string()).or_insert(Straggling {
            count: 0,
            since: Instant::now(),
        });
        entry.count = entry.count.saturating_add(1);
    }

    /// A previously timed-out invocation of `handler` finally returned.
    pub(crate) fn end(&self, handler: &str) {
        let mut running = self.lock();
        if let Some(entry) = running.get_mut(handler) {
            entry.count = entry.count.saturating_sub(1);
            if entry.count == 0 {
                running.remove(handler);
            }
        }
    }

    /// Whether new work for `handler` must wait for a straggler.
    pub(crate) fn blocks(&self, handler: &str) -> bool {
        let running = self.lock();
        let Some(entry) = running.get(handler) else {
            return false;
        };
        if entry.since.elapsed() < QUARANTINE_MAX {
            return true;
        }
        warn!(
            handler,
            outstanding = entry.count,
            "timed-out handler invocation still running after the quarantine; claiming again"
        );
        false
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Straggling>> {
        self.running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Whether a step error came from a device-side timeout.
pub(crate) fn is_device_timeout(error: &orch8_types::error::StepError) -> bool {
    let details = match error {
        orch8_types::error::StepError::Retryable { details, .. }
        | orch8_types::error::StepError::Permanent { details, .. } => details,
    };
    details
        .as_ref()
        .and_then(|details| details.get(DEVICE_TIMEOUT_DETAIL))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}

/// The retryable error returned when the device stops waiting for a handler.
pub(crate) fn device_timeout_error(message: String) -> orch8_types::error::StepError {
    orch8_types::error::StepError::Retryable {
        message,
        details: Some(serde_json::json!({ DEVICE_TIMEOUT_DETAIL: true })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn straggler_blocks_until_every_invocation_returns() {
        let stragglers = Stragglers::default();
        assert!(!stragglers.blocks("scan"));
        stragglers.begin("scan");
        stragglers.begin("scan");
        assert!(stragglers.blocks("scan"));
        assert!(!stragglers.blocks("other"));
        stragglers.end("scan");
        assert!(stragglers.blocks("scan"));
        stragglers.end("scan");
        assert!(!stragglers.blocks("scan"));
        stragglers.end("scan");
        assert!(!stragglers.blocks("scan"));
    }

    #[test]
    fn device_timeout_marker_round_trips() {
        assert!(is_device_timeout(&device_timeout_error("t".into())));
        assert!(!is_device_timeout(
            &orch8_types::error::StepError::Retryable {
                message: "x".into(),
                details: None,
            }
        ));
    }
}
