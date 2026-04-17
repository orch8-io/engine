use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use chrono::Utc;
use tracing::warn;

use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};

/// In-memory circuit breaker registry. Each handler gets its own breaker.
pub struct CircuitBreakerRegistry {
    breakers: Mutex<HashMap<String, CircuitBreakerState>>,
    default_threshold: u32,
    default_cooldown_secs: u64,
}

impl CircuitBreakerRegistry {
    pub fn new(default_threshold: u32, default_cooldown_secs: u64) -> Self {
        Self {
            breakers: Mutex::new(HashMap::new()),
            default_threshold,
            default_cooldown_secs,
        }
    }

    /// Check if a handler is allowed to execute. Returns `Ok(())` if allowed,
    /// or `Err(remaining_cooldown_secs)` if the circuit is open.
    pub fn check(&self, handler: &str) -> Result<(), u64> {
        let mut map = self.lock_breakers();
        let breaker = map
            .entry(handler.to_string())
            .or_insert_with(|| self.default_breaker(handler));

        match breaker.state {
            BreakerState::Closed | BreakerState::HalfOpen => Ok(()),
            BreakerState::Open => {
                // Check if cooldown has elapsed
                if let Some(opened_at) = breaker.opened_at {
                    #[allow(clippy::cast_sign_loss)]
                    let elapsed = (Utc::now() - opened_at).num_seconds().max(0) as u64;
                    if elapsed >= breaker.cooldown_secs {
                        breaker.state = BreakerState::HalfOpen;
                        Ok(())
                    } else {
                        Err(breaker.cooldown_secs - elapsed)
                    }
                } else {
                    // No opened_at means it was just set — cooldown starts now
                    breaker.opened_at = Some(Utc::now());
                    Err(breaker.cooldown_secs)
                }
            }
        }
    }

    /// Record a successful execution for a handler.
    pub fn record_success(&self, handler: &str) {
        let mut map = self.lock_breakers();
        if let Some(breaker) = map.get_mut(handler) {
            breaker.failure_count = 0;
            breaker.state = BreakerState::Closed;
            breaker.opened_at = None;
        }
    }

    /// Record a failure for a handler. May trip the circuit to Open.
    pub fn record_failure(&self, handler: &str) {
        let mut map = self.lock_breakers();
        let breaker = map
            .entry(handler.to_string())
            .or_insert_with(|| self.default_breaker(handler));

        breaker.failure_count += 1;

        if breaker.failure_count >= breaker.failure_threshold {
            breaker.state = BreakerState::Open;
            breaker.opened_at = Some(Utc::now());
        }
    }

    /// Get the current state of all breakers.
    pub fn list_all(&self) -> Vec<CircuitBreakerState> {
        let map = self.lock_breakers();
        map.values().cloned().collect()
    }

    /// Get the current state of a specific handler's breaker.
    pub fn get(&self, handler: &str) -> Option<CircuitBreakerState> {
        let map = self.lock_breakers();
        map.get(handler).cloned()
    }

    /// Reset a handler's circuit breaker to Closed.
    pub fn reset(&self, handler: &str) {
        let mut map = self.lock_breakers();
        if let Some(breaker) = map.get_mut(handler) {
            breaker.state = BreakerState::Closed;
            breaker.failure_count = 0;
            breaker.opened_at = None;
        }
    }

    fn lock_breakers(&self) -> MutexGuard<'_, HashMap<String, CircuitBreakerState>> {
        self.breakers.lock().unwrap_or_else(|poisoned| {
            warn!("circuit breaker mutex was poisoned, recovering");
            poisoned.into_inner()
        })
    }

    fn default_breaker(&self, handler: &str) -> CircuitBreakerState {
        CircuitBreakerState {
            handler: handler.to_string(),
            state: BreakerState::Closed,
            failure_count: 0,
            failure_threshold: self.default_threshold,
            cooldown_secs: self.default_cooldown_secs,
            opened_at: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_allows_requests() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        assert!(cb.check("test_handler").is_ok());
    }

    #[test]
    fn opens_after_threshold() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        cb.record_failure("test_handler");
        cb.record_failure("test_handler");
        assert!(cb.check("test_handler").is_ok());
        cb.record_failure("test_handler");
        assert!(cb.check("test_handler").is_err());
    }

    #[test]
    fn success_resets_failures() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        cb.record_failure("test_handler");
        cb.record_failure("test_handler");
        cb.record_success("test_handler");
        cb.record_failure("test_handler");
        cb.record_failure("test_handler");
        assert!(cb.check("test_handler").is_ok()); // Still under threshold
    }

    #[test]
    fn reset_closes_circuit() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        for _ in 0..3 {
            cb.record_failure("test_handler");
        }
        assert!(cb.check("test_handler").is_err());
        cb.reset("test_handler");
        assert!(cb.check("test_handler").is_ok());
    }

    #[test]
    fn half_open_after_cooldown() {
        let cb = CircuitBreakerRegistry::new(3, 0); // 0s cooldown for test
        for _ in 0..3 {
            cb.record_failure("test_handler");
        }
        // Cooldown is 0s, so it should transition to HalfOpen immediately
        assert!(cb.check("test_handler").is_ok());
        let state = cb.get("test_handler").unwrap();
        assert_eq!(state.state, BreakerState::HalfOpen);
    }

    #[test]
    fn check_is_noop_on_first_call_per_handler() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        // Two different handlers — each gets its own breaker, neither trips.
        assert!(cb.check("h1").is_ok());
        assert!(cb.check("h2").is_ok());
        assert_eq!(cb.list_all().len(), 2);
    }

    #[test]
    fn get_returns_none_for_unknown_handler() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        assert!(cb.get("never_checked").is_none());
    }

    #[test]
    fn breakers_are_isolated_per_handler() {
        let cb = CircuitBreakerRegistry::new(2, 30);
        cb.record_failure("h1");
        cb.record_failure("h1");
        assert!(cb.check("h1").is_err(), "h1 should be open");
        assert!(cb.check("h2").is_ok(), "h2 must remain closed");
    }

    #[test]
    fn record_success_on_untracked_handler_is_noop() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        // No breaker exists yet — success must not panic or auto-create.
        cb.record_success("ghost_handler");
        assert!(cb.get("ghost_handler").is_none());
    }

    #[test]
    fn reset_on_untracked_handler_is_noop() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        cb.reset("ghost");
        assert!(cb.get("ghost").is_none());
    }

    #[test]
    fn half_open_success_fully_closes() {
        let cb = CircuitBreakerRegistry::new(2, 0);
        cb.record_failure("h");
        cb.record_failure("h");
        // Transition to half-open.
        cb.check("h").unwrap();
        assert_eq!(cb.get("h").unwrap().state, BreakerState::HalfOpen);
        // Probe succeeds — breaker should fully close.
        cb.record_success("h");
        let s = cb.get("h").unwrap();
        assert_eq!(s.state, BreakerState::Closed);
        assert_eq!(s.failure_count, 0);
        assert!(s.opened_at.is_none());
    }

    #[test]
    fn failure_threshold_stored_from_defaults() {
        let cb = CircuitBreakerRegistry::new(7, 120);
        cb.record_failure("x");
        let s = cb.get("x").unwrap();
        assert_eq!(s.failure_threshold, 7);
        assert_eq!(s.cooldown_secs, 120);
        assert_eq!(s.failure_count, 1);
        assert_eq!(s.state, BreakerState::Closed);
    }

    #[test]
    fn list_all_reflects_cleared_state_after_reset() {
        let cb = CircuitBreakerRegistry::new(2, 30);
        cb.record_failure("a");
        cb.record_failure("a");
        assert!(cb.check("a").is_err());
        cb.reset("a");
        let all = cb.list_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].state, BreakerState::Closed);
        assert_eq!(all[0].failure_count, 0);
    }
}
