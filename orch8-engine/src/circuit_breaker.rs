//! In-memory circuit breaker registry, scoped per (tenant, handler).
//!
//! **Per-tenant isolation.** The registry is keyed by `(TenantId, String)` so a
//! failing handler for one tenant cannot trip the breaker for any other tenant
//! that shares the same handler name — a regression that would otherwise let
//! one noisy customer degrade service for everyone else calling the same code.
//!
//! **Persisted Open state.** The in-memory state is authoritative during a
//! process lifetime, but `Open` transitions are additionally mirrored to
//! storage (when a backend is injected via [`CircuitBreakerRegistry::with_storage`])
//! so that a crash mid-cooldown does not reset every tripped breaker to
//! `Closed`. On boot the server calls [`CircuitBreakerRegistry::load_from_storage`]
//! to rehydrate `Open` rows and preserve cooldown clocks.
//!
//! Only `Open` rows are persisted. `Closed` is the default state — no reason
//! to keep a row for every untripped breaker. `HalfOpen` is a transient probe
//! state owned by the live process; persisting it would be meaningless across
//! restarts.
//!
//! Persistence runs off the hot path: state transitions call
//! [`tokio::spawn`] to fire-and-forget the storage write so `check`,
//! `record_failure`, `record_success`, and `reset` remain synchronous. This
//! keeps the in-memory registry's semantics unchanged for callers on the
//! request path while still giving us a durable backstop.

use std::borrow::Borrow;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use dashmap::DashMap;
use tokio_util::task::TaskTracker;

use chrono::Utc;

use orch8_storage::StorageBackend;
use orch8_types::circuit_breaker::{BreakerState, CircuitBreakerState};
use orch8_types::ids::TenantId;

#[derive(Clone, Debug)]
struct Key(TenantId, String);

trait CircuitKey {
    fn tenant_id(&self) -> &str;
    fn handler(&self) -> &str;
}

impl CircuitKey for Key {
    fn tenant_id(&self) -> &str {
        &self.0.0
    }
    fn handler(&self) -> &str {
        &self.1
    }
}

impl<'a> Borrow<dyn CircuitKey + 'a> for Key {
    fn borrow(&self) -> &(dyn CircuitKey + 'a) {
        self
    }
}

impl Hash for dyn CircuitKey + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tenant_id().hash(state);
        self.handler().hash(state);
    }
}

impl PartialEq for dyn CircuitKey + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id() == other.tenant_id() && self.handler() == other.handler()
    }
}

impl Eq for dyn CircuitKey + '_ {}

impl Hash for Key {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.tenant_id().hash(state);
        self.handler().hash(state);
    }
}

impl PartialEq for Key {
    fn eq(&self, other: &Self) -> bool {
        self.tenant_id() == other.tenant_id() && self.handler() == other.handler()
    }
}

impl Eq for Key {}

struct KeyRef<'a>(&'a TenantId, &'a str);

impl CircuitKey for KeyRef<'_> {
    fn tenant_id(&self) -> &str {
        &self.0.0
    }
    fn handler(&self) -> &str {
        self.1
    }
}

/// Handlers that the circuit breaker should NOT track.
///
/// The breaker exists to protect against cascading failures in external
/// dependencies (HTTP APIs, LLM providers, plugin sidecars, gRPC services,
/// external worker queues). Pure control-flow / internal built-ins have no
/// external dependency and would only cause false positives — a `fail` step
/// in a test or a `send_signal` that loses a race shouldn't take down every
/// instance using that handler for a minute.
///
/// Policy lives alongside the breaker (and not e.g. on `StepHandler`) so the
/// set is a single visible list rather than scattered opt-outs. External
/// (unknown) handlers route through the worker queue and aren't dispatched
/// here either; they're tracked separately if/when the worker path is wired.
pub fn is_breaker_tracked(handler: &str) -> bool {
    !matches!(
        handler,
        "noop"
            | "log"
            | "sleep"
            | "fail"
            | "self_modify"
            | "emit_event"
            | "send_signal"
            | "query_instance"
            | "human_review"
    )
}

/// In-memory circuit breaker registry. Each `(tenant, handler)` pair gets its
/// own breaker; tenants are isolated.
pub struct CircuitBreakerRegistry {
    breakers: DashMap<Key, CircuitBreakerState>,
    default_threshold: u32,
    default_cooldown_secs: u64,
    /// Optional storage backend for persisting `Open` transitions. When `None`
    /// the registry behaves as pure in-memory (used by unit tests and in
    /// configurations where durability isn't required).
    storage: Option<Arc<dyn StorageBackend>>,
    /// Tracks outstanding fire-and-forget persistence tasks so the server
    /// can wait for them at shutdown via [`Self::flush`]. Without this the
    /// bare `tokio::spawn` used previously could be aborted mid-write when
    /// the Tokio runtime started shutting down — leaving persisted state
    /// inconsistent with the in-memory registry the next process saw.
    tracker: TaskTracker,
}

impl CircuitBreakerRegistry {
    #[must_use]
    pub fn new(default_threshold: u32, default_cooldown_secs: u64) -> Self {
        Self {
            breakers: DashMap::new(),
            default_threshold,
            default_cooldown_secs,
            storage: None,
            tracker: TaskTracker::new(),
        }
    }

    /// Close the tracker and await every in-flight persistence task.
    ///
    /// Callers should invoke this during graceful shutdown *after* all
    /// state-transition sources have stopped producing new writes. Once
    /// closed the tracker rejects new tasks, so this is a one-shot drain;
    /// subsequent `spawn_upsert` / `spawn_delete` calls become no-ops (the
    /// tracker returns early) and only in-memory state remains authoritative
    /// for the brief window before process exit.
    pub async fn flush(&self) {
        self.tracker.close();
        self.tracker.wait().await;
    }

    /// Builder-style constructor that wires a storage backend for persistence
    /// of `Open` transitions. Use this at server boot after the storage
    /// backend is constructed.
    #[must_use]
    pub fn with_storage(mut self, storage: Arc<dyn StorageBackend>) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Hydrate the registry from previously persisted `Open` rows. Call this
    /// once at boot after [`Self::with_storage`]. Rows still within their
    /// cooldown window are restored as `Open`; rows whose cooldown has
    /// already expired are restored as `HalfOpen` so the first subsequent
    /// call probes the handler.
    ///
    /// Non-fatal on storage errors — the registry remains empty and callers
    /// see the default `Closed` behaviour, which is a safer default than
    /// failing boot outright.
    pub async fn load_from_storage(&self) {
        let Some(storage) = self.storage.as_ref() else {
            return;
        };
        match storage.list_open_circuit_breakers().await {
            Ok(rows) => {
                for state in rows {
                    let key = Key(state.tenant_id.clone(), state.handler.clone());
                    self.breakers.insert(key, state);
                }
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "failed to load persisted circuit breakers; starting empty"
                );
            }
        }
    }

    /// Check if a handler is allowed to execute for a tenant. Returns `Ok(())`
    /// if allowed, or `Err(remaining_cooldown_secs)` if the circuit is open.
    pub fn check(&self, tenant_id: &TenantId, handler: &str) -> Result<(), u64> {
        let now = Utc::now();

        // Fast path: acquire a read lock. In the vast majority of cases (healthy
        // circuit), we can authorize execution without acquiring a write lock
        // on the DashMap shard, avoiding severe contention on the hot path.
        let search = KeyRef(tenant_id, handler);
        let q: &dyn CircuitKey = &search;
        if let Some(breaker) = self.breakers.get(q) {
            if matches!(breaker.state, BreakerState::Closed | BreakerState::HalfOpen) {
                return Ok(());
            }
        }

        let key = Key(tenant_id.clone(), handler.to_string());

        // Slow path: if the breaker is Open (or doesn't exist yet), we acquire
        // a write lock to potentially mutate it (e.g. initialize it, or check
        // if cooldown elapsed and transition to HalfOpen).
        let mut breaker = self
            .breakers
            .entry(key)
            .or_insert_with(|| self.default_breaker(tenant_id, handler));

        match breaker.state {
            BreakerState::Closed | BreakerState::HalfOpen => Ok(()),
            BreakerState::Open => {
                if let Some(opened_at) = breaker.opened_at {
                    #[allow(clippy::cast_sign_loss)]
                    let elapsed = (now - opened_at).num_seconds().max(0) as u64;
                    if elapsed >= breaker.cooldown_secs {
                        breaker.state = BreakerState::HalfOpen;
                        // Cooldown elapsed: transitioning to HalfOpen means the
                        // breaker is no longer protecting — remove from
                        // durable store so a crash now doesn't revive it.
                        let snapshot = breaker.clone();
                        drop(breaker);
                        self.spawn_delete(&snapshot);
                        Ok(())
                    } else {
                        Err(breaker.cooldown_secs - elapsed)
                    }
                } else {
                    // No opened_at means it was just set — cooldown starts now
                    breaker.opened_at = Some(now);
                    let cooldown = breaker.cooldown_secs;
                    let snapshot = breaker.clone();
                    drop(breaker);
                    self.spawn_upsert(&snapshot);
                    Err(cooldown)
                }
            }
        }
    }

    /// Record a successful execution for a tenant+handler. Clears failures and
    /// closes the breaker. If the breaker was previously `Open` the persisted
    /// row is removed so a subsequent crash doesn't revive it.
    pub fn record_success(&self, tenant_id: &TenantId, handler: &str) {
        let search = KeyRef(tenant_id, handler);
        let q: &dyn CircuitKey = &search;

        // Fast path: if the breaker is already completely healthy (Closed with 0 failures),
        // we can skip acquiring a write lock entirely. This makes the standard success
        // path effectively free of lock contention.
        if let Some(breaker) = self.breakers.get(q) {
            if breaker.state == BreakerState::Closed && breaker.failure_count == 0 {
                return;
            }
        }

        let mut delete_snapshot = None;
        if let Some(mut breaker) = self.breakers.get_mut(q) {
            let was_open = matches!(breaker.state, BreakerState::Open | BreakerState::HalfOpen);
            breaker.failure_count = 0;
            breaker.state = BreakerState::Closed;
            breaker.opened_at = None;
            if was_open {
                delete_snapshot = Some(breaker.clone());
            }
        }
        if let Some(snap) = delete_snapshot {
            self.spawn_delete(&snap);
        }
    }

    /// Record a failure for a tenant+handler. May trip the circuit to `Open`,
    /// which also persists the transition.
    pub fn record_failure(&self, tenant_id: &TenantId, handler: &str) {
        let now = Utc::now();
        let key = Key(tenant_id.clone(), handler.to_string());
        let mut tripped_snapshot = None;
        {
            let mut breaker = self
                .breakers
                .entry(key)
                .or_insert_with(|| self.default_breaker(tenant_id, handler));

            breaker.failure_count += 1;

            if breaker.failure_count >= breaker.failure_threshold
                && breaker.state != BreakerState::Open
            {
                breaker.state = BreakerState::Open;
                breaker.opened_at = Some(now);
                tripped_snapshot = Some(breaker.clone());
            }
        }
        if let Some(snap) = tripped_snapshot {
            self.spawn_upsert(&snap);
        }
    }

    /// List all breakers for a given tenant.
    pub fn list_for_tenant(&self, tenant_id: &TenantId) -> Vec<CircuitBreakerState> {
        self.breakers
            .iter()
            .filter(|item| &item.key().0 == tenant_id)
            .map(|item| item.value().clone())
            .collect()
    }

    /// List all breakers across every tenant. Intended for admin tooling.
    pub fn list_all(&self) -> Vec<CircuitBreakerState> {
        self.breakers
            .iter()
            .map(|item| item.value().clone())
            .collect()
    }

    /// Get the current state of a specific tenant+handler's breaker.
    pub fn get(&self, tenant_id: &TenantId, handler: &str) -> Option<CircuitBreakerState> {
        let search = KeyRef(tenant_id, handler);
        let q: &dyn CircuitKey = &search;
        self.breakers.get(q).map(|item| item.value().clone())
    }

    /// Reset a tenant+handler's breaker to `Closed`, clearing failures and
    /// removing any persisted row.
    pub fn reset(&self, tenant_id: &TenantId, handler: &str) {
        let search = KeyRef(tenant_id, handler);
        let q: &dyn CircuitKey = &search;
        let delete_snapshot = if let Some(mut breaker) = self.breakers.get_mut(q) {
            breaker.state = BreakerState::Closed;
            breaker.failure_count = 0;
            breaker.opened_at = None;
            Some(breaker.clone())
        } else {
            None
        };
        if let Some(snap) = delete_snapshot {
            self.spawn_delete(&snap);
        }
    }

    fn default_breaker(&self, tenant_id: &TenantId, handler: &str) -> CircuitBreakerState {
        CircuitBreakerState {
            tenant_id: tenant_id.clone(),
            handler: handler.to_string(),
            state: BreakerState::Closed,
            failure_count: 0,
            failure_threshold: self.default_threshold,
            cooldown_secs: self.default_cooldown_secs,
            opened_at: None,
        }
    }

    /// Tracked fire-and-forget upsert of a breaker snapshot. Silent no-op
    /// when no storage is wired (tests, non-durable configs). Once the
    /// tracker has been closed (see [`Self::flush`]) new tasks are rejected,
    /// which is the intended shutdown semantics.
    fn spawn_upsert(&self, snapshot: &CircuitBreakerState) {
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let state = snapshot.clone();
        self.tracker.spawn(async move {
            if let Err(err) = storage.upsert_circuit_breaker(&state).await {
                tracing::warn!(
                    tenant_id = %state.tenant_id,
                    handler = %state.handler,
                    error = %err,
                    "failed to persist circuit breaker open state"
                );
            }
        });
    }

    /// Tracked fire-and-forget delete of a breaker snapshot's persisted row.
    fn spawn_delete(&self, snapshot: &CircuitBreakerState) {
        let Some(storage) = self.storage.clone() else {
            return;
        };
        let tenant = snapshot.tenant_id.clone();
        let handler = snapshot.handler.clone();
        self.tracker.spawn(async move {
            if let Err(err) = storage.delete_circuit_breaker(&tenant, &handler).await {
                tracing::warn!(
                    tenant_id = %tenant,
                    handler = %handler,
                    error = %err,
                    "failed to delete persisted circuit breaker row"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(s: &str) -> TenantId {
        TenantId(s.into())
    }

    #[test]
    fn closed_allows_requests() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        assert!(cb.check(&tid("t"), "test_handler").is_ok());
    }

    #[test]
    fn opens_after_threshold() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        let t = tid("t");
        cb.record_failure(&t, "test_handler");
        cb.record_failure(&t, "test_handler");
        assert!(cb.check(&t, "test_handler").is_ok());
        cb.record_failure(&t, "test_handler");
        assert!(cb.check(&t, "test_handler").is_err());
    }

    #[test]
    fn success_resets_failures() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        let t = tid("t");
        cb.record_failure(&t, "test_handler");
        cb.record_failure(&t, "test_handler");
        cb.record_success(&t, "test_handler");
        cb.record_failure(&t, "test_handler");
        cb.record_failure(&t, "test_handler");
        assert!(cb.check(&t, "test_handler").is_ok());
    }

    #[test]
    fn reset_closes_circuit() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        let t = tid("t");
        for _ in 0..3 {
            cb.record_failure(&t, "test_handler");
        }
        assert!(cb.check(&t, "test_handler").is_err());
        cb.reset(&t, "test_handler");
        assert!(cb.check(&t, "test_handler").is_ok());
    }

    #[test]
    fn half_open_after_cooldown() {
        let cb = CircuitBreakerRegistry::new(3, 0);
        let t = tid("t");
        for _ in 0..3 {
            cb.record_failure(&t, "test_handler");
        }
        assert!(cb.check(&t, "test_handler").is_ok());
        let state = cb.get(&t, "test_handler").unwrap();
        assert_eq!(state.state, BreakerState::HalfOpen);
    }

    #[test]
    fn check_is_noop_on_first_call_per_handler() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        let t = tid("t");
        assert!(cb.check(&t, "h1").is_ok());
        assert!(cb.check(&t, "h2").is_ok());
        assert_eq!(cb.list_all().len(), 2);
    }

    #[test]
    fn get_returns_none_for_unknown_handler() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        assert!(cb.get(&tid("t"), "never_checked").is_none());
    }

    #[test]
    fn breakers_are_isolated_per_handler() {
        let cb = CircuitBreakerRegistry::new(2, 30);
        let t = tid("t");
        cb.record_failure(&t, "h1");
        cb.record_failure(&t, "h1");
        assert!(cb.check(&t, "h1").is_err());
        assert!(cb.check(&t, "h2").is_ok());
    }

    #[test]
    fn breakers_are_isolated_per_tenant() {
        // Same handler name, different tenants: one tenant tripping the
        // breaker must NOT affect the other tenant.
        let cb = CircuitBreakerRegistry::new(2, 30);
        let a = tid("tenant-a");
        let b = tid("tenant-b");
        cb.record_failure(&a, "shared_handler");
        cb.record_failure(&a, "shared_handler");
        assert!(cb.check(&a, "shared_handler").is_err());
        assert!(
            cb.check(&b, "shared_handler").is_ok(),
            "tenant-b must not be affected by tenant-a's failures on the same handler"
        );
    }

    #[test]
    fn list_for_tenant_filters_correctly() {
        let cb = CircuitBreakerRegistry::new(2, 30);
        let a = tid("a");
        let b = tid("b");
        cb.record_failure(&a, "h1");
        cb.record_failure(&b, "h2");
        cb.record_failure(&b, "h3");
        assert_eq!(cb.list_for_tenant(&a).len(), 1);
        assert_eq!(cb.list_for_tenant(&b).len(), 2);
        assert_eq!(cb.list_all().len(), 3);
    }

    #[test]
    fn record_success_on_untracked_handler_is_noop() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        cb.record_success(&tid("t"), "ghost_handler");
        assert!(cb.get(&tid("t"), "ghost_handler").is_none());
    }

    #[test]
    fn reset_on_untracked_handler_is_noop() {
        let cb = CircuitBreakerRegistry::new(3, 30);
        cb.reset(&tid("t"), "ghost");
        assert!(cb.get(&tid("t"), "ghost").is_none());
    }

    #[test]
    fn half_open_success_fully_closes() {
        let cb = CircuitBreakerRegistry::new(2, 0);
        let t = tid("t");
        cb.record_failure(&t, "h");
        cb.record_failure(&t, "h");
        cb.check(&t, "h").unwrap();
        assert_eq!(cb.get(&t, "h").unwrap().state, BreakerState::HalfOpen);
        cb.record_success(&t, "h");
        let s = cb.get(&t, "h").unwrap();
        assert_eq!(s.state, BreakerState::Closed);
        assert_eq!(s.failure_count, 0);
        assert!(s.opened_at.is_none());
    }

    #[test]
    fn failure_threshold_stored_from_defaults() {
        let cb = CircuitBreakerRegistry::new(7, 120);
        let t = tid("t");
        cb.record_failure(&t, "x");
        let s = cb.get(&t, "x").unwrap();
        assert_eq!(s.failure_threshold, 7);
        assert_eq!(s.cooldown_secs, 120);
        assert_eq!(s.failure_count, 1);
        assert_eq!(s.state, BreakerState::Closed);
    }

    #[test]
    fn list_all_reflects_cleared_state_after_reset() {
        let cb = CircuitBreakerRegistry::new(2, 30);
        let t = tid("t");
        cb.record_failure(&t, "a");
        cb.record_failure(&t, "a");
        assert!(cb.check(&t, "a").is_err());
        cb.reset(&t, "a");
        let all = cb.list_all();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].state, BreakerState::Closed);
        assert_eq!(all[0].failure_count, 0);
    }

    /// Stab#15: shutdown path must drain pending persistence writes.
    /// Pre-fix, `spawn_upsert` was a raw `tokio::spawn` with no handle —
    /// on runtime teardown those tasks could be aborted mid-write, leaving
    /// the DB lagging the in-memory state that the next process hydrated
    /// against at boot. `flush` closes the tracker and awaits all
    /// outstanding writes so the persisted row is guaranteed on disk.
    #[tokio::test]
    async fn flush_drains_pending_persistence_writes() {
        use orch8_storage::sqlite::SqliteStorage;
        let storage: Arc<dyn StorageBackend> = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let cb = CircuitBreakerRegistry::new(2, 30).with_storage(storage.clone());
        let t = tid("t");
        // Trip the breaker — this triggers `spawn_upsert` on the tracker.
        cb.record_failure(&t, "payment_api");
        cb.record_failure(&t, "payment_api");
        assert!(cb.check(&t, "payment_api").is_err());

        // Flush: must complete after the tracked upsert has finished.
        cb.flush().await;

        // The persisted row is now visible — a fresh registry rehydrating
        // from the same storage sees Open state and preserves isolation.
        let rehydrated = CircuitBreakerRegistry::new(2, 30).with_storage(storage);
        rehydrated.load_from_storage().await;
        let s = rehydrated.get(&t, "payment_api").expect("row persisted");
        assert!(matches!(
            s.state,
            BreakerState::Open | BreakerState::HalfOpen
        ));
    }

    #[test]
    fn noop_is_not_tracked() {
        assert!(!is_breaker_tracked("noop"));
    }

    #[test]
    fn log_is_not_tracked() {
        assert!(!is_breaker_tracked("log"));
    }

    #[test]
    fn sleep_is_not_tracked() {
        assert!(!is_breaker_tracked("sleep"));
    }

    #[test]
    fn fail_is_not_tracked() {
        assert!(!is_breaker_tracked("fail"));
    }

    #[test]
    fn http_handler_is_tracked() {
        assert!(is_breaker_tracked("http.get"));
    }

    #[test]
    fn llm_handler_is_tracked() {
        assert!(is_breaker_tracked("openai.chat"));
    }

    #[test]
    fn emit_event_is_not_tracked() {
        assert!(!is_breaker_tracked("emit_event"));
    }

    #[test]
    fn send_signal_is_not_tracked() {
        assert!(!is_breaker_tracked("send_signal"));
    }

    #[test]
    fn query_instance_is_not_tracked() {
        assert!(!is_breaker_tracked("query_instance"));
    }

    #[test]
    fn human_review_is_not_tracked() {
        assert!(!is_breaker_tracked("human_review"));
    }

    #[test]
    fn self_modify_is_not_tracked() {
        assert!(!is_breaker_tracked("self_modify"));
    }
}
