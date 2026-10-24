//! The control-plane credential shared by the node client (register,
//! advertise, worker leases, delegation) and the sync reporter.
//!
//! Preferred: a short-lived **device session** (`dst_…`) that the host app's
//! backend mints with `POST /runtimes/device-sessions` for this device and
//! this node's runtime id, delivered through a host [`TokenProvider`]
//! (`MobileEngine::set_token_provider`). Every request carries the current
//! token; a `401` asks the provider for a fresh one (once per stale token,
//! however many requests saw it) and retries the request once.
//!
//! Legacy: a static `sync_api_key` from the config. It keeps working, but a
//! stored API key inside an app binary is extractable by anyone who has the
//! app; when the control plane reports that the key is operator-capable (or
//! the root key) the SDK logs a warning once.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock as StdRwLock, Weak};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::AbortOnDropHandle;
use uuid::Uuid;

use tracing::{debug, warn};

use crate::error::TokenProvider;

/// Prefix of a device-session token.
pub(crate) const DEVICE_SESSION_PREFIX: &str = "dst_";
/// Longest the host's `refresh_token` may take while the single-flight lock is
/// held (MC-12). A hung provider must not wedge every authenticated request.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(20);
/// Retired owners may have non-cooperative callbacks still physically running.
/// Bound their aggregate work/identity metadata while allowing successor owners
/// to make progress until this credential-wide budget is exhausted.
// The smallest production runtime has four blocking threads. Limit refresh
// work to half of that pool so stale auth callbacks alone cannot occupy it.
const MAX_PHYSICAL_REFRESHES: usize = 2;
/// First retry delay after a refresh that failed or returned the same token;
/// doubles per consecutive failure up to [`REFRESH_BACKOFF_MAX`].
const REFRESH_BACKOFF_BASE: Duration = Duration::from_secs(5);
const REFRESH_BACKOFF_MAX: Duration = Duration::from_secs(5 * 60);
/// Response header naming an operator-capable principal on `/mobile/*`.
const PRINCIPAL_SCOPE_HEADER: &str = "x-orch8-principal-scope";

pub(crate) struct Credential {
    state: StdRwLock<CredentialState>,
    warned_operator: AtomicBool,
    refresh_timeout_ms: AtomicU64,
    physical_refresh_slots: Arc<Semaphore>,
}

struct CredentialState {
    closed: bool,
    owner: Uuid,
    revision: Uuid,
    cancelled: CancellationToken,
    refresh: Arc<tokio::sync::Mutex<()>>,
    physical_refresh: Arc<AtomicBool>,
    physical_owners: Vec<PhysicalOwner>,
    token: String,
    provider: Option<Arc<dyn TokenProvider>>,
    pending_install: bool,
    refresh_failure: Option<RefreshFailure>,
}

/// One atomic credential/header snapshot. Its opaque generation contains no token.
pub(crate) struct CredentialSnapshot {
    available: bool,
    pub token: String,
    pub generation: String,
}

struct PhysicalOwner {
    provider: Weak<dyn TokenProvider>,
    running: Weak<AtomicBool>,
}

impl CredentialState {
    fn physical_for(&mut self, provider: &Arc<dyn TokenProvider>) -> Arc<AtomicBool> {
        // Weak entries cannot retain retired providers or completed calls.
        self.physical_owners
            .retain(|entry| entry.running.strong_count() > 0 && entry.provider.strong_count() > 0);
        let candidate = Arc::downgrade(provider);
        if let Some(running) = self.physical_owners.iter().find_map(|entry| {
            // Do not upgrade/drop provider Arcs under the state lock: a last
            // strong reference can invoke foreign destruction and re-enter.
            if entry.provider.ptr_eq(&candidate) {
                entry.running.upgrade()
            } else {
                None
            }
        }) {
            return running;
        }
        let running = Arc::new(AtomicBool::new(false));
        self.physical_owners.push(PhysicalOwner {
            provider: candidate,
            running: Arc::downgrade(&running),
        });
        running
    }

    fn snapshot(&self) -> CredentialSnapshot {
        CredentialSnapshot {
            available: !self.closed && (self.provider.is_none() || !self.token.trim().is_empty()),
            token: self.token.clone(),
            generation: format!("{}:{}", self.owner, self.revision),
        }
    }
}

// Owned by the blocking closure, including while queued. Timeout/cancellation
// cannot free physical admission before the closure returns or is dropped.
struct PhysicalRefresh {
    // Fields drop in declaration order. Foreign destruction can block or
    // re-enter, so retain capacity until the provider is fully destroyed.
    provider: Arc<dyn TokenProvider>,
    running: Arc<AtomicBool>,
    _slot: OwnedSemaphorePermit,
}

impl Drop for PhysicalRefresh {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

struct RefreshFailure {
    revision: Uuid,
    retry_at: Instant,
    consecutive: u32,
}

fn refresh_backoff(consecutive: u32) -> Duration {
    REFRESH_BACKOFF_BASE
        .saturating_mul(2u32.saturating_pow(consecutive.saturating_sub(1).min(16)))
        .min(REFRESH_BACKOFF_MAX)
}

fn parse_generation(generation: &str) -> Option<(Uuid, Uuid)> {
    let (owner, revision) = generation.split_once(':')?;
    Some((
        Uuid::parse_str(owner).ok()?,
        Uuid::parse_str(revision).ok()?,
    ))
}

impl Credential {
    pub fn new(token: String) -> Arc<Self> {
        Arc::new(Self {
            state: StdRwLock::new(CredentialState {
                closed: false,
                owner: Uuid::new_v4(),
                revision: Uuid::new_v4(),
                cancelled: CancellationToken::new(),
                refresh: Arc::new(tokio::sync::Mutex::new(())),
                physical_refresh: Arc::new(AtomicBool::new(false)),
                physical_owners: Vec::new(),
                token,
                provider: None,
                pending_install: false,
                refresh_failure: None,
            }),
            physical_refresh_slots: Arc::new(Semaphore::new(MAX_PHYSICAL_REFRESHES)),
            warned_operator: AtomicBool::new(false),
            refresh_timeout_ms: AtomicU64::new(
                u64::try_from(REFRESH_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
            ),
        })
    }

    #[cfg(test)]
    fn set_refresh_timeout(&self, timeout: Duration) {
        self.refresh_timeout_ms.store(
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn note_refresh_failure(&self, owner: Uuid, revision: Uuid) {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner != owner || state.revision != revision {
            return;
        }
        let consecutive = state
            .refresh_failure
            .as_ref()
            .filter(|failure| failure.revision == revision)
            .map_or(1, |failure| failure.consecutive.saturating_add(1));
        state.refresh_failure = Some(RefreshFailure {
            revision,
            retry_at: Instant::now() + refresh_backoff(consecutive),
            consecutive,
        });
    }

    pub fn snapshot(&self) -> CredentialSnapshot {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
    }

    /// Legacy anonymous mode remains available; an installed provider that
    /// has not produced a token must never silently downgrade to that mode.
    pub fn request_snapshot(&self) -> Option<CredentialSnapshot> {
        let snapshot = self.snapshot();
        snapshot.available.then_some(snapshot)
    }

    /// Refresh an empty initial credential before any network dispatch. A
    /// replacement while waiting fails this request rather than adopting the
    /// successor's principal for an already prepared business operation.
    pub async fn prepare_snapshot(&self) -> Option<CredentialSnapshot> {
        let snapshot = self.snapshot();
        if snapshot.available {
            return Some(snapshot);
        }
        self.refresh_generation(&snapshot.generation).await;
        // The original current_token callback can also finish while refresh
        // runs. It remains a valid initial credential if refresh fails.
        self.retry_snapshot(&snapshot.generation)
    }

    pub fn current(&self) -> String {
        self.state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .token
            .clone()
    }

    pub fn is_configured(&self) -> bool {
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        !state.closed && (!state.token.is_empty() || state.provider.is_some())
    }

    /// Retire authorization before releasing foreign ownership. The caller must
    /// drop the returned provider outside its own locks and runtime bridges.
    pub fn close(&self) -> Option<Arc<dyn TokenProvider>> {
        let (cancelled, provider) = {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                return None;
            }
            state.closed = true;
            state.owner = Uuid::new_v4();
            state.revision = Uuid::new_v4();
            state.token.clear();
            state.pending_install = false;
            state.refresh_failure = None;
            (state.cancelled.clone(), state.provider.take())
        };
        cancelled.cancel();
        self.physical_refresh_slots.close();
        provider
    }

    pub fn is_device_session(&self) -> bool {
        self.current().starts_with(DEVICE_SESSION_PREFIX)
    }

    /// A provider installation is a new authorization owner, even if its
    /// initial token is empty or identical. Reserve before invoking host code.
    pub fn set_provider(&self, provider: Arc<dyn TokenProvider>) {
        let owner = Uuid::new_v4();
        let (previous, retired_provider) = {
            let mut state = self
                .state
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.closed {
                drop(state);
                return;
            }
            let previous = std::mem::replace(&mut state.cancelled, CancellationToken::new());
            state.owner = owner;
            state.refresh = Arc::new(tokio::sync::Mutex::new(()));
            state.physical_refresh = state.physical_for(&provider);
            state.revision = Uuid::new_v4();
            state.token.clear();
            let retired_provider = state.provider.replace(Arc::clone(&provider));
            state.pending_install = true;
            state.refresh_failure = None;
            (previous, retired_provider)
        };
        previous.cancel();
        drop(retired_provider);
        // Host callbacks run outside credential locks and can re-enter safely.
        let token = provider.current_token();
        let mut state = self
            .state
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.owner == owner && state.pending_install {
            state.pending_install = false;
            state.token = token;
            state.revision = Uuid::new_v4();
        }
    }

    /// Atomically choose retry headers only within the original provider owner.
    pub fn retry_snapshot(&self, generation: &str) -> Option<CredentialSnapshot> {
        let (owner, _) = parse_generation(generation)?;
        let state = self
            .state
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = state.snapshot();
        (state.owner == owner && snapshot.available).then_some(snapshot)
    }

    /// Refresh an owned generation before initial dispatch or after a 401. Replacement
    /// cancels queued/running waits and prevents any old callback publication.
    pub async fn refresh_generation(&self, generation: &str) -> bool {
        let Some((owner, revision)) = parse_generation(generation) else {
            return false;
        };
        let (cancelled, refresh) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.owner != owner {
                return false;
            }
            (state.cancelled.clone(), Arc::clone(&state.refresh))
        };
        let _single_flight = tokio::select! {
            biased;
            () = cancelled.cancelled() => return false,
            guard = refresh.lock() => guard,
        };
        let (rejected_token, physical, initializing) = {
            let state = self
                .state
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state.owner != owner {
                return false;
            }
            if state.revision != revision {
                return !state.token.trim().is_empty();
            }
            let Some(provider) = state.provider.clone() else {
                return false;
            };
            if state.refresh_failure.as_ref().is_some_and(|failure| {
                failure.revision == revision && Instant::now() < failure.retry_at
            }) {
                debug!("token refresh suppressed by backoff");
                return false;
            }
            let Ok(slot) = Arc::clone(&self.physical_refresh_slots).try_acquire_owned() else {
                // No queue and no negative-cache penalty: a different owner
                // can recover as soon as an old physical call actually ends.
                debug!("token refresh physical capacity exhausted");
                return false;
            };
            if state.physical_refresh.swap(true, Ordering::AcqRel) {
                // A timed-out host callback still owns physical admission.
                return false;
            }
            let physical = PhysicalRefresh {
                provider,
                running: Arc::clone(&state.physical_refresh),
                _slot: slot,
            };
            // Initial publication may finish while refresh runs. Keep it
            // eligible until a successful refresh supersedes it; a failed
            // refresh must not strand an otherwise usable installation.
            (state.token.clone(), physical, state.pending_install)
        };
        let timeout = Duration::from_millis(self.refresh_timeout_ms.load(Ordering::Relaxed));
        let callback_cancelled = cancelled.clone();
        let call = AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
            let physical_call = physical;
            if callback_cancelled.is_cancelled() {
                return Ok(String::new());
            }
            physical_call.provider.refresh_token()
        }));
        let result = tokio::select! {
            biased;
            () = cancelled.cancelled() => return false,
            result = tokio::time::timeout(timeout, call) => result,
        };
        match result {
            Ok(Ok(Ok(token))) if !token.trim().is_empty() && token != rejected_token => {
                let mut state = self
                    .state
                    .write()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.owner != owner {
                    return false;
                }
                // This owner holds the refresh mutex. If initialization was
                // pending at admission, only its initial callback can have
                // changed the revision meanwhile; refresh takes precedence.
                if state.revision != revision && !initializing {
                    return !state.token.trim().is_empty();
                }
                state.pending_install = false;
                state.token = token;
                state.revision = Uuid::new_v4();
                state.refresh_failure = None;
                debug!("node credential refreshed");
                return true;
            }
            Ok(Ok(Ok(_))) => {}
            Ok(Ok(Err(error))) => {
                warn!(%error, "token provider failed to refresh the node credential");
            }
            Ok(Err(error)) => warn!(%error, "token provider refresh panicked"),
            Err(_) => warn!(?timeout, "token provider refresh timed out"),
        }
        self.note_refresh_failure(owner, revision);
        false
    }

    /// Warn once when the control plane reports that this app authenticates
    /// with an operator-capable key or the root key.
    pub fn observe(&self, response: &reqwest::Response) {
        let scope = response
            .headers()
            .get(PRINCIPAL_SCOPE_HEADER)
            .and_then(|value| value.to_str().ok());
        if matches!(scope, Some("operator" | "root"))
            && !self.warned_operator.swap(true, Ordering::Relaxed)
        {
            warn!(
                scope = scope.unwrap_or_default(),
                "orch8: this app authenticates with an operator-capable API key; anyone who \
                 extracts it from the app binary controls the tenant. Mint short-lived device \
                 sessions on your backend (POST /runtimes/device-sessions) and pass them through \
                 MobileEngine.set_token_provider instead"
            );
        }
    }

    /// Retry at most once, using only a snapshot owned by the original provider.
    pub async fn send(
        &self,
        build: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, orch8_engine::remote_worker::LeaseRequestError> {
        let sent = self
            .prepare_snapshot()
            .await
            .ok_or(orch8_engine::remote_worker::LeaseRequestError::AuthorizationUnavailable)?;
        let response = build(&sent.token).send().await?;
        self.observe(&response);
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && self.refresh_generation(&sent.generation).await
            && let Some(retry) = self.retry_snapshot(&sent.generation)
        {
            let response = build(&retry.token).send().await?;
            self.observe(&response);
            return Ok(response);
        }
        Ok(response)
    }
}

impl std::fmt::Debug for Credential {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("Credential(<redacted>)")
    }
}

#[cfg(test)]
#[path = "credential_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "credential_budget_tests.rs"]
mod budget_tests;
