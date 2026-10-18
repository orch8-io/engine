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
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;

use tokio::time::Instant;

use tracing::{debug, warn};

use crate::error::TokenProvider;

/// Prefix of a device-session token.
pub(crate) const DEVICE_SESSION_PREFIX: &str = "dst_";
/// Longest the host's `refresh_token` may take while the single-flight lock is
/// held (MC-12). A hung provider must not wedge every authenticated request.
const REFRESH_TIMEOUT: Duration = Duration::from_secs(20);
/// First retry delay after a refresh that failed or returned the same token;
/// doubles per consecutive failure up to [`REFRESH_BACKOFF_MAX`].
const REFRESH_BACKOFF_BASE: Duration = Duration::from_secs(5);
const REFRESH_BACKOFF_MAX: Duration = Duration::from_secs(5 * 60);
/// Response header naming an operator-capable principal on `/mobile/*`.
const PRINCIPAL_SCOPE_HEADER: &str = "x-orch8-principal-scope";

pub(crate) struct Credential {
    token: StdRwLock<String>,
    provider: StdRwLock<Option<Arc<dyn TokenProvider>>>,
    refresh: tokio::sync::Mutex<()>,
    warned_operator: AtomicBool,
    refresh_timeout_ms: AtomicU64,
    /// Negative cache (MC-12): the stale token whose refresh just failed (or
    /// returned the same token), when to try again, and how many in a row.
    refresh_failure: StdMutex<Option<RefreshFailure>>,
}

struct RefreshFailure {
    stale: String,
    retry_at: Instant,
    consecutive: u32,
}

/// Delay before the next refresh attempt after `consecutive` failures.
fn refresh_backoff(consecutive: u32) -> Duration {
    REFRESH_BACKOFF_BASE
        .saturating_mul(2u32.saturating_pow(consecutive.saturating_sub(1).min(16)))
        .min(REFRESH_BACKOFF_MAX)
}

impl Credential {
    pub fn new(token: String) -> Arc<Self> {
        Arc::new(Self {
            token: StdRwLock::new(token),
            provider: StdRwLock::new(None),
            refresh: tokio::sync::Mutex::new(()),
            warned_operator: AtomicBool::new(false),
            refresh_timeout_ms: AtomicU64::new(
                u64::try_from(REFRESH_TIMEOUT.as_millis()).unwrap_or(u64::MAX),
            ),
            refresh_failure: StdMutex::new(None),
        })
    }

    #[cfg(test)]
    fn set_refresh_timeout(&self, timeout: Duration) {
        self.refresh_timeout_ms.store(
            u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn note_refresh_failure(&self, stale: &str) {
        let mut slot = self
            .refresh_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let consecutive = slot
            .as_ref()
            .filter(|failure| failure.stale == stale)
            .map_or(1, |failure| failure.consecutive.saturating_add(1));
        *slot = Some(RefreshFailure {
            stale: stale.to_owned(),
            retry_at: Instant::now() + refresh_backoff(consecutive),
            consecutive,
        });
    }

    fn refresh_suppressed(&self, stale: &str) -> bool {
        self.refresh_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|failure| failure.stale == stale && Instant::now() < failure.retry_at)
    }

    /// The token requests are sent with right now.
    pub fn current(&self) -> String {
        self.token
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether any credential is configured (a token or a provider).
    pub fn is_configured(&self) -> bool {
        !self.current().is_empty() || self.provider().is_some()
    }

    /// Whether the current token is a scoped device session.
    pub fn is_device_session(&self) -> bool {
        self.current().starts_with(DEVICE_SESSION_PREFIX)
    }

    fn provider(&self) -> Option<Arc<dyn TokenProvider>> {
        self.provider
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Install the host's token provider and adopt its current token.
    pub fn set_provider(&self, provider: Arc<dyn TokenProvider>) {
        let token = provider.current_token();
        *self
            .provider
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(provider);
        if !token.is_empty() {
            self.set(token);
        }
    }

    fn set(&self, token: String) {
        *self
            .token
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = token;
    }

    /// After a `401` answered a request sent with `stale`: ask the provider
    /// for a fresh token unless another request already did. `true` when a
    /// different token is now current (worth one retry).
    pub async fn refresh_after_unauthorized(&self, stale: &str) -> bool {
        let _single_flight = self.refresh.lock().await;
        if self.current() != stale {
            return true;
        }
        let Some(provider) = self.provider() else {
            return false;
        };
        // A provider that just failed (or handed back the same token) is not
        // asked again for this token until its backoff elapses: a rejected
        // credential must not hammer the host's backend on every 401.
        if self.refresh_suppressed(stale) {
            debug!("token refresh suppressed by backoff");
            return false;
        }
        // The host callback may block on its own network call, so it runs on
        // the blocking pool under a timeout (it holds the single-flight lock).
        let timeout = Duration::from_millis(self.refresh_timeout_ms.load(Ordering::Relaxed));
        let call = tokio::task::spawn_blocking(move || provider.refresh_token());
        let refreshed = match tokio::time::timeout(timeout, call).await {
            Ok(Ok(Ok(token))) if !token.is_empty() && token != stale => {
                debug!("node credential refreshed after 401");
                self.set(token);
                *self
                    .refresh_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                return true;
            }
            Ok(Ok(Ok(_))) => false,
            Ok(Ok(Err(error))) => {
                warn!(%error, "token provider failed to refresh the node credential");
                false
            }
            Ok(Err(error)) => {
                warn!(%error, "token provider refresh panicked");
                false
            }
            Err(_elapsed) => {
                warn!(?timeout, "token provider refresh timed out");
                false
            }
        };
        self.note_refresh_failure(stale);
        refreshed
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

    /// Send the request `build` makes for a token; on `401`, refresh the
    /// token (see [`Self::refresh_after_unauthorized`]) and send it once more.
    pub async fn send(
        &self,
        build: impl Fn(&str) -> reqwest::RequestBuilder,
    ) -> reqwest::Result<reqwest::Response> {
        let token = self.current();
        let response = build(&token).send().await?;
        self.observe(&response);
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && self.refresh_after_unauthorized(&token).await
        {
            let response = build(&self.current()).send().await?;
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
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicU32;

    struct Rotating {
        next: Mutex<Vec<String>>,
        refreshes: AtomicU32,
    }

    impl TokenProvider for Rotating {
        fn current_token(&self) -> String {
            "dst_first".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            self.next
                .lock()
                .unwrap()
                .pop()
                .ok_or(crate::MobileError::Engine {
                    message: "no more tokens".into(),
                })
        }
    }

    #[tokio::test]
    async fn refresh_is_single_flight_per_stale_token() {
        let credential = Credential::new(String::new());
        assert!(!credential.is_configured());
        let provider = Arc::new(Rotating {
            next: Mutex::new(vec!["dst_second".into()]),
            refreshes: AtomicU32::new(0),
        });
        credential.set_provider(provider.clone());
        assert!(credential.is_configured());
        assert!(credential.is_device_session());
        assert_eq!(credential.current(), "dst_first");

        assert!(credential.refresh_after_unauthorized("dst_first").await);
        assert_eq!(credential.current(), "dst_second");
        // A second request that also saw the stale token does not refresh
        // again: the token already moved on.
        assert!(credential.refresh_after_unauthorized("dst_first").await);
        assert_eq!(provider.refreshes.load(Ordering::SeqCst), 1);
        // The provider has nothing newer: no retry.
        assert!(!credential.refresh_after_unauthorized("dst_second").await);
    }

    struct Scripted {
        refreshes: AtomicU32,
        hang: bool,
    }

    impl TokenProvider for Scripted {
        fn current_token(&self) -> String {
            "dst_first".into()
        }
        fn refresh_token(&self) -> Result<String, crate::MobileError> {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            if self.hang {
                std::thread::sleep(Duration::from_millis(400));
            }
            // The backend keeps handing back the rejected token.
            Ok("dst_first".into())
        }
    }

    #[test]
    fn mc12_refresh_backoff_doubles_and_caps() {
        assert_eq!(refresh_backoff(1), Duration::from_secs(5));
        assert_eq!(refresh_backoff(2), Duration::from_secs(10));
        assert_eq!(refresh_backoff(3), Duration::from_secs(20));
        assert_eq!(refresh_backoff(50), REFRESH_BACKOFF_MAX);
    }

    #[tokio::test]
    async fn mc12_same_token_refresh_is_negative_cached_until_backoff_elapses() {
        let credential = Credential::new(String::new());
        let provider = Arc::new(Scripted {
            refreshes: AtomicU32::new(0),
            hang: false,
        });
        credential.set_provider(provider.clone());

        // Every 401 sees the same rejected token and the provider returns it.
        for _ in 0..5 {
            assert!(!credential.refresh_after_unauthorized("dst_first").await);
        }
        assert_eq!(
            provider.refreshes.load(Ordering::SeqCst),
            1,
            "the host backend must not be hit on every 401"
        );

        // Backoff elapses.
        credential
            .refresh_failure
            .lock()
            .unwrap()
            .as_mut()
            .unwrap()
            .retry_at = Instant::now();
        assert!(!credential.refresh_after_unauthorized("dst_first").await);
        assert_eq!(
            provider.refreshes.load(Ordering::SeqCst),
            2,
            "retried after backoff"
        );
    }

    #[tokio::test]
    async fn mc12_a_hung_provider_times_out_instead_of_holding_the_lock() {
        let credential = Credential::new(String::new());
        credential.set_refresh_timeout(Duration::from_millis(50));
        let provider = Arc::new(Scripted {
            refreshes: AtomicU32::new(0),
            hang: true,
        });
        credential.set_provider(provider);

        let started = std::time::Instant::now();
        assert!(!credential.refresh_after_unauthorized("dst_first").await);
        assert!(
            started.elapsed() < Duration::from_millis(350),
            "must give up at the timeout, not wait for the provider"
        );
    }

    #[tokio::test]
    async fn a_static_key_without_provider_never_refreshes() {
        let credential = Credential::new("sk_static".into());
        assert!(credential.is_configured());
        assert!(!credential.is_device_session());
        assert!(!credential.refresh_after_unauthorized("sk_static").await);
        assert_eq!(credential.current(), "sk_static");
        assert_eq!(format!("{credential:?}"), "Credential(<redacted>)");
    }
}
