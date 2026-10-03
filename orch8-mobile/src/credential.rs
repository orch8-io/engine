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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock as StdRwLock};

use tracing::{debug, warn};

use crate::error::TokenProvider;

/// Prefix of a device-session token.
pub(crate) const DEVICE_SESSION_PREFIX: &str = "dst_";
/// Response header naming an operator-capable principal on `/mobile/*`.
const PRINCIPAL_SCOPE_HEADER: &str = "x-orch8-principal-scope";

pub(crate) struct Credential {
    token: StdRwLock<String>,
    provider: StdRwLock<Option<Arc<dyn TokenProvider>>>,
    refresh: tokio::sync::Mutex<()>,
    warned_operator: AtomicBool,
}

impl Credential {
    pub fn new(token: String) -> Arc<Self> {
        Arc::new(Self {
            token: StdRwLock::new(token),
            provider: StdRwLock::new(None),
            refresh: tokio::sync::Mutex::new(()),
            warned_operator: AtomicBool::new(false),
        })
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
        // The host callback may block on its own network call.
        match tokio::task::spawn_blocking(move || provider.refresh_token()).await {
            Ok(Ok(token)) if !token.is_empty() && token != stale => {
                debug!("node credential refreshed after 401");
                self.set(token);
                true
            }
            Ok(Ok(_)) => false,
            Ok(Err(error)) => {
                warn!(%error, "token provider failed to refresh the node credential");
                false
            }
            Err(error) => {
                warn!(%error, "token provider refresh panicked");
                false
            }
        }
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
