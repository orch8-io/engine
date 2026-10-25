//! Public explicit-owner native SDK. Descriptors originate in trusted host
//! account configuration; signed sync independently authenticates its principal.
use crate::{HandlerError, InstanceStateKind, MobileEngine, MobileError};
use std::{
    future::Future,
    sync::{Arc, Weak},
};
use tokio_util::sync::CancellationToken;

mod account;
mod catalog;
mod run;
pub use account::MobileAccount;
pub use catalog::MobileCatalog;
pub use run::MobileCatalogRun;

#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileAccountIdentity {
    pub account_key: String,
    pub issuer: String,
    pub subject: String,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileCatalogConfiguration {
    pub catalog_key: String,
    pub source: String,
    /// Ed25519 verification key in the existing `ed25519:<base64>` format.
    pub signing_authority: String,
    pub manifest_url: String,
    pub identity_url: String,
}

/// Reuse an operation key only for an identical request. It remains charged
/// after cancellation; preparing before starting keeps its handle on failure.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileCatalogRunRequest {
    pub workflow: String,
    pub version: Option<i32>,
    pub input_json: String,
    pub operation_key: String,
}

/// Engine-issued callback identity. It is never accepted as authorization by
/// any SDK method, and is never derived from mutable workflow input.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileCatalogCallbackContext {
    pub account_id: String,
    pub catalog_id: String,
    pub owner_epoch: i64,
    pub run_id: String,
    pub block_id: String,
    pub attempt: u32,
    pub effect_id: String,
}

#[uniffi::export(with_foreign)]
pub trait MobileCatalogStepHandler: Send + Sync {
    fn execute(
        &self,
        context: MobileCatalogCallbackContext,
        step_name: String,
        input: String,
    ) -> Result<String, HandlerError>;
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileCatalogRunSnapshot {
    pub instance_id: String,
    pub account_id: String,
    pub catalog_id: String,
    /// The viewing session, which may differ from a historical execution epoch.
    pub session_epoch: i64,
    pub state: InstanceStateKind,
    pub data_json: String,
    pub pending_steps: Vec<String>,
    pub created_at: String,
    /// Pass this exact version to `complete_step`; a stale reply returns false.
    pub updated_at: String,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum MobileCatalogRequestState {
    Prepared,
    Cancelled,
}

#[derive(Clone, Debug, uniffi::Enum)]
pub enum MobileCatalogNativeState {
    NotMaterialized,
    Present { state: InstanceStateKind },
    Unavailable,
}

#[derive(Clone, Debug, uniffi::Record)]
pub struct MobileCatalogRunSummary {
    pub instance_id: String,
    pub account_id: String,
    pub catalog_id: String,
    pub owner_epoch: i64,
    pub workflow: String,
    pub version: i32,
    pub sequence_id: String,
    pub created_at: String,
    pub request_state: MobileCatalogRequestState,
    pub native_state: MobileCatalogNativeState,
}

/// Opaque pagination position. The destination catalog revalidates its scope
/// and session; possessing this object grants no access to a run.
#[derive(uniffi::Object)]
pub struct MobileCatalogHistoryCursor {
    cursor: crate::storage::scopes::runs::history::CatalogRunCursor,
}

#[derive(Clone, uniffi::Record)]
pub struct MobileCatalogRunPage {
    pub items: Vec<MobileCatalogRunSummary>,
    pub next: Option<Arc<MobileCatalogHistoryCursor>>,
}

struct View {
    engine: Weak<MobileEngine>,
    closed: CancellationToken,
}
impl View {
    fn root(engine: &Arc<MobileEngine>) -> Self {
        Self {
            engine: Arc::downgrade(engine),
            closed: engine.cancel.child_token(),
        }
    }
    fn child(&self) -> Self {
        Self {
            engine: self.engine.clone(),
            closed: self.closed.child_token(),
        }
    }
    fn engine(&self) -> Result<Arc<MobileEngine>, MobileError> {
        if self.closed.is_cancelled() {
            return Err(MobileError::Shutdown);
        }
        let engine = self.engine.upgrade().ok_or(MobileError::Shutdown)?;
        engine.ensure_open()?;
        Ok(engine)
    }
    fn run<T>(
        &self,
        engine: &MobileEngine,
        future: impl Future<Output = Result<T, MobileError>>,
    ) -> Result<T, MobileError> {
        let result = engine.run_with_timeout(async {
            tokio::select! {
                biased;
                () = self.closed.cancelled() => Err(MobileError::Shutdown),
                result = future => result,
            }
        });
        if self.closed.is_cancelled() {
            Err(MobileError::Shutdown)
        } else {
            result
        }
    }
}

/// Reuse the signed-catalog loopback transport fixture through the actual public
/// SDK sync method. Production SDK objects always construct a validated HTTPS
/// synchronizer and have no injected transport or insecure configuration flag.
#[cfg(test)]
pub(crate) fn catalog_for_transport_test(
    account: &MobileAccount,
    scope: crate::storage::scopes::CatalogScope,
    synchronizer: Arc<crate::sync::catalog::CatalogSynchronizer>,
) -> Arc<MobileCatalog> {
    assert_eq!(scope.account_id(), account.scope.account_id());
    assert_eq!(scope.epoch(), account.scope.epoch());
    Arc::new(MobileCatalog {
        view: account.view.child(),
        scope,
        test_synchronizer: Some(synchronizer),
    })
}

#[cfg(test)]
mod tests;
