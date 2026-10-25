//! A metadata history view retains its original scope, not a runtime/host owner.
use super::CatalogHandlers;
use crate::{
    MobileError,
    storage::scopes::{
        CatalogScope,
        runs::history::{CatalogRunCursor, CatalogRunPage},
    },
};
use std::sync::{Arc, Weak};
use tokio_util::sync::CancellationToken;

pub(crate) struct CatalogRunHistory {
    runtime: Weak<CatalogHandlers>,
    scope: CatalogScope,
    closed: CancellationToken,
}

impl CatalogHandlers {
    pub(crate) fn run_history(self: &Arc<Self>, scope: &CatalogScope) -> CatalogRunHistory {
        CatalogRunHistory {
            runtime: Arc::downgrade(self),
            scope: scope.clone(),
            closed: CancellationToken::new(),
        }
    }
}

impl CatalogRunHistory {
    pub(crate) fn close(&self) {
        self.closed.cancel();
    }

    fn ensure_open(&self, runtime: &CatalogHandlers) -> Result<(), MobileError> {
        if self.closed.is_cancelled() || runtime.stop.is_cancelled() {
            Err(MobileError::Shutdown)
        } else {
            Ok(())
        }
    }

    pub(crate) async fn page(
        &self,
        cursor: Option<&CatalogRunCursor>,
        limit: u32,
    ) -> Result<CatalogRunPage, MobileError> {
        let runtime = self.runtime.upgrade().ok_or(MobileError::Shutdown)?;
        self.ensure_open(&runtime)?;
        // Reads have no independently owned effect to settle. Closing a view
        // cancels even pool acquisition and releases the temporary runtime Arc.
        let result = tokio::select! {
            biased;
            () = self.closed.cancelled() => return Err(MobileError::Shutdown),
            () = runtime.stop.cancelled() => return Err(MobileError::Shutdown),
            result = runtime.storage.catalog_run_history(&self.scope, cursor, limit) => result,
        };
        self.ensure_open(&runtime)?;
        result.map_err(Into::into)
    }
}
