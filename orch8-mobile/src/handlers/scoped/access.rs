//! A run view is bound to its original runtime and catalog session.
//! It is not yet an exported SDK object; public lifecycle/listener work remains.
use super::{CatalogHandlers, CatalogScope};
use crate::MobileError;
use orch8_storage::MobileInstanceAccess;
use orch8_types::{
    ids::InstanceId,
    instance::{InstanceState, TaskInstance},
};
use std::sync::{Arc, Weak};
use tokio_util::sync::CancellationToken;

pub(crate) struct CatalogRunAccess {
    runtime: Weak<CatalogHandlers>,
    access: MobileInstanceAccess,
    id: InstanceId,
    closed: CancellationToken,
}
impl CatalogHandlers {
    pub(crate) fn run_access(
        self: &Arc<Self>,
        scope: &CatalogScope,
        id: InstanceId,
    ) -> CatalogRunAccess {
        CatalogRunAccess {
            runtime: Arc::downgrade(self),
            access: MobileInstanceAccess::Catalog {
                account_id: scope.account_id().to_owned(),
                catalog_id: scope.catalog_id().to_owned(),
                session_epoch: scope.epoch(),
            },
            id,
            closed: CancellationToken::new(),
        }
    }
}
impl CatalogRunAccess {
    /// Close only this view. No durable work/account is cancelled or retired.
    pub(crate) fn close(&self) {
        self.closed.cancel();
    }

    fn runtime(&self) -> Result<Arc<CatalogHandlers>, MobileError> {
        let runtime = self.runtime.upgrade().ok_or(MobileError::Shutdown)?;
        self.ensure_open(&runtime)?;
        Ok(runtime)
    }
    fn ensure_open(&self, runtime: &CatalogHandlers) -> Result<(), MobileError> {
        if self.closed.is_cancelled() || runtime.stop.is_cancelled() {
            Err(MobileError::Shutdown)
        } else {
            Ok(())
        }
    }
    pub(crate) async fn snapshot(&self) -> Result<Option<TaskInstance>, MobileError> {
        let runtime = self.runtime()?;
        let result = runtime
            .backend
            .get_mobile_instance(&self.access, self.id)
            .await;
        self.ensure_open(&runtime)?;
        result.map_err(Into::into)
    }
    pub(crate) async fn cancel(&self) -> Result<Option<InstanceState>, MobileError> {
        let runtime = self.runtime()?;
        let result = runtime
            .backend
            .cancel_mobile_instance(&self.access, self.id)
            .await;
        self.ensure_open(&runtime)?;
        result.map_err(Into::into)
    }
}
