use super::{
    MobileCatalogCallbackContext, MobileCatalogHistoryCursor, MobileCatalogNativeState,
    MobileCatalogRequestState, MobileCatalogRun, MobileCatalogRunPage, MobileCatalogRunRequest,
    MobileCatalogRunSummary, MobileCatalogStepHandler, View,
};
use crate::{HandlerError, MobileEngine, MobileError};
use crate::{
    SyncResult, TokenProvider,
    handlers::scoped::context::{CatalogCallbackContext, CatalogStepHandler},
    storage::scopes::{
        CatalogScope,
        runs::{CatalogRunLimits, CatalogRunRequest, CatalogRunState, history::CatalogNativeState},
    },
    sync::catalog::{CatalogSyncAdmission, CatalogSynchronizer},
};
use std::sync::Arc;
use std::time::Duration;

#[derive(uniffi::Object)]
pub struct MobileCatalog {
    pub(super) view: View,
    pub(super) scope: CatalogScope,
    #[cfg(test)]
    pub(super) test_synchronizer: Option<Arc<CatalogSynchronizer>>,
}

struct Handler(Arc<dyn MobileCatalogStepHandler>);

/// The native-owned host reference drops before admission. Neither cancelling
/// sync nor a blocked foreign destructor may admit another call for this owner.
struct ScopedTokenProvider {
    provider: Arc<dyn TokenProvider>,
    _admission: Arc<CatalogSyncAdmission>,
}
impl TokenProvider for ScopedTokenProvider {
    fn current_token(&self) -> String {
        self.provider.current_token()
    }
    fn refresh_token(&self) -> Result<String, MobileError> {
        self.provider.refresh_token()
    }
}
impl CatalogStepHandler for Handler {
    fn execute(
        &self,
        context: CatalogCallbackContext,
        step_name: String,
        input: String,
    ) -> Result<String, HandlerError> {
        self.0.execute(
            MobileCatalogCallbackContext {
                account_id: context.account_id().into(),
                catalog_id: context.catalog_id().into(),
                owner_epoch: context.owner_epoch(),
                run_id: context.run_id().to_string(),
                block_id: context.block_id().to_string(),
                attempt: context.attempt(),
                effect_id: context.effect_id().to_string(),
            },
            step_name,
            input,
        )
    }
}

fn limits() -> CatalogRunLimits {
    CatalogRunLimits {
        global_records: 4096,
        account_records: 1024,
        catalog_records: 256,
        global_input_bytes: 64 * 1024 * 1024,
        account_input_bytes: 16 * 1024 * 1024,
        catalog_input_bytes: 4 * 1024 * 1024,
    }
}

#[uniffi::export]
impl MobileCatalog {
    pub fn account_id(&self) -> String {
        self.scope.account_id().into()
    }
    pub fn catalog_id(&self) -> String {
        self.scope.catalog_id().into()
    }
    pub fn session_epoch(&self) -> i64 {
        self.scope.epoch()
    }
    pub fn close_view(&self) {
        self.view.closed.cancel();
    }

    /// Signed sync authenticates this immutable principal/catalog before private
    /// content requests. The provider belongs to this operation, not an ambient
    /// current account, and shares bounded physical token-call admission.
    pub fn sync(&self, token_provider: Arc<dyn TokenProvider>) -> Result<SyncResult, MobileError> {
        let engine = self.view.engine()?;
        self.view.run(&engine, async {
            let catalog = self.synchronizer(&engine).await?;
            let mut handlers = engine
                .catalog_handlers
                .names(&self.scope)
                .await
                .map_err(|_| MobileError::Engine {
                    message: "catalog handlers unavailable".into(),
                })?;
            handlers.extend(
                engine
                    .enabled_builtins
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .iter()
                    .map(|name| (*name).to_owned()),
            );
            let credential = engine.credential.clone();
            engine
                .catalog_sync
                .sync_with_admission(
                    catalog,
                    handlers,
                    self.view.closed.child_token(),
                    move |cancelled, admission| async move {
                        credential
                            .current_manifest_token(
                                Arc::new(ScopedTokenProvider {
                                    provider: token_provider,
                                    _admission: admission,
                                }),
                                &cancelled,
                            )
                            .await
                    },
                )
                .await
        })
    }

    pub fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn MobileCatalogStepHandler>,
        timeout_ms: u64,
    ) -> Result<(), MobileError> {
        let engine = self.view.engine()?;
        self.view.run(&engine, async {
            engine
                .catalog_handlers
                .bind_contextual(
                    &self.scope,
                    &name,
                    Arc::new(Handler(handler)),
                    Duration::from_millis(timeout_ms),
                )
                .await
                .map_err(|error| match error {
                    orch8_types::error::StepError::Permanent { .. } => MobileError::InvalidInput {
                        message: "invalid catalog handler binding".into(),
                    },
                    orch8_types::error::StepError::ClaimLost => MobileError::InvalidInput {
                        message: "catalog session is unavailable".into(),
                    },
                    orch8_types::error::StepError::Retryable { .. } => MobileError::ResourceLimit {
                        message: "catalog handler binding unavailable; retry after it settles"
                            .into(),
                    },
                })
        })
    }

    /// Persist and return the durable request before dispatch. Retry with the
    /// same operation key after a timeout; changing its request is refused.
    pub fn prepare_run(
        &self,
        request: MobileCatalogRunRequest,
    ) -> Result<Arc<MobileCatalogRun>, MobileError> {
        let engine = self.view.engine()?;
        let id = self.view.run(&engine, async {
            Ok(engine
                .lifecycle
                .mobile_storage()
                .prepare_catalog_run(
                    &self.scope,
                    CatalogRunRequest {
                        workflow: &request.workflow,
                        version: request.version,
                        input_json: &request.input_json,
                        dedup_key: Some(&request.operation_key),
                    },
                    limits(),
                )
                .await?
                .id)
        })?;
        Ok(self.run_view(id))
    }

    /// Create a view, not authority. Every subsequent operation checks the
    /// durable catalog/session and native identity before returning private data.
    pub fn run(&self, instance_id: String) -> Result<Arc<MobileCatalogRun>, MobileError> {
        self.view.engine()?;
        let id = uuid::Uuid::parse_str(&instance_id)
            .map(orch8_types::ids::InstanceId::from_uuid)
            .map_err(|_| MobileError::InvalidInput {
                message: "invalid run ID".into(),
            })?;
        Ok(self.run_view(id))
    }

    /// Live metadata-only keyset pages; page size is 1..128. A cursor cannot be
    /// reused in another account, catalog or reopened session.
    pub fn history(
        &self,
        cursor: Option<Arc<MobileCatalogHistoryCursor>>,
        limit: u32,
    ) -> Result<MobileCatalogRunPage, MobileError> {
        let engine = self.view.engine()?;
        let history = engine.catalog_handlers.run_history(&self.scope);
        let page = self.view.run(
            &engine,
            history.page(cursor.as_ref().map(|c| &c.cursor), limit),
        )?;
        Ok(MobileCatalogRunPage {
            items: page
                .items
                .into_iter()
                .map(|item| MobileCatalogRunSummary {
                    instance_id: item.id.to_string(),
                    account_id: item.account_id,
                    catalog_id: item.catalog_id,
                    owner_epoch: item.owner_epoch,
                    workflow: item.workflow,
                    version: item.version,
                    sequence_id: item.sequence_id.to_string(),
                    created_at: item.created_at.to_rfc3339(),
                    request_state: match item.request_state {
                        CatalogRunState::Prepared => MobileCatalogRequestState::Prepared,
                        CatalogRunState::Cancelled => MobileCatalogRequestState::Cancelled,
                        CatalogRunState::Archived => MobileCatalogRequestState::Archived,
                    },
                    native_state: match item.native_state {
                        CatalogNativeState::NotMaterialized => {
                            MobileCatalogNativeState::NotMaterialized
                        }
                        CatalogNativeState::Present(state) => MobileCatalogNativeState::Present {
                            state: state.into(),
                        },
                        CatalogNativeState::Recorded(state) => MobileCatalogNativeState::Recorded {
                            state: state.into(),
                        },
                        CatalogNativeState::Unavailable => MobileCatalogNativeState::Unavailable,
                    },
                })
                .collect(),
            next: page
                .next
                .map(|cursor| Arc::new(MobileCatalogHistoryCursor { cursor })),
        })
    }
}

impl MobileCatalog {
    async fn synchronizer(
        &self,
        engine: &MobileEngine,
    ) -> Result<Arc<CatalogSynchronizer>, MobileError> {
        #[cfg(test)]
        if let Some(synchronizer) = &self.test_synchronizer {
            return Ok(synchronizer.clone());
        }
        CatalogSynchronizer::new(
            engine.catalog_handlers.mobile_storage(),
            engine.storage.clone(),
            self.scope.clone(),
            engine.config.sdk_version.clone(),
        )
        .await
    }
    fn run_view(&self, id: orch8_types::ids::InstanceId) -> Arc<MobileCatalogRun> {
        Arc::new(MobileCatalogRun {
            view: self.view.child(),
            scope: self.scope.clone(),
            id,
        })
    }
}
