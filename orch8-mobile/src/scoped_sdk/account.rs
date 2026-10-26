use super::{MobileAccountIdentity, MobileCatalog, MobileCatalogConfiguration, View};
use crate::{
    MobileEngine, MobileError, RootKey,
    storage::scopes::{AccountScope, CatalogTransport},
};
use std::sync::Arc;

#[derive(uniffi::Object)]
pub struct MobileAccount {
    pub(super) view: View,
    pub(super) scope: AccountScope,
}

#[uniffi::export]
impl MobileEngine {
    /// Open an explicit trusted host identity. Reusing a key for another
    /// principal is refused. A token is independently verified by catalog sync.
    pub fn open_account(
        self: Arc<Self>,
        identity: MobileAccountIdentity,
    ) -> Result<Arc<MobileAccount>, MobileError> {
        let scope = self.run_with_timeout(async {
            Ok(self
                .lifecycle
                .mobile_storage()
                .open_account_scope(&identity.account_key, &identity.issuer, &identity.subject)
                .await?)
        })?;
        Ok(Arc::new(MobileAccount {
            view: View::root(&self),
            scope,
        }))
    }
}

#[uniffi::export]
impl MobileAccount {
    pub fn account_id(&self) -> String {
        self.scope.account_id().to_owned()
    }
    pub fn session_epoch(&self) -> i64 {
        self.scope.epoch()
    }

    /// Close this local view and its child views. Durable runs and registered
    /// handlers continue under their original owner until explicitly retired.
    pub fn close_view(&self) {
        self.view.closed.cancel();
    }

    pub fn open_catalog(
        &self,
        configuration: MobileCatalogConfiguration,
    ) -> Result<Arc<MobileCatalog>, MobileError> {
        let engine = self.view.engine()?;
        let transport = CatalogTransport {
            manifest_url: configuration.manifest_url,
            identity_url: configuration.identity_url,
        };
        transport.validate()?;
        let key = configuration
            .signing_authority
            .strip_prefix("ed25519:")
            .ok_or_else(|| MobileError::InvalidInput {
                detail: "catalog requires an ed25519 signing authority".into(),
            })?;
        RootKey::from_base64(key)?;
        let scope = self.view.run(&engine, async {
            let storage = engine.lifecycle.mobile_storage();
            let scope = storage
                .open_catalog_scope(
                    &self.scope,
                    &configuration.catalog_key,
                    &configuration.source,
                    &configuration.signing_authority,
                )
                .await?;
            storage.bind_catalog_transport(&scope, &transport).await?;
            Ok(scope)
        })?;
        Ok(Arc::new(MobileCatalog {
            view: self.view.child(),
            scope,
            #[cfg(test)]
            test_synchronizer: None,
        }))
    }

    /// Retire this exact durable session and detach its registered callbacks.
    /// Accepted retirement settles independently of this observer. Physical
    /// host calls may finish later and retain their capacity until destruction.
    pub fn retire(&self) -> Result<(), MobileError> {
        let engine = self.view.engine()?;
        let owner = engine.clone();
        let scope = self.scope.clone();
        let result = self.view.run(&engine, async move {
            tokio::spawn(async move {
                owner
                    .lifecycle
                    .mobile_storage()
                    .retire_account_scope(&scope)
                    .await?;
                owner
                    .catalog_handlers
                    .close_account_after_retirement(scope.account_id(), scope.epoch());
                owner.tick_controller.wake();
                Ok::<_, MobileError>(())
            })
            .await
            .map_err(|_| MobileError::Engine {
                detail: "account retirement task failed".into(),
            })?
        });
        if result.is_ok() {
            self.close_view();
        }
        result
    }
}
