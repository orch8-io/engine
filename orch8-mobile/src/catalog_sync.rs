//! Private catalog sync with immutable owners and a fresh authenticated identity
//! response before content requests. Public SDK/runtime integration is separate.
use super::{RootKey, ScopedSyncAuth, SyncOrchestrator, SyncResult, signature_url};
use crate::{
    MobileError,
    error::SyncError,
    storage::{
        MobileStorage,
        scopes::{AcceptedCatalogManifest, CatalogBinding, CatalogScope, CatalogTransport},
    },
};
use orch8_types::catalog::{CatalogIdentityRequest, CatalogIdentityResponse};
use std::{
    collections::{HashMap, HashSet},
    future::Future,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

fn refused() -> MobileError {
    SyncError::InvalidManifest {
        message: "catalog identity was not verified".into(),
    }
    .into()
}
fn busy() -> MobileError {
    MobileError::ResourceLimit {
        detail: "catalog sync capacity is occupied; retry after it settles".into(),
    }
}

enum EntryError {
    Abort(MobileError),
    Fetch,
    Integrity,
}
fn fetch_error(error: MobileError) -> EntryError {
    if matches!(&error, MobileError::Shutdown { .. }) {
        EntryError::Abort(error)
    } else {
        EntryError::Fetch
    }
}

/// One per engine, shared by every account and catalog. Account keys omit the
/// session epoch so retirement/reopen cannot bypass an unsettled run's slot.
pub(crate) struct CatalogSyncCoordinator {
    global: Arc<Semaphore>,
    accounts: Mutex<HashMap<String, Weak<Semaphore>>>,
}

/// Shared by the logical sync and its physically owned foreign token call.
/// A cancelled observer cannot free an account/global slot while host code or
/// its native-owned foreign reference is still alive. Contains no runtime.
pub(crate) struct CatalogSyncAdmission {
    _account: tokio::sync::OwnedSemaphorePermit,
    _global: tokio::sync::OwnedSemaphorePermit,
}

struct Wait {
    cancelled: CancellationToken,
    task: tokio::task::JoinHandle<Result<SyncResult, MobileError>>,
}
impl Drop for Wait {
    fn drop(&mut self) {
        self.cancelled.cancel();
    }
}

impl CatalogSyncCoordinator {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            global: Arc::new(Semaphore::new(2)),
            accounts: Mutex::new(HashMap::new()),
        })
    }

    fn account(&self, id: &str) -> Result<Arc<Semaphore>, MobileError> {
        let mut accounts = self
            .accounts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        accounts.retain(|_, gate| gate.strong_count() != 0);
        if let Some(gate) = accounts.get(id).and_then(Weak::upgrade) {
            return Ok(gate);
        }
        if accounts.len() >= 1024 {
            return Err(busy());
        }
        let gate = Arc::new(Semaphore::new(1));
        accounts.insert(id.into(), Arc::downgrade(&gate));
        Ok(gate)
    }

    #[cfg(test)]
    pub(crate) async fn sync<A, F>(
        &self,
        catalog: Arc<CatalogSynchronizer>,
        handlers: HashSet<String>,
        cancelled: CancellationToken,
        authorize: A,
    ) -> Result<SyncResult, MobileError>
    where
        A: FnOnce(CancellationToken) -> F + Send + 'static,
        F: Future<Output = Result<String, MobileError>> + Send + 'static,
    {
        self.sync_with_admission(
            catalog,
            handlers,
            cancelled,
            move |cancelled, _admission| authorize(cancelled),
        )
        .await
    }

    pub(crate) async fn sync_with_admission<A, F>(
        &self,
        catalog: Arc<CatalogSynchronizer>,
        handlers: HashSet<String>,
        cancelled: CancellationToken,
        authorize: A,
    ) -> Result<SyncResult, MobileError>
    where
        A: FnOnce(CancellationToken, Arc<CatalogSyncAdmission>) -> F + Send + 'static,
        F: Future<Output = Result<String, MobileError>> + Send + 'static,
    {
        let account = self
            .account(catalog.scope.account_id())?
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let global = Arc::clone(&self.global)
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let mut operation = catalog.transport.clone();
        operation.run_cancelled = Some(cancelled.clone());
        let task_cancelled = cancelled.clone();
        let admission = Arc::new(CatalogSyncAdmission {
            _account: account,
            _global: global,
        });
        let task = tokio::spawn(async move {
            // A cancelled caller never drops admission ahead of dispatched SQL.
            catalog.active(&operation, None).await?;
            let token = zeroize::Zeroizing::new(
                operation
                    .io(authorize(task_cancelled, admission.clone()))
                    .await??,
            );
            if token.trim().is_empty() {
                return Err(refused());
            }
            catalog.run(&operation, &token, &handlers).await
        });
        let mut wait = Wait { cancelled, task };
        (&mut wait.task).await.map_err(|_| MobileError::Engine {
            detail: "catalog sync task failed".into(),
        })?
    }
}

pub(crate) struct CatalogSynchronizer {
    storage: Arc<MobileStorage>,
    scope: CatalogScope,
    binding: CatalogBinding,
    endpoints: CatalogTransport,
    transport: SyncOrchestrator,
}

impl CatalogSynchronizer {
    async fn download_entry(
        &self,
        transport: &SyncOrchestrator,
        auth: &ScopedSyncAuth<'_>,
        entry_url: &str,
        expires: chrono::DateTime<chrono::Utc>,
    ) -> Result<(String, ed25519_dalek::Signature), EntryError> {
        let url = transport
            .resolve_entry_url(&self.endpoints.manifest_url, entry_url)
            .map_err(|_| EntryError::Fetch)?;
        let body = transport
            .download_sequence(url.as_str(), auth)
            .await
            .map_err(fetch_error)?;
        self.active(transport, Some(expires))
            .await
            .map_err(EntryError::Abort)?;
        let signature = transport
            .download_sequence(signature_url(&url).as_str(), auth)
            .await
            .map_err(fetch_error)?;
        let signature = super::decode_signature(&signature).map_err(|_| EntryError::Integrity)?;
        Ok((body, signature))
    }
    pub(crate) async fn new(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn orch8_storage::StorageBackend>,
        scope: CatalogScope,
        sdk: String,
    ) -> Result<Arc<Self>, MobileError> {
        Self::build(storage, backend, scope, sdk, false).await
    }

    async fn build(
        storage: Arc<MobileStorage>,
        backend: Arc<dyn orch8_storage::StorageBackend>,
        scope: CatalogScope,
        sdk: String,
        insecure: bool,
    ) -> Result<Arc<Self>, MobileError> {
        let binding = storage.catalog_binding(&scope).await?;
        let endpoints = storage.catalog_transport(&scope).await?;
        if !insecure {
            endpoints.validate()?;
        }
        let root = RootKey::from_base64(
            binding
                .authority
                .strip_prefix("ed25519:")
                .ok_or_else(refused)?,
        )?;
        let mut transport = SyncOrchestrator::new(Arc::clone(&storage), backend, root, sdk, 0);
        transport.allow_insecure_urls = insecure;
        Ok(Arc::new(Self {
            storage,
            scope,
            binding,
            endpoints,
            transport,
        }))
    }

    async fn active(
        &self,
        transport: &SyncOrchestrator,
        expires: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), MobileError> {
        transport.check_active()?;
        if expires.is_some_and(|expires| chrono::Utc::now() >= expires) {
            return Err(refused());
        }
        self.storage.catalog_binding(&self.scope).await?;
        transport.check_active()
    }

    async fn identity(
        &self,
        transport: &SyncOrchestrator,
        token: &str,
    ) -> Result<chrono::DateTime<chrono::Utc>, MobileError> {
        self.active(transport, None).await?;
        let challenge = CatalogIdentityRequest {
            nonce: uuid::Uuid::new_v4().to_string(),
            source: self.binding.audience.source.clone(),
            catalog: self.binding.audience.catalog.clone(),
        };
        let response = transport
            .io(transport
                .http_client()
                .post(&self.endpoints.identity_url)
                .header(reqwest::header::CACHE_CONTROL, "no-store")
                .header(reqwest::header::ACCEPT, "application/json")
                .bearer_auth(token)
                .json(&challenge)
                .send())
            .await?
            .map_err(|_| refused())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(refused());
        }
        let bytes = transport
            .io(orch8_engine::handlers::builtin::read_body_capped(
                response,
                16 * 1024,
            ))
            .await?
            .map_err(|_| refused())?;
        let response: CatalogIdentityResponse =
            serde_json::from_slice(&bytes).map_err(|_| refused())?;
        let now = chrono::Utc::now();
        if response.nonce != challenge.nonce
            || response.audience != self.binding.audience
            || response.issued_at > now + chrono::Duration::seconds(30)
            || response.expires_at <= now
            || response.expires_at <= response.issued_at
            || response.expires_at - response.issued_at > chrono::Duration::minutes(5)
        {
            return Err(refused());
        }
        self.active(transport, Some(response.expires_at)).await?;
        Ok(response.expires_at)
    }

    async fn reconcile(
        &self,
        transport: &SyncOrchestrator,
        accepted: &AcceptedCatalogManifest,
        expires: chrono::DateTime<chrono::Utc>,
    ) -> Result<u32, MobileError> {
        let manifest = accepted.manifest();
        let mut removed = 0;
        let names: HashSet<_> = manifest
            .sequences
            .iter()
            .map(|entry| entry.name.as_str())
            .collect();
        // v1 catalogs are complete snapshots. Page names, never bodies/history.
        let mut after = String::new();
        loop {
            self.active(transport, Some(expires)).await?;
            let page = self
                .storage
                .catalog_cached_page(&self.scope, &after, 100)
                .await?;
            let Some((last_name, _)) = page.last() else {
                break;
            };
            after = last_name.clone();
            for (name, _) in &page {
                if !names.contains(name.as_str()) {
                    self.active(transport, Some(expires)).await?;
                    self.storage
                        .remove_catalog_workflow(
                            &self.scope,
                            name,
                            manifest.manifest_version,
                            accepted.digest(),
                        )
                        .await?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    async fn install_entries(
        &self,
        transport: &SyncOrchestrator,
        auth: &ScopedSyncAuth<'_>,
        accepted: &AcceptedCatalogManifest,
        handlers: &HashSet<String>,
        expires: chrono::DateTime<chrono::Utc>,
    ) -> Result<(SyncResult, bool), MobileError> {
        let manifest = accepted.manifest();
        let mut result = SyncResult::default();
        let mut pending = false;
        for entry in &manifest.sequences {
            self.active(transport, Some(expires)).await?;
            if !transport.version_meets_min(&transport.sdk_version, &entry.min_sdk_version)
                || entry
                    .required_handlers
                    .iter()
                    .any(|handler| !handlers.contains(handler))
            {
                result.skipped += 1;
                continue;
            }
            let current = self
                .storage
                .catalog_cached_version(&self.scope, &entry.name)
                .await?;
            if current.is_some_and(|version| version >= entry.version) {
                continue;
            }
            let (body, signature) = match self
                .download_entry(transport, auth, &entry.url, expires)
                .await
            {
                Ok(entry) => entry,
                Err(EntryError::Abort(error)) => return Err(error),
                Err(EntryError::Fetch) => {
                    result.skipped += 1;
                    pending = true;
                    continue;
                }
                Err(EntryError::Integrity) => {
                    result.signature_failures += 1;
                    pending = true;
                    continue;
                }
            };
            self.active(transport, Some(expires)).await?;
            if let Err(error) = accepted
                .install_workflow(&entry.name, &body, &signature.to_bytes())
                .await
            {
                if matches!(
                    &error,
                    MobileError::Storage { .. } | MobileError::Shutdown { .. }
                ) {
                    return Err(error);
                }
                result.signature_failures += 1;
                pending = true;
                continue;
            }
            if current.is_some() {
                result.updated += 1;
            } else {
                result.added += 1;
            }
        }
        Ok((result, pending))
    }

    async fn run(
        &self,
        transport: &SyncOrchestrator,
        token: &str,
        handlers: &HashSet<String>,
    ) -> Result<SyncResult, MobileError> {
        let expires = self.identity(transport, token).await?;
        // Network retries and sleeps share the proof's monotonic lifetime.
        // SQL writes deliberately stay outside io() so they settle under their
        // epoch fence before the owned task releases physical sync admission.
        let remaining = (expires - chrono::Utc::now())
            .to_std()
            .map_err(|_| refused())?;
        let mut bounded = transport.clone();
        bounded.run_deadline = Some(tokio::time::Instant::now() + remaining);
        let transport = &bounded;
        let auth = ScopedSyncAuth::Bearer {
            token,
            manifest_url: reqwest::Url::parse(&self.endpoints.manifest_url)
                .map_err(|_| refused())?,
        };
        let fingerprint = transport.env_fingerprint(handlers);
        let env_changed = self
            .storage
            .catalog_metadata(&self.scope, super::ENV_FINGERPRINT_KEY)
            .await?
            .as_deref()
            != Some(fingerprint.as_str());
        let trust = self.storage.catalog_manifest_trust(&self.scope).await?;
        let etag = if !env_changed && !trust.pending() && trust.applied_version.is_some() {
            self.storage
                .catalog_metadata(&self.scope, "manifest_etag")
                .await?
        } else {
            None
        };
        self.active(transport, Some(expires)).await?;
        let response = transport
            .http_get_with_retry(&self.endpoints.manifest_url, &auth, etag.as_deref())
            .await?;
        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            if etag.is_none() {
                return Err(refused());
            }
            self.active(transport, Some(expires)).await?;
            return Ok(SyncResult::default());
        }
        if response.status() != reqwest::StatusCode::OK {
            return Err(refused());
        }
        let etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let bytes = transport
            .io(super::read_body_capped(response, "catalog manifest"))
            .await??;
        self.active(transport, Some(expires)).await?;
        let accepted = self
            .storage
            .accept_signed_catalog_manifest(&self.scope, &bytes)
            .await?;
        let manifest = accepted.manifest();
        if trust.applied_version == Some(manifest.manifest_version) && !env_changed {
            return Ok(SyncResult::default());
        }
        let removed = self.reconcile(transport, &accepted, expires).await?;
        self.active(transport, Some(expires)).await?;
        self.storage
            .mark_catalog_reconciled(&self.scope, manifest.manifest_version, accepted.digest())
            .await?;
        let (mut result, pending) = self
            .install_entries(transport, &auth, &accepted, handlers, expires)
            .await?;
        result.removed = removed;
        self.active(transport, Some(expires)).await?;
        if !pending {
            self.storage
                .commit_catalog_evaluation(
                    &self.scope,
                    manifest.manifest_version,
                    accepted.digest(),
                    etag.as_deref(),
                    &fingerprint,
                )
                .await?;
        }
        self.active(transport, Some(expires)).await?;
        Ok(result)
    }
}

#[cfg(test)]
#[path = "catalog_sync_tests.rs"]
mod tests;
