//! Account-bound private reads with a fresh device proof and one frozen token.
//! No refresh, ambient credential, provider execution or task-supplied URL.
use super::{MobileError, RemoteTask, read_capped};
use crate::preparation::{Permit, budget};
use crate::storage::{MobileStorage, scopes::peers::CatalogWorkerPeer};
use orch8_types::{
    delegation_definition::{
        DefinitionGraphLimits,
        claim::DelegationClaimContext,
        input::{DelegationInputEnvelope, InputLimits},
        wire::DefinitionWireEnvelope,
    },
    worker_identity::{WorkerIdentityRequest, WorkerIdentityResponse},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;

fn refused() -> MobileError {
    MobileError::Engine {
        detail: "scoped worker identity or payload was not verified".into(),
    }
}
#[allow(dead_code)] // Connected to scoped execution only after native admission is complete.
#[derive(Clone)]
pub(crate) struct ScopedPayloadReader {
    storage: Arc<MobileStorage>,
    peer: CatalogWorkerPeer,
    device: String,
    base: String,
    http: reqwest::Client,
    budget: Arc<Semaphore>,
    reserved: Option<Arc<Permit>>,
    #[cfg(test)]
    verify_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    prepare_hook: Option<Arc<dyn Fn() + Send + Sync>>,
    #[cfg(test)]
    initialize_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}
struct Waiting<T> {
    task: tokio::task::JoinHandle<Result<T, MobileError>>,
    stop: CancellationToken,
}
impl<T> Drop for Waiting<T> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
struct Operation {
    context: DelegationClaimContext,
    token: String,
    stop: CancellationToken,
    deadline: Instant,
}
impl Operation {
    fn live(&self) -> Result<(), MobileError> {
        if self.stop.is_cancelled()
            || Instant::now() >= self.deadline
            || chrono::Utc::now() >= self.context.expires_at
        {
            return Err(refused());
        }
        Ok(())
    }
    async fn io<T>(&self, future: impl std::future::Future<Output = T>) -> Result<T, MobileError> {
        self.live()?;
        tokio::select! {biased; ()=self.stop.cancelled()=>Err(refused()), result=tokio::time::timeout_at(self.deadline,future)=>result.map_err(|_|refused())}
    }
}
#[allow(dead_code)]
impl ScopedPayloadReader {
    pub(crate) fn new(
        storage: Arc<MobileStorage>,
        peer: CatalogWorkerPeer,
        device: String,
    ) -> Result<Arc<Self>, MobileError> {
        if device.trim().is_empty() || device.len() > 256 || device.chars().any(char::is_control) {
            return Err(refused());
        }
        let base = peer.api_base().to_owned();
        Ok(Arc::new(Self {
            storage,
            peer,
            device,
            base,
            http: crate::build_mobile_http_client(Duration::from_secs(30)),
            budget: budget(),
            reserved: None,
            #[cfg(test)]
            verify_hook: None,
            #[cfg(test)]
            prepare_hook: None,
            #[cfg(test)]
            initialize_hook: None,
        }))
    }
    pub(crate) async fn input(
        self: &Arc<Self>,
        task: &RemoteTask,
        token: &str,
        stop: CancellationToken,
    ) -> Result<DelegationInputEnvelope, MobileError> {
        task.validate_delegation(&self.peer.runtime_id().to_string())
            .map_err(|_| refused())?;
        self.input_context(task.delegation.clone().ok_or_else(refused)?, token, stop)
            .await
    }
    async fn input_context(
        self: &Arc<Self>,
        context: DelegationClaimContext,
        token: &str,
        stop: CancellationToken,
    ) -> Result<DelegationInputEnvelope, MobileError> {
        self.read(context, token, stop, "input", |context, bytes| {
            let envelope: DelegationInputEnvelope =
                serde_json::from_slice(&bytes).map_err(|_| refused())?;
            envelope
                .verify(
                    &context.scope,
                    &context.request_sha256,
                    &context.input,
                    InputLimits::default(),
                )
                .map_err(|_| refused())?;
            // Preserve the exact downloaded bytes for immutable local retention.
            Ok(envelope)
        })
        .await
    }
    pub(crate) async fn definition(
        self: &Arc<Self>,
        task: &RemoteTask,
        token: &str,
        stop: CancellationToken,
    ) -> Result<DefinitionWireEnvelope, MobileError> {
        task.validate_delegation(&self.peer.runtime_id().to_string())
            .map_err(|_| refused())?;
        self.definition_context(task.delegation.clone().ok_or_else(refused)?, token, stop)
            .await
    }
    async fn definition_context(
        self: &Arc<Self>,
        context: DelegationClaimContext,
        token: &str,
        stop: CancellationToken,
    ) -> Result<DefinitionWireEnvelope, MobileError> {
        self.read(context, token, stop, "definition", |context, bytes| {
            let envelope: DefinitionWireEnvelope =
                serde_json::from_slice(&bytes).map_err(|_| refused())?;
            envelope
                .verify(
                    &context.scope,
                    &context.definition,
                    DefinitionGraphLimits::default(),
                )
                .map_err(|_| refused())?;
            Ok(envelope)
        })
        .await
    }
    async fn active(&self, operation: &Operation) -> Result<(), MobileError> {
        operation.live()?;
        self.storage
            .check_worker_peer_device(&self.peer, &self.device)
            .await?;
        operation.live()
    }
    async fn proof(&self, operation: &mut Operation) -> Result<(), MobileError> {
        self.active(operation).await?;
        let challenge = WorkerIdentityRequest {
            nonce: uuid::Uuid::new_v4().to_string(),
        };
        let response = operation
            .io(self
                .http
                .post(format!("{}workers/identity", self.base))
                .header("x-api-key", &operation.token)
                .header("cache-control", "no-store")
                .json(&challenge)
                .send())
            .await?
            .map_err(|_| refused())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(refused());
        }
        let bytes = operation
            .io(orch8_engine::handlers::builtin::read_body_capped(
                response, 4096,
            ))
            .await?
            .map_err(|_| refused())?;
        let response: WorkerIdentityResponse =
            serde_json::from_slice(&bytes).map_err(|_| refused())?;
        let now = chrono::Utc::now();
        response
            .validate_for(
                &challenge.nonce,
                self.peer.remote_tenant(),
                self.peer.runtime_id(),
                &self.device,
                now,
            )
            .map_err(|_| refused())?;
        let remaining = (response.expires_at - now)
            .to_std()
            .map_err(|_| refused())?;
        operation.deadline = operation.deadline.min(Instant::now() + remaining);
        self.active(operation).await?;
        self.storage
            .bind_worker_peer_device(&self.peer, &self.device)
            .await?;
        self.active(operation).await
    }
    async fn read<T: Send + 'static>(
        self: &Arc<Self>,
        context: DelegationClaimContext,
        token: &str,
        stop: CancellationToken,
        resource: &'static str,
        verify: impl FnOnce(DelegationClaimContext, Vec<u8>) -> Result<T, MobileError> + Send + 'static,
    ) -> Result<T, MobileError> {
        self.peer
            .validate_claim_identity(&context)
            .map_err(|_| refused())?;
        if !token.starts_with(crate::credential::DEVICE_SESSION_PREFIX) || token.len() > 1024 * 1024
        {
            return Err(refused());
        }
        let permit = match &self.reserved {
            Some(permit) if permit.belongs_to(&self.budget) => permit.clone(),
            Some(_) => return Err(refused()),
            None => Permit::acquire(&self.budget)?,
        };
        let deadline = Instant::now() + Duration::from_secs(30);
        let owner = self.clone();
        let child = stop.child_token();
        let operation = Operation {
            context,
            token: token.to_owned(),
            stop: child.clone(),
            deadline,
        };
        let task =
            tokio::spawn(async move { owner.owned(operation, resource, verify, permit).await });
        let mut wait = Waiting { task, stop: child };
        tokio::select! {biased; ()=stop.cancelled()=>Err(refused()), result=tokio::time::timeout_at(deadline,&mut wait.task)=>result.map_err(|_|refused())?.map_err(|_|refused())?}
    }
    async fn owned<T: Send + 'static>(
        &self,
        mut operation: Operation,
        resource: &'static str,
        verify: impl FnOnce(DelegationClaimContext, Vec<u8>) -> Result<T, MobileError> + Send + 'static,
        permit: Arc<Permit>,
    ) -> Result<T, MobileError> {
        self.proof(&mut operation).await?;
        let scope = &operation.context.scope;
        let url = format!(
            "{}workers/tasks/{}/{resource}?worker_id={}&claim_epoch={}",
            self.base, scope.task_id, scope.worker_id, scope.claim_epoch
        );
        self.active(&operation).await?;
        let response = operation
            .io(self
                .http
                .get(url)
                .header("x-api-key", &operation.token)
                .header("x-device-id", &self.device)
                .header("cache-control", "no-store")
                .send())
            .await?
            .map_err(|_| refused())?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(refused());
        }
        let bytes = operation.io(read_capped(response)).await??;
        self.active(&operation).await?;
        let context = operation.context.clone();
        let cancelled = operation.stop.clone();
        #[cfg(test)]
        let hook = self.verify_hook.clone();
        let (permit, result) = tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(hook) = hook {
                hook();
            }
            let result = if cancelled.is_cancelled() {
                Err(refused())
            } else {
                verify(context, bytes)
            };
            (permit, result)
        })
        .await
        .map_err(|_| refused())?;
        let _permit = permit;
        let result = result?;
        self.active(&operation).await?;
        Ok(result)
    }
}
#[cfg(test)]
mod tests;

mod preparation;

#[allow(dead_code)] // Scoped native dispatch remains gated until its full authority path is ready.
pub(crate) mod claim_lease;
