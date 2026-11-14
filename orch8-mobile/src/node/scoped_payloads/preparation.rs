//! Own complete cached/downloaded preparation on one capacity-bound dedicated thread.
//! No provider invocation, scheduler admission or lease confirmation occurs here.
use super::{
    Arc, CancellationToken, Duration, Instant, MobileError, Operation, Permit, RemoteTask,
    ScopedPayloadReader, refused,
};
use crate::storage::scopes::{BoundClaimRun, runs::materialize::InstanceLimits};
use orch8_types::instance::TaskInstance;
use orch8_types::{delegation_definition::input::VerifiedDelegationInput, error::StorageError};

#[allow(dead_code)]
pub(crate) struct PreparedClaim {
    pub(crate) run: BoundClaimRun,
    pub(crate) input: VerifiedDelegationInput,
}
impl std::fmt::Debug for PreparedClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedClaim").finish_non_exhaustive()
    }
}
/// Paused native state only; neither a local nor remote execution grant.
#[allow(dead_code)]
pub(crate) struct InitializedClaim {
    pub(crate) run: BoundClaimRun,
    pub(crate) instance: TaskInstance,
}
impl std::fmt::Debug for InitializedClaim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InitializedClaim").finish_non_exhaustive()
    }
}
struct Preparing<T> {
    result: tokio::sync::oneshot::Receiver<Result<T, MobileError>>,
    stop: CancellationToken,
}
impl<T> Drop for Preparing<T> {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}
#[allow(dead_code)] // Staged until scoped native admission is complete.
impl ScopedPayloadReader {
    pub(crate) async fn prepare(
        self: &Arc<Self>,
        task: &RemoteTask,
        token: &str,
        stop: CancellationToken,
    ) -> Result<PreparedClaim, MobileError> {
        self.prepare_with(task, token, stop, |owner, mut operation| async move {
            owner.prepare_owned(&mut operation).await
        })
        .await
    }
    pub(crate) async fn initialize(
        self: &Arc<Self>,
        task: &RemoteTask,
        token: &str,
        stop: CancellationToken,
        limits: InstanceLimits,
    ) -> Result<InitializedClaim, MobileError> {
        self.prepare_with(task, token, stop, move |owner, mut operation| async move {
            let PreparedClaim { run, input } = owner.prepare_owned(&mut operation).await?;
            // Release decoded cache data before initialization reads it in its
            // writer-fenced snapshot. The physical preparation slot stays held.
            drop(input);
            #[cfg(test)]
            if let Some(hook) = &owner.initialize_hook {
                hook();
            }
            let instance = owner
                .stored(
                    &operation,
                    owner.storage.initialize_remote_instance_for_run(
                        &owner.peer,
                        &operation.context,
                        &run,
                        limits,
                        &operation.stop,
                    ),
                )
                .await?;
            if instance.id != run.local_run_id || instance.sequence_id != run.definitions.root_id {
                return Err(refused());
            }
            Ok(InitializedClaim { run, instance })
        })
        .await
    }
    async fn prepare_with<T, F, Fut>(
        self: &Arc<Self>,
        task: &RemoteTask,
        token: &str,
        stop: CancellationToken,
        work: F,
    ) -> Result<T, MobileError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<Self>, Operation) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = Result<T, MobileError>> + 'static,
    {
        task.validate_delegation(&self.peer.runtime_id().to_string())
            .map_err(|_| refused())?;
        let context = task.delegation.clone().ok_or_else(refused)?;
        self.peer
            .validate_claim_identity(&context)
            .map_err(|_| refused())?;
        if !token.starts_with(crate::credential::DEVICE_SESSION_PREFIX) || token.len() > 1024 * 1024
        {
            return Err(refused());
        }
        let permit = Permit::acquire(&self.budget)?;
        let mut reader = (**self).clone();
        reader.reserved = Some(permit);
        let owner = Arc::new(reader);
        let token = token.to_owned();
        let child = stop.child_token();
        let deadline = Instant::now() + Duration::from_secs(90);
        let operation = Operation {
            context,
            token,
            stop: child.clone(),
            deadline,
        };
        let runtime = tokio::runtime::Handle::current();
        // All cache decoding/fingerprinting stays off runtime executor threads.
        // Detaching the waiter leaves this owner/permit with physical work.
        let (sender, result) = tokio::sync::oneshot::channel();
        // Do not occupy Tokio's blocking pool while awaiting reader verification
        // or DNS/filesystem jobs from that same pool (which may have size one).
        std::thread::Builder::new()
            .name("orch8-prepare".into())
            .spawn(move || {
                let result = runtime.block_on(work(owner.clone(), operation));
                // An abandoned result is destroyed while physical capacity is held.
                if let Err(result) = sender.send(result) {
                    drop(result);
                }
                drop(owner);
            })
            .map_err(|_| refused())?;
        let mut waiting = Preparing {
            result,
            stop: child,
        };
        tokio::select! {biased; ()=stop.cancelled()=>Err(refused()), result=tokio::time::timeout_at(deadline,&mut waiting.result)=>result.map_err(|_|refused())?.map_err(|_|refused())?}
    }
    async fn stored<T>(
        &self,
        operation: &Operation,
        future: impl std::future::Future<Output = Result<T, StorageError>>,
    ) -> Result<T, MobileError> {
        self.active(operation).await?;
        // Do not cancel an in-flight transaction. Settle it before releasing capacity.
        let result = future.await.map_err(|_| refused())?;
        self.active(operation).await?;
        Ok(result)
    }
    async fn prepare_owned(
        self: &Arc<Self>,
        operation: &mut Operation,
    ) -> Result<PreparedClaim, MobileError> {
        // Cache hits require a fresh proof too; cached bytes never authenticate a session.
        self.proof(operation).await?;
        #[cfg(test)]
        if let Some(hook) = &self.prepare_hook {
            hook();
        }
        let scope = self.peer.scope();
        let server = self.peer.api_base();
        let context = &operation.context;
        let run = self
            .stored(
                operation,
                self.storage.try_bind_claim_run(
                    scope,
                    server,
                    &context.scope,
                    &context.definition,
                    &context.request_sha256,
                ),
            )
            .await?;
        let run = if let Some(run) = run {
            run
        } else {
            let envelope = self
                .definition_context(context.clone(), &operation.token, operation.stop.clone())
                .await?;
            self.stored(
                operation,
                self.storage.install_claim_definitions(
                    scope,
                    server,
                    &context.scope,
                    &context.definition,
                    &envelope,
                ),
            )
            .await?;
            self.stored(
                operation,
                self.storage.bind_claim_run(
                    scope,
                    server,
                    &context.scope,
                    &context.definition,
                    &context.request_sha256,
                ),
            )
            .await?
        };
        let input = self
            .stored(
                operation,
                self.storage.load_claim_input(scope, server, context),
            )
            .await?;
        let input = if let Some(input) = input {
            input
        } else {
            let envelope = self
                .input_context(context.clone(), &operation.token, operation.stop.clone())
                .await?;
            self.stored(
                operation,
                self.storage
                    .install_claim_input(scope, server, context, &envelope),
            )
            .await?
        };
        self.active(operation).await?;
        Ok(PreparedClaim { run, input })
    }
}
