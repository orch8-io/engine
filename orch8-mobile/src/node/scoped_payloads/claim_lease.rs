//! Frozen scoped authentication and exact task/lease identity; no native grant.
use super::{
    Arc, CancellationToken, DelegationClaimContext, Duration, Instant, MobileError, Operation,
    Permit, RemoteTask, ScopedPayloadReader, refused,
};
use orch8_engine::remote_worker::{
    HttpLeaseClient, LeaseResponse,
    lease::{ConfirmedLease, LeaseAuthority, LeaseEnd},
};
use std::{future::Future, sync::Mutex, task::Poll};

/// Only acquisition against the expected peer can construct this value.
pub(crate) struct ScopedClaimLease {
    lease: ConfirmedLease,
    client: HttpLeaseClient,
    authority: ScopedClaimAuthority,
    cadence: Duration,
}

/// The original immutable transport object remains attached to its observer.
/// This proves a live scoped claim, not permission to claim a native root.
#[derive(Clone)]
pub(crate) struct ScopedClaimAuthority {
    reader: Arc<ScopedPayloadReader>,
    task: Arc<RemoteTask>,
    context: DelegationClaimContext,
    lease: LeaseAuthority,
    revoke: CancellationToken,
    until: Instant,
    ended: Arc<Mutex<Option<LeaseEnd>>>,
}

impl std::fmt::Debug for ScopedClaimLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedClaimLease").finish_non_exhaustive()
    }
}
impl std::fmt::Debug for ScopedClaimAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScopedClaimAuthority")
            .finish_non_exhaustive()
    }
}

impl ScopedClaimAuthority {
    pub(crate) fn task(&self) -> &RemoteTask {
        &self.task
    }

    fn window(&self) -> Result<(), LeaseEnd> {
        self.window_at(Instant::now(), chrono::Utc::now())
    }

    fn window_at(&self, now: Instant, utc: chrono::DateTime<chrono::Utc>) -> Result<(), LeaseEnd> {
        let mut ended = self.ended.lock().map_err(|_| LeaseEnd::Unconfirmed)?;
        if let Some(end) = *ended {
            return Err(end);
        }
        let end = if self.revoke.is_cancelled() {
            Some(LeaseEnd::Unconfirmed)
        } else if now >= self.until || utc >= self.context.expires_at {
            Some(LeaseEnd::Expired)
        } else {
            None
        };
        // Observing logical expiry is terminal even if the wall clock moves
        // backwards before its conservative monotonic deadline.
        if let Some(end) = end {
            *ended = Some(end);
            return Err(end);
        }
        Ok(())
    }

    pub(crate) fn check(&self) -> Result<(), LeaseEnd> {
        self.window()?;
        self.lease.check()
    }

    /// Scope freshness surrounds the actual database read. It cannot authorize
    /// native instance writes, credentials or a different local execution root.
    pub(crate) async fn verify_current(&self) -> Result<(), LeaseEnd> {
        self.check()?;
        self.reader
            .storage
            .check_worker_peer_device(&self.reader.peer, &self.reader.device)
            .await
            .map_err(|_| {
                self.revoke.cancel();
                LeaseEnd::Unconfirmed
            })?;
        self.check()
    }

    /// Use the original immutable task, never a reconstructed workflow object.
    pub(crate) async fn verify_for(
        &self,
        reader: &ScopedPayloadReader,
        task: &RemoteTask,
    ) -> Result<(), LeaseEnd> {
        self.check()?;
        if !std::ptr::eq(self.task.as_ref(), task)
            || !Arc::ptr_eq(&self.reader.storage, &reader.storage)
            || !self.reader.peer.same_binding(&reader.peer)
            || self.reader.device != reader.device
            || self.reader.base != reader.base
        {
            return Err(LeaseEnd::Unconfirmed);
        }
        self.verify_current().await
    }
}

impl ScopedClaimLease {
    /// The work closure is invoked only after supervision activates and the
    /// current local scope is verified. Renewal uses the same frozen credential.
    pub(crate) async fn run<T, F, Fut>(self, work: F) -> Result<T, LeaseEnd>
    where
        F: FnOnce(ScopedClaimAuthority) -> Fut,
        Fut: Future<Output = T>,
    {
        let Self {
            lease,
            client,
            authority,
            cadence,
        } = self;
        let watching = authority.clone();
        let window = authority.clone();
        let revoke = authority.revoke.clone();
        let until = authority.until;
        let operation = async move {
            authority.verify_current().await?;
            let final_authority = authority.clone();
            let result = work(authority).await;
            final_authority.verify_current().await?;
            Ok(result)
        };
        let renewing = || {
            let authority = watching.clone();
            let client = client.clone();
            async move {
                if authority.verify_current().await.is_err() {
                    return LeaseResponse::Rejected;
                }
                let result = client
                    .heartbeat(
                        authority.context.scope.task_id,
                        authority.context.scope.claim_epoch,
                    )
                    .await;
                if authority.verify_current().await.is_err() {
                    return LeaseResponse::Rejected;
                }
                result
            }
        };
        let running = lease.run(cadence, operation, renewing);
        tokio::pin!(running);
        // Check the fixed logical expiry directly before each poll: a stalled
        // timer driver must not let ready work advance past it.
        let guarded = std::future::poll_fn(|cx| {
            if let Err(end) = window.window() {
                return Poll::Ready(Err(end));
            }
            running.as_mut().poll(cx)
        });
        tokio::select! {
            biased;
            () = revoke.cancelled() => Err(LeaseEnd::Unconfirmed),
            () = tokio::time::sleep_until(until) => Err(LeaseEnd::Expired),
            result = guarded => result?,
        }
    }
}

struct Acquisition {
    reader: Arc<ScopedPayloadReader>,
    task: Arc<RemoteTask>,
    operation: Operation,
    cadence: Duration,
    until: Instant,
    _permit: Arc<Permit>,
}

/// Cancel abandoned acquisition without cancelling a successful ownership handoff.
struct Acquiring {
    task: tokio::task::JoinHandle<Result<ScopedClaimLease, MobileError>>,
    stop: Option<CancellationToken>,
}
impl Drop for Acquiring {
    fn drop(&mut self) {
        if let Some(stop) = &self.stop {
            stop.cancel();
        }
    }
}

impl Acquisition {
    async fn execute(mut self) -> Result<ScopedClaimLease, MobileError> {
        self.reader.proof(&mut self.operation).await?;
        let client = HttpLeaseClient::new(
            self.reader.http.clone(),
            &self.reader.base,
            vec![("x-api-key", self.operation.token.clone())],
            self.operation.context.scope.worker_id.clone(),
        );
        let scope = &self.operation.context.scope;
        let lease = self
            .operation
            .io(ConfirmedLease::acquire(
                self.task.lease_secs,
                self.cadence,
                || client.heartbeat(scope.task_id, scope.claim_epoch),
            ))
            .await?
            .map_err(|_| refused())?;
        self.reader.active(&self.operation).await?;
        let authority = ScopedClaimAuthority {
            reader: self.reader,
            task: self.task,
            context: self.operation.context,
            lease: lease
                .authority()
                .with_revocation(self.operation.stop.clone(), LeaseEnd::Unconfirmed),
            revoke: self.operation.stop,
            until: self.until,
            ended: Arc::new(Mutex::new(None)),
        };
        Ok(ScopedClaimLease {
            lease,
            client,
            authority,
            cadence: self.cadence,
        })
    }
}

impl ScopedPayloadReader {
    pub(crate) async fn acquire_claim_lease(
        self: &Arc<Self>,
        task: Arc<RemoteTask>,
        token: &str,
        revoke: CancellationToken,
        cadence: Duration,
    ) -> Result<ScopedClaimLease, MobileError> {
        task.validate_delegation(&self.peer.runtime_id().to_string())
            .map_err(|_| refused())?;
        let context = task.delegation.clone().ok_or_else(refused)?;
        self.peer
            .validate_claim_identity(&context)
            .map_err(|_| refused())?;
        if self.reserved.is_some()
            || !token.starts_with(crate::credential::DEVICE_SESSION_PREFIX)
            || token.len() > 1024 * 1024
        {
            return Err(refused());
        }
        let started = Instant::now();
        let remaining = (context.expires_at - chrono::Utc::now())
            .to_std()
            .map_err(|_| refused())?;
        let until = started.checked_add(remaining).ok_or_else(refused)?;
        let permit = Permit::acquire(&self.budget)?;
        let stop = revoke.child_token();
        let deadline = (Instant::now() + Duration::from_secs(30)).min(until);
        let acquisition = Acquisition {
            reader: self.clone(),
            task,
            operation: Operation {
                context,
                token: token.to_owned(),
                stop: stop.clone(),
                deadline,
            },
            cadence,
            until,
            _permit: permit,
        };
        let task = tokio::spawn(acquisition.execute());
        let mut waiting = Acquiring {
            task,
            stop: Some(stop),
        };
        let result = tokio::select! {
            biased;
            () = revoke.cancelled() => Err(refused()),
            result = tokio::time::timeout_at(deadline, &mut waiting.task) =>
                result.map_err(|_| refused())?.map_err(|_| refused())?,
        };
        let lease = result?;
        // The returned lease owns the same child token. Cancellation of the
        // parent still revokes it; dropping this completed waiter must not.
        waiting.stop = None;
        Ok(lease)
    }
}

#[cfg(test)]
#[path = "claim_lease/tests.rs"]
mod tests;
