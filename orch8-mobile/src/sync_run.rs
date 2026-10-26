use super::{MobileError, SyncAuth, SyncOrchestrator, SyncResult};
use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

struct SyncWait {
    cancelled: CancellationToken,
    task: tokio::task::JoinHandle<Result<SyncResult, MobileError>>,
}

impl Drop for SyncWait {
    fn drop(&mut self) {
        // Do not abort the task: already-dispatched SQLite writes must settle
        // before another run can acquire its admission permit.
        self.cancelled.cancel();
    }
}

impl SyncOrchestrator {
    pub(crate) async fn sync_with_auth<F, A>(
        &self,
        manifest_url: &str,
        registered_handlers: &HashSet<String>,
        cancelled: CancellationToken,
        authorize: A,
    ) -> Result<SyncResult, MobileError>
    where
        F: Future<Output = Result<SyncAuth, MobileError>> + Send + 'static,
        A: FnOnce(CancellationToken) -> F + Send + 'static,
    {
        let permit = Arc::clone(&self.admission)
            .try_acquire_owned()
            .map_err(|_| MobileError::ResourceLimit {
                detail: "manifest sync already in progress; retry after it settles".into(),
            })?;
        let mut operation = self.clone();
        operation.run_cancelled = Some(cancelled.clone());
        let url = manifest_url.to_owned();
        let handlers = registered_handlers.clone();
        let authorization_cancelled = cancelled.clone();
        let task = tokio::spawn(async move {
            let _admission = permit;
            operation.check_active()?;
            let auth = operation.io(authorize(authorization_cancelled)).await??;
            operation.sync_inner(&url, &auth, &handlers).await
        });
        let mut waiting = SyncWait { cancelled, task };
        (&mut waiting.task).await.map_err(|_| MobileError::Engine {
            detail: "manifest sync task failed".into(),
        })?
    }

    #[cfg(test)]
    pub(crate) fn sync_in_progress(&self) -> bool {
        self.admission.available_permits() == 0
    }

    pub(super) fn check_active(&self) -> Result<(), MobileError> {
        if self
            .run_deadline
            .is_some_and(|at| tokio::time::Instant::now() >= at)
            || self
                .run_cancelled
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
            Err(MobileError::shutdown())
        } else {
            Ok(())
        }
    }

    pub(super) async fn io<F: Future>(&self, future: F) -> Result<F::Output, MobileError> {
        self.check_active()?;
        let deadline = async {
            match self.run_deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending::<()>().await,
            }
        };
        tokio::pin!(deadline);
        if let Some(cancelled) = self.run_cancelled.as_ref() {
            tokio::select! {
                biased;
                () = cancelled.cancelled() => Err(MobileError::shutdown()),
                () = &mut deadline => Err(MobileError::shutdown()),
                value = future => Ok(value),
            }
        } else {
            tokio::select! {
                biased;
                () = &mut deadline => Err(MobileError::shutdown()),
                value = future => Ok(value),
            }
        }
    }
}
