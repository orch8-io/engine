//! Keep stored-key grants valid while streaming, including idle/backpressured RPCs.
use super::CallerPrincipal;
use orch8_storage::StorageBackend;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

/// Cancelling an authenticated operation drops its owned future; no detached
/// monitor is left behind. Revalidation uses the same storage-scoped key cache
/// as unary requests (local revoke invalidates it; peer staleness is <=30s).
pub(crate) async fn guard_operation<T>(
    principal: Option<&CallerPrincipal>,
    storage: &Arc<dyn StorageBackend>,
    path: &str,
    operation: impl Future<Output = T>,
) -> Result<T, Status> {
    let Some(principal) = principal else {
        return Ok(operation.await);
    };
    check(principal, storage, path).await?;
    tokio::pin!(operation);
    let period = Duration::from_secs(1);
    let mut timer = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _=timer.tick()=>check(principal,storage,path).await?,
            result=&mut operation=>return Ok(result),
        }
    }
}
async fn check(
    principal: &CallerPrincipal,
    storage: &Arc<dyn StorageBackend>,
    path: &str,
) -> Result<(), Status> {
    tokio::time::timeout(Duration::from_secs(5), principal.reauthorize(storage, path))
        .await
        .map_err(|_| Status::unavailable("credential reauthorization timed out"))?
}

/// Stop owned work when authorization, the response consumer, or the server
/// lifetime ends, even if the work is blocked away from a network read/write.
pub(crate) async fn run_stream<T>(
    principal: Option<&CallerPrincipal>,
    storage: &Arc<dyn StorageBackend>,
    path: &str,
    sender: mpsc::Sender<Result<T, Status>>,
    terminal: oneshot::Sender<Status>,
    shutdown: &tokio_util::sync::CancellationToken,
    operation: impl Future<Output = ()>,
) {
    tokio::select! {
        biased;
        () = sender.closed() => {}
        () = shutdown.cancelled() => {let _ = terminal.send(Status::unavailable("server shutting down"));}
        result = guard_operation(principal, storage, path, operation) => {
            if let Err(status) = result {let _ = terminal.send(status);}
        }
    }
}

/// Terminal authentication errors bypass a full data queue. Normal producer
/// completion still drains queued data, then ends the response.
pub(crate) fn response_stream<T: Unpin>(
    receiver: mpsc::Receiver<Result<T, Status>>,
    failure: oneshot::Receiver<Status>,
) -> impl tokio_stream::Stream<Item = Result<T, Status>> {
    ResponseStream {
        receiver,
        failure,
        failure_done: false,
        terminal: false,
    }
}
struct ResponseStream<T> {
    receiver: mpsc::Receiver<Result<T, Status>>,
    failure: oneshot::Receiver<Status>,
    failure_done: bool,
    terminal: bool,
}
impl<T: Unpin> tokio_stream::Stream for ResponseStream<T> {
    type Item = Result<T, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminal {
            return Poll::Ready(None);
        }
        if !this.failure_done {
            match Pin::new(&mut this.failure).poll(cx) {
                Poll::Ready(Ok(status)) => {
                    this.terminal = true;
                    this.receiver.close();
                    return Poll::Ready(Some(Err(status)));
                }
                Poll::Ready(Err(_)) => this.failure_done = true,
                Poll::Pending => {}
            }
        }
        match this.receiver.poll_recv(cx) {
            Poll::Ready(None) if !this.failure_done => Poll::Pending,
            result => result,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::StreamExt as _;
    #[tokio::test]
    async fn terminal_authentication_error_bypasses_a_full_data_queue() {
        let (sender, receiver) = mpsc::channel(1);
        sender.send(Ok(7)).await.unwrap();
        let (failure, status) = oneshot::channel();
        failure.send(Status::unauthenticated("revoked")).unwrap();
        let mut stream = response_stream(receiver, status);
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert!(stream.next().await.is_none());
        assert!(sender.send(Ok(8)).await.is_err());
    }
    #[tokio::test]
    async fn normal_completion_drains_the_data_queue() {
        let (sender, receiver) = mpsc::channel(1);
        sender.send(Ok(7)).await.unwrap();
        drop(sender);
        let (failure, status) = oneshot::channel();
        drop(failure);
        let mut stream = response_stream(receiver, status);
        assert_eq!(stream.next().await.unwrap().unwrap(), 7);
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn closed_data_queue_waits_for_the_terminal_auth_decision() {
        let (sender, receiver) = mpsc::channel::<Result<u32, Status>>(1);
        drop(sender);
        let (failure, status) = oneshot::channel();
        let mut stream = response_stream(receiver, status);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), stream.next())
                .await
                .is_err()
        );
        failure.send(Status::unauthenticated("revoked")).unwrap();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn reauthorization_cancels_and_drops_a_blocked_operation() {
        use orch8_types::api_key::{ApiCapability, mint_scoped};
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let key = mint_scoped("test", "blocked", None, vec![ApiCapability::Worker]);
        storage.create_api_key(&key.record).await.unwrap();
        let principal = CallerPrincipal {
            record: key.record.clone(),
        };
        let dropped = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&dropped);
        let entered = Arc::new(tokio::sync::Notify::new());
        let ready = Arc::clone(&entered);
        let active_storage = Arc::clone(&storage);
        let task = tokio::spawn(async move {
            guard_operation(
                Some(&principal),
                &active_storage,
                "/orch8.Orch8Service/WorkerStream",
                async {
                    let _drop = Dropped(observed);
                    ready.notify_one();
                    std::future::pending::<()>().await;
                },
            )
            .await
        });
        entered.notified().await;
        assert!(storage.revoke_api_key(&key.record.id).await.unwrap());
        let result = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unauthenticated);
        assert!(dropped.load(Ordering::SeqCst));
    }
    #[tokio::test]
    async fn unavailable_key_storage_times_out_before_entering_the_operation() {
        use orch8_types::api_key::{ApiCapability, mint_scoped};
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let key = mint_scoped("test", "blocked-lookup", None, vec![ApiCapability::Worker]);
        storage.create_api_key(&key.record).await.unwrap();
        let principal = CallerPrincipal { record: key.record };
        let held = sqlite.pool().acquire().await.unwrap();
        let entered = std::sync::atomic::AtomicBool::new(false);
        let result = tokio::time::timeout(
            Duration::from_secs(7),
            guard_operation(
                Some(&principal),
                &storage,
                "/orch8.Orch8Service/WorkerStream",
                async {
                    entered.store(true, std::sync::atomic::Ordering::SeqCst);
                },
            ),
        )
        .await
        .unwrap();
        drop(held);
        assert_eq!(result.unwrap_err().code(), tonic::Code::Unavailable);
        assert!(!entered.load(std::sync::atomic::Ordering::SeqCst));
    }
    #[tokio::test]
    async fn consumer_disconnect_and_shutdown_drop_blocked_stream_work() {
        use std::sync::atomic::{AtomicBool, Ordering};
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let storage: Arc<dyn StorageBackend> = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        for disconnect in [true, false] {
            let (sender, receiver) = mpsc::channel::<Result<(), Status>>(1);
            let (terminal, status) = oneshot::channel();
            let shutdown = tokio_util::sync::CancellationToken::new();
            let stopping = shutdown.clone();
            let dropped = Arc::new(AtomicBool::new(false));
            let observed = Arc::clone(&dropped);
            let entered = Arc::new(tokio::sync::Notify::new());
            let ready = Arc::clone(&entered);
            let active_storage = Arc::clone(&storage);
            let task = tokio::spawn(async move {
                run_stream(
                    None,
                    &active_storage,
                    "/orch8.Orch8Service/WorkerStream",
                    sender,
                    terminal,
                    &stopping,
                    async {
                        let _drop = Dropped(observed);
                        ready.notify_one();
                        std::future::pending::<()>().await;
                    },
                )
                .await;
            });
            entered.notified().await;
            if disconnect {
                drop(receiver);
            } else {
                shutdown.cancel();
            }
            tokio::time::timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert!(dropped.load(Ordering::SeqCst));
            if !disconnect {
                assert_eq!(status.await.unwrap().code(), tonic::Code::Unavailable);
            }
        }
    }
}
