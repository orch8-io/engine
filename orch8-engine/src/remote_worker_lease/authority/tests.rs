use super::*;
use crate::remote_worker::{LeaseResponse, lease::ConfirmedLease};
use std::{
    future::{pending, ready},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};
use tokio::sync::Notify;

struct Probe<T: Unpin> {
    value: Option<T>,
    authority: LeaseAuthority,
    revoked_before_drop: Arc<AtomicBool>,
    entered: Arc<Notify>,
}
impl<T: Unpin> Future for Probe<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<T> {
        let this = self.get_mut();
        this.entered.notify_one();
        this.value.take().map_or(Poll::Pending, Poll::Ready)
    }
}
impl<T: Unpin> Drop for Probe<T> {
    fn drop(&mut self) {
        self.revoked_before_drop
            .store(self.authority.check().is_err(), Ordering::SeqCst);
    }
}
async fn acquire() -> ConfirmedLease {
    ConfirmedLease::acquire(Some(1), Duration::from_millis(100), || {
        ready(LeaseResponse::Accepted)
    })
    .await
    .unwrap()
}

#[tokio::test(start_paused = true)]
async fn acquired_and_unpolled_supervision_never_authorize_entry() {
    let lease = acquire().await;
    let authority = lease.authority();
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
    let running = lease.run(Duration::from_millis(100), pending::<()>(), || {
        ready(LeaseResponse::Accepted)
    });
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
    drop(running);
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
}

#[tokio::test(start_paused = true)]
async fn physical_owners_survive_revocation_but_not_the_last_enriched_observer() {
    let lease = acquire().await;
    let first = Arc::new(());
    let second = Arc::new(());
    let first_weak = Arc::downgrade(&first);
    let second_weak = Arc::downgrade(&second);
    let observer = lease
        .authority()
        .with_physical_owner(first)
        .with_physical_owner(second);
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    let copy = observer.clone();
    drop(lease);
    assert_eq!(copy.check(), Err(LeaseEnd::Unconfirmed));
    drop(observer);
    assert!(first_weak.upgrade().is_some());
    assert!(second_weak.upgrade().is_some());
    drop(copy);
    assert!(first_weak.upgrade().is_none());
    assert!(second_weak.upgrade().is_none());
}
#[tokio::test(start_paused = true)]
async fn success_revokes_before_operation_future_destruction() {
    let lease = acquire().await;
    let authority = lease.authority();
    let dropped = Arc::new(AtomicBool::new(false));
    let operation = Probe {
        value: Some(7),
        authority: authority.clone(),
        revoked_before_drop: dropped.clone(),
        entered: Arc::new(Notify::new()),
    };
    assert_eq!(
        lease
            .run(Duration::from_millis(100), operation, || ready(
                LeaseResponse::Accepted
            ))
            .await,
        Ok(7)
    );
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
}
#[tokio::test(start_paused = true)]
async fn cancellation_revokes_before_operation_future_destruction() {
    let lease = acquire().await;
    let authority = lease.authority();
    let dropped = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(Notify::new());
    let operation = Probe::<()> {
        value: None,
        authority: authority.clone(),
        revoked_before_drop: dropped.clone(),
        entered: entered.clone(),
    };
    let running = tokio::spawn(lease.run(Duration::from_millis(100), operation, || {
        pending::<LeaseResponse>()
    }));
    entered.notified().await;
    assert_eq!(authority.check(), Ok(()));
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
}
#[tokio::test(start_paused = true)]
async fn success_revokes_before_pending_renewal_future_destruction() {
    let lease = acquire().await;
    let authority = lease.authority();
    let dropped = Arc::new(AtomicBool::new(false));
    let renewal_authority = authority.clone();
    let renewal_dropped = dropped.clone();
    let result = lease
        .run(
            Duration::from_millis(1),
            tokio::time::sleep(Duration::from_millis(5)),
            move || Probe::<LeaseResponse> {
                value: None,
                authority: renewal_authority.clone(),
                revoked_before_drop: renewal_dropped.clone(),
                entered: Arc::new(Notify::new()),
            },
        )
        .await;
    assert!(result.is_ok());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(authority.check().is_err());
}
#[tokio::test(start_paused = true)]
async fn expiry_is_observable_without_polling_a_watcher_and_cannot_be_revived() {
    let authority = LeaseAuthority::new(Instant::now() + Duration::from_secs(1));
    authority.activate().unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(authority.check(), Err(LeaseEnd::Expired));
    assert_eq!(
        authority.renew(Instant::now() + Duration::from_secs(10)),
        Err(LeaseEnd::Expired)
    );
    assert_eq!(authority.activate(), Err(LeaseEnd::Expired));
}
#[tokio::test(start_paused = true)]
async fn accepted_renewals_publish_authority_beyond_the_original_deadline() {
    let lease = acquire().await;
    let authority = lease.authority();
    let observed = authority.clone();
    let result = lease
        .run(
            Duration::from_millis(100),
            async move {
                tokio::time::sleep(Duration::from_millis(1500)).await;
                observed.check()
            },
            || ready(LeaseResponse::Accepted),
        )
        .await;
    assert_eq!(result, Ok(Ok(())));
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
}
#[tokio::test(start_paused = true)]
async fn slow_renewal_does_not_extend_authority_by_response_latency() {
    let start = Instant::now();
    let lease = acquire().await;
    let authority = lease.authority();
    let mut n = 0;
    let result = lease
        .run(Duration::from_millis(100), pending::<()>(), || {
            n += 1;
            let first = n == 1;
            async move {
                if first {
                    tokio::time::sleep(Duration::from_millis(400)).await;
                    LeaseResponse::Accepted
                } else {
                    pending::<LeaseResponse>().await
                }
            }
        })
        .await;
    assert_eq!(result, Err(LeaseEnd::Expired));
    assert_eq!(authority.check(), Err(LeaseEnd::Expired));
    assert_eq!(start.elapsed(), Duration::from_millis(1100));
}
#[tokio::test(start_paused = true)]
async fn explicit_refusals_revoke_with_the_original_reason() {
    for (response, end) in [
        (LeaseResponse::LostOwnership, LeaseEnd::LostOwnership),
        (LeaseResponse::Rejected, LeaseEnd::Unconfirmed),
        (LeaseResponse::Unsupported, LeaseEnd::Unconfirmed),
    ] {
        let lease = acquire().await;
        let authority = lease.authority();
        assert_eq!(
            lease
                .run(Duration::from_millis(100), pending::<()>(), || ready(
                    response
                ))
                .await,
            Err(end)
        );
        assert_eq!(authority.check(), Err(end));
    }
}
#[tokio::test(start_paused = true)]
async fn renewal_panic_revokes_and_does_not_leave_observer_clones_live() {
    async fn failure() -> LeaseResponse {
        panic!("renewal fault injection")
    }
    let lease = acquire().await;
    let authority = lease.authority();
    let running = tokio::spawn(lease.run(Duration::from_millis(100), pending::<()>(), failure));
    assert!(running.await.unwrap_err().is_panic());
    assert!(authority.check().is_err());
    let next = acquire().await;
    let next_authority = next.authority();
    assert_eq!(
        next.run(
            Duration::from_millis(100),
            async { next_authority.check() },
            || ready(LeaseResponse::Accepted)
        )
        .await,
        Ok(Ok(()))
    );
    assert!(authority.check().is_err());
}
#[tokio::test(start_paused = true)]
async fn a_poisoned_state_is_never_used_as_live_authority() {
    let authority = LeaseAuthority::new(Instant::now() + Duration::from_secs(1));
    authority.activate().unwrap();
    let state = authority.state.clone();
    assert!(
        std::panic::catch_unwind(move || {
            let _held = state.lock().unwrap();
            panic!("state fault injection");
        })
        .is_err()
    );
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
    assert_eq!(
        authority.renew(Instant::now() + Duration::from_secs(1)),
        Err(LeaseEnd::Unconfirmed)
    );
    authority.revoke(LeaseEnd::LostOwnership);
    assert!(authority.check().is_err());
}

#[tokio::test]
async fn a_starved_timer_driver_cannot_let_ready_operation_work_past_expiry() {
    let lease = ConfirmedLease::confirmed(
        Duration::from_secs(1),
        Instant::now() + Duration::from_millis(100),
    );
    let ready = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(AtomicBool::new(false));
    let can_finish = ready.clone();
    let progressed = entered.clone();
    let operation = std::future::poll_fn(move |_| {
        if can_finish.load(Ordering::SeqCst) {
            progressed.store(true, Ordering::SeqCst);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    });
    let mut run = Box::pin(lease.run(Duration::from_millis(10), operation, || {
        pending::<LeaseResponse>()
    }));
    std::future::poll_fn(|cx| {
        assert!(run.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    std::thread::sleep(Duration::from_millis(110));
    ready.store(true, Ordering::SeqCst);
    assert_eq!(run.await, Err(LeaseEnd::Expired));
    assert!(!entered.load(Ordering::SeqCst));
}

#[tokio::test]
async fn operation_panic_revokes_observer_clones() {
    async fn failure() {
        tokio::task::yield_now().await;
        panic!("operation fault injection");
    }
    let lease = acquire().await;
    let authority = lease.authority();
    let running = tokio::spawn(lease.run(Duration::from_millis(100), failure(), || {
        ready(LeaseResponse::Accepted)
    }));
    assert!(running.await.unwrap_err().is_panic());
    assert_eq!(authority.check(), Err(LeaseEnd::Unconfirmed));
}
