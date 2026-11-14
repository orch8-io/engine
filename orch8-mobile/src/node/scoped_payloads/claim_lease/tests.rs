use super::super::tests::{Fixture, fixture};
use super::*;
use std::future::pending;
use tokio::sync::oneshot;

async fn acquire(f: &Fixture, stop: CancellationToken, cadence: Duration) -> ScopedClaimLease {
    f.reader
        .acquire_claim_lease(
            Arc::new(f.task.clone()),
            "dst_PRIVATE_FROZEN",
            stop,
            cadence,
        )
        .await
        .unwrap()
}

async fn retire(f: &Fixture) {
    let account = f
        .reader
        .storage
        .open_account_scope("alice", "issuer", "alice")
        .await
        .unwrap();
    f.reader
        .storage
        .retire_account_scope(&account)
        .await
        .unwrap();
}

#[tokio::test]
async fn successful_acquisition_hands_off_a_live_supervised_claim_without_cancelling_it() {
    let f = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_millis(10)).await;
    assert_eq!(f.reader.budget.available_permits(), 1);
    let observer = lease.authority.clone();
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    assert!(!format!("{lease:?} {observer:?}").contains("PRIVATE"));
    let returned = lease
        .run(|authority| async move {
            authority
                .verify_for(&authority.reader, authority.task())
                .await
                .unwrap();
            authority
        })
        .await
        .unwrap();
    assert_eq!(returned.check(), Err(LeaseEnd::Unconfirmed));
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    let state = f.state.lock().unwrap();
    assert!(state.seen.len() >= 2);
    assert!(state.seen[0].0.ends_with("/workers/identity"));
    assert!(
        state.seen[1..]
            .iter()
            .all(|(path, _)| path.ends_with("/heartbeat"))
    );
    assert!(
        state
            .seen
            .iter()
            .all(|(_, token)| token == "dst_PRIVATE_FROZEN")
    );
}

#[tokio::test]
async fn unpolled_supervision_never_calls_work_and_drop_revokes_observers() {
    let f = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    let observer = lease.authority.clone();
    let called = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let capture = called.clone();
    let running = lease.run(move |_| {
        capture.store(true, std::sync::atomic::Ordering::SeqCst);
        pending::<()>()
    });
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    drop(running);
    assert!(!called.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
}

#[tokio::test]
async fn identity_rejection_cannot_mint_a_claim_or_send_a_heartbeat() {
    for bad in ["worker", "tenant", "device", "nonce", "expired", "status"] {
        let f = fixture().await;
        f.state.lock().unwrap().bad = bad;
        let error = f
            .reader
            .acquire_claim_lease(
                Arc::new(f.task.clone()),
                "dst_PRIVATE_FROZEN",
                CancellationToken::new(),
                Duration::from_millis(10),
            )
            .await
            .unwrap_err();
        assert!(!format!("{error:?}").contains("PRIVATE"));
        assert_eq!(f.state.lock().unwrap().seen.len(), 1, "{bad}");
        assert_eq!(f.reader.budget.available_permits(), 1);
    }
}

#[tokio::test]
async fn heartbeat_denial_is_final_without_refresh_or_an_ambient_credential() {
    for bad in ["heartbeat-denied", "heartbeat-lost"] {
        let f = fixture().await;
        f.state.lock().unwrap().bad = bad;
        let result = f
            .reader
            .acquire_claim_lease(
                Arc::new(f.task.clone()),
                "dst_PRIVATE_FROZEN",
                CancellationToken::new(),
                Duration::from_millis(10),
            )
            .await;
        assert!(result.is_err());
        let state = f.state.lock().unwrap();
        assert_eq!(state.seen.len(), 2);
        assert!(
            state
                .seen
                .iter()
                .all(|(_, token)| token == "dst_PRIVATE_FROZEN")
        );
        assert_eq!(f.reader.budget.available_permits(), 1);
    }
}

#[tokio::test]
async fn malformed_mailbox_stopped_acquisition_and_retired_scope_do_no_io() {
    let f = fixture().await;
    for bad in [
        "attempt",
        "task",
        "generation",
        "worker",
        "params",
        "context",
        "missing",
        "expired",
    ] {
        let mut task = f.task.clone();
        match bad {
            "attempt" => task.attempt = 1,
            "task" => task.id = uuid::Uuid::new_v4(),
            "generation" => task.claim_epoch += 1,
            "worker" => task.delegation.as_mut().unwrap().scope.worker_id = "other".into(),
            "params" => task.params = serde_json::json!({"private":"data"}),
            "context" => task.context = serde_json::json!({"private":"data"}),
            "missing" => task.delegation = None,
            "expired" => {
                task.delegation.as_mut().unwrap().expires_at =
                    chrono::Utc::now() - chrono::Duration::seconds(1);
            }
            _ => unreachable!(),
        }
        assert!(
            f.reader
                .acquire_claim_lease(
                    Arc::new(task),
                    "dst_token",
                    CancellationToken::new(),
                    Duration::from_secs(10)
                )
                .await
                .is_err(),
            "{bad}"
        );
    }
    assert!(
        f.reader
            .acquire_claim_lease(
                Arc::new(f.task.clone()),
                "operator-key",
                CancellationToken::new(),
                Duration::from_secs(10)
            )
            .await
            .is_err()
    );
    let stop = CancellationToken::new();
    stop.cancel();
    assert!(
        f.reader
            .acquire_claim_lease(
                Arc::new(f.task.clone()),
                "dst_token",
                stop,
                Duration::from_secs(10)
            )
            .await
            .is_err()
    );
    // A stopped acquisition may still be settling its owned task.
    tokio::time::timeout(Duration::from_secs(2), async {
        while f.reader.budget.available_permits() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    retire(&f).await;
    assert!(
        f.reader
            .acquire_claim_lease(
                Arc::new(f.task.clone()),
                "dst_token",
                CancellationToken::new(),
                Duration::from_secs(10)
            )
            .await
            .is_err()
    );
    assert!(f.state.lock().unwrap().seen.is_empty());
}

#[tokio::test]
async fn authority_rejects_reconstructed_tasks_and_different_databases_devices_and_endpoints() {
    let f = fixture().await;
    let other = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    lease
        .run(|authority| async move {
            let task = authority.task().clone();
            assert_eq!(
                authority.verify_for(&f.reader, &task).await,
                Err(LeaseEnd::Unconfirmed)
            );
            let mut changed = f.reader.as_ref().clone();
            changed.device = "other-device".into();
            assert_eq!(
                authority.verify_for(&changed, authority.task()).await,
                Err(LeaseEnd::Unconfirmed)
            );
            changed = f.reader.as_ref().clone();
            changed.base = "https://other.example/api/v1/".into();
            assert_eq!(
                authority.verify_for(&changed, authority.task()).await,
                Err(LeaseEnd::Unconfirmed)
            );
            changed = f.reader.as_ref().clone();
            changed.storage = other.reader.storage.clone();
            assert_eq!(
                authority.verify_for(&changed, authority.task()).await,
                Err(LeaseEnd::Unconfirmed)
            );
            changed = f.reader.as_ref().clone();
            changed.peer = other.reader.peer.clone();
            assert_eq!(
                authority.verify_for(&changed, authority.task()).await,
                Err(LeaseEnd::Unconfirmed)
            );
            authority
                .verify_for(&f.reader, authority.task())
                .await
                .unwrap();
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn revocation_wakes_pending_work_without_waiting_for_renewal() {
    let f = fixture().await;
    let stop = CancellationToken::new();
    let lease = acquire(&f, stop.clone(), Duration::from_secs(10)).await;
    let (send, receive) = oneshot::channel();
    let mut running = tokio::spawn(lease.run(|authority| async move {
        send.send(authority).unwrap();
        pending::<()>().await;
    }));
    let observer = receive.await.unwrap();
    stop.cancel();
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    let result = tokio::time::timeout(Duration::from_secs(1), &mut running).await;
    if result.is_err() {
        running.abort();
        let _ = running.await;
    }
    assert_eq!(result.unwrap().unwrap(), Err(LeaseEnd::Unconfirmed));
}

#[tokio::test]
async fn logical_expiry_wakes_work_independently_of_long_heartbeat_cadence() {
    let f = fixture().await;
    let mut lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    lease.authority.until = Instant::now() + Duration::from_millis(50);
    let mut running = tokio::spawn(lease.run(|_| pending::<()>()));
    let result = tokio::time::timeout(Duration::from_secs(2), &mut running).await;
    if result.is_err() {
        running.abort();
        let _ = running.await;
    }
    assert_eq!(result.unwrap().unwrap(), Err(LeaseEnd::Expired));
}

#[tokio::test]
async fn expired_logical_authority_refuses_ready_work_with_a_stalled_timer_driver() {
    let f = fixture().await;
    let mut lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    lease.authority.until = Instant::now() + Duration::from_secs(1);
    let (send, receive) = oneshot::channel();
    let (finish, finished) = oneshot::channel();
    let running = tokio::spawn(lease.run(|authority| async move {
        send.send(authority).unwrap();
        finished.await.unwrap();
    }));
    let observer = receive.await.unwrap();
    std::thread::sleep(Duration::from_millis(1100));
    assert_eq!(observer.check(), Err(LeaseEnd::Expired));
    let _ = finish.send(());
    assert_eq!(running.await.unwrap(), Err(LeaseEnd::Expired));
}

#[tokio::test]
async fn observed_wall_clock_expiry_is_permanent_across_clones_and_clock_rollback() {
    let f = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    let result = lease
        .run(|authority| async move {
            let observer = authority.clone();
            let now = Instant::now();
            let expiry = authority.context.expires_at;
            assert_eq!(observer.window_at(now, expiry), Err(LeaseEnd::Expired));
            assert_eq!(
                authority.window_at(now, expiry - chrono::Duration::seconds(1)),
                Err(LeaseEnd::Expired)
            );
            assert_eq!(authority.check(), Err(LeaseEnd::Expired));
        })
        .await;
    assert_eq!(result, Err(LeaseEnd::Expired));
}

#[tokio::test]
async fn renewal_denial_revokes_observers_without_refresh_or_successful_work() {
    for (bad, expected) in [
        ("heartbeat-denied", LeaseEnd::Unconfirmed),
        ("heartbeat-lost", LeaseEnd::LostOwnership),
    ] {
        let f = fixture().await;
        let lease = acquire(&f, CancellationToken::new(), Duration::from_millis(10)).await;
        let observer = lease.authority.clone();
        f.state.lock().unwrap().bad = bad;
        let result = tokio::time::timeout(Duration::from_secs(2), lease.run(|_| pending::<()>()))
            .await
            .unwrap();
        assert_eq!(result, Err(expected));
        assert_eq!(observer.check(), Err(expected));
        let state = f.state.lock().unwrap();
        assert_eq!(state.seen.len(), 3);
        assert!(
            state
                .seen
                .iter()
                .all(|(_, token)| token == "dst_PRIVATE_FROZEN")
        );
    }
}

#[tokio::test]
async fn retirement_during_renewal_cannot_be_overridden_by_a_late_success() {
    let f = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_millis(10)).await;
    f.state.lock().unwrap().hold = "heartbeat";
    let (send, receive) = oneshot::channel();
    let running = tokio::spawn(lease.run(|authority| async move {
        send.send(authority).unwrap();
        pending::<()>().await;
    }));
    let observer = receive.await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), f.entered.notified())
        .await
        .unwrap();
    retire(&f).await;
    assert_eq!(observer.verify_current().await, Err(LeaseEnd::Unconfirmed));
    f.release.notify_one();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), running)
            .await
            .unwrap()
            .unwrap(),
        Err(LeaseEnd::Unconfirmed)
    );
    assert_eq!(observer.check(), Err(LeaseEnd::Unconfirmed));
    assert!(
        f.state
            .lock()
            .unwrap()
            .seen
            .iter()
            .all(|(_, token)| token == "dst_PRIVATE_FROZEN")
    );
}

#[tokio::test]
async fn abandoned_acquisition_cancels_owned_io_and_returns_capacity() {
    for stage in ["identity", "heartbeat"] {
        let f = fixture().await;
        f.state.lock().unwrap().hold = stage;
        let reader = f.reader.clone();
        let task = Arc::new(f.task.clone());
        let waiting = tokio::spawn(async move {
            reader
                .acquire_claim_lease(
                    task,
                    "dst_token",
                    CancellationToken::new(),
                    Duration::from_secs(10),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), f.entered.notified())
            .await
            .unwrap();
        assert_eq!(f.reader.budget.available_permits(), 0);
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(2), async {
            while f.reader.budget.available_permits() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        f.release.notify_one();
        assert_eq!(
            f.state.lock().unwrap().seen.len(),
            if stage == "identity" { 1 } else { 2 }
        );
    }
}

#[tokio::test]
async fn logical_expiry_during_a_ready_poll_cannot_return_success() {
    let f = fixture().await;
    let mut lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    lease.authority.until = Instant::now() + Duration::from_millis(200);
    let result = lease
        .run(|_| async {
            // A ready inner future may block its polling thread past the deadline.
            std::thread::sleep(Duration::from_millis(300));
            7
        })
        .await;
    assert_eq!(result, Err(LeaseEnd::Expired));
}

#[tokio::test]
async fn scope_retirement_during_a_ready_poll_cannot_return_success() {
    let f = fixture().await;
    let lease = acquire(&f, CancellationToken::new(), Duration::from_secs(10)).await;
    let result = lease
        .run(|_| async {
            retire(&f).await;
            7
        })
        .await;
    assert_eq!(result, Err(LeaseEnd::Unconfirmed));
}
