use super::*;
use std::future::Future as _;

enum Transition {
    GracefulStop,
    NewerClaim,
}

fn queued_entry(transition: Transition) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .max_blocking_threads(1)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let server = spawn_control_plane(route_with(Vec::new(), 200, 200)).await;
        let slots = Arc::new(Semaphore::new(1));
        let h = prepared_admission::prepared_harness(&server, slots.clone()).await;
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            entered_tx.send(()).unwrap();
            let _ = release_rx.recv_timeout(Duration::from_secs(10));
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        let id = uuid::Uuid::new_v4();
        let mut payload = task_json(id);
        payload["lease_secs"] = json!(30);
        let task: RemoteTask = serde_json::from_value(payload).unwrap();
        let mut newer = task.clone();
        newer.claim_epoch += 1;
        let cadence = Duration::from_millis(100);
        let mut execution = Box::pin(h.worker.execute(task, cadence));
        let started = async {
            loop {
                if prepared_admission::starts(&h).await == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        };
        tokio::select! {
            biased;
            result = tokio::time::timeout(Duration::from_secs(5), started) => result.unwrap(),
            () = &mut execution => panic!("worker ended before native reservation"),
        }
        for _ in 0..10 {
            std::future::poll_fn(|cx| {
                assert!(execution.as_mut().poll(cx).is_pending());
                std::task::Poll::Ready(())
            })
            .await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(slots.available_permits(), 0);
        let successor = match transition {
            Transition::GracefulStop => {
                h.worker.stop();
                None
            }
            Transition::NewerClaim => Some(h.worker.task_future(newer, cadence).unwrap()),
        };
        // No executor poll can drop the old invocation in this interval.
        // Physical admission returns only once its queued native call settles.
        release_tx.send(()).unwrap();
        let until = std::time::Instant::now() + Duration::from_secs(5);
        while slots.available_permits() == 0 && std::time::Instant::now() < until {
            std::thread::sleep(Duration::from_millis(1));
        }
        assert_eq!(slots.available_permits(), 1, "physical call did not settle");
        let calls = h.inputs.lock().unwrap().len();
        match transition {
            Transition::GracefulStop => {
                assert_eq!(calls, 1, "graceful stop must drain admitted work");
            }
            Transition::NewerClaim => {
                assert_eq!(calls, 0, "superseded queued callback entered the host");
            }
        }
        blocker.await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), execution)
            .await
            .unwrap();
        if let Some(successor) = successor {
            tokio::time::timeout(Duration::from_secs(5), successor)
                .await
                .unwrap();
            assert_eq!(h.worker.stats().lost, 1);
            assert_eq!(server.bodies(&format!("{id}/release"))[0]["started"], false);
        }
        assert_eq!(server.count(&format!("{id}/complete")), calls);
        assert_eq!(server.count(&format!("{id}/fail")), 0);
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
}

#[test]
fn supersession_refuses_queued_host_entry_without_executor_progress() {
    queued_entry(Transition::NewerClaim);
}

#[test]
fn graceful_worker_stop_drains_queued_leased_native_work() {
    queued_entry(Transition::GracefulStop);
}
