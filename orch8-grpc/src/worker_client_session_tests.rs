use super::*;

struct TestSession {
    transport: GrpcLeaseTransport,
    outbound: mpsc::Receiver<proto::WorkerStreamClient>,
    pending: Arc<StdMutex<VecDeque<Pending>>>,
    closed: CancellationToken,
    task: ClaimedTask,
}

impl TestSession {
    async fn new() -> Self {
        let transport = GrpcLeaseTransport::new(GrpcWorkerConfig {
            endpoint: "http://127.0.0.1:1".into(),
            api_key: "test-key".into(),
            tenant_id: "acme".into(),
            worker_id: "worker".into(),
            ca_pem: None,
            drain: CancellationToken::new(),
            headers: BTreeMap::new(),
        })
        .unwrap();
        let (sender, outbound) = mpsc::channel(1);
        let (_, tasks) = mpsc::channel(1);
        let pending = Arc::new(StdMutex::new(VecDeque::new()));
        let closed = CancellationToken::new();
        transport.live_generation.store(7, Ordering::Release);
        *transport.session.lock().await = Some(Session {
            generation: 7,
            heartbeat: None,
            sender,
            tasks,
            pending: Arc::clone(&pending),
            closed: closed.clone(),
        });
        let task = ClaimedTask {
            task: serde_json::from_value(serde_json::json!({
                "id": uuid::Uuid::now_v7(),
                "instance_id": uuid::Uuid::now_v7(),
                "block_id": "step",
                "handler_name": "noop",
                "claim_epoch": 1
            }))
            .unwrap(),
            session: 7,
        };
        Self {
            transport,
            outbound,
            pending,
            closed,
            task,
        }
    }

    fn heartbeat(&self) -> ClientPayload {
        ClientPayload::Heartbeat(proto::HeartbeatTaskRequest {
            task_id: self.task.task.id.to_string(),
            worker_id: "worker".into(),
            claim_epoch: self.task.task.claim_epoch,
        })
    }

    fn assert_retired(&self) {
        assert!(self.closed.is_cancelled());
        assert_eq!(self.transport.live_generation.load(Ordering::Acquire), 0);
        assert!(self.pending.lock().unwrap().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn ack_timeout_retires_session_and_releases_pending_mutations() {
    let mut session = TestSession::new().await;
    let mut mutation = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut mutation => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    assert_eq!(session.pending.lock().unwrap().len(), 1);
    assert_eq!(
        mutation.await,
        None,
        "unary fallback resolves the precise outcome"
    );
    session.assert_retired();
}

#[tokio::test(start_paused = true)]
async fn stream_send_is_bounded_by_the_ack_deadline() {
    let session = TestSession::new().await;
    session
        .transport
        .session
        .lock()
        .await
        .as_ref()
        .unwrap()
        .sender
        .try_send(frame(session.heartbeat()))
        .unwrap();
    let answer = tokio::time::timeout(
        ACK_TIMEOUT + Duration::from_secs(1),
        session
            .transport
            .via_stream(&session.task, "heartbeat", session.heartbeat()),
    )
    .await
    .expect("a full outbound queue must not block a lease operation forever");
    assert_eq!(answer, None);
    session.assert_retired();
}

#[tokio::test]
async fn cancelling_a_mutation_retires_its_ack_namespace() {
    let mut session = TestSession::new().await;
    let mut mutation = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut mutation => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    drop(mutation);
    session.assert_retired();
}

#[tokio::test]
async fn acknowledged_mutation_keeps_the_session_live() {
    let mut session = TestSession::new().await;
    let mut mutation = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut mutation => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    session
        .pending
        .lock()
        .unwrap()
        .pop_front()
        .unwrap()
        .reply
        .send(LeaseResponse::Accepted)
        .unwrap();
    assert_eq!(mutation.await, Some(LeaseResponse::Accepted));
    assert!(!session.closed.is_cancelled());
    assert_eq!(session.transport.live_generation.load(Ordering::Acquire), 7);
    assert!(session.pending.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn waiting_for_session_lock_cannot_block_unary_fallback_forever() {
    let session = TestSession::new().await;
    let _locked = session.transport.session.lock().await;
    let answer = tokio::time::timeout(
        ACK_TIMEOUT + Duration::from_secs(1),
        session
            .transport
            .via_stream(&session.task, "heartbeat", session.heartbeat()),
    )
    .await
    .expect("waiting for session state must have a deadline");
    assert_eq!(answer, None);
    assert!(session.pending.lock().unwrap().is_empty());
    // No frame was sent, so there is no abandoned ack to fence.
    assert!(!session.closed.is_cancelled());
}

#[tokio::test]
async fn cancelling_one_mutation_retries_all_waiters_in_the_old_session() {
    let mut session = TestSession::new().await;
    let mut first = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut first => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    let mut second = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut second => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    assert_eq!(session.pending.lock().unwrap().len(), 2);
    drop(first);
    assert_eq!(second.await, None);
    session.assert_retired();
}

#[tokio::test]
async fn retiring_an_old_session_preserves_a_new_generation() {
    let mut session = TestSession::new().await;
    let mut mutation = Box::pin(session.transport.via_stream(
        &session.task,
        "heartbeat",
        session.heartbeat(),
    ));
    tokio::select! {
        biased;
        answer = &mut mutation => panic!("mutation completed without an ack: {answer:?}"),
        message = session.outbound.recv() => assert!(message.is_some()),
    }
    session
        .transport
        .live_generation
        .store(8, Ordering::Release);
    drop(mutation);
    assert!(session.closed.is_cancelled());
    assert!(session.pending.lock().unwrap().is_empty());
    assert_eq!(session.transport.live_generation.load(Ordering::Acquire), 8);
}
