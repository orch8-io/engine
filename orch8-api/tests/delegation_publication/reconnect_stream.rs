//! Actual authenticated gRPC wire delivery, reconnect and cross-transport reads.
use super::*;
use super::{
    issued_context::{capabilities, verify_payloads},
    outcome::grpc_worker,
};
use orch8_engine::remote_worker::RemoteTask;
use orch8_grpc::proto::{
    self, worker_stream_client::Payload as ClientPayload,
    worker_stream_server::Payload as ServerPayload,
};
use std::time::Duration;
use tokio::sync::mpsc;

type Stream = (
    mpsc::Sender<proto::WorkerStreamClient>,
    tonic::Streaming<proto::WorkerStreamServer>,
);
async fn open(
    client: &mut orch8_grpc::proto::orch8_service_client::Orch8ServiceClient<
        tonic::transport::Channel,
    >,
    f: &Fixture,
    caps: &RuntimeCapabilities,
) -> Stream {
    let (sender, receiver) = mpsc::channel(4);
    sender
        .send(proto::WorkerStreamClient {
            payload: Some(ClientPayload::Open(proto::WorkerStreamOpen {
                worker_id: caps.runtime_id.to_string(),
                handler_names: vec!["orch8.delegation".into()],
                supported_features: vec!["task_delivery".into(), "runtime_capabilities".into()],
                max_in_flight: 1,
                protocol_version: orch8_grpc::WORKER_STREAM_PROTOCOL_VERSION,
                runtime_capabilities_json: serde_json::to_string(caps).unwrap(),
                tenant_id: f.owner.tenant_id.to_string(),
            })),
        })
        .await
        .unwrap();
    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
    request
        .metadata_mut()
        .insert("x-api-key", ROOT.parse().unwrap());
    request
        .metadata_mut()
        .insert("x-tenant-id", f.owner.tenant_id.as_str().parse().unwrap());
    let mut inbound = tokio::time::timeout(Duration::from_secs(5), client.worker_stream(request))
        .await
        .unwrap()
        .unwrap()
        .into_inner();
    let hello = inbound.message().await.unwrap().unwrap();
    assert!(matches!(hello.payload, Some(ServerPayload::Hello(_))));
    (sender, inbound)
}
async fn task(stream: &mut Stream) -> RemoteTask {
    let frame = tokio::time::timeout(Duration::from_secs(5), stream.1.message())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Some(ServerPayload::Task(frame)) = frame.payload else {
        panic!("expected task delivery");
    };
    assert!(frame.task_json.len() < 16_384);
    assert!(!frame.task_json.contains("GRPC_INPUT_CANARY"));
    let task: RemoteTask = serde_json::from_str(&frame.task_json).unwrap();
    assert_eq!(task.params, json!({}));
    assert_eq!(task.context, json!({}));
    task
}
async fn demand(stream: &Stream) {
    stream
        .0
        .send(proto::WorkerStreamClient {
            payload: Some(ClientPayload::Demand(proto::WorkerStreamDemand {
                capacity: 1,
            })),
        })
        .await
        .unwrap();
}
async fn delivery(b: TestBackend) {
    let server = spawn_test_server_on(b.storage.clone(), Some(ROOT)).await;
    let mut grpc = grpc_worker(&b).await;
    for mode in [Mode::Remote, Mode::Hosted, Mode::Inline] {
        let mut f = fixture(b.storage.as_ref(), &server, mode).await;
        f.request["input"] = json!({"private":"GRPC_INPUT_CANARY\0😺","wide":u64::MAX});
        let published = accepted(&server, &f.request).await;
        let caps = capabilities(&b, &f).await;
        let mut first = open(&mut grpc.client, &f, &caps).await;
        demand(&first).await;
        let issued = task(&mut first).await;
        issued
            .validate_delegation(&caps.runtime_id.to_string())
            .unwrap();
        let context = issued.delegation.unwrap();
        assert_eq!(json!(context.scope.task_id), published["mailbox_task_id"]);
        verify_payloads(&server, &context, &f.request["input"]).await;
        drop(first);
        let mut second = open(&mut grpc.client, &f, &caps).await;
        let recovered = task(&mut second).await;
        assert_eq!(recovered.claim_epoch, issued.claim_epoch);
        assert_eq!(recovered.delegation.as_ref(), Some(&context));
        recovered
            .validate_delegation(&caps.runtime_id.to_string())
            .unwrap();
        drop(second);
        let mut legacy = caps;
        legacy.delegation_protocol = None;
        let mut unsupported = open(&mut grpc.client, &f, &legacy).await;
        demand(&unsupported).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), unsupported.1.message())
                .await
                .is_err()
        );
        drop(unsupported);
    }
    drop(grpc.client);
    grpc.stop.cancel();
    grpc.server.await.unwrap();
}
orch8_storage::backend_test!(async fn grpc_stream_preserves_issued_context_across_reconnect_and_http_reads(b){delivery(b).await;});
