use super::*;
use crate::{
    credential::Credential,
    node::{Advertisement, NodeCapabilities},
};
use orch8_types::{
    continuity::RuntimeId,
    delegation_definition::{input::EncodedDelegationInput, protocol::DelegationResourceUsage},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

pub(in crate::node) fn payloads(
    worker: &str,
) -> (RemoteTask, DefinitionWireEnvelope, DelegationInputEnvelope) {
    let vector: Value = serde_json::from_str(include_str!(
        "../../../../contracts/vectors/delegation-definition-v1.json"
    ))
    .unwrap();
    let mut definition: DefinitionWireEnvelope =
        serde_json::from_value(vector["envelope"].clone()).unwrap();
    definition.scope.worker_id = worker.into();
    definition.scope.claim_epoch = 9_007_199_254_740_993;
    let graph = definition
        .verify(
            &definition.scope,
            &definition.reference,
            DefinitionGraphLimits::default(),
        )
        .unwrap();
    let input = EncodedDelegationInput::from_value(
        &json!({"private":"SDK_INPUT_CANARY\0😺","wide":u64::MAX}),
        InputLimits::default(),
    )
    .unwrap();
    let context = DelegationClaimContext {
        version: 1,
        scope: definition.scope.clone(),
        request_sha256: "a".repeat(64),
        expires_at: chrono::Utc::now() + chrono::Duration::minutes(1),
        definition: definition.reference.clone(),
        input: input.reference().clone(),
        usage: DelegationResourceUsage {
            definition_expanded_bytes: graph.expanded_bytes(),
            input_wire_version: 1,
            input_payload_bytes: input.reference().payload_bytes,
            input_expanded_bytes: input.reference().expanded_bytes,
        },
    };
    let envelope = DelegationInputEnvelope::new(
        context.scope.clone(),
        context.request_sha256.clone(),
        &input,
    )
    .unwrap();
    let task=serde_json::from_value(json!({"id":context.scope.task_id,"instance_id":uuid::Uuid::new_v4(),"block_id":"mailbox","handler_name":"orch8.delegation","params":{},"context":{},"claim_epoch":context.scope.claim_epoch,"delegation":context})).unwrap();
    (task, definition, envelope)
}
async fn setup() -> (
    Arc<NodeClient>,
    Arc<Mutex<(u16, Value, Value)>>,
    Arc<Mutex<Vec<String>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/api/v1/", listener.local_addr().unwrap());
    let runtime = RuntimeId::new();
    let client = NodeClient::new_unchecked(
        base,
        Credential::new("test-key".into()),
        "device".into(),
        runtime,
        Advertisement {
            handlers: vec!["orch8.delegation".into()],
            caps: NodeCapabilities::default(),
            draining: false,
        },
    );
    let (_, definition, input) = payloads(&client.worker_id());
    let replies = Arc::new(Mutex::new((
        200,
        serde_json::to_value(definition).unwrap(),
        serde_json::to_value(input).unwrap(),
    )));
    let paths = Arc::new(Mutex::new(Vec::new()));
    let state = replies.clone();
    let observed = paths.clone();
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buffer = [0u8; 1024];
            while !bytes.windows(4).any(|part| part == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    break;
                }
                bytes.extend_from_slice(&buffer[..count]);
                assert!(bytes.len() <= 8192);
            }
            let request = String::from_utf8(bytes).unwrap();
            let path = request.split_whitespace().nth(1).unwrap();
            observed.lock().unwrap().push(path.to_owned());
            let (status, body) = {
                let response = state.lock().unwrap();
                (
                    response.0,
                    if path.split('?').next().unwrap().ends_with("/definition") {
                        response.1.clone()
                    } else {
                        response.2.clone()
                    },
                )
            };
            let body = serde_json::to_vec(&body).unwrap();
            let head = format!(
                "HTTP/1.1 {status} Response\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(&body).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    (client, replies, paths, server)
}
#[tokio::test]
async fn payload_reads_use_fixed_claim_paths_and_independent_references() {
    let (client, _, paths, server) = setup().await;
    let (task, _, _) = payloads(&client.worker_id());
    client.claim_definition(&task).await.unwrap();
    let input = client.claim_input(&task).await.unwrap();
    assert_eq!(input.value()["wide"].as_u64(), Some(u64::MAX));
    let observed = paths.lock().unwrap();
    assert_eq!(observed.len(), 2);
    assert!(observed[0].contains(&format!("/workers/tasks/{}/definition?", task.id)));
    assert!(observed[1].contains("claim_epoch=9007199254740993"));
    drop(observed);
    server.abort();
}
#[tokio::test]
async fn mismatches_make_no_request_and_bad_bodies_or_statuses_do_not_leak_data() {
    let (client, replies, paths, server) = setup().await;
    let (task, _, _) = payloads(&client.worker_id());
    let mut foreign = task.clone();
    foreign.delegation.as_mut().unwrap().scope.worker_id = "other".into();
    assert!(client.claim_input(&foreign).await.is_err());
    assert!(paths.lock().unwrap().is_empty());
    replies.lock().unwrap().2["payload_base64"] = json!("SDK_PRIVATE_BAD_PAYLOAD");
    let error = client.claim_input(&task).await.unwrap_err();
    assert!(!format!("{error:?}").contains("SDK_PRIVATE_BAD_PAYLOAD"));
    {
        let mut response = replies.lock().unwrap();
        response.0 = 403;
        response.2 = json!({"private":"SDK_PRIVATE_HTTP_BODY"});
    }
    let error = client.claim_input(&task).await.unwrap_err();
    assert!(!format!("{error:?}").contains("SDK_PRIVATE_HTTP_BODY"));
    server.abort();
}
