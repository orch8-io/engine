//! E2E tests for `GET /instances/{id}/stream` — specifically the forwarding
//! of live `llm_delta` events from the in-process engine stream bus.
//!
//! The test harness does not run the engine tick loop, so instead of
//! executing a real streaming `llm_call` we publish to the bus directly
//! (exactly what the handler's `DeltaSink` does) and assert a connected SSE
//! client receives the events. The handler-side publication path is covered
//! by `orch8-engine`'s mock-SSE provider tests.

use std::time::Duration;

use orch8_api::test_harness::spawn_test_server;
use orch8_engine::stream_bus::{stream_bus, StreamEvent};
use orch8_types::ids::InstanceId;
use reqwest::StatusCode;
use serde_json::json;
use uuid::Uuid;

fn mk_sequence_body(id: Uuid) -> serde_json::Value {
    json!({
        "id": id,
        "tenant_id": "t1",
        "namespace": "ns1",
        "name": "stream-seq",
        "version": 1,
        "deprecated": false,
        "blocks": [
            {
                "type": "step",
                "id": "s1",
                "handler": "noop",
                "params": {},
                "cancellable": true
            }
        ],
        "interceptors": null,
        "created_at": chrono::Utc::now().to_rfc3339()
    })
}

/// Create a sequence and an instance of it; returns the instance id.
async fn create_instance(client: &reqwest::Client, base_url: &str) -> Uuid {
    let seq_id = Uuid::now_v7();
    let resp = client
        .post(format!("{base_url}/sequences"))
        .header("X-Tenant-Id", "t1")
        .json(&mk_sequence_body(seq_id))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);

    let resp = client
        .post(format!("{base_url}/instances"))
        .header("X-Tenant-Id", "t1")
        .json(&json!({
            "sequence_id": seq_id,
            "tenant_id": "t1",
            "namespace": "ns1",
            "context": { "data": {}, "config": {}, "audit": [] }
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created: serde_json::Value = resp.json().await.unwrap();
    created["id"].as_str().unwrap().parse().unwrap()
}

/// Read SSE chunks into `buf` until it contains `needle` (10s safety cap).
async fn read_until(resp: &mut reqwest::Response, buf: &mut String, needle: &str) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !buf.contains(needle) {
            let chunk = resp
                .chunk()
                .await
                .expect("SSE read failed")
                .expect("SSE stream ended before expected event");
            buf.push_str(&String::from_utf8_lossy(&chunk));
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {needle:?} in SSE stream; got: {buf}"));
}

#[tokio::test]
async fn stream_forwards_llm_delta_events_from_bus() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst_id = create_instance(&client, &srv.base_url).await;

    let mut resp = client
        .get(format!("{}/instances/{inst_id}/stream", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .query(&[("poll_ms", "100")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // The bus subscription is established before the response headers are
    // sent, but wait for the first poll-driven `state` event anyway so we
    // know the stream task is fully live.
    let mut buf = String::new();
    read_until(&mut resp, &mut buf, "event: state").await;

    // Publish exactly what a streaming llm_call's DeltaSink publishes.
    let instance_id = InstanceId::from_uuid(inst_id);
    for delta in ["Hel", "lo"] {
        stream_bus().publish(
            instance_id,
            StreamEvent::LlmDelta {
                block_id: "s1".to_string(),
                delta: delta.to_string(),
            },
        );
    }

    read_until(&mut resp, &mut buf, "event: llm_delta").await;
    read_until(
        &mut resp,
        &mut buf,
        r#"{"type":"llm_delta","block_id":"s1","delta":"Hel"}"#,
    )
    .await;
    read_until(
        &mut resp,
        &mut buf,
        r#"{"type":"llm_delta","block_id":"s1","delta":"lo"}"#,
    )
    .await;
}

#[tokio::test]
async fn stream_ignores_deltas_for_other_instances() {
    let srv = spawn_test_server().await;
    let client = reqwest::Client::new();
    let inst_id = create_instance(&client, &srv.base_url).await;

    let mut resp = client
        .get(format!("{}/instances/{inst_id}/stream", srv.base_url))
        .header("X-Tenant-Id", "t1")
        .query(&[("poll_ms", "100")])
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let mut buf = String::new();
    read_until(&mut resp, &mut buf, "event: state").await;

    // A delta for a DIFFERENT instance must not reach this client; one for
    // the watched instance (published after) must. Receiving the marker
    // proves the foreign delta was not interleaved before it.
    stream_bus().publish(
        InstanceId::new(),
        StreamEvent::LlmDelta {
            block_id: "other".to_string(),
            delta: "foreign".to_string(),
        },
    );
    stream_bus().publish(
        InstanceId::from_uuid(inst_id),
        StreamEvent::LlmDelta {
            block_id: "s1".to_string(),
            delta: "marker".to_string(),
        },
    );

    read_until(&mut resp, &mut buf, r#""delta":"marker""#).await;
    assert!(
        !buf.contains("foreign"),
        "delta for another instance leaked into this stream: {buf}"
    );
}
