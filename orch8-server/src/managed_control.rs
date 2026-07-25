//! Outbound, control-only managed-cloud session.

use std::time::Duration;

use anyhow::{Context, Result};
use orch8_grpc::proto::orch8_service_client::Orch8ServiceClient;
use orch8_grpc::proto::worker_stream_client::Payload as ClientPayload;
use orch8_grpc::proto::worker_stream_server::Payload as ServerPayload;
use orch8_grpc::proto::{RuntimeHeartbeat, WorkerCommandAck, WorkerStreamClient, WorkerStreamOpen};
use orch8_types::SecretString;
use orch8_types::continuity::{
    RuntimeCapabilities, RuntimeConnectivity, RuntimeId, RuntimeKind, RuntimeTrustLevel,
};
use orch8_types::worker::{WorkerCommand, WorkerCommandKind};
use tokio_util::sync::CancellationToken;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{ClientTlsConfig, Endpoint};

#[derive(Clone)]
pub(crate) struct ManagedControlConfig {
    pub endpoint: String,
    pub api_key: SecretString,
    pub tenant_id: String,
    pub worker_id: String,
    pub runtime_id: RuntimeId,
    pub kind: RuntimeKind,
}

fn client_frame(payload: ClientPayload) -> WorkerStreamClient {
    WorkerStreamClient {
        payload: Some(payload),
    }
}

fn safe_capabilities(config: &ManagedControlConfig, draining: bool) -> RuntimeCapabilities {
    let now = chrono::Utc::now();
    RuntimeCapabilities {
        runtime_id: config.runtime_id,
        kind: config.kind,
        trust: RuntimeTrustLevel::Unverified,
        handlers: vec!["managed-control".into()],
        plugins: Vec::new(),
        credentials: Vec::new(),
        regions: Vec::new(),
        hardware: Vec::new(),
        offline_capable: false,
        connectivity: Some(RuntimeConnectivity::Ethernet),
        battery_percent: None,
        estimated_cost_microunits: None,
        estimated_latency_ms: None,
        draining,
        capsule_signing_public_key: None,
        observed_at: now,
        expires_at: now + chrono::Duration::seconds(45),
    }
}

fn capabilities_json(config: &ManagedControlConfig, draining: bool) -> Result<String> {
    serde_json::to_string(&safe_capabilities(config, draining)).context("encode safe capabilities")
}

async fn run_session(config: &ManagedControlConfig, shutdown: &CancellationToken) -> Result<()> {
    let endpoint = Endpoint::from_shared(config.endpoint.clone())?
        .connect_timeout(Duration::from_secs(10))
        .tls_config(ClientTlsConfig::new().with_webpki_roots())?;
    let channel = endpoint
        .connect()
        .await
        .context("connect managed control")?;
    let mut client = Orch8ServiceClient::new(channel);
    let (sender, receiver) = tokio::sync::mpsc::channel(8);
    sender
        .send(client_frame(ClientPayload::Open(WorkerStreamOpen {
            worker_id: config.worker_id.clone(),
            handler_names: vec!["managed-control".into()],
            supported_features: vec![
                "task_delivery".into(),
                "runtime_capabilities".into(),
                "draining".into(),
                "placement_commands".into(),
            ],
            max_in_flight: 1,
            protocol_version: 1,
            runtime_capabilities_json: capabilities_json(config, false)?,
            tenant_id: config.tenant_id.clone(),
        })))
        .await
        .context("queue managed control open")?;
    let mut request = Request::new(tokio_stream::wrappers::ReceiverStream::new(receiver));
    request.metadata_mut().insert(
        "x-api-key",
        MetadataValue::try_from(config.api_key.expose()).context("managed API key is not ASCII")?,
    );
    request.metadata_mut().insert(
        "x-tenant-id",
        MetadataValue::try_from(config.tenant_id.as_str())
            .context("managed tenant id is not ASCII")?,
    );
    let mut inbound = client
        .worker_stream(request)
        .await
        .context("open managed control stream")?
        .into_inner();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(15));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            () = shutdown.cancelled() => return Ok(()),
            _ = heartbeat.tick() => {
                sender.send(client_frame(ClientPayload::RuntimeHeartbeat(RuntimeHeartbeat {
                    runtime_capabilities_json: capabilities_json(config, false)?,
                }))).await.context("send managed heartbeat")?;
            }
            message = inbound.message() => {
                let Some(message) = message.context("receive managed control frame")? else {
                    anyhow::bail!("managed control stream closed");
                };
                if let Some(ServerPayload::Command(frame)) = message.payload {
                    let command: WorkerCommand = serde_json::from_str(&frame.command_json)
                        .context("decode managed control command")?;
                    match command.command {
                        WorkerCommandKind::Ping | WorkerCommandKind::Reload => {
                            sender.send(client_frame(ClientPayload::CommandAck(WorkerCommandAck {
                                command_id: command.id.to_string(),
                            }))).await.context("ack managed control command")?;
                        }
                        WorkerCommandKind::Drain => {
                            sender.send(client_frame(ClientPayload::CommandAck(WorkerCommandAck {
                                command_id: command.id.to_string(),
                            }))).await.context("ack managed drain")?;
                            shutdown.cancel();
                        }
                        WorkerCommandKind::Place => {
                            // This tunnel deliberately carries no workload demand or data.
                            // Placement remains pending for a workload-capable executor channel.
                            tracing::warn!(command_id = %command.id, "managed control-only session refused placement payload");
                        }
                    }
                }
            }
        }
    }
}

pub(crate) fn spawn(
    config: ManagedControlConfig,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = 1u64;
        while !shutdown.is_cancelled() {
            match run_session(&config, &shutdown).await {
                Ok(()) => break,
                Err(error) => {
                    tracing::warn!(%error, retry_secs = backoff, "managed control session disconnected");
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(Duration::from_secs(backoff)) => {}
                    }
                    backoff = (backoff * 2).min(30);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_advertisement_contains_no_protected_workload_data() {
        let config = ManagedControlConfig {
            endpoint: "https://control.example.com".into(),
            api_key: "secret".into(),
            tenant_id: "acme".into(),
            worker_id: "edge-1".into(),
            runtime_id: RuntimeId::new(),
            kind: RuntimeKind::Edge,
        };
        let value = serde_json::to_value(safe_capabilities(&config, false)).unwrap();
        let rendered = value.to_string();
        assert_eq!(value["handlers"], serde_json::json!(["managed-control"]));
        assert!(value.get("credentials").is_none());
        for forbidden in [
            "context", "params", "payload", "output", "artifact", "secret",
        ] {
            assert!(!rendered.contains(forbidden));
        }
    }
}
