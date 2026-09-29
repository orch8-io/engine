//! Remote (hybrid) executor assembly: an `executor` node joined to a
//! managed engine with **no database of its own**
//! ([`EngineConfig::is_remote_executor`]).
//!
//! The process dials out only: a managed-control session (ping, reload,
//! drain) and the worker protocol (gRPC worker stream, or HTTP polling) to
//! the same engine. It listens on `api.http_addr` for health probes only —
//! no API, no gRPC listener. See `docs/HYBRID.md`.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use orch8_engine::credentials::LocalCredentials;
use orch8_engine::remote_executor::{
    ExecutorIdentity, HttpLeaseTransport, LeaseTransport, RemoteExecutor, RemoteExecutorParts,
    RemoteExecutorSettings, executor_registry, replica_runtime_id,
};
use orch8_engine::remote_worker::HttpLeaseClient;
use orch8_grpc::worker_client::{GrpcLeaseTransport, GrpcWorkerConfig, OpenError};
use orch8_types::config::{EngineConfig, ExecutorTransport};
use tokio_util::sync::CancellationToken;

use crate::managed_control::{self, ManagedControlConfig};

/// Bound on waiting for the managed-control session to flush its final
/// `draining` advertisement at shutdown.
const CONTROL_FLUSH: Duration = Duration::from_secs(3);

/// The worker HTTP API base: `[executor] api_url`, else the managed
/// endpoint + `/api/v1`.
fn api_base(config: &EngineConfig) -> String {
    if config.executor.api_url.trim().is_empty() {
        format!(
            "{}/api/v1",
            config.node.managed_control_endpoint.trim_end_matches('/')
        )
    } else {
        config.executor.api_url.trim_end_matches('/').to_owned()
    }
}

fn http_transport(
    config: &EngineConfig,
    identity: &ExecutorIdentity,
    ca_pem: Option<&[u8]>,
) -> anyhow::Result<HttpLeaseTransport> {
    // HTTP/1.1 keep-alive is enough for lease polling and lets TLS
    // front-ends route by ALPN (h2 → gRPC, http/1.1 → REST).
    let mut builder = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .http1_only();
    if let Some(pem) = ca_pem {
        let certs = reqwest::Certificate::from_pem_bundle(pem)
            .context("executor.ca_cert_path is not a PEM certificate bundle")?;
        builder = builder.tls_certs_only(certs);
    }
    let client = builder.build().context("build the executor HTTP client")?;
    let lease = HttpLeaseClient::new(
        client,
        &api_base(config),
        vec![
            (
                "x-api-key",
                config.node.managed_control_api_key.expose().to_owned(),
            ),
            ("x-tenant-id", config.node.managed_control_tenant_id.clone()),
        ],
        identity.worker_id(),
    );
    Ok(HttpLeaseTransport::new(
        lease,
        config.node.managed_control_tenant_id.clone(),
        env!("CARGO_PKG_VERSION").to_owned(),
    ))
}

/// Pick the transport: the gRPC worker stream, HTTP polling, or (`auto`)
/// the stream when the endpoint serves it and HTTP otherwise.
async fn select_transport(
    config: &EngineConfig,
    identity: &ExecutorIdentity,
    ca_pem: Option<&[u8]>,
    drain: CancellationToken,
) -> anyhow::Result<Arc<dyn LeaseTransport>> {
    let grpc = || {
        GrpcLeaseTransport::new(GrpcWorkerConfig {
            endpoint: config.node.managed_control_endpoint.trim().to_owned(),
            api_key: config.node.managed_control_api_key.clone(),
            tenant_id: config.node.managed_control_tenant_id.clone(),
            worker_id: identity.worker_id(),
            ca_pem: ca_pem.map(<[u8]>::to_vec),
            drain: drain.clone(),
        })
        .map_err(anyhow::Error::msg)
    };
    match config.executor.transport {
        ExecutorTransport::Grpc => Ok(Arc::new(grpc()?)),
        ExecutorTransport::Http => Ok(Arc::new(http_transport(config, identity, ca_pem)?)),
        ExecutorTransport::Auto => {
            let stream = grpc()?;
            let capabilities = identity.capabilities(false);
            match stream.probe(&identity.handlers, &capabilities).await {
                Ok(()) => return Ok(Arc::new(stream)),
                Err(error) => {
                    tracing::warn!(%error, "gRPC worker stream unavailable; trying HTTP polling");
                    let http = http_transport(config, identity, ca_pem)?;
                    match http.advertise(&capabilities).await {
                        Ok(()) => return Ok(Arc::new(http)),
                        Err(http_error) => {
                            tracing::warn!(error = %http_error, "HTTP worker API unavailable too");
                            if matches!(error, OpenError::Unsupported(_)) {
                                return Ok(Arc::new(http));
                            }
                        }
                    }
                }
            }
            // Neither answered yet (network down, engine starting): keep the
            // stream; claims retry with back-off.
            Ok(Arc::new(stream))
        }
    }
}

async fn health_server(
    addr: std::net::SocketAddr,
    shutdown: CancellationToken,
) -> anyhow::Result<tokio::task::JoinHandle<()>> {
    use axum::routing::get;
    let ready = shutdown.clone();
    let app = axum::Router::new()
        .route("/health/live", get(|| async { "ok" }))
        .route(
            "/health/ready",
            get(move || {
                let ready = ready.clone();
                async move {
                    if ready.is_cancelled() {
                        (http::StatusCode::SERVICE_UNAVAILABLE, "draining")
                    } else {
                        (http::StatusCode::OK, "ok")
                    }
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("Failed to bind the executor health listener")?;
    Ok(tokio::spawn(async move {
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(async move { shutdown.cancelled().await })
            .await;
    }))
}

/// Run a remote executor until SIGTERM/SIGINT or a managed `drain`.
///
/// # Errors
/// Invalid configuration (fails before any connection is made).
#[allow(clippy::too_many_lines)] // one linear startup sequence
pub(crate) async fn run(mut config: EngineConfig) -> anyhow::Result<()> {
    if let Err(errors) = config.validate() {
        bail!(
            "remote executor configuration is invalid:\n  - {}",
            errors.join("\n  - ")
        );
    }
    let http_addr: std::net::SocketAddr = config
        .api
        .http_addr
        .parse()
        .context("api.http_addr is invalid")?;
    if let Ok(raw) = std::env::var(orch8_engine::handlers::builtin::ALLOWED_INTERNAL_CIDRS_ENV) {
        orch8_engine::handlers::builtin::parse_cidr_list(&raw).map_err(|e| {
            anyhow::anyhow!(
                "{}: {e}",
                orch8_engine::handlers::builtin::ALLOWED_INTERNAL_CIDRS_ENV
            )
        })?;
    }
    let otel = crate::init_observability(&config)?;

    let ca_pem = if config.executor.ca_cert_path.trim().is_empty() {
        None
    } else {
        Some(
            std::fs::read(config.executor.ca_cert_path.trim())
                .with_context(|| format!("read {}", config.executor.ca_cert_path))?,
        )
    };
    let token_runtime = uuid::Uuid::parse_str(&config.node.managed_control_runtime_id)
        .context("invalid managed control runtime id")?;
    let worker_name = config.node.managed_control_worker_id.clone();

    let registry = executor_registry(&config.executor.handlers);
    let mut handlers: Vec<String> = registry
        .handler_names()
        .into_iter()
        .map(ToOwned::to_owned)
        .collect();
    handlers.sort();
    let credentials_dir = (!config.executor.credentials_dir.trim().is_empty())
        .then(|| std::path::PathBuf::from(config.executor.credentials_dir.trim()));
    let credentials = LocalCredentials::from_env(credentials_dir);
    let identity = ExecutorIdentity {
        runtime_id: replica_runtime_id(token_runtime, &worker_name),
        host: worker_name.clone(),
        tenant_id: config.node.managed_control_tenant_id.clone(),
        labels: config.node.labels.clone(),
        regions: if config.node.region.trim().is_empty() {
            Vec::new()
        } else {
            vec![config.node.region.trim().to_owned()]
        },
        handlers,
        credentials: credentials.names(),
    };
    identity
        .validate()
        .map_err(|e| anyhow::anyhow!("executor identity is invalid: {e}"))?;
    let vault = crate::federation_wiring::payload_vault_from_env()?;

    let shutdown = CancellationToken::new();
    crate::spawn_signal_handler(shutdown.clone())?;
    let health = health_server(http_addr, shutdown.clone()).await?;
    let control = managed_control::spawn(
        ManagedControlConfig {
            endpoint: config.node.managed_control_endpoint.clone(),
            api_key: config.node.managed_control_api_key.clone(),
            tenant_id: config.node.managed_control_tenant_id.clone(),
            worker_id: worker_name.clone(),
            runtime_id: orch8_types::continuity::RuntimeId::from_uuid(token_runtime),
            region: (!config.node.region.trim().is_empty()).then(|| config.node.region.clone()),
            kind: orch8_types::continuity::RuntimeKind::Server,
            ca_pem: ca_pem.clone(),
        },
        shutdown.clone(),
    );
    let transport =
        select_transport(&config, &identity, ca_pem.as_deref(), shutdown.clone()).await?;
    // The long-lived config no longer needs the secret.
    config.node.managed_control_api_key = orch8_types::SecretString::default();

    tracing::info!(
        endpoint = %config.node.managed_control_endpoint,
        tenant = %identity.tenant_id,
        worker = %worker_name,
        runtime_id = %identity.runtime_id,
        transport = transport.name(),
        vault = vault.is_some(),
        "remote executor joined (no local database)"
    );
    let scratch: Arc<dyn orch8_storage::StorageBackend> = Arc::new(
        orch8_storage::sqlite::SqliteStorage::in_memory()
            .await
            .context("create the executor scratch store")?,
    );
    let executor = RemoteExecutor::new(RemoteExecutorParts {
        identity,
        settings: RemoteExecutorSettings {
            max_concurrent_tasks: config.executor.max_concurrent_tasks,
            poll_interval: Duration::from_millis(config.executor.poll_interval_ms),
            drain_timeout: Duration::from_secs(config.executor.drain_timeout_secs),
            externalize_bytes: config.executor.externalize_bytes,
            max_heartbeat: (config.executor.heartbeat_secs > 0)
                .then(|| Duration::from_secs(config.executor.heartbeat_secs)),
            ..RemoteExecutorSettings::default()
        },
        transport,
        registry,
        credentials,
        vault,
        scratch,
    })
    .map_err(anyhow::Error::msg)?;
    let stats = executor.run(shutdown.clone()).await;
    shutdown.cancel();
    let _ = tokio::time::timeout(CONTROL_FLUSH, control).await;
    let _ = tokio::time::timeout(CONTROL_FLUSH, health).await;
    otel.shutdown().await;
    tracing::info!(?stats, "remote executor shutdown complete");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_base_defaults_to_the_managed_endpoint() {
        let mut config = EngineConfig::default();
        config.node.managed_control_endpoint = "https://engine.example.com/".into();
        assert_eq!(api_base(&config), "https://engine.example.com/api/v1");
        config.executor.api_url = "https://api.example.com/api/v1/".into();
        assert_eq!(api_base(&config), "https://api.example.com/api/v1");
    }
}
