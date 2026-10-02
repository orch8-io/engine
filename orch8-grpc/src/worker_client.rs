//! Client side of the negotiated gRPC worker stream, as a
//! [`LeaseTransport`] for the hybrid remote executor
//! (`orch8_engine::remote_executor`).
//!
//! The executor dials **out** to the engine and opens one
//! `Orch8Service.WorkerStream` session: the open frame carries its runtime
//! capability advertisement, `Demand` frames claim tasks through the
//! engine's capability predicate (so placed steps reach it), and completion,
//! failure, and heartbeat frames settle them. Nothing new is added to the
//! protocol:
//!
//! - a mutation for a task delivered by the live session goes over the
//!   stream; one from an earlier session (the stream was recycled or
//!   dropped) uses the unary `CompleteTask` / `FailTask` / `HeartbeatTask`
//!   RPCs, which fence on `(worker_id, claim_epoch)` just the same;
//! - `release` always uses the unary `ReleaseTask` RPC (the stream has no
//!   release frame);
//! - when the session ends, pending stream mutations are retried once over
//!   the unary RPCs so their precise status (stale epoch, task gone) is
//!   observed instead of guessed.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use orch8_engine::remote_executor::{Claimed, ClaimedTask, LeaseTransport};
use orch8_engine::remote_worker::{LeaseResponse, RemoteTask};
use orch8_types::SecretString;
use orch8_types::continuity::RuntimeCapabilities;
use orch8_types::worker::{WorkerCommand, WorkerCommandKind};
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Request, Status};
use tracing::{debug, info, warn};

use crate::proto;
use crate::proto::orch8_service_client::Orch8ServiceClient;
use crate::proto::worker_stream_client::Payload as ClientPayload;
use crate::proto::worker_stream_server::Payload as ServerPayload;

/// Features this client negotiates (all of them exist in protocol v2).
const FEATURES: &[&str] = &[
    "task_delivery",
    "completion",
    "failure",
    "heartbeat",
    "cancellation",
    "runtime_capabilities",
    "draining",
    "placement_commands",
];
/// Session window requested from the server. The executor's own slot limit
/// bounds real concurrency; a wide window keeps a mutation that had to use a
/// unary RPC from shrinking the session's capacity.
const MAX_IN_FLIGHT: u32 = 256;
/// How long a claim round waits for the first task after a demand.
const FIRST_TASK_WAIT: Duration = Duration::from_millis(300);
/// Gap after which a claim round stops collecting a burst of tasks.
const BURST_GAP: Duration = Duration::from_millis(25);
/// Bound on waiting for a stream acknowledgement.
const ACK_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on the open/hello handshake.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);

/// TLS settings for an outbound connection to a managed control plane:
/// the public web PKI roots, or *only* the given PEM bundle (private CA,
/// TLS-inspecting proxy).
#[must_use]
pub fn client_tls(ca_pem: Option<&[u8]>) -> ClientTlsConfig {
    match ca_pem {
        Some(pem) => ClientTlsConfig::new().ca_certificate(Certificate::from_pem(pem)),
        None => ClientTlsConfig::new().with_webpki_roots(),
    }
}

/// Add routing headers (e.g. `fly-force-instance-id`) to outbound gRPC
/// metadata. Names are lowercased; credentials are set separately and are
/// never overwritten by a routing header.
///
/// # Errors
/// A header name or value that is not valid gRPC ASCII metadata.
pub fn insert_routing_metadata(
    metadata: &mut tonic::metadata::MetadataMap,
    headers: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (name, value) in headers {
        let key =
            tonic::metadata::AsciiMetadataKey::from_bytes(name.to_ascii_lowercase().as_bytes())
                .map_err(|_| format!("routing header name `{name}` is not valid gRPC metadata"))?;
        if key.as_str() == "x-api-key" || key.as_str() == "x-tenant-id" {
            return Err(format!(
                "routing header `{name}` may not override credentials"
            ));
        }
        let value = MetadataValue::try_from(value.as_str())
            .map_err(|_| format!("routing header `{name}` has a non-ASCII value"))?;
        metadata.insert(key, value);
    }
    Ok(())
}

/// Connection settings of a [`GrpcLeaseTransport`].
#[derive(Clone)]
pub struct GrpcWorkerConfig {
    /// `https://…` gRPC endpoint of the engine.
    pub endpoint: String,
    pub api_key: SecretString,
    pub tenant_id: String,
    /// Lease identity (the executor's runtime id).
    pub worker_id: String,
    /// PEM bundle trusted instead of the public roots.
    pub ca_pem: Option<Vec<u8>>,
    /// Cancelled when the engine sends a `drain` command on this session.
    pub drain: CancellationToken,
    /// Routing headers sent as metadata on every call (join token `headers`).
    pub headers: BTreeMap<String, String>,
}

impl std::fmt::Debug for GrpcWorkerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcWorkerConfig")
            .field("endpoint", &self.endpoint)
            .field("tenant_id", &self.tenant_id)
            .field("worker_id", &self.worker_id)
            .field("headers", &self.headers)
            .finish_non_exhaustive()
    }
}

/// Why a session could not be opened.
#[derive(Debug)]
pub enum OpenError {
    /// The endpoint does not serve the worker stream (not gRPC, or an older
    /// engine): a caller in `auto` mode should use HTTP instead.
    Unsupported(String),
    /// Anything else (network, TLS, auth); retry later.
    Failed(String),
}

impl std::fmt::Display for OpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(message) => write!(f, "worker stream unsupported: {message}"),
            Self::Failed(message) => write!(f, "worker stream unavailable: {message}"),
        }
    }
}

struct Pending {
    operation: &'static str,
    task_id: String,
    reply: oneshot::Sender<LeaseResponse>,
}

struct Session {
    generation: u64,
    /// Heartbeat cadence the engine asked for in its hello.
    heartbeat: Option<Duration>,
    sender: mpsc::Sender<proto::WorkerStreamClient>,
    tasks: mpsc::Receiver<RemoteTask>,
    pending: Arc<StdMutex<VecDeque<Pending>>>,
    closed: CancellationToken,
}

/// [`LeaseTransport`] over the negotiated gRPC worker stream.
pub struct GrpcLeaseTransport {
    config: GrpcWorkerConfig,
    channel: Channel,
    session: tokio::sync::Mutex<Option<Session>>,
    live_generation: Arc<AtomicU64>,
    next_generation: AtomicU64,
}

fn frame(payload: ClientPayload) -> proto::WorkerStreamClient {
    proto::WorkerStreamClient {
        payload: Some(payload),
    }
}

fn status_is_unsupported(status: &Status) -> bool {
    matches!(status.code(), Code::Unimplemented)
        || (status.code() == Code::Unknown
            && ["content-type", "h2 protocol", "http2", "frame"]
                .iter()
                .any(|needle| status.message().to_ascii_lowercase().contains(needle)))
}

/// Unary-RPC status → lease outcome.
fn classify_status(status: &Status) -> LeaseResponse {
    match status.code() {
        Code::NotFound | Code::FailedPrecondition | Code::PermissionDenied => {
            LeaseResponse::LostOwnership
        }
        Code::Unimplemented => LeaseResponse::Unsupported,
        Code::InvalidArgument | Code::Unauthenticated | Code::OutOfRange => LeaseResponse::Rejected,
        _ => LeaseResponse::Retry,
    }
}

impl GrpcLeaseTransport {
    /// Build the transport. Connects lazily.
    ///
    /// # Errors
    /// An invalid endpoint URI or TLS configuration.
    pub fn new(config: GrpcWorkerConfig) -> Result<Self, String> {
        let mut endpoint = Endpoint::from_shared(config.endpoint.clone())
            .map_err(|e| format!("invalid gRPC endpoint: {e}"))?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true);
        if config.endpoint.starts_with("https://") {
            endpoint = endpoint
                .tls_config(client_tls(config.ca_pem.as_deref()))
                .map_err(|e| format!("invalid gRPC TLS configuration: {e}"))?;
        }
        Ok(Self {
            channel: endpoint.connect_lazy(),
            config,
            session: tokio::sync::Mutex::new(None),
            live_generation: Arc::new(AtomicU64::new(0)),
            next_generation: AtomicU64::new(1),
        })
    }

    fn request<T>(&self, message: T) -> Result<Request<T>, String> {
        let mut request = Request::new(message);
        insert_routing_metadata(request.metadata_mut(), &self.config.headers)?;
        request.metadata_mut().insert(
            "x-api-key",
            MetadataValue::try_from(self.config.api_key.expose())
                .map_err(|_| "API key is not ASCII".to_string())?,
        );
        request.metadata_mut().insert(
            "x-tenant-id",
            MetadataValue::try_from(self.config.tenant_id.as_str())
                .map_err(|_| "tenant id is not ASCII".to_string())?,
        );
        Ok(request)
    }

    fn client(&self) -> Orch8ServiceClient<Channel> {
        Orch8ServiceClient::new(self.channel.clone())
            .max_decoding_message_size(4 * 1024 * 1024)
            .max_encoding_message_size(4 * 1024 * 1024)
    }

    /// Open (or confirm) a live session; used by `auto` mode to decide
    /// between gRPC and HTTP.
    ///
    /// # Errors
    /// [`OpenError`].
    pub async fn probe(
        &self,
        handlers: &[String],
        capabilities: &RuntimeCapabilities,
    ) -> Result<(), OpenError> {
        let mut guard = self.session.lock().await;
        self.ensure_session(&mut guard, handlers, capabilities)
            .await
            .map(|_| ())
    }

    async fn ensure_session<'a>(
        &self,
        slot: &'a mut Option<Session>,
        handlers: &[String],
        capabilities: &RuntimeCapabilities,
    ) -> Result<&'a mut Session, OpenError> {
        if slot
            .as_ref()
            .is_some_and(|session| session.closed.is_cancelled())
        {
            *slot = None;
        }
        if slot.is_none() {
            *slot = Some(self.open(handlers, capabilities).await?);
        }
        slot.as_mut()
            .ok_or_else(|| OpenError::Failed("session unavailable".into()))
    }

    async fn open(
        &self,
        handlers: &[String],
        capabilities: &RuntimeCapabilities,
    ) -> Result<Session, OpenError> {
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let capabilities_json = serde_json::to_string(capabilities)
            .map_err(|e| OpenError::Failed(format!("encode capabilities: {e}")))?;
        let (sender, receiver) = mpsc::channel(64);
        sender
            .send(frame(ClientPayload::Open(proto::WorkerStreamOpen {
                worker_id: self.config.worker_id.clone(),
                handler_names: handlers.to_vec(),
                supported_features: FEATURES.iter().map(|f| (*f).to_owned()).collect(),
                max_in_flight: MAX_IN_FLIGHT,
                protocol_version: crate::WORKER_STREAM_PROTOCOL_VERSION,
                runtime_capabilities_json: capabilities_json,
                tenant_id: self.config.tenant_id.clone(),
            })))
            .await
            .map_err(|_| OpenError::Failed("queue open frame".into()))?;
        let request = self
            .request(tokio_stream::wrappers::ReceiverStream::new(receiver))
            .map_err(OpenError::Failed)?;
        let response =
            tokio::time::timeout(HANDSHAKE_TIMEOUT, self.client().worker_stream(request))
                .await
                .map_err(|_| OpenError::Failed("worker stream handshake timed out".into()))?;
        let mut inbound = match response {
            Ok(response) => response.into_inner(),
            Err(status) if status_is_unsupported(&status) => {
                return Err(OpenError::Unsupported(status.to_string()));
            }
            Err(status) => return Err(OpenError::Failed(status.to_string())),
        };
        let hello = tokio::time::timeout(HANDSHAKE_TIMEOUT, inbound.message())
            .await
            .map_err(|_| OpenError::Failed("no hello from the engine".into()))?;
        let heartbeat = match hello {
            Ok(Some(proto::WorkerStreamServer {
                payload: Some(ServerPayload::Hello(hello)),
            })) => {
                debug!(features = ?hello.negotiated_features, heartbeat = hello.heartbeat_interval_secs,
                    "worker stream negotiated");
                (hello.heartbeat_interval_secs > 0)
                    .then(|| Duration::from_secs(u64::from(hello.heartbeat_interval_secs)))
            }
            Ok(_) => return Err(OpenError::Failed("the engine did not say hello".into())),
            Err(status) if status_is_unsupported(&status) => {
                return Err(OpenError::Unsupported(status.to_string()));
            }
            Err(status) => return Err(OpenError::Failed(status.to_string())),
        };
        let (task_sender, tasks) = mpsc::channel(MAX_IN_FLIGHT as usize);
        let pending: Arc<StdMutex<VecDeque<Pending>>> = Arc::default();
        let closed = CancellationToken::new();
        self.live_generation.store(generation, Ordering::Release);
        tokio::spawn(read_session(
            inbound,
            task_sender,
            Arc::clone(&pending),
            sender.clone(),
            closed.clone(),
            self.config.drain.clone(),
            Arc::clone(&self.live_generation),
            generation,
        ));
        info!(generation, "worker stream session open");
        Ok(Session {
            generation,
            heartbeat,
            sender,
            tasks,
            pending,
            closed,
        })
    }

    /// Send a lease mutation over the live session when `task` belongs to
    /// it. `None` = not routable over the stream (use the unary RPC).
    async fn via_stream(
        &self,
        task: &ClaimedTask,
        operation: &'static str,
        payload: ClientPayload,
    ) -> Option<LeaseResponse> {
        if task.session == 0 || task.session != self.live_generation.load(Ordering::Acquire) {
            return None;
        }
        let (sender, pending) = {
            let guard = self.session.lock().await;
            let session = guard.as_ref()?;
            if session.generation != task.session || session.closed.is_cancelled() {
                return None;
            }
            (session.sender.clone(), Arc::clone(&session.pending))
        };
        let (reply, response) = oneshot::channel();
        pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_back(Pending {
                operation,
                task_id: task.task.id.to_string(),
                reply,
            });
        if sender.send(frame(payload)).await.is_err() {
            return None;
        }
        match tokio::time::timeout(ACK_TIMEOUT, response).await {
            Ok(Ok(LeaseResponse::Retry) | Err(_)) | Err(_) => None,
            Ok(Ok(answer)) => Some(answer),
        }
    }

    async fn unary<F, Fut>(&self, call: F) -> LeaseResponse
    where
        F: FnOnce(Orch8ServiceClient<Channel>) -> Fut,
        Fut: std::future::Future<Output = Result<tonic::Response<proto::Empty>, Status>>,
    {
        match call(self.client()).await {
            Ok(_) => LeaseResponse::Accepted,
            Err(status) => {
                debug!(%status, "unary lease call failed");
                classify_status(&status)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn read_session(
    mut inbound: tonic::Streaming<proto::WorkerStreamServer>,
    tasks: mpsc::Sender<RemoteTask>,
    pending: Arc<StdMutex<VecDeque<Pending>>>,
    sender: mpsc::Sender<proto::WorkerStreamClient>,
    closed: CancellationToken,
    drain: CancellationToken,
    live_generation: Arc<AtomicU64>,
    generation: u64,
) {
    let resolve = |operation: &str, task_id: &str, answer: LeaseResponse| {
        let mut queue = pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = queue
            .iter()
            .position(|p| p.operation == operation && p.task_id == task_id)
            && let Some(entry) = queue.remove(index)
        {
            let _ = entry.reply.send(answer);
        }
    };
    loop {
        let message = match inbound.message().await {
            Ok(Some(message)) => message,
            Ok(None) => {
                debug!(generation, "worker stream closed by the engine");
                break;
            }
            Err(status) => {
                warn!(generation, %status, "worker stream ended");
                break;
            }
        };
        match message.payload {
            Some(ServerPayload::Task(task)) => {
                match serde_json::from_str::<RemoteTask>(&task.task_json) {
                    Ok(task) => {
                        if tasks.send(task).await.is_err() {
                            break;
                        }
                    }
                    Err(error) => warn!(%error, "undecodable task on the worker stream"),
                }
            }
            Some(ServerPayload::Ack(ack)) => match ack.operation.as_str() {
                "complete" => resolve("complete", &ack.task_id, LeaseResponse::Accepted),
                "fail" => resolve("fail", &ack.task_id, LeaseResponse::Accepted),
                "heartbeat" => resolve("heartbeat", &ack.task_id, LeaseResponse::Accepted),
                _ => {}
            },
            Some(ServerPayload::Cancellation(cancellation)) => {
                resolve(
                    "heartbeat",
                    &cancellation.task_id,
                    LeaseResponse::LostOwnership,
                );
            }
            Some(ServerPayload::Command(command)) => {
                let Ok(command) = serde_json::from_str::<WorkerCommand>(&command.command_json)
                else {
                    continue;
                };
                if command.command == WorkerCommandKind::Place {
                    continue;
                }
                let _ = sender
                    .send(frame(ClientPayload::CommandAck(proto::WorkerCommandAck {
                        command_id: command.id.to_string(),
                    })))
                    .await;
                if command.command == WorkerCommandKind::Drain {
                    info!("drain command received on the worker stream");
                    drain.cancel();
                }
            }
            Some(ServerPayload::Hello(_)) | None => {}
        }
    }
    closed.cancel();
    let _ = live_generation.compare_exchange(generation, 0, Ordering::AcqRel, Ordering::Acquire);
    // Pending stream mutations get `Retry`: the caller re-sends them over
    // the unary RPCs, which report the precise outcome.
    let drained: Vec<Pending> = pending
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .drain(..)
        .collect();
    for entry in drained {
        let _ = entry.reply.send(LeaseResponse::Retry);
    }
}

#[async_trait]
impl LeaseTransport for GrpcLeaseTransport {
    fn name(&self) -> &'static str {
        "grpc"
    }

    async fn advertise(&self, capabilities: &RuntimeCapabilities) -> Result<(), String> {
        let json = serde_json::to_string(capabilities).map_err(|e| e.to_string())?;
        let guard = self.session.lock().await;
        match guard.as_ref() {
            Some(session) if !session.closed.is_cancelled() => session
                .sender
                .send(frame(ClientPayload::RuntimeHeartbeat(
                    proto::RuntimeHeartbeat {
                        runtime_capabilities_json: json,
                    },
                )))
                .await
                .map_err(|_| "worker stream closed".to_string()),
            // The next session's open frame carries a fresh advertisement.
            _ => Ok(()),
        }
    }

    async fn claim(
        &self,
        handlers: &[String],
        capacity: u32,
        capabilities: &RuntimeCapabilities,
    ) -> Result<Claimed, String> {
        let mut guard = self.session.lock().await;
        let session = self
            .ensure_session(&mut guard, handlers, capabilities)
            .await
            .map_err(|e| e.to_string())?;
        let generation = session.generation;
        let heartbeat = session.heartbeat;
        let mut tasks = Vec::new();
        // Tasks that arrived after the previous round returned come first.
        while tasks.len() < capacity as usize {
            match session.tasks.try_recv() {
                Ok(task) => tasks.push(ClaimedTask {
                    task,
                    session: generation,
                }),
                Err(_) => break,
            }
        }
        if tasks.is_empty() && capacity > 0 {
            session
                .sender
                .send(frame(ClientPayload::Demand(proto::WorkerStreamDemand {
                    capacity,
                })))
                .await
                .map_err(|_| "worker stream closed".to_string())?;
            let mut wait = FIRST_TASK_WAIT;
            while tasks.len() < capacity as usize {
                match tokio::time::timeout(wait, session.tasks.recv()).await {
                    Ok(Some(task)) => {
                        tasks.push(ClaimedTask {
                            task,
                            session: generation,
                        });
                        wait = BURST_GAP;
                    }
                    Ok(None) | Err(_) => break,
                }
            }
        }
        Ok(Claimed {
            tasks,
            poll_after: None,
            heartbeat,
        })
    }

    async fn heartbeat(&self, task: &ClaimedTask) -> LeaseResponse {
        let request = proto::HeartbeatTaskRequest {
            task_id: task.task.id.to_string(),
            worker_id: self.config.worker_id.clone(),
            claim_epoch: task.task.claim_epoch,
        };
        if let Some(answer) = self
            .via_stream(task, "heartbeat", ClientPayload::Heartbeat(request.clone()))
            .await
        {
            return answer;
        }
        let Ok(request) = self.request(request) else {
            return LeaseResponse::Rejected;
        };
        self.unary(|mut client| async move { client.heartbeat_task(request).await })
            .await
    }

    async fn complete(&self, task: &ClaimedTask, output: &Value) -> LeaseResponse {
        let Ok(output_json) = serde_json::to_string(output) else {
            return LeaseResponse::Rejected;
        };
        let request = proto::CompleteTaskRequest {
            task_id: task.task.id.to_string(),
            worker_id: self.config.worker_id.clone(),
            output_json,
            claim_epoch: task.task.claim_epoch,
        };
        if let Some(answer) = self
            .via_stream(task, "complete", ClientPayload::Complete(request.clone()))
            .await
        {
            return answer;
        }
        let Ok(request) = self.request(request) else {
            return LeaseResponse::Rejected;
        };
        self.unary(|mut client| async move { client.complete_task(request).await })
            .await
    }

    async fn fail(&self, task: &ClaimedTask, message: &str, retryable: bool) -> LeaseResponse {
        let request = proto::FailTaskRequest {
            task_id: task.task.id.to_string(),
            worker_id: self.config.worker_id.clone(),
            message: message.to_owned(),
            retryable,
            claim_epoch: task.task.claim_epoch,
        };
        if let Some(answer) = self
            .via_stream(task, "fail", ClientPayload::Fail(request.clone()))
            .await
        {
            return answer;
        }
        let Ok(request) = self.request(request) else {
            return LeaseResponse::Rejected;
        };
        self.unary(|mut client| async move { client.fail_task(request).await })
            .await
    }

    async fn release(&self, task: &ClaimedTask, started: bool) -> LeaseResponse {
        let Ok(request) = self.request(proto::ReleaseTaskRequest {
            task_id: task.task.id.to_string(),
            worker_id: self.config.worker_id.clone(),
            claim_epoch: task.task.claim_epoch,
            started,
        }) else {
            return LeaseResponse::Rejected;
        };
        self.unary(|mut client| async move { client.release_task(request).await })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unary_statuses_map_to_lease_outcomes() {
        assert_eq!(
            classify_status(&Status::failed_precondition("worker task lease changed")),
            LeaseResponse::LostOwnership
        );
        assert_eq!(
            classify_status(&Status::not_found("worker_task")),
            LeaseResponse::LostOwnership
        );
        assert_eq!(
            classify_status(&Status::unavailable("down")),
            LeaseResponse::Retry
        );
        assert_eq!(
            classify_status(&Status::invalid_argument("bad")),
            LeaseResponse::Rejected
        );
        assert_eq!(
            classify_status(&Status::unimplemented("old")),
            LeaseResponse::Unsupported
        );
    }

    #[test]
    fn routing_metadata_is_added_but_never_overrides_credentials() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        insert_routing_metadata(
            &mut metadata,
            &BTreeMap::from([("fly-force-instance-id".into(), "148e21ea7d9389".into())]),
        )
        .unwrap();
        assert_eq!(
            metadata.get("fly-force-instance-id").unwrap(),
            "148e21ea7d9389"
        );
        for name in ["x-api-key", "X-Tenant-Id"] {
            assert!(
                insert_routing_metadata(
                    &mut metadata,
                    &BTreeMap::from([(name.into(), "v".into())])
                )
                .is_err()
            );
        }
        assert!(
            insert_routing_metadata(
                &mut metadata,
                &BTreeMap::from([("bad name".into(), "v".into())])
            )
            .is_err()
        );
    }

    #[test]
    fn unsupported_endpoints_are_recognised() {
        assert!(status_is_unsupported(&Status::unimplemented("x")));
        assert!(status_is_unsupported(&Status::unknown(
            "grpc-status header missing, mapped from HTTP status code 404 content-type text/html"
        )));
        assert!(!status_is_unsupported(&Status::unavailable(
            "connect refused"
        )));
    }

    #[tokio::test]
    async fn transport_builds_lazily_for_https_and_rejects_bad_uris() {
        let config = GrpcWorkerConfig {
            endpoint: "https://control.example.com".into(),
            api_key: "k".into(),
            tenant_id: "acme".into(),
            worker_id: "w".into(),
            ca_pem: None,
            drain: CancellationToken::new(),
            headers: BTreeMap::new(),
        };
        assert!(GrpcLeaseTransport::new(config.clone()).is_ok());
        let bad = GrpcWorkerConfig {
            endpoint: "not a uri".into(),
            ..config
        };
        assert!(GrpcLeaseTransport::new(bad).is_err());
    }
}
