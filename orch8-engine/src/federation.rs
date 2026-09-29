//! Explicit, opt-in federation transport (sender side).
//!
//! A `federate` step runs a sequence at a registered peer engine — another
//! organization (`relationship = "organization"`) or another cluster of the
//! same organization (`relationship = "cluster"`, i.e. a cross-cluster child
//! workflow) — and resumes when the remote run finishes.
//!
//! The step is shaped exactly like `wait_for_event`: it must declare
//! `wait_for_input`, so the engine parks the instance *before* the handler
//! runs. The pre-park hook ([`register_call_before_park`]) only writes a
//! durable [`FederationCall`] row — no network I/O happens on the scheduler
//! path. The background [`run_poller`] owns all transport: it delivers a
//! signed `start` envelope to the peer's gateway, polls `status`, and when the
//! remote run is terminal stores the (peer-minimized) result and delivers the
//! standard `human_input:<block>` resume signal. The gate opens and the
//! handler ([`handle_federate`]) returns the stored result as the block
//! output. If the local instance is cancelled or fails while the call is
//! outstanding, the poller propagates a signed `cancel` to the peer.
//!
//! Idempotency: the call id is derived from `(tenant, instance, block,
//! call_key)`, and the peer keys the remote instance on `(peer, call id)`
//! through the ordinary instance idempotency key, so poller retries, multiple
//! engine nodes, and crash recovery converge on one remote run.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, Utc};
use ed25519_dalek::SigningKey;
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_storage::StorageBackend;
use orch8_types::continuity::{DataClassification, ExecutionEpoch, RuntimeId};
use orch8_types::continuity_advanced::{FederationPeer, FederationPeerId};
use orch8_types::error::{StepError, StorageError};
use orch8_types::federation::{
    DISCLOSE_ALL, FEDERATION_INBOUND_PATH, FEDERATION_PROTOCOL_VERSION, FederationCall,
    FederationCallState, FederationIdentity, FederationParentRef, FederationPeerRecord,
    FederationRequest, FederationRequestKind, FederationResponse, PeerRelationship,
    RemoteCallState, SignedFederationMessage, call_continuity_id, derive_call_id,
    peer_id_from_public_key, public_key_fingerprint,
};
use orch8_types::ids::{BlockId, InstanceId, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::signal::{Signal, SignalType};

use crate::handlers::StepContext;

/// Registered handler name.
pub const FEDERATE_HANDLER: &str = "federate";
/// Envelope lifetime for transport messages (the primitive caps it at 300 s).
const ENVELOPE_TTL_SECONDS: i64 = 60;
/// Bound on the encoded response body accepted from a peer.
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
/// Calls processed per poller pass.
const POLL_BATCH: u32 = 100;
/// Cancel delivery attempts before the local side gives up on telling the
/// peer (the local call is still marked cancelled).
const MAX_CANCEL_ATTEMPTS: u32 = 10;

/// Public identity derived from an engine signing key.
#[must_use]
pub fn identity_for(signing_key: &SigningKey) -> FederationIdentity {
    let public_key = BASE64.encode(signing_key.verifying_key().to_bytes());
    FederationIdentity {
        peer_id: peer_id_from_public_key(&public_key)
            .expect("an ed25519 verifying key is always 32 bytes"),
        trust_root_sha256: public_key_fingerprint(&public_key).unwrap_or_default(),
        public_key,
    }
}

/// Sign `payload` for delivery to `receiver`.
///
/// `receiver_tenant` is the tenant *at the receiver* the message is bound to;
/// the receiver only accepts it if its registry entry for us names that
/// tenant as the owner.
#[must_use]
pub fn sign_message(
    signing_key: &SigningKey,
    sender: FederationPeerId,
    receiver: FederationPeerId,
    receiver_tenant: TenantId,
    call_id: uuid::Uuid,
    payload: &[u8],
    now: DateTime<Utc>,
) -> SignedFederationMessage {
    let envelope = crate::continuity_advanced::sign_federation_envelope(
        signing_key,
        crate::continuity_advanced::FederationSigningRequest {
            peer_id: sender,
            tenant_id: receiver_tenant,
            continuity_id: call_continuity_id(call_id),
            epoch: ExecutionEpoch::initial(),
            destination_runtime_id: RuntimeId::from_uuid(receiver.into_uuid()),
            payload,
            issued_at: now,
            expires_at: now + chrono::Duration::seconds(ENVELOPE_TTL_SECONDS),
        },
    );
    SignedFederationMessage {
        envelope,
        payload_base64: BASE64.encode(payload),
    }
}

/// Verify a message against a trust root and return its payload bytes.
///
/// # Errors
/// Returns a non-revealing reason when the payload encoding, signature,
/// freshness, tenant binding, or digest is invalid.
pub fn open_message(
    trust_root: &FederationPeer,
    message: &SignedFederationMessage,
    now: DateTime<Utc>,
) -> Result<Vec<u8>, String> {
    let payload = BASE64
        .decode(&message.payload_base64)
        .map_err(|_| "payload_base64 is invalid".to_owned())?;
    crate::continuity_advanced::verify_federation_envelope(
        trust_root,
        &message.envelope,
        &payload,
        now,
    )
    .map_err(|error| error.to_string())?;
    Ok(payload)
}

/// Transport outcome classification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The peer understood and refused the request (4xx). Retrying the same
    /// request cannot succeed.
    Rejected(String),
    /// Network failure, timeout, 5xx/429, or an unverifiable response.
    Unavailable(String),
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => write!(f, "peer rejected the request: {message}"),
            Self::Unavailable(message) => write!(f, "peer unavailable: {message}"),
        }
    }
}

/// Outbound HTTPS client. Holds the engine's federation signing key.
pub struct FederationClient {
    signing_key: SigningKey,
    identity: FederationIdentity,
    http: reqwest::Client,
}

impl std::fmt::Debug for FederationClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FederationClient")
            .field("identity", &self.identity)
            .finish_non_exhaustive()
    }
}

impl FederationClient {
    /// Build a client. `allow_http` permits plain-HTTP peer endpoints and is
    /// meant for loopback tests only.
    ///
    /// # Errors
    /// Returns the HTTP client construction error.
    pub fn new(signing_key: SigningKey, allow_http: bool) -> Result<Self, String> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(5))
            .https_only(!allow_http)
            .build()
            .map_err(|error| error.to_string())?;
        let identity = identity_for(&signing_key);
        Ok(Self {
            signing_key,
            identity,
            http,
        })
    }

    #[must_use]
    pub const fn identity(&self) -> &FederationIdentity {
        &self.identity
    }

    /// Deliver one signed request to `peer` and verify its signed response.
    ///
    /// # Errors
    /// See [`TransportError`].
    pub async fn send(
        &self,
        peer: &FederationPeerRecord,
        request: &FederationRequest,
    ) -> Result<FederationResponse, TransportError> {
        let payload = serde_json::to_vec(request)
            .map_err(|error| TransportError::Rejected(error.to_string()))?;
        let message = sign_message(
            &self.signing_key,
            self.identity.peer_id,
            peer.peer_id,
            peer.remote_tenant_id.clone(),
            request.call_id,
            &payload,
            Utc::now(),
        );
        let url = format!(
            "{}{FEDERATION_INBOUND_PATH}",
            peer.endpoint.trim_end_matches('/')
        );
        let response = self
            .http
            .post(&url)
            .json(&message)
            .send()
            .await
            .map_err(|error| TransportError::Unavailable(error.without_url().to_string()))?;
        let status = response.status();
        let body = crate::outbound::read_body_capped(response, MAX_RESPONSE_BYTES)
            .await
            .map_err(|error| TransportError::Unavailable(format!("{error:?}")))?;
        if !status.is_success() {
            let text: String = String::from_utf8_lossy(&body).chars().take(256).collect();
            return Err(
                if status.is_client_error() && status != reqwest::StatusCode::TOO_MANY_REQUESTS {
                    TransportError::Rejected(format!("HTTP {}: {text}", status.as_u16()))
                } else {
                    TransportError::Unavailable(format!("HTTP {}", status.as_u16()))
                },
            );
        }
        let signed: SignedFederationMessage = serde_json::from_slice(&body)
            .map_err(|_| TransportError::Unavailable("malformed peer response".into()))?;
        let payload =
            open_message(&peer.inbound_trust_root(), &signed, Utc::now()).map_err(|reason| {
                TransportError::Unavailable(format!("unverifiable response: {reason}"))
            })?;
        if signed.envelope.continuity_id != call_continuity_id(request.call_id) {
            return Err(TransportError::Unavailable(
                "response is bound to a different call".into(),
            ));
        }
        let response: FederationResponse = serde_json::from_slice(&payload)
            .map_err(|_| TransportError::Unavailable("malformed response payload".into()))?;
        if response.call_id != request.call_id || response.v != FEDERATION_PROTOCOL_VERSION {
            return Err(TransportError::Unavailable(
                "response call id or protocol version mismatch".into(),
            ));
        }
        Ok(response)
    }
}

/// Parsed `federate` params.
#[derive(Debug, Clone, PartialEq)]
struct FederateParams {
    peer: String,
    sequence: String,
    sequence_version: Option<i32>,
    input: Value,
    call_key: Option<String>,
}

fn parse_params(params: &Value) -> Result<FederateParams, String> {
    let text = |key: &str| {
        params
            .get(key)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    };
    let peer = text("peer").ok_or("`peer` (peer id or registered name) is required")?;
    let sequence = text("sequence").ok_or("`sequence` is required")?;
    let sequence_version = match params.get("version") {
        None | Some(Value::Null) => None,
        Some(value) => Some(
            value
                .as_i64()
                .and_then(|v| i32::try_from(v).ok())
                .ok_or("`version` must be an integer")?,
        ),
    };
    let input = match params.get("input") {
        None | Some(Value::Null) => json!({}),
        Some(value @ Value::Object(_)) => value.clone(),
        Some(_) => return Err("`input` must be an object".into()),
    };
    let call_key = match params.get("call_key") {
        None | Some(Value::Null) => None,
        Some(Value::String(key)) if key.len() <= 256 => Some(key.clone()),
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(_) => return Err("`call_key` must be a string of at most 256 bytes".into()),
    };
    Ok(FederateParams {
        peer,
        sequence,
        sequence_version,
        input,
        call_key,
    })
}

fn call_id_for(
    tenant: &TenantId,
    instance: InstanceId,
    block: &BlockId,
    key: Option<&str>,
) -> uuid::Uuid {
    match key {
        None => derive_call_id(tenant, instance, block),
        Some(key) => derive_call_id(
            tenant,
            instance,
            &BlockId::new(format!("{block}\u{0}{key}")),
        ),
    }
}

async fn resolve_peer(
    storage: &dyn StorageBackend,
    tenant: &TenantId,
    peer: &str,
) -> Result<Option<FederationPeerRecord>, StorageError> {
    if let Ok(uuid) = uuid::Uuid::parse_str(peer) {
        return storage
            .get_federation_peer(tenant, FederationPeerId::from_uuid(uuid))
            .await;
    }
    Ok(storage
        .list_federation_peers(tenant)
        .await?
        .into_iter()
        .find(|record| record.name == peer))
}

/// Pre-park hook for `federate` steps: persist the outbound call so the
/// poller can deliver it. Never performs network I/O and never fails the
/// park — a policy violation is recorded as an already-failed call whose
/// resume signal makes the handler fail the step with the reason.
pub async fn register_call_before_park(
    storage: &Arc<dyn StorageBackend>,
    instance: &TaskInstance,
    step_def: &orch8_types::sequence::StepDef,
) {
    if step_def.handler != FEDERATE_HANDLER {
        return;
    }
    let resolved = crate::template::resolve(&step_def.params, &instance.context, &json!({}))
        .unwrap_or_else(|_| step_def.params.clone());
    let now = Utc::now();
    let parsed = parse_params(&resolved);
    let call_key = parsed.as_ref().ok().and_then(|p| p.call_key.clone());
    let call_id = call_id_for(
        &instance.tenant_id,
        instance.id,
        &step_def.id,
        call_key.as_deref(),
    );
    let mut call = FederationCall {
        call_id,
        tenant_id: instance.tenant_id.clone(),
        instance_id: instance.id,
        block_id: step_def.id.clone(),
        peer_id: FederationPeerId::from_uuid(uuid::Uuid::nil()),
        sequence: String::new(),
        sequence_version: None,
        input: json!({}),
        withheld_sha256: Vec::new(),
        state: FederationCallState::Pending,
        remote_instance_id: None,
        result: None,
        error: None,
        attempts: 0,
        notified: false,
        next_poll_at: now,
        created_at: now,
        updated_at: now,
        version: 0,
    };
    match build_call(storage.as_ref(), instance, parsed, &mut call, now).await {
        Ok(()) => {}
        Err(reason) => {
            call.state = FederationCallState::Failed;
            call.error = Some(reason);
        }
    }
    match storage.create_federation_call(&call).await {
        Ok(true) => debug!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            call_id = %call_id,
            state = ?call.state,
            "federation call registered"
        ),
        Ok(false) => {}
        Err(error) => warn!(
            instance_id = %instance.id,
            block_id = %step_def.id,
            %error,
            "federate: failed to register the outbound call; the instance parks until its wait_for_input timeout"
        ),
    }
}

async fn build_call(
    storage: &dyn StorageBackend,
    instance: &TaskInstance,
    parsed: Result<FederateParams, String>,
    call: &mut FederationCall,
    now: DateTime<Utc>,
) -> Result<(), String> {
    let params = parsed.map_err(|reason| format!("federate: {reason}"))?;
    call.sequence.clone_from(&params.sequence);
    call.sequence_version = params.sequence_version;
    let peer = resolve_peer(storage, &instance.tenant_id, &params.peer)
        .await
        .map_err(|error| format!("federate: peer lookup failed: {error}"))?
        .ok_or_else(|| format!("federate: peer '{}' is not registered", params.peer))?;
    call.peer_id = peer.peer_id;
    if !peer.is_active(now) {
        return Err(format!(
            "federate: peer '{}' is revoked or expired",
            peer.name
        ));
    }
    if !peer.outbound_allows_sequence(&params.sequence) {
        return Err(format!(
            "federate: sequence '{}' is not in the outbound allowlist for peer '{}'",
            params.sequence, peer.name
        ));
    }
    let disclose_all = peer.relationship == PeerRelationship::Cluster
        && peer
            .outbound
            .disclosed_fields
            .iter()
            .any(|f| f == DISCLOSE_ALL);
    let allowed: BTreeSet<String> = peer.outbound.disclosed_fields.iter().cloned().collect();
    let minimized = crate::continuity_advanced::minimize_disclosure(
        &params.input,
        &allowed,
        if disclose_all {
            DataClassification::Public
        } else {
            DataClassification::Internal
        },
    );
    call.input = minimized.disclosed;
    call.withheld_sha256 = minimized.withheld_sha256;
    Ok(())
}

/// `federate` handler: runs once the gate opens and returns the stored result.
pub async fn handle_federate(ctx: StepContext) -> Result<Value, StepError> {
    if ctx.is_dry_run() {
        return Ok(json!({ "dry_run": true, "state": "skipped" }));
    }
    let key = parse_params(&ctx.params).ok().and_then(|p| p.call_key);
    let call_id = call_id_for(
        &ctx.tenant_id,
        ctx.instance_id,
        &ctx.block_id,
        key.as_deref(),
    );
    let call = ctx
        .storage
        .get_federation_call(&ctx.tenant_id, call_id)
        .await
        .map_err(|error| StepError::Retryable {
            message: format!("federate: call lookup failed: {error}"),
            details: None,
        })?
        .ok_or_else(|| StepError::Permanent {
            message: "federate: no outbound call is registered; a federate step must declare \
                      `wait_for_input` so the engine parks while the remote run executes"
                .into(),
            details: None,
        })?;
    let summary = json!({
        "call_id": call.call_id,
        "peer_id": call.peer_id,
        "sequence": call.sequence,
        "remote_instance_id": call.remote_instance_id,
        "withheld_fields": call.withheld_sha256.len(),
    });
    match call.state {
        FederationCallState::Completed => {
            let mut out = summary;
            out["state"] = json!("completed");
            out["outputs"] = call.result.unwrap_or_else(|| json!({}));
            Ok(out)
        }
        FederationCallState::Failed => Err(StepError::Permanent {
            message: call
                .error
                .unwrap_or_else(|| "federate: remote run failed".into()),
            details: Some(summary),
        }),
        FederationCallState::Cancelled => Err(StepError::Permanent {
            message: "federate: remote run was cancelled".into(),
            details: Some(summary),
        }),
        FederationCallState::Pending | FederationCallState::Running => Err(StepError::Retryable {
            message: "federate: remote run is still in progress".into(),
            details: Some(summary),
        }),
    }
}

/// Deliver the resume signal that opens the step's `wait_for_input` gate.
async fn resume_instance(
    storage: &dyn StorageBackend,
    call: &FederationCall,
) -> Result<(), StorageError> {
    let signal = Signal {
        id: uuid::Uuid::now_v7(),
        instance_id: call.instance_id,
        signal_type: SignalType::Custom(format!("human_input:{}", call.block_id)),
        payload: json!({ "value": "yes", "federation_call": call.call_id }),
        delivered: false,
        created_at: Utc::now(),
        delivered_at: None,
    };
    match storage.enqueue_signal_if_active(&signal).await {
        Ok(()) => {}
        Err(StorageError::TerminalTarget { .. } | StorageError::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(error),
    }
    if let Ok(Some(instance)) = storage.get_instance(call.instance_id).await
        && matches!(
            instance.state,
            InstanceState::Waiting | InstanceState::Scheduled
        )
    {
        let _ = storage
            .conditional_update_instance_state(
                call.instance_id,
                instance.state,
                instance.state,
                Some(Utc::now()),
            )
            .await;
    }
    Ok(())
}

fn next_poll_delay(call: &FederationCall, now: DateTime<Utc>) -> chrono::Duration {
    // Young calls are polled every second; long-running ones back off to 30 s.
    let age = (now - call.created_at).num_seconds().max(0);
    chrono::Duration::seconds((age / 10).clamp(1, 30))
}

fn failure_backoff(attempts: u32) -> chrono::Duration {
    chrono::Duration::seconds(1_i64 << attempts.min(6))
}

async fn commit(
    storage: &dyn StorageBackend,
    current: &FederationCall,
    mut next: FederationCall,
) -> Result<Option<FederationCall>, StorageError> {
    next.version = current.version + 1;
    next.updated_at = Utc::now();
    Ok(storage
        .cas_federation_call(current.version, &next)
        .await?
        .then_some(next))
}

/// Send a signed `cancel` for an outstanding call and compute its next state.
async fn propagate_cancel(
    client: &FederationClient,
    peer: &FederationPeerRecord,
    call: &FederationCall,
    now: DateTime<Utc>,
) -> FederationCall {
    let request = FederationRequest {
        v: FEDERATION_PROTOCOL_VERSION,
        kind: FederationRequestKind::Cancel,
        call_id: call.call_id,
        sequence: None,
        sequence_version: None,
        input: None,
        parent: None,
    };
    let mut next = call.clone();
    match client.send(peer, &request).await {
        Ok(_) | Err(TransportError::Rejected(_)) => {
            next.state = FederationCallState::Cancelled;
            next.notified = true;
            info!(call_id = %call.call_id, "federation cancel propagated to peer");
        }
        Err(TransportError::Unavailable(reason)) => {
            next.attempts = call.attempts.saturating_add(1);
            if next.attempts >= MAX_CANCEL_ATTEMPTS {
                warn!(call_id = %call.call_id, %reason, "giving up propagating federation cancel");
                next.state = FederationCallState::Cancelled;
                next.notified = true;
            } else {
                next.next_poll_at = now + failure_backoff(next.attempts);
            }
        }
    }
    next
}

/// Fold a verified peer response into the call's next state.
fn apply_response(
    next: &mut FederationCall,
    call: &FederationCall,
    response: FederationResponse,
    now: DateTime<Utc>,
) {
    next.attempts = 0;
    next.next_poll_at = now + next_poll_delay(call, now);
    match response.state {
        RemoteCallState::Unknown => {
            // The peer lost (or never had) the run: restart it.
            next.remote_instance_id = None;
            next.state = FederationCallState::Pending;
        }
        RemoteCallState::Running => {
            next.remote_instance_id = response.remote_instance_id;
            next.state = FederationCallState::Running;
        }
        RemoteCallState::Completed => {
            next.remote_instance_id = response.remote_instance_id;
            next.state = FederationCallState::Completed;
            next.result = Some(response.outputs.unwrap_or_else(|| json!({})));
            next.next_poll_at = now;
        }
        RemoteCallState::Failed => {
            next.remote_instance_id = response.remote_instance_id;
            next.state = FederationCallState::Failed;
            next.error = Some("federate: remote run failed".into());
            next.next_poll_at = now;
        }
        RemoteCallState::Cancelled => {
            next.remote_instance_id = response.remote_instance_id;
            next.state = FederationCallState::Cancelled;
            next.next_poll_at = now;
        }
    }
}

/// Advance one due call by at most one network round-trip.
async fn advance_call(
    storage: &dyn StorageBackend,
    client: &FederationClient,
    call: FederationCall,
    now: DateTime<Utc>,
) -> Result<(), StorageError> {
    if call.state.is_terminal() {
        resume_instance(storage, &call).await?;
        let mut next = call.clone();
        next.notified = true;
        commit(storage, &call, next).await?;
        return Ok(());
    }

    let parent_state = storage
        .get_instance(call.instance_id)
        .await?
        .map(|instance| instance.state);
    let peer = storage
        .get_federation_peer(&call.tenant_id, call.peer_id)
        .await?;
    let parent_gone = parent_state.is_none_or(InstanceState::is_terminal);

    let Some(peer) = peer.filter(|peer| peer.is_active(now)) else {
        let mut next = call.clone();
        next.state = FederationCallState::Failed;
        next.error =
            Some("federate: peer is no longer registered, or was revoked or expired".into());
        next.notified = parent_gone;
        commit(storage, &call, next).await?;
        return Ok(());
    };

    if parent_gone {
        // Cancellation propagates to the peer (which cascades to the remote
        // instance's own children through its normal cancel path).
        let next = propagate_cancel(client, &peer, &call, now).await;
        commit(storage, &call, next).await?;
        return Ok(());
    }

    let request = if call.remote_instance_id.is_none() {
        FederationRequest {
            v: FEDERATION_PROTOCOL_VERSION,
            kind: FederationRequestKind::Start,
            call_id: call.call_id,
            sequence: Some(call.sequence.clone()),
            sequence_version: call.sequence_version,
            input: Some(call.input.clone()),
            parent: (peer.relationship == PeerRelationship::Cluster).then(|| FederationParentRef {
                instance_id: call.instance_id,
                block_id: call.block_id.clone(),
            }),
        }
    } else {
        FederationRequest {
            v: FEDERATION_PROTOCOL_VERSION,
            kind: FederationRequestKind::Status,
            call_id: call.call_id,
            sequence: None,
            sequence_version: None,
            input: None,
            parent: None,
        }
    };
    let mut next = call.clone();
    match client.send(&peer, &request).await {
        Ok(response) => apply_response(&mut next, &call, response, now),
        Err(TransportError::Rejected(reason)) => {
            next.state = FederationCallState::Failed;
            next.error = Some(format!("federate: {reason}"));
            next.next_poll_at = now;
        }
        Err(TransportError::Unavailable(reason)) => {
            next.attempts = call.attempts.saturating_add(1);
            next.next_poll_at = now + failure_backoff(next.attempts);
            debug!(call_id = %call.call_id, attempts = next.attempts, %reason, "federation peer unavailable");
        }
    }
    if let Some(committed) = commit(storage, &call, next).await?
        && committed.state.is_terminal()
    {
        // Resume immediately instead of waiting for the next pass.
        resume_instance(storage, &committed).await?;
        let mut notified = committed.clone();
        notified.notified = true;
        commit(storage, &committed, notified).await?;
    }
    Ok(())
}

/// One poller pass. Returns the number of calls examined.
///
/// # Errors
/// Propagates the storage error from listing due calls; per-call failures
/// are logged and retried on the next pass.
pub async fn poll_once(
    storage: &dyn StorageBackend,
    client: &FederationClient,
) -> Result<usize, StorageError> {
    let now = Utc::now();
    let due = storage.list_due_federation_calls(now, POLL_BATCH).await?;
    let count = due.len();
    for call in due {
        let call_id = call.call_id;
        if let Err(error) = advance_call(storage, client, call, now).await {
            warn!(%call_id, %error, "federation call could not be advanced");
        }
    }
    Ok(count)
}

/// Background loop driving every outbound federation call.
pub async fn run_poller(
    storage: Arc<dyn StorageBackend>,
    client: Arc<FederationClient>,
    interval: Duration,
    cancel: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            () = cancel.cancelled() => break,
            _ = ticker.tick() => {
                if let Err(error) = poll_once(storage.as_ref(), &client).await {
                    warn!(%error, "federation poller pass failed");
                }
            }
        }
    }
    info!("federation poller stopped");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_require_peer_and_sequence_and_object_input() {
        assert!(parse_params(&json!({"sequence": "s"})).is_err());
        assert!(parse_params(&json!({"peer": "p"})).is_err());
        assert!(parse_params(&json!({"peer": "p", "sequence": "s", "input": [1]})).is_err());
        let parsed = parse_params(&json!({"peer": "p", "sequence": "s", "version": 3})).unwrap();
        assert_eq!(parsed.sequence_version, Some(3));
        assert_eq!(parsed.input, json!({}));
    }

    #[test]
    fn call_key_disambiguates_loop_iterations() {
        let tenant = TenantId::new("t").unwrap();
        let instance = InstanceId::new();
        let block = BlockId::new("call");
        assert_ne!(
            call_id_for(&tenant, instance, &block, Some("a")),
            call_id_for(&tenant, instance, &block, Some("b"))
        );
        assert_eq!(
            call_id_for(&tenant, instance, &block, None),
            derive_call_id(&tenant, instance, &block)
        );
    }

    #[test]
    fn signed_messages_verify_only_for_the_bound_tenant() {
        let key = SigningKey::from_bytes(&[9; 32]);
        let identity = identity_for(&key);
        let receiver = FederationPeerId::new();
        let tenant = TenantId::new("globex").unwrap();
        let call_id = uuid::Uuid::now_v7();
        let now = Utc::now();
        let message = sign_message(
            &key,
            identity.peer_id,
            receiver,
            tenant.clone(),
            call_id,
            b"{}",
            now,
        );
        let mut root = FederationPeer {
            id: identity.peer_id,
            name: "acme".into(),
            trust_root_sha256: identity.trust_root_sha256.clone(),
            public_key: identity.public_key.clone(),
            endpoint: "https://acme.invalid".into(),
            allowed_tenants: vec![tenant],
            revoked_at: None,
        };
        assert_eq!(open_message(&root, &message, now).unwrap(), b"{}");
        root.allowed_tenants = vec![TenantId::new("other").unwrap()];
        assert!(open_message(&root, &message, now).is_err());
    }

    #[test]
    fn poll_delay_grows_with_age_and_is_bounded() {
        let now = Utc::now();
        let mut call_time = now;
        let delay = |created: DateTime<Utc>| {
            let call = FederationCall {
                call_id: uuid::Uuid::nil(),
                tenant_id: TenantId::new("t").unwrap(),
                instance_id: InstanceId::new(),
                block_id: BlockId::new("b"),
                peer_id: FederationPeerId::new(),
                sequence: "s".into(),
                sequence_version: None,
                input: json!({}),
                withheld_sha256: vec![],
                state: FederationCallState::Running,
                remote_instance_id: None,
                result: None,
                error: None,
                attempts: 0,
                notified: false,
                next_poll_at: now,
                created_at: created,
                updated_at: now,
                version: 0,
            };
            next_poll_delay(&call, now).num_seconds()
        };
        assert_eq!(delay(call_time), 1);
        call_time = now - chrono::Duration::hours(1);
        assert_eq!(delay(call_time), 30);
        assert_eq!(failure_backoff(20).num_seconds(), 64);
    }
}
