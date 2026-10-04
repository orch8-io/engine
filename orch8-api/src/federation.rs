//! Federation transport API (see `docs/FEDERATION.md`).
//!
//! - `GET  /federation/identity` — this engine's federation identity (peer id
//!   + public key) that other engines register.
//! - `GET|POST /federation/peers`, `GET|PUT|DELETE /federation/peers/{peer_id}`
//!   — the per-tenant trust registry. Writes require the root/admin key: a
//!   peer entry authorizes another organization to start sequences here and
//!   receive declared outputs, which is a platform-level trust decision.
//! - `GET /federation/calls/{call_id}` — outbound call status (tenant-scoped).
//! - `POST /api/v1/federation/inbound` — the transport endpoint. It is mounted
//!   *outside* API-key auth ([`inbound_routes`]); the ed25519 envelope from a
//!   registered, unexpired peer is the only credential it accepts.

use std::collections::BTreeSet;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use orch8_types::context::ExecutionContext;
use orch8_types::continuity::{ContinuityExecution, ExecutionEpoch, OwnershipState, RuntimeId};
use orch8_types::continuity_advanced::FederationPeerId;
use orch8_types::error::StorageError;
use orch8_types::federation::{
    DISCLOSE_ALL, FEDERATION_INBOUND_PATH, FEDERATION_PROTOCOL_VERSION, FederationCall,
    FederationIdentity, FederationPeerRecord, FederationRequest, FederationRequestKind,
    FederationResponse, PeerInboundPolicy, PeerOutboundPolicy, PeerRelationship, RemoteCallState,
    SignedFederationMessage, call_continuity_id, inbound_idempotency_key,
};
use orch8_types::ids::{BlockId, InstanceId, Namespace, TenantId};
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::signal::{Signal, SignalType};

use crate::AppState;
use crate::api_keys::require_admin;
use crate::auth::{OptionalAdmin, OptionalTenant};
use crate::error::ApiError;

/// Inbound transport bodies are bounded well below the global 10 MiB limit.
const MAX_INBOUND_BODY_BYTES: usize = 4 * 1024 * 1024;

static ALLOW_HTTP_OVERRIDE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// `ORCH8_FEDERATION_ALLOW_HTTP=1` permits `http://` peer endpoints. Loopback
/// tests only; production peers must be HTTPS.
#[must_use]
pub fn allow_http_peers() -> bool {
    ALLOW_HTTP_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed)
        || std::env::var("ORCH8_FEDERATION_ALLOW_HTTP")
            .is_ok_and(|value| matches!(value.as_str(), "1" | "true" | "yes"))
}

/// Process-wide equivalent of `ORCH8_FEDERATION_ALLOW_HTTP=1` for in-process
/// loopback test servers ([`crate::test_harness::spawn_federation_test_server`]).
pub fn allow_http_peers_for_loopback_tests() {
    ALLOW_HTTP_OVERRIDE.store(true, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/federation/identity", get(get_identity))
        .route("/federation/peers", get(list_peers).post(create_peer))
        .route(
            "/federation/peers/{peer_id}",
            get(get_peer).put(update_peer).delete(delete_peer),
        )
        .route("/federation/calls/{call_id}", get(get_call))
}

/// Signature-authenticated transport route. Merge it *outside* the API-key
/// and tenant middleware.
pub fn inbound_routes() -> Router<AppState> {
    Router::new()
        .route(FEDERATION_INBOUND_PATH, post(inbound))
        .layer(DefaultBodyLimit::max(MAX_INBOUND_BODY_BYTES))
}

fn signing_key(state: &AppState) -> Result<&ed25519_dalek::SigningKey, ApiError> {
    state
        .continuity_crypto
        .as_ref()
        .map(|crypto| &crypto.signing_key)
        .ok_or_else(|| {
            ApiError::Unavailable(
                "federation is disabled: configure the engine encryption key".into(),
            )
        })
}

#[derive(Debug, Deserialize, IntoParams)]
pub(crate) struct TenantQuery {
    /// Tenant (ignored when the caller is bound to a tenant by its key/header).
    #[serde(default)]
    tenant_id: Option<String>,
}

fn tenant_of(
    tenant_ctx: &OptionalTenant,
    admin: &OptionalAdmin,
    query: Option<&str>,
) -> Result<TenantId, ApiError> {
    if tenant_ctx.is_none() {
        require_admin(admin)?;
    }
    crate::auth::scoped_tenant_id(tenant_ctx, query)
        .ok_or_else(|| ApiError::InvalidArgument("tenant_id (or X-Tenant-Id) is required".into()))
}

#[utoipa::path(get, path = "/federation/identity", tag = "federation",
    responses(
        (status = 200, body = FederationIdentity),
        (status = 503, description = "No engine encryption key configured"),
    )
)]
pub(crate) async fn get_identity(
    State(state): State<AppState>,
) -> Result<Json<FederationIdentity>, ApiError> {
    Ok(Json(orch8_engine::federation::identity_for(signing_key(
        &state,
    )?)))
}

/// Create/replace body for a trust-registry entry.
#[derive(Debug, Deserialize, ToSchema)]
pub(crate) struct PeerRequest {
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Peer federation identity from the peer's `GET /federation/identity`.
    /// Must match `public_key`.
    pub peer_id: FederationPeerId,
    pub name: String,
    #[serde(default)]
    pub relationship: PeerRelationship,
    /// HTTPS base URL of the peer's gateway.
    pub endpoint: String,
    /// Base64 raw 32-byte ed25519 public key of the peer.
    pub public_key: String,
    /// Tenant at the peer this relationship binds to.
    pub remote_tenant_id: String,
    #[serde(default)]
    pub outbound: PeerOutboundPolicy,
    #[serde(default)]
    pub inbound: PeerInboundPolicy,
    #[serde(default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub revoked_at: Option<DateTime<Utc>>,
}

fn build_peer(
    req: PeerRequest,
    tenant: TenantId,
    created_at: DateTime<Utc>,
) -> Result<FederationPeerRecord, ApiError> {
    let remote_tenant_id = TenantId::new(req.remote_tenant_id)
        .map_err(|error| ApiError::InvalidArgument(format!("remote_tenant_id: {error}")))?;
    let record = FederationPeerRecord {
        tenant_id: tenant,
        peer_id: req.peer_id,
        name: req.name.trim().to_owned(),
        relationship: req.relationship,
        endpoint: req.endpoint.trim().trim_end_matches('/').to_owned(),
        public_key: req.public_key.trim().to_owned(),
        remote_tenant_id,
        outbound: req.outbound,
        inbound: req.inbound,
        expires_at: req.expires_at,
        revoked_at: req.revoked_at,
        created_at,
        updated_at: Utc::now(),
    };
    record
        .validate(allow_http_peers())
        .map_err(ApiError::InvalidArgument)?;
    Ok(record)
}

fn map_peer_storage(error: StorageError) -> ApiError {
    match error {
        StorageError::Conflict(_) => {
            ApiError::Conflict("a peer with this name already exists for the tenant".into())
        }
        other => ApiError::from_storage(other, "federation peer"),
    }
}

#[utoipa::path(get, path = "/federation/peers", tag = "federation",
    params(TenantQuery),
    responses((status = 200, body = [FederationPeerRecord]))
)]
pub(crate) async fn list_peers(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<FederationPeerRecord>>, ApiError> {
    let tenant = tenant_of(&tenant_ctx, &admin, query.tenant_id.as_deref())?;
    Ok(Json(
        state
            .storage
            .list_federation_peers(&tenant)
            .await
            .map_err(map_peer_storage)?,
    ))
}

#[utoipa::path(post, path = "/federation/peers", tag = "federation",
    request_body = PeerRequest,
    responses(
        (status = 201, body = FederationPeerRecord),
        (status = 400, description = "Invalid peer (identity/key mismatch, non-HTTPS endpoint, wildcard disclosure for an organization peer)"),
        (status = 403, description = "Requires the root/admin API key"),
        (status = 409, description = "Peer already registered"),
    )
)]
pub(crate) async fn create_peer(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Json(req): Json<PeerRequest>,
) -> Result<(StatusCode, Json<FederationPeerRecord>), ApiError> {
    require_admin(&admin)?;
    let tenant = tenant_of(&tenant_ctx, &admin, req.tenant_id.as_deref())?;
    if state
        .storage
        .get_federation_peer(&tenant, req.peer_id)
        .await
        .map_err(map_peer_storage)?
        .is_some()
    {
        return Err(ApiError::Conflict(
            "peer already registered; use PUT to update".into(),
        ));
    }
    let record = build_peer(req, tenant, Utc::now())?;
    state
        .storage
        .upsert_federation_peer(&record)
        .await
        .map_err(map_peer_storage)?;
    Ok((StatusCode::CREATED, Json(record)))
}

#[utoipa::path(get, path = "/federation/peers/{peer_id}", tag = "federation",
    params(("peer_id" = Uuid, Path, description = "Peer federation id"), TenantQuery),
    responses((status = 200, body = FederationPeerRecord), (status = 404))
)]
pub(crate) async fn get_peer(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(peer_id): Path<Uuid>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<FederationPeerRecord>, ApiError> {
    let tenant = tenant_of(&tenant_ctx, &admin, query.tenant_id.as_deref())?;
    state
        .storage
        .get_federation_peer(&tenant, FederationPeerId::from_uuid(peer_id))
        .await
        .map_err(map_peer_storage)?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("federation peer {peer_id}")))
}

#[utoipa::path(put, path = "/federation/peers/{peer_id}", tag = "federation",
    request_body = PeerRequest,
    params(("peer_id" = Uuid, Path, description = "Peer federation id")),
    responses(
        (status = 200, body = FederationPeerRecord),
        (status = 403, description = "Requires the root/admin API key"),
        (status = 404),
    )
)]
pub(crate) async fn update_peer(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(peer_id): Path<Uuid>,
    Json(req): Json<PeerRequest>,
) -> Result<Json<FederationPeerRecord>, ApiError> {
    require_admin(&admin)?;
    if req.peer_id.into_uuid() != peer_id {
        return Err(ApiError::InvalidArgument(
            "peer_id in body does not match the path".into(),
        ));
    }
    let tenant = tenant_of(&tenant_ctx, &admin, req.tenant_id.as_deref())?;
    let existing = state
        .storage
        .get_federation_peer(&tenant, req.peer_id)
        .await
        .map_err(map_peer_storage)?
        .ok_or_else(|| ApiError::NotFound(format!("federation peer {peer_id}")))?;
    let record = build_peer(req, tenant, existing.created_at)?;
    state
        .storage
        .upsert_federation_peer(&record)
        .await
        .map_err(map_peer_storage)?;
    Ok(Json(record))
}

#[utoipa::path(delete, path = "/federation/peers/{peer_id}", tag = "federation",
    params(("peer_id" = Uuid, Path, description = "Peer federation id"), TenantQuery),
    responses(
        (status = 204),
        (status = 403, description = "Requires the root/admin API key"),
        (status = 404),
    )
)]
pub(crate) async fn delete_peer(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(peer_id): Path<Uuid>,
    Query(query): Query<TenantQuery>,
) -> Result<StatusCode, ApiError> {
    require_admin(&admin)?;
    let tenant = tenant_of(&tenant_ctx, &admin, query.tenant_id.as_deref())?;
    if state
        .storage
        .delete_federation_peer(&tenant, FederationPeerId::from_uuid(peer_id))
        .await
        .map_err(map_peer_storage)?
    {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError::NotFound(format!("federation peer {peer_id}")))
    }
}

#[utoipa::path(get, path = "/federation/calls/{call_id}", tag = "federation",
    params(("call_id" = Uuid, Path, description = "Outbound call id"), TenantQuery),
    responses((status = 200, body = FederationCall), (status = 404))
)]
pub(crate) async fn get_call(
    State(state): State<AppState>,
    admin: OptionalAdmin,
    tenant_ctx: OptionalTenant,
    Path(call_id): Path<Uuid>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<FederationCall>, ApiError> {
    let tenant = tenant_of(&tenant_ctx, &admin, query.tenant_id.as_deref())?;
    state
        .storage
        .get_federation_call(&tenant, call_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "federation call"))?
        .map(Json)
        .ok_or_else(|| ApiError::NotFound(format!("federation call {call_id}")))
}

/// Uniform refusal: never tells an unauthenticated caller *why*.
fn denied() -> ApiError {
    ApiError::Forbidden("federation request denied".into())
}

#[utoipa::path(post, path = "/federation/inbound", tag = "federation",
    request_body = SignedFederationMessage,
    responses(
        (status = 200, description = "Signed response envelope", body = SignedFederationMessage),
        (status = 403, description = "Unknown/revoked/expired peer, bad signature, stale envelope, or policy denial"),
        (status = 404, description = "Allowed sequence is not deployed"),
        (status = 503, description = "Federation disabled (no engine encryption key)"),
    ),
    security(())
)]
pub(crate) async fn inbound(
    State(state): State<AppState>,
    Json(message): Json<SignedFederationMessage>,
) -> Result<Json<SignedFederationMessage>, ApiError> {
    let key = signing_key(&state)?.clone();
    let now = Utc::now();
    let tenant = message.envelope.tenant_id.clone();
    let peer = state
        .storage
        .get_federation_peer(&tenant, message.envelope.peer_id)
        .await
        .map_err(|error| ApiError::from_storage(error, "federation peer"))?
        .filter(|peer| peer.is_active(now))
        .ok_or_else(denied)?;
    let payload = orch8_engine::federation::open_message(&peer.inbound_trust_root(), &message, now)
        .map_err(|reason| {
            tracing::warn!(peer_id = %peer.peer_id, %reason, "federation envelope rejected");
            denied()
        })?;
    let request: FederationRequest = serde_json::from_slice(&payload)
        .map_err(|_| ApiError::InvalidArgument("malformed federation request".into()))?;
    if request.v != FEDERATION_PROTOCOL_VERSION
        || message.envelope.continuity_id != call_continuity_id(request.call_id)
    {
        return Err(ApiError::InvalidArgument(
            "unsupported protocol version or envelope/call mismatch".into(),
        ));
    }

    let idempotency_key = inbound_idempotency_key(peer.peer_id, request.call_id);
    let mut instance = state
        .storage
        .find_by_idempotency_key(&tenant, &idempotency_key)
        .await
        .map_err(|error| ApiError::from_storage(error, "instance"))?;

    match request.kind {
        FederationRequestKind::Start if instance.is_none() => {
            instance = Some(start_instance(&state, &peer, &request, &idempotency_key).await?);
        }
        FederationRequestKind::Cancel => {
            if let Some(existing) = instance.as_ref()
                && !existing.state.is_terminal()
            {
                let signal = Signal {
                    id: Uuid::now_v7(),
                    instance_id: existing.id,
                    signal_type: SignalType::Cancel,
                    payload: serde_json::json!({ "reason": "cancelled by federation peer" }),
                    delivered: false,
                    created_at: now,
                    delivered_at: None,
                };
                match state.storage.enqueue_signal_if_active(&signal).await {
                    Ok(()) | Err(StorageError::TerminalTarget { .. }) => {}
                    Err(error) => return Err(ApiError::from_storage(error, "signal")),
                }
                if matches!(
                    existing.state,
                    InstanceState::Waiting | InstanceState::Scheduled
                ) {
                    let _ = state
                        .storage
                        .conditional_update_instance_state(
                            existing.id,
                            existing.state,
                            existing.state,
                            Some(now),
                        )
                        .await;
                }
            }
        }
        FederationRequestKind::Start | FederationRequestKind::Status => {}
    }

    if let Some(instance) = instance.as_ref() {
        record_receipt(&state, &message, instance).await;
    }
    let response = build_response(&state, &peer, request.call_id, instance.as_ref()).await?;
    let response_bytes =
        serde_json::to_vec(&response).map_err(|error| ApiError::Internal(error.to_string()))?;
    let identity = orch8_engine::federation::identity_for(&key);
    Ok(Json(orch8_engine::federation::sign_message(
        &key,
        identity.peer_id,
        peer.peer_id,
        peer.remote_tenant_id.clone(),
        request.call_id,
        &response_bytes,
        Utc::now(),
    )))
}

/// Refuse sequences that reach handlers outside the peer's handler
/// allowlist. Sub-sequences cannot be inspected statically here, so they
/// are refused whenever an allowlist is set.
fn handlers_allowed(
    policy: &PeerInboundPolicy,
    sequence: &orch8_types::sequence::SequenceDefinition,
) -> bool {
    let Some(allowed) = policy.handlers.as_ref() else {
        return true;
    };
    let allowed: BTreeSet<&str> = allowed.iter().map(String::as_str).collect();
    let blocks = serde_json::to_value(&sequence.blocks).unwrap_or_default();
    !contains_sub_sequence(&blocks)
        && sequence
            .handler_names()
            .iter()
            .all(|handler| allowed.contains(handler.as_str()))
}

fn contains_sub_sequence(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("type").and_then(serde_json::Value::as_str) == Some("sub_sequence")
                || map.values().any(contains_sub_sequence)
        }
        serde_json::Value::Array(items) => items.iter().any(contains_sub_sequence),
        _ => false,
    }
}

/// Bind a federated instance to its call's continuity scope so envelope
/// receipts (replay evidence) and effect receipts share it.
async fn bind_call_scope(
    state: &AppState,
    peer: &FederationPeerRecord,
    call_id: Uuid,
    instance: &TaskInstance,
    now: DateTime<Utc>,
) {
    let execution = ContinuityExecution {
        continuity_id: call_continuity_id(call_id),
        tenant_id: instance.tenant_id.clone(),
        current_instance_id: instance.id,
        owner_runtime_id: RuntimeId::from_uuid(peer.peer_id.into_uuid()),
        epoch: ExecutionEpoch::initial(),
        state: OwnershipState::Owned,
        updated_at: now,
    };
    if let Err(error) = state.storage.ensure_continuity_execution(&execution).await {
        tracing::warn!(instance_id = %instance.id, %error, "federation continuity scope not recorded");
    }
}

/// Metadata stamped on a federated instance. Parent linkage is honoured
/// only within the same organization.
fn federation_metadata(
    peer: &FederationPeerRecord,
    request: &FederationRequest,
) -> serde_json::Value {
    let mut meta = serde_json::json!({
        "peer_id": peer.peer_id,
        "peer_name": peer.name,
        "call_id": request.call_id,
        "relationship": peer.relationship,
    });
    if peer.relationship == PeerRelationship::Cluster
        && let Some(parent) = &request.parent
    {
        meta["parent"] = serde_json::json!(parent);
    }
    meta
}

async fn start_instance(
    state: &AppState,
    peer: &FederationPeerRecord,
    request: &FederationRequest,
    idempotency_key: &str,
) -> Result<TaskInstance, ApiError> {
    let tenant = peer.tenant_id.clone();
    let sequence_name = request
        .sequence
        .as_deref()
        .ok_or_else(|| ApiError::InvalidArgument("start requires a sequence".into()))?;
    if !peer.inbound_allows_sequence(sequence_name) {
        tracing::warn!(peer_id = %peer.peer_id, sequence = sequence_name, "federation start outside the inbound allowlist");
        return Err(denied());
    }
    let sequence = state
        .storage
        .get_sequence_by_name(
            &tenant,
            &Namespace::new("default"),
            sequence_name,
            request.sequence_version,
        )
        .await
        .map_err(|error| ApiError::from_storage(error, "sequence"))?
        .ok_or_else(|| ApiError::NotFound(format!("sequence {sequence_name}")))?;
    if !handlers_allowed(&peer.inbound, &sequence) {
        tracing::warn!(peer_id = %peer.peer_id, sequence = sequence_name, "federation start reaches a handler outside the inbound allowlist");
        return Err(denied());
    }
    let input = request
        .input
        .clone()
        .unwrap_or_else(|| serde_json::json!({}));
    if !input.is_object() {
        return Err(ApiError::InvalidArgument("input must be an object".into()));
    }
    crate::input_schema::validate_input(sequence.input_schema.as_ref(), &input)?;
    let context = ExecutionContext {
        data: input,
        ..ExecutionContext::default()
    };
    context.check_size(state.max_context_bytes)?;
    let plan = crate::entitlements::admit_instances(
        state,
        &tenant,
        std::slice::from_ref(&sequence.namespace),
        1,
        context.serialized_size(),
    )?;
    let federation_meta = federation_metadata(peer, request);
    let now = Utc::now();
    let instance = TaskInstance {
        sub_tenant: None,
        id: InstanceId::new(),
        sequence_id: sequence.id,
        tenant_id: tenant.clone(),
        namespace: sequence.namespace.clone(),
        state: InstanceState::Scheduled,
        next_fire_at: Some(now),
        priority: Priority::default(),
        timezone: "UTC".into(),
        metadata: serde_json::json!({ "federation": federation_meta }),
        context,
        concurrency_key: None,
        max_concurrency: None,
        idempotency_key: Some(idempotency_key.to_owned()),
        session_id: None,
        parent_instance_id: None,
        budget: None,
        created_at: now,
        updated_at: now,
    };
    match state
        .storage
        .create_instance_admitted(&instance, plan.max_active_instances)
        .await
    {
        Ok(()) => {
            bind_call_scope(state, peer, request.call_id, &instance, now).await;
            tracing::info!(
                instance_id = %instance.id,
                peer_id = %peer.peer_id,
                call_id = %request.call_id,
                sequence = sequence_name,
                "federated instance started"
            );
            Ok(instance)
        }
        Err(StorageError::Conflict(_)) => state
            .storage
            .find_by_idempotency_key(&tenant, idempotency_key)
            .await
            .map_err(|error| ApiError::from_storage(error, "instance"))?
            .ok_or_else(|| ApiError::Conflict("concurrent federated start".into())),
        Err(error) => Err(ApiError::from_storage(error, "instance")),
    }
}

/// Persist the envelope receipt (the existing replay-evidence table). A
/// repeated envelope is tolerated: every operation is idempotent on the
/// call id, and the receipt only ever records the first acceptance.
async fn record_receipt(
    state: &AppState,
    message: &SignedFederationMessage,
    instance: &TaskInstance,
) {
    let Ok(Some(execution)) = state
        .storage
        .get_continuity_execution_by_instance(&instance.tenant_id, instance.id)
        .await
    else {
        return;
    };
    if execution.continuity_id != message.envelope.continuity_id {
        return;
    }
    let Ok(encoded) = serde_json::to_vec(&message.envelope) else {
        return;
    };
    let digest = {
        use sha2::{Digest, Sha256};
        use std::fmt::Write as _;
        Sha256::digest(&encoded)
            .iter()
            .fold(String::with_capacity(64), |mut out, byte| {
                let _ = write!(out, "{byte:02x}");
                out
            })
    };
    match state
        .storage
        .accept_federation_message(&message.envelope, &digest, Utc::now())
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            tracing::debug!(message_id = %message.envelope.id, "federation envelope replayed; answered idempotently");
        }
        Err(error) => {
            tracing::warn!(%error, "federation receipt not recorded");
        }
    }
}

async fn build_response(
    state: &AppState,
    peer: &FederationPeerRecord,
    call_id: Uuid,
    instance: Option<&TaskInstance>,
) -> Result<FederationResponse, ApiError> {
    let Some(instance) = instance else {
        return Ok(FederationResponse {
            v: FEDERATION_PROTOCOL_VERSION,
            call_id,
            state: RemoteCallState::Unknown,
            remote_instance_id: None,
            outputs: None,
        });
    };
    let remote_state = match instance.state {
        InstanceState::Completed => RemoteCallState::Completed,
        InstanceState::Failed => RemoteCallState::Failed,
        InstanceState::Cancelled => RemoteCallState::Cancelled,
        _ => RemoteCallState::Running,
    };
    let outputs = if remote_state == RemoteCallState::Completed {
        Some(returned_outputs(state, peer, instance.id).await?)
    } else {
        None
    };
    Ok(FederationResponse {
        v: FEDERATION_PROTOCOL_VERSION,
        call_id,
        state: remote_state,
        remote_instance_id: Some(instance.id.into_uuid()),
        outputs,
    })
}

/// Only block outputs named in the peer's `returned_outputs` leave.
async fn returned_outputs(
    state: &AppState,
    peer: &FederationPeerRecord,
    instance_id: InstanceId,
) -> Result<serde_json::Value, ApiError> {
    let wildcard = peer.relationship == PeerRelationship::Cluster
        && peer
            .inbound
            .returned_outputs
            .iter()
            .any(|b| b == DISCLOSE_ALL);
    let block_ids: Vec<BlockId> = if wildcard {
        state
            .storage
            .get_all_outputs(instance_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "outputs"))?
            .into_iter()
            .map(|output| output.block_id)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    } else {
        peer.inbound
            .returned_outputs
            .iter()
            .map(|id| BlockId::new(id.clone()))
            .collect()
    };
    let mut out = serde_json::Map::new();
    for block_id in block_ids {
        let Some(output) = state
            .storage
            .get_block_output(instance_id, &block_id)
            .await
            .map_err(|error| ApiError::from_storage(error, "output"))?
        else {
            continue;
        };
        let value = match output.output_ref.as_deref() {
            Some("__retry__" | "__in_progress__") => continue,
            Some(reference) => state
                .storage
                .get_externalized_state(instance_id, reference)
                .await
                .map_err(|error| ApiError::from_storage(error, "output"))?
                .unwrap_or(serde_json::Value::Null),
            None => output.output,
        };
        out.insert(block_id.as_str().to_owned(), value);
    }
    Ok(serde_json::Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sub_sequences_are_detected_anywhere_in_the_tree() {
        let blocks = serde_json::json!([
            {"type": "step", "id": "a", "handler": "noop"},
            {"type": "parallel", "id": "p", "branches": [[{"type": "sub_sequence", "id": "s"}]]}
        ]);
        assert!(contains_sub_sequence(&blocks));
        assert!(!contains_sub_sequence(
            &serde_json::json!([{"type": "step", "id": "a"}])
        ));
    }
}
