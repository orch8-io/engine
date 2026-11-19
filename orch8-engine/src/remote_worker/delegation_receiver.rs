//! Desktop receiver for a pinned, effect-free phone delegation.
use super::{
    HttpLeaseClient, LeaseResponse, RemoteTask, heartbeat_interval,
    lease::{ConfirmedLease, LeaseEnd},
};
use crate::{
    delegation::DELEGATION_HANDLER,
    handlers::builtin::read_body_capped,
    outbound::{self, Profile},
};
use orch8_types::{
    continuity::{RuntimeCapabilities, RuntimeId, RuntimeKind, RuntimeTrustLevel},
    delegation_definition::{
        DefinitionGraphLimits,
        claim::DelegationClaimContext,
        input::{DelegationInputEnvelope, InputLimits},
        protocol::DelegationProtocolCapabilities,
        wire::DefinitionWireEnvelope,
    },
    ids::{SequenceId, TenantId},
    sequence::{BlockDefinition, SequenceDefinition, StepDef},
};
use serde::de::DeserializeOwned;
use serde_json::json;
use std::time::Duration;
use tokio::time::{Instant, sleep, timeout_at};

/// The one handler the receiver executes. Its result is the verified input.
pub const ECHO_HANDLER: &str = "orch8.delegation.echo";

const DEFINITION_LIMITS: DefinitionGraphLimits = DefinitionGraphLimits {
    definitions: 1,
    selectors: 0,
    serialized_bytes: 64 * 1024,
    expanded_bytes: 64 * 1024,
};
const INPUT_BYTES: u32 = 64 * 1024;
const INPUT_EXPANDED_BYTES: u32 = 256 * 1024;
const INPUT_LIMITS: InputLimits = InputLimits {
    serialized_bytes: INPUT_BYTES as usize,
    expanded_bytes: INPUT_EXPANDED_BYTES as usize,
};
// A base64 envelope for a maximum-sized definition or input plus metadata.
const MAX_ENVELOPE_BYTES: usize = (INPUT_BYTES as usize).div_ceil(3) * 4 + 4096;

/// An opt-in receiver for a single simple echo step, bound to its backend,
/// tenant, worker credential and sequence ID. The only operation is a bounded
/// JSON projection, so a new claim can recompute it after a crash.
pub struct EchoDelegationReceiver {
    client: HttpLeaseClient,
    tenant: TenantId,
    runtime: RuntimeId,
    sequence: SequenceId,
    protocol: DelegationProtocolCapabilities,
}

#[derive(Debug, thiserror::Error)]
pub enum EchoReceiverError {
    #[error("invalid delegation receiver configuration")]
    Configuration,
    #[error("delegation receiver registration failed")]
    Registration,
    #[error("delegation receiver poll failed")]
    Poll,
    #[error("delegation claim authority ended: {0:?}")]
    Lease(LeaseEnd),
    #[error("delegation payload or definition does not match the echo receiver")]
    Payload,
    #[error("delegation outcome was rejected")]
    Acknowledgement,
}

impl EchoDelegationReceiver {
    /// Use the versioned backend API URL and a tenant-scoped worker credential.
    /// HTTP is allowed for loopback addresses used by local development.
    ///
    /// # Errors
    /// The API origin, tenant, runtime, sequence or credential is invalid.
    pub fn new(
        api_base: &str,
        tenant: TenantId,
        runtime: RuntimeId,
        sequence: SequenceId,
        credential: String,
    ) -> Result<Self, EchoReceiverError> {
        let url = url::Url::parse(api_base).map_err(|_| EchoReceiverError::Configuration)?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            _ => false,
        };
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host().is_none()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || credential.trim().is_empty()
            || reqwest::header::HeaderValue::from_str(&credential).is_err()
            || tenant.as_str().trim().is_empty()
            || tenant.as_str().len() > 128
            || runtime.as_uuid().is_nil()
            || sequence.into_uuid().is_nil()
        {
            return Err(EchoReceiverError::Configuration);
        }
        let http = outbound::builder(Profile::LocalSidecar)
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| EchoReceiverError::Configuration)?;
        let client = HttpLeaseClient::new(
            http,
            url.as_str(),
            vec![("x-api-key", credential)],
            runtime.to_string(),
        );
        let protocol = DelegationProtocolCapabilities::v1(
            DEFINITION_LIMITS,
            INPUT_BYTES,
            INPUT_EXPANDED_BYTES,
        )
        .map_err(|_| EchoReceiverError::Configuration)?;
        Ok(Self {
            client,
            tenant,
            runtime,
            sequence,
            protocol,
        })
    }

    fn capabilities(&self) -> RuntimeCapabilities {
        let now = chrono::Utc::now();
        RuntimeCapabilities {
            delegation_protocol: Some(self.protocol),
            runtime_id: self.runtime,
            kind: RuntimeKind::Desktop,
            trust: RuntimeTrustLevel::Registered,
            handlers: vec![DELEGATION_HANDLER.into(), ECHO_HANDLER.into()],
            plugins: vec![],
            credentials: vec![],
            regions: vec![],
            hardware: vec![],
            offline_capable: false,
            connectivity: None,
            battery_percent: None,
            estimated_cost_microunits: None,
            estimated_latency_ms: None,
            draining: false,
            capsule_signing_public_key: None,
            labels: std::collections::BTreeMap::new(),
            observed_at: now,
            expires_at: now + chrono::Duration::seconds(60),
        }
    }

    /// Advertise the receiver before the phone publishes its delegation. Polls
    /// refresh the capability once the receiver begins serving.
    ///
    /// # Errors
    /// The backend is unavailable or rejects the worker identity.
    pub async fn register(&self) -> Result<(), EchoReceiverError> {
        self.client
            .register_runtime(self.tenant.as_str(), &self.capabilities())
            .await
            .map_err(|_| EchoReceiverError::Registration)
    }

    /// Poll one mailbox task. Return its ID only after backend acceptance.
    ///
    /// # Errors
    /// Polling, claim confirmation, payload verification or acknowledgement failed.
    pub async fn receive_once(&mut self) -> Result<Option<uuid::Uuid>, EchoReceiverError> {
        let mut poll = self
            .client
            .poll(DELEGATION_HANDLER, 1, None, &self.capabilities())
            .await
            .map_err(|_| EchoReceiverError::Poll)?;
        let Some(task) = poll.tasks.pop() else {
            return Ok(None);
        };
        if !poll.tasks.is_empty() {
            return Err(EchoReceiverError::Poll);
        }
        let cadence = heartbeat_interval(
            poll.heartbeat_interval_secs,
            poll.lease_secs,
            task.lease_secs,
        );
        self.receive(&task, cadence).await?;
        Ok(Some(task.id))
    }

    async fn receive(&self, task: &RemoteTask, cadence: Duration) -> Result<(), EchoReceiverError> {
        let context = task
            .delegation
            .as_ref()
            .filter(|c| c.scope.tenant_id == self.tenant)
            .ok_or(EchoReceiverError::Payload)?;
        let started = Instant::now();
        let remaining = (context.expires_at - chrono::Utc::now())
            .to_std()
            .map_err(|_| EchoReceiverError::Lease(LeaseEnd::Expired))?;
        let deadline = started
            .checked_add(remaining)
            .ok_or(EchoReceiverError::Lease(LeaseEnd::Expired))?;
        let heartbeat = || self.client.heartbeat(task.id, task.claim_epoch);
        let lease = timeout_at(
            deadline,
            ConfirmedLease::acquire(task.lease_secs, cadence, heartbeat),
        )
        .await
        .map_err(|_| EchoReceiverError::Lease(LeaseEnd::Expired))?
        .map_err(EchoReceiverError::Lease)?;
        let authority = lease.authority();
        let live = || {
            if Instant::now() >= deadline || chrono::Utc::now() >= context.expires_at {
                return Err(EchoReceiverError::Lease(LeaseEnd::Expired));
            }
            authority.check().map_err(EchoReceiverError::Lease)
        };
        timeout_at(
            deadline,
            lease.run(
                cadence,
                async {
                    // Bound each fetch and use the exact issued scope, digests and resources.
                    live()?;
                    let definition: DefinitionWireEnvelope =
                        self.payload(context, "definition").await?;
                    let graph = definition
                        .verify(&context.scope, &context.definition, DEFINITION_LIMITS)
                        .map_err(|_| EchoReceiverError::Payload)?;
                    if graph.expanded_bytes() != context.usage.definition_expanded_bytes {
                        return Err(EchoReceiverError::Payload);
                    }
                    let step = self.echo_step(&graph.root().definition)?;
                    live()?;
                    let input: DelegationInputEnvelope = self.payload(context, "input").await?;
                    let input = input
                        .verify(
                            &context.scope,
                            &context.request_sha256,
                            &context.input,
                            INPUT_LIMITS,
                        )
                        .map_err(|_| EchoReceiverError::Payload)?;
                    live()?;
                    let mut outputs = serde_json::Map::new();
                    outputs.insert(step.id.to_string(), input.into_value());
                    let output = json!({"state": "completed", "outputs": outputs});
                    loop {
                        live()?;
                        match self
                            .client
                            .complete(task.id, task.claim_epoch, &output)
                            .await
                        {
                            LeaseResponse::Accepted => return Ok(()),
                            LeaseResponse::Retry => sleep(Duration::from_millis(250)).await,
                            LeaseResponse::LostOwnership => {
                                return Err(EchoReceiverError::Lease(LeaseEnd::LostOwnership));
                            }
                            _ => return Err(EchoReceiverError::Acknowledgement),
                        }
                    }
                },
                heartbeat,
            ),
        )
        .await
        .map_err(|_| EchoReceiverError::Lease(LeaseEnd::Expired))?
        .map_err(EchoReceiverError::Lease)?
    }

    async fn payload<T: DeserializeOwned>(
        &self,
        context: &DelegationClaimContext,
        resource: &str,
    ) -> Result<T, EchoReceiverError> {
        let scope = &context.scope;
        let path = format!(
            "workers/tasks/{}/{resource}?worker_id={}&claim_epoch={}",
            scope.task_id, self.runtime, scope.claim_epoch
        );
        let response = self
            .client
            .get(&path)
            .await
            .map_err(|_| EchoReceiverError::Payload)?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(EchoReceiverError::Payload);
        }
        let bytes = read_body_capped(response, MAX_ENVELOPE_BYTES)
            .await
            .map_err(|_| EchoReceiverError::Payload)?;
        serde_json::from_slice(&bytes).map_err(|_| EchoReceiverError::Payload)
    }

    fn echo_step<'a>(
        &self,
        root: &'a SequenceDefinition,
    ) -> Result<&'a StepDef, EchoReceiverError> {
        if root.id != self.sequence
            || root.tenant_id != self.tenant
            || root.interceptors.is_some()
            || root.on_failure.is_some()
            || root.on_cancel.is_some()
            || root.input_schema.is_some()
            || root.sla.is_some()
            || root.placement.is_some()
            || root.sub_tenant.is_some()
        {
            return Err(EchoReceiverError::Payload);
        }
        let [BlockDefinition::Step(step)] = root.blocks.as_slice() else {
            return Err(EchoReceiverError::Payload);
        };
        let fields = serde_json::to_value(step.as_ref()).map_err(|_| EchoReceiverError::Payload)?;
        if step.handler != ECHO_HANDLER
            || (!step.params.is_null() && step.params != json!({}))
            || !step.cancellable
            || fields.as_object().is_none_or(|fields| {
                fields
                    .keys()
                    .any(|key| !matches!(key.as_str(), "id" | "handler" | "params" | "cancellable"))
            })
        {
            return Err(EchoReceiverError::Payload);
        }
        Ok(step)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_only_the_pinned_single_effect_free_echo() {
        let sequence = SequenceId::new();
        let tenant = TenantId::new("preview").unwrap();
        let receiver = EchoDelegationReceiver::new(
            "http://127.0.0.1:8080/api/v1",
            tenant.clone(),
            RuntimeId::new(),
            sequence,
            "test-worker-key".into(),
        )
        .unwrap();
        let root = json!({
            "id": sequence, "tenant_id": tenant, "namespace": "default",
            "name": "echo", "version": 1, "created_at": "2026-01-01T00:00:00Z",
            "blocks": [{"type":"step","id":"echo","handler":ECHO_HANDLER,"params":{}}]
        });
        let definition: SequenceDefinition = serde_json::from_value(root.clone()).unwrap();
        assert_eq!(receiver.echo_step(&definition).unwrap().id.as_str(), "echo");

        let mut wrong_id = root.clone();
        wrong_id["id"] = json!(SequenceId::new());
        let mut wrong_tenant = root.clone();
        wrong_tenant["tenant_id"] = json!("other");
        let mut scheduled_retry = root.clone();
        scheduled_retry["blocks"][0]["retry"] =
            json!({"max_attempts":2,"initial_backoff":10,"max_backoff":10});
        let mut wrong_handler = root.clone();
        wrong_handler["blocks"][0]["handler"] = json!("noop");
        let mut second_step = root.clone();
        second_step["blocks"]
            .as_array_mut()
            .unwrap()
            .push(root["blocks"][0].clone());
        for rejected in [
            wrong_id,
            wrong_tenant,
            scheduled_retry,
            wrong_handler,
            second_step,
        ] {
            let definition = serde_json::from_value(rejected).unwrap();
            assert!(matches!(
                receiver.echo_step(&definition),
                Err(EchoReceiverError::Payload)
            ));
        }
    }

    #[tokio::test]
    async fn an_expired_publication_cannot_start_claim_confirmation() {
        let runtime = RuntimeId::new();
        let receiver = EchoDelegationReceiver::new(
            "http://127.0.0.1:1/api/v1",
            TenantId::new("preview").unwrap(),
            runtime,
            SequenceId::new(),
            "test-worker-key".into(),
        )
        .unwrap();
        let task_id = uuid::Uuid::new_v4();
        let task: RemoteTask = serde_json::from_value(json!({
            "id": task_id, "instance_id": uuid::Uuid::new_v4(), "block_id": "echo",
            "handler_name": DELEGATION_HANDLER, "claim_epoch": 7,
            "delegation": {
                "version": 1,
                "scope": {"tenant_id":"preview","task_id":task_id,"worker_id":runtime,"claim_epoch":"7"},
                "request_sha256": "a".repeat(64),
                "expires_at": chrono::Utc::now() - chrono::Duration::seconds(1),
                "definition": {
                    "version":1, "sequence_sha256":"b".repeat(64), "bundle_sha256":"c".repeat(64),
                    "payload_sha256":"d".repeat(64), "payload_bytes":128,
                    "definition_count":1, "selector_count":0
                },
                "input":{"version":1,"payload_sha256":"e".repeat(64),"payload_bytes":4,"expanded_bytes":64},
                "usage":{
                    "definition_expanded_bytes":512,"input_wire_version":1,
                    "input_payload_bytes":4,"input_expanded_bytes":64
                }
            }
        })).unwrap();
        assert!(matches!(
            receiver.receive(&task, Duration::from_millis(10)).await,
            Err(EchoReceiverError::Lease(LeaseEnd::Expired))
        ));
    }
}
