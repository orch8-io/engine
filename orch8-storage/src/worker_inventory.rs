//! Recent transport observations used to explain which workers can claim a route.

use chrono::{DateTime, Utc};
use orch8_types::{
    continuity::{RuntimeCapabilities, RuntimeId},
    ids::TenantId,
};
use uuid::Uuid;

#[derive(Clone, Copy)]
pub enum WorkerTransport {
    Http,
    GrpcUnary,
    GrpcStream(Uuid),
}

impl WorkerTransport {
    pub(crate) fn key(self) -> (&'static str, String) {
        match self {
            Self::Http => ("http", String::new()),
            Self::GrpcUnary => ("grpc_unary", String::new()),
            Self::GrpcStream(id) => ("grpc_stream", id.to_string()),
        }
    }
}

/// All handlers advertised by the same transport poll or stream session.
pub struct WorkerObservation<'a> {
    pub tenant: Option<&'a TenantId>,
    pub worker_id: &'a str,
    pub handlers: &'a [String],
    pub queue_name: Option<&'a str>,
    pub all_queues: bool,
    pub version: Option<&'a str>,
    pub transport: WorkerTransport,
    pub runtime_id: Option<RuntimeId>,
    /// Shared HTTP polls have no tenant to own a capability registration.
    pub shared_runtime: Option<&'a RuntimeCapabilities>,
    pub available_poll_slots: Option<u32>,
    pub draining: bool,
}

/// Bounded, tenant-scoped selection on a single effective route.
pub struct WorkerRoute<'a> {
    pub tenant: &'a TenantId,
    pub handler: &'a str,
    pub queue: Option<&'a str>,
    pub seen_since: DateTime<Utc>,
    pub now: DateTime<Utc>,
    pub limit: u32,
}

pub struct RouteObservation {
    pub worker_id: String,
    pub shared: bool,
    pub version: Option<String>,
    pub available_poll_slots: Option<u32>,
    pub runtime: Option<RuntimeCapabilities>,
    pub draining: bool,
    /// This HTTP worker claimed with an explicit capability advertisement.
    /// Its next poll needs current capability evidence.
    pub capability_unavailable: bool,
}
