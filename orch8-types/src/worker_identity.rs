//! Fresh transport identity, never a durable grant or provider execution authority.
use crate::{
    continuity::{RuntimeId, RuntimeKind},
    ids::TenantId,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentityRequest {
    pub nonce: String,
}
impl WorkerIdentityRequest {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.nonce.len() != 36 {
            return Err("invalid worker identity challenge");
        }
        let id = self
            .nonce
            .parse::<uuid::Uuid>()
            .map_err(|_| "invalid worker identity challenge")?;
        if id.is_nil() || id.to_string() != self.nonce {
            return Err("invalid worker identity challenge");
        }
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct WorkerIdentityResponse {
    pub version: u32,
    pub nonce: String,
    pub tenant_id: TenantId,
    pub runtime_id: RuntimeId,
    pub kind: RuntimeKind,
    pub device_id: String,
    pub issued_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
impl WorkerIdentityResponse {
    pub fn validate_for(
        &self,
        nonce: &str,
        tenant: &TenantId,
        runtime: RuntimeId,
        device: &str,
        now: DateTime<Utc>,
    ) -> Result<(), &'static str> {
        if (WorkerIdentityRequest {
            nonce: nonce.to_owned(),
        })
        .validate()
        .is_err()
            || self.version != 1
            || self.nonce != nonce
            || &self.tenant_id != tenant
            || self.runtime_id != runtime
            || self.kind != RuntimeKind::Mobile
            || self.device_id != device
            || device.trim().is_empty()
            || device.len() > 256
            || device.chars().any(char::is_control)
            || self.issued_at.signed_duration_since(now) > chrono::Duration::seconds(30)
            || self.expires_at <= now
            || self.expires_at <= self.issued_at
            || self.expires_at - self.issued_at > chrono::Duration::seconds(60)
        {
            return Err("worker identity was not verified");
        }
        Ok(())
    }
}
impl std::fmt::Debug for WorkerIdentityResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerIdentityResponse")
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests;
