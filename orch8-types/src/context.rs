use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::{BlockId, ResourceKey};
use crate::sequence::ContextAccess;

/// Multi-section execution context with different permission semantics.
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ExecutionContext {
    /// Read/write by step handlers.
    #[serde(default)]
    pub data: serde_json::Value,
    /// Read-only after initialization.
    #[serde(default)]
    pub config: serde_json::Value,
    /// Append-only audit trail.
    #[serde(default)]
    pub audit: Vec<AuditEntry>,
    /// Engine-managed runtime state. Read-only to step handlers.
    #[serde(default)]
    pub runtime: RuntimeContext,
}

impl ExecutionContext {
    /// Return a filtered copy of the context based on section-level permissions.
    /// Denied sections are replaced with their default (empty) values.
    #[must_use]
    pub fn filtered(&self, access: &ContextAccess) -> Self {
        Self {
            data: if access.data {
                self.data.clone()
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            },
            config: if access.config {
                self.config.clone()
            } else {
                serde_json::Value::Object(serde_json::Map::new())
            },
            audit: if access.audit {
                self.audit.clone()
            } else {
                Vec::new()
            },
            runtime: if access.runtime {
                self.runtime.clone()
            } else {
                RuntimeContext::default()
            },
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct RuntimeContext {
    pub current_step: Option<BlockId>,
    #[serde(default)]
    pub attempt: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub resource_key: Option<ResourceKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub event: String,
    pub details: serde_json::Value,
}
