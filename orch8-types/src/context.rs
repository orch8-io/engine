use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::ids::{BlockId, ResourceKey};

/// Multi-section execution context with different permission semantics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RuntimeContext {
    pub current_step: Option<BlockId>,
    #[serde(default)]
    pub attempt: u32,
    pub started_at: Option<DateTime<Utc>>,
    pub resource_key: Option<ResourceKey>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEntry {
    pub timestamp: DateTime<Utc>,
    pub event: String,
    pub details: serde_json::Value,
}
