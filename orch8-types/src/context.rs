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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequence::ContextAccess;

    fn sample_context() -> ExecutionContext {
        ExecutionContext {
            data: serde_json::json!({"key": "value"}),
            config: serde_json::json!({"db": "postgres"}),
            audit: vec![AuditEntry {
                timestamp: Utc::now(),
                event: "started".into(),
                details: serde_json::json!({}),
            }],
            runtime: RuntimeContext {
                current_step: Some(BlockId("step-1".into())),
                attempt: 2,
                started_at: Some(Utc::now()),
                resource_key: None,
            },
        }
    }

    #[test]
    fn filtered_all_allowed() {
        let ctx = sample_context();
        let access = ContextAccess {
            data: true,
            config: true,
            audit: true,
            runtime: true,
        };
        let f = ctx.filtered(&access);
        assert_eq!(f.data, ctx.data);
        assert_eq!(f.config, ctx.config);
        assert_eq!(f.audit.len(), 1);
        assert_eq!(f.runtime.attempt, 2);
    }

    #[test]
    fn filtered_all_denied() {
        let ctx = sample_context();
        let access = ContextAccess {
            data: false,
            config: false,
            audit: false,
            runtime: false,
        };
        let f = ctx.filtered(&access);
        assert_eq!(f.data, serde_json::json!({}));
        assert_eq!(f.config, serde_json::json!({}));
        assert!(f.audit.is_empty());
        assert_eq!(f.runtime.attempt, 0);
        assert!(f.runtime.current_step.is_none());
    }

    #[test]
    fn filtered_partial_access() {
        let ctx = sample_context();
        let access = ContextAccess {
            data: true,
            config: false,
            audit: false,
            runtime: true,
        };
        let f = ctx.filtered(&access);
        assert_eq!(f.data, ctx.data);
        assert_eq!(f.config, serde_json::json!({}));
        assert!(f.audit.is_empty());
        assert_eq!(f.runtime.attempt, 2);
    }

    #[test]
    fn default_context_is_empty() {
        let ctx = ExecutionContext::default();
        assert_eq!(ctx.data, serde_json::Value::Null);
        assert_eq!(ctx.config, serde_json::Value::Null);
        assert!(ctx.audit.is_empty());
        assert_eq!(ctx.runtime.attempt, 0);
    }

    #[test]
    fn runtime_context_default() {
        let rt = RuntimeContext::default();
        assert!(rt.current_step.is_none());
        assert_eq!(rt.attempt, 0);
        assert!(rt.started_at.is_none());
        assert!(rt.resource_key.is_none());
    }

    #[test]
    fn audit_entry_serde_round_trip() {
        let entry = AuditEntry {
            timestamp: Utc::now(),
            event: "step_completed".into(),
            details: serde_json::json!({"block_id": "b1", "result": "ok"}),
        };
        let json = serde_json::to_string(&entry).unwrap();
        let back: AuditEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(back.event, "step_completed");
        assert_eq!(back.details["result"], "ok");
    }
}
