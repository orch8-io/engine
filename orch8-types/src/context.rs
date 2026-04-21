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

/// Default ceiling for a single instance's serialized `ExecutionContext`.
/// Picked to keep scheduler claim latency healthy — the whole context
/// travels on every tick. See `docs/CONTEXT_MANAGEMENT.md` §9.
pub const DEFAULT_MAX_CONTEXT_BYTES: u32 = 256 * 1024;

/// Returned when a write would make the instance context exceed its size ceiling.
#[derive(Debug, Clone, thiserror::Error)]
#[error(
    "context too large: {actual} bytes exceeds configured max of {max} bytes \
     (see ORCH8_SCHEDULER__MAX_CONTEXT_BYTES)"
)]
pub struct ContextTooLarge {
    pub actual: usize,
    pub max: usize,
}

impl ExecutionContext {
    /// Serialized byte size of this context when written to JSON.
    ///
    /// Perf#6: serializes through a byte-counting `io::Write` sink instead
    /// of allocating the full `Vec<u8>` the way `serde_json::to_vec` does.
    /// For a 1 MiB context the counter allocates zero bytes for the output;
    /// the previous implementation would allocate and memcpy the whole
    /// serialized form every time `check_size` ran on a write path.
    #[must_use]
    pub fn serialized_size(&self) -> usize {
        /// `io::Write` sink that discards bytes but tracks the count —
        /// keeps `serialized_size` off the allocator's hot path.
        #[derive(Default)]
        struct Counter(usize);
        impl std::io::Write for Counter {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0 += buf.len();
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut counter = Counter::default();
        if serde_json::to_writer(&mut counter, self).is_err() {
            return 0;
        }
        counter.0
    }

    /// Reject contexts whose serialized form exceeds `max_bytes`.
    /// `max_bytes == 0` disables the check (escape hatch for tests/tools).
    ///
    /// # Errors
    /// Returns `ContextTooLarge` when serialization exceeds the ceiling.
    pub fn check_size(&self, max_bytes: u32) -> Result<(), ContextTooLarge> {
        if max_bytes == 0 {
            return Ok(());
        }
        let actual = self.serialized_size();
        if actual > max_bytes as usize {
            return Err(ContextTooLarge {
                actual,
                max: max_bytes as usize,
            });
        }
        Ok(())
    }

    /// Return a filtered copy of the context based on section-level permissions.
    /// Denied sections are replaced with their default (empty) values.
    ///
    /// For `data`, [`FieldAccess::Fields`] trims `context.data` (which must be
    /// a JSON object) down to the listed top-level keys. Non-object `data`
    /// falls back to "clone if any field allowed, else empty".
    #[must_use]
    pub fn filtered(&self, access: &ContextAccess) -> Self {
        let data = match &access.data {
            crate::sequence::FieldAccess::Fields { fields } => {
                if let serde_json::Value::Object(map) = &self.data {
                    let mut out = serde_json::Map::new();
                    for field in fields {
                        if let Some(v) = map.get(field) {
                            out.insert(field.clone(), v.clone());
                        }
                    }
                    serde_json::Value::Object(out)
                } else if access.data.allows_any() && !fields.is_empty() {
                    self.data.clone()
                } else {
                    serde_json::Value::Object(serde_json::Map::new())
                }
            }
            _ => {
                if access.data.allows_any() {
                    self.data.clone()
                } else {
                    serde_json::Value::Object(serde_json::Map::new())
                }
            }
        };
        Self {
            data,
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
            data: crate::sequence::FieldAccess::ALL,
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
            data: crate::sequence::FieldAccess::NONE,
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
            data: crate::sequence::FieldAccess::ALL,
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
    fn filtered_data_fields_trims_to_listed_keys() {
        let mut ctx = sample_context();
        ctx.data = serde_json::json!({
            "user_id": "u1",
            "order_id": "o1",
            "secret": "s1",
        });
        let access = ContextAccess {
            data: crate::sequence::FieldAccess::Fields {
                fields: vec!["user_id".into(), "order_id".into()],
            },
            config: true,
            audit: true,
            runtime: true,
        };
        let f = ctx.filtered(&access);
        assert_eq!(
            f.data,
            serde_json::json!({"user_id": "u1", "order_id": "o1"})
        );
    }

    #[test]
    fn filtered_data_fields_empty_yields_empty_object() {
        let ctx = sample_context();
        let access = ContextAccess {
            data: crate::sequence::FieldAccess::Fields { fields: vec![] },
            config: false,
            audit: false,
            runtime: false,
        };
        let f = ctx.filtered(&access);
        assert_eq!(f.data, serde_json::json!({}));
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
    fn check_size_allows_small_context() {
        let ctx = sample_context();
        // 256 KiB — comfortably over the sample size.
        ctx.check_size(256 * 1024).unwrap();
    }

    #[test]
    fn check_size_rejects_oversize() {
        // ~1 KiB of payload in `data`.
        let ctx = ExecutionContext {
            data: serde_json::json!({ "blob": "x".repeat(1024) }),
            ..ExecutionContext::default()
        };
        let err = ctx.check_size(128).unwrap_err();
        assert!(err.actual > 128);
        assert_eq!(err.max, 128);
        // Surface the env var hint in the error message so operators know
        // which knob to turn.
        assert!(err
            .to_string()
            .contains("ORCH8_SCHEDULER__MAX_CONTEXT_BYTES"));
    }

    #[test]
    fn check_size_zero_disables_check() {
        // Large context — 10 KiB — passes when the limit is 0.
        let ctx = ExecutionContext {
            data: serde_json::json!({ "blob": "x".repeat(10_000) }),
            ..ExecutionContext::default()
        };
        ctx.check_size(0).unwrap();
    }

    #[test]
    fn serialized_size_tracks_payload_growth() {
        let empty = ExecutionContext::default().serialized_size();
        let ctx = ExecutionContext {
            data: serde_json::json!({ "blob": "y".repeat(512) }),
            ..ExecutionContext::default()
        };
        let grown = ctx.serialized_size();
        assert!(
            grown > empty + 500,
            "payload growth should dominate envelope"
        );
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
