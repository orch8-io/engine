use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{Namespace, SequenceId, TenantId};

/// Exact filters applied before selecting a cron inventory page.
#[derive(Debug, Default, Serialize, ToSchema)]
pub struct CronInventoryFilter {
    pub namespace: Option<String>,
    pub sequence_id: Option<Uuid>,
    pub enabled: Option<bool>,
}

impl CronInventoryFilter {
    pub fn is_empty(&self) -> bool {
        self.namespace.is_none() && self.sequence_id.is_none() && self.enabled.is_none()
    }
}

/// What to do when a schedule fires while a previous run it created is
/// still active (scheduled / running / waiting / paused).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OverlapPolicy {
    /// Fire regardless of previous runs (default — the pre-policy behavior).
    #[default]
    Allow,
    /// Skip the occurrence when a previous run is still active. Skips are
    /// counted on the schedule (`skipped_fires`, `last_skipped_at`) and in
    /// the `orch8_cron_skipped_total` metric.
    Skip,
    /// Defer the occurrence until the previous run finishes, then fire once.
    /// Multiple missed occurrences collapse into a single buffered fire.
    BufferOne,
    /// Cancel still-active previous runs, then fire.
    CancelPrevious,
}

impl FromStr for OverlapPolicy {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "allow" => Ok(Self::Allow),
            "skip" => Ok(Self::Skip),
            "buffer_one" => Ok(Self::BufferOne),
            "cancel_previous" => Ok(Self::CancelPrevious),
            other => Err(format!("unknown overlap policy: {other}")),
        }
    }
}

impl std::fmt::Display for OverlapPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Allow => f.write_str("allow"),
            Self::Skip => f.write_str("skip"),
            Self::BufferOne => f.write_str("buffer_one"),
            Self::CancelPrevious => f.write_str("cancel_previous"),
        }
    }
}

/// A cron schedule that periodically creates instances of a sequence.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CronSchedule {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub namespace: Namespace,
    pub sequence_id: SequenceId,
    /// Standard cron expression (e.g. "0 9 * * MON-FRI").
    pub cron_expr: String,
    pub timezone: String,
    pub enabled: bool,
    /// Extra metadata to inject into created instances.
    #[serde(default)]
    pub metadata: serde_json::Value,
    /// Behavior when a fire is due while a previous run is still active.
    #[serde(default)]
    pub overlap_policy: OverlapPolicy,
    /// Occurrences skipped by the `skip` overlap policy.
    #[serde(default)]
    pub skipped_fires: i64,
    /// When the `skip` overlap policy last skipped an occurrence.
    #[serde(default)]
    pub last_skipped_at: Option<DateTime<Utc>>,
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl CronSchedule {
    /// Configuration identity for a reviewed change. Claims and fire-time
    /// observations advance independently of operator configuration.
    pub fn configuration_revision(&self) -> Result<String, serde_json::Error> {
        let configuration = serde_json::to_value((
            "cron-configuration-v1",
            self.id,
            &self.tenant_id,
            &self.namespace,
            self.sequence_id,
            self.created_at,
            &self.cron_expr,
            &self.timezone,
            self.enabled,
            &self.metadata,
            self.overlap_policy,
        ))?;
        Ok(crate::delegation_definition::semantic_json_sha256(
            &configuration,
        ))
    }

    /// Stable identity of the currently due occurrence.
    #[must_use]
    pub fn occurrence_key(&self) -> Option<String> {
        self.next_fire_at
            .map(|fire_at| format!("cron:{}:{}", self.id, fire_at.to_rfc3339()))
    }
}

/// Fenced source state used to transfer one recurring schedule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronMigrationSnapshot {
    pub migration_id: Uuid,
    pub source_epoch: u64,
    pub last_fire_at: Option<DateTime<Utc>>,
    pub schedule: CronSchedule,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronMigrationAcknowledgement {
    pub schedule_id: Uuid,
    pub migration_id: Uuid,
    pub ownership_epoch: u64,
    pub snapshot_sha256: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_schedule_serde_roundtrip() {
        let now = Utc::now();
        let cs = CronSchedule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("prod"),
            sequence_id: SequenceId::new(),
            cron_expr: "0 9 * * MON-FRI".into(),
            timezone: "America/New_York".into(),
            enabled: true,
            metadata: serde_json::json!({"tag": "daily"}),
            overlap_policy: OverlapPolicy::Skip,
            skipped_fires: 2,
            last_skipped_at: Some(now),
            last_triggered_at: Some(now),
            next_fire_at: Some(now),
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&cs).unwrap();
        let back: CronSchedule = serde_json::from_str(&json).unwrap();
        assert_eq!(back.cron_expr, "0 9 * * MON-FRI");
        assert_eq!(back.timezone, "America/New_York");
        assert!(back.enabled);
        assert_eq!(back.metadata["tag"], "daily");
    }

    #[test]
    fn cron_schedule_optional_fields_nullable() {
        let now = Utc::now();
        let cs = CronSchedule {
            id: Uuid::now_v7(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            sequence_id: SequenceId::new(),
            cron_expr: "* * * * *".into(),
            timezone: "UTC".into(),
            enabled: false,
            metadata: serde_json::json!({}),
            overlap_policy: OverlapPolicy::default(),
            skipped_fires: 0,
            last_skipped_at: None,
            last_triggered_at: None,
            next_fire_at: None,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&cs).unwrap();
        let back: CronSchedule = serde_json::from_str(&json).unwrap();
        assert!(back.last_triggered_at.is_none());
        assert!(back.next_fire_at.is_none());
        assert!(!back.enabled);
    }
}
