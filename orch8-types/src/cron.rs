use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{Namespace, SequenceId, TenantId};

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
    pub last_triggered_at: Option<DateTime<Utc>>,
    pub next_fire_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cron_schedule_serde_roundtrip() {
        let now = Utc::now();
        let cs = CronSchedule {
            id: Uuid::now_v7(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("prod".into()),
            sequence_id: SequenceId(Uuid::now_v7()),
            cron_expr: "0 9 * * MON-FRI".into(),
            timezone: "America/New_York".into(),
            enabled: true,
            metadata: serde_json::json!({"tag": "daily"}),
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
            tenant_id: TenantId("t".into()),
            namespace: Namespace("ns".into()),
            sequence_id: SequenceId(Uuid::now_v7()),
            cron_expr: "* * * * *".into(),
            timezone: "UTC".into(),
            enabled: false,
            metadata: serde_json::json!({}),
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
