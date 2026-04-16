use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::ids::{ResourceKey, TenantId};

/// Rotation strategy for assigning resources from a pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RotationStrategy {
    /// Assign resources in order, cycling through the list.
    RoundRobin,
    /// Assign resources proportional to their weight.
    Weighted,
    /// Assign resources randomly.
    Random,
}

/// A pool of resources that can be assigned to instances.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ResourcePool {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub name: String,
    pub strategy: RotationStrategy,
    /// Tracks the next index for round-robin rotation.
    #[serde(default)]
    pub round_robin_index: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A single resource within a pool.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PoolResource {
    pub id: Uuid,
    pub pool_id: Uuid,
    pub resource_key: ResourceKey,
    /// Display name for the resource.
    pub name: String,
    /// Weight for weighted rotation (higher = more assignments). Default 1.
    #[serde(default = "default_weight")]
    pub weight: u32,
    /// Whether this resource is active and available for assignment.
    #[serde(default = "default_true_pool")]
    pub enabled: bool,
    /// Maximum assignments per day (0 = unlimited).
    #[serde(default)]
    pub daily_cap: u32,
    /// Current daily usage count.
    #[serde(default)]
    pub daily_usage: u32,
    /// Date of the current daily usage counter.
    pub daily_usage_date: Option<NaiveDate>,
    // --- Warmup ramp fields ---
    /// Date when this resource was added to the pool.
    pub warmup_start: Option<NaiveDate>,
    /// Number of days for the warmup ramp (0 = no warmup).
    #[serde(default)]
    pub warmup_days: u32,
    /// Starting daily cap during warmup (ramps linearly to `daily_cap`).
    #[serde(default)]
    pub warmup_start_cap: u32,
    pub created_at: DateTime<Utc>,
}

fn default_weight() -> u32 {
    1
}

fn default_true_pool() -> bool {
    true
}

impl PoolResource {
    /// Calculate the effective daily cap, accounting for warmup ramp.
    #[must_use]
    pub fn effective_daily_cap(&self, today: NaiveDate) -> u32 {
        if self.daily_cap == 0 {
            return 0; // unlimited
        }

        let Some(warmup_start) = self.warmup_start else {
            return self.daily_cap;
        };

        if self.warmup_days == 0 {
            return self.daily_cap;
        }

        #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
        let days_active = (today - warmup_start).num_days().max(0) as u32;
        if days_active >= self.warmup_days {
            return self.daily_cap;
        }

        // Linear ramp from warmup_start_cap to daily_cap over warmup_days.
        let range = self.daily_cap.saturating_sub(self.warmup_start_cap);
        self.warmup_start_cap + (range * days_active / self.warmup_days)
    }

    /// Check if this resource has capacity remaining today.
    #[must_use]
    pub fn has_capacity(&self, today: NaiveDate) -> bool {
        let cap = self.effective_daily_cap(today);
        if cap == 0 {
            return true; // unlimited
        }

        // Reset if it's a new day.
        let usage = if self.daily_usage_date == Some(today) {
            self.daily_usage
        } else {
            0
        };

        usage < cap
    }
}

/// Result of a pool assignment attempt.
#[derive(Debug, Clone)]
pub enum PoolAssignment {
    /// Resource assigned successfully.
    Assigned(ResourceKey),
    /// All resources in the pool are exhausted (daily cap reached).
    Exhausted,
    /// Pool has no enabled resources.
    Empty,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_resource(daily_cap: u32, warmup_days: u32, warmup_start_cap: u32) -> PoolResource {
        PoolResource {
            id: Uuid::new_v4(),
            pool_id: Uuid::new_v4(),
            resource_key: ResourceKey("test".into()),
            name: "test".into(),
            weight: 1,
            enabled: true,
            daily_cap,
            daily_usage: 0,
            daily_usage_date: None,
            warmup_start: Some(NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()),
            warmup_days,
            warmup_start_cap,
            created_at: Utc::now(),
        }
    }

    #[test]
    fn no_warmup_returns_full_cap() {
        let r = make_resource(100, 0, 0);
        let today = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert_eq!(r.effective_daily_cap(today), 100);
    }

    #[test]
    fn warmup_ramp_day_zero() {
        let r = make_resource(100, 10, 10);
        let today = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(r.effective_daily_cap(today), 10); // start cap
    }

    #[test]
    fn warmup_ramp_midway() {
        let r = make_resource(100, 10, 10);
        let today = NaiveDate::from_ymd_opt(2024, 1, 6).unwrap(); // day 5
        let cap = r.effective_daily_cap(today);
        assert_eq!(cap, 55); // 10 + (90 * 5 / 10) = 10 + 45 = 55
    }

    #[test]
    fn warmup_ramp_complete() {
        let r = make_resource(100, 10, 10);
        let today = NaiveDate::from_ymd_opt(2024, 1, 11).unwrap(); // day 10
        assert_eq!(r.effective_daily_cap(today), 100);
    }

    #[test]
    fn warmup_ramp_past_complete() {
        let r = make_resource(100, 10, 10);
        let today = NaiveDate::from_ymd_opt(2024, 6, 1).unwrap(); // way past
        assert_eq!(r.effective_daily_cap(today), 100);
    }

    #[test]
    fn unlimited_cap() {
        let r = make_resource(0, 0, 0);
        let today = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        assert!(r.has_capacity(today));
        assert_eq!(r.effective_daily_cap(today), 0);
    }

    #[test]
    fn has_capacity_respects_daily_usage() {
        let mut r = make_resource(10, 0, 0);
        let today = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        r.daily_usage = 9;
        r.daily_usage_date = Some(today);
        assert!(r.has_capacity(today));

        r.daily_usage = 10;
        assert!(!r.has_capacity(today));
    }

    #[test]
    fn daily_usage_resets_on_new_day() {
        let mut r = make_resource(10, 0, 0);
        let yesterday = NaiveDate::from_ymd_opt(2024, 1, 4).unwrap();
        let today = NaiveDate::from_ymd_opt(2024, 1, 5).unwrap();
        r.daily_usage = 10;
        r.daily_usage_date = Some(yesterday);
        // New day — usage resets to 0
        assert!(r.has_capacity(today));
    }
}
