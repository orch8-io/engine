//! Per-request guards shared by every embed-token route: a token-bucket rate
//! limiter keyed by `(tenant, sub-tenant)` (EM-06) and a short-lived cache in
//! front of the token-revocation lookup (EM-04).

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use orch8_types::config::EmbedConfig;
use orch8_types::sub_tenant::SubTenantLimits;
use uuid::Uuid;

/// Upper bound on tracked keys for each in-memory guard map.
const MAX_TRACKED: usize = 10_000;
/// How long a revocation lookup result is reused. Bounds how long a revoked
/// token keeps working on a node other than the one that processed the
/// revocation.
pub const REVOCATION_CACHE_TTL: Duration = Duration::from_secs(5);

/// Resolved `[embed]` limits (see [`EmbedConfig`] for the meaning of each).
#[derive(Debug, Clone)]
pub struct EmbedLimits {
    pub max_sequences_per_sub_tenant: u32,
    pub max_versions_per_sequence: u32,
    pub default_max_concurrent: u32,
    pub default_max_executions_per_month: u64,
    pub rate_limit_per_sec: u32,
    pub rate_limit_burst: u32,
}

impl Default for EmbedLimits {
    fn default() -> Self {
        Self::from(&EmbedConfig::default())
    }
}

impl From<&EmbedConfig> for EmbedLimits {
    fn from(c: &EmbedConfig) -> Self {
        Self {
            max_sequences_per_sub_tenant: c.max_sequences_per_sub_tenant,
            max_versions_per_sequence: c.max_versions_per_sequence,
            default_max_concurrent: c.default_max_concurrent_per_sub_tenant,
            default_max_executions_per_month: c.default_max_executions_per_month_per_sub_tenant,
            rate_limit_per_sec: c.rate_limit_per_sec,
            rate_limit_burst: c.rate_limit_burst,
        }
    }
}

impl EmbedLimits {
    /// Limits row given to a sub-tenant without one; `None` when both
    /// default caps are disabled.
    #[must_use]
    pub fn default_sub_tenant_limits(&self) -> Option<SubTenantLimits> {
        let limits = SubTenantLimits {
            max_executions_per_month: (self.default_max_executions_per_month > 0)
                .then_some(self.default_max_executions_per_month),
            max_concurrent: (self.default_max_concurrent > 0)
                .then_some(self.default_max_concurrent),
        };
        (!limits.is_unlimited()).then_some(limits)
    }
}

/// Token-bucket limiter keyed by `(tenant, sub-tenant)`.
#[derive(Debug, Default)]
pub struct EmbedRateLimiter {
    buckets: Mutex<HashMap<(String, String), (f64, Instant)>>,
}

impl EmbedRateLimiter {
    /// Take one token. `false` = over the limit. A zero rate disables.
    pub fn allow(&self, tenant: &str, sub_tenant: &str, limits: &EmbedLimits) -> bool {
        self.allow_at(tenant, sub_tenant, limits, Instant::now())
    }

    fn allow_at(&self, tenant: &str, sub_tenant: &str, limits: &EmbedLimits, now: Instant) -> bool {
        if limits.rate_limit_per_sec == 0 {
            return true;
        }
        let rate = f64::from(limits.rate_limit_per_sec);
        let burst = f64::from(limits.rate_limit_burst.max(1));
        let Ok(mut buckets) = self.buckets.lock() else {
            return true;
        };
        if buckets.len() >= MAX_TRACKED {
            // Drop idle (fully refilled) buckets; they restart full anyway.
            buckets.retain(|_, (tokens, at)| {
                *tokens + now.saturating_duration_since(*at).as_secs_f64() * rate < burst
            });
            if buckets.len() >= MAX_TRACKED {
                buckets.clear();
            }
        }
        let entry = buckets
            .entry((tenant.to_string(), sub_tenant.to_string()))
            .or_insert((burst, now));
        let elapsed = now.saturating_duration_since(entry.1).as_secs_f64();
        entry.0 = (entry.0 + elapsed * rate).min(burst);
        entry.1 = now;
        if entry.0 >= 1.0 {
            entry.0 -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Caches "is this token revoked" answers for [`REVOCATION_CACHE_TTL`].
#[derive(Debug)]
pub struct RevocationCache {
    ttl: Duration,
    entries: Mutex<HashMap<Uuid, (Instant, bool)>>,
}

impl Default for RevocationCache {
    fn default() -> Self {
        Self::with_ttl(REVOCATION_CACHE_TTL)
    }
}

impl RevocationCache {
    #[must_use]
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
        }
    }

    /// Cached verdict for `jti`, if still fresh.
    pub fn get(&self, jti: Uuid) -> Option<bool> {
        let entries = self.entries.lock().ok()?;
        entries
            .get(&jti)
            .filter(|(at, _)| at.elapsed() < self.ttl)
            .map(|(_, revoked)| *revoked)
    }

    pub fn put(&self, jti: Uuid, revoked: bool) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        if entries.len() >= MAX_TRACKED {
            let ttl = self.ttl;
            entries.retain(|_, (at, _)| at.elapsed() < ttl);
            if entries.len() >= MAX_TRACKED {
                entries.clear();
            }
        }
        entries.insert(jti, (Instant::now(), revoked));
    }

    /// Forget every cached verdict (a revocation was just recorded here).
    pub fn clear(&self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limits(rate: u32, burst: u32) -> EmbedLimits {
        EmbedLimits {
            rate_limit_per_sec: rate,
            rate_limit_burst: burst,
            ..EmbedLimits::default()
        }
    }

    #[test]
    fn bucket_allows_burst_then_refills_per_sub_tenant() {
        let limiter = EmbedRateLimiter::default();
        let l = limits(10, 3);
        let t0 = Instant::now();
        for _ in 0..3 {
            assert!(limiter.allow_at("t", "acme", &l, t0));
        }
        assert!(!limiter.allow_at("t", "acme", &l, t0), "burst exhausted");
        assert!(
            limiter.allow_at("t", "globex", &l, t0),
            "another sub-tenant has its own bucket"
        );
        assert!(
            limiter.allow_at("t", "acme", &l, t0 + Duration::from_millis(150)),
            "10/s refills one token in 100ms"
        );
        assert!(!limiter.allow_at("t", "acme", &l, t0 + Duration::from_millis(160)));
    }

    #[test]
    fn zero_rate_disables_the_limiter() {
        let limiter = EmbedRateLimiter::default();
        let l = limits(0, 1);
        for _ in 0..100 {
            assert!(limiter.allow("t", "acme", &l));
        }
    }

    #[test]
    fn revocation_cache_expires_and_clears() {
        let cache = RevocationCache::with_ttl(Duration::from_millis(30));
        let jti = Uuid::new_v4();
        assert_eq!(cache.get(jti), None);
        cache.put(jti, false);
        assert_eq!(cache.get(jti), Some(false));
        cache.clear();
        assert_eq!(cache.get(jti), None);
        cache.put(jti, true);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(cache.get(jti), None, "stale entries are not served");
    }

    #[test]
    fn default_limits_follow_config_and_zero_means_unlimited() {
        let l = EmbedLimits::default();
        let d = l.default_sub_tenant_limits().unwrap();
        assert_eq!(d.max_concurrent, Some(100));
        assert_eq!(d.max_executions_per_month, Some(100_000));
        let off = EmbedLimits {
            default_max_concurrent: 0,
            default_max_executions_per_month: 0,
            ..l
        };
        assert!(off.default_sub_tenant_limits().is_none());
    }
}
