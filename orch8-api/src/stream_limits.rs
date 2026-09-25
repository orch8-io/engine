//! Admission control for long-lived SSE streams.
//!
//! Every stream holds a slot of the process-wide [`crate::AppState::stream_limiter`]
//! AND a slot of its tenant's budget ([`MAX_STREAMS_PER_TENANT`]). Without the
//! per-tenant cap a single tenant could open every global slot and lock all
//! other tenants out of streaming.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Maximum concurrent SSE streams a single tenant may hold per process.
pub const MAX_STREAMS_PER_TENANT: usize = 32;

static TENANT_STREAMS: LazyLock<Mutex<HashMap<String, usize>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// Why a stream was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamLimitError {
    /// The process-wide stream ceiling is exhausted.
    Global,
    /// This tenant already holds [`MAX_STREAMS_PER_TENANT`] streams.
    Tenant,
}

/// RAII guard for one admitted stream; releases both slots on drop.
#[derive(Debug)]
pub struct StreamPermit {
    tenant: String,
    _global: OwnedSemaphorePermit,
}

impl Drop for StreamPermit {
    fn drop(&mut self) {
        let mut map = TENANT_STREAMS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(count) = map.get_mut(&self.tenant) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                map.remove(&self.tenant);
            }
        }
    }
}

/// Admit one stream for `tenant`, taking a global and a per-tenant slot.
pub fn acquire(global: &Arc<Semaphore>, tenant: &str) -> Result<StreamPermit, StreamLimitError> {
    acquire_with_cap(global, tenant, MAX_STREAMS_PER_TENANT)
}

fn acquire_with_cap(
    global: &Arc<Semaphore>,
    tenant: &str,
    per_tenant: usize,
) -> Result<StreamPermit, StreamLimitError> {
    let mut map = TENANT_STREAMS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let count = map.entry(tenant.to_owned()).or_insert(0);
    if *count >= per_tenant {
        return Err(StreamLimitError::Tenant);
    }
    let Ok(global) = global.clone().try_acquire_owned() else {
        if *count == 0 {
            map.remove(tenant);
        }
        return Err(StreamLimitError::Global);
    };
    *count += 1;
    Ok(StreamPermit {
        tenant: tenant.to_owned(),
        _global: global,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_tenant_cannot_take_every_global_slot() {
        let global = Arc::new(Semaphore::new(4));
        let a1 = acquire_with_cap(&global, "stream-limit-a", 2).unwrap();
        let _a2 = acquire_with_cap(&global, "stream-limit-a", 2).unwrap();
        assert_eq!(
            acquire_with_cap(&global, "stream-limit-a", 2).unwrap_err(),
            StreamLimitError::Tenant
        );
        // Another tenant still gets in.
        let _b = acquire_with_cap(&global, "stream-limit-b", 2).unwrap();
        // Releasing frees both the tenant and the global slot.
        drop(a1);
        assert_eq!(global.available_permits(), 2);
        let _a3 = acquire_with_cap(&global, "stream-limit-a", 2).unwrap();
    }

    #[test]
    fn global_ceiling_still_applies() {
        let global = Arc::new(Semaphore::new(1));
        let _c = acquire_with_cap(&global, "stream-limit-c", 8).unwrap();
        assert_eq!(
            acquire_with_cap(&global, "stream-limit-d", 8).unwrap_err(),
            StreamLimitError::Global
        );
        assert!(
            !TENANT_STREAMS
                .lock()
                .unwrap()
                .contains_key("stream-limit-d")
        );
    }
}
