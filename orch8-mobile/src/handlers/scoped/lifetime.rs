//! A point lookup at execution admission, independent of bounded periodic GC.
//! Logical expiry never aborts a foreign host or returns its physical permit.
use crate::storage::MobileStorage;
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
};
use std::{sync::Arc, time::Duration};

pub(crate) struct Lifetime {
    storage: Arc<MobileStorage>,
    seconds: u64,
    clock: orch8_types::clock::SharedClock,
}
impl Lifetime {
    pub(crate) fn new(storage: Arc<MobileStorage>, seconds: u64) -> Self {
        Self {
            storage,
            seconds,
            clock: orch8_types::clock::SharedClock::default(),
        }
    }
    #[cfg(test)]
    pub(super) fn set_clock(&mut self, clock: orch8_types::clock::SharedClock) {
        self.clock = clock;
    }
    pub(crate) async fn verify(
        &self,
        id: InstanceId,
        tenant: &TenantId,
        token: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        let mut tx = self.storage.pool().begin().await?;
        let created: Option<String> = sqlx::query_scalar(
            "SELECT r.created_at FROM task_instances i JOIN mobile_catalog_run_requests r ON r.id=i.id
             WHERE i.id=?1 AND i.tenant_id=?2 AND i.state='running' AND i.claimed_at=?3
             AND i.created_at=r.created_at"
        ).bind(id.to_string()).bind(tenant.as_str()).bind(token.to_rfc3339()).fetch_optional(&mut *tx).await?;
        let Some(created) = created else {
            tx.commit().await?;
            return Ok(false);
        };
        let created = chrono::DateTime::parse_from_rfc3339(&created)
            .map_err(|_| StorageError::Query("invalid scoped creation timestamp".into()))?;
        let expired = cutoff(self.seconds, self.clock.now()).is_some_and(|cutoff| created < cutoff);
        if expired {
            // The original claim is essential: a late observer cannot fail a
            // successor execution or replace an already settled terminal state.
            sqlx::query("UPDATE task_instances SET state='failed',claimed_at=NULL,next_fire_at=NULL,updated_at=?4 WHERE id=?1 AND tenant_id=?2 AND state='running' AND claimed_at=?3")
                .bind(id.to_string()).bind(tenant.as_str()).bind(token.to_rfc3339()).bind(chrono::Utc::now().to_rfc3339()).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(!expired)
    }
}
fn cutoff(
    seconds: u64,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::Duration::from_std(Duration::from_secs(seconds))
        .ok()
        .and_then(|lifetime| now.checked_sub_signed(lifetime))
}
