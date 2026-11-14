//! Acquire namespace mutexes before starting a repeatable-read snapshot.
//! Session locks bridge that boundary. An abandoned guard closes its connection;
//! a connection holding session locks must never return to the pool.
use orch8_types::{
    error::StorageError,
    ids::{InstanceId, TenantId},
};
use sqlx::{PgConnection, PgPool, Postgres, pool::PoolConnection};

pub(super) struct PublicationLocks {
    connection: Option<PoolConnection<Postgres>>,
    keys: Vec<String>,
}
impl PublicationLocks {
    pub(super) async fn acquire(
        pool: &PgPool,
        tenant: &TenantId,
        mut ids: Vec<InstanceId>,
    ) -> Result<Self, StorageError> {
        ids.sort();
        ids.dedup();
        let keys = ids
            .into_iter()
            .map(|id| super::portable_effects::instance_lock_key(tenant, id))
            .collect::<Result<Vec<_>, _>>()?;
        let mut guard = Self {
            connection: Some(pool.acquire().await?),
            keys,
        };
        let previous: String = sqlx::query_scalar("SELECT current_setting('lock_timeout')")
            .fetch_one(guard.connection())
            .await?;
        // A cancelled blocking acquisition must not retain an earlier mutex
        // indefinitely while PostgreSQL waits for another lock. Honour any
        // shorter configured timeout, and restore the setting before BEGIN.
        sqlx::query("SELECT set_config('lock_timeout', (CASE WHEN current_setting('lock_timeout')='0' THEN 1000 ELSE LEAST(EXTRACT(EPOCH FROM current_setting('lock_timeout')::interval)*1000,1000)::bigint END)::text || 'ms',false)")
            .execute(guard.connection()).await?;
        for key in &guard.keys {
            sqlx::query("SELECT pg_advisory_lock(hashtextextended($1,0))")
                .bind(key)
                .execute(
                    &mut **guard
                        .connection
                        .as_mut()
                        .expect("live publication connection"),
                )
                .await
                .map_err(|error| {
                    if error
                        .as_database_error()
                        .is_some_and(|e| e.code().as_deref() == Some("55P03"))
                    {
                        StorageError::Connection("publication namespace is busy".into())
                    } else {
                        error.into()
                    }
                })?;
        }
        sqlx::query("SELECT set_config('lock_timeout',$1,false)")
            .bind(previous)
            .execute(guard.connection())
            .await?;
        Ok(guard)
    }

    pub(super) fn connection(&mut self) -> &mut PgConnection {
        self.connection
            .as_deref_mut()
            .expect("live publication connection")
    }

    pub(super) async fn release(mut self) -> Result<(), StorageError> {
        for key in self.keys.iter().rev() {
            let unlocked: bool =
                sqlx::query_scalar("SELECT pg_advisory_unlock(hashtextextended($1,0))")
                    .bind(key)
                    .fetch_one(
                        &mut **self
                            .connection
                            .as_mut()
                            .expect("live publication connection"),
                    )
                    .await?;
            if !unlocked {
                return Err(StorageError::Connection(
                    "publication namespace lock was lost".into(),
                ));
            }
        }
        // Only a fully unlocked connection may use the pool's normal Drop path.
        drop(self.connection.take());
        Ok(())
    }
}
impl Drop for PublicationLocks {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.as_mut() {
            connection.close_on_drop();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::backends::TestBackend;
    use std::time::Duration;

    async fn single_connection(pool: &PgPool) -> PgPool {
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap()
    }
    #[tokio::test]
    async fn publication_session_locks_reuse_only_unlocked_connections_and_restore_settings() {
        let Some(b) = TestBackend::postgres().await else {
            return;
        };
        let pool = single_connection(b.pg_pool()).await;
        sqlx::query("SELECT set_config('lock_timeout','125ms',false)")
            .execute(&pool)
            .await
            .unwrap();
        let tenant = TenantId::unchecked(format!("locks-{}", uuid::Uuid::now_v7()));
        let id = InstanceId::new();
        let mut guard = PublicationLocks::acquire(&pool, &tenant, vec![id, id])
            .await
            .unwrap();
        let pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(guard.connection())
            .await
            .unwrap();
        let timeout: String = sqlx::query_scalar("SHOW lock_timeout")
            .fetch_one(guard.connection())
            .await
            .unwrap();
        assert_eq!(timeout, "125ms");
        guard.release().await.unwrap();
        let reused: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            pid, reused,
            "normal release should retain the healthy pooled connection"
        );
        let available: bool =
            sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))")
                .bind(super::super::portable_effects::instance_lock_key(&tenant, id).unwrap())
                .fetch_one(b.pg_pool())
                .await
                .unwrap();
        assert!(available, "a session mutex leaked back into the pool");
        pool.close().await;
    }

    #[tokio::test]
    async fn publication_session_locks_release_on_cancellation_and_timeout() {
        let Some(b) = TestBackend::postgres().await else {
            return;
        };
        let pool = single_connection(b.pg_pool()).await;
        let first = InstanceId::from_uuid(uuid::Uuid::from_u128(1));
        let second = InstanceId::from_uuid(uuid::Uuid::from_u128(2));
        for cancel in [true, false] {
            let tenant = TenantId::unchecked(format!("locks-{}", uuid::Uuid::now_v7()));
            let key = super::super::portable_effects::instance_lock_key(&tenant, second).unwrap();
            let mut blocker = b.pg_pool().begin().await.unwrap();
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
                .bind(key)
                .execute(&mut *blocker)
                .await
                .unwrap();
            let blocking: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&mut *blocker)
                .await
                .unwrap();
            let old_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
                .fetch_one(&pool)
                .await
                .unwrap();
            let pool_copy = pool.clone();
            let scope = tenant.clone();
            let job = tokio::spawn(async move {
                PublicationLocks::acquire(&pool_copy, &scope, vec![second, first]).await
            });
            tokio::time::timeout(Duration::from_secs(2),async {
                loop {
                    let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))").bind(blocking).fetch_one(b.pg_pool()).await.unwrap();
                    if waiting {break;}
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("guard never reached second mutex wait");
            if cancel {
                job.abort();
                assert!(matches!(job.await, Err(error) if error.is_cancelled()));
            } else {
                assert!(matches!(
                    job.await.unwrap(),
                    Err(StorageError::Connection(_))
                ));
            }
            let new_pid: i32 = tokio::time::timeout(
                Duration::from_secs(3),
                sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&pool),
            )
            .await
            .unwrap()
            .unwrap();
            assert_ne!(
                old_pid, new_pid,
                "an abandoned session must not return to the pool"
            );
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let free: bool = sqlx::query_scalar(
                        "SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))",
                    )
                    .bind(
                        super::super::portable_effects::instance_lock_key(&tenant, first).unwrap(),
                    )
                    .fetch_one(b.pg_pool())
                    .await
                    .unwrap();
                    if free {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("cancelled or timed-out guard retained its first mutex");
            blocker.rollback().await.unwrap();
        }
        pool.close().await;
    }
}
