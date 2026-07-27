//! Authoritative tenant-to-storage placement and fail-closed routing.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;

use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use crate::StorageBackend;
use crate::postgres::PostgresStorage;
use crate::sqlite::SqliteStorage;

/// Durable control-plane record selecting a storage backend for one tenant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantStoragePlacement {
    pub tenant_id: TenantId,
    pub backend_id: String,
    /// Monotonically increasing fencing epoch. Stale writers cannot move a tenant.
    pub epoch: i64,
    pub updated_at: DateTime<Utc>,
}

/// Narrow control-plane interface, deliberately separate from `StorageBackend`.
#[async_trait]
pub trait TenantPlacementStore: Send + Sync + 'static {
    async fn get_tenant_placement(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantStoragePlacement>, StorageError>;

    /// Insert or advance a placement. The epoch must be positive and strictly
    /// greater than the existing epoch.
    async fn advance_tenant_placement(
        &self,
        placement: &TenantStoragePlacement,
    ) -> Result<(), StorageError>;
}

/// A routed backend paired with the authoritative placement used to select it.
#[derive(Clone)]
pub struct RoutedTenantStorage {
    pub placement: TenantStoragePlacement,
    pub backend: Arc<dyn StorageBackend>,
}

/// Routes each request from a durable placement record. There is intentionally
/// no default backend: absent or invalid control-plane state fails closed.
pub struct TenantPartitionRouter {
    placements: Arc<dyn TenantPlacementStore>,
    backends: HashMap<String, Arc<dyn StorageBackend>>,
}

impl TenantPartitionRouter {
    #[must_use]
    pub fn new(placements: Arc<dyn TenantPlacementStore>) -> Self {
        Self {
            placements,
            backends: HashMap::new(),
        }
    }

    pub fn register_backend(
        &mut self,
        backend_id: impl Into<String>,
        backend: Arc<dyn StorageBackend>,
    ) -> Result<(), StorageError> {
        let backend_id = backend_id.into();
        validate_backend_id(&backend_id)?;
        match self.backends.entry(backend_id.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(backend);
            }
            Entry::Occupied(_) => {
                return Err(StorageError::Conflict(format!(
                    "storage backend '{backend_id}' is already registered"
                )));
            }
        }
        Ok(())
    }

    pub async fn route(&self, tenant_id: &TenantId) -> Result<RoutedTenantStorage, StorageError> {
        let placement = self
            .placements
            .get_tenant_placement(tenant_id)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "tenant storage placement",
                id: tenant_id.to_string(),
            })?;
        let backend = self.backends.get(&placement.backend_id).ok_or_else(|| {
            StorageError::Unsupported(format!(
                "tenant '{}' is assigned to unregistered storage backend '{}'",
                tenant_id, placement.backend_id
            ))
        })?;
        Ok(RoutedTenantStorage {
            placement,
            backend: Arc::clone(backend),
        })
    }
}

fn validate_backend_id(backend_id: &str) -> Result<(), StorageError> {
    if backend_id.is_empty()
        || backend_id.len() > 128
        || !backend_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(StorageError::Unsupported(
            "storage backend id must be 1-128 ASCII letters, digits, '.', '-' or '_'".into(),
        ));
    }
    Ok(())
}

fn validate_placement(placement: &TenantStoragePlacement) -> Result<(), StorageError> {
    validate_backend_id(&placement.backend_id)?;
    if placement.epoch <= 0 {
        return Err(StorageError::Conflict(
            "tenant storage placement epoch must be positive".into(),
        ));
    }
    Ok(())
}

#[async_trait]
impl TenantPlacementStore for SqliteStorage {
    async fn get_tenant_placement(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantStoragePlacement>, StorageError> {
        let row = sqlx::query(
            "SELECT tenant_id, backend_id, epoch, updated_at FROM tenant_storage_placements WHERE tenant_id = ?1",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            let timestamp = row.get::<String, _>("updated_at");
            Ok(TenantStoragePlacement {
                tenant_id: TenantId::new(row.get::<String, _>("tenant_id"))
                    .map_err(StorageError::Query)?,
                backend_id: row.get("backend_id"),
                epoch: row.get("epoch"),
                updated_at: DateTime::parse_from_rfc3339(&timestamp)
                    .map_err(|error| StorageError::Query(error.to_string()))?
                    .with_timezone(&Utc),
            })
        })
        .transpose()
    }

    async fn advance_tenant_placement(
        &self,
        placement: &TenantStoragePlacement,
    ) -> Result<(), StorageError> {
        validate_placement(placement)?;
        let result = sqlx::query(
            "INSERT INTO tenant_storage_placements (tenant_id, backend_id, epoch, updated_at) VALUES (?1, ?2, ?3, ?4) ON CONFLICT(tenant_id) DO UPDATE SET backend_id = excluded.backend_id, epoch = excluded.epoch, updated_at = excluded.updated_at WHERE excluded.epoch > tenant_storage_placements.epoch",
        )
        .bind(placement.tenant_id.as_str())
        .bind(&placement.backend_id)
        .bind(placement.epoch)
        .bind(placement.updated_at.to_rfc3339())
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(format!(
                "tenant '{}' placement epoch {} is not newer",
                placement.tenant_id, placement.epoch
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl TenantPlacementStore for PostgresStorage {
    async fn get_tenant_placement(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Option<TenantStoragePlacement>, StorageError> {
        let row = sqlx::query(
            "SELECT tenant_id, backend_id, epoch, updated_at FROM tenant_storage_placements WHERE tenant_id = $1",
        )
        .bind(tenant_id.as_str())
        .fetch_optional(&self.pool)
        .await?;
        row.map(|row| {
            Ok(TenantStoragePlacement {
                tenant_id: TenantId::new(row.get::<String, _>("tenant_id"))
                    .map_err(StorageError::Query)?,
                backend_id: row.get("backend_id"),
                epoch: row.get("epoch"),
                updated_at: row.get("updated_at"),
            })
        })
        .transpose()
    }

    async fn advance_tenant_placement(
        &self,
        placement: &TenantStoragePlacement,
    ) -> Result<(), StorageError> {
        validate_placement(placement)?;
        let result = sqlx::query(
            "INSERT INTO tenant_storage_placements (tenant_id, backend_id, epoch, updated_at) VALUES ($1, $2, $3, $4) ON CONFLICT(tenant_id) DO UPDATE SET backend_id = EXCLUDED.backend_id, epoch = EXCLUDED.epoch, updated_at = EXCLUDED.updated_at WHERE EXCLUDED.epoch > tenant_storage_placements.epoch",
        )
        .bind(placement.tenant_id.as_str())
        .bind(&placement.backend_id)
        .bind(placement.epoch)
        .bind(placement.updated_at)
        .execute(&self.pool)
        .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Conflict(format!(
                "tenant '{}' placement epoch {} is not newer",
                placement.tenant_id, placement.epoch
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement(tenant: &str, backend: &str, epoch: i64) -> TenantStoragePlacement {
        TenantStoragePlacement {
            tenant_id: TenantId::new(tenant).unwrap(),
            backend_id: backend.into(),
            epoch,
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn routes_by_authoritative_placement_and_fails_closed() {
        let control = Arc::new(SqliteStorage::in_memory().await.unwrap());
        let backend_a: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let backend_b: Arc<dyn StorageBackend> =
            Arc::new(SqliteStorage::in_memory().await.unwrap());
        let placement_store: Arc<dyn TenantPlacementStore> = control.clone();
        let mut router = TenantPartitionRouter::new(placement_store);
        router.register_backend("shard-a", backend_a).unwrap();
        router.register_backend("shard-b", backend_b).unwrap();

        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();
        control
            .advance_tenant_placement(&placement("tenant-a", "shard-a", 1))
            .await
            .unwrap();
        control
            .advance_tenant_placement(&placement("tenant-b", "shard-b", 1))
            .await
            .unwrap();

        assert_eq!(
            router.route(&tenant_a).await.unwrap().placement.backend_id,
            "shard-a"
        );
        assert_eq!(
            router.route(&tenant_b).await.unwrap().placement.backend_id,
            "shard-b"
        );
        assert!(matches!(
            router.route(&TenantId::new("unplaced").unwrap()).await,
            Err(StorageError::NotFound { .. })
        ));

        control
            .advance_tenant_placement(&placement("tenant-a", "shard-b", 2))
            .await
            .unwrap();
        assert_eq!(
            router.route(&tenant_a).await.unwrap().placement.backend_id,
            "shard-b"
        );
    }

    #[tokio::test]
    async fn fences_stale_updates_and_unknown_backends() {
        let control = Arc::new(SqliteStorage::in_memory().await.unwrap());
        control
            .advance_tenant_placement(&placement("tenant-a", "missing", 2))
            .await
            .unwrap();
        assert!(matches!(
            control
                .advance_tenant_placement(&placement("tenant-a", "shard-a", 2))
                .await,
            Err(StorageError::Conflict(_))
        ));

        let placement_store: Arc<dyn TenantPlacementStore> = control;
        let router = TenantPartitionRouter::new(placement_store);
        assert!(matches!(
            router.route(&TenantId::new("tenant-a").unwrap()).await,
            Err(StorageError::Unsupported(_))
        ));
    }
}

#[cfg(test)]
#[path = "tenant_partition_coverage_tests.rs"]
mod tenant_partition_coverage_tests;
