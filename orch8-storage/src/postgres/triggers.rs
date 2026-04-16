use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::trigger::{TriggerDef, TriggerType};

use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO triggers (slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(&trigger.tenant_id)
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(&trigger.secret)
    .bind(trigger.trigger_type.to_string())
    .bind(&trigger.config)
    .bind(trigger.created_at)
    .bind(trigger.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    slug: &str,
) -> Result<Option<TriggerDef>, StorageError> {
    let row = sqlx::query_as::<_, TriggerRow>(
        r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
          FROM triggers WHERE slug = $1",
    )
    .bind(slug)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(TriggerRow::into_trigger))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<TriggerDef>, StorageError> {
    let rows = match tenant_id {
        Some(tid) => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers WHERE tenant_id = $1 ORDER BY created_at",
            )
            .bind(&tid.0)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, TriggerRow>(
                r"SELECT slug, sequence_name, version, tenant_id, namespace, enabled, secret, trigger_type, config, created_at, updated_at
                  FROM triggers ORDER BY created_at",
            )
            .fetch_all(&store.pool)
            .await?
        }
    };
    Ok(rows.into_iter().map(TriggerRow::into_trigger).collect())
}

pub(super) async fn update(
    store: &PostgresStorage,
    trigger: &TriggerDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE triggers SET sequence_name=$2, version=$3, tenant_id=$4, namespace=$5,
          enabled=$6, secret=$7, trigger_type=$8, config=$9, updated_at=NOW()
          WHERE slug=$1",
    )
    .bind(&trigger.slug)
    .bind(&trigger.sequence_name)
    .bind(trigger.version)
    .bind(&trigger.tenant_id)
    .bind(&trigger.namespace)
    .bind(trigger.enabled)
    .bind(&trigger.secret)
    .bind(trigger.trigger_type.to_string())
    .bind(&trigger.config)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, slug: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM triggers WHERE slug = $1")
        .bind(slug)
        .execute(&store.pool)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct TriggerRow {
    slug: String,
    sequence_name: String,
    version: Option<i32>,
    tenant_id: String,
    namespace: String,
    enabled: bool,
    secret: Option<String>,
    trigger_type: String,
    config: serde_json::Value,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl TriggerRow {
    fn into_trigger(self) -> TriggerDef {
        TriggerDef {
            slug: self.slug,
            sequence_name: self.sequence_name,
            version: self.version,
            tenant_id: self.tenant_id,
            namespace: self.namespace,
            enabled: self.enabled,
            secret: self.secret,
            trigger_type: TriggerType::from_str_loose(&self.trigger_type)
                .unwrap_or_default(),
            config: self.config,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
