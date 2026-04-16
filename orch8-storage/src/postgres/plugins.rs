use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::plugin::{PluginDef, PluginType};

use super::PostgresStorage;

pub(super) async fn create(
    store: &PostgresStorage,
    plugin: &PluginDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"INSERT INTO plugins (name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(&plugin.name)
    .bind(plugin.plugin_type.to_string())
    .bind(&plugin.source)
    .bind(&plugin.tenant_id)
    .bind(plugin.enabled)
    .bind(&plugin.config)
    .bind(&plugin.description)
    .bind(plugin.created_at)
    .bind(plugin.updated_at)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &PostgresStorage,
    name: &str,
) -> Result<Option<PluginDef>, StorageError> {
    let row = sqlx::query_as::<_, PluginRow>(
        r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
          FROM plugins WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(PluginRow::into_plugin))
}

pub(super) async fn list(
    store: &PostgresStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<PluginDef>, StorageError> {
    let rows = match tenant_id {
        Some(tid) => {
            sqlx::query_as::<_, PluginRow>(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins WHERE tenant_id = $1 OR tenant_id = '' ORDER BY name",
            )
            .bind(&tid.0)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as::<_, PluginRow>(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins ORDER BY name",
            )
            .fetch_all(&store.pool)
            .await?
        }
    };
    Ok(rows.into_iter().map(PluginRow::into_plugin).collect())
}

pub(super) async fn update(
    store: &PostgresStorage,
    plugin: &PluginDef,
) -> Result<(), StorageError> {
    sqlx::query(
        r"UPDATE plugins SET plugin_type=$2, source=$3, tenant_id=$4, enabled=$5,
          config=$6, description=$7, updated_at=NOW()
          WHERE name=$1",
    )
    .bind(&plugin.name)
    .bind(plugin.plugin_type.to_string())
    .bind(&plugin.source)
    .bind(&plugin.tenant_id)
    .bind(plugin.enabled)
    .bind(&plugin.config)
    .bind(&plugin.description)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &PostgresStorage, name: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM plugins WHERE name = $1")
        .bind(name)
        .execute(&store.pool)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct PluginRow {
    name: String,
    plugin_type: String,
    source: String,
    tenant_id: String,
    enabled: bool,
    config: serde_json::Value,
    description: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

impl PluginRow {
    fn into_plugin(self) -> PluginDef {
        PluginDef {
            name: self.name,
            plugin_type: PluginType::from_str_loose(&self.plugin_type)
                .unwrap_or(PluginType::Wasm),
            source: self.source,
            tenant_id: self.tenant_id,
            enabled: self.enabled,
            config: self.config,
            description: self.description,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}
