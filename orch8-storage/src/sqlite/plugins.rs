use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::plugin::{PluginDef, PluginType};

use super::helpers::parse_ts;
use super::SqliteStorage;

pub(super) async fn create(store: &SqliteStorage, plugin: &PluginDef) -> Result<(), StorageError> {
    let config_str = plugin.config.to_string();
    let created = plugin.created_at.to_rfc3339();
    let updated = plugin.updated_at.to_rfc3339();
    sqlx::query(
        r"INSERT INTO plugins (name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
    )
    .bind(&plugin.name)
    .bind(plugin.plugin_type.to_string())
    .bind(&plugin.source)
    .bind(&plugin.tenant_id)
    .bind(plugin.enabled)
    .bind(&config_str)
    .bind(&plugin.description)
    .bind(&created)
    .bind(&updated)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &SqliteStorage,
    name: &str,
) -> Result<Option<PluginDef>, StorageError> {
    let row: Option<PluginRow> = sqlx::query_as(
        r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
          FROM plugins WHERE name = ?1",
    )
    .bind(name)
    .fetch_optional(&store.pool)
    .await?;
    Ok(row.map(PluginRow::into_plugin))
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<PluginDef>, StorageError> {
    let rows: Vec<PluginRow> = match tenant_id {
        Some(tid) => {
            sqlx::query_as(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins WHERE tenant_id = ?1 OR tenant_id = '' ORDER BY name",
            )
            .bind(&tid.0)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins ORDER BY name",
            )
            .fetch_all(&store.pool)
            .await?
        }
    };
    Ok(rows.into_iter().map(PluginRow::into_plugin).collect())
}

pub(super) async fn update(store: &SqliteStorage, plugin: &PluginDef) -> Result<(), StorageError> {
    let config_str = plugin.config.to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        r"UPDATE plugins SET plugin_type=?2, source=?3, tenant_id=?4, enabled=?5,
          config=?6, description=?7, updated_at=?8
          WHERE name=?1",
    )
    .bind(&plugin.name)
    .bind(plugin.plugin_type.to_string())
    .bind(&plugin.source)
    .bind(&plugin.tenant_id)
    .bind(plugin.enabled)
    .bind(&config_str)
    .bind(&plugin.description)
    .bind(&now)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &SqliteStorage, name: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM plugins WHERE name = ?1")
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
    config: String,
    description: Option<String>,
    created_at: String,
    updated_at: String,
}

impl PluginRow {
    fn into_plugin(self) -> PluginDef {
        PluginDef {
            name: self.name,
            plugin_type: PluginType::from_str_loose(&self.plugin_type).unwrap_or(PluginType::Wasm),
            source: self.source,
            tenant_id: self.tenant_id,
            enabled: self.enabled,
            config: serde_json::from_str(&self.config).unwrap_or_default(),
            description: self.description,
            created_at: parse_ts(&self.created_at),
            updated_at: parse_ts(&self.updated_at),
        }
    }
}
