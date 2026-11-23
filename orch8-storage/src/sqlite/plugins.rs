use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;
use orch8_types::plugin::{PluginDef, PluginType};

use super::SqliteStorage;
use super::helpers::parse_ts;

pub(super) async fn reconcile_tenant_key(store: &SqliteStorage) -> Result<(), StorageError> {
    let mut tx = super::helpers::begin_immediate(&store.pool).await?;
    let tenant_is_key: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('plugins') WHERE name = 'tenant_id' AND pk = 1)",
    ).fetch_one(&mut *tx).await?;
    if !tenant_is_key {
        sqlx::raw_sql(r"
            CREATE TABLE orch8_plugins_v79 (
                name TEXT NOT NULL,
                plugin_type TEXT NOT NULL DEFAULT 'wasm',
                source TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT '',
                enabled INTEGER NOT NULL DEFAULT 1 CHECK(enabled IN (0, 1)),
                config TEXT NOT NULL DEFAULT '{}',
                description TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (tenant_id, name)
            );
            INSERT INTO orch8_plugins_v79 (name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at)
                SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at FROM plugins;
            DROP TABLE plugins;
            ALTER TABLE orch8_plugins_v79 RENAME TO plugins;
        ").execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

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
    tenant_id: Option<&TenantId>,
    name: &str,
) -> Result<Option<PluginDef>, StorageError> {
    const AMBIGUOUS: &str = "plugin alias is shared by multiple private tenants; select a tenant";
    if let Some(tenant) = tenant_id {
        let row: Option<PluginRow> = sqlx::query_as(
            r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
              FROM plugins WHERE name = ?1 AND (tenant_id = ?2 OR tenant_id = '')
              ORDER BY (tenant_id = ?2) DESC LIMIT 1",
        ).bind(name).bind(tenant.as_str()).fetch_optional(&store.pool).await?;
        return row.map(PluginRow::into_plugin).transpose();
    }
    let rows: Vec<PluginRow> = sqlx::query_as(
        r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
          FROM plugins WHERE name = ?1 ORDER BY (tenant_id = '') DESC, tenant_id LIMIT 2",
    ).bind(name).fetch_all(&store.pool).await?;
    if rows.len() > 1 && !rows[0].tenant_id.is_empty() {
        return Err(StorageError::Conflict(AMBIGUOUS.into()));
    }
    rows.into_iter()
        .next()
        .map(PluginRow::into_plugin)
        .transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<PluginDef>, StorageError> {
    let rows: Vec<PluginRow> = match tenant_id {
        Some(tid) => {
            sqlx::query_as(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins WHERE tenant_id = ?1 OR (tenant_id = '' AND NOT EXISTS (
                    SELECT 1 FROM plugins tenant_plugin WHERE tenant_plugin.tenant_id = ?1 AND tenant_plugin.name = plugins.name
                  )) ORDER BY name",
            )
            .bind(tid.as_str())
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as(
                r"SELECT name, plugin_type, source, tenant_id, enabled, config, description, created_at, updated_at
                  FROM plugins ORDER BY name, tenant_id",
            )
            .fetch_all(&store.pool)
            .await?
        }
    };
    rows.into_iter()
        .map(PluginRow::into_plugin)
        .collect::<Result<Vec<_>, _>>()
}

pub(super) async fn update(store: &SqliteStorage, plugin: &PluginDef) -> Result<(), StorageError> {
    let config_str = plugin.config.to_string();
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        r"UPDATE plugins SET plugin_type=?2, source=?3, enabled=?5,
          config=?6, description=?7, updated_at=?8
          WHERE name=?1 AND tenant_id=?4",
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
    if let Some(plugin) = get(store, None, name).await? {
        delete_exact(store, &plugin.tenant_id, name).await?;
    }
    Ok(())
}

pub(super) async fn delete_exact(
    store: &SqliteStorage,
    tenant_id: &str,
    name: &str,
) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM plugins WHERE name = ?1 AND tenant_id = ?2")
        .bind(name)
        .bind(tenant_id)
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
    fn into_plugin(self) -> Result<PluginDef, StorageError> {
        Ok(PluginDef {
            name: self.name,
            plugin_type: PluginType::from_str_loose(&self.plugin_type).ok_or_else(|| {
                StorageError::Query(format!("unknown plugin type: {}", self.plugin_type))
            })?,
            source: self.source,
            tenant_id: self.tenant_id,
            enabled: self.enabled,
            config: serde_json::from_str(&self.config).map_err(StorageError::Serialization)?,
            description: self.description,
            created_at: parse_ts(&self.created_at)?,
            updated_at: parse_ts(&self.updated_at)?,
        })
    }
}
