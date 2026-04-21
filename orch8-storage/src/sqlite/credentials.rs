use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::error::StorageError;
use orch8_types::ids::TenantId;

use super::helpers::parse_ts;
use super::SqliteStorage;

pub(super) async fn create(
    store: &SqliteStorage,
    credential: &CredentialDef,
) -> Result<(), StorageError> {
    let expires = credential.expires_at.map(|t| t.to_rfc3339());
    let created = credential.created_at.to_rfc3339();
    let updated = credential.updated_at.to_rfc3339();
    sqlx::query(
        r"INSERT INTO credentials (id, tenant_id, name, kind, value, expires_at, refresh_url, refresh_token, enabled, description, created_at, updated_at)
          VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
    )
    .bind(&credential.id)
    .bind(&credential.tenant_id)
    .bind(&credential.name)
    .bind(credential.kind.to_string())
    .bind(credential.value.expose().to_string())
    .bind(&expires)
    .bind(&credential.refresh_url)
    .bind(credential.refresh_token.as_ref().map(|s| s.expose().to_string()))
    .bind(credential.enabled)
    .bind(&credential.description)
    .bind(&created)
    .bind(&updated)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn get(
    store: &SqliteStorage,
    id: &str,
) -> Result<Option<CredentialDef>, StorageError> {
    let row: Option<CredentialRow> = sqlx::query_as(
        r"SELECT id, tenant_id, name, kind, value, expires_at, refresh_url, refresh_token, enabled, description, created_at, updated_at
          FROM credentials WHERE id = ?1",
    )
    .bind(id)
    .fetch_optional(&store.pool)
    .await?;
    row.map(CredentialRow::into_credential).transpose()
}

pub(super) async fn list(
    store: &SqliteStorage,
    tenant_id: Option<&TenantId>,
) -> Result<Vec<CredentialDef>, StorageError> {
    let rows: Vec<CredentialRow> = match tenant_id {
        Some(tid) => {
            sqlx::query_as(
                r"SELECT id, tenant_id, name, kind, value, expires_at, refresh_url, refresh_token, enabled, description, created_at, updated_at
                  FROM credentials WHERE tenant_id = ?1 OR tenant_id = '' ORDER BY id",
            )
            .bind(&tid.0)
            .fetch_all(&store.pool)
            .await?
        }
        None => {
            sqlx::query_as(
                r"SELECT id, tenant_id, name, kind, value, expires_at, refresh_url, refresh_token, enabled, description, created_at, updated_at
                  FROM credentials ORDER BY id",
            )
            .fetch_all(&store.pool)
            .await?
        }
    };
    rows.into_iter()
        .map(CredentialRow::into_credential)
        .collect::<Result<Vec<_>, _>>()
}

pub(super) async fn update(
    store: &SqliteStorage,
    credential: &CredentialDef,
) -> Result<(), StorageError> {
    let expires = credential.expires_at.map(|t| t.to_rfc3339());
    let now = chrono::Utc::now().to_rfc3339();
    sqlx::query(
        r"UPDATE credentials SET tenant_id=?2, name=?3, kind=?4, value=?5,
          expires_at=?6, refresh_url=?7, refresh_token=?8, enabled=?9, description=?10,
          updated_at=?11
          WHERE id=?1",
    )
    .bind(&credential.id)
    .bind(&credential.tenant_id)
    .bind(&credential.name)
    .bind(credential.kind.to_string())
    .bind(credential.value.expose().to_string())
    .bind(&expires)
    .bind(&credential.refresh_url)
    .bind(
        credential
            .refresh_token
            .as_ref()
            .map(|s| s.expose().to_string()),
    )
    .bind(credential.enabled)
    .bind(&credential.description)
    .bind(&now)
    .execute(&store.pool)
    .await?;
    Ok(())
}

pub(super) async fn delete(store: &SqliteStorage, id: &str) -> Result<(), StorageError> {
    sqlx::query("DELETE FROM credentials WHERE id = ?1")
        .bind(id)
        .execute(&store.pool)
        .await?;
    Ok(())
}

pub(super) async fn list_due_for_refresh(
    store: &SqliteStorage,
    threshold: std::time::Duration,
) -> Result<Vec<CredentialDef>, StorageError> {
    let cutoff = chrono::Utc::now()
        + chrono::Duration::from_std(threshold).unwrap_or(chrono::Duration::seconds(300));
    let cutoff_str = cutoff.to_rfc3339();
    let rows: Vec<CredentialRow> = sqlx::query_as(
        r"SELECT id, tenant_id, name, kind, value, expires_at, refresh_url, refresh_token, enabled, description, created_at, updated_at
          FROM credentials
          WHERE kind = 'oauth2' AND enabled = 1
            AND refresh_url IS NOT NULL AND refresh_token IS NOT NULL
            AND expires_at IS NOT NULL AND expires_at <= ?1
          ORDER BY expires_at",
    )
    .bind(&cutoff_str)
    .fetch_all(&store.pool)
    .await?;
    rows.into_iter()
        .map(CredentialRow::into_credential)
        .collect::<Result<Vec<_>, _>>()
}

#[derive(sqlx::FromRow)]
struct CredentialRow {
    id: String,
    tenant_id: String,
    name: String,
    kind: String,
    value: String,
    expires_at: Option<String>,
    refresh_url: Option<String>,
    refresh_token: Option<String>,
    enabled: bool,
    description: Option<String>,
    created_at: String,
    updated_at: String,
}

impl CredentialRow {
    fn into_credential(self) -> Result<CredentialDef, StorageError> {
        Ok(CredentialDef {
            id: self.id,
            tenant_id: self.tenant_id,
            name: self.name,
            kind: CredentialKind::from_str_loose(&self.kind).ok_or_else(|| {
                StorageError::Query(format!("unknown credential kind: {}", self.kind))
            })?,
            value: orch8_types::config::SecretString::new(self.value),
            expires_at: self.expires_at.as_deref().map(parse_ts).transpose()?,
            refresh_url: self.refresh_url,
            refresh_token: self
                .refresh_token
                .map(orch8_types::config::SecretString::new),
            enabled: self.enabled,
            description: self.description,
            created_at: parse_ts(&self.created_at)?,
            updated_at: parse_ts(&self.updated_at)?,
        })
    }
}
