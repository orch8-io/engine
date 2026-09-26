//! `PostgreSQL` [`crate::AiStore`]: prompt registry, `llm_call` response
//! cache, tenant budgets (migrations 095–097).

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::Row;
use sqlx::postgres::PgRow;

use orch8_types::ai::{BudgetAlert, LlmCacheEntry, PromptLabel, PromptTemplate, TenantBudget};
use orch8_types::error::StorageError;

use super::PostgresStorage;

fn encode<T: serde::Serialize>(value: &T) -> Result<serde_json::Value, StorageError> {
    serde_json::to_value(value).map_err(StorageError::Serialization)
}

fn decode<T: serde::de::DeserializeOwned>(value: serde_json::Value) -> Result<T, StorageError> {
    serde_json::from_value(value).map_err(StorageError::Serialization)
}

fn label_from_row(row: &PgRow) -> PromptLabel {
    let canary_version: Option<i32> = row.get("canary_version");
    let canary_percent: i16 = row.get("canary_percent");
    PromptLabel {
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        label: row.get("label"),
        version: row.get("version"),
        canary: canary_version.map(|version| orch8_types::ai::PromptCanary {
            version,
            percent: u8::try_from(canary_percent).unwrap_or(0),
        }),
        updated_at: row.get("updated_at"),
    }
}

fn cache_from_row(row: &PgRow) -> LlmCacheEntry {
    LlmCacheEntry {
        tenant_id: row.get("tenant_id"),
        cache_key: row.get("cache_key"),
        partition_key: row.get("partition_key"),
        provider: row.get("provider"),
        model: row.get("model"),
        response: row.get("response"),
        embedding: row.get("embedding"),
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        size_bytes: row.get("size_bytes"),
        created_at: row.get("created_at"),
        expires_at: row.get("expires_at"),
    }
}

const CACHE_COLUMNS: &str = "tenant_id, cache_key, partition_key, provider, model, response, \
     embedding, input_tokens, output_tokens, size_bytes, created_at, expires_at";

#[async_trait]
impl crate::AiStore for PostgresStorage {
    async fn insert_prompt_version(&self, prompt: &PromptTemplate) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO prompt_versions (tenant_id, name, version, content_hash, record, created_at)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(&prompt.tenant_id)
        .bind(&prompt.name)
        .bind(prompt.version)
        .bind(&prompt.content_hash)
        .bind(encode(prompt)?)
        .bind(prompt.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_prompt_version(
        &self,
        tenant_id: &str,
        name: &str,
        version: i32,
    ) -> Result<Option<PromptTemplate>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM prompt_versions WHERE tenant_id = $1 AND name = $2 AND version = $3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| decode(r.get("record"))).transpose()
    }

    async fn get_latest_prompt_version(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<PromptTemplate>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM prompt_versions WHERE tenant_id = $1 AND name = $2
             ORDER BY version DESC LIMIT 1",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;
        row.map(|r| decode(r.get("record"))).transpose()
    }

    async fn list_prompt_versions(
        &self,
        tenant_id: &str,
        name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<PromptTemplate>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM prompt_versions
             WHERE tenant_id = $1 AND ($2::TEXT IS NULL OR name = $2)
             ORDER BY name, version DESC LIMIT $3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|r| decode(r.get("record"))).collect()
    }

    async fn upsert_prompt_label(&self, label: &PromptLabel) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO prompt_labels
               (tenant_id, name, label, version, canary_version, canary_percent, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (tenant_id, name, label) DO UPDATE SET
               version = EXCLUDED.version,
               canary_version = EXCLUDED.canary_version,
               canary_percent = EXCLUDED.canary_percent,
               updated_at = EXCLUDED.updated_at",
        )
        .bind(&label.tenant_id)
        .bind(&label.name)
        .bind(&label.label)
        .bind(label.version)
        .bind(label.canary.map(|c| c.version))
        .bind(label.canary.map_or(0_i16, |c| i16::from(c.percent)))
        .bind(label.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn get_prompt_label(
        &self,
        tenant_id: &str,
        name: &str,
        label: &str,
    ) -> Result<Option<PromptLabel>, StorageError> {
        let row = sqlx::query(
            "SELECT * FROM prompt_labels WHERE tenant_id = $1 AND name = $2 AND label = $3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(label)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(label_from_row))
    }

    async fn list_prompt_labels(
        &self,
        tenant_id: &str,
        name: Option<&str>,
    ) -> Result<Vec<PromptLabel>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM prompt_labels
             WHERE tenant_id = $1 AND ($2::TEXT IS NULL OR name = $2)
             ORDER BY name, label",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(label_from_row).collect())
    }

    async fn delete_prompt_label(
        &self,
        tenant_id: &str,
        name: &str,
        label: &str,
    ) -> Result<bool, StorageError> {
        let res = sqlx::query(
            "DELETE FROM prompt_labels WHERE tenant_id = $1 AND name = $2 AND label = $3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(label)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn get_llm_cache_entry(
        &self,
        tenant_id: &str,
        cache_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<LlmCacheEntry>, StorageError> {
        let row = sqlx::query(&format!(
            "SELECT {CACHE_COLUMNS} FROM llm_response_cache
             WHERE tenant_id = $1 AND cache_key = $2 AND expires_at > $3"
        ))
        .bind(tenant_id)
        .bind(cache_key)
        .bind(now)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(cache_from_row))
    }

    async fn put_llm_cache_entry(&self, entry: &LlmCacheEntry) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO llm_response_cache
               (tenant_id, cache_key, partition_key, provider, model, response, embedding,
                input_tokens, output_tokens, size_bytes, created_at, expires_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
             ON CONFLICT (tenant_id, cache_key) DO UPDATE SET
               partition_key = EXCLUDED.partition_key,
               provider = EXCLUDED.provider,
               model = EXCLUDED.model,
               response = EXCLUDED.response,
               embedding = EXCLUDED.embedding,
               input_tokens = EXCLUDED.input_tokens,
               output_tokens = EXCLUDED.output_tokens,
               size_bytes = EXCLUDED.size_bytes,
               created_at = EXCLUDED.created_at,
               expires_at = EXCLUDED.expires_at",
        )
        .bind(&entry.tenant_id)
        .bind(&entry.cache_key)
        .bind(&entry.partition_key)
        .bind(&entry.provider)
        .bind(&entry.model)
        .bind(&entry.response)
        .bind(&entry.embedding)
        .bind(entry.input_tokens)
        .bind(entry.output_tokens)
        .bind(entry.size_bytes)
        .bind(entry.created_at)
        .bind(entry.expires_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_llm_cache_partition(
        &self,
        tenant_id: &str,
        partition_key: &str,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<LlmCacheEntry>, StorageError> {
        let rows = sqlx::query(&format!(
            "SELECT {CACHE_COLUMNS} FROM llm_response_cache
             WHERE tenant_id = $1 AND partition_key = $2 AND expires_at > $3
             ORDER BY created_at DESC LIMIT $4"
        ))
        .bind(tenant_id)
        .bind(partition_key)
        .bind(now)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(cache_from_row).collect())
    }

    async fn purge_llm_cache(&self, tenant_id: &str) -> Result<u64, StorageError> {
        let res = sqlx::query("DELETE FROM llm_response_cache WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected())
    }

    async fn delete_expired_llm_cache(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        let res = sqlx::query(
            "DELETE FROM llm_response_cache WHERE ctid IN (
               SELECT ctid FROM llm_response_cache WHERE expires_at <= $1 LIMIT $2)",
        )
        .bind(now)
        .bind(i64::from(limit))
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected())
    }

    async fn upsert_tenant_budget(&self, budget: &TenantBudget) -> Result<(), StorageError> {
        let res = sqlx::query(
            "INSERT INTO tenant_budgets
               (id, tenant_id, model, period, limit_usd, hard_cap, record, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             ON CONFLICT (id) DO UPDATE SET
               model = EXCLUDED.model,
               period = EXCLUDED.period,
               limit_usd = EXCLUDED.limit_usd,
               hard_cap = EXCLUDED.hard_cap,
               record = EXCLUDED.record,
               updated_at = EXCLUDED.updated_at
             WHERE tenant_budgets.tenant_id = EXCLUDED.tenant_id",
        )
        .bind(budget.id)
        .bind(&budget.tenant_id)
        .bind(&budget.model)
        .bind(budget.period.as_str())
        .bind(budget.limit_usd)
        .bind(budget.hard_cap)
        .bind(encode(budget)?)
        .bind(budget.created_at)
        .bind(budget.updated_at)
        .execute(&self.pool)
        .await?;
        if res.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "tenant_budget",
                id: budget.id.to_string(),
            });
        }
        Ok(())
    }

    async fn list_tenant_budgets(&self, tenant_id: &str) -> Result<Vec<TenantBudget>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM tenant_budgets WHERE tenant_id = $1 ORDER BY created_at, id",
        )
        .bind(tenant_id)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|r| decode(r.get("record"))).collect()
    }

    async fn delete_tenant_budget(
        &self,
        tenant_id: &str,
        id: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        let res = sqlx::query("DELETE FROM tenant_budgets WHERE tenant_id = $1 AND id = $2")
            .bind(tenant_id)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn record_budget_alert(&self, alert: &BudgetAlert) -> Result<bool, StorageError> {
        let res = sqlx::query(
            "INSERT INTO tenant_budget_alerts
               (id, tenant_id, budget_id, period_start, threshold_percent, record, created_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (budget_id, period_start, threshold_percent) DO NOTHING",
        )
        .bind(alert.id)
        .bind(&alert.tenant_id)
        .bind(alert.budget_id)
        .bind(alert.period_start)
        .bind(i16::from(alert.threshold_percent))
        .bind(encode(alert)?)
        .bind(alert.created_at)
        .execute(&self.pool)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_budget_alerts(
        &self,
        tenant_id: &str,
        limit: u32,
    ) -> Result<Vec<BudgetAlert>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM tenant_budget_alerts WHERE tenant_id = $1
             ORDER BY created_at DESC, threshold_percent DESC LIMIT $2",
        )
        .bind(tenant_id)
        .bind(i64::from(limit))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(|r| decode(r.get("record"))).collect()
    }
}
