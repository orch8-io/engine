//! `SQLite` [`crate::AiStore`]: prompt registry, `llm_call` response cache,
//! tenant budgets. Mirrors `postgres/ai.rs`.

use async_trait::async_trait;
use chrono::{DateTime, SecondsFormat, Utc};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

use orch8_types::ai::{BudgetAlert, LlmCacheEntry, PromptLabel, PromptTemplate, TenantBudget};
use orch8_types::error::StorageError;

use super::SqliteStorage;

fn encode<T: serde::Serialize>(value: &T) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(StorageError::Serialization)
}

fn decode<T: serde::de::DeserializeOwned>(value: &str) -> Result<T, StorageError> {
    serde_json::from_str(value).map_err(StorageError::Serialization)
}

/// Fixed-width UTC timestamp so `TEXT` comparison orders chronologically.
fn ts(t: DateTime<Utc>) -> String {
    t.to_rfc3339_opts(SecondsFormat::Micros, true)
}

fn parse_ts(s: &str) -> Result<DateTime<Utc>, StorageError> {
    DateTime::parse_from_rfc3339(s)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| StorageError::Query(format!("bad timestamp {s:?}: {e}")))
}

fn label_from_row(row: &SqliteRow) -> Result<PromptLabel, StorageError> {
    let canary_version: Option<i32> = row.get("canary_version");
    let canary_percent: i64 = row.get("canary_percent");
    Ok(PromptLabel {
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        label: row.get("label"),
        version: row.get("version"),
        canary: canary_version.map(|version| orch8_types::ai::PromptCanary {
            version,
            percent: u8::try_from(canary_percent).unwrap_or(0),
        }),
        updated_at: parse_ts(&row.get::<String, _>("updated_at"))?,
    })
}

fn cache_from_row(row: &SqliteRow) -> Result<LlmCacheEntry, StorageError> {
    let embedding: Option<String> = row.get("embedding");
    Ok(LlmCacheEntry {
        tenant_id: row.get("tenant_id"),
        cache_key: row.get("cache_key"),
        partition_key: row.get("partition_key"),
        provider: row.get("provider"),
        model: row.get("model"),
        response: decode(&row.get::<String, _>("response"))?,
        embedding: embedding.as_deref().map(decode).transpose()?,
        input_tokens: row.get("input_tokens"),
        output_tokens: row.get("output_tokens"),
        size_bytes: row.get("size_bytes"),
        created_at: parse_ts(&row.get::<String, _>("created_at"))?,
        expires_at: parse_ts(&row.get::<String, _>("expires_at"))?,
    })
}

const CACHE_COLUMNS: &str = "tenant_id, cache_key, partition_key, provider, model, response, \
     embedding, input_tokens, output_tokens, size_bytes, created_at, expires_at";

#[async_trait]
impl crate::AiStore for SqliteStorage {
    async fn insert_prompt_version(&self, prompt: &PromptTemplate) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO prompt_versions (tenant_id, name, version, content_hash, record, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        )
        .bind(&prompt.tenant_id)
        .bind(&prompt.name)
        .bind(prompt.version)
        .bind(&prompt.content_hash)
        .bind(encode(prompt)?)
        .bind(ts(prompt.created_at))
        .execute(self.pool())
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
            "SELECT record FROM prompt_versions WHERE tenant_id = ?1 AND name = ?2 AND version = ?3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(version)
        .fetch_optional(self.pool())
        .await?;
        row.map(|r| decode(&r.get::<String, _>("record")))
            .transpose()
    }

    async fn get_latest_prompt_version(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<PromptTemplate>, StorageError> {
        let row = sqlx::query(
            "SELECT record FROM prompt_versions WHERE tenant_id = ?1 AND name = ?2
             ORDER BY version DESC LIMIT 1",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_optional(self.pool())
        .await?;
        row.map(|r| decode(&r.get::<String, _>("record")))
            .transpose()
    }

    async fn list_prompt_versions(
        &self,
        tenant_id: &str,
        name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<PromptTemplate>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM prompt_versions
             WHERE tenant_id = ?1 AND (?2 IS NULL OR name = ?2)
             ORDER BY name, version DESC LIMIT ?3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|r| decode(&r.get::<String, _>("record")))
            .collect()
    }

    async fn upsert_prompt_label(&self, label: &PromptLabel) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO prompt_labels
               (tenant_id, name, label, version, canary_version, canary_percent, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (tenant_id, name, label) DO UPDATE SET
               version = excluded.version,
               canary_version = excluded.canary_version,
               canary_percent = excluded.canary_percent,
               updated_at = excluded.updated_at",
        )
        .bind(&label.tenant_id)
        .bind(&label.name)
        .bind(&label.label)
        .bind(label.version)
        .bind(label.canary.map(|c| c.version))
        .bind(label.canary.map_or(0_i64, |c| i64::from(c.percent)))
        .bind(ts(label.updated_at))
        .execute(self.pool())
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
            "SELECT * FROM prompt_labels WHERE tenant_id = ?1 AND name = ?2 AND label = ?3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(label)
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(label_from_row).transpose()
    }

    async fn list_prompt_labels(
        &self,
        tenant_id: &str,
        name: Option<&str>,
    ) -> Result<Vec<PromptLabel>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM prompt_labels
             WHERE tenant_id = ?1 AND (?2 IS NULL OR name = ?2)
             ORDER BY name, label",
        )
        .bind(tenant_id)
        .bind(name)
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(label_from_row).collect()
    }

    async fn delete_prompt_label(
        &self,
        tenant_id: &str,
        name: &str,
        label: &str,
    ) -> Result<bool, StorageError> {
        let res = sqlx::query(
            "DELETE FROM prompt_labels WHERE tenant_id = ?1 AND name = ?2 AND label = ?3",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(label)
        .execute(self.pool())
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
             WHERE tenant_id = ?1 AND cache_key = ?2 AND expires_at > ?3"
        ))
        .bind(tenant_id)
        .bind(cache_key)
        .bind(ts(now))
        .fetch_optional(self.pool())
        .await?;
        row.as_ref().map(cache_from_row).transpose()
    }

    async fn put_llm_cache_entry(&self, entry: &LlmCacheEntry) -> Result<(), StorageError> {
        let embedding = entry.embedding.as_ref().map(encode).transpose()?;
        sqlx::query(
            "INSERT INTO llm_response_cache
               (tenant_id, cache_key, partition_key, provider, model, response, embedding,
                input_tokens, output_tokens, size_bytes, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
             ON CONFLICT (tenant_id, cache_key) DO UPDATE SET
               partition_key = excluded.partition_key,
               provider = excluded.provider,
               model = excluded.model,
               response = excluded.response,
               embedding = excluded.embedding,
               input_tokens = excluded.input_tokens,
               output_tokens = excluded.output_tokens,
               size_bytes = excluded.size_bytes,
               created_at = excluded.created_at,
               expires_at = excluded.expires_at",
        )
        .bind(&entry.tenant_id)
        .bind(&entry.cache_key)
        .bind(&entry.partition_key)
        .bind(&entry.provider)
        .bind(&entry.model)
        .bind(encode(&entry.response)?)
        .bind(embedding)
        .bind(entry.input_tokens)
        .bind(entry.output_tokens)
        .bind(entry.size_bytes)
        .bind(ts(entry.created_at))
        .bind(ts(entry.expires_at))
        .execute(self.pool())
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
             WHERE tenant_id = ?1 AND partition_key = ?2 AND expires_at > ?3
             ORDER BY created_at DESC LIMIT ?4"
        ))
        .bind(tenant_id)
        .bind(partition_key)
        .bind(ts(now))
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.iter().map(cache_from_row).collect()
    }

    async fn purge_llm_cache(&self, tenant_id: &str) -> Result<u64, StorageError> {
        let res = sqlx::query("DELETE FROM llm_response_cache WHERE tenant_id = ?1")
            .bind(tenant_id)
            .execute(self.pool())
            .await?;
        Ok(res.rows_affected())
    }

    async fn delete_expired_llm_cache(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        let res = sqlx::query(
            "DELETE FROM llm_response_cache WHERE rowid IN (
               SELECT rowid FROM llm_response_cache WHERE expires_at <= ?1 LIMIT ?2)",
        )
        .bind(ts(now))
        .bind(i64::from(limit))
        .execute(self.pool())
        .await?;
        Ok(res.rows_affected())
    }

    async fn upsert_tenant_budget(&self, budget: &TenantBudget) -> Result<(), StorageError> {
        let res = sqlx::query(
            "INSERT INTO tenant_budgets
               (id, tenant_id, model, period, limit_usd, hard_cap, record, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT (id) DO UPDATE SET
               model = excluded.model,
               period = excluded.period,
               limit_usd = excluded.limit_usd,
               hard_cap = excluded.hard_cap,
               record = excluded.record,
               updated_at = excluded.updated_at
             WHERE tenant_budgets.tenant_id = excluded.tenant_id",
        )
        .bind(budget.id.to_string())
        .bind(&budget.tenant_id)
        .bind(&budget.model)
        .bind(budget.period.as_str())
        .bind(budget.limit_usd)
        .bind(budget.hard_cap)
        .bind(encode(budget)?)
        .bind(ts(budget.created_at))
        .bind(ts(budget.updated_at))
        .execute(self.pool())
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
            "SELECT record FROM tenant_budgets WHERE tenant_id = ?1 ORDER BY created_at, id",
        )
        .bind(tenant_id)
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|r| decode(&r.get::<String, _>("record")))
            .collect()
    }

    async fn delete_tenant_budget(
        &self,
        tenant_id: &str,
        id: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        let res = sqlx::query("DELETE FROM tenant_budgets WHERE tenant_id = ?1 AND id = ?2")
            .bind(tenant_id)
            .bind(id.to_string())
            .execute(self.pool())
            .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn record_budget_alert(&self, alert: &BudgetAlert) -> Result<bool, StorageError> {
        let res = sqlx::query(
            "INSERT INTO tenant_budget_alerts
               (id, tenant_id, budget_id, period_start, threshold_percent, record, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT (budget_id, period_start, threshold_percent) DO NOTHING",
        )
        .bind(alert.id.to_string())
        .bind(&alert.tenant_id)
        .bind(alert.budget_id.to_string())
        .bind(ts(alert.period_start))
        .bind(i64::from(alert.threshold_percent))
        .bind(encode(alert)?)
        .bind(ts(alert.created_at))
        .execute(self.pool())
        .await?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_budget_alerts(
        &self,
        tenant_id: &str,
        limit: u32,
    ) -> Result<Vec<BudgetAlert>, StorageError> {
        let rows = sqlx::query(
            "SELECT record FROM tenant_budget_alerts WHERE tenant_id = ?1
             ORDER BY created_at DESC, threshold_percent DESC LIMIT ?2",
        )
        .bind(tenant_id)
        .bind(i64::from(limit))
        .fetch_all(self.pool())
        .await?;
        rows.iter()
            .map(|r| decode(&r.get::<String, _>("record")))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AiStore;
    use chrono::Duration;
    use orch8_types::ai::{BudgetPeriod, PromptCanary, PromptMessage};
    use serde_json::json;

    async fn store() -> SqliteStorage {
        SqliteStorage::in_memory().await.unwrap()
    }

    fn prompt(tenant: &str, name: &str, version: i32) -> PromptTemplate {
        let mut p = PromptTemplate {
            tenant_id: tenant.into(),
            name: name.into(),
            version,
            system: Some("sys".into()),
            messages: vec![PromptMessage {
                role: "user".into(),
                content: format!("v{version} {{{{ q }}}}"),
            }],
            variables: vec!["q".into()],
            model_params: json!({"model": "gpt-4o"}),
            response_schema: None,
            description: None,
            content_hash: String::new(),
            created_at: Utc::now(),
        };
        p.content_hash = p.compute_content_hash();
        p
    }

    #[tokio::test]
    async fn prompt_versions_are_immutable_and_tenant_scoped() {
        let s = store().await;
        s.insert_prompt_version(&prompt("t1", "triage", 1)).await.unwrap();
        s.insert_prompt_version(&prompt("t1", "triage", 2)).await.unwrap();
        s.insert_prompt_version(&prompt("t2", "triage", 1)).await.unwrap();
        let dup = s.insert_prompt_version(&prompt("t1", "triage", 2)).await;
        assert!(matches!(dup, Err(StorageError::Conflict(_))), "{dup:?}");

        let latest = s.get_latest_prompt_version("t1", "triage").await.unwrap().unwrap();
        assert_eq!(latest.version, 2);
        assert!(s.get_prompt_version("t1", "triage", 3).await.unwrap().is_none());
        assert_eq!(s.list_prompt_versions("t1", None, 100).await.unwrap().len(), 2);
        assert_eq!(s.list_prompt_versions("t2", Some("triage"), 100).await.unwrap().len(), 1);
        assert!(s.get_latest_prompt_version("t3", "triage").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn labels_upsert_move_and_require_existing_versions() {
        let s = store().await;
        s.insert_prompt_version(&prompt("t1", "p", 1)).await.unwrap();
        s.insert_prompt_version(&prompt("t1", "p", 2)).await.unwrap();
        let mut label = PromptLabel {
            tenant_id: "t1".into(),
            name: "p".into(),
            label: "production".into(),
            version: 1,
            canary: Some(PromptCanary {
                version: 2,
                percent: 10,
            }),
            updated_at: Utc::now(),
        };
        s.upsert_prompt_label(&label).await.unwrap();
        label.version = 2;
        label.canary = None;
        s.upsert_prompt_label(&label).await.unwrap();
        let got = s.get_prompt_label("t1", "p", "production").await.unwrap().unwrap();
        assert_eq!((got.version, got.canary), (2, None));
        assert!(s.get_prompt_label("t2", "p", "production").await.unwrap().is_none());

        label.version = 99;
        assert!(s.upsert_prompt_label(&label).await.is_err(), "FK to versions");
        assert_eq!(s.list_prompt_labels("t1", Some("p")).await.unwrap().len(), 1);
        assert!(s.delete_prompt_label("t1", "p", "production").await.unwrap());
        assert!(!s.delete_prompt_label("t1", "p", "production").await.unwrap());
    }

    fn entry(tenant: &str, key: &str, ttl: i64) -> LlmCacheEntry {
        let now = Utc::now();
        LlmCacheEntry {
            tenant_id: tenant.into(),
            cache_key: key.into(),
            partition_key: "part".into(),
            provider: "openai".into(),
            model: "gpt-4o".into(),
            response: json!({"message": {"content": key}}),
            embedding: Some(json!([0.1, 0.2])),
            input_tokens: 10,
            output_tokens: 5,
            size_bytes: 42,
            created_at: now,
            expires_at: now + Duration::seconds(ttl),
        }
    }

    #[tokio::test]
    async fn cache_respects_tenant_and_expiry() {
        let s = store().await;
        let now = Utc::now();
        s.put_llm_cache_entry(&entry("t1", "k1", 60)).await.unwrap();
        s.put_llm_cache_entry(&entry("t1", "old", -1)).await.unwrap();
        let hit = s.get_llm_cache_entry("t1", "k1", now).await.unwrap().unwrap();
        assert_eq!(hit.embedding, Some(json!([0.1, 0.2])));
        assert!(s.get_llm_cache_entry("t2", "k1", now).await.unwrap().is_none());
        assert!(s.get_llm_cache_entry("t1", "old", now).await.unwrap().is_none());
        assert_eq!(
            s.list_llm_cache_partition("t1", "part", now, 10).await.unwrap().len(),
            1
        );
        assert_eq!(s.delete_expired_llm_cache(now, 100).await.unwrap(), 1);
        assert_eq!(s.purge_llm_cache("t1").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn budgets_and_alerts_dedupe_per_period_threshold() {
        let s = store().await;
        let now = Utc::now();
        let budget = TenantBudget {
            id: uuid::Uuid::now_v7(),
            tenant_id: "t1".into(),
            model: None,
            period: BudgetPeriod::Monthly,
            limit_usd: 5.0,
            thresholds: vec![50, 100],
            hard_cap: true,
            created_at: now,
            updated_at: now,
        };
        s.upsert_tenant_budget(&budget).await.unwrap();
        let mut other_tenant = budget.clone();
        other_tenant.tenant_id = "t2".into();
        assert!(
            s.upsert_tenant_budget(&other_tenant).await.is_err(),
            "cannot hijack another tenant's budget id"
        );
        assert_eq!(s.list_tenant_budgets("t1").await.unwrap(), vec![budget.clone()]);
        assert!(s.list_tenant_budgets("t2").await.unwrap().is_empty());

        let alert = BudgetAlert {
            id: uuid::Uuid::now_v7(),
            event: orch8_types::ai::BUDGET_THRESHOLD_EVENT.into(),
            tenant_id: "t1".into(),
            budget_id: budget.id,
            model: None,
            period: BudgetPeriod::Monthly,
            period_start: now,
            threshold_percent: 50,
            spend_usd: 2.6,
            limit_usd: 5.0,
            blocking: false,
            created_at: now,
        };
        assert!(s.record_budget_alert(&alert).await.unwrap());
        let mut again = alert.clone();
        again.id = uuid::Uuid::now_v7();
        assert!(!s.record_budget_alert(&again).await.unwrap(), "once per period");
        assert_eq!(s.list_budget_alerts("t1", 10).await.unwrap().len(), 1);
        assert!(s.delete_tenant_budget("t1", budget.id).await.unwrap());
        assert!(!s.delete_tenant_budget("t2", budget.id).await.unwrap());
    }
}
