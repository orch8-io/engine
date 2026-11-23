//! Offline integrity checks over the same consistent source used by backup.
use std::sync::Arc;

use anyhow::{Context as _, Result, bail};
use clap::Args;
use orch8_storage::StorageBackend;
use orch8_storage::encrypting::{EncryptingStorage, is_vault_reference};
use orch8_storage::externalizing::visit_context_refs;
use orch8_types::config::SecretString;
use orch8_types::encryption::FieldEncryptor;
use orch8_types::filter::InstanceFilter;
use orch8_types::ids::InstanceId;
use serde::Serialize;

use super::{PAGE, SourceGuard, open_source, parse_target, sha256_hex};
use crate::OutputFormat;

#[derive(Debug, Args)]
pub struct VerifyExternalizedCmd {
    /// Source database URL or `SQLite` path.
    #[arg(long, env = "ORCH8_DATABASE_URL", hide_env_values = true)]
    database_url: String,
    /// Encryption key used by the source. Prefer the environment variable.
    #[arg(long, env = "ORCH8_ENCRYPTION_KEY", hide_env_values = true)]
    encryption_key: Option<SecretString>,
}

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum IssueKind {
    Missing,
    WrongOwner,
    UnreadablePayload,
    UnreadableContext,
    VaultPayloadUnverified,
}

#[derive(Debug, Serialize)]
struct Issue {
    instance_id: InstanceId,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference_sha256: Option<String>,
    kind: IssueKind,
}

#[derive(Default, Serialize)]
struct Report {
    instances: usize,
    references: usize,
    issue_count: usize,
    issues: Vec<Issue>,
}

impl Report {
    fn record(&mut self, instance_id: InstanceId, reference: Option<&str>, kind: IssueKind) {
        self.issue_count += 1;
        if self.issues.len() < 1_000 {
            self.issues.push(Issue {
                instance_id,
                reference_sha256: reference.map(|key| sha256_hex(key.as_bytes())),
                kind,
            });
        }
    }
}

impl SourceGuard {
    // Scoped payload reads intentionally return None for another owner.
    // The offline command can distinguish those two cases using metadata
    // from the same database snapshot, without reading the other payload.
    async fn reference_exists(&self, key: &str) -> Result<bool> {
        let found = if let Some(pool) = &self.sqlite_pool {
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM externalized_state WHERE ref_key = ?")
                .bind(key)
                .fetch_optional(pool)
                .await?
                .is_some()
        } else if let Some(pool) = &self.postgres_pool {
            sqlx::query_scalar::<_, i32>("SELECT 1 FROM externalized_state WHERE ref_key = $1")
                .bind(key)
                .fetch_optional(pool)
                .await?
                .is_some()
        } else {
            bail!("missing integrity scan source");
        };
        Ok(found)
    }
}

async fn check_reference(
    storage: &dyn StorageBackend,
    source: &SourceGuard,
    id: InstanceId,
    key: &str,
    report: &mut Report,
) -> Result<()> {
    report.references += 1;
    let kind = match storage.get_externalized_state(id, key).await {
        Ok(Some(value)) if is_vault_reference(&value) => IssueKind::VaultPayloadUnverified,
        Ok(Some(value)) if !FieldEncryptor::is_encrypted(&value) => return Ok(()),
        Ok(Some(_)) | Err(_) => IssueKind::UnreadablePayload,
        Ok(None) if source.reference_exists(key).await? => IssueKind::WrongOwner,
        Ok(None) => IssueKind::Missing,
    };
    report.record(id, Some(key), kind);
    Ok(())
}

async fn scan(
    raw: &dyn StorageBackend,
    storage: &dyn StorageBackend,
    source: &SourceGuard,
) -> Result<Report> {
    let mut report = Report::default();
    let mut cursor = None;
    loop {
        let page = raw
            .list_instances_keyset(&InstanceFilter::default(), cursor, PAGE)
            .await
            .context("list instances for integrity verification")?;
        if page.is_empty() {
            return Ok(report);
        }
        cursor = page.last().map(|instance| instance.id);
        for mut instance in page {
            let id = instance.id;
            report.instances += 1;
            if FieldEncryptor::is_encrypted(&instance.context.data) {
                if let Ok(Some(decoded)) = storage.get_instance(id).await
                    && !FieldEncryptor::is_encrypted(&decoded.context.data)
                {
                    instance.context.data = decoded.context.data;
                } else {
                    report.record(id, None, IssueKind::UnreadableContext);
                }
            }
            let mut refs = Vec::new();
            visit_context_refs(&instance.context.data, |key, _| refs.push(key.to_owned()));
            for key in refs {
                check_reference(storage, source, id, &key, &mut report).await?;
            }
            for output in raw
                .get_all_outputs(id)
                .await
                .context("list block outputs")?
            {
                if let Some(key) = payload_reference(output.output_ref.as_deref()) {
                    check_reference(storage, source, id, key, &mut report).await?;
                }
            }
        }
    }
}

fn payload_reference(reference: Option<&str>) -> Option<&str> {
    reference.filter(|key| !matches!(*key, "__error__" | "__retry__" | "__in_progress__"))
}

pub async fn run_verify_externalized(
    cmd: VerifyExternalizedCmd,
    format: OutputFormat,
) -> Result<()> {
    let key = cmd
        .encryption_key
        .as_ref()
        .filter(|key| !key.expose().is_empty());
    let encryptor = key
        .map(|key| FieldEncryptor::from_hex_key(key.expose()))
        .transpose()
        .context("invalid externalized-state encryption key")?;
    let (raw, source) = open_source(&parse_target(&cmd.database_url)?).await?;
    let storage: Arc<dyn StorageBackend> = match encryptor {
        Some(encryptor) => Arc::new(EncryptingStorage::new(raw.clone(), encryptor)),
        None => raw.clone(),
    };
    let report = scan(raw.as_ref(), storage.as_ref(), &source).await;
    source.close().await;
    let report = report?;
    match format {
        OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
        OutputFormat::Table => {
            println!(
                "Checked {} references across {} instances. Found {} issues.",
                report.references, report.instances, report.issue_count,
            );
            for issue in &report.issues {
                println!(
                    "{} {:?} {}",
                    issue.instance_id,
                    issue.kind,
                    issue.reference_sha256.as_deref().unwrap_or("-"),
                );
            }
        }
    }
    if report.issue_count > 0 {
        bail!(
            "externalized state verification found {} issues",
            report.issue_count
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_bookkeeping_markers_are_not_payload_references() {
        for sentinel in ["__error__", "__retry__", "__in_progress__"] {
            assert_eq!(payload_reference(Some(sentinel)), None);
        }
        assert_eq!(payload_reference(None), None);
        assert_eq!(payload_reference(Some("output:ref")), Some("output:ref"));
    }

    #[test]
    fn integrity_issue_report_contains_a_digest_instead_of_the_reference() {
        let mut report = Report::default();
        report.record(
            InstanceId::new(),
            Some("private-key"),
            IssueKind::WrongOwner,
        );
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains(&sha256_hex(b"private-key")));
        assert!(!json.contains("private-key"));
    }
}
