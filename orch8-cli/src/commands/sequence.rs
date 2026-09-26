use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use uuid::Uuid;

use crate::atomic_write;
use crate::{OutputFormat, print_response};

#[derive(Subcommand)]
pub enum SequenceCmd {
    /// Create a sequence from a JSON or YAML file.
    Create {
        /// Path to the definition file (`.json`, `.yaml`, or `.yml`).
        #[arg(long, short)]
        file: PathBuf,
    },
    /// Get a sequence by ID.
    Get { id: Uuid },
    /// Look up a sequence by name.
    Lookup {
        tenant_id: String,
        namespace: String,
        name: String,
        #[arg(long)]
        version: Option<i32>,
    },
    /// List all versions of a sequence.
    Versions {
        tenant_id: String,
        namespace: String,
        name: String,
    },
    /// Deprecate a sequence version.
    Deprecate { id: Uuid },
    /// Rebind a non-terminal instance to another stored sequence version.
    MigrateInstance {
        instance_id: Uuid,
        target_sequence_id: Uuid,
    },
    /// Git-ops apply: diff a local sequence definition against the server and,
    /// on change, upload it with the version bumped. Accepts a file or a
    /// directory of `.json` / `.yaml` / `.yml` files. Idempotent — an
    /// unchanged sequence is left alone.
    Apply {
        /// Path to a sequence file (JSON or YAML) or a directory of them.
        path: PathBuf,
        /// Show what would change without applying.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run a readiness preflight: definition validity, lint, workers,
    /// version pins, credentials, plugins, queues, and sub-sequences.
    /// Exits non-zero unless the report is pass/warning (CI-friendly).
    Preflight {
        /// Stored sequence id to check.
        #[arg(long, conflicts_with = "file")]
        id: Option<Uuid>,
        /// Local draft definition (JSON or YAML) to check instead of a stored sequence.
        #[arg(long, short)]
        file: Option<PathBuf>,
    },
    /// Compile typed producer/consumer references and generate deterministic
    /// TypeScript, Python, Swift, Kotlin, and canonical schema artifacts.
    Dataflow {
        /// Stored sequence id to compile.
        #[arg(long, conflicts_with = "file")]
        id: Option<Uuid>,
        /// Local draft definition (JSON or YAML) to compile instead of a stored sequence.
        #[arg(long, short)]
        file: Option<PathBuf>,
        /// Atomically write TS/Python/Swift/Kotlin types, schema, and report.
        #[arg(long)]
        out_dir: Option<PathBuf>,
    },
    /// Upgrade a sequence document to the current persisted format.
    UpgradeFormat {
        /// Existing sequence document (JSON or YAML).
        file: PathBuf,
        /// Destination (`.json` / `.yaml`); omit to print upgraded JSON to stdout.
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

fn upgrade_block_types(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(items) => {
            for item in items {
                upgrade_block_types(item);
            }
        }
        serde_json::Value::Object(object) => {
            if object.get("type").and_then(serde_json::Value::as_str) == Some("a_b_split") {
                object.insert("type".into(), serde_json::json!("ab_split"));
            }
            for child in object.values_mut() {
                upgrade_block_types(child);
            }
        }
        _ => {}
    }
}

/// The content fields that define a sequence's behavior — everything except
/// server-assigned identity (`id`, `version`, `created_at`, `deprecated`,
/// `status`). Two sequences with the same fingerprint are functionally equal.
fn content_fingerprint(v: &serde_json::Value) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for key in [
        "blocks",
        "interceptors",
        "input_schema",
        "sla",
        "on_failure",
        "on_cancel",
    ] {
        if let Some(val) = v.get(key)
            && !val.is_null()
        {
            obj.insert(key.to_string(), val.clone());
        }
    }
    serde_json::Value::Object(obj)
}

/// The apply decision for one sequence given the server's current version.
#[derive(Debug, PartialEq, Eq)]
enum ApplyDecision {
    /// Content matches the server; nothing to do (carries the current version).
    Unchanged(i64),
    /// Content differs (or the sequence is new); apply at this version.
    Apply(i32),
}

/// Decide what to do with `local` given the `server`'s current version (if any).
/// New sequence → apply v1; identical content → unchanged; differing content →
/// apply at `server.version + 1`.
fn decide(server: Option<&serde_json::Value>, local: &serde_json::Value) -> Result<ApplyDecision> {
    let Some(server) = server else {
        return Ok(ApplyDecision::Apply(1));
    };
    let current = server["version"]
        .as_i64()
        .and_then(|version| i32::try_from(version).ok())
        .context("server sequence has a missing or invalid i32 version")?;
    if content_fingerprint(server) == content_fingerprint(local) {
        return Ok(ApplyDecision::Unchanged(i64::from(current)));
    }
    let next = current
        .checked_add(1)
        .context("sequence version limit reached; cannot apply a newer version")?;
    Ok(ApplyDecision::Apply(next))
}

/// Apply a single sequence JSON file. Returns a human-readable status line.
async fn apply_one(
    client: &Client,
    base: &str,
    file: &std::path::Path,
    dry_run: bool,
) -> Result<String> {
    let mut local = crate::seqdoc::read_document(file)?;

    let tenant_id = local["tenant_id"].as_str().map(str::to_string);
    let namespace = local["namespace"].as_str().map(str::to_string);
    let name = local["name"].as_str().map(str::to_string);
    let (Some(tenant_id), Some(namespace), Some(name)) = (tenant_id, namespace, name) else {
        anyhow::bail!(
            "{}: sequence JSON must include tenant_id, namespace, and name to apply",
            file.display()
        );
    };

    // Fetch the current server version, if any.
    let resp = client
        .get(format!("{base}/sequences/by-name"))
        .query(&[
            ("tenant_id", &tenant_id),
            ("namespace", &namespace),
            ("name", &name),
        ])
        .send()
        .await?;

    let server = if resp.status().as_u16() == 404 {
        None
    } else if resp.status().is_success() {
        Some(
            resp.json::<serde_json::Value>()
                .await
                .context("invalid sequence lookup response")?,
        )
    } else {
        let status = resp.status();
        anyhow::bail!("{}: server returned {status}", file.display());
    };

    let next_version = match decide(server.as_ref(), &local)? {
        ApplyDecision::Unchanged(v) => return Ok(format!("unchanged  {name} v{v} (no diff)")),
        ApplyDecision::Apply(v) => v,
    };

    if dry_run {
        return Ok(format!("would apply {name} v{next_version} (dry-run)"));
    }

    // Stamp server-assigned identity and POST.
    local["id"] = serde_json::json!(Uuid::now_v7());
    local["version"] = serde_json::json!(next_version);
    local["created_at"] = serde_json::json!(chrono::Utc::now().to_rfc3339());

    let resp = client
        .post(format!("{base}/sequences"))
        .json(&local)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
        anyhow::bail!("{}: apply failed ({status}): {body}", file.display());
    }
    Ok(format!("applied    {name} v{next_version}"))
}

/// Collect sequence documents from a path (a single file, or every
/// `.json` / `.yaml` / `.yml` in a dir).
fn collect_json_files(path: &std::path::Path) -> Result<Vec<PathBuf>> {
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?
            .into_iter()
            .filter(|p| crate::seqdoc::is_document_path(p))
            .collect();
        files.sort();
        if files.is_empty() {
            anyhow::bail!("no .json/.yaml/.yml files found in {}", path.display());
        }
        Ok(files)
    } else {
        Ok(vec![path.to_path_buf()])
    }
}

#[allow(clippy::too_many_lines)]
pub async fn run(
    client: &Client,
    base: &str,
    cmd: SequenceCmd,
    format: OutputFormat,
) -> Result<()> {
    match cmd {
        SequenceCmd::Create { file } => {
            let body = crate::seqdoc::read_document(&file)?;
            let resp = client
                .post(format!("{base}/sequences"))
                .json(&body)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Get { id } => {
            let resp = client.get(format!("{base}/sequences/{id}")).send().await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Lookup {
            tenant_id,
            namespace,
            name,
            version,
        } => {
            let mut params = vec![
                ("tenant_id", tenant_id),
                ("namespace", namespace),
                ("name", name),
            ];
            if let Some(v) = &version {
                params.push(("version", v.to_string()));
            }
            let resp = client
                .get(format!("{base}/sequences/by-name"))
                .query(&params)
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Versions {
            tenant_id,
            namespace,
            name,
        } => {
            let resp = client
                .get(format!("{base}/sequences/versions"))
                .query(&[
                    ("tenant_id", &tenant_id),
                    ("namespace", &namespace),
                    ("name", &name),
                ])
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::Deprecate { id } => {
            let resp = client
                .post(format!("{base}/sequences/{id}/deprecate"))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::MigrateInstance {
            instance_id,
            target_sequence_id,
        } => {
            let resp = client
                .post(format!("{base}/sequences/migrate-instance"))
                .json(&serde_json::json!({
                    "instance_id": instance_id,
                    "target_sequence_id": target_sequence_id,
                }))
                .send()
                .await?;
            print_response(resp, format).await?;
        }
        SequenceCmd::UpgradeFormat { file, out } => {
            let mut value = crate::seqdoc::read_document(&file)?;
            let object = value
                .as_object_mut()
                .context("sequence document must be a JSON object")?;
            object.insert(
                "$schema".into(),
                serde_json::json!("https://orch8.io/contracts/sequence.schema.json"),
            );
            object.insert(
                "schema_version".into(),
                serde_json::json!(orch8_types::sequence::SEQUENCE_SCHEMA_VERSION),
            );
            upgrade_block_types(&mut value);
            let sequence = orch8_types::sequence::deserialize_sequence_strict(&value)
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            sequence
                .validate()
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
            if let Some(out) = out {
                let rendered = crate::seqdoc::render_for_path(&out, &value)?;
                atomic_write(&out, rendered.as_bytes())?;
                println!("upgraded {} → {}", file.display(), out.display());
            } else {
                print!(
                    "{}",
                    crate::seqdoc::render(&value, crate::seqdoc::DocumentFormat::Json)?
                );
            }
        }
        SequenceCmd::Apply { path, dry_run } => {
            let files = collect_json_files(&path)?;
            let mut changed = 0usize;
            for file in &files {
                let line = apply_one(client, base, file, dry_run).await?;
                if line.starts_with("applied") || line.starts_with("would apply") {
                    changed += 1;
                }
                println!("{line}");
            }
            let _ = format;
            println!(
                "{} file(s), {changed} {}.",
                files.len(),
                if dry_run { "to change" } else { "applied" }
            );
        }
        SequenceCmd::Preflight { id, file } => {
            let resp = match (id, file) {
                (Some(id), None) => {
                    client
                        .get(format!("{base}/sequences/{id}/preflight"))
                        .send()
                        .await?
                }
                (None, Some(file)) => {
                    let body = crate::seqdoc::read_document(&file)?;
                    client
                        .post(format!("{base}/sequences/preflight"))
                        .json(&body)
                        .send()
                        .await?
                }
                _ => anyhow::bail!("pass exactly one of --id or --file"),
            };
            if !resp.status().is_success() {
                anyhow::bail!("preflight request failed: {}", resp.status());
            }
            let report: serde_json::Value = resp.json().await?;
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&report)?),
                OutputFormat::Table => print_preflight_report(&report),
            }
            let overall = report["overall"].as_str().unwrap_or("unknown");
            if !matches!(overall, "pass" | "warning") {
                std::process::exit(1);
            }
        }
        SequenceCmd::Dataflow { id, file, out_dir } => {
            let resp = match (id, file) {
                (Some(id), None) => {
                    client
                        .get(format!("{base}/sequences/{id}/dataflow"))
                        .send()
                        .await?
                }
                (None, Some(file)) => {
                    let body = crate::seqdoc::read_document(&file)?;
                    client
                        .post(format!("{base}/sequences/dataflow"))
                        .json(&body)
                        .send()
                        .await?
                }
                _ => anyhow::bail!("pass exactly one of --id or --file"),
            };
            if !resp.status().is_success() {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                anyhow::bail!("dataflow request failed ({status}): {body}");
            }
            let result: serde_json::Value = resp.json().await?;
            let report: orch8_engine::dataflow::DataflowReport = serde_json::from_value(
                result
                    .get("report")
                    .cloned()
                    .context("dataflow response omitted report")?,
            )
            .context("invalid dataflow report returned by server")?;
            let generated: orch8_engine::dataflow::GeneratedDataflowTypes = serde_json::from_value(
                result
                    .get("generated")
                    .cloned()
                    .context("dataflow response omitted generated artifacts")?,
            )
            .context("invalid generated dataflow artifacts returned by server")?;
            if let Some(directory) = out_dir {
                std::fs::create_dir_all(&directory).with_context(|| {
                    format!("create dataflow output directory {}", directory.display())
                })?;
                atomic_write(&directory.join("types.ts"), generated.typescript.as_bytes())?;
                atomic_write(&directory.join("types.py"), generated.python.as_bytes())?;
                atomic_write(&directory.join("Types.swift"), generated.swift.as_bytes())?;
                atomic_write(&directory.join("Types.kt"), generated.kotlin.as_bytes())?;
                atomic_write(
                    &directory.join("schema.json"),
                    serde_json::to_string_pretty(&generated.schema)?.as_bytes(),
                )?;
                atomic_write(
                    &directory.join("report.json"),
                    serde_json::to_string_pretty(&report)?.as_bytes(),
                )?;
                println!(
                    "generated typed dataflow artifacts in {}",
                    directory.display()
                );
            }
            match format {
                OutputFormat::Json => println!("{}", serde_json::to_string_pretty(&result)?),
                OutputFormat::Table => print_dataflow_report(&report, &generated),
            }
            if !report.is_compatible() {
                anyhow::bail!("typed dataflow is incompatible");
            }
        }
    }
    Ok(())
}

fn print_dataflow_report(
    report: &orch8_engine::dataflow::DataflowReport,
    generated: &orch8_engine::dataflow::GeneratedDataflowTypes,
) {
    println!(
        "typed dataflow — {} reference(s), generator {}",
        report.references_checked, generated.generator_version
    );
    for finding in &report.findings {
        println!(
            "  [{}] {} -> {}: {}",
            match finding.severity {
                orch8_engine::dataflow::DataflowSeverity::Warning => "warning",
                orch8_engine::dataflow::DataflowSeverity::Error => "error",
            },
            finding.reference,
            finding.consumer,
            finding.summary
        );
    }
}

/// Render a preflight report for humans: one line per check, findings
/// indented with their remediation commands.
fn print_preflight_report(report: &serde_json::Value) {
    println!(
        "preflight for {} v{} — overall: {}\n",
        report["sequence_name"].as_str().unwrap_or("?"),
        report["sequence_version"],
        report["overall"].as_str().unwrap_or("?")
    );
    for check in report["checks"].as_array().into_iter().flatten() {
        println!(
            "  [{}] {}: {}",
            check["status"].as_str().unwrap_or("?").to_uppercase(),
            check["id"].as_str().unwrap_or("?"),
            check["summary"].as_str().unwrap_or("")
        );
        for finding in check["findings"].as_array().into_iter().flatten() {
            println!(
                "      - {} {}",
                finding["code"].as_str().unwrap_or(""),
                finding["summary"].as_str().unwrap_or("")
            );
            for rem in finding["remediation"].as_array().into_iter().flatten() {
                if let Some(cmd) = rem["command"].as_str() {
                    println!("        fix: {cmd}");
                } else if let Some(s) = rem["summary"].as_str() {
                    println!("        fix: {s}");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seq(version: i64, handler: &str) -> serde_json::Value {
        json!({
            "id": Uuid::now_v7(),
            "tenant_id": "t", "namespace": "ns", "name": "flow", "version": version,
            "blocks": [{ "type": "step", "id": "s1", "handler": handler, "params": {} }],
            "created_at": "2026-01-01T00:00:00Z"
        })
    }

    #[test]
    fn new_sequence_applies_v1() {
        assert_eq!(decide(None, &seq(0, "a")).unwrap(), ApplyDecision::Apply(1));
    }

    #[test]
    fn identical_content_is_unchanged_ignoring_identity() {
        // Same blocks, different id/version/created_at → unchanged.
        let server = seq(3, "a");
        let local = seq(99, "a"); // different id + version, same content
        assert_eq!(
            decide(Some(&server), &local).unwrap(),
            ApplyDecision::Unchanged(3)
        );
    }

    #[test]
    fn changed_content_bumps_version() {
        let server = seq(3, "a");
        let local = seq(3, "b"); // different handler
        assert_eq!(
            decide(Some(&server), &local).unwrap(),
            ApplyDecision::Apply(4)
        );
    }

    #[test]
    fn fingerprint_ignores_identity_fields() {
        let a = json!({ "id": "x", "version": 1, "created_at": "t", "blocks": [1] });
        let b = json!({ "id": "y", "version": 9, "created_at": "u", "blocks": [1] });
        assert_eq!(content_fingerprint(&a), content_fingerprint(&b));
        let c = json!({ "blocks": [2] });
        assert_ne!(content_fingerprint(&a), content_fingerprint(&c));
    }

    #[test]
    fn fingerprint_skips_null_valued_keys() {
        // A key present but explicitly null must be omitted, so it fingerprints
        // identically to the same content with the key absent — otherwise a
        // serializer that emits `"sla": null` would force a spurious version bump.
        let with_null = json!({ "blocks": [1], "sla": null, "on_failure": null });
        let absent = json!({ "blocks": [1] });
        assert_eq!(
            content_fingerprint(&with_null),
            content_fingerprint(&absent)
        );
    }

    #[test]
    fn fingerprint_tracks_every_content_key() {
        // Changing any whitelisted content key (beyond `blocks`) must change the
        // fingerprint — guards against a key being dropped from the whitelist.
        let base = json!({ "blocks": [1] });
        for key in [
            "interceptors",
            "input_schema",
            "sla",
            "on_failure",
            "on_cancel",
        ] {
            let mut changed = base.clone();
            changed[key] = json!({ "marker": key });
            assert_ne!(
                content_fingerprint(&base),
                content_fingerprint(&changed),
                "fingerprint ignored content key `{key}`"
            );
        }
    }

    #[test]
    fn invalid_server_versions_are_rejected_even_without_content_changes() {
        for version in [
            json!(null),
            json!("3"),
            json!(1.5),
            json!(i64::from(i32::MAX) + 1),
        ] {
            let mut server = seq(3, "a");
            server["version"] = version;
            for handler in ["a", "b"] {
                assert!(decide(Some(&server), &seq(3, handler)).is_err());
            }
        }
        let mut server = seq(3, "a");
        server.as_object_mut().unwrap().remove("version");
        assert!(decide(Some(&server), &seq(3, "a")).is_err());
    }

    #[test]
    fn maximum_version_is_unchanged_or_rejected_but_never_reused() {
        let server = seq(i64::from(i32::MAX), "a");
        assert_eq!(
            decide(Some(&server), &seq(1, "a")).unwrap(),
            ApplyDecision::Unchanged(i64::from(i32::MAX))
        );
        assert!(decide(Some(&server), &seq(1, "b")).is_err());
        let penultimate = seq(i64::from(i32::MAX - 1), "a");
        assert_eq!(
            decide(Some(&penultimate), &seq(1, "b")).unwrap(),
            ApplyDecision::Apply(i32::MAX)
        );
    }

    #[tokio::test]
    async fn apply_rejects_invalid_lookup_before_posting() {
        use crate::commands::test_support::mock_api_with_responses;
        for response in [
            "not JSON".to_string(),
            "{}".to_string(),
            seq(i64::from(i32::MAX), "a").to_string(),
        ] {
            let api = mock_api_with_responses(vec![(reqwest::StatusCode::OK, response)]).await;
            let dir = tempfile::tempdir().unwrap();
            let file = dir.path().join("sequence.json");
            std::fs::write(&file, seq(1, "b").to_string()).unwrap();
            assert!(
                apply_one(&Client::new(), &api.base, &file, false)
                    .await
                    .is_err()
            );
            let requests = api.log.snapshot();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].method, reqwest::Method::GET);
        }
    }
}
