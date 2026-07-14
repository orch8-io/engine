use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Subcommand;
use reqwest::Client;
use uuid::Uuid;

use crate::{OutputFormat, print_response};

#[derive(Subcommand)]
pub enum SequenceCmd {
    /// Create a sequence from a JSON file.
    Create {
        /// Path to the JSON definition file.
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
    /// Git-ops apply: diff a local sequence definition against the server and,
    /// on change, upload it with the version bumped. Accepts a file or a
    /// directory of `.json` files. Idempotent — an unchanged sequence is left
    /// alone.
    Apply {
        /// Path to a sequence JSON file or a directory of them.
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
        /// Local draft definition to check instead of a stored sequence.
        #[arg(long, short)]
        file: Option<PathBuf>,
    },
    /// Compile typed producer/consumer references and generate deterministic
    /// TypeScript, Python, and canonical schema artifacts.
    Dataflow {
        /// Stored sequence id to compile.
        #[arg(long, conflicts_with = "file")]
        id: Option<Uuid>,
        /// Local draft definition to compile instead of a stored sequence.
        #[arg(long, short)]
        file: Option<PathBuf>,
        /// Atomically write types.ts, types.py, schema.json, and report.json.
        #[arg(long)]
        out_dir: Option<PathBuf>,
    },
}

fn atomic_write(path: &std::path::Path, contents: &[u8]) -> Result<()> {
    use std::io::Write as _;

    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(std::path::Path::new("."));
    let mut file = tempfile::NamedTempFile::new_in(parent)
        .with_context(|| format!("create temporary file beside {}", path.display()))?;
    file.write_all(contents)?;
    file.as_file().sync_all()?;
    file.persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("atomically replace {}", path.display()))?;
    Ok(())
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
fn decide(server: Option<&serde_json::Value>, local: &serde_json::Value) -> ApplyDecision {
    match server {
        None => ApplyDecision::Apply(1),
        Some(s) => {
            if content_fingerprint(s) == content_fingerprint(local) {
                ApplyDecision::Unchanged(s["version"].as_i64().unwrap_or(0))
            } else {
                let cur = s["version"]
                    .as_i64()
                    .and_then(|v| i32::try_from(v).ok())
                    .unwrap_or(0);
                ApplyDecision::Apply(cur + 1)
            }
        }
    }
}

/// Apply a single sequence JSON file. Returns a human-readable status line.
async fn apply_one(
    client: &Client,
    base: &str,
    file: &std::path::Path,
    dry_run: bool,
) -> Result<String> {
    let content = std::fs::read_to_string(file)
        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", file.display()))?;
    let mut local: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", file.display()))?;

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
                .unwrap_or(serde_json::Value::Null),
        )
    } else {
        let status = resp.status();
        anyhow::bail!("{}: server returned {status}", file.display());
    };

    let next_version = match decide(server.as_ref(), &local) {
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

/// Collect `.json` files from a path (a single file, or every `.json` in a dir).
fn collect_json_files(path: &std::path::Path) -> Result<Vec<PathBuf>> {
    if path.is_dir() {
        let mut files: Vec<PathBuf> = std::fs::read_dir(path)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        files.sort();
        if files.is_empty() {
            anyhow::bail!("no .json files found in {}", path.display());
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
            let content = std::fs::read_to_string(&file)
                .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", file.display()))?;
            let body: serde_json::Value = serde_json::from_str(&content)
                .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", file.display()))?;
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
                    let content = std::fs::read_to_string(&file)
                        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", file.display()))?;
                    let body: serde_json::Value = serde_json::from_str(&content)
                        .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", file.display()))?;
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
                    let content = std::fs::read_to_string(&file)
                        .map_err(|e| anyhow::anyhow!("failed to read {}: {e}", file.display()))?;
                    let body: serde_json::Value = serde_json::from_str(&content)
                        .map_err(|e| anyhow::anyhow!("invalid JSON in {}: {e}", file.display()))?;
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
        assert_eq!(decide(None, &seq(0, "a")), ApplyDecision::Apply(1));
    }

    #[test]
    fn identical_content_is_unchanged_ignoring_identity() {
        // Same blocks, different id/version/created_at → unchanged.
        let server = seq(3, "a");
        let local = seq(99, "a"); // different id + version, same content
        assert_eq!(decide(Some(&server), &local), ApplyDecision::Unchanged(3));
    }

    #[test]
    fn changed_content_bumps_version() {
        let server = seq(3, "a");
        let local = seq(3, "b"); // different handler
        assert_eq!(decide(Some(&server), &local), ApplyDecision::Apply(4));
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
    fn decide_unchanged_with_missing_server_version_defaults_to_zero() {
        // Server record matches content but carries no `version` field.
        let server =
            json!({ "blocks": [{ "type": "step", "id": "s1", "handler": "a", "params": {} }] });
        let local = seq(7, "a");
        assert_eq!(decide(Some(&server), &local), ApplyDecision::Unchanged(0));
    }

    #[test]
    fn decide_changed_with_missing_server_version_applies_v1() {
        // Changed content + absent server version → bump from the 0 fallback to 1.
        let server =
            json!({ "blocks": [{ "type": "step", "id": "s1", "handler": "a", "params": {} }] });
        let local = seq(7, "b");
        assert_eq!(decide(Some(&server), &local), ApplyDecision::Apply(1));
    }

    #[test]
    fn decide_changed_with_overflowing_server_version_does_not_panic() {
        // A version beyond i32::MAX must clamp to the 0 fallback (→ Apply(1)),
        // never panic or wrap.
        let mut server = seq(0, "a");
        server["version"] = json!(i64::from(i32::MAX) + 1000);
        let local = seq(0, "b");
        assert_eq!(decide(Some(&server), &local), ApplyDecision::Apply(1));
    }
}
