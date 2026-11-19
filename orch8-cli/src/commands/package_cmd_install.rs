//! Verify the complete install plan before publishing any sequence. Each plan
//! has a durable local journal so a retry can prove ownership of earlier
//! creates even when their HTTP responses never arrived.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use chrono::Timelike as _;
use orch8::SequenceDefinition;
use orch8::contract::{RunOptions, resolve_pinned_children, run_suite};
use orch8_publisher::package::{
    PackageArchive, SignedPackage, TrustLevel, TrustPolicy, check_trust, check_upgrade,
    contract_files, install_namespace, sequence_files, verify_package,
};
use orch8_types::contract::ContractSuite;
use orch8_types::ids::{SequenceId, TenantId};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::atomic_write;

const LOCKFILE: &str = "orch8-packages.lock";
const JOURNAL_DIR: &str = ".orch8-package-installs";

#[derive(Deserialize, Serialize)]
struct InstallJournal {
    created_at: chrono::DateTime<chrono::Utc>,
    ids: BTreeMap<String, SequenceId>,
}

#[derive(Deserialize)]
struct CreateReceipt {
    id: SequenceId,
}

pub(super) async fn install(
    client: &Client,
    base: &str,
    path: &Path,
    tenant_id: &str,
    trusted_keys: &[String],
    allow_untrusted: bool,
    skip_contracts: bool,
) -> Result<()> {
    let pkg = verify_trust(path, trusted_keys, allow_untrusted)?;
    let manifest = &pkg.archive.manifest;
    let namespace = install_namespace(&manifest.name);
    let tenant = TenantId::new(tenant_id).map_err(anyhow::Error::msg)?;
    let mut definitions = load_definitions(&pkg.archive, &tenant, &namespace)?;
    check_contracts(&pkg.archive, &definitions, skip_contracts).await?;

    let journal_dir = Path::new(JOURNAL_DIR);
    // The lock serializes installs in this project, including updates to the
    // shared provenance file and use of a pending journal by concurrent CLIs.
    let _lock = lock_project(journal_dir)?;
    let journal_key = journal_key(base, &tenant, &pkg)?;
    let journal_path = journal_dir.join(format!("{journal_key}.json"));
    let existing_journal = read_json::<InstallJournal>(&journal_path)?;
    let lock_path = Path::new(LOCKFILE);
    let mut provenance = read_json::<BTreeMap<String, Value>>(lock_path)?.unwrap_or_default();
    if let Some(entry) = provenance.get(&manifest.name)
        && let Some(installed) = entry["version"].as_str()
    {
        let completed_retry = existing_journal.is_some()
            && entry["content_hash"] == pkg.content_hash
            && entry["public_key"] == pkg.public_key
            && entry["install_journal"] == journal_key;
        if !completed_retry {
            check_upgrade(installed, &manifest.version)?;
        }
    }

    let needs_journal = existing_journal.is_none();
    let journal = existing_journal.unwrap_or_else(|| InstallJournal {
        // PostgreSQL stores timestamps to microsecond precision. Use a
        // whole-second timestamp so both backends round-trip it exactly.
        created_at: chrono::Utc::now()
            .with_nanosecond(0)
            .expect("zero nanoseconds within a second"),
        ids: definitions
            .iter()
            .map(|(path, seq)| (path.clone(), seq.id))
            .collect(),
    });
    ensure!(
        journal.ids.keys().eq(definitions.keys())
            && journal.ids.values().collect::<BTreeSet<_>>().len() == definitions.len(),
        "install journal does not match the package's definitions ({})",
        journal_path.display()
    );

    // Check every remote name/version before creating any sequence. Only
    // a definition with the journal's identity and exact content is ours.
    let mut pending = Vec::new();
    for (path, seq) in &mut definitions {
        seq.id = journal.ids[path];
        seq.created_at = journal.created_at;
        if !already_created(client, base, seq).await? {
            pending.push(path.clone());
        }
    }
    // Confirm the journal is durable before sending any remaining creates.
    // A previous invocation may have renamed it before its fsync failed.
    if needs_journal || !pending.is_empty() {
        atomic_write(&journal_path, &serde_json::to_vec(&journal)?)?;
    }
    for path in pending {
        create_sequence(client, base, &definitions[&path])
            .await
            .with_context(|| {
                format!(
                    "failed to install {path}; rerun from this directory to resume this package"
                )
            })?;
    }

    println!(
        "installed {} v{} → namespace {namespace} ({} sequence(s))",
        manifest.name,
        manifest.version,
        definitions.len()
    );
    for seq in definitions.values() {
        report_preflight(client, base, seq).await?;
    }

    provenance.insert(
        manifest.name.clone(),
        json!({
            "version": manifest.version,
            "content_hash": pkg.content_hash,
            "public_key": pkg.public_key,
            "namespace": namespace,
            "install_journal": journal_key,
            "installed_at": chrono::Utc::now().to_rfc3339(),
        }),
    );
    atomic_write(lock_path, &serde_json::to_vec_pretty(&provenance)?)?;
    println!("provenance recorded in {LOCKFILE}");
    Ok(())
}

fn verify_trust(
    path: &Path,
    trusted_keys: &[String],
    allow_untrusted: bool,
) -> Result<SignedPackage> {
    let pkg = super::read_package(path)?;
    verify_package(&pkg)?;
    let trust = check_trust(
        &pkg,
        &TrustPolicy {
            trusted_keys: trusted_keys.to_vec(),
            allow_untrusted,
        },
    )?;
    if trust == TrustLevel::UntrustedAllowed {
        eprintln!("WARNING: installing from an UNTRUSTED publisher (explicitly allowed)");
    }
    Ok(pkg)
}

fn lock_project(dir: &Path) -> Result<File> {
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    // Make the parent directory entry durable before a journal can authorize
    // any server write. atomic_write separately syncs the journal directory.
    #[cfg(unix)]
    File::open(".")?.sync_all()?;

    let path = dir.join("lock");
    let lock = File::options()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    lock.lock()
        .with_context(|| format!("locking {}", path.display()))?;
    Ok(lock)
}

fn journal_key(base: &str, tenant: &TenantId, pkg: &SignedPackage) -> Result<String> {
    // Length-delimited via JSON so separate tuples never alias.
    let identity = serde_json::to_vec(&(base, tenant, &pkg.content_hash, &pkg.public_key))?;
    Ok(URL_SAFE_NO_PAD.encode(Sha256::digest(identity)))
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match fs::read(path) {
        Ok(raw) => serde_json::from_slice(&raw)
            .with_context(|| format!("{} is corrupt", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
    }
}

fn load_definitions(
    archive: &PackageArchive,
    tenant: &TenantId,
    namespace: &str,
) -> Result<BTreeMap<String, SequenceDefinition>> {
    let mut definitions = BTreeMap::new();
    let mut revisions = BTreeSet::new();
    for (path, raw) in sequence_files(archive) {
        let mut raw: Value =
            serde_json::from_str(raw).with_context(|| format!("reading {path}"))?;
        let doc = raw
            .as_object_mut()
            .with_context(|| format!("{path} must contain a sequence object"))?;
        doc.insert("id".into(), json!(SequenceId::new()));
        doc.insert("tenant_id".into(), json!(tenant));
        doc.insert("namespace".into(), json!(namespace));
        doc.insert("created_at".into(), json!(archive.manifest.created_at));

        let mut seq: SequenceDefinition =
            serde_json::from_value(raw).with_context(|| format!("decoding {path}"))?;
        seq.sub_tenant = orch8_api::sub_tenants::SubTenantHeader::default()
            .for_create(seq.sub_tenant.as_deref())
            .with_context(|| format!("validating {path}"))?;
        orch8_api::sequences::validate_sequence_definition(&seq)
            .with_context(|| format!("validating {path}"))?;
        ensure!(
            revisions.insert((seq.name.clone(), seq.version)),
            "{path}: duplicate package sequence '{}' v{}",
            seq.name,
            seq.version
        );
        definitions.insert(path.clone(), seq);
    }
    Ok(definitions)
}

async fn check_contracts(
    archive: &PackageArchive,
    definitions: &BTreeMap<String, SequenceDefinition>,
    skip_contracts: bool,
) -> Result<()> {
    for (contract_path, raw) in contract_files(archive) {
        let stem = contract_path
            .strip_prefix("contracts/")
            .and_then(|relative| relative.strip_suffix(".contracts.json"))
            .context("invalid packaged contract path")?;
        let seq_path = format!("sequences/{stem}.json");
        let seq = definitions
            .get(&seq_path)
            .with_context(|| format!("{contract_path} has no matching sequence {seq_path}"))?;
        let suite: ContractSuite =
            serde_json::from_str(raw).with_context(|| format!("reading {contract_path}"))?;
        suite
            .validate()
            .map_err(anyhow::Error::msg)
            .with_context(|| format!("validating {contract_path}"))?;
        let children =
            resolve_pinned_children(seq, definitions.values().filter(|child| child.id != seq.id))
                .with_context(|| format!("resolving children for {contract_path}"))?;
        if skip_contracts {
            continue;
        }
        let options = RunOptions {
            child_sequences: children.into_iter().cloned().collect(),
            ..RunOptions::default()
        };
        let report = run_suite(seq, &suite, &options)
            .await
            .with_context(|| format!("running {contract_path}"))?;
        if !report.passed {
            for case in report.failed_cases() {
                eprintln!("contract FAILED [{stem} / {}]:", case.name);
                for failure in &case.failures {
                    eprintln!("    {failure}");
                }
            }
            bail!("packaged contracts failed — refusing to install");
        }
        println!(
            "contracts: {} — {} case(s) passed",
            stem,
            report.cases.len()
        );
    }
    Ok(())
}

async fn already_created(client: &Client, base: &str, seq: &SequenceDefinition) -> Result<bool> {
    let existing = client
        .get(format!("{base}/sequences/by-name"))
        .query(&[
            ("tenant_id", seq.tenant_id.as_str()),
            ("namespace", seq.namespace.as_str()),
            ("name", seq.name.as_str()),
            ("version", &seq.version.to_string()),
        ])
        .send()
        .await?;
    let status = existing.status();
    if status == StatusCode::NOT_FOUND {
        return Ok(false);
    }
    let label = format!("{}/{} v{}", seq.namespace, seq.name, seq.version);
    ensure!(
        status.is_success(),
        "conflict check for {label} failed: server returned {status} — refusing to install"
    );
    let mut actual: SequenceDefinition = existing
        .json()
        .await
        .with_context(|| format!("decoding existing sequence {label}"))?;
    // Both storage backends drop the editor's schema URL and schema
    // version metadata. All persisted definition fields must still match.
    actual.schema.clone_from(&seq.schema);
    actual.schema_version = seq.schema_version;
    ensure!(
        actual.id == seq.id && serde_json::to_value(&actual)? == serde_json::to_value(seq)?,
        "sequence {label} already exists with a different install identity or definition — refusing to overwrite"
    );
    Ok(true)
}

async fn create_sequence(client: &Client, base: &str, seq: &SequenceDefinition) -> Result<()> {
    let resp = client
        .post(format!("{base}/sequences"))
        .json(seq)
        .send()
        .await?;
    let status = resp.status();
    if !status.is_success() {
        bail!("{status} {}", resp.text().await.unwrap_or_default());
    }
    let created: CreateReceipt = resp.json().await.context("reading create response")?;
    ensure!(
        created.id == seq.id,
        "server returned a different sequence ID"
    );
    Ok(())
}

async fn report_preflight(client: &Client, base: &str, seq: &SequenceDefinition) -> Result<()> {
    let resp = client
        .get(format!("{base}/sequences/{}/preflight", seq.id))
        .send()
        .await?;
    if let Ok(report) = resp.json::<Value>().await {
        println!(
            "  preflight {} v{}: {}",
            seq.name,
            seq.version,
            report["overall"].as_str().unwrap_or("?")
        );
        for check in report["checks"].as_array().into_iter().flatten() {
            let status = check["status"].as_str().unwrap_or("?");
            if status != "pass" {
                println!(
                    "    [{}] {}: {}",
                    status.to_uppercase(),
                    check["id"].as_str().unwrap_or("?"),
                    check["summary"].as_str().unwrap_or("")
                );
            }
        }
    }
    Ok(())
}
