//! `orch8 package` — build, verify, inspect, and install signed
//! workflow packages.
//!
//! Install safety, in order: signature + integrity verification, trust
//! policy (explicit keys or explicit untrusted opt-in), packaged
//! contracts executed locally (offline, mocked, virtual time), lockfile
//! downgrade check, conflict check against the server (a package never
//! overwrites existing sequences — it installs under its own
//! `pkg.<publisher>.<name>` namespace), then upload + per-sequence
//! preflight report. Packages never run install-time code.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use clap::Subcommand;
use reqwest::Client;
use serde_json::{Value, json};

use orch8_publisher::package::{
    PackageManifest, PackageRequirements, SignedPackage, TrustLevel, TrustPolicy, build_package,
    check_trust, check_upgrade, contract_files, install_namespace, sequence_files, verify_package,
};

use crate::OutputFormat;

const LOCKFILE: &str = "orch8-packages.lock";

#[derive(Subcommand)]
pub enum PackageCmd {
    /// Generate a publisher signing keypair (prints base64 seed + public key).
    Keygen,
    /// Build and sign a package from a directory containing
    /// `package.json`, `sequences/`, and optionally `contracts/` + docs.
    Build {
        /// Package source directory.
        dir: PathBuf,
        /// Base64-encoded 32-byte Ed25519 seed (or @file containing it).
        #[arg(long)]
        key: String,
        /// Output path (defaults to `<publisher>-<name>-<version>.orch8pkg`).
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Verify a package's integrity, signature, and (optionally) trust.
    Verify {
        file: PathBuf,
        /// Trusted publisher public keys (base64). Repeatable.
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
    },
    /// Show a package's manifest, contents, and requirements.
    Inspect { file: PathBuf },
    /// Verify, test, and install a package's sequences under its own
    /// namespace. Never overwrites existing sequences.
    Install {
        file: PathBuf,
        #[arg(long)]
        tenant_id: String,
        /// Trusted publisher public keys (base64). Repeatable.
        #[arg(long = "trusted-key")]
        trusted_keys: Vec<String>,
        /// Explicitly allow installing from an untrusted publisher.
        #[arg(long)]
        allow_untrusted: bool,
        /// Skip running the packaged contracts before install.
        #[arg(long)]
        skip_contracts: bool,
    },
}

pub async fn run(client: &Client, base: &str, cmd: PackageCmd, format: OutputFormat) -> Result<()> {
    match cmd {
        PackageCmd::Keygen => {
            keygen();
            Ok(())
        }
        PackageCmd::Build { dir, key, out } => build(&dir, &key, out.as_deref()),
        PackageCmd::Verify { file, trusted_keys } => verify(&file, &trusted_keys),
        PackageCmd::Inspect { file } => inspect(&file, format),
        PackageCmd::Install {
            file,
            tenant_id,
            trusted_keys,
            allow_untrusted,
            skip_contracts,
        } => {
            install(
                client,
                base,
                &file,
                &tenant_id,
                &trusted_keys,
                allow_untrusted,
                skip_contracts,
            )
            .await
        }
    }
}

fn keygen() {
    let key = ed25519_dalek::SigningKey::generate(&mut rand_core::OsRng);
    println!(
        "seed (SECRET — store safely): {}",
        BASE64.encode(key.to_bytes())
    );
    println!(
        "public key (share/trust this): {}",
        BASE64.encode(key.verifying_key().to_bytes())
    );
}

fn load_signing_key(key_arg: &str) -> Result<ed25519_dalek::SigningKey> {
    let raw = if let Some(path) = key_arg.strip_prefix('@') {
        std::fs::read_to_string(path)
            .with_context(|| format!("reading key file {path}"))?
            .trim()
            .to_string()
    } else {
        key_arg.to_string()
    };
    let bytes: [u8; 32] = BASE64
        .decode(&raw)
        .context("key must be base64")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("key seed must be exactly 32 bytes"))?;
    Ok(ed25519_dalek::SigningKey::from_bytes(&bytes))
}

fn read_package(path: &Path) -> Result<SignedPackage> {
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&raw).context("file is not a signed orch8 package")
}

fn build(dir: &Path, key_arg: &str, out: Option<&Path>) -> Result<()> {
    let signing_key = load_signing_key(key_arg)?;

    let manifest_raw = std::fs::read_to_string(dir.join("package.json"))
        .with_context(|| format!("reading {}/package.json", dir.display()))?;
    let manifest_json: Value =
        serde_json::from_str(&manifest_raw).context("package.json is not valid JSON")?;
    let manifest = PackageManifest {
        name: manifest_json["name"]
            .as_str()
            .context("package.json: 'name' is required")?
            .to_string(),
        version: manifest_json["version"]
            .as_str()
            .context("package.json: 'version' is required")?
            .to_string(),
        description: manifest_json["description"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        publisher: manifest_json["publisher"]
            .as_str()
            .unwrap_or("")
            .to_string(),
        requirements: manifest_json
            .get("requirements")
            .map(|r| serde_json::from_value::<PackageRequirements>(r.clone()))
            .transpose()
            .context("package.json: invalid 'requirements'")?
            .unwrap_or_default(),
        created_at: chrono::Utc::now(),
    };

    // Collect files deterministically: sequences/, contracts/, README.md.
    let mut files = BTreeMap::new();
    for sub in ["sequences", "contracts"] {
        let sub_dir = dir.join(sub);
        if !sub_dir.is_dir() {
            continue;
        }
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&sub_dir)?
            .filter_map(std::result::Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "json"))
            .collect();
        entries.sort();
        for entry in entries {
            let name = entry
                .file_name()
                .and_then(|n| n.to_str())
                .context("non-utf8 file name")?;
            files.insert(
                format!("{sub}/{name}"),
                std::fs::read_to_string(&entry)
                    .with_context(|| format!("reading {}", entry.display()))?,
            );
        }
    }
    let readme = dir.join("README.md");
    if readme.is_file() {
        files.insert("README.md".to_string(), std::fs::read_to_string(&readme)?);
    }

    // Every packaged sequence must parse — a package with broken JSON
    // must never leave the publisher's machine.
    for (path, content) in &files {
        if path.starts_with("sequences/") {
            serde_json::from_str::<orch8::SequenceDefinition>(content)
                .with_context(|| format!("{path} is not a valid sequence definition"))?;
        }
        if path.starts_with("contracts/") {
            let suite: orch8_types::contract::ContractSuite = serde_json::from_str(content)
                .with_context(|| format!("{path} is not a valid contract suite"))?;
            suite
                .validate()
                .map_err(|e| anyhow::anyhow!("{path}: {e}"))?;
        }
    }

    let pkg = build_package(manifest, files, &signing_key)?;
    let default_name = format!(
        "{}-{}.orch8pkg",
        pkg.archive.manifest.name.replace('/', "-"),
        pkg.archive.manifest.version
    );
    let out_path = out.map_or_else(|| PathBuf::from(default_name), Path::to_path_buf);
    std::fs::write(&out_path, serde_json::to_string_pretty(&pkg)?)?;
    println!(
        "built {} v{} → {} (hash {})",
        pkg.archive.manifest.name,
        pkg.archive.manifest.version,
        out_path.display(),
        &pkg.content_hash[..16],
    );
    Ok(())
}

fn verify(path: &Path, trusted_keys: &[String]) -> Result<()> {
    let pkg = read_package(path)?;
    verify_package(&pkg)?;
    println!("integrity: OK (sha256 {})", pkg.content_hash);
    println!("signature: OK (publisher key {})", pkg.public_key);
    if trusted_keys.is_empty() {
        println!("trust:     not checked (pass --trusted-key to check)");
    } else {
        let policy = TrustPolicy {
            trusted_keys: trusted_keys.to_vec(),
            allow_untrusted: false,
        };
        match check_trust(&pkg, &policy) {
            Ok(TrustLevel::Trusted) => println!("trust:     TRUSTED publisher"),
            Ok(TrustLevel::UntrustedAllowed) => unreachable!("allow_untrusted is false"),
            Err(e) => {
                println!("trust:     NOT TRUSTED — {e}");
                std::process::exit(1);
            }
        }
    }
    Ok(())
}

fn inspect(path: &Path, format: OutputFormat) -> Result<()> {
    let pkg = read_package(path)?;
    let integrity = verify_package(&pkg).map_or("FAILED", |()| "verified");
    match format {
        OutputFormat::Json => {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "manifest": pkg.archive.manifest,
                    "files": pkg.archive.files.keys().collect::<Vec<_>>(),
                    "content_hash": pkg.content_hash,
                    "public_key": pkg.public_key,
                    "integrity": integrity,
                    "install_namespace": install_namespace(&pkg.archive.manifest.name),
                }))?
            );
        }
        OutputFormat::Table => {
            let m = &pkg.archive.manifest;
            println!("{} v{} — {}", m.name, m.version, m.description);
            println!("publisher: {} (key {})", m.publisher, pkg.public_key);
            println!("integrity: {integrity}   hash: {}", pkg.content_hash);
            println!("installs into namespace: {}", install_namespace(&m.name));
            println!("\nrequirements:");
            let r = &m.requirements;
            println!("  handlers:    {:?}", r.handlers);
            println!("  credentials: {:?}", r.credentials);
            println!("  plugins:     {:?}", r.plugins);
            println!("  queues:      {:?}", r.queues);
            println!("\nfiles:");
            for f in pkg.archive.files.keys() {
                println!("  {f}");
            }
        }
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn install(
    client: &Client,
    base: &str,
    path: &Path,
    tenant_id: &str,
    trusted_keys: &[String],
    allow_untrusted: bool,
    skip_contracts: bool,
) -> Result<()> {
    let pkg = read_package(path)?;

    // 1. Integrity + signature.
    verify_package(&pkg)?;
    // 2. Trust.
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

    let manifest = &pkg.archive.manifest;
    // 3. Lockfile downgrade check.
    let mut lock: BTreeMap<String, Value> = std::fs::read_to_string(LOCKFILE)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default();
    if let Some(entry) = lock.get(&manifest.name)
        && let Some(installed) = entry["version"].as_str()
    {
        check_upgrade(installed, &manifest.version)?;
    }

    // 4. Run packaged contracts locally (offline, mocked, virtual time).
    if !skip_contracts {
        for (contract_path, contract_raw) in contract_files(&pkg.archive) {
            let stem = contract_path
                .trim_start_matches("contracts/")
                .trim_end_matches(".contracts.json");
            let seq_path = format!("sequences/{stem}.json");
            let Some(seq_raw) = pkg.archive.files.get(&seq_path) else {
                eprintln!("skipping {contract_path}: no matching {seq_path}");
                continue;
            };
            let seq: orch8::SequenceDefinition = serde_json::from_str(seq_raw)?;
            let suite: orch8_types::contract::ContractSuite = serde_json::from_str(contract_raw)?;
            let report =
                orch8::contract::run_suite(&seq, &suite, &orch8::contract::RunOptions::default())
                    .await?;
            if report.passed {
                println!(
                    "contracts: {} — {} case(s) passed",
                    stem,
                    report.cases.len()
                );
            } else {
                for case in report.failed_cases() {
                    eprintln!("contract FAILED [{stem} / {}]:", case.name);
                    for failure in &case.failures {
                        eprintln!("    {failure}");
                    }
                }
                bail!("packaged contracts failed — refusing to install");
            }
        }
    }

    // 5. Conflict check + upload, all under the package's own namespace.
    let namespace = install_namespace(&manifest.name);
    let mut installed = Vec::new();
    for (seq_path, seq_raw) in sequence_files(&pkg.archive) {
        let mut seq: Value = serde_json::from_str(seq_raw)?;
        let name = seq["name"]
            .as_str()
            .context("sequence missing name")?
            .to_string();
        let version = seq["version"].as_i64().unwrap_or(1);

        // Never overwrite: abort when this (name, version) already exists.
        let existing = client
            .get(format!("{base}/sequences/by-name"))
            .query(&[
                ("tenant_id", tenant_id),
                ("namespace", namespace.as_str()),
                ("name", name.as_str()),
                ("version", &version.to_string()),
            ])
            .send()
            .await?;
        if existing.status().is_success() {
            bail!(
                "sequence {namespace}/{name} v{version} already exists — refusing to \
                 overwrite (uninstall or bump the package version)"
            );
        }

        seq["id"] = json!(uuid::Uuid::now_v7());
        seq["tenant_id"] = json!(tenant_id);
        seq["namespace"] = json!(namespace);
        seq["created_at"] = json!(chrono::Utc::now().to_rfc3339());
        let resp = client
            .post(format!("{base}/sequences"))
            .json(&seq)
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!(
                "failed to install {seq_path}: {} {}",
                resp.status(),
                resp.text().await.unwrap_or_default()
            );
        }
        let created: Value = resp.json().await?;
        installed.push((
            name,
            version,
            created["id"].as_str().unwrap_or("").to_string(),
        ));
    }

    // 6. Preflight every installed sequence and show what still needs
    //    configuring (workers, credentials, plugins).
    println!(
        "installed {} v{} → namespace {namespace} ({} sequence(s))",
        manifest.name,
        manifest.version,
        installed.len()
    );
    for (name, version, id) in &installed {
        let resp = client
            .get(format!("{base}/sequences/{id}/preflight"))
            .send()
            .await?;
        if let Ok(report) = resp.json::<Value>().await {
            println!(
                "  preflight {name} v{version}: {}",
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
    }

    // 7. Record provenance in the lockfile.
    lock.insert(
        manifest.name.clone(),
        json!({
            "version": manifest.version,
            "content_hash": pkg.content_hash,
            "public_key": pkg.public_key,
            "namespace": namespace,
            "installed_at": chrono::Utc::now().to_rfc3339(),
        }),
    );
    std::fs::write(LOCKFILE, serde_json::to_string_pretty(&lock)?)?;
    println!("provenance recorded in {LOCKFILE}");
    Ok(())
}
