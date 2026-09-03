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

use orch8_publisher::cdn::S3CdnBackend;
use orch8_publisher::package::{
    PackageManifest, PackageRequirements, SignedPackage, TrustLevel, TrustPolicy, build_package,
    check_trust, check_upgrade, contract_files, install_namespace, sequence_files, verify_package,
};
use orch8_publisher::registry::PackageRegistryPublisher;
use orch8_publisher::registry::{RegistryIndex, RegistryVersion, TransparencyLedger};

use crate::OutputFormat;
use crate::atomic_write;

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
    /// Search a verified hosted registry index.
    Search {
        query: Option<String>,
        #[arg(long, env = "ORCH8_PACKAGE_REGISTRY_URL")]
        registry_url: String,
    },
    /// Publish a signed package to an S3-compatible, append-only registry.
    Publish {
        file: PathBuf,
        /// Base64 publisher seed; must match the key used to sign the package.
        #[arg(long)]
        key: String,
        #[arg(long)]
        tenant_id: String,
        #[arg(long)]
        namespace: String,
        /// Public bucket URL used to load the current index and ledger.
        #[arg(long, env = "ORCH8_PACKAGE_REGISTRY_PUBLIC_URL")]
        public_url: String,
        #[arg(long, env = "ORCH8_REGISTRY_S3_ENDPOINT")]
        endpoint: String,
        #[arg(long, env = "ORCH8_REGISTRY_S3_BUCKET")]
        bucket: String,
        #[arg(long, env = "ORCH8_REGISTRY_S3_REGION", default_value = "auto")]
        region: String,
        #[arg(long, env = "ORCH8_REGISTRY_S3_ACCESS_KEY", hide_env_values = true)]
        access_key: String,
        #[arg(long, env = "ORCH8_REGISTRY_S3_SECRET_KEY", hide_env_values = true)]
        secret_key: String,
    },
    /// Verify, test, and install a package's sequences under its own
    /// namespace. Never overwrites existing sequences.
    Install {
        /// Local .orch8pkg file. Omit when using --name.
        file: Option<PathBuf>,
        /// Package name from a hosted registry (for example acme/checkout).
        #[arg(long, conflicts_with = "file")]
        name: Option<String>,
        /// Exact version; defaults to the newest published version.
        #[arg(long, requires = "name")]
        version: Option<String>,
        /// Hosted registry namespace root containing index.json.
        #[arg(long, env = "ORCH8_PACKAGE_REGISTRY_URL", requires = "name")]
        registry_url: Option<String>,
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
        PackageCmd::Search {
            query,
            registry_url,
        } => search_registry(&registry_url, query.as_deref()).await,
        PackageCmd::Publish {
            file,
            key,
            tenant_id,
            namespace,
            public_url,
            endpoint,
            bucket,
            region,
            access_key,
            secret_key,
        } => {
            let package = read_package(&file)?;
            verify_package(&package)?;
            let signing_key = load_signing_key(&key)?;
            let root = format!(
                "{}/{}/registry/{}",
                public_url.trim_end_matches('/'),
                tenant_id,
                namespace
            );
            let (mut index, mut ledger) =
                load_registry_state(&root, &tenant_id, &namespace).await?;
            let backend = S3CdnBackend::new(endpoint, bucket, region, access_key, secret_key);
            let publisher = PackageRegistryPublisher::new(Box::new(backend), tenant_id, namespace)?;
            let publication = publisher
                .publish(
                    &package,
                    &mut index,
                    &mut ledger,
                    &signing_key,
                    chrono::Utc::now(),
                )
                .await?;
            println!(
                "published {}@{} → {}",
                package.archive.manifest.name, publication.version, publication.package_url
            );
            Ok(())
        }
        PackageCmd::Install {
            file,
            name,
            version,
            registry_url,
            tenant_id,
            trusted_keys,
            allow_untrusted,
            skip_contracts,
        } => {
            let downloaded;
            let file = if let Some(file) = file {
                file
            } else {
                let name = name.context("install requires a local file or --name")?;
                let registry_url =
                    registry_url.context("--registry-url is required with --name")?;
                let (_, selected) =
                    select_registry_package(&registry_url, &name, version.as_deref()).await?;
                downloaded = tempfile::NamedTempFile::new()?;
                let base = reqwest::Url::parse(&registry_url)?;
                let url = base.join(&selected.package_url)?;
                download_registry_package(url, downloaded.path()).await?;
                downloaded.path().to_path_buf()
            };
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

async fn load_registry_state(
    root: &str,
    tenant: &str,
    namespace: &str,
) -> Result<(RegistryIndex, TransparencyLedger)> {
    let client = Client::new();
    let response = client.get(format!("{root}/index.json")).send().await?;
    if response.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok((
            RegistryIndex::new(tenant, namespace),
            TransparencyLedger::default(),
        ));
    }
    let response = response.error_for_status()?;
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let mut index: RegistryIndex = response.json().await?;
    if let Some(etag) = etag {
        index = index.with_source_etag(etag);
    }
    let ledger = if let Some(head) = index.ledger_head.as_deref() {
        client
            .get(format!("{root}/transparency/ledgers/{head}.json"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?
    } else {
        TransparencyLedger::default()
    };
    index.verify_against(&ledger)?;
    Ok((index, ledger))
}

async fn verified_registry(base: &str) -> Result<RegistryIndex> {
    // Hosted registries are a separate trust boundary and must never receive
    // the engine client's default x-api-key or tenant headers.
    let client = Client::new();
    let base = base.trim_end_matches('/');
    let index: RegistryIndex = client
        .get(format!("{base}/index.json"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let head = index
        .ledger_head
        .as_deref()
        .context("registry index has no ledger head")?;
    let ledger: TransparencyLedger = client
        .get(format!("{base}/transparency/ledgers/{head}.json"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    index.verify_against(&ledger)?;
    Ok(index)
}

async fn search_registry(base: &str, query: Option<&str>) -> Result<()> {
    let index = verified_registry(base).await?;
    let needle = query.unwrap_or("").to_ascii_lowercase();
    for (name, versions) in index.packages {
        if needle.is_empty() || name.to_ascii_lowercase().contains(&needle) {
            let latest = versions
                .last()
                .map_or("-", |version| version.version.as_str());
            println!("{name}\t{latest}\t{} version(s)", versions.len());
        }
    }
    Ok(())
}

async fn select_registry_package(
    base: &str,
    name: &str,
    version: Option<&str>,
) -> Result<(RegistryIndex, RegistryVersion)> {
    let index = verified_registry(base).await?;
    let versions = index
        .packages
        .get(name)
        .with_context(|| format!("package {name} not found"))?;
    let selected = version
        .map_or_else(
            || versions.last(),
            |wanted| versions.iter().find(|item| item.version == wanted),
        )
        .with_context(|| {
            format!(
                "package {name} version {} not found",
                version.unwrap_or("latest")
            )
        })?
        .clone();
    Ok((index, selected))
}

async fn download_registry_package(mut url: reqwest::Url, path: &Path) -> Result<()> {
    use tokio::io::AsyncWriteExt as _;

    const MAX_PACKAGE_BYTES: u64 = 64 * 1024 * 1024;
    // Never forward URL credentials into logs or redirects.
    if !url.username().is_empty() || url.password().is_some() {
        bail!("registry package URL must not contain credentials");
    }
    url.set_fragment(None);
    let mut response = Client::new().get(url).send().await?.error_for_status()?;
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PACKAGE_BYTES)
    {
        bail!("registry package exceeds the 64 MiB download limit");
    }
    let mut output = tokio::fs::File::create(path).await?;
    let mut total = 0_u64;
    while let Some(chunk) = response.chunk().await? {
        total = total.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if total > MAX_PACKAGE_BYTES {
            bail!("registry package exceeds the 64 MiB download limit");
        }
        output.write_all(&chunk).await?;
    }
    output.sync_all().await?;
    Ok(())
}

fn keygen() {
    let key = ed25519_dalek::SigningKey::generate(&mut rand::rng());
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
    atomic_write(&out_path, serde_json::to_string_pretty(&pkg)?.as_bytes())?;
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
            Ok(TrustLevel::UntrustedAllowed) => {
                bail!("package is untrusted (allow_untrusted is false)")
            }
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
    // 3. Lockfile downgrade check. A missing lockfile is fine; a corrupt one
    // must not silently disable the check (step 7 would then overwrite it).
    let mut lock: BTreeMap<String, Value> = match std::fs::read_to_string(LOCKFILE) {
        Ok(raw) => serde_json::from_str(&raw).with_context(|| {
            format!("{LOCKFILE} is corrupt — fix or remove it before installing")
        })?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
        Err(e) => bail!("read {LOCKFILE}: {e}"),
    };
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
        // Only a genuine 404 means "does not exist"; any other non-success
        // status (500, 401/403, …) must not silently degrade the
        // never-overwrite guarantee.
        let status = existing.status();
        if status.is_success() {
            bail!(
                "sequence {namespace}/{name} v{version} already exists — refusing to \
                 overwrite (uninstall or bump the package version)"
            );
        } else if status.as_u16() != 404 {
            bail!(
                "conflict check for {namespace}/{name} v{version} failed: server returned \
                 {status} — refusing to install"
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
    atomic_write(
        Path::new(LOCKFILE),
        serde_json::to_string_pretty(&lock)?.as_bytes(),
    )?;
    println!("provenance recorded in {LOCKFILE}");
    Ok(())
}

#[cfg(test)]
#[path = "package_cmd_coverage_tests.rs"]
mod package_cmd_coverage_tests;
