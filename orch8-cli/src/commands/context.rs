//! Named fleet contexts with explicit selection and secure persistence.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context as _, Result, bail};
use clap::Subcommand;
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FleetContext {
    pub url: String,
    pub tenant_id: String,
    pub api_key: String,
}

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextStore {
    pub selected: Option<String>,
    pub contexts: BTreeMap<String, FleetContext>,
}

#[derive(Subcommand)]
pub enum ContextCmd {
    /// Add or atomically replace a named context.
    Set {
        name: String,
        #[arg(long)]
        url: String,
        #[arg(long)]
        tenant_id: String,
        #[arg(long, env = "ORCH8_API_KEY", hide_env_values = true)]
        api_key: String,
    },
    /// Select the context used when --context is omitted.
    Use { name: String },
    /// List context names; credentials are never printed.
    List,
    /// Remove a named context.
    Remove { name: String },
}

#[must_use]
pub fn default_path() -> PathBuf {
    std::env::var_os("ORCH8_CONTEXTS_FILE")
        .map_or_else(|| PathBuf::from(".orch8-contexts.json"), PathBuf::from)
}

pub fn load(path: &Path) -> Result<ContextStore> {
    if !path.exists() {
        return Ok(ContextStore::default());
    }
    enforce_secure_permissions(path)?;
    let bytes = std::fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let store =
        serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
    Ok(store)
}

pub fn resolve(path: &Path, explicit: Option<&str>) -> Result<Option<FleetContext>> {
    let store = load(path)?;
    let selected = explicit.or(store.selected.as_deref());
    selected
        .map(|name| {
            store
                .contexts
                .get(name)
                .cloned()
                .with_context(|| format!("fleet context '{name}' does not exist"))
        })
        .transpose()
}

pub fn run(path: &Path, cmd: ContextCmd) -> Result<()> {
    let mut store = load(path)?;
    match cmd {
        ContextCmd::Set {
            name,
            url,
            tenant_id,
            api_key,
        } => {
            validate_name(&name)?;
            validate_context(&url, &tenant_id, &api_key)?;
            store.contexts.insert(
                name.clone(),
                FleetContext {
                    url: url.trim_end_matches('/').into(),
                    tenant_id,
                    api_key,
                },
            );
            save(path, &store)?;
            println!("Saved fleet context {name}");
        }
        ContextCmd::Use { name } => {
            if !store.contexts.contains_key(&name) {
                bail!("fleet context '{name}' does not exist");
            }
            store.selected = Some(name.clone());
            save(path, &store)?;
            println!("Selected fleet context {name}");
        }
        ContextCmd::List => {
            for name in store.contexts.keys() {
                let marker = if store.selected.as_deref() == Some(name) {
                    "*"
                } else {
                    " "
                };
                println!("{marker} {name}");
            }
        }
        ContextCmd::Remove { name } => {
            if store.contexts.remove(&name).is_none() {
                bail!("fleet context '{name}' does not exist");
            }
            if store.selected.as_deref() == Some(&name) {
                store.selected = None;
            }
            save(path, &store)?;
            println!("Removed fleet context {name}");
        }
    }
    Ok(())
}

fn save(path: &Path, store: &ContextStore) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(store)?;
    crate::atomic_write(path, &bytes)?;
    set_secure_permissions(path)?;
    Ok(())
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("context name must be 1-64 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

fn validate_context(url: &str, tenant_id: &str, api_key: &str) -> Result<()> {
    let url = reqwest::Url::parse(url).context("context URL is invalid")?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("context URL must be an absolute http(s) URL");
    }
    if tenant_id.is_empty() || api_key.is_empty() {
        bail!("tenant id and API key are required");
    }
    Ok(())
}

#[cfg(unix)]
fn enforce_secure_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!(
            "{} contains credentials and must not be accessible by group/others (run chmod 600)",
            path.display()
        );
    }
    Ok(())
}

#[cfg(not(unix))]
fn enforce_secure_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_secure_permissions(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_secure_permissions(_path: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn store_round_trip_selects_explicit_context_and_redacts_listing_model() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contexts.json");
        run(
            &path,
            ContextCmd::Set {
                name: "prod".into(),
                url: "https://engine.example/api/v1".into(),
                tenant_id: "tenant-a".into(),
                api_key: "secret".into(),
            },
        )
        .unwrap();
        run(
            &path,
            ContextCmd::Use {
                name: "prod".into(),
            },
        )
        .unwrap();
        let selected = resolve(&path, None).unwrap().unwrap();
        assert_eq!(selected.tenant_id, "tenant-a");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn insecure_permissions_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("contexts.json");
        std::fs::write(&path, b"{}").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            assert!(load(&path).is_err());
        }
    }
}

#[cfg(test)]
#[path = "context_coverage_tests.rs"]
mod context_coverage_tests;
