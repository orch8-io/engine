use std::collections::HashSet;

use orch8_publisher::package::{SignedPackage, TrustPolicy, check_trust, verify_package};
use orch8_types::ids::Namespace;
use orch8_types::sequence::deserialize_sequence_strict;

use crate::storage::MobileStorage;
use crate::{MobileEngine, MobileError, mobile_tenant_id};

const MAX_PACKAGE_BYTES: usize = 8 * 1024 * 1024;

/// Whether first-launch installation could seed the local catalog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum BundledCatalogDisposition {
    Seeded,
    AlreadyPresent,
    ManifestOwned,
}

/// Result of a first-launch catalog installation.
#[derive(Debug, Clone, PartialEq, Eq, uniffi::Record)]
pub struct BundledCatalogInstall {
    pub disposition: BundledCatalogDisposition,
    pub installed: u32,
    pub skipped: u32,
}

fn invalid(detail: impl Into<String>) -> MobileError {
    MobileError::InvalidInput {
        detail: detail.into(),
    }
}

pub(crate) async fn install(
    engine: &MobileEngine,
    json: String,
) -> Result<BundledCatalogInstall, MobileError> {
    if engine.config.root_public_key.is_empty() {
        return Err(invalid(
            "a configured root key is required for a bundled catalog",
        ));
    }
    if json.len() > MAX_PACKAGE_BYTES {
        return Err(MobileError::ResourceLimit {
            detail: "bundled catalog exceeds 8 MiB".into(),
        });
    }
    let package: SignedPackage = serde_json::from_str(&json)?;
    drop(json);
    verify_package(&package).map_err(|error| invalid(error.to_string()))?;
    check_trust(
        &package,
        &TrustPolicy {
            trusted_keys: vec![engine.config.root_public_key.clone()],
            allow_untrusted: false,
        },
    )
    .map_err(|error| invalid(error.to_string()))?;

    let tenant = mobile_tenant_id();
    let namespace = Namespace::new("default");
    let mut names = HashSet::new();
    let mut sequences = Vec::new();
    for (path, definition) in package.archive.files {
        if !path.starts_with("sequences/") || !path.as_bytes().ends_with(b".json") {
            continue;
        }
        if definition.len() as u64 > engine.config.max_sequence_size_bytes {
            return Err(MobileError::ResourceLimit {
                detail: "bundled sequence exceeds the configured sequence size limit".into(),
            });
        }
        let value = serde_json::from_str(&definition)?;
        let sequence =
            deserialize_sequence_strict(&value).map_err(|error| invalid(error.to_string()))?;
        sequence
            .validate()
            .map_err(|error| invalid(error.to_string()))?;
        if sequence.tenant_id != tenant
            || sequence.namespace != namespace
            || sequence.sub_tenant.is_some()
        {
            return Err(invalid(
                "bundled sequences must belong to the mobile default catalog",
            ));
        }
        if sequence.deprecated || !names.insert(sequence.name.clone()) {
            return Err(invalid(
                "bundled sequences require unique names and non-deprecated versions",
            ));
        }
        sequences.push(sequence);
        if sequences.len() > engine.config.max_stored_sequences as usize {
            return Err(MobileError::ResourceLimit {
                detail: "bundled catalog exceeds the configured sequence count limit".into(),
            });
        }
    }
    if sequences.is_empty() {
        return Err(invalid("bundled catalog has no JSON sequence definitions"));
    }
    Ok(MobileStorage::new(engine.sqlite.clone())
        .install_legacy_seed(&sequences, engine.config.max_stored_sequences)
        .await?)
}
