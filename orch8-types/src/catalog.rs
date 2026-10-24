//! Signed catalog document contract. Local account IDs and bearer tokens never
//! select the remote audience; the catalog authority signs stable descriptors.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogFormat {
    #[serde(rename = "orch8.catalog.v1")]
    V1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CatalogAccess {
    Public,
    Principal { issuer: String, subject: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogAudience {
    /// Stable source descriptor; rotating URL credentials are not part of it.
    pub source: String,
    pub catalog: String,
    pub access: CatalogAccess,
}

impl CatalogAudience {
    pub fn validate(&self) -> Result<(), &'static str> {
        let mut fields = vec![self.source.as_str(), self.catalog.as_str()];
        if let CatalogAccess::Principal { issuer, subject } = &self.access {
            fields.extend([issuer.as_str(), subject.as_str()]);
        }
        if fields.iter().any(|value| {
            value.trim().is_empty() || value.len() > 4096 || value.chars().any(char::is_control)
        }) {
            return Err("invalid catalog audience descriptor");
        }
        Ok(())
    }
}

/// The complete envelope is signed, including its format and audience. It is
/// deliberately distinct from an unscoped legacy manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogManifest<T> {
    pub format: CatalogFormat,
    pub audience: CatalogAudience,
    pub manifest: T,
}
