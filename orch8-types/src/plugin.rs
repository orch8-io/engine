use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// The kind of external plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum PluginType {
    Wasm,
    Grpc,
}

impl fmt::Display for PluginType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wasm => f.write_str("wasm"),
            Self::Grpc => f.write_str("grpc"),
        }
    }
}

impl PluginType {
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s {
            "wasm" => Some(Self::Wasm),
            "grpc" => Some(Self::Grpc),
            _ => None,
        }
    }
}

/// A persisted plugin definition that maps a handler name to an external implementation.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PluginDef {
    /// Unique plugin name (used as handler name prefix, e.g. `wasm://my-plugin`).
    pub name: String,
    /// Plugin type: `wasm`, `grpc`.
    pub plugin_type: PluginType,
    /// Path or URL to the plugin binary/endpoint.
    /// - WASM: file path to `.wasm` module
    /// - gRPC: `host:port/Service.Method`
    pub source: String,
    /// Tenant that owns this plugin (empty = global).
    #[serde(default)]
    pub tenant_id: String,
    /// Whether this plugin is active.
    #[serde(default = "crate::serde_defaults::yes")]
    pub enabled: bool,
    /// Plugin-specific configuration (JSON).
    #[serde(default)]
    pub config: serde_json::Value,
    /// Human-readable description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_type_display() {
        assert_eq!(PluginType::Wasm.to_string(), "wasm");
        assert_eq!(PluginType::Grpc.to_string(), "grpc");
    }

    #[test]
    fn plugin_type_from_str_loose() {
        assert_eq!(PluginType::from_str_loose("wasm"), Some(PluginType::Wasm));
        assert_eq!(PluginType::from_str_loose("grpc"), Some(PluginType::Grpc));
        assert_eq!(PluginType::from_str_loose("http"), None);
        assert_eq!(PluginType::from_str_loose(""), None);
        assert_eq!(PluginType::from_str_loose("WASM"), None);
    }

    #[test]
    fn plugin_type_serde_round_trip() {
        let wasm: PluginType = serde_json::from_str(r#""wasm""#).unwrap();
        assert_eq!(wasm, PluginType::Wasm);
        assert_eq!(serde_json::to_string(&wasm).unwrap(), r#""wasm""#);

        let grpc: PluginType = serde_json::from_str(r#""grpc""#).unwrap();
        assert_eq!(grpc, PluginType::Grpc);
    }

    #[test]
    fn plugin_type_serde_rejects_invalid() {
        let result = serde_json::from_str::<PluginType>(r#""http""#);
        assert!(result.is_err());
    }

    #[test]
    fn plugin_def_defaults() {
        let json = r#"{
            "name": "test",
            "plugin_type": "wasm",
            "source": "/opt/plugins/test.wasm",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let def: PluginDef = serde_json::from_str(json).unwrap();
        assert!(def.enabled); // default true
        assert_eq!(def.tenant_id, ""); // default empty
        assert_eq!(def.config, serde_json::Value::Null); // default null
        assert!(def.description.is_none());
    }

    #[test]
    fn plugin_def_round_trip() {
        let now = Utc::now();
        let def = PluginDef {
            name: "my-plugin".into(),
            plugin_type: PluginType::Grpc,
            source: "localhost:50051/Svc.Run".into(),
            tenant_id: "t1".into(),
            enabled: false,
            config: serde_json::json!({"key": "val"}),
            description: Some("test plugin".into()),
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&def).unwrap();
        let back: PluginDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.name, "my-plugin");
        assert_eq!(back.plugin_type, PluginType::Grpc);
        assert!(!back.enabled);
        assert_eq!(back.description, Some("test plugin".into()));
    }
}
