//! Standard mobile device capability bridge.
//!
//! Platform code owns permission prompts and OS APIs. Orch8 owns a bounded,
//! auditable request/response contract so workflows do not receive arbitrary
//! host access.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::error::HandlerError;

const MAX_REQUEST_BYTES: usize = 256 * 1024;
const MAX_RESPONSE_BYTES: usize = 5 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, uniffi::Enum)]
#[serde(rename_all = "snake_case")]
pub enum DeviceCapability {
    Camera,
    File,
    Biometric,
    SecureStorage,
}

impl DeviceCapability {
    #[must_use]
    pub fn handler_name(self) -> &'static str {
        match self {
            Self::Camera => "device.camera",
            Self::File => "device.file",
            Self::Biometric => "device.biometric",
            Self::SecureStorage => "device.secure_storage",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, uniffi::Record)]
pub struct CapabilityDescriptor {
    pub handler: String,
    pub capability: DeviceCapability,
    pub operations: Vec<String>,
    pub requires_foreground: bool,
    pub handles_protected_data: bool,
    pub max_response_bytes: u64,
}

#[must_use]
pub fn standard_capability_descriptors() -> Vec<CapabilityDescriptor> {
    vec![
        descriptor(DeviceCapability::Camera, &["capture"], true, true),
        descriptor(DeviceCapability::File, &["pick", "read_scoped"], true, true),
        descriptor(DeviceCapability::Biometric, &["verify"], true, false),
        descriptor(
            DeviceCapability::SecureStorage,
            &["get", "put", "delete"],
            false,
            true,
        ),
    ]
}

fn descriptor(
    capability: DeviceCapability,
    operations: &[&str],
    requires_foreground: bool,
    handles_protected_data: bool,
) -> CapabilityDescriptor {
    CapabilityDescriptor {
        handler: capability.handler_name().into(),
        capability,
        operations: operations.iter().map(ToString::to_string).collect(),
        requires_foreground,
        handles_protected_data,
        max_response_bytes: MAX_RESPONSE_BYTES as u64,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityRequest {
    pub operation: String,
    #[serde(default)]
    pub arguments: BTreeMap<String, String>,
    pub foreground: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityResponse {
    pub value: serde_json::Value,
    /// Opaque host reference; raw camera/file/secret bytes should normally be
    /// retained by the host and represented by this handle.
    pub protected_reference: Option<String>,
}

/// Platform-owned implementation. Swift/Kotlin implementations should retain
/// protected bytes inside OS storage and return opaque references.
pub trait CapabilityHost: Send + Sync {
    fn invoke(
        &self,
        capability: DeviceCapability,
        request: &CapabilityRequest,
    ) -> Result<CapabilityResponse, HandlerError>;
}

pub struct DeviceToolBridge<H> {
    host: H,
}

impl<H: CapabilityHost> DeviceToolBridge<H> {
    pub fn new(host: H) -> Self {
        Self { host }
    }

    pub fn execute(
        &self,
        capability: DeviceCapability,
        request_json: &str,
    ) -> Result<String, HandlerError> {
        if request_json.len() > MAX_REQUEST_BYTES {
            return Err(permanent("device capability request exceeds 256 KiB"));
        }
        let request: CapabilityRequest = serde_json::from_str(request_json)
            .map_err(|error| permanent(format!("invalid capability request: {error}")))?;
        let descriptor = standard_capability_descriptors()
            .into_iter()
            .find(|item| item.capability == capability)
            .ok_or_else(|| permanent("unknown device capability"))?;
        if !descriptor.operations.contains(&request.operation) {
            return Err(permanent(format!(
                "operation {} is not allowed for {}",
                request.operation, descriptor.handler
            )));
        }
        if descriptor.requires_foreground && !request.foreground {
            return Err(HandlerError::Retryable {
                message: "capability requires an active foreground session".into(),
            });
        }
        let response = self.host.invoke(capability, &request)?;
        let encoded = serde_json::to_string(&response)
            .map_err(|error| permanent(format!("serialize capability response: {error}")))?;
        if encoded.len() > MAX_RESPONSE_BYTES {
            return Err(permanent("device capability response exceeds 5 MiB"));
        }
        Ok(encoded)
    }
}

fn permanent(message: impl Into<String>) -> HandlerError {
    HandlerError::Permanent {
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct ReferenceHost;

    impl CapabilityHost for ReferenceHost {
        fn invoke(
            &self,
            capability: DeviceCapability,
            request: &CapabilityRequest,
        ) -> Result<CapabilityResponse, HandlerError> {
            let reference = match capability {
                DeviceCapability::Camera => Some("camera://capture/1"),
                DeviceCapability::File => Some("file://scoped/1"),
                DeviceCapability::SecureStorage if request.operation == "get" => {
                    Some("secure://key/1")
                }
                _ => None,
            };
            Ok(CapabilityResponse {
                value: serde_json::json!({"approved": true}),
                protected_reference: reference.map(str::to_owned),
            })
        }
    }

    #[test]
    fn descriptors_cover_all_reference_handlers() {
        let descriptors = standard_capability_descriptors();
        assert_eq!(descriptors.len(), 4);
        assert!(
            descriptors
                .iter()
                .any(|item| item.handler == "device.camera")
        );
        assert!(descriptors.iter().any(|item| item.handler == "device.file"));
        assert!(
            descriptors
                .iter()
                .any(|item| item.handler == "device.biometric")
        );
        assert!(
            descriptors
                .iter()
                .any(|item| item.handler == "device.secure_storage")
        );
    }

    #[test]
    fn reference_bridge_returns_only_opaque_protected_handles() {
        let bridge = DeviceToolBridge::new(ReferenceHost);
        for (capability, operation) in [
            (DeviceCapability::Camera, "capture"),
            (DeviceCapability::File, "pick"),
            (DeviceCapability::Biometric, "verify"),
            (DeviceCapability::SecureStorage, "get"),
        ] {
            let output = bridge
                .execute(
                    capability,
                    &serde_json::json!({"operation": operation, "foreground": true}).to_string(),
                )
                .unwrap();
            assert!(!output.contains("raw_bytes"));
        }
    }

    #[test]
    fn bridge_rejects_unlisted_and_background_operations() {
        let bridge = DeviceToolBridge::new(ReferenceHost);
        let invalid = serde_json::json!({"operation": "delete_all", "foreground": true});
        assert!(
            bridge
                .execute(DeviceCapability::File, &invalid.to_string())
                .is_err()
        );
        let background = serde_json::json!({"operation": "capture", "foreground": false});
        assert!(matches!(
            bridge.execute(DeviceCapability::Camera, &background.to_string()),
            Err(HandlerError::Retryable { .. })
        ));
    }
}
