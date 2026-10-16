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

    #[derive(Debug, PartialEq, Eq)]
    enum Outcome {
        Ok,
        Retryable,
        Permanent,
    }

    fn outcome(result: &Result<String, HandlerError>) -> Outcome {
        match result {
            Ok(_) => Outcome::Ok,
            Err(HandlerError::Retryable { .. }) => Outcome::Retryable,
            Err(HandlerError::Permanent { .. }) => Outcome::Permanent,
        }
    }

    fn request(operation: &str, foreground: bool) -> String {
        serde_json::json!({"operation": operation, "foreground": foreground}).to_string()
    }

    /// Unlisted operations are permanent failures (retrying cannot help);
    /// a listed operation outside the foreground is retryable for
    /// foreground-only capabilities; secure storage works in the background.
    #[test]
    fn bridge_outcome_follows_the_operation_allow_list_and_foreground_rule() {
        use DeviceCapability::{Biometric, Camera, File, SecureStorage};
        let cases = [
            (Camera, "capture", true, Outcome::Ok),
            (Camera, "capture", false, Outcome::Retryable),
            (Camera, "pick", true, Outcome::Permanent),
            (Camera, "pick", false, Outcome::Permanent),
            (File, "pick", true, Outcome::Ok),
            (File, "read_scoped", true, Outcome::Ok),
            (File, "read_scoped", false, Outcome::Retryable),
            (File, "capture", true, Outcome::Permanent),
            (Biometric, "verify", true, Outcome::Ok),
            (Biometric, "verify", false, Outcome::Retryable),
            (Biometric, "get", true, Outcome::Permanent),
            (SecureStorage, "get", false, Outcome::Ok),
            (SecureStorage, "put", false, Outcome::Ok),
            (SecureStorage, "delete", false, Outcome::Ok),
            (SecureStorage, "delete", true, Outcome::Ok),
            (SecureStorage, "list", false, Outcome::Permanent),
            (SecureStorage, "", false, Outcome::Permanent),
            (SecureStorage, "GET", false, Outcome::Permanent),
            (SecureStorage, " get ", false, Outcome::Permanent),
        ];
        let bridge = DeviceToolBridge::new(ReferenceHost);
        for (capability, operation, foreground, expected) in cases {
            let result = bridge.execute(capability, &request(operation, foreground));

            assert_eq!(
                outcome(&result),
                expected,
                "{capability:?} {operation:?} foreground={foreground}: {result:?}"
            );
        }
    }

    #[test]
    fn malformed_requests_fail_permanently() {
        let bridge = DeviceToolBridge::new(ReferenceHost);
        for body in [
            "",
            "null",
            "[]",
            r#"{"foreground":true}"#,
            r#"{"operation":"capture"}"#,
        ] {
            let result = bridge.execute(DeviceCapability::Camera, body);

            assert_eq!(outcome(&result), Outcome::Permanent, "body: {body:?}");
        }
    }

    /// Counts host calls and answers with a fixed response or error.
    struct ScriptedHost {
        calls: std::sync::atomic::AtomicUsize,
        reply: fn() -> Result<CapabilityResponse, HandlerError>,
    }

    impl ScriptedHost {
        fn new(reply: fn() -> Result<CapabilityResponse, HandlerError>) -> Self {
            Self {
                calls: std::sync::atomic::AtomicUsize::new(0),
                reply,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl CapabilityHost for &ScriptedHost {
        fn invoke(
            &self,
            _capability: DeviceCapability,
            _request: &CapabilityRequest,
        ) -> Result<CapabilityResponse, HandlerError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            (self.reply)()
        }
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "matches the ScriptedHost reply signature"
    )]
    fn small_response() -> Result<CapabilityResponse, HandlerError> {
        Ok(CapabilityResponse {
            value: serde_json::json!({"ok": true}),
            protected_reference: None,
        })
    }

    /// A secure-storage `put` request whose JSON encoding is exactly `len` bytes.
    fn put_request_of_len(len: usize) -> String {
        let shell = |pad: &str| {
            serde_json::json!({"operation": "put", "foreground": false, "arguments": {"v": pad}})
                .to_string()
        };
        let pad = "x".repeat(len - shell("").len());
        let body = shell(&pad);
        assert_eq!(body.len(), len);
        body
    }

    #[test]
    fn request_at_the_256_kib_limit_reaches_the_host() {
        let host = ScriptedHost::new(small_response);
        let bridge = DeviceToolBridge::new(&host);

        let result = bridge.execute(
            DeviceCapability::SecureStorage,
            &put_request_of_len(MAX_REQUEST_BYTES),
        );

        assert_eq!(outcome(&result), Outcome::Ok, "{result:?}");
        assert_eq!(host.calls(), 1);
    }

    #[test]
    fn request_over_256_kib_fails_permanently_without_calling_the_host() {
        let host = ScriptedHost::new(small_response);
        let bridge = DeviceToolBridge::new(&host);

        let result = bridge.execute(
            DeviceCapability::SecureStorage,
            &put_request_of_len(MAX_REQUEST_BYTES + 1),
        );

        assert!(
            matches!(&result, Err(HandlerError::Permanent { message }) if message.contains("256 KiB")),
            "{result:?}"
        );
        assert_eq!(host.calls(), 0);
    }

    /// `{"value":"<pad>","protected_reference":null}` encoded to exactly `len` bytes.
    fn response_of_len(len: usize) -> CapabilityResponse {
        let response = |pad: String| CapabilityResponse {
            value: serde_json::Value::String(pad),
            protected_reference: None,
        };
        let overhead = serde_json::to_string(&response(String::new()))
            .unwrap()
            .len();
        response("x".repeat(len - overhead))
    }

    #[test]
    fn response_at_the_5_mib_limit_is_returned() {
        let host = ScriptedHost::new(|| Ok(response_of_len(MAX_RESPONSE_BYTES)));
        let bridge = DeviceToolBridge::new(&host);

        let result = bridge.execute(DeviceCapability::SecureStorage, &request("get", false));

        assert_eq!(result.map(|body| body.len()).ok(), Some(MAX_RESPONSE_BYTES));
    }

    #[test]
    fn response_over_5_mib_fails_permanently() {
        let host = ScriptedHost::new(|| Ok(response_of_len(MAX_RESPONSE_BYTES + 1)));
        let bridge = DeviceToolBridge::new(&host);

        let result = bridge.execute(DeviceCapability::SecureStorage, &request("get", false));

        assert!(
            matches!(&result, Err(HandlerError::Permanent { message }) if message.contains("5 MiB")),
            "{result:?}"
        );
    }

    #[test]
    fn host_errors_propagate_with_their_retry_class_and_message() {
        let retryable = ScriptedHost::new(|| {
            Err(HandlerError::Retryable {
                message: "keychain locked".into(),
            })
        });
        let permanent = ScriptedHost::new(|| {
            Err(HandlerError::Permanent {
                message: "permission denied".into(),
            })
        });

        let retry = DeviceToolBridge::new(&retryable)
            .execute(DeviceCapability::SecureStorage, &request("get", false));
        let fail = DeviceToolBridge::new(&permanent)
            .execute(DeviceCapability::SecureStorage, &request("get", false));

        assert!(
            matches!(&retry, Err(HandlerError::Retryable { message }) if message == "keychain locked"),
            "{retry:?}"
        );
        assert!(
            matches!(&fail, Err(HandlerError::Permanent { message }) if message == "permission denied"),
            "{fail:?}"
        );
    }
}
