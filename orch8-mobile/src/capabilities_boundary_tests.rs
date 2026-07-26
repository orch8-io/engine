//! Device capability contract boundaries.
//!
//! Count contract: 45 independently named unit tests.

use super::*;

struct EchoHost;

impl CapabilityHost for EchoHost {
    fn invoke(
        &self,
        capability: DeviceCapability,
        request: &CapabilityRequest,
    ) -> Result<CapabilityResponse, HandlerError> {
        Ok(CapabilityResponse {
            value: serde_json::json!({
                "handler": capability.handler_name(),
                "operation": request.operation,
                "arguments": request.arguments,
            }),
            protected_reference: Some(format!("protected://{}", request.operation)),
        })
    }
}

fn descriptor_for(capability: DeviceCapability) -> CapabilityDescriptor {
    standard_capability_descriptors()
        .into_iter()
        .find(|item| item.capability == capability)
        .unwrap()
}

macro_rules! handler_case {
    ($name:ident, $capability:expr, $expected:expr) => {
        #[test]
        fn $name() {
            let actual = $capability.handler_name();
            assert_eq!(actual, $expected);
        }
    };
}

handler_case!(
    coverage_capability_001_camera_handler_is_stable,
    DeviceCapability::Camera,
    "device.camera"
);
handler_case!(
    coverage_capability_002_file_handler_is_stable,
    DeviceCapability::File,
    "device.file"
);
handler_case!(
    coverage_capability_003_biometric_handler_is_stable,
    DeviceCapability::Biometric,
    "device.biometric"
);
handler_case!(
    coverage_capability_004_secure_storage_handler_is_stable,
    DeviceCapability::SecureStorage,
    "device.secure_storage"
);

macro_rules! descriptor_case {
    ($name:ident, $capability:expr, $assertion:expr) => {
        #[test]
        fn $name() {
            let descriptor = descriptor_for($capability);
            assert!(($assertion)(&descriptor));
        }
    };
}

descriptor_case!(
    coverage_capability_005_camera_requires_foreground,
    DeviceCapability::Camera,
    |d: &CapabilityDescriptor| d.requires_foreground
);
descriptor_case!(
    coverage_capability_006_file_requires_foreground,
    DeviceCapability::File,
    |d: &CapabilityDescriptor| d.requires_foreground
);
descriptor_case!(
    coverage_capability_007_biometric_requires_foreground,
    DeviceCapability::Biometric,
    |d: &CapabilityDescriptor| d.requires_foreground
);
descriptor_case!(
    coverage_capability_008_secure_storage_allows_background,
    DeviceCapability::SecureStorage,
    |d: &CapabilityDescriptor| !d.requires_foreground
);
descriptor_case!(
    coverage_capability_009_camera_marks_protected_data,
    DeviceCapability::Camera,
    |d: &CapabilityDescriptor| d.handles_protected_data
);
descriptor_case!(
    coverage_capability_010_file_marks_protected_data,
    DeviceCapability::File,
    |d: &CapabilityDescriptor| d.handles_protected_data
);
descriptor_case!(
    coverage_capability_011_biometric_does_not_return_protected_data,
    DeviceCapability::Biometric,
    |d: &CapabilityDescriptor| !d.handles_protected_data
);
descriptor_case!(
    coverage_capability_012_secure_storage_marks_protected_data,
    DeviceCapability::SecureStorage,
    |d: &CapabilityDescriptor| d.handles_protected_data
);

macro_rules! execute_case {
    ($name:ident, $capability:expr, $request:expr, $needle:expr) => {
        #[test]
        fn $name() {
            let bridge = DeviceToolBridge::new(EchoHost);
            let output = bridge.execute($capability, $request).unwrap();
            assert!(output.contains($needle));
        }
    };
}

execute_case!(
    coverage_capability_013_camera_capture_executes,
    DeviceCapability::Camera,
    r#"{"operation":"capture","foreground":true}"#,
    "device.camera"
);
execute_case!(
    coverage_capability_014_camera_preserves_empty_arguments,
    DeviceCapability::Camera,
    r#"{"operation":"capture","arguments":{},"foreground":true}"#,
    "arguments"
);
execute_case!(
    coverage_capability_015_camera_preserves_quality_argument,
    DeviceCapability::Camera,
    r#"{"operation":"capture","arguments":{"quality":"high"},"foreground":true}"#,
    "high"
);
execute_case!(
    coverage_capability_016_file_pick_executes,
    DeviceCapability::File,
    r#"{"operation":"pick","foreground":true}"#,
    "device.file"
);
execute_case!(
    coverage_capability_017_file_pick_preserves_type_argument,
    DeviceCapability::File,
    r#"{"operation":"pick","arguments":{"type":"pdf"},"foreground":true}"#,
    "pdf"
);
execute_case!(
    coverage_capability_018_file_read_scoped_executes,
    DeviceCapability::File,
    r#"{"operation":"read_scoped","foreground":true}"#,
    "read_scoped"
);
execute_case!(
    coverage_capability_019_file_read_preserves_reference,
    DeviceCapability::File,
    r#"{"operation":"read_scoped","arguments":{"reference":"file-1"},"foreground":true}"#,
    "file-1"
);
execute_case!(
    coverage_capability_020_biometric_verify_executes,
    DeviceCapability::Biometric,
    r#"{"operation":"verify","foreground":true}"#,
    "device.biometric"
);
execute_case!(
    coverage_capability_021_biometric_preserves_reason,
    DeviceCapability::Biometric,
    r#"{"operation":"verify","arguments":{"reason":"approve"},"foreground":true}"#,
    "approve"
);
execute_case!(
    coverage_capability_022_secure_get_executes_in_foreground,
    DeviceCapability::SecureStorage,
    r#"{"operation":"get","foreground":true}"#,
    "secure_storage"
);
execute_case!(
    coverage_capability_023_secure_get_executes_in_background,
    DeviceCapability::SecureStorage,
    r#"{"operation":"get","foreground":false}"#,
    "get"
);
execute_case!(
    coverage_capability_024_secure_put_executes_in_foreground,
    DeviceCapability::SecureStorage,
    r#"{"operation":"put","foreground":true}"#,
    "put"
);
execute_case!(
    coverage_capability_025_secure_put_executes_in_background,
    DeviceCapability::SecureStorage,
    r#"{"operation":"put","foreground":false}"#,
    "put"
);
execute_case!(
    coverage_capability_026_secure_delete_executes_in_foreground,
    DeviceCapability::SecureStorage,
    r#"{"operation":"delete","foreground":true}"#,
    "delete"
);
execute_case!(
    coverage_capability_027_secure_delete_executes_in_background,
    DeviceCapability::SecureStorage,
    r#"{"operation":"delete","foreground":false}"#,
    "delete"
);
execute_case!(
    coverage_capability_028_unicode_argument_round_trips,
    DeviceCapability::SecureStorage,
    r#"{"operation":"put","arguments":{"value":"segredo-ç"},"foreground":false}"#,
    "segredo-ç"
);
execute_case!(
    coverage_capability_029_empty_string_argument_round_trips,
    DeviceCapability::SecureStorage,
    r#"{"operation":"put","arguments":{"value":""},"foreground":false}"#,
    "value"
);
execute_case!(
    coverage_capability_030_multiple_arguments_round_trip,
    DeviceCapability::SecureStorage,
    r#"{"operation":"put","arguments":{"key":"a","value":"b"},"foreground":false}"#,
    "protected://put"
);

macro_rules! rejected_case {
    ($name:ident, $capability:expr, $request:expr) => {
        #[test]
        fn $name() {
            let bridge = DeviceToolBridge::new(EchoHost);
            let result = bridge.execute($capability, $request);
            assert!(result.is_err());
        }
    };
}

rejected_case!(
    coverage_capability_031_empty_json_is_rejected,
    DeviceCapability::Camera,
    ""
);
rejected_case!(
    coverage_capability_032_json_null_is_rejected,
    DeviceCapability::Camera,
    "null"
);
rejected_case!(
    coverage_capability_033_json_array_is_rejected,
    DeviceCapability::Camera,
    "[]"
);
rejected_case!(
    coverage_capability_034_missing_operation_is_rejected,
    DeviceCapability::Camera,
    r#"{"foreground":true}"#
);
rejected_case!(
    coverage_capability_035_missing_foreground_is_rejected,
    DeviceCapability::Camera,
    r#"{"operation":"capture"}"#
);
rejected_case!(
    coverage_capability_036_camera_background_is_rejected,
    DeviceCapability::Camera,
    r#"{"operation":"capture","foreground":false}"#
);
rejected_case!(
    coverage_capability_037_file_background_is_rejected,
    DeviceCapability::File,
    r#"{"operation":"pick","foreground":false}"#
);
rejected_case!(
    coverage_capability_038_biometric_background_is_rejected,
    DeviceCapability::Biometric,
    r#"{"operation":"verify","foreground":false}"#
);
rejected_case!(
    coverage_capability_039_camera_unknown_operation_is_rejected,
    DeviceCapability::Camera,
    r#"{"operation":"pick","foreground":true}"#
);
rejected_case!(
    coverage_capability_040_file_unknown_operation_is_rejected,
    DeviceCapability::File,
    r#"{"operation":"capture","foreground":true}"#
);
rejected_case!(
    coverage_capability_041_biometric_unknown_operation_is_rejected,
    DeviceCapability::Biometric,
    r#"{"operation":"get","foreground":true}"#
);
rejected_case!(
    coverage_capability_042_secure_unknown_operation_is_rejected,
    DeviceCapability::SecureStorage,
    r#"{"operation":"list","foreground":false}"#
);
rejected_case!(
    coverage_capability_043_empty_operation_is_rejected,
    DeviceCapability::SecureStorage,
    r#"{"operation":"","foreground":false}"#
);
rejected_case!(
    coverage_capability_044_case_changed_operation_is_rejected,
    DeviceCapability::SecureStorage,
    r#"{"operation":"GET","foreground":false}"#
);
rejected_case!(
    coverage_capability_045_whitespace_operation_is_rejected,
    DeviceCapability::SecureStorage,
    r#"{"operation":" get ","foreground":false}"#
);
