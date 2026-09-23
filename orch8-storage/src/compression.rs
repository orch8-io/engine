//! zstd compression for externalized state payloads.
//!
//! Level 3 is the default — best ratio/CPU tradeoff per upstream benchmarks,
//! roughly 100 MB/s per core for JSON-like payloads.
//!
//! Payloads below [`COMPRESSION_THRESHOLD_BYTES`] are left as raw JSON because
//! the zstd frame header (~12 bytes) dominates the savings on tiny inputs.
//!
//! When the `compression` feature is disabled (e.g. on iOS where zstd's C code
//! doesn't link), payloads are stored as raw JSON.

use orch8_types::error::StorageError;

/// Minimum raw JSON size (in bytes) before compression is applied. Below this
/// threshold the frame overhead typically exceeds any savings.
pub const COMPRESSION_THRESHOLD_BYTES: usize = 1024;

/// Maximum serialized JSON size accepted for an externalized payload.
pub const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// Maximum decompressed payload size. Prevents a small compressed payload
/// (a "compression bomb") from expanding to an arbitrary size and exhausting
/// memory during deserialization.
pub fn validate_payload_size(len: usize) -> Result<(), StorageError> {
    if len > MAX_PAYLOAD_BYTES {
        return Err(StorageError::Constraint(
            "externalized payload exceeds 16 MiB limit".into(),
        ));
    }
    Ok(())
}

/// Only feature-enabled builds may mark a row as zstd-compressed.
pub const fn should_compress(raw_len: usize) -> bool {
    cfg!(feature = "compression") && raw_len >= COMPRESSION_THRESHOLD_BYTES
}

#[cfg(feature = "compression")]
const ZSTD_LEVEL: i32 = 3;

/// Serialize `value` to JSON and compress with zstd level 3.
#[cfg(feature = "compression")]
pub fn compress(value: &serde_json::Value) -> Result<Vec<u8>, StorageError> {
    let json = serde_json::to_vec(value).map_err(StorageError::Serialization)?;
    validate_payload_size(json.len())?;
    zstd::encode_all(&json[..], ZSTD_LEVEL)
        .map_err(|e| StorageError::Query(format!("zstd encode: {e}")))
}

/// Decompress zstd bytes and parse the payload back into JSON.
#[cfg(feature = "compression")]
pub fn decompress(bytes: &[u8]) -> Result<serde_json::Value, StorageError> {
    use std::io::{Read, Write};

    let mut decoder = zstd::stream::read::Decoder::new(bytes)
        .map_err(|e| StorageError::Query(format!("zstd decode init: {e}")))?;
    let mut output = Vec::with_capacity(bytes.len().min(MAX_PAYLOAD_BYTES));
    let mut buf = [0u8; 8192];
    loop {
        let n = decoder
            .read(&mut buf)
            .map_err(|e| StorageError::Query(format!("zstd decode: {e}")))?;
        if n == 0 {
            break;
        }
        if output.len() + n > MAX_PAYLOAD_BYTES {
            return Err(StorageError::Query(
                "decompressed payload exceeds 16 MiB limit".into(),
            ));
        }
        output
            .write_all(&buf[..n])
            .map_err(|e| StorageError::Query(format!("zstd decode write: {e}")))?;
    }
    serde_json::from_slice(&output).map_err(StorageError::Serialization)
}

#[cfg(not(feature = "compression"))]
pub fn compress(value: &serde_json::Value) -> Result<Vec<u8>, StorageError> {
    let json = serde_json::to_vec(value).map_err(StorageError::Serialization)?;
    validate_payload_size(json.len())?;
    Ok(json)
}

#[cfg(not(feature = "compression"))]
pub fn decompress(bytes: &[u8]) -> Result<serde_json::Value, StorageError> {
    // Older feature-disabled builds mislabeled raw JSON as zstd. Keep those
    // rows readable, but report an actual zstd frame as unsupported.
    serde_json::from_slice(bytes).map_err(|_| {
        StorageError::Unsupported("zstd-compressed payload requires the compression feature".into())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn roundtrip_preserves_value() {
        let v = json!({
            "items": (0..50).map(|i| format!("item-{i}")).collect::<Vec<_>>()
        });
        let compressed = compress(&v).unwrap();
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(decompressed, v);
    }

    #[test]
    fn writer_limit_matches_reader_limit() {
        assert!(validate_payload_size(MAX_PAYLOAD_BYTES).is_ok());
        assert!(matches!(
            validate_payload_size(MAX_PAYLOAD_BYTES + 1),
            Err(StorageError::Constraint(_))
        ));
        let oversized = json!({"blob": "x".repeat(MAX_PAYLOAD_BYTES)});
        assert!(matches!(
            compress(&oversized),
            Err(StorageError::Constraint(_))
        ));
    }

    #[test]
    fn compression_marker_depends_on_build_feature() {
        assert!(!should_compress(COMPRESSION_THRESHOLD_BYTES - 1));
        assert_eq!(
            should_compress(COMPRESSION_THRESHOLD_BYTES),
            cfg!(feature = "compression")
        );
    }

    #[cfg(not(feature = "compression"))]
    #[test]
    fn feature_disabled_reader_supports_legacy_mislabeled_raw_json() {
        let value = json!({"legacy": true});
        let raw = serde_json::to_vec(&value).unwrap();
        assert_eq!(decompress(&raw).unwrap(), value);
        assert!(matches!(
            decompress(&[0x28, 0xb5, 0x2f, 0xfd]),
            Err(StorageError::Unsupported(_))
        ));
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compressed_smaller_than_raw_on_repetitive_payload() {
        let v = json!({"blob": "x".repeat(10_000)});
        let raw = serde_json::to_vec(&v).unwrap();
        let compressed = compress(&v).unwrap();
        assert!(
            compressed.len() < raw.len() / 4,
            "expected >4x compression on repetitive payload, got {}B -> {}B",
            raw.len(),
            compressed.len()
        );
    }

    #[test]
    fn decompress_rejects_invalid_bytes() {
        // When compression is enabled, invalid zstd bytes are rejected.
        // When disabled, invalid JSON bytes are rejected.
        assert!(decompress(&[0xff, 0xff, 0xff]).is_err());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn decompress_rejects_compression_bomb() {
        // A tiny zstd payload that decompresses to a huge repeated string.
        let huge = "x".repeat(MAX_PAYLOAD_BYTES + 1);
        let value = json!({ "blob": huge });
        let raw = serde_json::to_vec(&value).unwrap();
        let compressed = zstd::encode_all(raw.as_slice(), ZSTD_LEVEL).unwrap();
        assert!(
            compressed.len() < huge.len() / 100,
            "expected compression bomb to be much smaller than raw"
        );
        let err = decompress(&compressed).expect_err("bomb must be rejected");
        assert!(
            err.to_string().contains("exceeds 16 MiB limit"),
            "error should mention size limit: {err}"
        );
    }

    #[test]
    fn roundtrip_preserves_unicode_and_nested_structures() {
        let v = json!({
            "pt-BR": "olá mundo",
            "ru": "привет",
            "emoji": "🧪",
            "nested": {"a": [1, 2, {"b": null}]},
        });
        let compressed = compress(&v).unwrap();
        assert_eq!(decompress(&compressed).unwrap(), v);
    }
}
