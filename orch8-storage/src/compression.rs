//! zstd compression for externalized state payloads.
//!
//! Level 3 is the default — best ratio/CPU tradeoff per upstream benchmarks,
//! roughly 100 MB/s per core for JSON-like payloads.
//!
//! Payloads below [`COMPRESSION_THRESHOLD_BYTES`] are left as raw JSON because
//! the zstd frame header (~12 bytes) dominates the savings on tiny inputs.

use orch8_types::error::StorageError;

const ZSTD_LEVEL: i32 = 3;

/// Minimum raw JSON size (in bytes) before compression is applied. Below this
/// threshold the frame overhead typically exceeds any savings.
pub const COMPRESSION_THRESHOLD_BYTES: usize = 1024;

/// Serialize `value` to JSON and compress with zstd level 3.
pub fn compress(value: &serde_json::Value) -> Result<Vec<u8>, StorageError> {
    let json = serde_json::to_vec(value).map_err(StorageError::Serialization)?;
    zstd::encode_all(&json[..], ZSTD_LEVEL)
        .map_err(|e| StorageError::Query(format!("zstd encode: {e}")))
}

/// Decompress zstd bytes and parse the payload back into JSON.
pub fn decompress(bytes: &[u8]) -> Result<serde_json::Value, StorageError> {
    let json =
        zstd::decode_all(bytes).map_err(|e| StorageError::Query(format!("zstd decode: {e}")))?;
    serde_json::from_slice(&json).map_err(StorageError::Serialization)
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
        assert!(decompress(&[0xff, 0xff, 0xff]).is_err());
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
