//! Transparent AES-256-GCM encryption for sensitive JSON values stored at rest.
//!
//! Usage:
//! ```ignore
//! let enc = FieldEncryptor::from_hex_key("...64-char hex key...").unwrap();
//! let encrypted = enc.encrypt_value(&json!({"secret": "data"})).unwrap();
//! let decrypted = enc.decrypt_value(&encrypted).unwrap();
//! ```

use aes_gcm::aead::{Aead, KeyInit, OsRng};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;

/// Encrypts and decrypts `serde_json::Value` fields using AES-256-GCM.
///
/// Encrypted values are stored as JSON strings with the prefix `"enc:v1:"`,
/// followed by base64-encoded `nonce || ciphertext`.
#[derive(Clone)]
pub struct FieldEncryptor {
    cipher: Aes256Gcm,
}

const ENC_PREFIX: &str = "enc:v1:";

impl FieldEncryptor {
    /// Create an encryptor from a 32-byte (64 hex char) key.
    #[must_use = "returns a new FieldEncryptor; does not modify state"]
    pub fn from_hex_key(hex_key: &str) -> Result<Self, EncryptionError> {
        let key_bytes = hex_decode(hex_key)?;
        if key_bytes.len() != 32 {
            return Err(EncryptionError::InvalidKeyLength(key_bytes.len()));
        }
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
        Ok(Self {
            cipher: Aes256Gcm::new(key),
        })
    }

    /// Create an encryptor from raw 32 bytes.
    pub fn from_bytes(key: &[u8; 32]) -> Self {
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
        Self {
            cipher: Aes256Gcm::new(key),
        }
    }

    /// Encrypt a JSON value into an opaque string value: `"enc:v1:<base64(nonce||ciphertext)>"`.
    pub fn encrypt_value(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, EncryptionError> {
        let plaintext = serde_json::to_vec(value)?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext.as_slice())
            .map_err(|_| EncryptionError::EncryptFailed)?;

        let mut payload = Vec::with_capacity(12 + ciphertext.len());
        payload.extend_from_slice(&nonce);
        payload.extend_from_slice(&ciphertext);

        let encoded = format!("{ENC_PREFIX}{}", B64.encode(&payload));
        Ok(serde_json::Value::String(encoded))
    }

    /// Decrypt a value previously encrypted by `encrypt_value`.
    /// If the value is not encrypted (no `enc:v1:` prefix), returns it unchanged.
    pub fn decrypt_value(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, EncryptionError> {
        let serde_json::Value::String(s) = value else {
            return Ok(value.clone());
        };
        let Some(encoded) = s.strip_prefix(ENC_PREFIX) else {
            return Ok(value.clone());
        };

        let payload = B64.decode(encoded)?;
        if payload.len() < 12 {
            return Err(EncryptionError::InvalidCiphertext);
        }
        let (nonce_bytes, ciphertext) = payload.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptFailed)?;

        let value = serde_json::from_slice(&plaintext)?;
        Ok(value)
    }

    /// Returns true if the value appears to be encrypted.
    pub fn is_encrypted(value: &serde_json::Value) -> bool {
        matches!(value, serde_json::Value::String(s) if s.starts_with(ENC_PREFIX))
    }
}

/// Decode a hex string into bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, EncryptionError> {
    if !hex.len().is_multiple_of(2) {
        return Err(EncryptionError::InvalidHex);
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).map_err(|_| EncryptionError::InvalidHex))
        .collect()
}

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("invalid hex string")]
    InvalidHex,
    #[error("encryption failed")]
    EncryptFailed,
    #[error("decryption failed — wrong key or corrupted data")]
    DecryptFailed,
    #[error("invalid ciphertext format")]
    InvalidCiphertext,
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("base64 decode error: {0}")]
    Base64(#[from] base64::DecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(clippy::cast_possible_truncation)]
    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = i as u8;
        }
        key
    }

    #[test]
    fn roundtrip_encrypt_decrypt() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!({"secret": "data", "nested": {"value": 42}});
        let encrypted = enc.encrypt_value(&original).unwrap();

        // Encrypted value is a string starting with prefix.
        assert!(FieldEncryptor::is_encrypted(&encrypted));
        assert!(matches!(encrypted, serde_json::Value::String(ref s) if s.starts_with(ENC_PREFIX)));

        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(original, decrypted);
    }

    #[test]
    fn non_encrypted_passthrough() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let plain = serde_json::json!({"not": "encrypted"});
        let result = enc.decrypt_value(&plain).unwrap();
        assert_eq!(plain, result);
    }

    #[test]
    fn wrong_key_fails() {
        let enc1 = FieldEncryptor::from_bytes(&test_key());
        let mut other_key = test_key();
        other_key[0] = 255;
        let enc2 = FieldEncryptor::from_bytes(&other_key);

        let encrypted = enc1.encrypt_value(&serde_json::json!("secret")).unwrap();
        let result = enc2.decrypt_value(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn from_hex_key() {
        let hex = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let enc = FieldEncryptor::from_hex_key(hex).unwrap();
        let original = serde_json::json!("hello");
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value(&encrypted).unwrap();
        assert_eq!(original, decrypted);
    }

    #[test]
    fn invalid_key_length() {
        let result = FieldEncryptor::from_hex_key("0011");
        assert!(result.is_err());
    }

    #[test]
    fn each_encryption_unique() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let val = serde_json::json!("same");
        let e1 = enc.encrypt_value(&val).unwrap();
        let e2 = enc.encrypt_value(&val).unwrap();
        // Different nonces produce different ciphertexts.
        assert_ne!(e1, e2);
        // Both decrypt to the same value.
        assert_eq!(enc.decrypt_value(&e1).unwrap(), val);
        assert_eq!(enc.decrypt_value(&e2).unwrap(), val);
    }
}
