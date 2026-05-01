//! Transparent AES-256-GCM encryption for sensitive JSON values stored at rest.
//!
//! Usage:
//! ```rust,no_run
//! use orch8_types::encryption::FieldEncryptor;
//! use serde_json::json;
//!
//! let enc = FieldEncryptor::from_hex_key("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").unwrap();
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
///
/// Supports an optional old key for key rotation: encryption always uses the
/// primary key, while decryption tries the primary key first and falls back
/// to the old key when present.
#[derive(Clone)]
pub struct FieldEncryptor {
    cipher: Aes256Gcm,
    /// Previous key used during key rotation. Decryption falls back to this
    /// cipher when the primary key fails to decrypt a value.
    old_cipher: Option<Aes256Gcm>,
}

// `Aes256Gcm` does not implement Debug; we redact the cipher material
// and only expose whether a rotation key is present.
#[allow(clippy::missing_fields_in_debug)]
impl std::fmt::Debug for FieldEncryptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldEncryptor")
            .field("cipher", &"<redacted>")
            .field("has_old_cipher", &self.old_cipher.is_some())
            .finish()
    }
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
            old_cipher: None,
        })
    }

    /// Create an encryptor from raw 32 bytes.
    #[must_use]
    pub fn from_bytes(key: &[u8; 32]) -> Self {
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
        Self {
            cipher: Aes256Gcm::new(key),
            old_cipher: None,
        }
    }

    /// Return a copy of this encryptor with an additional old key for rotation.
    ///
    /// Encryption always uses the primary key. Decryption tries the primary
    /// key first; if that fails, it retries with the old key. This allows
    /// reading rows that were encrypted with the previous key.
    #[must_use = "returns a new FieldEncryptor with the old key; does not modify self"]
    pub fn with_old_key(mut self, hex_key: &str) -> Result<Self, EncryptionError> {
        let key_bytes = hex_decode(hex_key)?;
        if key_bytes.len() != 32 {
            return Err(EncryptionError::InvalidKeyLength(key_bytes.len()));
        }
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
        self.old_cipher = Some(Aes256Gcm::new(key));
        Ok(self)
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
    ///
    /// When an old key is configured, decryption tries the primary key first
    /// and falls back to the old key on failure.
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

        // Try primary key first.
        if let Ok(plaintext) = self.cipher.decrypt(nonce, ciphertext) {
            let value = serde_json::from_slice(&plaintext)?;
            return Ok(value);
        }

        // Fall back to old key if present.
        if let Some(ref old) = self.old_cipher {
            let plaintext = old
                .decrypt(nonce, ciphertext)
                .map_err(|_| EncryptionError::DecryptFailed)?;
            let value = serde_json::from_slice(&plaintext)?;
            return Ok(value);
        }

        Err(EncryptionError::DecryptFailed)
    }

    /// Returns true if the value appears to be encrypted.
    pub fn is_encrypted(value: &serde_json::Value) -> bool {
        matches!(value, serde_json::Value::String(s) if s.starts_with(ENC_PREFIX))
    }
}

/// Decode a hex string into bytes.
fn hex_decode(hex: &str) -> Result<Vec<u8>, EncryptionError> {
    if !hex.is_ascii() || !hex.len().is_multiple_of(2) {
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

    fn to_hex(key: &[u8]) -> String {
        use std::fmt::Write;
        key.iter().fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
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

    #[test]
    fn dual_key_decrypts_old_key_data() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] = 255;

        // Encrypt with key1.
        let enc1 = FieldEncryptor::from_bytes(&key1);
        let original = serde_json::json!({"rotated": true});
        let encrypted = enc1.encrypt_value(&original).unwrap();

        // Create encryptor with key2 as primary and key1 as old.
        let hex_old = to_hex(&key1);
        let enc2 = FieldEncryptor::from_bytes(&key2)
            .with_old_key(&hex_old)
            .unwrap();

        // Should decrypt via fallback to old key.
        let decrypted = enc2.decrypt_value(&encrypted).unwrap();
        assert_eq!(original, decrypted);
    }

    #[test]
    fn dual_key_encrypts_with_new_key() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] = 255;

        let hex_old = to_hex(&key1);
        let enc_dual = FieldEncryptor::from_bytes(&key2)
            .with_old_key(&hex_old)
            .unwrap();

        let original = serde_json::json!("new-data");
        let encrypted = enc_dual.encrypt_value(&original).unwrap();

        // New data should be decryptable with key2 alone.
        let enc2_only = FieldEncryptor::from_bytes(&key2);
        let decrypted = enc2_only.decrypt_value(&encrypted).unwrap();
        assert_eq!(original, decrypted);

        // New data should NOT be decryptable with key1 alone.
        let enc1_only = FieldEncryptor::from_bytes(&key1);
        assert!(enc1_only.decrypt_value(&encrypted).is_err());
    }

    #[test]
    fn is_encrypted_detects_prefix() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let encrypted = enc.encrypt_value(&serde_json::json!("hello")).unwrap();
        assert!(FieldEncryptor::is_encrypted(&encrypted));
        // Also matches a bare string with the prefix.
        assert!(FieldEncryptor::is_encrypted(&serde_json::Value::String(
            format!("{ENC_PREFIX}anything")
        )));
    }

    #[test]
    fn is_encrypted_false_for_plain_values() {
        assert!(!FieldEncryptor::is_encrypted(&serde_json::json!(
            "plain string"
        )));
        assert!(!FieldEncryptor::is_encrypted(&serde_json::json!(42)));
        assert!(!FieldEncryptor::is_encrypted(&serde_json::json!(null)));
        assert!(!FieldEncryptor::is_encrypted(
            &serde_json::json!({"k": "v"})
        ));
        assert!(!FieldEncryptor::is_encrypted(&serde_json::json!([1, 2, 3])));
        // A string that merely contains the prefix mid-way is not encrypted.
        assert!(!FieldEncryptor::is_encrypted(&serde_json::json!(
            "not enc:v1: prefixed"
        )));
    }

    #[test]
    fn from_hex_key_rejects_wrong_length() {
        // 62 hex chars -> 31 bytes, not 32.
        let short = "00".repeat(31);
        let err = FieldEncryptor::from_hex_key(&short).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidKeyLength(31)));

        // 66 hex chars -> 33 bytes.
        let long = "00".repeat(33);
        let err = FieldEncryptor::from_hex_key(&long).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidKeyLength(33)));
    }

    #[test]
    fn from_hex_key_rejects_invalid_hex_chars() {
        // 64 chars, but contains non-hex characters.
        let bad = "zz".repeat(32);
        let err = FieldEncryptor::from_hex_key(&bad).unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidHex));

        // Odd length triggers InvalidHex as well.
        let err = FieldEncryptor::from_hex_key("0").unwrap_err();
        assert!(matches!(err, EncryptionError::InvalidHex));
    }

    #[test]
    fn with_old_key_rejects_invalid_hex() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        // Non-hex characters.
        let res = enc.clone().with_old_key(&"zz".repeat(32));
        assert!(matches!(res, Err(EncryptionError::InvalidHex)));
        // Wrong length.
        let res = enc.with_old_key(&"00".repeat(16));
        assert!(matches!(res, Err(EncryptionError::InvalidKeyLength(16))));
    }

    #[test]
    fn dual_key_primary_takes_priority() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] = 255;

        let hex_old = to_hex(&key1);
        let enc_dual = FieldEncryptor::from_bytes(&key2)
            .with_old_key(&hex_old)
            .unwrap();

        // Encrypt with key2 (the primary in dual mode).
        let original = serde_json::json!("primary-key-data");
        let encrypted = enc_dual.encrypt_value(&original).unwrap();

        // Should decrypt with primary key (no fallback needed).
        let decrypted = enc_dual.decrypt_value(&encrypted).unwrap();
        assert_eq!(original, decrypted);
    }
}
