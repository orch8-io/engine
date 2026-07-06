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

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::{AeadCore, Aes256Gcm, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use zeroize::Zeroize;

/// NIST SP 800-38D's cautionary limit on AES-GCM invocations with a single
/// key under random 96-bit nonces (~2^32, to keep nonce-collision
/// probability negligible). This engine encrypts on nearly every scheduler
/// write, so the bound is reachable in days-to-weeks of sustained load if
/// the key is never rotated; crossing it logs a warning once per process
/// (see [`FieldEncryptor::encrypt_count`]).
const NONCE_BUDGET_WARN_THRESHOLD: u64 = 1 << 32;

/// Encrypts and decrypts `serde_json::Value` fields using AES-256-GCM.
///
/// Encrypted values are stored as JSON strings with the prefix `"enc:v1:"`,
/// followed by base64-encoded `nonce || ciphertext`. [`Self::encrypt_value_with_aad`]
/// produces the newer `"enc:v2:"` format, which additionally binds the
/// ciphertext to caller-supplied associated data (e.g. `instance_id`) so a
/// ciphertext copied to a different row/tenant fails to decrypt there instead
/// of silently succeeding.
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
    /// Count of AEAD seal operations (`encrypt_value`/`encrypt_bytes`/
    /// `encrypt_value_with_aad`) performed with the *primary* key, shared via
    /// `Arc` across clones (e.g. a per-request handle) so the budget tracker
    /// isn't reset by cloning.
    encrypt_count: Arc<AtomicU64>,
    /// Set once [`NONCE_BUDGET_WARN_THRESHOLD`] is crossed, so the warning
    /// logs once per process instead of on every subsequent call.
    budget_warned: Arc<AtomicBool>,
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
const ENC_PREFIX_V2: &str = "enc:v2:";

impl FieldEncryptor {
    /// Create an encryptor from a 32-byte (64 hex char) key.
    #[must_use = "returns a new FieldEncryptor; does not modify state"]
    pub fn from_hex_key(hex_key: &str) -> Result<Self, EncryptionError> {
        // L-9: `hex_decode` returns an owned `Vec<u8>` holding the raw key
        // material. `aes_gcm::Key::from_slice` copies it into the cipher's
        // own storage, so once that copy exists this buffer must be
        // explicitly zeroized rather than left for a plain `Drop` (which
        // just deallocates without clearing) -- otherwise the key can linger
        // readable in freed heap memory.
        let mut key_bytes = hex_decode(hex_key)?;
        let len = key_bytes.len();
        if len != 32 {
            key_bytes.zeroize();
            return Err(EncryptionError::InvalidKeyLength(len));
        }
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
        let cipher = Aes256Gcm::new(key);
        key_bytes.zeroize();
        Ok(Self {
            cipher,
            old_cipher: None,
            encrypt_count: Arc::new(AtomicU64::new(0)),
            budget_warned: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Create an encryptor from raw 32 bytes.
    #[must_use]
    pub fn from_bytes(key: &[u8; 32]) -> Self {
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(key);
        Self {
            cipher: Aes256Gcm::new(key),
            old_cipher: None,
            encrypt_count: Arc::new(AtomicU64::new(0)),
            budget_warned: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Number of AEAD seal operations performed with the primary key so far
    /// (shared across all clones of this encryptor). Exposed so a caller can
    /// surface it as a metric; the budget warning is also logged internally
    /// once the NIST SP 800-38D nonce budget (~2^32 invocations) is crossed.
    #[must_use]
    pub fn encrypt_count(&self) -> u64 {
        self.encrypt_count.load(Ordering::Relaxed)
    }

    /// Increment the encrypt counter and warn once if the nonce budget has
    /// been exceeded. Called from every primary-key AEAD seal operation.
    fn record_encryption(&self) {
        let count = self.encrypt_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= NONCE_BUDGET_WARN_THRESHOLD
            && self
                .budget_warned
                .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(
                encrypt_count = count,
                threshold = NONCE_BUDGET_WARN_THRESHOLD,
                "FieldEncryptor: primary key has exceeded the recommended random-nonce AES-GCM \
                 invocation budget (NIST SP 800-38D); rotate the encryption key"
            );
        }
    }

    /// Return a copy of this encryptor with an additional old key for rotation.
    ///
    /// Encryption always uses the primary key. Decryption tries the primary
    /// key first; if that fails, it retries with the old key. This allows
    /// reading rows that were encrypted with the previous key.
    #[must_use = "returns a new FieldEncryptor with the old key; does not modify self"]
    pub fn with_old_key(mut self, hex_key: &str) -> Result<Self, EncryptionError> {
        // L-9: see `from_hex_key` -- zeroize the decoded key bytes once
        // they've been copied into the cipher, rather than leaving them for
        // a plain `Drop`.
        let mut key_bytes = hex_decode(hex_key)?;
        let len = key_bytes.len();
        if len != 32 {
            key_bytes.zeroize();
            return Err(EncryptionError::InvalidKeyLength(len));
        }
        let key = aes_gcm::Key::<Aes256Gcm>::from_slice(&key_bytes);
        self.old_cipher = Some(Aes256Gcm::new(key));
        key_bytes.zeroize();
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
        self.record_encryption();

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
    ///
    /// Rejects `"enc:v2:"` payloads (AAD-bound -- see
    /// [`Self::encrypt_value_with_aad`]) since decrypting those without their
    /// associated data would either fail every time or, worse, invite a
    /// caller to pass the wrong AAD. Use [`Self::decrypt_value_with_aad`] for
    /// any field that might be v2.
    pub fn decrypt_value(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, EncryptionError> {
        let serde_json::Value::String(s) = value else {
            return Ok(value.clone());
        };
        if s.starts_with(ENC_PREFIX_V2) {
            return Err(EncryptionError::DecryptFailed);
        }
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

    /// Decrypt a value that is *required* to always be encrypted at rest
    /// (M-2) -- e.g. a credential secret or refresh token, as opposed to
    /// opportunistically-encrypted fields like `context.data` that must stay
    /// readable if they predate encryption being enabled.
    ///
    /// Unlike [`Self::decrypt_value`], a value with no `"enc:v1:"`/`"enc:v2:"`
    /// prefix is treated as an error rather than passed through unchanged.
    /// For an always-encrypted field, unprefixed data at rest means the write
    /// path failed to encrypt it -- silently accepting it as "already
    /// plaintext" would mask that bug indefinitely instead of surfacing it.
    pub fn decrypt_value_strict(
        &self,
        value: &serde_json::Value,
    ) -> Result<serde_json::Value, EncryptionError> {
        let serde_json::Value::String(s) = value else {
            return Err(EncryptionError::NotEncrypted);
        };
        if !s.starts_with(ENC_PREFIX) && !s.starts_with(ENC_PREFIX_V2) {
            return Err(EncryptionError::NotEncrypted);
        }
        self.decrypt_value(value)
    }

    /// Encrypt a JSON value with associated data binding the ciphertext to
    /// caller-supplied context (e.g. `instance_id` bytes), producing an
    /// `"enc:v2:"` payload. The same `aad` must be supplied to
    /// [`Self::decrypt_value_with_aad`] -- a ciphertext copied to a different
    /// row/tenant (different AAD) fails to decrypt there instead of silently
    /// succeeding, which plain `"enc:v1:"` payloads are vulnerable to.
    pub fn encrypt_value_with_aad(
        &self,
        value: &serde_json::Value,
        aad: &[u8],
    ) -> Result<serde_json::Value, EncryptionError> {
        let plaintext = serde_json::to_vec(value)?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext,
                    aad,
                },
            )
            .map_err(|_| EncryptionError::EncryptFailed)?;
        self.record_encryption();

        let mut payload = Vec::with_capacity(12 + ciphertext.len());
        payload.extend_from_slice(&nonce);
        payload.extend_from_slice(&ciphertext);

        let encoded = format!("{ENC_PREFIX_V2}{}", B64.encode(&payload));
        Ok(serde_json::Value::String(encoded))
    }

    /// Decrypt a value produced by [`Self::encrypt_value_with_aad`] (or, for
    /// backward compatibility, a legacy `"enc:v1:"` value with no AAD).
    /// `aad` must exactly match what was passed to `encrypt_value_with_aad`.
    pub fn decrypt_value_with_aad(
        &self,
        value: &serde_json::Value,
        aad: &[u8],
    ) -> Result<serde_json::Value, EncryptionError> {
        let serde_json::Value::String(s) = value else {
            return Ok(value.clone());
        };
        let Some(encoded) = s.strip_prefix(ENC_PREFIX_V2) else {
            // Not v2 -- fall back to the plain (no-AAD) path, which also
            // handles the "not encrypted at all" passthrough case.
            return self.decrypt_value(value);
        };

        let payload = B64.decode(encoded)?;
        if payload.len() < 12 {
            return Err(EncryptionError::InvalidCiphertext);
        }
        let (nonce_bytes, ciphertext) = payload.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        if let Ok(plaintext) = self.cipher.decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        ) {
            return Ok(serde_json::from_slice(&plaintext)?);
        }
        if let Some(ref old) = self.old_cipher {
            let plaintext = old
                .decrypt(
                    nonce,
                    Payload {
                        msg: ciphertext,
                        aad,
                    },
                )
                .map_err(|_| EncryptionError::DecryptFailed)?;
            return Ok(serde_json::from_slice(&plaintext)?);
        }
        Err(EncryptionError::DecryptFailed)
    }

    /// Returns true if the value appears to be encrypted (either the legacy
    /// `"enc:v1:"` or the AAD-bound `"enc:v2:"` format).
    pub fn is_encrypted(value: &serde_json::Value) -> bool {
        matches!(value, serde_json::Value::String(s) if s.starts_with(ENC_PREFIX) || s.starts_with(ENC_PREFIX_V2))
    }

    /// Encrypt a raw byte buffer, returning `nonce(12) || ciphertext` (no
    /// base64/JSON framing — used for binary artifact blobs, not JSON fields).
    ///
    /// # Errors
    /// Returns [`EncryptionError::EncryptFailed`] if the AEAD seal fails.
    pub fn encrypt_bytes(&self, plaintext: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| EncryptionError::EncryptFailed)?;
        self.record_encryption();
        let mut out = Vec::with_capacity(12 + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a buffer produced by [`Self::encrypt_bytes`]. Tries the primary
    /// key, then the old key (if configured) for key rotation.
    ///
    /// # Errors
    /// Returns [`EncryptionError::InvalidCiphertext`] if too short, or
    /// [`EncryptionError::DecryptFailed`] if no key can open it.
    pub fn decrypt_bytes(&self, sealed: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if sealed.len() < 12 {
            return Err(EncryptionError::InvalidCiphertext);
        }
        let (nonce_bytes, ciphertext) = sealed.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        if let Ok(plain) = self.cipher.decrypt(nonce, ciphertext) {
            return Ok(plain);
        }
        if let Some(ref old) = self.old_cipher {
            return old
                .decrypt(nonce, ciphertext)
                .map_err(|_| EncryptionError::DecryptFailed);
        }
        Err(EncryptionError::DecryptFailed)
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
    #[error("expected an encrypted value but found plaintext")]
    NotEncrypted,
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
    fn roundtrip_encrypt_decrypt_bytes() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let plain = b"\x89PNG\r\n\x1a\nbinary artifact bytes \x00\xff";
        let sealed = enc.encrypt_bytes(plain).unwrap();
        // Ciphertext differs from plaintext and carries the 12-byte nonce.
        assert_ne!(&sealed[..], &plain[..]);
        assert!(sealed.len() > plain.len());
        assert_eq!(enc.decrypt_bytes(&sealed).unwrap(), plain);
    }

    #[test]
    fn decrypt_bytes_rejects_truncated() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        assert!(enc.decrypt_bytes(b"short").is_err());
    }

    #[test]
    fn decrypt_bytes_wrong_key_fails() {
        let enc1 = FieldEncryptor::from_bytes(&test_key());
        let mut other = test_key();
        other[0] ^= 0xff;
        let enc2 = FieldEncryptor::from_bytes(&other);
        let sealed = enc1.encrypt_bytes(b"secret").unwrap();
        assert!(enc2.decrypt_bytes(&sealed).is_err());
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

    /// L-9: `from_hex_key`/`with_old_key` zeroize the decoded key bytes
    /// after copying them into the cipher, rather than leaving them for a
    /// plain `Drop` (which deallocates without clearing). That memory-level
    /// guarantee isn't observable from safe Rust (and this workspace denies
    /// `unsafe_code`, including in tests), so this pins the behavior one
    /// level up: `Vec<u8>::zeroize()` (the exact call both sites make)
    /// clears the vec's logical contents rather than being a no-op wrapper.
    #[test]
    fn vec_zeroize_clears_logical_contents() {
        let mut buf: Vec<u8> = vec![0xAA; 32];
        buf.zeroize();
        assert!(
            buf.iter().all(|&b| b == 0) || buf.is_empty(),
            "zeroize must not leave the original key bytes readable"
        );
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

    // ========================================================================
    // Regression coverage for the 2026-07 deep storage review's finding #11:
    // AAD ciphertext binding + nonce-usage budget tracking.
    // ========================================================================

    #[test]
    fn aad_roundtrip_with_matching_aad() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!({"secret": "data"});
        let encrypted = enc
            .encrypt_value_with_aad(&original, b"instance-1")
            .unwrap();
        assert!(FieldEncryptor::is_encrypted(&encrypted));
        assert!(matches!(encrypted, serde_json::Value::String(ref s) if s.starts_with("enc:v2:")));

        let decrypted = enc
            .decrypt_value_with_aad(&encrypted, b"instance-1")
            .unwrap();
        assert_eq!(original, decrypted);
    }

    /// The core fix for #11: a ciphertext produced with one AAD (e.g. one
    /// instance's ID) must fail to decrypt under a *different* AAD (e.g. a
    /// different instance/row it was copied to) -- this is exactly what
    /// prevents an attacker with DB write access from transplanting an
    /// `enc:v1:` blob between rows/tenants.
    #[test]
    fn aad_mismatch_fails_to_decrypt() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!({"secret": "data"});
        let encrypted = enc
            .encrypt_value_with_aad(&original, b"tenant-a:instance-1")
            .unwrap();

        // Same ciphertext, wrong AAD (as if copied to a different row).
        let result = enc.decrypt_value_with_aad(&encrypted, b"tenant-b:instance-2");
        assert!(
            result.is_err(),
            "ciphertext bound to one AAD must not decrypt under a different AAD"
        );
    }

    #[test]
    fn aad_empty_aad_is_valid_and_distinct_from_no_aad_v1() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!("data");
        let encrypted = enc.encrypt_value_with_aad(&original, b"").unwrap();
        let decrypted = enc.decrypt_value_with_aad(&encrypted, b"").unwrap();
        assert_eq!(original, decrypted);
    }

    /// `decrypt_value_with_aad` must transparently handle legacy `enc:v1:`
    /// payloads (written before AAD binding existed) so a rollout doesn't
    /// require re-encrypting every existing row.
    #[test]
    fn aad_decrypt_falls_back_to_legacy_v1_payloads() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!({"legacy": true});
        let encrypted_v1 = enc.encrypt_value(&original).unwrap();
        assert!(
            matches!(encrypted_v1, serde_json::Value::String(ref s) if s.starts_with("enc:v1:"))
        );

        // Any AAD value is accepted for v1 payloads since they carry none.
        let decrypted = enc
            .decrypt_value_with_aad(&encrypted_v1, b"whatever-context")
            .unwrap();
        assert_eq!(original, decrypted);
    }

    /// The plain (non-AAD) `decrypt_value` must refuse v2 payloads outright
    /// rather than silently mis-decrypting or misinterpreting them --
    /// callers that might see v2 must use `decrypt_value_with_aad`.
    #[test]
    fn decrypt_value_rejects_v2_payloads() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let encrypted = enc
            .encrypt_value_with_aad(&serde_json::json!("x"), b"aad")
            .unwrap();
        assert!(enc.decrypt_value(&encrypted).is_err());
    }

    /// M-2: `decrypt_value_strict` must round-trip a genuinely encrypted
    /// value exactly like `decrypt_value`.
    #[test]
    fn decrypt_value_strict_decrypts_valid_ciphertext() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let original = serde_json::json!({"secret": "sauce"});
        let encrypted = enc.encrypt_value(&original).unwrap();
        let decrypted = enc.decrypt_value_strict(&encrypted).unwrap();
        assert_eq!(original, decrypted);
    }

    /// M-2: unlike `decrypt_value`, an unprefixed plaintext string must be
    /// rejected -- an always-encrypted field finding plaintext at rest means
    /// the write path failed to encrypt it, and that must surface as an
    /// error rather than be silently accepted as "already decrypted".
    #[test]
    fn decrypt_value_strict_rejects_plaintext_string() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let plain = serde_json::json!("not-encrypted-at-all");
        assert!(matches!(
            enc.decrypt_value_strict(&plain),
            Err(EncryptionError::NotEncrypted)
        ));
    }

    #[test]
    fn decrypt_value_strict_rejects_non_string_value() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let obj = serde_json::json!({"already": "an object"});
        assert!(matches!(
            enc.decrypt_value_strict(&obj),
            Err(EncryptionError::NotEncrypted)
        ));
    }

    /// A v2 (AAD-bound) payload is still "encrypted", just via the wrong
    /// entry point -- `decrypt_value_strict` should surface the same
    /// `DecryptFailed` that plain `decrypt_value` gives for v2, not
    /// `NotEncrypted` (which would incorrectly suggest the data was never
    /// encrypted at all).
    #[test]
    fn decrypt_value_strict_rejects_v2_payload_as_decrypt_failed_not_not_encrypted() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let encrypted = enc
            .encrypt_value_with_aad(&serde_json::json!("x"), b"aad")
            .unwrap();
        assert!(matches!(
            enc.decrypt_value_strict(&encrypted),
            Err(EncryptionError::DecryptFailed)
        ));
    }

    #[test]
    fn aad_dual_key_rotation_works_with_aad() {
        let key1 = test_key();
        let mut key2 = test_key();
        key2[0] = 255;

        let enc1 = FieldEncryptor::from_bytes(&key1);
        let original = serde_json::json!({"rotated": true});
        let encrypted = enc1.encrypt_value_with_aad(&original, b"ctx").unwrap();

        let hex_old = to_hex(&key1);
        let enc2 = FieldEncryptor::from_bytes(&key2)
            .with_old_key(&hex_old)
            .unwrap();

        let decrypted = enc2.decrypt_value_with_aad(&encrypted, b"ctx").unwrap();
        assert_eq!(original, decrypted);
    }

    #[test]
    fn encrypt_count_tracks_all_seal_operations() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        assert_eq!(enc.encrypt_count(), 0);
        enc.encrypt_value(&serde_json::json!("a")).unwrap();
        assert_eq!(enc.encrypt_count(), 1);
        enc.encrypt_value_with_aad(&serde_json::json!("b"), b"aad")
            .unwrap();
        assert_eq!(enc.encrypt_count(), 2);
        enc.encrypt_bytes(b"raw bytes").unwrap();
        assert_eq!(enc.encrypt_count(), 3);
    }

    #[test]
    fn encrypt_count_is_shared_across_clones() {
        let enc = FieldEncryptor::from_bytes(&test_key());
        let cloned = enc.clone();
        enc.encrypt_value(&serde_json::json!("a")).unwrap();
        cloned.encrypt_value(&serde_json::json!("b")).unwrap();
        // Both handles observe the combined count -- the tracker isn't reset
        // or duplicated by cloning (e.g. constructing a per-request handle).
        assert_eq!(enc.encrypt_count(), 2);
        assert_eq!(cloned.encrypt_count(), 2);
    }
}
