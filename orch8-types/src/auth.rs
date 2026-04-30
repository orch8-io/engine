use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Constant-time secret comparison via SHA-256 length normalisation.
///
/// Both inputs are hashed to a fixed 32-byte digest before comparison,
/// preventing timing side-channels from differing input lengths.
/// SHA-256 here is purely a length-normaliser — collision resistance is
/// not load-bearing.
pub fn verify_secret_constant_time(provided: &str, expected: &str) -> bool {
    let provided_digest = Sha256::digest(provided.as_bytes());
    let expected_digest = Sha256::digest(expected.as_bytes());
    bool::from(provided_digest.ct_eq(expected_digest.as_slice()))
}

/// Precompute a SHA-256 digest of a secret for repeated verification.
pub fn precompute_secret_digest(secret: &str) -> [u8; 32] {
    Sha256::digest(secret.as_bytes()).into()
}

/// Verify a provided secret against a precomputed SHA-256 digest.
pub fn verify_secret_against_digest(provided: &str, expected_digest: &[u8; 32]) -> bool {
    let provided_digest = Sha256::digest(provided.as_bytes());
    bool::from(provided_digest.ct_eq(expected_digest.as_slice()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_secrets_verify() {
        assert!(verify_secret_constant_time("my-secret", "my-secret"));
    }

    #[test]
    fn mismatched_secrets_reject() {
        assert!(!verify_secret_constant_time("my-secret", "wrong"));
    }

    #[test]
    fn different_length_secrets_reject() {
        assert!(!verify_secret_constant_time(
            "short",
            "a-much-longer-secret-key"
        ));
    }

    #[test]
    fn empty_provided_rejects() {
        assert!(!verify_secret_constant_time("", "secret"));
    }

    #[test]
    fn precomputed_digest_matches() {
        let digest = precompute_secret_digest("s3cret");
        assert!(verify_secret_against_digest("s3cret", &digest));
        assert!(!verify_secret_against_digest("wrong", &digest));
    }
}
