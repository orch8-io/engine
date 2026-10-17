//! Strict decoding for API *input*, separate from the persisted shape.
//!
//! Persisted types tolerate unknown fields so a rolled-back binary can still
//! read rows a newer binary wrote. Request bodies, on the other hand, should
//! reject a typo (`percentag`) instead of silently dropping it. Wrap the body
//! type in [`Strict`] (or use [`deserialize`] as a field's
//! `deserialize_with`) to get that strictness at the API boundary only.

use serde::de::{Deserialize, Deserializer, Error as _};

/// Deserialize `T`, failing on any field `T` does not know (at any depth).
///
/// # Errors
/// The inner deserializer's error, or `unknown field <path>` for the first
/// unrecognised field.
pub fn deserialize<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    let mut unknown: Option<String> = None;
    let value = serde_ignored::deserialize(deserializer, |path| {
        if unknown.is_none() {
            unknown = Some(path.to_string());
        }
    })?;
    match unknown {
        Some(path) => Err(D::Error::custom(format!("unknown field `{path}`"))),
        None => Ok(value),
    }
}

/// A `T` decoded with [`deserialize`]: unknown fields are an error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Strict<T>(pub T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Strict<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserialize(deserializer).map(Strict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Deserialize)]
    struct Limits {
        max: Option<u32>,
    }

    #[test]
    fn strict_rejects_an_unknown_field() {
        let err = serde_json::from_str::<Strict<Limits>>(r#"{"max":1,"maxx":2}"#).unwrap_err();

        assert!(err.to_string().contains("unknown field `maxx`"), "{err}");
    }

    #[test]
    fn strict_accepts_known_fields() {
        let Strict(limits) = serde_json::from_str::<Strict<Limits>>(r#"{"max":1}"#).unwrap();

        assert_eq!(limits, Limits { max: Some(1) });
    }
}
