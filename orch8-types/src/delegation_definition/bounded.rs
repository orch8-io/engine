//! Streaming resource limits: reject an excess element before decoding it.
use super::{
    DefinitionBinding, DefinitionGraphLimits, DefinitionManifest, FrozenDelegationDefinition,
};
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor},
};
use std::{fmt, marker::PhantomData};

const LIMIT_ERROR: &str = "collection exceeds supported limit";

pub(super) struct VecSeed<S> {
    maximum: usize,
    element: S,
}
impl<S> VecSeed<S> {
    pub(super) fn new(maximum: usize, element: S) -> Self {
        Self { maximum, element }
    }
}
impl<'de, S: DeserializeSeed<'de> + Clone> DeserializeSeed<'de> for VecSeed<S> {
    type Value = Vec<S::Value>;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_seq(self)
    }
}
impl<'de, S: DeserializeSeed<'de> + Clone> Visitor<'de> for VecSeed<S> {
    type Value = Vec<S::Value>;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "an array of at most {} elements", self.maximum)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        if sequence.size_hint().is_some_and(|size| size > self.maximum) {
            return Err(A::Error::custom(LIMIT_ERROR));
        }
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(self.maximum));
        for _ in 0..self.maximum {
            let Some(value) = sequence.next_element_seed(self.element.clone())? else {
                return Ok(values);
            };
            values.push(value);
        }
        // SeqAccess invokes this seed only when another element exists. Do not
        // decode even IgnoredAny: that would still traverse an oversized value.
        sequence.next_element_seed(RejectElement)?;
        Ok(values)
    }
}
struct RejectElement;
impl<'de> DeserializeSeed<'de> for RejectElement {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
        Err(D::Error::custom(LIMIT_ERROR))
    }
}

pub(super) fn definitions<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<FrozenDelegationDefinition>, D::Error> {
    VecSeed::new(
        DefinitionGraphLimits::default().definitions,
        PhantomData::<FrozenDelegationDefinition>,
    )
    .deserialize(deserializer)
}

pub(super) struct ManifestSeed {
    pub(super) selectors: usize,
}
impl<'de> DeserializeSeed<'de> for ManifestSeed {
    type Value = DefinitionManifest;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct(
            "DefinitionManifest",
            &["version", "root_sha256", "bindings"],
            self,
        )
    }
}
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    Version,
    RootSha256,
    Bindings,
}

impl<'de> Visitor<'de> for ManifestSeed {
    type Value = DefinitionManifest;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded definition manifest object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (mut version, mut root, mut bindings) = (None, None, None);
        while let Some(field) = map.next_key()? {
            match field {
                Field::Version => {
                    if version.is_some() {
                        return Err(A::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                Field::RootSha256 => {
                    if root.is_some() {
                        return Err(A::Error::duplicate_field("root_sha256"));
                    }
                    root = Some(map.next_value()?);
                }
                Field::Bindings => {
                    if bindings.is_some() {
                        return Err(A::Error::duplicate_field("bindings"));
                    }
                    bindings = Some(map.next_value_seed(VecSeed::new(
                        self.selectors,
                        PhantomData::<DefinitionBinding>,
                    ))?);
                }
            }
        }
        Ok(DefinitionManifest {
            version: version.ok_or_else(|| A::Error::missing_field("version"))?,
            root_sha256: root.ok_or_else(|| A::Error::missing_field("root_sha256"))?,
            bindings: bindings.ok_or_else(|| A::Error::missing_field("bindings"))?,
        })
    }
}
