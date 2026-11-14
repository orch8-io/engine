use super::{DefinitionGraphLimits, RawDocument, RawGraph};
use crate::delegation_definition::bounded::{ManifestSeed, VecSeed};
use crate::delegation_definition::value_budget::ValueSeed;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error as _, MapAccess, Visitor},
};
use std::{cell::Cell, fmt};

pub(super) struct GraphSeed {
    limits: DefinitionGraphLimits,
    budget: Cell<usize>,
}
impl GraphSeed {
    pub(super) fn new(limits: DefinitionGraphLimits) -> Self {
        Self {
            limits,
            budget: Cell::new(limits.expanded_bytes),
        }
    }
}
impl<'de> DeserializeSeed<'de> for GraphSeed {
    type Value = RawGraph;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_struct("RawGraph", &["manifest", "definitions"], self)
    }
}
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum Field {
    Manifest,
    Definitions,
}

impl<'de> Visitor<'de> for GraphSeed {
    type Value = RawGraph;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded definition graph object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let (mut manifest, mut definitions) = (None, None);
        while let Some(field) = map.next_key()? {
            match field {
                Field::Manifest => {
                    if manifest.is_some() {
                        return Err(A::Error::duplicate_field("manifest"));
                    }
                    manifest = Some(map.next_value_seed(ManifestSeed {
                        selectors: self.limits.selectors,
                    })?);
                }
                Field::Definitions => {
                    if definitions.is_some() {
                        return Err(A::Error::duplicate_field("definitions"));
                    }
                    definitions = Some(map.next_value_seed(VecSeed::new(
                        self.limits.definitions,
                        DocumentSeed {
                            budget: &self.budget,
                        },
                    ))?);
                }
            }
        }
        Ok(RawGraph {
            manifest: manifest.ok_or_else(|| A::Error::missing_field("manifest"))?,
            definitions: definitions.ok_or_else(|| A::Error::missing_field("definitions"))?,
            expanded_bytes: self.limits.expanded_bytes - self.budget.get(),
        })
    }
}

#[derive(Clone, Copy)]
struct DocumentSeed<'a> {
    budget: &'a Cell<usize>,
}
impl<'de> DeserializeSeed<'de> for DocumentSeed<'_> {
    type Value = RawDocument;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<RawDocument, D::Error> {
        deserializer.deserialize_struct(
            "RawDocument",
            &["version", "sequence_sha256", "definition"],
            self,
        )
    }
}
#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum DocumentField {
    Version,
    SequenceSha256,
    Definition,
}
impl<'de> Visitor<'de> for DocumentSeed<'_> {
    type Value = RawDocument;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded frozen definition object")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<RawDocument, A::Error> {
        let (mut version, mut hash, mut definition) = (None, None, None);
        while let Some(field) = map.next_key()? {
            match field {
                DocumentField::Version => {
                    if version.is_some() {
                        return Err(A::Error::duplicate_field("version"));
                    }
                    version = Some(map.next_value()?);
                }
                DocumentField::SequenceSha256 => {
                    if hash.is_some() {
                        return Err(A::Error::duplicate_field("sequence_sha256"));
                    }
                    hash = Some(map.next_value()?);
                }
                DocumentField::Definition => {
                    if definition.is_some() {
                        return Err(A::Error::duplicate_field("definition"));
                    }
                    definition = Some(map.next_value_seed(ValueSeed {
                        remaining: self.budget,
                    })?);
                }
            }
        }
        Ok(RawDocument {
            version: version.ok_or_else(|| A::Error::missing_field("version"))?,
            sequence_sha256: hash.ok_or_else(|| A::Error::missing_field("sequence_sha256"))?,
            definition: definition.ok_or_else(|| A::Error::missing_field("definition"))?,
        })
    }
}
