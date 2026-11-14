//! Bound decoded JSON growth as well as its wire representation. The charge is
//! a conservative logical budget, not an allocator-specific heap measurement.
use serde::{
    Deserializer,
    de::{DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;
use std::{cell::Cell, fmt};

/// Charge 64 per value/key plus UTF-8 string bytes before growing the tree.
const NODE_CHARGE: usize = 64;
const LIMIT_ERROR: &str = "definition exceeds decoded resource budget";

/// Typed graph verification shares the same accounting without cloning the
/// whole graph. Limit recursion even for values constructed outside JSON.
pub(super) fn check(
    value: &Value,
    remaining: &Cell<usize>,
    depth: usize,
) -> Result<(), crate::error::StorageError> {
    if depth >= 128 {
        return Err(crate::error::StorageError::Constraint(LIMIT_ERROR.into()));
    }
    let seed = ValueSeed { remaining };
    seed.charge::<serde_json::Error>(value.as_str().map_or(0, str::len))?;
    match value {
        Value::Array(values) => {
            for value in values {
                check(value, remaining, depth + 1)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                seed.charge::<serde_json::Error>(key.len())?;
                check(value, remaining, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) struct ValueSeed<'a> {
    pub(super) remaining: &'a Cell<usize>,
}
impl ValueSeed<'_> {
    fn charge<E: serde::de::Error>(self, bytes: usize) -> Result<(), E> {
        let remaining = self
            .remaining
            .get()
            .checked_sub(NODE_CHARGE.saturating_add(bytes))
            .ok_or_else(|| E::custom(LIMIT_ERROR))?;
        self.remaining.set(remaining);
        Ok(())
    }
}
impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;
    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        deserializer.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for ValueSeed<'_> {
    type Value = Value;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a definition value within its decoded resource budget")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Value, E> {
        self.charge(0)?;
        Ok(value.into())
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Value, E> {
        self.charge(0)?;
        Ok(value.into())
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Value, E> {
        self.charge(0)?;
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite definition number"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Value, E> {
        self.charge(value.len())?;
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Value, E> {
        self.charge(value.len())?;
        Ok(Value::String(value))
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Value, E> {
        self.charge(0)?;
        Ok(Value::Null)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Value, E> {
        self.visit_unit()
    }
    fn visit_some<D: Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        self.deserialize(deserializer)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Value, A::Error> {
        self.charge(0)?;
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(self)? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        self.charge(0)?;
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            self.charge(key.len())?;
            values.insert(key, map.next_value_seed(self)?);
        }
        Ok(Value::Object(values))
    }
}
