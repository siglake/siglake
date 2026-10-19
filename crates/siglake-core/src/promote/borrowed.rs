//! Diagnostic candidate: borrow unescaped strings while keeping serde_json's
//! parser, numeric conversions, validation and recursion limit. Complex output
//! is still produced by the original owned extractor, never by this tree.
use std::{borrow::Cow, collections::BTreeMap, fmt};

use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Number, Value};

#[derive(Debug)]
pub(super) enum Node<'de> {
    Scalar(Value),
    String(Cow<'de, str>),
    // Retain and fully validate every element, including unselected values.
    Array(#[allow(dead_code)] Vec<Node<'de>>),
    Object(Object<'de>),
}

#[derive(Debug)]
pub(super) struct Object<'de>(BTreeMap<Cow<'de, str>, Node<'de>>);

impl<'de> Object<'de> {
    pub(super) fn lookup(&self, key: &str) -> Option<&Node<'de>> {
        if let Some(value) = self.0.get(key) {
            return Some(value);
        }
        let (head, tail) = key.split_once('.')?;
        match self.0.get(head)? {
            Node::Object(object) => object.lookup(tail),
            _ => None,
        }
    }
}

struct Text<'de>(Cow<'de, str>);
impl<'de> Deserialize<'de> for Text<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TextVisitor;
        impl<'de> Visitor<'de> for TextVisitor {
            type Value = Text<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON string")
            }
            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                s: &'de str,
            ) -> Result<Self::Value, E> {
                Ok(Text(Cow::Borrowed(s)))
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Text(Cow::Owned(s.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Self::Value, E> {
                Ok(Text(Cow::Owned(s)))
            }
        }
        deserializer.deserialize_str(TextVisitor)
    }
}

fn object<'de, M: MapAccess<'de>>(mut map: M) -> Result<Object<'de>, M::Error> {
    let mut values = BTreeMap::new();
    while let Some(Text(key)) = map.next_key()? {
        // These names have special meaning under optional serde_json features.
        // Defer the whole row to the original parser instead of interpreting
        // private representations or assuming a particular feature union.
        if key.starts_with("$serde_json::private::") {
            return Err(serde::de::Error::custom("use owned JSON representation"));
        }
        let value = map.next_value()?;
        values.insert(key, value); // Same decoded-key, last-value-wins behavior.
    }
    Ok(Object(values))
}

impl<'de> Deserialize<'de> for Object<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ObjectVisitor;
        impl<'de> Visitor<'de> for ObjectVisitor {
            type Value = Object<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a JSON object")
            }
            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                object(map)
            }
        }
        deserializer.deserialize_map(ObjectVisitor)
    }
}

impl<'de> Deserialize<'de> for Node<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct NodeVisitor;
        impl<'de> Visitor<'de> for NodeVisitor {
            type Value = Node<'de>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("any valid JSON value")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
                Ok(Node::Scalar(Value::Bool(v)))
            }
            fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
                Ok(Node::Scalar(Value::Number(v.into())))
            }
            fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
                Ok(Node::Scalar(Value::Number(v.into())))
            }
            fn visit_i128<E: serde::de::Error>(self, v: i128) -> Result<Self::Value, E> {
                Number::deserialize(serde::de::value::I128Deserializer::new(v))
                    .map(|n| Node::Scalar(Value::Number(n)))
            }
            fn visit_u128<E: serde::de::Error>(self, v: u128) -> Result<Self::Value, E> {
                Number::deserialize(serde::de::value::U128Deserializer::new(v))
                    .map(|n| Node::Scalar(Value::Number(n)))
            }
            fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
                Ok(Node::Scalar(
                    Number::from_f64(v).map_or(Value::Null, Value::Number),
                ))
            }
            fn visit_borrowed_str<E: serde::de::Error>(
                self,
                s: &'de str,
            ) -> Result<Self::Value, E> {
                Ok(Node::String(Cow::Borrowed(s)))
            }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Node::String(Cow::Owned(s.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(self, s: String) -> Result<Self::Value, E> {
                Ok(Node::String(Cow::Owned(s)))
            }
            fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(Node::Scalar(Value::Null))
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(Node::Scalar(Value::Null))
            }
            fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
                Deserialize::deserialize(d)
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Self::Value, S::Error> {
                let mut values = Vec::new();
                while let Some(value) = seq.next_element()? {
                    values.push(value);
                }
                Ok(Node::Array(values))
            }
            fn visit_map<M: MapAccess<'de>>(self, map: M) -> Result<Self::Value, M::Error> {
                object(map).map(Node::Object)
            }
        }
        deserializer.deserialize_any(NodeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn borrows_plain_keys_and_values_but_owns_unescaped_scratch() {
        let parsed: Object<'_> =
            serde_json::from_str(r#"{"plain":"value","escaped\u0020key":"v\nalue"}"#).unwrap();
        let (key, value) = parsed.0.get_key_value("plain").unwrap();
        assert!(matches!(key, Cow::Borrowed(_)));
        assert!(matches!(value, Node::String(Cow::Borrowed("value"))));
        let (key, value) = parsed.0.get_key_value("escaped key").unwrap();
        assert!(matches!(key, Cow::Owned(_)));
        assert!(matches!(value, Node::String(Cow::Owned(v)) if v == "v\nalue"));
    }
}
