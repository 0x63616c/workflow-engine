//! Opaque user data.
//!
//! A workflow takes exactly one input [`Payload`] and returns exactly one
//! result [`Payload`]; the same goes for activities and signals. The server
//! never interprets `data`: the SDK's data converter and codec chain
//! (compression, object-store offload, ...) own its meaning and describe it in
//! `metadata`. Keeping it a struct, not a bare byte string, is what lets us
//! grow it later without breaking stored histories.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Metadata key naming how `data` is encoded.
pub const META_ENCODING: &str = "encoding";
/// `data` is a JSON document.
pub const ENCODING_JSON: &str = "json/plain";
/// `data` is empty and means "no value".
pub const ENCODING_NULL: &str = "binary/null";
/// `data` is raw bytes.
pub const ENCODING_RAW: &str = "binary/plain";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Payload {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub metadata: BTreeMap<String, String>,
    #[serde(default, with = "bytes_serde")]
    pub data: Vec<u8>,
}

impl Payload {
    pub fn null() -> Payload {
        let mut metadata = BTreeMap::new();
        metadata.insert(META_ENCODING.to_string(), ENCODING_NULL.to_string());
        Payload { metadata, data: Vec::new() }
    }

    pub fn raw(data: impl Into<Vec<u8>>) -> Payload {
        let mut metadata = BTreeMap::new();
        metadata.insert(META_ENCODING.to_string(), ENCODING_RAW.to_string());
        Payload { metadata, data: data.into() }
    }

    pub fn json<T: Serialize + ?Sized>(value: &T) -> Result<Payload, serde_json::Error> {
        let mut metadata = BTreeMap::new();
        metadata.insert(META_ENCODING.to_string(), ENCODING_JSON.to_string());
        Ok(Payload { metadata, data: serde_json::to_vec(value)? })
    }

    pub fn encoding(&self) -> Option<&str> {
        self.metadata.get(META_ENCODING).map(String::as_str)
    }

    pub fn to_json<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        if self.encoding() == Some(ENCODING_NULL) || self.data.is_empty() {
            return serde_json::from_slice(b"null");
        }
        serde_json::from_slice(&self.data)
    }

    /// Bytes this payload occupies in a history event, for limit checks.
    pub fn size(&self) -> usize {
        self.data.len() + self.metadata.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
    }
}

/// Serialize byte buffers compactly in binary formats and as base64 in
/// human-readable ones (JSON), where arrays of numbers would be unusable.
pub mod bytes_serde {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() { s.serialize_str(&super::base64_encode(bytes)) } else { s.serialize_bytes(bytes) }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        if d.is_human_readable() {
            let s: String = serde::Deserialize::deserialize(d)?;
            super::base64_decode(&s).ok_or_else(|| serde::de::Error::custom("invalid base64"))
        } else {
            let buf: serde_bytes::ByteBuf = serde::Deserialize::deserialize(d)?;
            Ok(buf.into_vec())
        }
    }
}

/// Replace every serialized payload in a JSON document (`{"metadata":
/// {"encoding": ...}, "data": "<base64>"}`) that holds plain JSON with the
/// JSON itself, so documents read as what the workflow saw. Payloads in
/// other encodings (compressed, offloaded) are left as they are.
pub fn readable(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::Object(map) => {
            if let (Some(Value::String(data)), Some(Value::Object(meta)), 2) =
                (map.get("data"), map.get("metadata"), map.len())
            {
                match meta.get(META_ENCODING).and_then(Value::as_str) {
                    Some(ENCODING_NULL) if meta.len() == 1 => return Value::Null,
                    Some(ENCODING_JSON) if meta.len() == 1 => {
                        if let Some(decoded) = base64_decode(data).and_then(|b| serde_json::from_slice(&b).ok()) {
                            return decoded;
                        }
                    }
                    _ => {}
                }
            }
            Value::Object(map.into_iter().map(|(k, v)| (k, readable(v))).collect())
        }
        Value::Array(items) => Value::Array(items.into_iter().map(readable).collect()),
        scalar @ (Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_)) => scalar,
    }
}

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { B64[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { B64[n as usize & 63] as char } else { '=' });
    }
    out
}

pub fn base64_decode(s: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u32> {
        match c {
            b'A'..=b'Z' => Some(u32::from(c - b'A')),
            b'a'..=b'z' => Some(u32::from(c - b'a') + 26),
            b'0'..=b'9' => Some(u32::from(c - b'0') + 52),
            b'+' | b'-' => Some(62),
            b'/' | b'_' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = s.bytes().filter(|b| *b != b'=' && !b.is_ascii_whitespace()).collect();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() == 1 {
            return None;
        }
        let mut n = 0u32;
        for (&c, shift) in chunk.iter().zip([18, 12, 6, 0]) {
            n |= val(c)? << shift;
        }
        let [_, first, second, third] = n.to_be_bytes();
        out.push(first);
        if chunk.len() > 2 {
            out.push(second);
        }
        if chunk.len() > 3 {
            out.push(third);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_round_trip() {
        for len in 0..40u8 {
            let data: Vec<u8> = (0..len).map(|i| i.wrapping_mul(37)).collect();
            assert_eq!(base64_decode(&base64_encode(&data)).unwrap(), data);
        }
    }

    #[test]
    fn payload_is_base64_in_json_and_binary_in_msgpack() {
        let p = Payload::json(&serde_json::json!({"a": 1})).unwrap();
        let json = serde_json::to_string(&p).unwrap();
        assert!(json.contains("eyJhIjoxfQ"), "{json}");
        assert_eq!(serde_json::from_str::<Payload>(&json).unwrap(), p);
        let bin = crate::codec::encode(&p);
        assert_eq!(crate::codec::decode::<Payload>(&bin).unwrap(), p);
    }
}
