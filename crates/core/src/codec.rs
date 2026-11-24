//! The one place that decides how values become bytes on disk and on the wire.
//!
//! MessagePack with named fields: deterministic for the types we use, and
//! schema-evolvable - new fields get `#[serde(default)]` and old readers
//! ignore fields they do not know. Byte buffers use `serde_bytes` so they
//! encode as binary, not as arrays.

use serde::{Serialize, de::DeserializeOwned};

#[derive(Debug, thiserror::Error)]
#[error("decode error: {0}")]
pub struct DecodeError(pub String);

pub fn encode<T: Serialize + ?Sized>(value: &T) -> Vec<u8> {
    rmp_serde::to_vec_named(value).expect("encoding an in-memory value cannot fail")
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, DecodeError> {
    rmp_serde::from_slice(bytes).map_err(|e| DecodeError(e.to_string()))
}
