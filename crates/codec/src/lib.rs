//! Payload codecs.
//!
//! The server never looks inside a [`Payload`]; codecs run in the SDK, on
//! the way in and out. A codec wraps a payload in an envelope: the original
//! (metadata and data) is serialized, transformed, and stored as the new
//! payload's data, with metadata saying which codec to undo.
//!
//! - [`Compression`]: LZ4, for payloads above a size threshold.
//! - [`ClaimCheck`]: payloads above a threshold are written to an
//!   [`ObjectStore`] under their content hash, and the payload that travels
//!   through the workflow engine is only the reference. The stores are
//!   [`MemoryStore`], [`FsStore`] and [`S3Store`] (anything that speaks the
//!   S3 API: AWS, MinIO, R2, Garage...).
//! - [`Chain`]: codecs applied in order on encode, in reverse on decode. The
//!   usual chain is compression, then claim check.
//!
//! Codecs are synchronous so that they also run inside the simulator; an
//! async caller wraps them in `spawn_blocking`.

mod s3;

pub use s3::{S3Config, S3Store};

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use wfe_core::Payload;
use wfe_core::codec::{decode, encode};

/// Metadata key naming the codec that produced an envelope.
pub const META_CODEC: &str = "wfe-codec";
pub const CODEC_LZ4: &str = "lz4";
pub const CODEC_CLAIM: &str = "claim-check";

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("payload is corrupt: {0}")]
    Corrupt(String),
    #[error("object store: {0}")]
    Store(String),
}

pub trait Codec: Send + Sync {
    fn encode(&self, payload: Payload) -> Result<Payload, CodecError>;
    /// Must return payloads it did not produce unchanged.
    fn decode(&self, payload: Payload) -> Result<Payload, CodecError>;
}

/// No codec.
pub struct Identity;

impl Codec for Identity {
    fn encode(&self, payload: Payload) -> Result<Payload, CodecError> {
        Ok(payload)
    }
    fn decode(&self, payload: Payload) -> Result<Payload, CodecError> {
        Ok(payload)
    }
}

fn envelope(codec: &str, data: Vec<u8>, extra: &[(&str, String)]) -> Payload {
    let mut metadata = BTreeMap::new();
    metadata.insert(META_CODEC.to_string(), codec.to_string());
    for (k, v) in extra {
        metadata.insert(k.to_string(), v.clone());
    }
    Payload { metadata, data }
}

fn is(payload: &Payload, codec: &str) -> bool {
    payload.metadata.get(META_CODEC).map(String::as_str) == Some(codec)
}

/// LZ4 compression of payloads of at least `threshold` bytes, kept only
/// when it actually makes them smaller.
pub struct Compression {
    pub threshold: usize,
}

impl Default for Compression {
    fn default() -> Compression {
        Compression { threshold: 1024 }
    }
}

impl Codec for Compression {
    fn encode(&self, payload: Payload) -> Result<Payload, CodecError> {
        if payload.size() < self.threshold {
            return Ok(payload);
        }
        let plain = encode(&payload);
        let packed = lz4_flex::compress_prepend_size(&plain);
        if packed.len() >= plain.len() {
            return Ok(payload);
        }
        Ok(envelope(CODEC_LZ4, packed, &[]))
    }

    fn decode(&self, payload: Payload) -> Result<Payload, CodecError> {
        if !is(&payload, CODEC_LZ4) {
            return Ok(payload);
        }
        let plain =
            lz4_flex::decompress_size_prepended(&payload.data).map_err(|e| CodecError::Corrupt(e.to_string()))?;
        decode(&plain).map_err(|e| CodecError::Corrupt(e.to_string()))
    }
}

/// Blob storage for claim-checked payloads.
pub trait ObjectStore: Send + Sync {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), CodecError>;
    fn get(&self, key: &str) -> Result<Vec<u8>, CodecError>;
}

impl<T: ObjectStore + ?Sized> ObjectStore for Arc<T> {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), CodecError> {
        (**self).put(key, bytes)
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, CodecError> {
        (**self).get(key)
    }
}

#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
}

impl MemoryStore {
    fn objects(&self) -> MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.objects.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub fn len(&self) -> usize {
        self.objects().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl ObjectStore for MemoryStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), CodecError> {
        self.objects().insert(key.to_string(), bytes.to_vec());
        Ok(())
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, CodecError> {
        self.objects().get(key).cloned().ok_or_else(|| CodecError::Store(format!("no object {key}")))
    }
}

/// Objects as files under a directory.
pub struct FsStore {
    pub root: PathBuf,
}

impl ObjectStore for FsStore {
    fn put(&self, key: &str, bytes: &[u8]) -> Result<(), CodecError> {
        let path = self.root.join(key);
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).map_err(|e| CodecError::Store(e.to_string()))?;
        }
        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, bytes)
            .and_then(|_| std::fs::rename(&tmp, &path))
            .map_err(|e| CodecError::Store(e.to_string()))
    }
    fn get(&self, key: &str) -> Result<Vec<u8>, CodecError> {
        std::fs::read(self.root.join(key)).map_err(|e| CodecError::Store(format!("{key}: {e}")))
    }
}

/// Payloads of at least `threshold` bytes live in the object store; the
/// workflow engine only carries the reference.
///
/// Objects are named by content hash, so encoding the same payload twice
/// (a retried request, a replayed workflow) writes the same object.
pub struct ClaimCheck<S> {
    pub store: S,
    pub threshold: usize,
    /// Key prefix inside the store.
    pub prefix: String,
}

impl<S: ObjectStore> ClaimCheck<S> {
    pub fn new(store: S, threshold: usize) -> ClaimCheck<S> {
        ClaimCheck { store, threshold, prefix: "payloads/".to_string() }
    }
}

impl<S: ObjectStore> Codec for ClaimCheck<S> {
    fn encode(&self, payload: Payload) -> Result<Payload, CodecError> {
        if payload.size() < self.threshold {
            return Ok(payload);
        }
        let bytes = encode(&payload);
        let hash = blake3::hash(&bytes).to_hex().to_string();
        let key = format!("{}{}/{}", self.prefix, &hash[..2], hash);
        self.store.put(&key, &bytes)?;
        Ok(envelope(CODEC_CLAIM, key.into_bytes(), &[("hash", hash), ("size", bytes.len().to_string())]))
    }

    fn decode(&self, payload: Payload) -> Result<Payload, CodecError> {
        if !is(&payload, CODEC_CLAIM) {
            return Ok(payload);
        }
        let key = String::from_utf8(payload.data).map_err(|e| CodecError::Corrupt(e.to_string()))?;
        let bytes = self.store.get(&key)?;
        if let Some(hash) = payload.metadata.get("hash")
            && blake3::hash(&bytes).to_hex().as_str() != hash
        {
            return Err(CodecError::Corrupt(format!("object {key} does not match its hash")));
        }
        decode(&bytes).map_err(|e| CodecError::Corrupt(e.to_string()))
    }
}

/// Codecs applied first to last on encode, last to first on decode.
#[derive(Clone, Default)]
pub struct Chain(pub Vec<Arc<dyn Codec>>);

impl Chain {
    pub fn new() -> Chain {
        Chain(Vec::new())
    }
    pub fn with(mut self, codec: impl Codec + 'static) -> Chain {
        self.0.push(Arc::new(codec));
        self
    }
    /// Compression, then claim check: the usual production chain.
    pub fn standard(store: impl ObjectStore + 'static, compress_above: usize, offload_above: usize) -> Chain {
        Chain::new().with(Compression { threshold: compress_above }).with(ClaimCheck::new(store, offload_above))
    }
}

impl Codec for Chain {
    fn encode(&self, mut payload: Payload) -> Result<Payload, CodecError> {
        for codec in &self.0 {
            payload = codec.encode(payload)?;
        }
        Ok(payload)
    }
    fn decode(&self, mut payload: Payload) -> Result<Payload, CodecError> {
        for codec in self.0.iter().rev() {
            payload = codec.decode(payload)?;
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chain_round_trips() {
        let store = Arc::new(MemoryStore::default());
        let chain = Chain::standard(store.clone(), 64, 4096);
        let small = Payload::json(&"hi").unwrap();
        assert_eq!(chain.encode(small.clone()).unwrap(), small);

        let compressible = Payload::raw(vec![7u8; 2000]);
        let packed = chain.encode(compressible.clone()).unwrap();
        assert!(packed.size() < 200 && is(&packed, CODEC_LZ4));
        assert_eq!(chain.decode(packed).unwrap(), compressible);

        let mut prng = wfe_core::Prng::new(1);
        let mut noise = vec![0u8; 20_000];
        prng.fill(&mut noise);
        let incompressible_and_large = Payload::raw(noise);
        let claim = chain.encode(incompressible_and_large.clone()).unwrap();
        assert!(is(&claim, CODEC_CLAIM) && claim.size() < 300);
        assert_eq!(store.len(), 1);
        assert_eq!(chain.encode(incompressible_and_large.clone()).unwrap(), claim, "content addressed");
        assert_eq!(chain.decode(claim).unwrap(), incompressible_and_large);
    }
}
