//! 128-bit checksums used for every on-disk structure and every network
//! message, and for hash-chaining log entries.
//!
//! BLAKE3 truncated to 128 bits: fast, and strong enough that a checksum can
//! double as a content identity (a block is addressed by address + checksum).

pub type Checksum = u128;

pub fn checksum(data: &[u8]) -> Checksum {
    truncate(&blake3::hash(data))
}

fn truncate(hash: &blake3::Hash) -> Checksum {
    let bytes = hash.as_bytes();
    u128::from_le_bytes(std::array::from_fn(|i| bytes[i]))
}

/// Checksum over several slices, as if concatenated.
pub fn checksum_parts(parts: &[&[u8]]) -> Checksum {
    let mut hasher = blake3::Hasher::new();
    for p in parts {
        hasher.update(p);
    }
    truncate(&hasher.finalize())
}

/// Incremental hasher for rolling digests (state digests, hash chains).
#[derive(Clone, Default)]
pub struct Hasher(blake3::Hasher);

impl Hasher {
    pub fn new() -> Hasher {
        Hasher(blake3::Hasher::new())
    }

    pub fn update(&mut self, data: &[u8]) -> &mut Hasher {
        self.0.update(data);
        self
    }

    pub fn update_u64(&mut self, v: u64) -> &mut Hasher {
        self.0.update(&v.to_le_bytes());
        self
    }

    pub fn update_u128(&mut self, v: u128) -> &mut Hasher {
        self.0.update(&v.to_le_bytes());
        self
    }

    pub fn finish(&self) -> Checksum {
        truncate(&self.0.finalize())
    }
}

/// Stable 64-bit hash for routing (shard selection, ramp buckets). Must never
/// change: it decides which shard owns an entity.
pub fn route_hash(data: &[u8]) -> u64 {
    let hash = blake3::hash(data);
    let bytes = hash.as_bytes();
    u64::from_le_bytes(std::array::from_fn(|i| bytes[i]))
}
