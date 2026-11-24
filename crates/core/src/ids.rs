//! Identifiers, addresses and time.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A cell is one sharded, replicated cluster.
pub type CellId = u32;
/// A shard is one consensus group inside a cell.
pub type ShardId = u32;
/// Index of a replica within its shard's replica set.
pub type ReplicaIndex = u8;
/// VSR view number.
pub type View = u32;
/// Position in a shard's replicated log.
pub type OpNumber = u64;

/// Microseconds since the Unix epoch.
///
/// Microseconds keep the value below 2^53, so it survives JSON tooling that
/// parses numbers as doubles.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);
    pub const MAX: Timestamp = Timestamp(u64::MAX);

    pub const fn from_micros(us: u64) -> Timestamp {
        Timestamp(us)
    }
    pub const fn from_millis(ms: u64) -> Timestamp {
        Timestamp(ms * 1_000)
    }
    pub const fn micros(self) -> u64 {
        self.0
    }
    pub const fn millis(self) -> u64 {
        self.0 / 1_000
    }
    pub const fn add_ms(self, ms: u64) -> Timestamp {
        Timestamp(self.0.saturating_add(ms.saturating_mul(1_000)))
    }
    pub const fn add_micros(self, us: u64) -> Timestamp {
        Timestamp(self.0.saturating_add(us))
    }
    /// Milliseconds from `earlier` to `self`, zero if `earlier` is later.
    pub const fn since_ms(self, earlier: Timestamp) -> u64 {
        self.0.saturating_sub(earlier.0) / 1_000
    }
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// The UTC calendar date and time of day, to whole seconds.
    pub fn civil(self) -> CivilTime {
        let secs = self.0 / 1_000_000;
        let (year, month, day) = civil_from_days(secs / 86_400);
        let rem = secs % 86_400;
        CivilTime { year, month, day, hour: rem / 3_600, minute: rem % 3_600 / 60, second: rem % 60 }
    }

    /// `2026-11-24T10:15:30Z` (UTC, whole seconds), or `-` for zero.
    pub fn rfc3339(self) -> String {
        if self.0 == 0 {
            return "-".into();
        }
        let t = self.civil();
        format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", t.year, t.month, t.day, t.hour, t.minute, t.second)
    }
}

/// A UTC calendar date and time of day, to whole seconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CivilTime {
    pub year: i64,
    pub month: i64,
    pub day: i64,
    pub hour: u64,
    pub minute: u64,
    pub second: u64,
}

// <https://howardhinnant.github.io/date_algorithms.html#civil_from_days>
fn civil_from_days(days_since_epoch: u64) -> (i64, i64, i64) {
    let days = i64::try_from(days_since_epoch).expect("u64 microseconds span fewer than i64::MAX days");
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{:06}", self.0 / 1_000_000, self.0 % 1_000_000)
    }
}

/// A 128-bit identifier rendered as 32 hex characters.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id128(pub u128);

impl Id128 {
    pub const ZERO: Id128 = Id128(0);

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    pub fn parse(s: &str) -> Option<Id128> {
        let hex: String = s.chars().filter(|c| *c != '-').collect();
        if hex.len() != 32 {
            return None;
        }
        u128::from_str_radix(&hex, 16).ok().map(Id128)
    }

    /// Derive an id by hashing; deterministic, so replicas agree on it.
    pub fn derive(parts: &[&[u8]]) -> Id128 {
        Id128(crate::checksum::checksum_parts(parts))
    }
}

impl fmt::Display for Id128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl fmt::Debug for Id128 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl Serialize for Id128 {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if s.is_human_readable() {
            s.serialize_str(&self.to_string())
        } else {
            s.serialize_bytes(&self.0.to_be_bytes())
        }
    }
}

impl<'de> Deserialize<'de> for Id128 {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Id128, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = Id128;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a 128-bit id as 32 hex characters or 16 bytes")
            }
            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Id128, E> {
                Id128::parse(v).ok_or_else(|| E::custom("invalid 128-bit id"))
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Id128, E> {
                let arr: [u8; 16] =
                    v.try_into().map_err(|e| E::custom(format!("a binary Id128 is 16 bytes, got {}: {e}", v.len())))?;
                Ok(Id128(u128::from_be_bytes(arr)))
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Id128, A::Error> {
                let mut arr = [0u8; 16];
                for (i, slot) in arr.iter_mut().enumerate() {
                    *slot = seq.next_element()?.ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(Id128(u128::from_be_bytes(arr)))
            }
        }
        if d.is_human_readable() { d.deserialize_str(V) } else { d.deserialize_bytes(V) }
    }
}

/// Address of an actor on the message bus.
///
/// The deterministic core only ever names peers by `Addr`; the simulator and
/// the production runtime each map addresses to a transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Addr {
    /// One replica of one shard of one cell.
    Replica { cell: CellId, shard: ShardId, replica: ReplicaIndex },
    /// A stateless gateway of a cell (routes API calls to shards).
    Gateway { cell: CellId, gateway: u16 },
    /// An external process: SDK client, worker, CLI, controller.
    Client { id: u64 },
    /// S3-compatible object storage.
    ObjectStore { id: u16 },
}

impl Addr {
    pub fn replica(cell: CellId, shard: ShardId, replica: ReplicaIndex) -> Addr {
        Addr::Replica { cell, shard, replica }
    }
    pub fn gateway(cell: CellId, gateway: u16) -> Addr {
        Addr::Gateway { cell, gateway }
    }
    pub fn client(id: u64) -> Addr {
        Addr::Client { id }
    }
    pub fn cell(&self) -> Option<CellId> {
        match self {
            Addr::Replica { cell, .. } | Addr::Gateway { cell, .. } => Some(*cell),
            Addr::Client { .. } | Addr::ObjectStore { .. } => None,
        }
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Addr::Replica { cell, shard, replica } => write!(f, "c{cell}/s{shard}/r{replica}"),
            Addr::Gateway { cell, gateway } => write!(f, "c{cell}/gw{gateway}"),
            Addr::Client { id } => write!(f, "client{id}"),
            Addr::ObjectStore { id } => write!(f, "s3-{id}"),
        }
    }
}

/// Which shard of a cell owns a business id.
pub fn shard_for(business_id: &str, shard_count: u32) -> ShardId {
    assert!(shard_count > 0, "shard_for({business_id:?}, 0): a cell always has at least one shard");
    ShardId::try_from(crate::checksum::route_hash(business_id.as_bytes()) % u64::from(shard_count))
        .expect("a hash reduced modulo a u32 shard count fits in a u32")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id128_round_trips() {
        let id = Id128(0x0123_4567_89ab_cdef_0011_2233_4455_6677);
        assert_eq!(Id128::parse(&id.to_string()), Some(id));
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(serde_json::from_str::<Id128>(&json).unwrap(), id);
        let bin = crate::codec::encode(&id);
        assert_eq!(crate::codec::decode::<Id128>(&bin).unwrap(), id);
    }
}
