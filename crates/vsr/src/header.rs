//! The 128-byte prepare header: the unit of the replicated, hash-chained log.

use crate::{le, usize_from};
use serde::{Deserialize, Serialize};
use wfe_core::checksum::checksum;

pub const HEADER_SIZE: usize = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Kind {
    /// An empty WAL slot. `op` holds the slot index so it is self-locating.
    Reserved = 0,
    /// Op 0, the common ancestor of every log in the cluster.
    Root = 1,
    /// A batch of client requests.
    Requests = 2,
    /// Time passing: lets the state machine fire timers deterministically.
    Pulse = 3,
}

impl Kind {
    fn from_u8(v: u8) -> Option<Kind> {
        match v {
            0 => Some(Kind::Reserved),
            1 => Some(Kind::Root),
            2 => Some(Kind::Requests),
            3 => Some(Kind::Pulse),
            _ => None,
        }
    }
}

/// Byte layout (little endian):
/// `checksum 0..16 | checksum_body 16..32 | parent 32..48 | cluster 48..64 |
///  size 64..68 | view 68..72 | op 72..80 | commit 80..88 | timestamp 88..96 |
///  kind 96 | replica 97 | zero 98..128`
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Header {
    /// Checksum of bytes 16..128 of the encoded header.
    pub checksum: u128,
    pub checksum_body: u128,
    /// Checksum of the header at `op - 1`: the hash chain.
    pub parent: u128,
    pub cluster: u128,
    /// Header plus body, in bytes.
    pub size: u32,
    /// The view in which this prepare was created.
    pub view: u32,
    pub op: u64,
    /// The primary's commit number when it created this prepare.
    pub commit: u64,
    /// Assigned by the primary, strictly increasing along the log. The only
    /// notion of time the state machine ever sees.
    pub timestamp: u64,
    pub kind: Kind,
    /// The primary that created this prepare.
    pub replica: u8,
}

/// The fields of a [`Header`] that its creator chooses; [`Header::new`] derives the rest.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HeaderFields {
    pub cluster: u128,
    pub view: u32,
    pub op: u64,
    pub parent: u128,
    pub commit: u64,
    pub timestamp: u64,
    pub kind: Kind,
    pub replica: u8,
}

impl HeaderFields {
    fn empty(cluster: u128, op: u64, kind: Kind) -> HeaderFields {
        HeaderFields { cluster, view: 0, op, parent: 0, commit: 0, timestamp: 0, kind, replica: 0 }
    }
}

impl Header {
    pub fn new(fields: HeaderFields, body: &[u8]) -> Header {
        let HeaderFields { cluster, view, op, parent, commit, timestamp, kind, replica } = fields;
        let mut header = Header {
            checksum: 0,
            checksum_body: checksum(body),
            parent,
            cluster,
            size: u32::try_from(HEADER_SIZE + body.len())
                .expect("a prepare is at most Layout::message_size_max bytes, which is a u32"),
            view,
            op,
            commit,
            timestamp,
            kind,
            replica,
        };
        header.checksum = header.compute_checksum();
        header
    }

    /// The genesis header. Identical on every replica of a cluster.
    pub fn root(cluster: u128) -> Header {
        Header::new(HeaderFields::empty(cluster, 0, Kind::Root), &[])
    }

    /// The header of an empty slot.
    pub fn reserved(cluster: u128, slot: u64) -> Header {
        Header::new(HeaderFields::empty(cluster, slot, Kind::Reserved), &[])
    }

    pub fn body_size(&self) -> usize {
        usize_from(self.size) - HEADER_SIZE
    }

    fn encode_fields(&self, buf: &mut [u8; HEADER_SIZE]) {
        buf[16..32].copy_from_slice(&self.checksum_body.to_le_bytes());
        buf[32..48].copy_from_slice(&self.parent.to_le_bytes());
        buf[48..64].copy_from_slice(&self.cluster.to_le_bytes());
        buf[64..68].copy_from_slice(&self.size.to_le_bytes());
        buf[68..72].copy_from_slice(&self.view.to_le_bytes());
        buf[72..80].copy_from_slice(&self.op.to_le_bytes());
        buf[80..88].copy_from_slice(&self.commit.to_le_bytes());
        buf[88..96].copy_from_slice(&self.timestamp.to_le_bytes());
        buf[96] = self.kind as u8;
        buf[97] = self.replica;
    }

    fn compute_checksum(&self) -> u128 {
        let mut buf = [0u8; HEADER_SIZE];
        self.encode_fields(&mut buf);
        checksum(&buf[16..])
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        self.encode_fields(&mut buf);
        buf[0..16].copy_from_slice(&self.checksum.to_le_bytes());
        buf
    }

    /// Decode and verify the header checksum. Fields are only interpreted
    /// once the checksum has been verified.
    pub fn from_bytes(buf: &[u8]) -> Option<Header> {
        if buf.len() < HEADER_SIZE {
            return None;
        }
        let stored = le::u128_at(buf, 0);
        if stored != checksum(&buf[16..HEADER_SIZE]) {
            return None;
        }
        let size = le::u32_at(buf, 64);
        if usize_from(size) < HEADER_SIZE || buf[98..HEADER_SIZE].iter().any(|b| *b != 0) {
            return None;
        }
        Some(Header {
            checksum: stored,
            checksum_body: le::u128_at(buf, 16),
            parent: le::u128_at(buf, 32),
            cluster: le::u128_at(buf, 48),
            size,
            view: le::u32_at(buf, 68),
            op: le::u64_at(buf, 72),
            commit: le::u64_at(buf, 80),
            timestamp: le::u64_at(buf, 88),
            kind: Kind::from_u8(buf[96])?,
            replica: buf[97],
        })
    }

    /// Whether a header received in a message is internally consistent.
    pub fn valid(&self) -> bool {
        self.checksum == self.compute_checksum() && usize_from(self.size) >= HEADER_SIZE
    }

    pub fn valid_body(&self, body: &[u8]) -> bool {
        self.body_size() == body.len() && self.checksum_body == checksum(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_tamper() {
        let fields = HeaderFields {
            cluster: 7,
            view: 3,
            op: 42,
            parent: 99,
            commit: 40,
            timestamp: 123_456,
            kind: Kind::Requests,
            replica: 2,
        };
        let h = Header::new(fields, b"body");
        let bytes = h.to_bytes();
        assert_eq!(Header::from_bytes(&bytes), Some(h));
        assert!(h.valid() && h.valid_body(b"body") && !h.valid_body(b"bodx"));
        for i in 0..HEADER_SIZE {
            let mut bad = bytes;
            bad[i] ^= 1;
            assert_eq!(Header::from_bytes(&bad), None, "byte {i}");
        }
        assert_ne!(Header::root(1).checksum, Header::root(2).checksum);
        assert_ne!(Header::reserved(1, 0).checksum, Header::reserved(1, 1).checksum);
    }
}
