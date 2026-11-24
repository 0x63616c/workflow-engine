//! Data file layout and the superblock.
//!
//! ```text
//! [superblock x4][WAL headers ring][WAL prepares ring][grid]
//! ```
//!
//! The superblock holds the little state that must survive on its own: which
//! views this replica has promised itself to, and where its checkpoint is.
//! It is written as four copies in two synced pairs, so a crash can tear at
//! most one pair and a later read always finds an intact copy: either the old
//! state or the new one.

use crate::header::{HEADER_SIZE, Header};
use crate::message::CheckpointInfo;
use crate::{le, usize_from};
use serde::{Deserialize, Serialize};
use wfe_core::checksum::checksum;
use wfe_core::device::{Device, SECTOR_SIZE, sector_ceil};
use wfe_lsm::GridConfig;

pub const SUPERBLOCK_COPIES: u64 = 4;
const SUPERBLOCK_COPY_BYTES: usize = 8 * SECTOR_SIZE;
pub const SUPERBLOCK_COPY_SIZE: u64 = SUPERBLOCK_COPY_BYTES as u64;
const COPY_HEADER: usize = 24;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Layout {
    /// WAL capacity in prepares.
    pub slot_count: u32,
    /// Largest prepare (header plus body). A multiple of the sector size.
    pub message_size_max: u32,
    pub grid_block_size: u32,
    pub grid_block_count: u64,
}

impl Layout {
    pub fn validate(&self) {
        assert!(self.slot_count >= 16, "WAL too small");
        let message_size_max = usize_from(self.message_size_max);
        assert!(
            message_size_max >= SECTOR_SIZE && message_size_max.is_multiple_of(SECTOR_SIZE),
            "message_size_max must be a multiple of the sector size"
        );
        self.grid().validate();
    }

    pub fn superblock_offset(&self, copy: u64) -> u64 {
        copy * SUPERBLOCK_COPY_SIZE
    }

    pub fn wal_headers_offset(&self) -> u64 {
        SUPERBLOCK_COPIES * SUPERBLOCK_COPY_SIZE
    }

    pub fn wal_headers_size(&self) -> u64 {
        sector_ceil(u64::from(self.slot_count) * HEADER_SIZE as u64)
    }

    pub fn wal_prepares_offset(&self) -> u64 {
        self.wal_headers_offset() + self.wal_headers_size()
    }

    pub fn grid(&self) -> GridConfig {
        GridConfig {
            offset: self.wal_prepares_offset() + u64::from(self.slot_count) * u64::from(self.message_size_max),
            block_size: self.grid_block_size,
            block_count: self.grid_block_count,
        }
    }

    pub fn body_size_max(&self) -> usize {
        usize_from(self.message_size_max) - HEADER_SIZE
    }

    /// Size of the whole data file.
    pub fn total_size(&self) -> u64 {
        self.grid().end_offset()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuperblockState {
    /// Incremented on every write; the highest valid copy wins.
    pub sequence: u64,
    pub cluster: u128,
    pub replica: u8,
    pub replica_count: u8,
    pub layout: Layout,
    /// The highest view this replica has joined.
    pub view: u32,
    /// The latest view in which this replica's log was made canonical.
    pub log_view: u32,
    /// The top of the canonical log when `log_view` began (ascending,
    /// contiguous, never empty). Everything the replica claims about its log
    /// during a view change is anchored here.
    pub view_headers: Vec<Header>,
    pub checkpoint: CheckpointInfo,
}

pub struct Superblock {
    pub state: SuperblockState,
}

impl Superblock {
    pub fn format(dev: &mut dyn Device, mut state: SuperblockState) -> Superblock {
        state.sequence = 0;
        let mut superblock = Superblock { state };
        superblock.write(dev);
        superblock
    }

    /// Persist `self.state`. Durable when this returns.
    pub fn write(&mut self, dev: &mut dyn Device) {
        self.state.sequence += 1;
        let payload = wfe_core::codec::encode(&self.state);
        assert!(COPY_HEADER + payload.len() <= SUPERBLOCK_COPY_BYTES, "superblock state too large");
        let payload_len =
            u32::try_from(payload.len()).expect("the payload fits in one superblock copy, asserted above");
        for pair in 0..SUPERBLOCK_COPIES / 2 {
            for copy in [pair * 2, pair * 2 + 1] {
                let mut buf = vec![0u8; SUPERBLOCK_COPY_BYTES];
                buf[16..20].copy_from_slice(&payload_len.to_le_bytes());
                let copy_index = u32::try_from(copy).expect("copy indexes are below SUPERBLOCK_COPIES");
                buf[20..24].copy_from_slice(&copy_index.to_le_bytes());
                buf[COPY_HEADER..COPY_HEADER + payload.len()].copy_from_slice(&payload);
                let sum = checksum(&buf[16..COPY_HEADER + payload.len()]);
                buf[0..16].copy_from_slice(&sum.to_le_bytes());
                dev.write(self.state.layout.superblock_offset(copy), &buf);
            }
            dev.sync();
        }
    }

    /// Read the newest intact copy. `None` means the device is not formatted
    /// (or every copy is gone).
    pub fn open(dev: &mut dyn Device) -> Option<Superblock> {
        let mut best: Option<SuperblockState> = None;
        for copy in 0..SUPERBLOCK_COPIES {
            let mut buf = vec![0u8; SUPERBLOCK_COPY_BYTES];
            dev.read(copy * SUPERBLOCK_COPY_SIZE, &mut buf);
            let len = usize_from(le::u32_at(&buf, 16));
            if len == 0 || COPY_HEADER + len > buf.len() {
                continue;
            }
            let stored = le::u128_at(&buf, 0);
            if stored != checksum(&buf[16..COPY_HEADER + len]) {
                continue;
            }
            let misdirected_write = u64::from(le::u32_at(&buf, 20)) != copy;
            if misdirected_write {
                continue;
            }
            let Ok(state) = wfe_core::codec::decode::<SuperblockState>(&buf[COPY_HEADER..COPY_HEADER + len]) else {
                continue;
            };
            if best.as_ref().is_none_or(|b| state.sequence > b.sequence) {
                best = Some(state);
            }
        }
        best.map(|state| Superblock { state })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wfe_core::device::MemDevice;

    fn state() -> SuperblockState {
        let layout = Layout { slot_count: 32, message_size_max: 8192, grid_block_size: 4096, grid_block_count: 64 };
        SuperblockState {
            sequence: 0,
            cluster: 9,
            replica: 1,
            replica_count: 3,
            layout,
            view: 0,
            log_view: 0,
            view_headers: vec![Header::root(9)],
            checkpoint: CheckpointInfo { header: Header::root(9), lsm: None },
        }
    }

    #[test]
    fn survives_torn_and_corrupt_copies() {
        let mut dev = MemDevice::new();
        let mut sb = Superblock::format(&mut dev, state());
        sb.state.view = 5;
        sb.write(&mut dev);
        assert_eq!(Superblock::open(&mut dev).unwrap().state.view, 5);

        let before = dev.bytes().to_vec();
        sb.state.view = 6;
        sb.write(&mut dev);
        let after = dev.bytes().to_vec();
        let first_pair = 2 * SUPERBLOCK_COPY_BYTES;
        let mut crashed_after_first_pair = before.clone();
        crashed_after_first_pair[..first_pair].copy_from_slice(&after[..first_pair]);
        *dev.bytes_mut() = crashed_after_first_pair;
        assert_eq!(
            Superblock::open(&mut dev).unwrap().state.view,
            6,
            "a crash after the first synced pair keeps the new state"
        );

        let mut first_pair_torn_mid_write = before;
        first_pair_torn_mid_write[100] ^= 0xFF;
        first_pair_torn_mid_write[SUPERBLOCK_COPY_BYTES + 100] ^= 0xFF;
        *dev.bytes_mut() = first_pair_torn_mid_write;
        assert_eq!(Superblock::open(&mut dev).unwrap().state.view, 5, "a torn first pair falls back to the old state");

        *dev.bytes_mut() = vec![0u8; after.len()];
        assert!(Superblock::open(&mut dev).is_none());
    }
}
