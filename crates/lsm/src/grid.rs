//! The grid: fixed-size, checksummed, self-locating blocks in a region of the
//! device, plus the block allocator.
//!
//! A block is identified by `(address, checksum)`. Because storage is
//! physically deterministic across replicas, that pair also identifies the
//! block on every *other* replica, which is what makes block-level repair and
//! state sync possible.

use serde::{Deserialize, Serialize};
use wfe_core::checksum::checksum;
use wfe_core::device::{Device, SECTOR_SIZE};

pub const BLOCK_HEADER_SIZE: usize = 48;
const CHECKSUM_AT: usize = 0;
const ADDRESS_AT: usize = 16;
const BODY_LEN_AT: usize = 24;
const KIND_AT: usize = 28;
const CHECKSUMMED_FROM: usize = ADDRESS_AT;
const _: () = assert!(KIND_AT < BLOCK_HEADER_SIZE);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GridConfig {
    /// Device offset of block address 1. Sector aligned.
    pub offset: u64,
    /// Multiple of `SECTOR_SIZE`.
    pub block_size: u32,
    /// Highest usable block address.
    pub block_count: u64,
}

impl GridConfig {
    pub fn validate(&self) {
        assert!(self.offset.is_multiple_of(SECTOR_SIZE as u64), "grid offset must be sector aligned");
        assert!(
            self.block_size as usize >= SECTOR_SIZE && (self.block_size as usize).is_multiple_of(SECTOR_SIZE),
            "block size must be a multiple of the sector size"
        );
        assert!(self.block_count >= 8, "grid has {} blocks; it needs at least 8", self.block_count);
    }

    pub fn block_offset(&self, address: u64) -> u64 {
        debug_assert!(address >= 1 && address <= self.block_count);
        self.offset + (address - 1) * u64::from(self.block_size)
    }

    pub fn body_max(&self) -> usize {
        self.block_size as usize - BLOCK_HEADER_SIZE
    }

    /// Device offset one past the last block.
    pub fn end_offset(&self) -> u64 {
        self.offset + self.block_count * u64::from(self.block_size)
    }
}

/// Identity of a block. `address == 0` means "none".
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BlockRef {
    pub address: u64,
    pub checksum: u128,
}

impl BlockRef {
    pub const NONE: BlockRef = BlockRef { address: 0, checksum: 0 };

    pub fn is_none(&self) -> bool {
        self.address == 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BlockKind {
    Data = 1,
    Index = 2,
    Manifest = 3,
}

impl BlockKind {
    fn from_u8(v: u8) -> Option<BlockKind> {
        match v {
            1 => Some(BlockKind::Data),
            2 => Some(BlockKind::Index),
            3 => Some(BlockKind::Manifest),
            _ => None,
        }
    }
}

fn put(buf: &mut [u8], at: usize, bytes: &[u8]) {
    buf[at..at + bytes.len()].copy_from_slice(bytes);
}

pub(crate) fn get<const N: usize>(buf: &[u8], at: usize) -> [u8; N] {
    std::array::from_fn(|i| buf[at + i])
}

/// Serialize a block image. Returns the full `block_size` buffer and its ref.
///
/// The header holds the checksum, address, body length and kind at the `*_AT`
/// offsets, zero up to `BLOCK_HEADER_SIZE`; the body follows, zero-padded to
/// `block_size`. The checksum covers everything from the address to the end
/// of the body.
pub fn encode_block(grid: &GridConfig, address: u64, kind: BlockKind, body: &[u8]) -> (Vec<u8>, BlockRef) {
    assert!(
        body.len() <= grid.body_max(),
        "block body is {} bytes; at most {} fit in a {}-byte block",
        body.len(),
        grid.body_max(),
        grid.block_size
    );
    let body_len = u32::try_from(body.len()).expect("a body no larger than body_max() fits in the u32 block size");
    let mut buf = vec![0u8; grid.block_size as usize];
    put(&mut buf, ADDRESS_AT, &address.to_le_bytes());
    put(&mut buf, BODY_LEN_AT, &body_len.to_le_bytes());
    buf[KIND_AT] = kind as u8;
    put(&mut buf, BLOCK_HEADER_SIZE, body);
    let sum = checksum(&buf[CHECKSUMMED_FROM..BLOCK_HEADER_SIZE + body.len()]);
    put(&mut buf, CHECKSUM_AT, &sum.to_le_bytes());
    (buf, BlockRef { address, checksum: sum })
}

/// Validate a block image read from `address`. Returns its checksum, kind and
/// the body range, or `None` if it is corrupt or belongs elsewhere.
pub fn decode_block(grid: &GridConfig, address: u64, buf: &[u8]) -> Option<(u128, BlockKind, std::ops::Range<usize>)> {
    if buf.len() != grid.block_size as usize {
        return None;
    }
    let len = usize::try_from(u32::from_le_bytes(get(buf, BODY_LEN_AT))).ok()?;
    if len > grid.body_max() {
        return None;
    }
    let stored = u128::from_le_bytes(get(buf, CHECKSUM_AT));
    if stored != checksum(&buf[CHECKSUMMED_FROM..BLOCK_HEADER_SIZE + len]) {
        return None;
    }
    let misdirected = u64::from_le_bytes(get(buf, ADDRESS_AT)) != address;
    if misdirected {
        return None;
    }
    let kind = BlockKind::from_u8(buf[KIND_AT])?;
    Some((stored, kind, BLOCK_HEADER_SIZE..BLOCK_HEADER_SIZE + len))
}

/// Read block `block` and return its body if it is exactly that block.
pub fn read_block_body(dev: &mut dyn Device, grid: &GridConfig, block: BlockRef, kind: BlockKind) -> Option<Vec<u8>> {
    if block.address == 0 || block.address > grid.block_count {
        return None;
    }
    let mut buf = vec![0u8; grid.block_size as usize];
    dev.read(grid.block_offset(block.address), &mut buf);
    match decode_block(grid, block.address, &buf) {
        Some((sum, k, range)) if sum == block.checksum && k == kind => Some(buf[range].to_vec()),
        _ => None,
    }
}

/// Read and validate the block at `address`, whatever it is.
pub fn read_block_raw(dev: &mut dyn Device, grid: &GridConfig, address: u64) -> Option<(BlockRef, Vec<u8>)> {
    if address == 0 || address > grid.block_count {
        return None;
    }
    let mut buf = vec![0u8; grid.block_size as usize];
    dev.read(grid.block_offset(address), &mut buf);
    let (sum, _, _) = decode_block(grid, address, &buf)?;
    Some((BlockRef { address, checksum: sum }, buf))
}

/// Install a block received from a peer, if it really is `expect`.
pub fn write_block_raw(dev: &mut dyn Device, grid: &GridConfig, expect: BlockRef, bytes: &[u8]) -> bool {
    if expect.address == 0 || expect.address > grid.block_count {
        return false;
    }
    match decode_block(grid, expect.address, bytes) {
        Some((sum, _, _)) if sum == expect.checksum => {
            dev.write(grid.block_offset(expect.address), bytes);
            true
        }
        _ => false,
    }
}

/// Block allocator: a bitset with "lowest free address first" allocation and
/// deferred release.
///
/// Blocks released during a checkpoint interval stay unavailable until the
/// *next* checkpoint is durable: a crash recovers to the previous checkpoint,
/// which still references them.
#[derive(Clone, Debug)]
pub struct Allocator {
    bits: Vec<u64>,
    block_count: u64,
    lowest_word_maybe_free: usize,
    used: u64,
    released_since_checkpoint: Vec<u64>,
    released_until_checkpoint_durable: Vec<u64>,
}

const BITMAP_FITS_IN_MEMORY: &str = "block_count bits fit in the allocator bitmap, which is in memory";

fn bit_index(address: u64) -> usize {
    usize::try_from(address - 1).expect(BITMAP_FITS_IN_MEMORY)
}

impl Allocator {
    pub fn new(block_count: u64) -> Allocator {
        let words = usize::try_from(block_count).expect(BITMAP_FITS_IN_MEMORY).div_ceil(64);
        Allocator {
            bits: vec![0; words],
            block_count,
            lowest_word_maybe_free: 0,
            used: 0,
            released_since_checkpoint: Vec::new(),
            released_until_checkpoint_durable: Vec::new(),
        }
    }

    pub fn used(&self) -> u64 {
        self.used
    }

    pub fn is_allocated(&self, address: u64) -> bool {
        let i = bit_index(address);
        self.bits[i / 64] >> (i % 64) & 1 == 1
    }

    /// Mark a block allocated (used when rebuilding from a checkpoint).
    pub fn mark(&mut self, address: u64) {
        assert!(address >= 1 && address <= self.block_count);
        let i = bit_index(address);
        if self.bits[i / 64] >> (i % 64) & 1 == 0 {
            self.bits[i / 64] |= 1 << (i % 64);
            self.used += 1;
        }
    }

    pub fn acquire(&mut self) -> Option<u64> {
        while self.lowest_word_maybe_free < self.bits.len() {
            let word = self.bits[self.lowest_word_maybe_free];
            if word != u64::MAX {
                let bit = (!word).trailing_zeros() as usize;
                let address = (self.lowest_word_maybe_free * 64 + bit) as u64 + 1;
                if address > self.block_count {
                    return None;
                }
                self.bits[self.lowest_word_maybe_free] |= 1 << bit;
                self.used += 1;
                return Some(address);
            }
            self.lowest_word_maybe_free += 1;
        }
        None
    }

    /// Release at the next durable checkpoint.
    pub fn release(&mut self, address: u64) {
        debug_assert!(self.is_allocated(address));
        self.released_since_checkpoint.push(address);
    }

    /// A checkpoint was taken: everything staged so far is unreferenced by it.
    pub fn checkpoint(&mut self) {
        self.released_until_checkpoint_durable.append(&mut self.released_since_checkpoint);
    }

    /// The checkpoint is durable: its predecessors' garbage is reusable.
    pub fn checkpoint_durable(&mut self) {
        for address in std::mem::take(&mut self.released_until_checkpoint_durable) {
            let i = bit_index(address);
            debug_assert!(self.bits[i / 64] >> (i % 64) & 1 == 1);
            self.bits[i / 64] &= !(1 << (i % 64));
            self.used -= 1;
            self.lowest_word_maybe_free = self.lowest_word_maybe_free.min(i / 64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wfe_core::device::MemDevice;

    fn grid() -> GridConfig {
        GridConfig { offset: 8192, block_size: 4096, block_count: 64 }
    }

    #[test]
    fn block_round_trip_and_misdirection() {
        let g = grid();
        let mut dev = MemDevice::new();
        let (buf, r) = encode_block(&g, 3, BlockKind::Data, b"hello");
        dev.write(g.block_offset(3), &buf);
        let (got, bytes) = read_block_raw(&mut dev, &g, 3).unwrap();
        assert_eq!(got, r);
        assert_eq!(bytes, buf);
        dev.write(g.block_offset(4), &buf);
        assert!(read_block_raw(&mut dev, &g, 4).is_none(), "the same bytes at the wrong address are rejected");
        dev.bytes_mut()[usize::try_from(g.block_offset(3)).unwrap() + 50] ^= 1;
        assert!(read_block_raw(&mut dev, &g, 3).is_none(), "a flipped bit is rejected");
        assert!(write_block_raw(&mut dev, &g, r, &buf));
        assert!(read_block_raw(&mut dev, &g, 3).is_some());
    }

    #[test]
    fn allocator_lowest_first_and_deferred_release() {
        let mut a = Allocator::new(10);
        assert_eq!(a.acquire(), Some(1));
        assert_eq!(a.acquire(), Some(2));
        assert_eq!(a.acquire(), Some(3));
        a.release(2);
        assert_eq!(a.acquire(), Some(4), "released block is not reusable yet");
        a.checkpoint();
        assert_eq!(a.acquire(), Some(5));
        a.checkpoint_durable();
        assert_eq!(a.acquire(), Some(2));
        assert_eq!(a.used(), 5);
    }
}
