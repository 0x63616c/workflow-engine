//! WFE's storage engine: a log-structured merge tree over a grid of
//! checksummed blocks.
//!
//! The defining property is **physical determinism**: the same sequence of
//! `put` / `delete` / `maintain` / `checkpoint` calls produces byte-identical
//! blocks at identical addresses on every replica. Consequences:
//!
//! - a [`CheckpointRef`] (the checksum of the manifest, which covers every
//!   table, which covers every block) names the entire state, so two replicas
//!   with equal refs hold equal data;
//! - a corrupt block can be repaired by copying `(address, checksum)` from any
//!   peer, and a lagging replica can catch up by copying a checkpoint's blocks;
//! - after a crash, reopening the last checkpoint and replaying the log
//!   rewrites exactly the blocks that were lost.
//!
//! To keep that property there is no background work: flushes and compactions
//! happen inside [`Lsm::maintain`], driven only by the operation sequence, and
//! blocks are always allocated lowest-address-first.
//!
//! Durability is the caller's job. Nothing here calls `sync`; the caller syncs
//! the device after [`Lsm::checkpoint`], persists the returned ref (in its
//! superblock), syncs again, and only then calls [`Lsm::checkpoint_durable`],
//! which lets blocks freed before the checkpoint be reused.
//!
//! Any [`StorageError::Fault`] leaves the in-memory tree unusable: drop it,
//! repair the device (see [`missing_blocks`]), reopen from the checkpoint and
//! replay.

pub mod grid;
pub mod table;

pub use grid::{BlockRef, GridConfig, read_block_raw, write_block_raw};

use grid::{Allocator, BlockKind, read_block_body};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;
use table::{BlockCache, LevelIter, Merge, Reader, Source, Table, TableBuilder, TableInfo, TableIter, Writer};
use wfe_core::device::Device;

pub type KeyValue = (Vec<u8>, Vec<u8>);

/// Number of levels, including level 0.
pub const LEVELS: usize = 7;

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    /// A block is missing or corrupt.
    #[error("storage fault at block {}", .0.address)]
    Fault(BlockRef),
    /// The grid has no free blocks.
    #[error("storage is full")]
    Full,
    /// The key or the key plus value does not fit in a block.
    #[error("entry too large")]
    EntryTooLarge,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmOptions {
    /// Flush the memtable once it holds this many bytes.
    pub memtable_bytes_max: usize,
    /// Compact level 0 into level 1 once it holds more tables than this.
    pub l0_tables_max: usize,
    /// Each level holds this many times more tables than the one above.
    pub level_growth: usize,
    /// Maximum data blocks per table.
    pub table_data_blocks_max: usize,
    /// Decoded data blocks kept in memory.
    pub cache_blocks: usize,
}

impl Default for LsmOptions {
    fn default() -> LsmOptions {
        LsmOptions {
            memtable_bytes_max: 4 << 20,
            l0_tables_max: 4,
            level_growth: 8,
            table_data_blocks_max: 128,
            cache_blocks: 2048,
        }
    }
}

/// Names a complete, immutable state of the tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CheckpointRef {
    pub manifest: BlockRef,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LsmStats {
    pub blocks_total: u64,
    pub blocks_used: u64,
    pub tables_per_level: Vec<u32>,
    pub memtable_bytes: u64,
    pub memtable_entries: u64,
    pub flushes: u64,
    pub compactions: u64,
    pub moves: u64,
    pub blocks_read: u64,
    pub blocks_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

#[derive(Serialize, Deserialize)]
struct ManifestData {
    next_table_id: u64,
    levels: Vec<Vec<TableInfo>>,
    cursors: Vec<serde_bytes::ByteBuf>,
}

const MANIFEST_LINK: usize = size_of::<u64>() + size_of::<u128>();

#[derive(Default)]
struct Counters {
    flushes: u64,
    compactions: u64,
    moves: u64,
    blocks_read: u64,
    blocks_written: u64,
}

pub struct Lsm {
    grid: GridConfig,
    opts: LsmOptions,
    alloc: Allocator,
    memtable: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    memtable_bytes: usize,
    levels: Vec<Vec<Arc<Table>>>,
    last_key_compacted_out_per_level: Vec<Vec<u8>>,
    next_table_id: u64,
    manifest_blocks: Vec<u64>,
    checkpoint: Option<CheckpointRef>,
    cache: BlockCache,
    counters: Counters,
}

const MEM_ENTRY_OVERHEAD: usize = 32;

impl Lsm {
    /// Open the state named by `checkpoint`, or an empty tree for `None`.
    pub fn open(
        dev: &mut dyn Device,
        grid: GridConfig,
        opts: LsmOptions,
        checkpoint: Option<CheckpointRef>,
    ) -> Result<Lsm, StorageError> {
        grid.validate();
        let mut lsm = Lsm {
            grid,
            alloc: Allocator::new(grid.block_count),
            memtable: BTreeMap::new(),
            memtable_bytes: 0,
            levels: vec![Vec::new(); LEVELS],
            last_key_compacted_out_per_level: vec![Vec::new(); LEVELS],
            next_table_id: 1,
            manifest_blocks: Vec::new(),
            checkpoint,
            cache: BlockCache::new(opts.cache_blocks),
            counters: Counters::default(),
            opts,
        };
        let Some(checkpoint) = checkpoint else { return Ok(lsm) };

        let (manifest, manifest_blocks) =
            read_manifest(dev, &grid, checkpoint.manifest).map_err(StorageError::Fault)?;
        if manifest.levels.len() != LEVELS || manifest.cursors.len() != LEVELS {
            return Err(StorageError::Fault(checkpoint.manifest));
        }
        for &address in &manifest_blocks {
            lsm.alloc.mark(address);
        }
        lsm.manifest_blocks = manifest_blocks;
        lsm.next_table_id = manifest.next_table_id;
        lsm.last_key_compacted_out_per_level =
            manifest.cursors.into_iter().map(serde_bytes::ByteBuf::into_vec).collect();
        let mut r = Reader { dev, grid: &lsm.grid, cache: &mut lsm.cache, blocks_read: &mut lsm.counters.blocks_read };
        for (level, infos) in manifest.levels.into_iter().enumerate() {
            for info in infos {
                let index = r.read_index(info.index)?;
                lsm.alloc.mark(info.index.address);
                for entry in &index {
                    lsm.alloc.mark(entry.block.address);
                }
                lsm.levels[level].push(Arc::new(Table { info, index }));
            }
        }
        Ok(lsm)
    }

    pub fn grid(&self) -> &GridConfig {
        &self.grid
    }

    /// The last checkpoint taken or opened.
    pub fn checkpoint_ref(&self) -> Option<CheckpointRef> {
        self.checkpoint
    }

    pub fn max_key_size(&self) -> usize {
        (self.grid.body_max() / 8).min(1024)
    }

    /// Largest `key.len() + value.len()` accepted by [`Lsm::put`].
    pub fn max_entry_size(&self) -> usize {
        self.grid.body_max() - table::ENTRY_OVERHEAD
    }

    pub fn get(&mut self, dev: &mut dyn Device, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        if let Some(value) = self.memtable.get(key) {
            return Ok(value.clone());
        }
        let mut r =
            Reader { dev, grid: &self.grid, cache: &mut self.cache, blocks_read: &mut self.counters.blocks_read };
        for table in self.levels[0].iter().rev() {
            if table.info.key_min.as_slice() <= key
                && key <= table.info.key_max.as_slice()
                && let Some(found) = table::table_get(&mut r, table, key)?
            {
                return Ok(found);
            }
        }
        for level in &self.levels[1..] {
            let idx = level.partition_point(|t| t.info.key_max.as_slice() < key);
            if let Some(table) = level.get(idx)
                && table.info.key_min.as_slice() <= key
                && let Some(found) = table::table_get(&mut r, table, key)?
            {
                return Ok(found);
            }
        }
        Ok(None)
    }

    /// Up to `limit` live entries with `start <= key < end`, ascending, or
    /// descending when `reverse`. An empty `end` means unbounded.
    pub fn scan(
        &mut self,
        dev: &mut dyn Device,
        start: &[u8],
        end: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<KeyValue>, StorageError> {
        let mut out = Vec::new();
        if limit == 0 || (!end.is_empty() && start >= end) {
            return Ok(out);
        }
        let seek = if reverse { end } else { start };
        let mut sources = Vec::new();
        let upper = if end.is_empty() { Bound::Unbounded } else { Bound::Excluded(end) };
        let range = self.memtable.range::<[u8], _>((Bound::Included(start), upper));
        sources.push(Source::Mem(if reverse { Box::new(range.rev()) } else { Box::new(range) }));
        for table in self.levels[0].iter().rev() {
            let before = table.info.key_max.as_slice() < start;
            let after = !end.is_empty() && table.info.key_min.as_slice() >= end;
            if !before && !after {
                sources.push(Source::Table(TableIter::new(table.clone(), reverse, seek)));
            }
        }
        for level in &self.levels[1..] {
            let lo = level.partition_point(|t| t.info.key_max.as_slice() < start);
            let hi =
                if end.is_empty() { level.len() } else { level.partition_point(|t| t.info.key_min.as_slice() < end) };
            if lo < hi {
                sources.push(Source::Level(LevelIter::new(level[lo..hi].to_vec(), reverse, seek)));
            }
        }
        let mut merge = Merge::new(sources, reverse);
        let mut r =
            Reader { dev, grid: &self.grid, cache: &mut self.cache, blocks_read: &mut self.counters.blocks_read };
        while let Some((key, value)) = merge.next(&mut r)? {
            let past = if reverse { key.as_slice() < start } else { !end.is_empty() && key.as_slice() >= end };
            if past {
                break;
            }
            if let Some(value) = value {
                out.push((key, value));
                if out.len() >= limit {
                    break;
                }
            }
        }
        Ok(out)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        if key.is_empty() || key.len() > self.max_key_size() || key.len() + value.len() > self.max_entry_size() {
            return Err(StorageError::EntryTooLarge);
        }
        self.mem_insert(key, Some(value.to_vec()));
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) {
        let cannot_have_been_stored = key.is_empty() || key.len() > self.max_key_size();
        if cannot_have_been_stored {
            return;
        }
        self.mem_insert(key, None);
    }

    fn mem_insert(&mut self, key: &[u8], value: Option<Vec<u8>>) {
        let added = key.len() + value.as_ref().map_or(0, Vec::len) + MEM_ENTRY_OVERHEAD;
        if let Some(old) = self.memtable.insert(key.to_vec(), value) {
            self.memtable_bytes -= key.len() + old.map_or(0, |v| v.len()) + MEM_ENTRY_OVERHEAD;
        }
        self.memtable_bytes += added;
    }

    /// Flush and compact as far as the tree's shape requires. Call after
    /// every committed operation, on every replica, so that the work happens
    /// at the same point in the operation sequence everywhere.
    pub fn maintain(&mut self, dev: &mut dyn Device) -> Result<(), StorageError> {
        if self.memtable_bytes >= self.opts.memtable_bytes_max {
            self.flush(dev)?;
        }
        while self.compact_once(dev)? {}
        Ok(())
    }

    fn flush(&mut self, dev: &mut dyn Device) -> Result<(), StorageError> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        let memtable = std::mem::take(&mut self.memtable);
        self.memtable_bytes = 0;
        let mut w = Writer {
            dev,
            grid: &self.grid,
            alloc: &mut self.alloc,
            next_table_id: &mut self.next_table_id,
            blocks_written: &mut self.counters.blocks_written,
        };
        let mut builder = TableBuilder::new(&self.grid, self.opts.table_data_blocks_max);
        for (key, value) in &memtable {
            builder.add(&mut w, key, value.as_deref())?;
        }
        let tables = builder.finish(&mut w)?;
        self.levels[0].extend(tables);
        self.counters.flushes += 1;
        Ok(())
    }

    fn level_capacity(&self, level: usize) -> usize {
        self.opts.l0_tables_max.max(1).saturating_mul(
            self.opts.level_growth.max(2).saturating_pow(u32::try_from(level).expect("a level index is below LEVELS")),
        )
    }

    fn compact_once(&mut self, dev: &mut dyn Device) -> Result<bool, StorageError> {
        if self.levels[0].len() > self.opts.l0_tables_max {
            let oldest_first = std::mem::take(&mut self.levels[0]);
            let min = oldest_first
                .iter()
                .map(|t| &t.info.key_min)
                .min()
                .expect("level 0 holds more than l0_tables_max tables, so it is not empty")
                .clone();
            let max = oldest_first
                .iter()
                .map(|t| &t.info.key_max)
                .max()
                .expect("level 0 holds more than l0_tables_max tables, so it is not empty")
                .clone();
            let newest_first = oldest_first.into_iter().rev().collect();
            self.merge_into(dev, newest_first, 1, &min, &max)?;
            return Ok(true);
        }
        for level in 1..LEVELS - 1 {
            if self.levels[level].len() > self.level_capacity(level) {
                let cursor = self.last_key_compacted_out_per_level[level].as_slice();
                let mut idx = self.levels[level].partition_point(|t| t.info.key_min.as_slice() <= cursor);
                if idx >= self.levels[level].len() {
                    idx = 0;
                }
                let victim = self.levels[level].remove(idx);
                self.last_key_compacted_out_per_level[level] = victim.info.key_max.clone();
                let (min, max) = (victim.info.key_min.clone(), victim.info.key_max.clone());
                self.merge_into(dev, vec![victim], level + 1, &min, &max)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn merge_into(
        &mut self,
        dev: &mut dyn Device,
        mut upper: Vec<Arc<Table>>,
        target: usize,
        min: &[u8],
        max: &[u8],
    ) -> Result<(), StorageError> {
        let lo = self.levels[target].partition_point(|t| t.info.key_max.as_slice() < min);
        let hi = self.levels[target].partition_point(|t| t.info.key_min.as_slice() <= max);
        let lower: Vec<Arc<Table>> = self.levels[target].drain(lo..hi.max(lo)).collect();
        if lower.is_empty()
            && upper.len() == 1
            && let Some(only) = upper.pop()
        {
            self.levels[target].insert(lo, only);
            self.counters.moves += 1;
            return Ok(());
        }

        let nothing_older_below = self.levels[target + 1..].iter().all(Vec::is_empty);
        let drop_tombstones = nothing_older_below;
        let mut sources: Vec<Source> =
            upper.iter().map(|t| Source::Table(TableIter::new(t.clone(), false, &[]))).collect();
        if !lower.is_empty() {
            sources.push(Source::Level(LevelIter::new(lower.clone(), false, &[])));
        }
        let mut merge = Merge::new(sources, false);
        let mut builder = TableBuilder::new(&self.grid, self.opts.table_data_blocks_max);
        loop {
            let entry = {
                let mut r = Reader {
                    dev: &mut *dev,
                    grid: &self.grid,
                    cache: &mut self.cache,
                    blocks_read: &mut self.counters.blocks_read,
                };
                merge.next(&mut r)?
            };
            let Some((key, value)) = entry else { break };
            if value.is_none() && drop_tombstones {
                continue;
            }
            let mut w = Writer {
                dev: &mut *dev,
                grid: &self.grid,
                alloc: &mut self.alloc,
                next_table_id: &mut self.next_table_id,
                blocks_written: &mut self.counters.blocks_written,
            };
            builder.add(&mut w, &key, value.as_deref())?;
        }
        let mut w = Writer {
            dev,
            grid: &self.grid,
            alloc: &mut self.alloc,
            next_table_id: &mut self.next_table_id,
            blocks_written: &mut self.counters.blocks_written,
        };
        let outputs = builder.finish(&mut w)?;
        for table in upper.iter().chain(lower.iter()) {
            for entry in &table.index {
                self.alloc.release(entry.block.address);
            }
            self.alloc.release(table.info.index.address);
        }
        self.levels[target].splice(lo..lo, outputs);
        debug_assert!(
            self.levels[target].windows(2).all(|w| w[0].info.key_max < w[1].info.key_min),
            "level {target} tables overlap"
        );
        self.counters.compactions += 1;
        Ok(())
    }

    /// Flush the memtable and write a manifest naming the whole tree. The
    /// result is not durable until the caller syncs the device.
    pub fn checkpoint(&mut self, dev: &mut dyn Device) -> Result<CheckpointRef, StorageError> {
        self.flush(dev)?;
        let manifest = ManifestData {
            next_table_id: self.next_table_id,
            levels: self.levels.iter().map(|level| level.iter().map(|t| t.info.clone()).collect()).collect(),
            cursors: self
                .last_key_compacted_out_per_level
                .iter()
                .map(|c| serde_bytes::ByteBuf::from(c.clone()))
                .collect(),
        };
        let bytes = wfe_core::codec::encode(&manifest);
        let chunk_size = self.grid.body_max() - MANIFEST_LINK;
        let chunks: Vec<&[u8]> = bytes.chunks(chunk_size).collect();
        let mut addresses = Vec::with_capacity(chunks.len());
        for _ in &chunks {
            addresses.push(self.alloc.acquire().ok_or(StorageError::Full)?);
        }
        let mut successor = BlockRef::NONE;
        for (chunk, &address) in chunks.iter().zip(&addresses).rev() {
            let mut body = Vec::with_capacity(MANIFEST_LINK + chunk.len());
            body.extend_from_slice(&successor.address.to_le_bytes());
            body.extend_from_slice(&successor.checksum.to_le_bytes());
            body.extend_from_slice(chunk);
            let (buf, block) = grid::encode_block(&self.grid, address, BlockKind::Manifest, &body);
            dev.write(self.grid.block_offset(address), &buf);
            self.counters.blocks_written += 1;
            successor = block;
        }
        let head = successor;
        for address in std::mem::replace(&mut self.manifest_blocks, addresses) {
            self.alloc.release(address);
        }
        self.alloc.checkpoint();
        let checkpoint = CheckpointRef { manifest: head };
        self.checkpoint = Some(checkpoint);
        Ok(checkpoint)
    }

    /// The last checkpoint is durable (device synced, ref persisted): blocks
    /// only its predecessors referenced may now be reused.
    pub fn checkpoint_durable(&mut self) {
        self.alloc.checkpoint_durable();
    }

    pub fn stats(&self) -> LsmStats {
        LsmStats {
            blocks_total: self.grid.block_count,
            blocks_used: self.alloc.used(),
            tables_per_level: self.levels.iter().map(|l| u32::try_from(l.len()).unwrap_or(u32::MAX)).collect(),
            memtable_bytes: self.memtable_bytes as u64,
            memtable_entries: self.memtable.len() as u64,
            flushes: self.counters.flushes,
            compactions: self.counters.compactions,
            moves: self.counters.moves,
            blocks_read: self.counters.blocks_read,
            blocks_written: self.counters.blocks_written,
            cache_hits: self.cache.hits,
            cache_misses: self.cache.misses,
        }
    }
}

fn read_manifest(
    dev: &mut dyn Device,
    grid: &GridConfig,
    head: BlockRef,
) -> Result<(ManifestData, Vec<u64>), BlockRef> {
    let mut bytes = Vec::new();
    let mut addresses = Vec::new();
    let mut next = head;
    while !next.is_none() {
        if addresses.len() as u64 > grid.block_count {
            return Err(head);
        }
        let body = read_block_body(dev, grid, next, BlockKind::Manifest).ok_or(next)?;
        if body.len() < MANIFEST_LINK {
            return Err(next);
        }
        addresses.push(next.address);
        next = BlockRef {
            address: u64::from_le_bytes(grid::get(&body, 0)),
            checksum: u128::from_le_bytes(grid::get(&body, 8)),
        };
        bytes.extend_from_slice(&body[MANIFEST_LINK..]);
    }
    #[expect(
        clippy::map_err_ignore,
        reason = "the caller repairs a manifest that verifies but does not decode exactly like a missing one: by its head block"
    )]
    let manifest = wfe_core::codec::decode::<ManifestData>(&bytes).map_err(|_| head)?;
    Ok((manifest, addresses))
}

/// Blocks that `checkpoint` references but the device does not hold intact,
/// at most `max` of them. The walk is top-down (manifest, then table indexes,
/// then data), and a missing block hides everything beneath it, so call again
/// after installing the returned blocks until the result is empty.
pub fn missing_blocks(
    dev: &mut dyn Device,
    grid: &GridConfig,
    checkpoint: &CheckpointRef,
    max: usize,
) -> Vec<BlockRef> {
    let mut missing = Vec::new();
    let manifest = match read_manifest(dev, grid, checkpoint.manifest) {
        Ok((manifest, _)) => manifest,
        Err(block) => {
            missing.push(block);
            return missing;
        }
    };
    for info in manifest.levels.iter().flatten() {
        if missing.len() >= max {
            break;
        }
        let index = read_block_body(dev, grid, info.index, BlockKind::Index).and_then(|b| table::decode_index(&b));
        let Some(index) = index else {
            missing.push(info.index);
            continue;
        };
        for entry in index {
            if missing.len() >= max {
                break;
            }
            if read_block_body(dev, grid, entry.block, BlockKind::Data).is_none() {
                missing.push(entry.block);
            }
        }
    }
    missing
}
