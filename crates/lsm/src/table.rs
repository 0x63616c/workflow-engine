//! Immutable sorted tables: a run of data blocks plus one index block.
//!
//! Data block body:  repeated `[key_len u16][value_len u32][key][value]`,
//!                   `value_len == u32::MAX` marks a tombstone.
//! Index block body: `[count u32]` then per data block
//!                   `[key_len u16][first_key][address u64][checksum u128]`.

use crate::StorageError;
use crate::grid::{Allocator, BlockKind, BlockRef, GridConfig, decode_block, encode_block};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use wfe_core::device::Device;

/// `None` is a tombstone.
pub type Entry = (Vec<u8>, Option<Vec<u8>>);
pub type DecodedBlock = Vec<Entry>;

const TOMBSTONE: u32 = u32::MAX;
pub const ENTRY_OVERHEAD: usize = 6;
pub const INDEX_ENTRY_OVERHEAD: usize = 2 + 8 + 16;
const INDEX_HEADER: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexEntry {
    pub first_key: Vec<u8>,
    pub block: BlockRef,
}

/// What the manifest records about a table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableInfo {
    pub id: u64,
    pub index: BlockRef,
    #[serde(with = "serde_bytes")]
    pub key_min: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub key_max: Vec<u8>,
    pub data_blocks: u32,
    pub entries: u64,
}

#[derive(Debug)]
pub struct Table {
    pub info: TableInfo,
    pub index: Vec<IndexEntry>,
}

impl Table {
    pub fn overlaps(&self, min: &[u8], max: &[u8]) -> bool {
        self.info.key_min.as_slice() <= max && min <= self.info.key_max.as_slice()
    }
}

fn encode_entry(buf: &mut Vec<u8>, key: &[u8], value: Option<&[u8]>) {
    buf.extend_from_slice(
        &u16::try_from(key.len())
            .expect("put() and delete() reject keys longer than max_key_size(), which is at most 1024")
            .to_le_bytes(),
    );
    match value {
        Some(v) => buf.extend_from_slice(
            &u32::try_from(v.len())
                .expect("put() rejects entries larger than max_entry_size(), which is below the u32 block size")
                .to_le_bytes(),
        ),
        None => buf.extend_from_slice(&TOMBSTONE.to_le_bytes()),
    }
    buf.extend_from_slice(key);
    if let Some(v) = value {
        buf.extend_from_slice(v);
    }
}

fn decode_data(body: &[u8]) -> Option<DecodedBlock> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < body.len() {
        let key_len = u16::from_le_bytes(body.get(at..at + 2)?.try_into().ok()?) as usize;
        let value_len = u32::from_le_bytes(body.get(at + 2..at + 6)?.try_into().ok()?);
        at += ENTRY_OVERHEAD;
        let key = body.get(at..at + key_len)?.to_vec();
        at += key_len;
        let value = if value_len == TOMBSTONE {
            None
        } else {
            let v = body.get(at..at + value_len as usize)?.to_vec();
            at += value_len as usize;
            Some(v)
        };
        out.push((key, value));
    }
    Some(out)
}

fn encode_index(entries: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    let count = u32::try_from(entries.len()).expect("an index fits in one block, whose size is a u32");
    buf.extend_from_slice(&count.to_le_bytes());
    for e in entries {
        buf.extend_from_slice(
            &u16::try_from(e.first_key.len())
                .expect("put() and delete() reject keys longer than max_key_size(), which is at most 1024")
                .to_le_bytes(),
        );
        buf.extend_from_slice(&e.first_key);
        buf.extend_from_slice(&e.block.address.to_le_bytes());
        buf.extend_from_slice(&e.block.checksum.to_le_bytes());
    }
    buf
}

pub fn decode_index(body: &[u8]) -> Option<Vec<IndexEntry>> {
    let count = u32::from_le_bytes(body.get(0..4)?.try_into().ok()?) as usize;
    let mut out = Vec::with_capacity(count);
    let mut at = INDEX_HEADER;
    for _ in 0..count {
        let key_len = u16::from_le_bytes(body.get(at..at + 2)?.try_into().ok()?) as usize;
        at += 2;
        let first_key = body.get(at..at + key_len)?.to_vec();
        at += key_len;
        let address = u64::from_le_bytes(body.get(at..at + 8)?.try_into().ok()?);
        let checksum = u128::from_le_bytes(body.get(at + 8..at + 24)?.try_into().ok()?);
        at += 24;
        out.push(IndexEntry { first_key, block: BlockRef { address, checksum } });
    }
    (at == body.len()).then_some(out)
}

/// Decoded data blocks, keyed by address and validated by checksum (addresses
/// are reused). Eviction is FIFO, which keeps it deterministic and trivial.
pub struct BlockCache {
    map: BTreeMap<u64, (u128, Arc<DecodedBlock>)>,
    order: VecDeque<u64>,
    capacity: usize,
    pub hits: u64,
    pub misses: u64,
}

impl BlockCache {
    pub fn new(capacity: usize) -> BlockCache {
        BlockCache { map: BTreeMap::new(), order: VecDeque::new(), capacity: capacity.max(1), hits: 0, misses: 0 }
    }

    fn get(&mut self, block: BlockRef) -> Option<Arc<DecodedBlock>> {
        match self.map.get(&block.address) {
            Some((sum, decoded)) if *sum == block.checksum => {
                self.hits += 1;
                Some(decoded.clone())
            }
            _ => None,
        }
    }

    fn insert(&mut self, block: BlockRef, decoded: Arc<DecodedBlock>) {
        if self.map.insert(block.address, (block.checksum, decoded)).is_none() {
            self.order.push_back(block.address);
            if self.order.len() > self.capacity
                && let Some(evict) = self.order.pop_front()
            {
                self.map.remove(&evict);
            }
        }
    }
}

/// Everything a read needs.
pub struct Reader<'a> {
    pub dev: &'a mut dyn Device,
    pub grid: &'a GridConfig,
    pub cache: &'a mut BlockCache,
    pub blocks_read: &'a mut u64,
}

impl Reader<'_> {
    /// Read and verify a block, returning its body.
    pub fn read_body(&mut self, block: BlockRef, kind: BlockKind) -> Result<Vec<u8>, StorageError> {
        if block.address == 0 || block.address > self.grid.block_count {
            return Err(StorageError::Fault(block));
        }
        let mut buf = vec![0u8; self.grid.block_size as usize];
        self.dev.read(self.grid.block_offset(block.address), &mut buf);
        *self.blocks_read += 1;
        match decode_block(self.grid, block.address, &buf) {
            Some((sum, k, range)) if sum == block.checksum && k == kind => Ok(buf[range].to_vec()),
            _ => Err(StorageError::Fault(block)),
        }
    }

    pub fn read_data(&mut self, block: BlockRef) -> Result<Arc<DecodedBlock>, StorageError> {
        if let Some(hit) = self.cache.get(block) {
            return Ok(hit);
        }
        self.cache.misses += 1;
        let body = self.read_body(block, BlockKind::Data)?;
        let decoded = Arc::new(decode_data(&body).ok_or(StorageError::Fault(block))?);
        self.cache.insert(block, decoded.clone());
        Ok(decoded)
    }

    pub fn read_index(&mut self, block: BlockRef) -> Result<Vec<IndexEntry>, StorageError> {
        let body = self.read_body(block, BlockKind::Index)?;
        decode_index(&body).ok_or(StorageError::Fault(block))
    }
}

/// Everything a table write needs.
pub struct Writer<'a> {
    pub dev: &'a mut dyn Device,
    pub grid: &'a GridConfig,
    pub alloc: &'a mut Allocator,
    pub next_table_id: &'a mut u64,
    pub blocks_written: &'a mut u64,
}

impl Writer<'_> {
    pub fn write_block(&mut self, address: u64, kind: BlockKind, body: &[u8]) -> BlockRef {
        let (buf, block) = encode_block(self.grid, address, kind, body);
        self.dev.write(self.grid.block_offset(address), &buf);
        *self.blocks_written += 1;
        block
    }

    pub fn acquire(&mut self) -> Result<u64, StorageError> {
        self.alloc.acquire().ok_or(StorageError::Full)
    }
}

/// Streams sorted entries into one or more tables.
pub struct TableBuilder {
    body_max: usize,
    data_blocks_max: usize,
    block: Vec<u8>,
    block_first_key: Vec<u8>,
    index: Vec<IndexEntry>,
    index_bytes: usize,
    key_min: Vec<u8>,
    key_max: Vec<u8>,
    entries: u64,
    out: Vec<Arc<Table>>,
}

impl TableBuilder {
    pub fn new(grid: &GridConfig, data_blocks_max: usize) -> TableBuilder {
        TableBuilder {
            body_max: grid.body_max(),
            data_blocks_max: data_blocks_max.max(1),
            block: Vec::new(),
            block_first_key: Vec::new(),
            index: Vec::new(),
            index_bytes: INDEX_HEADER,
            key_min: Vec::new(),
            key_max: Vec::new(),
            entries: 0,
            out: Vec::new(),
        }
    }

    /// Keys must arrive in strictly increasing order.
    pub fn add(&mut self, w: &mut Writer, key: &[u8], value: Option<&[u8]>) -> Result<(), StorageError> {
        debug_assert!(self.entries == 0 || self.key_max.as_slice() < key, "table keys out of order");
        let need = ENTRY_OVERHEAD + key.len() + value.map_or(0, <[u8]>::len);
        debug_assert!(need <= self.body_max);
        if !self.block.is_empty() && self.block.len() + need > self.body_max {
            self.finish_block(w)?;
        }
        if self.block.is_empty() {
            let index_full = self.index.len() >= self.data_blocks_max
                || self.index_bytes + INDEX_ENTRY_OVERHEAD + key.len() > self.body_max;
            if index_full {
                self.finish_table(w)?;
            }
            self.block_first_key = key.to_vec();
        }
        if self.entries == 0 {
            self.key_min = key.to_vec();
        }
        encode_entry(&mut self.block, key, value);
        self.key_max.clear();
        self.key_max.extend_from_slice(key);
        self.entries += 1;
        Ok(())
    }

    fn finish_block(&mut self, w: &mut Writer) -> Result<(), StorageError> {
        let address = w.acquire()?;
        let block = w.write_block(address, BlockKind::Data, &self.block);
        self.index_bytes += INDEX_ENTRY_OVERHEAD + self.block_first_key.len();
        self.index.push(IndexEntry { first_key: std::mem::take(&mut self.block_first_key), block });
        self.block.clear();
        Ok(())
    }

    fn finish_table(&mut self, w: &mut Writer) -> Result<(), StorageError> {
        if !self.block.is_empty() {
            self.finish_block(w)?;
        }
        if self.index.is_empty() {
            return Ok(());
        }
        let address = w.acquire()?;
        let index = std::mem::take(&mut self.index);
        let block = w.write_block(address, BlockKind::Index, &encode_index(&index));
        let id = *w.next_table_id;
        *w.next_table_id += 1;
        self.out.push(Arc::new(Table {
            info: TableInfo {
                id,
                index: block,
                key_min: std::mem::take(&mut self.key_min),
                key_max: std::mem::take(&mut self.key_max),
                data_blocks: u32::try_from(index.len())
                    .expect("a table has at most table_data_blocks_max data blocks, a small count"),
                entries: self.entries,
            },
            index,
        }));
        self.index_bytes = INDEX_HEADER;
        self.entries = 0;
        Ok(())
    }

    pub fn finish(mut self, w: &mut Writer) -> Result<Vec<Arc<Table>>, StorageError> {
        self.finish_table(w)?;
        Ok(self.out)
    }
}

/// Point lookup. `Some(None)` means "deleted here": stop looking deeper.
pub fn table_get(r: &mut Reader, table: &Table, key: &[u8]) -> Result<Option<Option<Vec<u8>>>, StorageError> {
    let idx = table.index.partition_point(|e| e.first_key.as_slice() <= key);
    if idx == 0 {
        return Ok(None);
    }
    let block = r.read_data(table.index[idx - 1].block)?;
    Ok(block.binary_search_by(|e| e.0.as_slice().cmp(key)).ok().map(|i| block[i].1.clone()))
}

const POS_END: usize = usize::MAX;

/// Ordered cursor over one table, in either direction.
pub struct TableIter {
    table: Arc<Table>,
    reverse: bool,
    block_idx: usize,
    block: Option<Arc<DecodedBlock>>,
    next_or_one_past_next: usize,
    seek_once_first_block_loads: Option<Vec<u8>>,
    done: bool,
}

impl TableIter {
    /// Forward: start at the first key `>= seek`. Reverse: start at the last
    /// key `< seek`. An empty `seek` means "from the very start/end".
    pub fn new(table: Arc<Table>, reverse: bool, seek: &[u8]) -> TableIter {
        let blocks = table.index.len();
        let mut it = TableIter {
            table,
            reverse,
            block_idx: 0,
            block: None,
            next_or_one_past_next: 0,
            seek_once_first_block_loads: None,
            done: blocks == 0,
        };
        if it.done {
            return it;
        }
        if !reverse {
            if !seek.is_empty() {
                let idx = it.table.index.partition_point(|e| e.first_key.as_slice() <= seek);
                it.block_idx = idx.saturating_sub(1);
                it.seek_once_first_block_loads = Some(seek.to_vec());
            }
        } else if seek.is_empty() {
            it.block_idx = blocks - 1;
            it.next_or_one_past_next = POS_END;
        } else {
            let idx = it.table.index.partition_point(|e| e.first_key.as_slice() < seek);
            if idx == 0 {
                it.done = true;
            } else {
                it.block_idx = idx - 1;
                it.seek_once_first_block_loads = Some(seek.to_vec());
            }
        }
        it
    }

    pub fn next(&mut self, r: &mut Reader) -> Result<Option<Entry>, StorageError> {
        loop {
            if self.done {
                return Ok(None);
            }
            let block = match &self.block {
                Some(block) => Arc::clone(block),
                None => {
                    let block = r.read_data(self.table.index[self.block_idx].block)?;
                    if let Some(seek) = self.seek_once_first_block_loads.take() {
                        self.next_or_one_past_next = block.partition_point(|e| e.0 < seek);
                    } else if self.next_or_one_past_next == POS_END {
                        self.next_or_one_past_next = block.len();
                    }
                    self.block = Some(Arc::clone(&block));
                    block
                }
            };
            if !self.reverse {
                if self.next_or_one_past_next < block.len() {
                    self.next_or_one_past_next += 1;
                    return Ok(Some(block[self.next_or_one_past_next - 1].clone()));
                }
                self.block_idx += 1;
                self.next_or_one_past_next = 0;
                self.done = self.block_idx >= self.table.index.len();
            } else {
                if self.next_or_one_past_next > 0 {
                    self.next_or_one_past_next -= 1;
                    return Ok(Some(block[self.next_or_one_past_next].clone()));
                }
                if self.block_idx == 0 {
                    self.done = true;
                } else {
                    self.block_idx -= 1;
                    self.next_or_one_past_next = POS_END;
                }
            }
            self.block = None;
        }
    }
}

/// Ordered cursor over a run of non-overlapping tables sorted by key.
pub struct LevelIter {
    tables: Vec<Arc<Table>>,
    reverse: bool,
    next_or_one_past_next_table: usize,
    current: Option<TableIter>,
    seek: Vec<u8>,
}

impl LevelIter {
    pub fn new(tables: Vec<Arc<Table>>, reverse: bool, seek: &[u8]) -> LevelIter {
        let next_table = if !reverse {
            tables.partition_point(|t| t.info.key_max.as_slice() < seek)
        } else if seek.is_empty() {
            tables.len()
        } else {
            tables.partition_point(|t| t.info.key_min.as_slice() < seek)
        };
        LevelIter { tables, reverse, next_or_one_past_next_table: next_table, current: None, seek: seek.to_vec() }
    }

    pub fn next(&mut self, r: &mut Reader) -> Result<Option<Entry>, StorageError> {
        loop {
            if let Some(current) = &mut self.current {
                if let Some(entry) = current.next(r)? {
                    return Ok(Some(entry));
                }
                self.current = None;
            }
            let table = if !self.reverse {
                if self.next_or_one_past_next_table >= self.tables.len() {
                    return Ok(None);
                }
                self.next_or_one_past_next_table += 1;
                self.tables[self.next_or_one_past_next_table - 1].clone()
            } else {
                if self.next_or_one_past_next_table == 0 {
                    return Ok(None);
                }
                self.next_or_one_past_next_table -= 1;
                self.tables[self.next_or_one_past_next_table].clone()
            };
            let seek_only_in_first_table = std::mem::take(&mut self.seek);
            self.current = Some(TableIter::new(table, self.reverse, &seek_only_in_first_table));
        }
    }
}

type MemRange<'a> = Box<dyn Iterator<Item = (&'a Vec<u8>, &'a Option<Vec<u8>>)> + 'a>;

pub enum Source<'a> {
    Mem(MemRange<'a>),
    Table(TableIter),
    Level(LevelIter),
}

impl Source<'_> {
    fn next(&mut self, r: &mut Reader) -> Result<Option<Entry>, StorageError> {
        match self {
            Source::Mem(it) => Ok(it.next().map(|(k, v)| (k.clone(), v.clone()))),
            Source::Table(it) => it.next(r),
            Source::Level(it) => it.next(r),
        }
    }
}

/// K-way merge. Sources are listed newest first: on equal keys the earliest
/// source wins and the others' entries are skipped. Tombstones are yielded.
pub struct Merge<'a> {
    sources: Vec<Source<'a>>,
    heads: Vec<Option<Entry>>,
    reverse: bool,
    primed: bool,
}

impl<'a> Merge<'a> {
    pub fn new(sources: Vec<Source<'a>>, reverse: bool) -> Merge<'a> {
        let heads = sources.iter().map(|_| None).collect();
        Merge { sources, heads, reverse, primed: false }
    }

    pub fn next(&mut self, r: &mut Reader) -> Result<Option<Entry>, StorageError> {
        if !self.primed {
            for i in 0..self.sources.len() {
                self.heads[i] = self.sources[i].next(r)?;
            }
            self.primed = true;
        }
        let mut best: Option<(usize, &[u8])> = None;
        for (i, head) in self.heads.iter().enumerate() {
            let Some((key, _)) = head else { continue };
            let better = match best {
                None => true,
                Some((_, best_key)) => {
                    if self.reverse {
                        key.as_slice() > best_key
                    } else {
                        key.as_slice() < best_key
                    }
                }
            };
            if better {
                best = Some((i, key));
            }
        }
        let Some((best, _)) = best else { return Ok(None) };
        let winner = self.heads[best].take().expect("the best head was chosen because it is Some");
        self.heads[best] = self.sources[best].next(r)?;
        for i in best + 1..self.sources.len() {
            if self.heads[i].as_ref().is_some_and(|(k, _)| *k == winner.0) {
                self.heads[i] = self.sources[i].next(r)?;
            }
        }
        Ok(Some(winner))
    }
}
