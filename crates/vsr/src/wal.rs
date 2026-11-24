//! The write-ahead log: a ring of prepares plus a ring of redundant headers.
//!
//! Each prepare lives in the slot `op % slot_count`. Its header is stored
//! twice: at the front of the prepare, and in the headers ring. An append
//! writes and syncs the prepare first and the redundant header second, and
//! only then is the prepare acknowledged. That ordering is what lets recovery
//! tell apart the two reasons a prepare can be unreadable:
//!
//! - the redundant header does not name it: the append was torn by a crash
//!   and was never acknowledged, so it is safe to forget;
//! - the redundant header does name it: it was durable and has since been
//!   damaged, so it must be repaired from a peer and never disowned.
//!
//! This module only knows about slots. Which of the prepares on disk form the
//! replica's log is decided by the replica, from the hash chain.

use crate::header::{HEADER_SIZE, Header, Kind};
use crate::superblock::Layout;
use crate::usize_from;
use std::ops::Range;
use wfe_core::device::{Device, SECTOR_SIZE, sector_ceil};

fn ring_entry(slot: u64) -> Range<usize> {
    let start = usize_from(slot) * HEADER_SIZE;
    start..start + HEADER_SIZE
}

fn ring_sector_of(slot: u64) -> usize {
    usize_from(slot) * HEADER_SIZE / SECTOR_SIZE
}

fn sector_bytes(sector: usize) -> Range<usize> {
    sector * SECTOR_SIZE..(sector + 1) * SECTOR_SIZE
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Slot {
    /// The valid prepare physically in this slot.
    pub disk: Option<Header>,
    /// A prepare this replica durably held here and can no longer read.
    pub lost: Option<Header>,
    /// Both copies are unreadable: something unknown may have been here.
    pub uncertain: bool,
}

pub struct Wal {
    cluster: u128,
    slot_count: u64,
    message_size_max: u64,
    headers_offset: u64,
    prepares_offset: u64,
    headers_ring_on_disk: Vec<u8>,
    slots: Vec<Slot>,
}

impl Wal {
    fn new(layout: &Layout, cluster: u128) -> Wal {
        Wal {
            cluster,
            slot_count: u64::from(layout.slot_count),
            message_size_max: u64::from(layout.message_size_max),
            headers_offset: layout.wal_headers_offset(),
            prepares_offset: layout.wal_prepares_offset(),
            headers_ring_on_disk: vec![0u8; usize_from(layout.wal_headers_size())],
            slots: vec![Slot::default(); usize_from(layout.slot_count)],
        }
    }

    /// Initialize an empty log holding only the root prepare.
    pub fn format(dev: &mut dyn Device, layout: &Layout, cluster: u128) {
        let mut wal = Wal::new(layout, cluster);
        let zero = vec![0u8; SECTOR_SIZE];
        for slot in 0..wal.slot_count {
            let reserved = Header::reserved(cluster, slot).to_bytes();
            wal.headers_ring_on_disk[ring_entry(slot)].copy_from_slice(&reserved);
            dev.write(wal.prepare_offset(slot), &zero);
        }
        dev.write(wal.headers_offset, &wal.headers_ring_on_disk);
        dev.sync();
        wal.append(dev, &Header::root(cluster), &[]);
    }

    /// Read both rings and classify every slot.
    pub fn recover(dev: &mut dyn Device, layout: &Layout, cluster: u128) -> Wal {
        let mut wal = Wal::new(layout, cluster);
        dev.read(wal.headers_offset, &mut wal.headers_ring_on_disk);
        let mut dirty_sectors = Vec::new();
        for slot in 0..wal.slot_count {
            let redundant = Header::from_bytes(&wal.headers_ring_on_disk[ring_entry(slot)])
                .filter(|h| h.cluster == cluster && wal.belongs(h, slot));
            let prepare = wal.read_and_verify_prepare_in_slot(dev, slot);
            let state = &mut wal.slots[usize_from(slot)];
            match (redundant, prepare) {
                (redundant, Some(prepare)) => {
                    state.disk = Some(prepare);
                    let ring_names_a_newer_prepare_whose_write_was_lost = redundant.filter(|h| {
                        h.kind != Kind::Reserved && (h.op > prepare.op || (h.op == prepare.op && h.view > prepare.view))
                    });
                    let ring_entry_torn_or_damaged = redundant != Some(prepare);
                    if let Some(lost) = ring_names_a_newer_prepare_whose_write_was_lost {
                        state.lost = Some(lost);
                    } else if ring_entry_torn_or_damaged {
                        wal.headers_ring_on_disk[ring_entry(slot)].copy_from_slice(&prepare.to_bytes());
                        let sector = ring_sector_of(slot);
                        if dirty_sectors.last() != Some(&sector) {
                            dirty_sectors.push(sector);
                        }
                    }
                }
                (Some(redundant), None) => {
                    if redundant.kind != Kind::Reserved {
                        state.lost = Some(redundant);
                    }
                }
                (None, None) => state.uncertain = true,
            }
        }
        for &sector in &dirty_sectors {
            let range = sector_bytes(sector);
            dev.write(wal.headers_offset + range.start as u64, &wal.headers_ring_on_disk[range]);
        }
        if !dirty_sectors.is_empty() {
            dev.sync();
        }
        wal
    }

    fn belongs(&self, header: &Header, slot: u64) -> bool {
        match header.kind {
            Kind::Reserved => header.op == slot,
            Kind::Root | Kind::Requests | Kind::Pulse => header.op % self.slot_count == slot,
        }
    }

    fn prepare_offset(&self, slot: u64) -> u64 {
        self.prepares_offset + slot * self.message_size_max
    }

    fn read_and_verify_prepare_in_slot(&self, dev: &mut dyn Device, slot: u64) -> Option<Header> {
        self.read_slot_body(dev, slot).map(|(header, _)| header)
    }

    fn read_slot_body(&self, dev: &mut dyn Device, slot: u64) -> Option<(Header, Vec<u8>)> {
        let mut first = vec![0u8; SECTOR_SIZE];
        dev.read(self.prepare_offset(slot), &mut first);
        let header = Header::from_bytes(&first)?;
        if header.cluster != self.cluster
            || header.kind == Kind::Reserved
            || header.op % self.slot_count != slot
            || u64::from(header.size) > self.message_size_max
        {
            return None;
        }
        let total = usize_from(sector_ceil(u64::from(header.size)));
        let mut buf = first;
        if total > SECTOR_SIZE {
            buf.resize(total, 0);
            dev.read(self.prepare_offset(slot) + SECTOR_SIZE as u64, &mut buf[SECTOR_SIZE..]);
        }
        let body = &buf[HEADER_SIZE..usize_from(header.size)];
        header.valid_body(body).then(|| (header, body.to_vec()))
    }

    pub fn slot_count(&self) -> u64 {
        self.slot_count
    }

    fn index(&self, op: u64) -> usize {
        usize_from(op % self.slot_count)
    }

    pub fn slot(&self, op: u64) -> &Slot {
        &self.slots[self.index(op)]
    }

    /// The header of the prepare for exactly `op`, if it is on disk.
    pub fn header_at(&self, op: u64) -> Option<Header> {
        self.slot(op).disk.filter(|h| h.op == op)
    }

    pub fn has(&self, op: u64, checksum: u128) -> bool {
        self.slot(op).disk.is_some_and(|h| h.op == op && h.checksum == checksum)
    }

    /// Forget recovery-time doubts about slots that can no longer matter.
    pub fn clear_uncertain(&mut self, above_op: u64, window_end: u64) {
        let mut op = above_op + 1;
        while op <= window_end && op - above_op <= self.slot_count {
            let index = self.index(op);
            let slot = &mut self.slots[index];
            slot.uncertain = false;
            slot.lost = None;
            op += 1;
        }
    }

    /// Write a prepare. Durable (both copies) when this returns.
    pub fn append(&mut self, dev: &mut dyn Device, header: &Header, body: &[u8]) {
        assert!(header.kind != Kind::Reserved && u64::from(header.size) <= self.message_size_max);
        debug_assert!(header.valid() && header.valid_body(body));
        let slot = header.op % self.slot_count;
        let mut buf = vec![0u8; usize_from(sector_ceil(u64::from(header.size)))];
        buf[..HEADER_SIZE].copy_from_slice(&header.to_bytes());
        buf[HEADER_SIZE..usize_from(header.size)].copy_from_slice(body);
        self.slots[usize_from(slot)] = Slot::default();
        dev.write(self.prepare_offset(slot), &buf);
        dev.sync();

        self.headers_ring_on_disk[ring_entry(slot)].copy_from_slice(&header.to_bytes());
        let range = sector_bytes(ring_sector_of(slot));
        dev.write(self.headers_offset + range.start as u64, &self.headers_ring_on_disk[range]);
        dev.sync();
        self.slots[usize_from(slot)].disk = Some(*header);
    }

    /// Read the body of the prepare `(op, checksum)` if this slot holds it.
    /// A prepare that should be here but fails verification is marked lost.
    pub fn read(&mut self, dev: &mut dyn Device, op: u64, checksum: u128) -> Option<Vec<u8>> {
        if !self.has(op, checksum) {
            return None;
        }
        let slot = op % self.slot_count;
        match self.read_slot_body(dev, slot) {
            Some((header, body)) if header.checksum == checksum => Some(body),
            _ => {
                let state = &mut self.slots[usize_from(slot)];
                state.lost = state.disk.take();
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HeaderFields;
    use wfe_core::device::MemDevice;

    const CLUSTER: u128 = 77;

    fn layout() -> Layout {
        Layout { slot_count: 16, message_size_max: 8192, grid_block_size: 4096, grid_block_count: 8 }
    }

    fn chain(dev: &mut MemDevice, wal: &mut Wal, from: u64, to: u64, parent: Header) -> Header {
        let mut parent = parent;
        for op in from..=to {
            let fill = u8::try_from(op).expect("test chains stay below op 256");
            let body = vec![fill; 100 + usize_from(op) * 300];
            let fields = HeaderFields {
                cluster: CLUSTER,
                view: 0,
                op,
                parent: parent.checksum,
                commit: 0,
                timestamp: op,
                kind: Kind::Requests,
                replica: 0,
            };
            let header = Header::new(fields, &body);
            wal.append(dev, &header, &body);
            parent = header;
        }
        parent
    }

    #[test]
    fn recovers_what_was_appended() {
        let mut dev = MemDevice::new();
        Wal::format(&mut dev, &layout(), CLUSTER);
        let mut wal = Wal::recover(&mut dev, &layout(), CLUSTER);
        assert_eq!(wal.header_at(0), Some(Header::root(CLUSTER)));
        let head = chain(&mut dev, &mut wal, 1, 20, Header::root(CLUSTER));
        let mut wal = Wal::recover(&mut dev, &layout(), CLUSTER);
        assert_eq!(wal.header_at(20), Some(head));
        assert!(wal.header_at(4).is_none(), "slot 4 was overwritten by op 20");
        for op in 5..=20 {
            let header = wal.header_at(op).unwrap();
            let body = wal.read(&mut dev, op, header.checksum).unwrap();
            assert!(header.valid_body(&body));
            assert_eq!(*wal.slot(op), Slot { disk: Some(header), lost: None, uncertain: false });
        }
    }

    #[test]
    fn distinguishes_torn_from_lost() {
        let l = layout();
        let mut dev = MemDevice::new();
        Wal::format(&mut dev, &l, CLUSTER);
        let mut wal = Wal::recover(&mut dev, &l, CLUSTER);
        chain(&mut dev, &mut wal, 1, 6, Header::root(CLUSTER));
        let h5 = wal.header_at(5).unwrap();
        let h6 = wal.header_at(6).unwrap();
        let prepare = |op: u64| usize_from(l.wal_prepares_offset() + op * u64::from(l.message_size_max));
        let ring = |op: u64| usize_from(l.wal_headers_offset()) + usize_from(op) * HEADER_SIZE;
        let damaged_after_ack = 5;
        let crashed_between_prepare_and_ring_write = 6;
        let torn_append = 7;
        let both_copies_destroyed = 8;

        dev.bytes_mut()[prepare(damaged_after_ack) + 200] ^= 1;
        let reserved = Header::reserved(CLUSTER, crashed_between_prepare_and_ring_write).to_bytes();
        let ring_6 = ring(crashed_between_prepare_and_ring_write);
        dev.bytes_mut()[ring_6..ring_6 + HEADER_SIZE].copy_from_slice(&reserved);
        dev.bytes_mut()[prepare(torn_append)..prepare(torn_append) + 64].fill(0xAB);
        dev.bytes_mut()[prepare(both_copies_destroyed)..prepare(both_copies_destroyed) + 64].fill(0xCD);
        dev.bytes_mut()[ring(both_copies_destroyed)] ^= 1;

        let wal = Wal::recover(&mut dev, &l, CLUSTER);
        assert_eq!(*wal.slot(damaged_after_ack), Slot { disk: None, lost: Some(h5), uncertain: false });
        assert_eq!(
            *wal.slot(crashed_between_prepare_and_ring_write),
            Slot { disk: Some(h6), lost: None, uncertain: false }
        );
        assert_eq!(*wal.slot(torn_append), Slot::default());
        assert_eq!(*wal.slot(both_copies_destroyed), Slot { disk: None, lost: None, uncertain: true });
        assert_eq!(
            Header::from_bytes(&dev.bytes()[ring_6..ring_6 + HEADER_SIZE]),
            Some(h6),
            "recovery rewrites a stale ring entry from the intact prepare"
        );
    }

    #[test]
    fn read_detects_later_damage() {
        let l = layout();
        let mut dev = MemDevice::new();
        Wal::format(&mut dev, &l, CLUSTER);
        let mut wal = Wal::recover(&mut dev, &l, CLUSTER);
        chain(&mut dev, &mut wal, 1, 3, Header::root(CLUSTER));
        let h2 = wal.header_at(2).unwrap();
        let offset = usize_from(l.wal_prepares_offset() + 2 * u64::from(l.message_size_max));
        dev.bytes_mut()[offset + 300] ^= 1;
        assert_eq!(wal.read(&mut dev, 2, h2.checksum), None);
        assert_eq!(*wal.slot(2), Slot { disk: None, lost: Some(h2), uncertain: false });
    }
}
