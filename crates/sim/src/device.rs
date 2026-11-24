//! A simulated disk: loses or tears unsynced writes on a crash, and rots.

use wfe_core::Prng;
use wfe_core::device::{Device, SECTOR_SIZE};

#[derive(Clone, Default)]
pub struct SimDevice {
    data: Vec<u8>,
    undo_log_since_sync: Vec<(u64, Vec<u8>)>,
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
    pub sectors_corrupted: u64,
}

impl SimDevice {
    pub fn new() -> SimDevice {
        SimDevice::default()
    }

    pub fn len(&self) -> u64 {
        self.data.len() as u64
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    fn at(offset: u64) -> usize {
        usize::try_from(offset).expect("a simulated disk lives in memory, so every offset into it fits in usize")
    }

    fn ensure(&mut self, end: usize) {
        if self.data.len() < end {
            self.data.resize(end, 0);
        }
    }

    /// Power loss: every sector written since the last sync independently
    /// either made it to the platter or did not.
    pub fn crash(&mut self, prng: &mut Prng) {
        const LOSE_EVERY_SECTOR: u64 = 0;
        const KEEP_EVERY_SECTOR: u64 = 1;
        let unsynced = std::mem::take(&mut self.undo_log_since_sync);
        let half_all_or_nothing = prng.below(4);
        for (offset, old) in unsynced.into_iter().rev() {
            for (i, sector) in old.chunks(SECTOR_SIZE).enumerate() {
                let lose = match half_all_or_nothing {
                    LOSE_EVERY_SECTOR => true,
                    KEEP_EVERY_SECTOR => false,
                    _ => prng.bool(),
                };
                if lose {
                    let start = Self::at(offset) + i * SECTOR_SIZE;
                    self.data[start..start + sector.len()].copy_from_slice(sector);
                }
            }
        }
    }

    /// Latent corruption of the sector containing `offset`.
    pub fn corrupt_sector(&mut self, offset: u64, prng: &mut Prng) {
        let start = (Self::at(offset) / SECTOR_SIZE) * SECTOR_SIZE;
        self.ensure(start + SECTOR_SIZE);
        if self.in_undo_log(start, start + SECTOR_SIZE) {
            return;
        }
        let at = start + prng.index(SECTOR_SIZE);
        self.data[at] ^= 1 << prng.below(8);
        self.sectors_corrupted += 1;
    }

    fn in_undo_log(&self, start: usize, end: usize) -> bool {
        self.undo_log_since_sync.iter().any(|(o, old)| Self::at(*o) < end && start < Self::at(*o) + old.len())
    }
}

impl Device for SimDevice {
    fn read(&mut self, offset: u64, buf: &mut [u8]) {
        self.reads += 1;
        let start = Self::at(offset);
        let have = self.data.len().saturating_sub(start).min(buf.len());
        if have > 0 {
            buf[..have].copy_from_slice(&self.data[start..start + have]);
        }
        buf[have..].fill(0);
    }

    fn write(&mut self, offset: u64, data: &[u8]) {
        let start = Self::at(offset);
        assert!(
            start.is_multiple_of(SECTOR_SIZE) && data.len().is_multiple_of(SECTOR_SIZE),
            "write at {offset} of {} bytes is not aligned to the {SECTOR_SIZE}-byte sector size",
            data.len()
        );
        self.writes += 1;
        self.ensure(start + data.len());
        self.undo_log_since_sync.push((offset, self.data[start..start + data.len()].to_vec()));
        self.data[start..start + data.len()].copy_from_slice(data);
    }

    fn sync(&mut self) {
        self.syncs += 1;
        self.undo_log_since_sync.clear();
    }
}
