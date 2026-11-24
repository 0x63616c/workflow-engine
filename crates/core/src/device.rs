//! Block device abstraction.
//!
//! A shard replica stores everything in one device (a preallocated file in
//! production, an in-memory fault-injecting device in the simulator). The
//! interface is deliberately tiny and synchronous: sector-aligned positional
//! reads and writes, and `sync`.
//!
//! Contract:
//! - `read` never fails. A region that was never written, or that the medium
//!   lost or corrupted, reads back as arbitrary bytes. Callers must verify a
//!   checksum on everything they read.
//! - `write` is not durable until a later `sync` returns. After a crash, each
//!   unsynced write may have been applied fully, partially (torn at sector
//!   granularity), or not at all.
//! - Unrecoverable write/sync errors abort the process (retrying a failed
//!   fsync is never safe).

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const SECTOR_SIZE: usize = 4096;

pub fn is_sector_aligned(n: u64) -> bool {
    n.is_multiple_of(SECTOR_SIZE as u64)
}

/// Round `n` up to a whole number of sectors.
pub fn sector_ceil(n: u64) -> u64 {
    n.div_ceil(SECTOR_SIZE as u64) * SECTOR_SIZE as u64
}

pub trait Device {
    /// Fill `buf` from `offset`. Offset and length are sector-aligned.
    fn read(&mut self, offset: u64, buf: &mut [u8]);
    /// Write `data` at `offset`. Offset and length are sector-aligned.
    fn write(&mut self, offset: u64, data: &[u8]);
    /// Make all completed writes durable.
    fn sync(&mut self);
}

impl<D: Device + ?Sized> Device for &mut D {
    fn read(&mut self, offset: u64, buf: &mut [u8]) {
        (**self).read(offset, buf)
    }
    fn write(&mut self, offset: u64, data: &[u8]) {
        (**self).write(offset, data)
    }
    fn sync(&mut self) {
        (**self).sync()
    }
}

impl<D: Device + ?Sized> Device for Box<D> {
    fn read(&mut self, offset: u64, buf: &mut [u8]) {
        (**self).read(offset, buf)
    }
    fn write(&mut self, offset: u64, data: &[u8]) {
        (**self).write(offset, data)
    }
    fn sync(&mut self) {
        (**self).sync()
    }
}

/// A fault-free in-memory device, for unit tests and tools.
#[derive(Clone, Default)]
pub struct MemDevice {
    data: Vec<u8>,
    pub reads: u64,
    pub writes: u64,
    pub syncs: u64,
}

impl MemDevice {
    pub fn new() -> MemDevice {
        MemDevice::default()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn bytes_mut(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }
}

impl Device for MemDevice {
    fn read(&mut self, offset: u64, buf: &mut [u8]) {
        debug_assert!(is_sector_aligned(offset) && is_sector_aligned(buf.len() as u64));
        self.reads += 1;
        let start = usize::try_from(offset).expect("a MemDevice offset fits in memory, so it fits in usize");
        let have = self.data.len().saturating_sub(start).min(buf.len());
        if have > 0 {
            buf[..have].copy_from_slice(&self.data[start..start + have]);
        }
        buf[have..].fill(0);
    }

    fn write(&mut self, offset: u64, data: &[u8]) {
        debug_assert!(is_sector_aligned(offset) && is_sector_aligned(data.len() as u64));
        self.writes += 1;
        let start = usize::try_from(offset).expect("a MemDevice offset fits in memory, so it fits in usize");
        let end = start + data.len();
        if self.data.len() < end {
            self.data.resize(end, 0);
        }
        self.data[start..end].copy_from_slice(data);
    }

    fn sync(&mut self) {
        self.syncs += 1;
    }
}

/// A real file. Reads that fail (EIO, short read past EOF) come back zeroed
/// and are caught by the caller's checksum; write and sync failures abort.
/// `sync` is durable on Apple platforms too: std's `sync_data` issues
/// `F_FULLFSYNC` there.
pub struct FileDevice {
    file: File,
}

impl FileDevice {
    #[expect(
        clippy::disallowed_methods,
        reason = "FileDevice is the one Device backed by a real file; all other storage I/O goes through Device"
    )]
    pub fn open(path: &Path, create: bool) -> io::Result<FileDevice> {
        let file = OpenOptions::new().read(true).write(true).create(create).truncate(false).open(path)?;
        Ok(FileDevice { file })
    }

    pub fn exists(path: &Path) -> bool {
        path.is_file()
    }
}

impl Device for FileDevice {
    fn read(&mut self, offset: u64, buf: &mut [u8]) {
        let mut done = 0;
        while done < buf.len() {
            match self.file.read_at(&mut buf[done..], offset + done as u64) {
                Ok(0) => break,
                Ok(n) => done += n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => break,
            }
        }
        buf[done..].fill(0);
    }

    fn write(&mut self, offset: u64, data: &[u8]) {
        if let Err(e) = self.file.write_all_at(data, offset) {
            panic!("fatal: data file write failed at offset {offset}: {e}");
        }
    }

    fn sync(&mut self) {
        if let Err(e) = self.file.sync_data() {
            panic!("fatal: data file sync failed: {e}");
        }
    }
}
