//! Viewstamped Replication for one shard.
//!
//! A [`Replica`] is a deterministic state machine: it is driven only by
//! [`Replica::on_packet`] and [`Replica::tick`], reads no clock, and talks to
//! the world by queuing packets and by synchronous calls on its block
//! device. The simulator and the production runtime drive the same code.
//!
//! The protocol follows VSR as refined by TigerBeetle, simplified where
//! synchronous storage allows:
//!
//! - **Log**: hash-chained 128-byte headers ([`header::Header`]); a replica's
//!   durable log is always a contiguous chain above its checkpoint.
//! - **WAL** ([`wal`]): prepares plus a redundant header ring, so recovery
//!   can tell a torn append (never acknowledged) from a lost prepare.
//! - **View change**: votes do not change the view until a quorum agrees;
//!   `log_view` and the canonical headers are durable before the log is
//!   touched; a new primary truncates an uncommitted op only on a quorum of
//!   nacks, and never resumes after a restart.
//! - **Repair**: everything is fetched by checksum walking the hash chain
//!   down from a trusted header, so any peer can serve and every answer is
//!   self-verifying.
//! - **Checkpoints and state sync**: the state machine's storage is
//!   physically deterministic ([`wfe_lsm`]), so a checkpoint is named by one
//!   checksum, damaged blocks are copied from peers, and a replica that falls
//!   off the WAL adopts a peer's checkpoint.

pub mod header;
mod le;
pub mod message;
pub mod replica;
pub mod superblock;
pub mod wal;

pub use header::{Header, HeaderFields, Kind};
pub use message::{CheckpointInfo, Message, Packet, Reply, Request};
pub use replica::{Counters, Event, OpenError, Replica, ReplicaStatus, Status};
pub use superblock::Layout;

use wfe_core::device::Device;
use wfe_core::{Addr, CellId, ReplicaIndex, ShardId, Timestamp};
use wfe_lsm::{Lsm, LsmOptions, StorageError};

/// A key and its value, as returned by [`Store::scan`].
pub type KeyValue = (Vec<u8>, Vec<u8>);

const _: () = assert!(usize::BITS >= u64::BITS, "wfe-vsr indexes buffers with u64 offsets; it needs a 64-bit target");

pub(crate) fn usize_from(n: impl Into<u64>) -> usize {
    usize::try_from(n.into()).expect("usize is at least 64 bits wide, asserted at compile time above")
}

/// The state machine's view of storage during one operation.
pub struct Store<'a> {
    pub lsm: &'a mut Lsm,
    pub dev: &'a mut dyn Device,
}

impl Store<'_> {
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, StorageError> {
        self.lsm.get(self.dev, key)
    }

    pub fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<KeyValue>, StorageError> {
        self.lsm.scan(self.dev, start, end, limit, reverse)
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), StorageError> {
        self.lsm.put(key, value)
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.lsm.delete(key)
    }

    pub fn max_entry_size(&self) -> usize {
        self.lsm.max_entry_size()
    }
}

/// Where an operation sits in the log.
#[derive(Clone, Copy, Debug)]
pub struct OpContext {
    pub op: u64,
    /// Position of the request within its prepare.
    pub index: u32,
    /// The prepare's timestamp: the state machine's only clock.
    pub timestamp: Timestamp,
    pub request_id: u128,
}

/// Things a state machine asks its replica to do outside the log.
pub struct Effects<P> {
    /// Packets for other processes.
    pub send: Vec<(Addr, P)>,
    /// Operations to propose to this shard (the replica must be primary;
    /// otherwise they are dropped and the state machine will retry).
    pub propose: Vec<Vec<u8>>,
}

impl<P> Default for Effects<P> {
    fn default() -> Self {
        Effects { send: Vec::new(), propose: Vec::new() }
    }
}

/// The deterministic application replicated by a shard.
///
/// `apply` and `pulse` run on every replica, in log order, and must depend
/// only on their arguments and on storage. A storage error aborts the
/// operation: the replica repairs its storage, reopens the last checkpoint
/// (calling `open` again) and replays.
pub trait StateMachine {
    /// The state machine's own wire protocol (to gateways, other shards...).
    type Packet;

    /// Reset volatile state from storage.
    fn open(&mut self, store: &mut Store) -> Result<(), StorageError>;

    /// Execute one committed request and return its reply.
    fn apply(&mut self, store: &mut Store, ctx: &OpContext, body: &[u8]) -> Result<Vec<u8>, StorageError>;

    /// Time has advanced to `ctx.timestamp`: run whatever is due.
    fn pulse(&mut self, store: &mut Store, ctx: &OpContext) -> Result<(), StorageError>;

    /// The next time `pulse` has work to do, as of the last committed op.
    fn pulse_at(&self) -> Option<Timestamp>;

    /// Called regularly on the primary once it has applied everything it
    /// knows to be committed: perform side effects recorded in storage.
    fn drive(
        &mut self,
        _store: &mut Store,
        _now: Timestamp,
        _fx: &mut Effects<Self::Packet>,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    /// A packet addressed to the state machine on this replica.
    fn on_packet(
        &mut self,
        _store: Option<&mut Store>,
        _primary: bool,
        _from: Addr,
        _packet: Self::Packet,
        _now: Timestamp,
        _fx: &mut Effects<Self::Packet>,
    ) -> Result<(), StorageError> {
        Ok(())
    }
}

/// Timeouts, in ticks. `Timing::default()` is sized for a 10 ms tick.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Timing {
    /// Primary heartbeat interval.
    pub heartbeat: u64,
    /// A backup suspects the primary after this long without hearing from it...
    pub election: u64,
    /// ...plus a random amount up to this.
    pub election_jitter: u64,
    /// Resend interval for votes, view change messages and repair requests.
    pub resend: u64,
    /// Give up on a view that has not started after this long.
    pub view_change: u64,
    /// Primary resends its oldest unacknowledged prepare this often.
    pub prepare_resend: u64,
    /// A primary that gets no acknowledgements for this long stops leading.
    pub abdicate: u64,
    /// How often the primary runs the state machine's `drive`.
    pub drive: u64,
}

impl Default for Timing {
    fn default() -> Timing {
        Timing {
            heartbeat: 10,
            election: 100,
            election_jitter: 50,
            resend: 20,
            view_change: 300,
            prepare_resend: 25,
            abdicate: 300,
            drive: 1,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    /// Random id shared by the replicas of one shard; guards against
    /// cross-wired clusters and data files.
    pub cluster: u128,
    pub cell: CellId,
    pub shard: ShardId,
    pub replica: ReplicaIndex,
    pub replica_count: u8,
    pub layout: Layout,
    pub lsm: LsmOptions,
    /// Checkpoint every this many ops.
    pub checkpoint_interval: u64,
    /// Most prepares that may be uncommitted at once.
    pub pipeline_max: u64,
    /// Most requests queued at the primary.
    pub request_queue_max: usize,
    pub timing: Timing,
}

impl ReplicaConfig {
    pub fn validate(&self) {
        self.layout.validate();
        assert!(self.replica_count >= 1 && self.replica < self.replica_count);
        assert!(self.replica_count <= 16, "at most 16 replicas per shard");
        assert!(self.pipeline_max >= 1 && self.checkpoint_interval >= 1);
        assert!(
            self.checkpoint_interval + 2 * self.pipeline_max + 2 <= u64::from(self.layout.slot_count),
            "the WAL must hold a checkpoint interval plus the pipeline"
        );
        assert!(
            (self.pipeline_max + 1) * 320 + 4096 <= superblock::SUPERBLOCK_COPY_SIZE,
            "pipeline_max is too large for the superblock"
        );
    }

    pub fn addr(&self, replica: ReplicaIndex) -> Addr {
        Addr::replica(self.cell, self.shard, replica)
    }
}
