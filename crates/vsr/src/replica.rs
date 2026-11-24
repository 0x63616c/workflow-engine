//! The replica: one member of a shard's consensus group.
//!
//! # The log
//!
//! `log` holds the headers of the replica's durable log, from its checkpoint
//! up to `op`. It is always a hash chain, and it is always *canonical as of
//! `log_view`*: a prefix of the log that the primary of `log_view` led with.
//! `chain` holds canonical headers above `op` that the replica knows of but
//! has not made durable yet (their prepares are missing or out of order).
//!
//! Everything else follows from keeping those two statements true:
//!
//! - prepares are appended only in order, onto a matching parent;
//! - anything in `log` at or below `commit_max` can be executed;
//! - a view change reports `log ∪ chain` and nothing else;
//! - when a new view's log is installed, whatever cannot be proven to be on
//!   it is dropped back to the committed prefix and fetched again.
//!
//! Violations of these invariants are bugs, and are asserted: a replica that
//! finds its log in conflict with the canonical log stops rather than guess.

use crate::header::{Header, HeaderFields, Kind};
use crate::message::{
    CheckpointInfo, Commit, DoViewChange, Held, Message, Packet, Prepare, RejectReason, Reply, Request, StartView,
};
use crate::superblock::{Superblock, SuperblockState};
use crate::wal::Wal;
use crate::{Effects, OpContext, ReplicaConfig, StateMachine, Store, usize_from};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use wfe_core::device::Device;
use wfe_core::{Addr, Id128, Prng, Timestamp};
use wfe_lsm::{BlockRef, Lsm, LsmStats, StorageError, missing_blocks, read_block_raw, write_block_raw};

const HEADERS_PER_MESSAGE: usize = 64;
const REPAIR_PREPARES: usize = 8;
const BLOCKS_PER_REQUEST: usize = 16;
const BLOCKS_WANTED: usize = 256;
const BATCH_REQUESTS_MAX: usize = 256;
const REQUEST_OVERHEAD: usize = 192;
const EVENTS_MAX: usize = 8192;

const REQUEST_HEADERS: u8 = 0;
const REQUEST_PREPARE: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Status {
    Normal,
    /// Waiting for a new view to start.
    ViewChange,
    /// The head of the log could not be determined after a crash: the replica
    /// cannot vouch for what it did or did not hold, so it sits out view
    /// changes until a primary tells it where the log ends.
    RecoveringHead,
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("the data file is not formatted")]
    NotFormatted,
    #[error("the data file belongs to a different replica: {0}")]
    Mismatch(String),
    #[error("unrecoverable data loss: {0}")]
    Unrecoverable(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Counters {
    pub prepares: u64,
    pub commits: u64,
    pub checkpoints: u64,
    pub view_changes: u64,
    pub requests_rejected: u64,
    pub wal_faults: u64,
    pub storage_faults: u64,
    pub state_syncs: u64,
    pub blocks_repaired: u64,
    pub prepares_repaired: u64,
}

/// A point-in-time description of a replica, for operators and checkers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaStatus {
    pub replica: u8,
    pub status: Status,
    pub primary: bool,
    pub view: u32,
    pub log_view: u32,
    pub op: u64,
    pub commit_min: u64,
    pub commit_max: u64,
    pub checkpoint_op: u64,
    pub checkpoint_id: Id128,
    /// Canonical ops known but not yet durable here.
    pub log_missing: u64,
    /// The state machine's storage is being repaired or synced.
    pub storage_repairing: bool,
    pub pipeline: u64,
    pub queue: u64,
    pub counters: Counters,
    pub storage: Option<LsmStats>,
}

/// Things that happened, for the simulator's checkers and for observability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Committed { op: u64, checksum: u128, timestamp: u64 },
    Checkpointed { op: u64, id: u128 },
    ViewChangeStarted { view: u32 },
    BecamePrimary { view: u32 },
    BecameBackup { view: u32 },
    SyncStarted { op: u64 },
    StorageOpened { op: u64 },
    WalFault { op: u64 },
    StorageFault,
}

pub struct Replica<S: StateMachine, D: Device> {
    config: ReplicaConfig,
    dev: D,
    sm: S,
    prng: Prng,
    superblock: Superblock,
    wal: Wal,
    lsm: Option<Lsm>,

    status: Status,
    view: u32,
    log_view: u32,
    head_op: u64,
    executed_op: u64,
    committed_op: u64,
    log: BTreeMap<u64, Header>,
    chain: BTreeMap<u64, Header>,
    header_repair_starts_from: Option<(u64, u128)>,
    prepares_ahead_of_turn: BTreeMap<u64, Prepare>,
    recent_prepare_bodies: BTreeMap<u64, (u128, Vec<u8>)>,

    pipeline_acks: BTreeMap<u64, u32>,
    queue: VecDeque<Request>,
    inflight: BTreeSet<u128>,
    pulse_pending: bool,
    internal_seq: u64,

    latest_vote_heard_from: Vec<(u32, u64)>,
    voting: bool,
    my_vote: u32,
    dvcs: BTreeMap<u8, DoViewChange>,
    elect_log_chosen_and_durable: bool,
    elect_committed_through: u64,

    wanted: BTreeMap<u64, BlockRef>,
    blocks_pending: usize,

    ticks: u64,
    now: Timestamp,
    heard_at: u64,
    election_timeout: u64,
    view_change_at: u64,
    heartbeat_at: u64,
    resend_at: u64,
    prepare_resend_at: u64,
    progress_at: u64,
    drive_at: u64,
    start_view_requested_at: u64,
    blocks_requested_at: u64,
    requested: BTreeMap<(u8, u64), u64>,

    outbox: Vec<(Addr, Packet<S::Packet>)>,
    events: Vec<Event>,
    counters: Counters,
}

#[derive(Debug)]
#[expect(dead_code, reason = "read only through Debug, in assertion messages")]
struct DvcSummary {
    replica: u8,
    log_view: u32,
    top_op: u64,
    commit_max: u64,
    headers: Vec<(u64, u32, Held)>,
}

fn valid_headers(cluster: u128, headers: &[Header]) -> bool {
    !headers.is_empty()
        && headers.iter().all(|h| h.valid() && h.cluster == cluster && h.kind != Kind::Reserved)
        && headers.windows(2).all(|w| w[1].op == w[0].op + 1 && w[1].parent == w[0].checksum)
}

fn dvc_shows_its_sender_never_acked(dvc: &DoViewChange, h: &Header) -> bool {
    if h.op > dvc.top_op {
        return true;
    }
    match dvc.headers.iter().find(|(x, _)| x.op == h.op) {
        Some((x, held)) => x.checksum != h.checksum || *held == Held::Nack,
        None => false,
    }
}

fn execute<S: StateMachine>(
    sm: &mut S,
    lsm: &mut Lsm,
    dev: &mut dyn Device,
    header: &Header,
    body: &[u8],
    replies: &mut Vec<(Addr, Reply)>,
) -> Result<(), StorageError> {
    let mut store = Store { lsm, dev };
    let timestamp = Timestamp(header.timestamp);
    match header.kind {
        Kind::Requests => {
            let requests: Vec<Request> = wfe_core::codec::decode(body).expect("a committed prepare must decode");
            for (index, request) in requests.into_iter().enumerate() {
                let index = u32::try_from(index).expect("a prepare holds at most BATCH_REQUESTS_MAX requests");
                let ctx = OpContext { op: header.op, index, timestamp, request_id: request.request_id };
                let body = sm.apply(&mut store, &ctx, &request.body)?;
                replies.push((request.client, Reply { request_id: request.request_id, body }));
            }
        }
        Kind::Pulse => {
            let ctx = OpContext { op: header.op, index: 0, timestamp, request_id: 0 };
            sm.pulse(&mut store, &ctx)?;
        }
        Kind::Root | Kind::Reserved => unreachable!("op {} is not executable", header.op),
    }
    store.lsm.maintain(store.dev)
}

impl<S: StateMachine, D: Device> Replica<S, D> {
    /// Initialize an empty data file. The superblock is written last, so a
    /// crash part way through leaves a file that still reads as unformatted.
    pub fn format(dev: &mut D, config: &ReplicaConfig) {
        config.validate();
        let root = Header::root(config.cluster);
        Wal::format(dev, &config.layout, config.cluster);
        Superblock::format(
            dev,
            SuperblockState {
                sequence: 0,
                cluster: config.cluster,
                replica: config.replica,
                replica_count: config.replica_count,
                layout: config.layout,
                view: 0,
                log_view: 0,
                view_headers: vec![root],
                checkpoint: CheckpointInfo { header: root, lsm: None },
            },
        );
    }

    pub fn open(mut dev: D, config: ReplicaConfig, sm: S, seed: u64, now: Timestamp) -> Result<Self, OpenError> {
        config.validate();
        let superblock = Superblock::open(&mut dev).ok_or(OpenError::NotFormatted)?;
        {
            let st = &superblock.state;
            if st.cluster != config.cluster
                || st.replica != config.replica
                || st.replica_count != config.replica_count
                || st.layout != config.layout
            {
                return Err(OpenError::Mismatch(format!(
                    "file is replica {}/{} of cluster {:032x}, expected {}/{} of {:032x}",
                    st.replica, st.replica_count, st.cluster, config.replica, config.replica_count, config.cluster
                )));
            }
        }
        let wal = Wal::recover(&mut dev, &config.layout, config.cluster);
        let replica_count = config.replica_count as usize;
        let mut replica = Replica {
            view: superblock.state.view,
            log_view: superblock.state.log_view,
            config,
            dev,
            sm,
            prng: Prng::new(seed),
            superblock,
            wal,
            lsm: None,
            status: Status::Normal,
            head_op: 0,
            executed_op: 0,
            committed_op: 0,
            log: BTreeMap::new(),
            chain: BTreeMap::new(),
            header_repair_starts_from: None,
            prepares_ahead_of_turn: BTreeMap::new(),
            recent_prepare_bodies: BTreeMap::new(),
            pipeline_acks: BTreeMap::new(),
            queue: VecDeque::new(),
            inflight: BTreeSet::new(),
            pulse_pending: false,
            internal_seq: 0,
            latest_vote_heard_from: vec![(0, 0); replica_count],
            voting: false,
            my_vote: 0,
            dvcs: BTreeMap::new(),
            elect_log_chosen_and_durable: false,
            elect_committed_through: 0,
            wanted: BTreeMap::new(),
            blocks_pending: 0,
            ticks: 0,
            now,
            heard_at: 0,
            election_timeout: 0,
            view_change_at: 0,
            heartbeat_at: 0,
            resend_at: 0,
            prepare_resend_at: 0,
            progress_at: 0,
            drive_at: 0,
            start_view_requested_at: 0,
            blocks_requested_at: 0,
            requested: BTreeMap::new(),
            outbox: Vec::new(),
            events: Vec::new(),
            counters: Counters::default(),
        };
        let uncertain = replica.recover_log_reporting_whether_head_is_uncertain();
        if replica_count == 1 && !replica.chain.is_empty() {
            return Err(OpenError::Unrecoverable(format!(
                "prepare {} is damaged and there is no other replica to repair it from",
                replica.head_op + 1
            )));
        }
        let opened = replica.open_storage();
        if replica_count == 1 && !opened {
            return Err(OpenError::Unrecoverable("the checkpoint is damaged and there is no other replica".into()));
        }
        replica.recover_status(uncertain);
        replica.execute_committed_ops();
        Ok(replica)
    }

    fn recover_log_reporting_whether_head_is_uncertain(&mut self) -> bool {
        let st = &self.superblock.state;
        let checkpoint = st.checkpoint.header;
        let view_headers: BTreeMap<u64, Header> =
            st.view_headers.iter().filter(|h| h.op > checkpoint.op).map(|h| (h.op, *h)).collect();
        let view_top = st.view_headers.last().map_or(0, |h| h.op);
        let limit = checkpoint.op + self.wal.slot_count();

        let mut walked_up_from_checkpoint: Vec<(Header, bool)> = Vec::new();
        let mut cur = checkpoint;
        while cur.op < limit {
            let n = cur.op + 1;
            let slot = *self.wal.slot(n);
            let durable = slot.disk.filter(|h| h.op == n);
            let lost_but_header_survives_in_ring = slot.lost.filter(|h| h.op == n);
            let Some(h) = durable.or(lost_but_header_survives_in_ring) else { break };
            if h.parent != cur.checksum {
                break;
            }
            let leftover_of_a_truncated_older_view = n > view_top && h.view != st.log_view;
            let conflicts_with_log_view_headers =
                n <= view_top && view_headers.get(&n).is_some_and(|v| v.checksum != h.checksum);
            if leftover_of_a_truncated_older_view || conflicts_with_log_view_headers {
                break;
            }
            walked_up_from_checkpoint.push((h, durable.is_some()));
            cur = h;
        }
        let uncertain = cur.op < limit && self.wal.slot(cur.op + 1).uncertain;

        let walk_reaches_the_headers_log_view_began_with = match view_headers.first_key_value() {
            None => true,
            Some((&first, h)) => cur.op >= first || (cur.op + 1 == first && h.parent == cur.checksum),
        };

        self.log.insert(checkpoint.op, checkpoint);
        self.head_op = checkpoint.op;
        if walk_reaches_the_headers_log_view_began_with {
            let mut contiguous = true;
            for (h, durable) in walked_up_from_checkpoint {
                if contiguous && durable {
                    self.log.insert(h.op, h);
                    self.head_op = h.op;
                } else {
                    contiguous = false;
                    self.chain.insert(h.op, h);
                }
            }
        }
        for (op, h) in view_headers {
            if op > self.head_op {
                self.chain.entry(op).or_insert(h);
            }
        }
        self.executed_op = checkpoint.op;
        self.committed_op = checkpoint.op.max(self.head_op.saturating_sub(self.config.pipeline_max));
        uncertain
    }

    fn recover_status(&mut self, uncertain: bool) {
        self.arm_election();
        let alone_so_durable_means_committed = self.config.replica_count == 1;
        if alone_so_durable_means_committed {
            self.status = Status::Normal;
            self.committed_op = self.head_op;
            return;
        }
        if uncertain {
            self.status = Status::RecoveringHead;
        } else if self.view > self.log_view {
            self.status = Status::ViewChange;
        } else if self.primary_of(self.view) == self.config.replica {
            self.move_past_the_view_led_before_restart();
        } else {
            self.status = Status::Normal;
        }
    }

    fn move_past_the_view_led_before_restart(&mut self) {
        self.view += 1;
        self.superblock.state.view = self.view;
        self.superblock.write(&mut self.dev);
        self.status = Status::ViewChange;
    }

    fn open_storage(&mut self) -> bool {
        let checkpoint = self.superblock.state.checkpoint;
        let grid = self.config.layout.grid();
        let mut lsm = match Lsm::open(&mut self.dev, grid, self.config.lsm.clone(), checkpoint.lsm) {
            Ok(lsm) => lsm,
            Err(StorageError::Fault(_)) => return false,
            Err(e) => panic!("replica {}: cannot open storage: {e}", self.config.replica),
        };
        match self.sm.open(&mut Store { lsm: &mut lsm, dev: &mut self.dev }) {
            Ok(()) => {}
            Err(StorageError::Fault(_)) => return false,
            Err(e) => panic!("replica {}: cannot open the state machine: {e}", self.config.replica),
        }
        self.lsm = Some(lsm);
        self.executed_op = checkpoint.op();
        self.emit(Event::StorageOpened { op: checkpoint.op() });
        true
    }

    pub fn config(&self) -> &ReplicaConfig {
        &self.config
    }

    pub fn device(&self) -> &D {
        &self.dev
    }

    pub fn device_mut(&mut self) -> &mut D {
        &mut self.dev
    }

    /// Give the device back, as a crash would.
    pub fn into_device(self) -> D {
        self.dev
    }

    pub fn state_machine(&self) -> &S {
        &self.sm
    }

    pub fn state_machine_mut(&mut self) -> &mut S {
        &mut self.sm
    }

    /// Run `f` against the state machine and its storage as of `commit_min`.
    pub fn with_store<R>(&mut self, f: impl FnOnce(&mut S, &mut Store) -> R) -> Option<R> {
        let lsm = self.lsm.as_mut()?;
        Some(f(&mut self.sm, &mut Store { lsm, dev: &mut self.dev }))
    }

    /// Reads through the store. A read that hits a damaged block starts
    /// the same repair from the other replicas as a damaged block found
    /// while applying an operation, and yields `None`.
    pub fn read_store<R>(&mut self, f: impl FnOnce(&mut S, &mut Store) -> Result<R, StorageError>) -> Option<R> {
        let lsm = self.lsm.as_mut()?;
        match f(&mut self.sm, &mut Store { lsm, dev: &mut self.dev }) {
            Ok(read) => Some(read),
            Err(StorageError::Fault(_)) => {
                self.storage_fault();
                None
            }
            Err(e) => panic!("replica {}: reading the store failed: {e}", self.config.replica),
        }
    }

    pub fn checkpoint(&self) -> CheckpointInfo {
        self.superblock.state.checkpoint
    }

    pub fn is_primary(&self) -> bool {
        self.status == Status::Normal && self.primary_of(self.view) == self.config.replica
    }

    pub fn view(&self) -> u32 {
        self.view
    }

    /// The header of a log entry this replica holds durably.
    pub fn log_header(&self, op: u64) -> Option<Header> {
        self.log.get(&op).copied()
    }

    pub fn status(&self) -> ReplicaStatus {
        let checkpoint = &self.superblock.state.checkpoint;
        ReplicaStatus {
            replica: self.config.replica,
            status: self.status,
            primary: self.is_primary(),
            view: self.view,
            log_view: self.log_view,
            op: self.head_op,
            commit_min: self.executed_op,
            commit_max: self.committed_op,
            checkpoint_op: checkpoint.op(),
            checkpoint_id: Id128(checkpoint.id()),
            log_missing: self.chain.len() as u64,
            storage_repairing: self.lsm.is_none(),
            pipeline: self.pipeline_acks.len() as u64,
            queue: self.queue.len() as u64,
            counters: self.counters.clone(),
            storage: self.lsm.as_ref().map(|lsm| lsm.stats()),
        }
    }

    pub fn take_outbox(&mut self) -> Vec<(Addr, Packet<S::Packet>)> {
        std::mem::take(&mut self.outbox)
    }

    pub fn take_events(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }

    fn emit(&mut self, event: Event) {
        if self.events.len() >= EVENTS_MAX {
            self.events.drain(..EVENTS_MAX / 2);
        }
        self.events.push(event);
    }

    fn me(&self) -> u8 {
        self.config.replica
    }

    fn replica_count(&self) -> usize {
        self.config.replica_count as usize
    }

    fn majority(&self) -> usize {
        self.replica_count() / 2 + 1
    }

    fn nack_quorum(&self) -> usize {
        self.replica_count() - self.majority() + 1
    }

    fn primary_of(&self, view: u32) -> u8 {
        u8::try_from(view % u32::from(self.config.replica_count))
            .expect("a remainder of the u8 replica_count fits in a u8")
    }

    fn head(&self) -> Header {
        self.log[&self.head_op]
    }

    fn checkpoint_op(&self) -> u64 {
        self.superblock.state.checkpoint.op()
    }

    fn leading(&self) -> bool {
        self.is_primary() && !self.voting
    }

    fn send(&mut self, to: u8, message: Message) {
        if to != self.config.replica {
            self.outbox.push((self.config.addr(to), Packet::Vsr(message)));
        }
    }

    fn broadcast(&mut self, message: Message) {
        for to in 0..self.config.replica_count {
            if to != self.config.replica {
                self.outbox.push((self.config.addr(to), Packet::Vsr(message.clone())));
            }
        }
    }

    fn random_peer(&mut self) -> u8 {
        let other = u8::try_from(self.prng.below(u64::from(self.config.replica_count) - 1))
            .expect("below(replica_count - 1) is less than the u8 replica_count");
        if other >= self.config.replica { other + 1 } else { other }
    }

    fn arm_election(&mut self) {
        let t = &self.config.timing;
        self.election_timeout = t.election + self.prng.below(t.election_jitter + 1);
        self.heard_at = self.ticks;
    }

    pub fn on_packet(&mut self, from: Addr, packet: Packet<S::Packet>, now: Timestamp) {
        self.now = now;
        match packet {
            Packet::App(packet) => self.on_app(from, packet),
            Packet::Vsr(Message::Request(request)) => self.on_request(request),
            Packet::Vsr(message) => {
                let Addr::Replica { cell, shard, replica } = from else { return };
                if cell != self.config.cell
                    || shard != self.config.shard
                    || replica >= self.config.replica_count
                    || replica == self.config.replica
                {
                    return;
                }
                self.on_message(replica, message);
            }
        }
    }

    fn on_message(&mut self, from: u8, message: Message) {
        match message {
            Message::Request(_) | Message::Reply(_) | Message::Reject { .. } => {}
            Message::Prepare(prepare) => self.on_prepare(from, prepare),
            Message::PrepareOk { view, op, checksum } => self.on_prepare_ok(from, view, op, checksum),
            Message::Commit(commit) => self.on_commit(from, commit),
            Message::StartViewChange { view, joined } => self.on_start_view_change(from, view, joined),
            Message::DoViewChange(dvc) => self.on_do_view_change(from, dvc),
            Message::StartView(sv) => self.on_start_view(from, sv),
            Message::RequestStartView { view } => {
                if self.is_primary() && self.view >= view {
                    self.send_start_view(from);
                }
            }
            Message::RequestHeaders { op, checksum, floor } => self.on_request_headers(from, op, checksum, floor),
            Message::Headers { headers } => self.on_headers(headers),
            Message::RequestPrepare { op, checksum } => self.on_request_prepare(from, op, checksum),
            Message::RequestBlocks { blocks } => self.on_request_blocks(from, blocks),
            Message::Block { block, bytes } => self.on_block(block, bytes),
            Message::RequestSyncTarget => {
                if self.lsm.is_some() {
                    let message = self.sync_target_message();
                    self.send(from, message);
                }
            }
            Message::SyncTarget { view, checkpoint } => self.on_sync_target(view, checkpoint),
        }
    }

    pub fn tick(&mut self, now: Timestamp) {
        self.now = now;
        self.ticks += 1;
        let t = self.config.timing.clone();
        if self.replica_count() > 1 {
            match self.status {
                Status::Normal if self.is_primary() => {
                    if !self.voting && self.ticks >= self.heartbeat_at {
                        self.heartbeat_at = self.ticks + t.heartbeat;
                        let message = self.commit_message();
                        self.broadcast(message);
                    }
                    if self.head_op > self.committed_op {
                        if self.ticks >= self.prepare_resend_at {
                            self.prepare_resend_at = self.ticks + t.prepare_resend;
                            self.resend_prepares();
                        }
                        let nobody_acknowledges_so_let_backups_elect_someone_heard =
                            !self.voting && self.is_primary() && self.ticks >= self.progress_at + t.abdicate;
                        if nobody_acknowledges_so_let_backups_elect_someone_heard {
                            self.start_vote();
                        }
                    }
                }
                Status::Normal => {
                    if !self.voting && self.ticks >= self.heard_at + self.election_timeout {
                        self.start_vote();
                    }
                }
                Status::ViewChange => {
                    if self.ticks >= self.view_change_at + t.view_change + self.election_timeout - t.election {
                        self.view_change_at = self.ticks;
                        self.start_vote();
                    }
                }
                Status::RecoveringHead => {}
            }
            if self.ticks >= self.resend_at {
                self.resend_at = self.ticks + t.resend;
                if self.voting {
                    self.broadcast(Message::StartViewChange { view: self.my_vote, joined: false });
                }
                if self.status == Status::ViewChange {
                    self.broadcast(Message::StartViewChange { view: self.view, joined: true });
                }
                match self.status {
                    Status::ViewChange => {
                        self.send_do_view_change();
                        self.try_elect();
                    }
                    Status::RecoveringHead => self.broadcast(Message::RequestStartView { view: self.view }),
                    Status::Normal => {}
                }
                let now = self.ticks;
                self.requested.retain(|_, at| *at + t.resend > now);
                self.repair();
                self.repair_storage();
            }
            self.check_votes();
        }
        if self.leading() {
            self.prepare_queued_work_while_pipeline_has_room();
            if self.ticks >= self.drive_at {
                self.drive_at = self.ticks + t.drive;
                self.drive();
            }
        }
    }

    fn on_app(&mut self, from: Addr, packet: S::Packet) {
        let primary = self.leading();
        let mut fx = Effects::default();
        let result = match self.lsm.as_mut() {
            Some(lsm) => {
                let mut store = Store { lsm, dev: &mut self.dev };
                self.sm.on_packet(Some(&mut store), primary, from, packet, self.now, &mut fx)
            }
            None => self.sm.on_packet(None, primary, from, packet, self.now, &mut fx),
        };
        self.finish_effects(result, fx);
    }

    fn drive(&mut self) {
        if self.executed_op < self.committed_op {
            return;
        }
        let Some(lsm) = self.lsm.as_mut() else { return };
        let mut fx = Effects::default();
        let result = self.sm.drive(&mut Store { lsm, dev: &mut self.dev }, self.now, &mut fx);
        self.finish_effects(result, fx);
    }

    fn finish_effects(&mut self, result: Result<(), StorageError>, fx: Effects<S::Packet>) {
        match result {
            Ok(()) => {}
            Err(StorageError::Fault(_)) => return self.storage_fault(),
            Err(e) => panic!("replica {}: state machine failed: {e}", self.config.replica),
        }
        for (to, packet) in fx.send {
            self.outbox.push((to, Packet::App(packet)));
        }
        if self.leading() && !fx.propose.is_empty() {
            for body in fx.propose {
                let queue_full_and_the_state_machine_proposes_again_later =
                    self.queue.len() >= self.config.request_queue_max * 2;
                if queue_full_and_the_state_machine_proposes_again_later {
                    break;
                }
                self.internal_seq += 1;
                let request_id = (u128::from(self.view) << 96)
                    | (u128::from(self.config.replica) << 88)
                    | u128::from(self.internal_seq);
                self.queue.push_back(Request { request_id, client: self.config.addr(self.config.replica), body });
            }
            self.prepare_queued_work_while_pipeline_has_room();
        }
    }

    fn reject(&mut self, request: &Request, reason: RejectReason) {
        self.counters.requests_rejected += 1;
        let message = Message::Reject { request_id: request.request_id, view: self.view, reason };
        self.outbox.push((request.client, Packet::Vsr(message)));
    }

    fn on_request(&mut self, request: Request) {
        if !self.leading() {
            return self.reject(&request, RejectReason::NotPrimary);
        }
        if self.inflight.contains(&request.request_id) {
            return;
        }
        let size = wfe_core::codec::encode(std::slice::from_ref(&request)).len();
        if size > self.config.layout.body_size_max() {
            return self.reject(&request, RejectReason::TooLarge);
        }
        if self.queue.len() >= self.config.request_queue_max {
            return self.reject(&request, RejectReason::Busy);
        }
        self.inflight.insert(request.request_id);
        self.queue.push_back(request);
        self.prepare_queued_work_while_pipeline_has_room();
    }

    fn can_prepare(&self) -> bool {
        let uncommitted_stays_within_pipeline_max = self.head_op - self.executed_op < self.config.pipeline_max;
        let wal_has_room_before_the_next_checkpoint = self.head_op < self.checkpoint_op() + self.wal.slot_count();
        uncommitted_stays_within_pipeline_max && wal_has_room_before_the_next_checkpoint
    }

    fn prepare_queued_work_while_pipeline_has_room(&mut self) {
        while self.leading() && self.chain.is_empty() && self.can_prepare() {
            if !self.pulse_pending && self.lsm.is_some() && self.sm.pulse_at().is_some_and(|at| at <= self.now) {
                self.pulse_pending = true;
                self.prepare(Kind::Pulse, Vec::new());
                continue;
            }
            if self.queue.is_empty() {
                break;
            }
            let max = self.config.layout.body_size_max();
            let mut batch: Vec<Request> = Vec::new();
            let mut size = 16;
            while let Some(request) = self.queue.front() {
                let cost = request.body.len() + REQUEST_OVERHEAD;
                if !batch.is_empty() && (size + cost > max || batch.len() >= BATCH_REQUESTS_MAX) {
                    break;
                }
                size += cost;
                batch.push(self.queue.pop_front().expect("the loop condition just saw a request at the front"));
            }
            let mut body = wfe_core::codec::encode(&batch);
            while body.len() > max {
                let size_estimate_was_off =
                    batch.pop().expect("one request always fits a prepare: on_request rejects larger ones as TooLarge");
                self.queue.push_front(size_estimate_was_off);
                body = wfe_core::codec::encode(&batch);
            }
            self.prepare(Kind::Requests, body);
        }
    }

    fn prepare(&mut self, kind: Kind, body: Vec<u8>) {
        let head = self.head();
        let op = self.head_op + 1;
        let timestamp = (head.timestamp + 1).max(self.now.micros());
        let fields = HeaderFields {
            cluster: self.config.cluster,
            view: self.view,
            op,
            parent: head.checksum,
            commit: self.committed_op,
            timestamp,
            kind,
            replica: self.config.replica,
        };
        let header = Header::new(fields, &body);
        self.wal.append(&mut self.dev, &header, &body);
        if self.head_op == self.committed_op {
            self.progress_at = self.ticks;
            self.prepare_resend_at = self.ticks + self.config.timing.prepare_resend;
        }
        self.log.insert(op, header);
        self.head_op = op;
        self.pipeline_acks.insert(op, 1 << self.config.replica);
        self.counters.prepares += 1;
        self.broadcast(Message::Prepare(Prepare { header, body: body.clone() }));
        self.cache_body(op, header.checksum, body);
        self.advance_commit();
    }

    fn on_prepare_ok(&mut self, from: u8, view: u32, op: u64, checksum: u128) {
        if !self.is_primary() || view != self.view {
            return;
        }
        self.heard_from_a_backup_that_hears_us();
        if self.log.get(&op).is_none_or(|h| h.checksum != checksum) {
            return;
        }
        if let Some(acks) = self.pipeline_acks.get_mut(&op) {
            *acks |= 1 << from;
        }
        self.advance_commit();
        self.prepare_queued_work_while_pipeline_has_room();
    }

    fn heard_from_a_backup_that_hears_us(&mut self) {
        self.progress_at = self.ticks;
        self.voting = false;
    }

    fn advance_commit(&mut self) {
        let majority = self.majority();
        while self
            .pipeline_acks
            .get(&(self.committed_op + 1))
            .is_some_and(|acks| acks.count_ones() as usize >= majority)
        {
            self.committed_op += 1;
        }
        self.execute_committed_ops();
    }

    fn commit_message(&self) -> Message {
        let checkpoint = &self.superblock.state.checkpoint;
        Message::Commit(Commit {
            view: self.view,
            commit: self.committed_op,
            commit_checksum: self.log[&self.committed_op].checksum,
            checkpoint_op: checkpoint.op(),
            checkpoint_id: checkpoint.id(),
        })
    }

    fn resend_prepares(&mut self) {
        let pending: Vec<(u64, u32)> =
            self.pipeline_acks.range(self.committed_op + 1..).take(4).map(|(&op, &acks)| (op, acks)).collect();
        for (op, acks) in pending {
            let header = self.log[&op];
            let Some(body) = self.body_of(&header) else { return self.wal_fault(op) };
            for to in 0..self.config.replica_count {
                if acks & (1 << to) == 0 {
                    self.send(to, Message::Prepare(Prepare { header, body: body.clone() }));
                }
            }
        }
    }

    fn heard_from_primary(&mut self) {
        self.heard_at = self.ticks;
        self.voting = false;
        self.my_vote = self.view;
    }

    fn valid_prepare(&self, prepare: &Prepare) -> bool {
        let h = &prepare.header;
        h.valid()
            && h.cluster == self.config.cluster
            && matches!(h.kind, Kind::Requests | Kind::Pulse)
            && h.size <= self.config.layout.message_size_max
            && h.valid_body(&prepare.body)
    }

    fn on_prepare(&mut self, from: u8, prepare: Prepare) {
        if !self.valid_prepare(&prepare) {
            return;
        }
        let h = prepare.header;
        let body_of_a_known_canonical_header_from_anyone =
            self.chain.get(&h.op).is_some_and(|c| c.checksum == h.checksum);
        if body_of_a_known_canonical_header_from_anyone {
            self.counters.prepares_repaired += 1;
            self.stash_put(prepare);
            self.progress();
            return;
        }
        let a_view_we_have_not_joined_has_started = h.view > self.view;
        if a_view_we_have_not_joined_has_started {
            return self.request_start_view(h.view);
        }
        if self.status != Status::Normal || from != self.primary_of(self.view) {
            if self.status != Status::Normal && h.view == self.view && from == self.primary_of(self.view) {
                self.request_start_view(self.view);
            }
            return;
        }
        let already_held = h.op <= self.head_op;
        if already_held {
            match self.log.get(&h.op) {
                Some(mine) if mine.checksum == h.checksum => {
                    let ack_again_as_the_first_may_be_lost =
                        Message::PrepareOk { view: self.view, op: h.op, checksum: h.checksum };
                    self.send(from, ack_again_as_the_first_may_be_lost)
                }
                Some(_) if h.view == self.view => {
                    panic!("replica {}: op {} from the primary conflicts with the log", self.config.replica, h.op)
                }
                _ => {}
            }
            return;
        }
        if h.view != self.view || h.replica != from {
            return;
        }
        self.heard_from_primary();
        self.committed_op = self.committed_op.max(h.commit);
        if h.op > self.head_op + self.wal.slot_count() {
            return;
        }
        if let Some(known) = self.chain.insert(h.op, h) {
            assert_eq!(known.checksum, h.checksum, "replica {}: two canonical headers for op {}", self.me(), h.op);
        }
        self.stash_put(prepare);
        self.progress();
    }

    fn stash_put(&mut self, prepare: Prepare) {
        let cap = usize_from(self.config.pipeline_max) * 2 + 16;
        let op = prepare.header.op;
        if self.prepares_ahead_of_turn.len() >= cap && !self.prepares_ahead_of_turn.contains_key(&op) {
            let (&highest, _) =
                self.prepares_ahead_of_turn.last_key_value().expect("the stash is at capacity, so it is not empty");
            if highest <= op {
                return;
            }
            self.prepares_ahead_of_turn.remove(&highest);
        }
        self.prepares_ahead_of_turn.insert(op, prepare);
    }

    fn on_commit(&mut self, from: u8, heartbeat: Commit) {
        let Commit { view, commit, commit_checksum, checkpoint_op, checkpoint_id } = heartbeat;
        if view < self.view || from != self.primary_of(view) {
            return;
        }
        if view > self.view || self.status != Status::Normal {
            return self.request_start_view(view);
        }
        self.heard_from_primary();
        let checkpoint = &self.superblock.state.checkpoint;
        if checkpoint_op == checkpoint.op() {
            assert_eq!(
                checkpoint_id,
                checkpoint.id(),
                "replica {}: checkpoint at op {} diverged from the primary's; storage is physically deterministic, \
                 so the same ops must produce the same bytes on every replica",
                self.config.replica,
                checkpoint_op
            );
        }
        self.committed_op = self.committed_op.max(commit);
        if commit > self.head_op {
            match self.chain.get(&commit) {
                Some(known) => assert_eq!(
                    known.checksum, commit_checksum,
                    "replica {}: commit point {} conflicts with the canonical headers",
                    self.config.replica, commit
                ),
                None => self.header_repair_starts_from = Some((commit, commit_checksum)),
            }
        } else if let Some(mine) = self.log.get(&commit) {
            assert_eq!(
                mine.checksum, commit_checksum,
                "replica {}: log diverged from the primary's at op {}",
                self.config.replica, commit
            );
        }
        self.progress();
    }

    fn cache_body(&mut self, op: u64, checksum: u128, body: Vec<u8>) {
        let cap = usize_from(self.config.pipeline_max) * 2 + 8;
        self.recent_prepare_bodies.insert(op, (checksum, body));
        while self.recent_prepare_bodies.len() > cap {
            self.recent_prepare_bodies.pop_first();
        }
    }

    fn have_body(&self, h: &Header) -> bool {
        self.wal.has(h.op, h.checksum)
            || self.prepares_ahead_of_turn.get(&h.op).is_some_and(|p| p.header.checksum == h.checksum)
    }

    fn body_of(&mut self, h: &Header) -> Option<Vec<u8>> {
        if let Some((checksum, body)) = self.recent_prepare_bodies.get(&h.op)
            && *checksum == h.checksum
        {
            return Some(body.clone());
        }
        if let Some(prepare) = self.prepares_ahead_of_turn.get(&h.op)
            && prepare.header.checksum == h.checksum
        {
            return Some(prepare.body.clone());
        }
        self.wal.read(&mut self.dev, h.op, h.checksum)
    }

    fn find_header(&self, op: u64, checksum: u128) -> Option<Header> {
        let matches = |h: &&Header| h.op == op && h.checksum == checksum;
        self.log
            .get(&op)
            .filter(matches)
            .or_else(|| self.chain.get(&op).filter(matches))
            .copied()
            .or_else(|| self.wal.slot(op).disk.filter(|h| h.op == op && h.checksum == checksum))
    }

    fn truncate_to(&mut self, op: u64) {
        assert!(
            op >= self.committed_op.min(self.head_op) && op >= self.checkpoint_op(),
            "replica {}: refusing to truncate committed ops ({} < {})",
            self.config.replica,
            op,
            self.committed_op
        );
        self.log.split_off(&(op + 1));
        self.recent_prepare_bodies.split_off(&(op + 1));
        self.pipeline_acks.split_off(&(op + 1));
        self.head_op = op;
    }

    fn install(&mut self, headers: &[Header], new_log_view: bool) {
        let first = headers[0];
        let top = headers[headers.len() - 1];
        if new_log_view {
            self.chain.clear();
            self.header_repair_starts_from = None;
            self.requested.clear();
            let floor = self.committed_op.min(self.head_op);
            assert!(
                top.op >= floor,
                "replica {}: view {} starts at op {}, below the committed op {}; the committed prefix must be on every \
                 later log",
                self.config.replica,
                self.view,
                top.op,
                floor
            );
            if floor >= first.op {
                assert_eq!(
                    self.log[&floor].checksum,
                    headers[usize_from(floor - first.op)].checksum,
                    "replica {}: committed op {} is not on the log of view {}",
                    self.config.replica,
                    floor,
                    self.view
                );
            }
            let mut keep = floor;
            let mut n = self.head_op.min(top.op);
            while n > floor && n >= first.op {
                if self.log[&n].checksum == headers[usize_from(n - first.op)].checksum {
                    keep = n;
                    break;
                }
                n -= 1;
            }
            if keep == floor
                && first.op > floor
                && first.op - 1 <= self.head_op
                && self.log[&(first.op - 1)].checksum == first.parent
            {
                keep = first.op - 1;
            }
            self.truncate_to(keep);
        }
        for h in headers {
            if h.op > self.head_op {
                self.chain.insert(h.op, *h);
            }
        }
    }

    fn appending(&self) -> bool {
        match self.status {
            Status::Normal => true,
            Status::ViewChange => self.elect_log_chosen_and_durable,
            Status::RecoveringHead => false,
        }
    }

    fn move_canonical_prepares_into_durable_log(&mut self) {
        if !self.appending() {
            return;
        }
        while let Some((&n, &h)) = self.chain.first_key_value() {
            if n <= self.head_op {
                if let Some(mine) = self.log.get(&n) {
                    assert_eq!(
                        mine.checksum, h.checksum,
                        "replica {}: canonical op {} conflicts with the log",
                        self.config.replica, n
                    );
                }
                self.chain.remove(&n);
                continue;
            }
            if n > self.head_op + 1 {
                break;
            }
            assert_eq!(
                h.parent,
                self.head().checksum,
                "replica {}: canonical op {} does not extend the log",
                self.config.replica,
                n
            );
            let no_wal_room_until_the_next_checkpoint = n > self.checkpoint_op() + self.wal.slot_count();
            if no_wal_room_until_the_next_checkpoint {
                break;
            }
            if !self.wal.has(n, h.checksum) {
                let Some(prepare) = self.prepares_ahead_of_turn.remove(&n).filter(|p| p.header.checksum == h.checksum)
                else {
                    break;
                };
                self.wal.append(&mut self.dev, &h, &prepare.body);
                self.cache_body(n, h.checksum, prepare.body);
            }
            self.chain.remove(&n);
            self.log.insert(n, h);
            self.head_op = n;
            if self.status == Status::Normal {
                let primary = self.primary_of(self.view);
                self.send(primary, Message::PrepareOk { view: self.view, op: n, checksum: h.checksum });
            }
        }
        let op = self.head_op;
        self.prepares_ahead_of_turn.retain(|n, _| *n > op);
    }

    fn progress(&mut self) {
        loop {
            let before = (self.head_op, self.executed_op);
            self.move_canonical_prepares_into_durable_log();
            self.execute_committed_ops();
            if (self.head_op, self.executed_op) == before {
                break;
            }
        }
        self.try_elect();
        self.repair();
    }

    fn execute_committed_ops(&mut self) {
        while self.executed_op < self.committed_op.min(self.head_op) && self.lsm.is_some() {
            let n = self.executed_op + 1;
            let header = self.log[&n];
            let Some(body) = self.body_of(&header) else { return self.wal_fault(n) };
            let Some(lsm) = self.lsm.as_mut() else { return };
            let mut replies = Vec::new();
            let result = execute(&mut self.sm, lsm, &mut self.dev, &header, &body, &mut replies);
            match result {
                Ok(()) => {}
                Err(StorageError::Fault(_)) => return self.storage_fault(),
                Err(e) => panic!("replica {}: op {} failed: {e}", self.config.replica, n),
            }
            self.executed_op = n;
            self.counters.commits += 1;
            self.recent_prepare_bodies.remove(&n);
            self.pipeline_acks.remove(&n);
            if header.kind == Kind::Pulse {
                self.pulse_pending = false;
            }
            let primary = self.is_primary();
            let me = self.config.addr(self.config.replica);
            for (client, reply) in replies {
                self.inflight.remove(&reply.request_id);
                if primary && client != me {
                    self.outbox.push((client, Packet::Vsr(Message::Reply(reply))));
                }
            }
            self.emit(Event::Committed { op: n, checksum: header.checksum, timestamp: header.timestamp });
            if n.is_multiple_of(self.config.checkpoint_interval) {
                match self.write_checkpoint() {
                    Ok(()) => {}
                    Err(StorageError::Fault(_)) => return self.storage_fault(),
                    Err(e) => panic!("replica {}: checkpoint at op {} failed: {e}", self.config.replica, n),
                }
            }
        }
    }

    fn write_checkpoint(&mut self) -> Result<(), StorageError> {
        let op = self.executed_op;
        let lsm = self.lsm.as_mut().expect("write_checkpoint runs only from commit_ops, which requires open storage");
        let reference = lsm.checkpoint(&mut self.dev)?;
        self.dev.sync();
        self.superblock.state.checkpoint = CheckpointInfo { header: self.log[&op], lsm: Some(reference) };
        self.superblock.write(&mut self.dev);
        lsm.checkpoint_durable();
        self.log = self.log.split_off(&op);
        self.counters.checkpoints += 1;
        self.emit(Event::Checkpointed { op, id: self.superblock.state.checkpoint.id() });
        Ok(())
    }

    fn wal_fault(&mut self, op: u64) {
        self.counters.wal_faults += 1;
        self.emit(Event::WalFault { op });
        assert!(
            self.replica_count() > 1,
            "replica {}: prepare {} is damaged and there is no other replica",
            self.config.replica,
            op
        );
        let lost = self.log.split_off(&op);
        self.chain.extend(lost);
        self.pipeline_acks.split_off(&op);
        self.head_op = op - 1;
        let primary_must_hold_its_whole_log = self.is_primary();
        if primary_must_hold_its_whole_log {
            self.enter_view_change(self.view + 1);
        }
        self.repair();
    }

    fn start_vote(&mut self) {
        if !self.voting || self.my_vote <= self.view {
            self.my_vote = self.view + 1;
        }
        self.voting = true;
        self.broadcast(Message::StartViewChange { view: self.my_vote, joined: false });
        self.check_votes();
    }

    fn on_start_view_change(&mut self, from: u8, view: u32, joined: bool) {
        let sender_is_behind_or_waiting_for_this_view_to_start = view <= self.view;
        if sender_is_behind_or_waiting_for_this_view_to_start {
            if self.is_primary() {
                self.send_start_view(from);
            }
            return;
        }
        let sender_durably_left_our_view_on_a_majority_vote_and_would_be_stranded = joined;
        if sender_durably_left_our_view_on_a_majority_vote_and_would_be_stranded {
            self.enter_view_change(view);
            return;
        }
        self.latest_vote_heard_from[from as usize] = (view, self.ticks);
        self.check_votes();
    }

    fn check_votes(&mut self) {
        let ttl = self.config.timing.resend * 4;
        let mut views: Vec<u32> = Vec::with_capacity(self.replica_count());
        for replica in 0..self.replica_count() {
            if replica == self.config.replica as usize {
                if self.voting && self.my_vote > self.view {
                    views.push(self.my_vote);
                }
                continue;
            }
            let (view, at) = self.latest_vote_heard_from[replica];
            if view > self.view && self.ticks.saturating_sub(at) <= ttl {
                views.push(view);
            }
        }
        views.sort_unstable_by(|a, b| b.cmp(a));
        let majority = self.majority();
        if views.len() >= majority {
            self.enter_view_change(views[majority - 1]);
        }
    }

    fn enter_view_change(&mut self, view: u32) {
        assert!(view > self.view && self.replica_count() > 1);
        self.view = view;
        self.superblock.state.view = view;
        self.superblock.write(&mut self.dev);
        if self.status != Status::RecoveringHead {
            self.status = Status::ViewChange;
        }
        self.counters.view_changes += 1;
        self.emit(Event::ViewChangeStarted { view });
        self.reset_leadership();
        self.view_change_at = self.ticks;
        self.elect_log_chosen_and_durable = false;
        self.dvcs.clear();
        self.broadcast(Message::StartViewChange { view, joined: true });
        self.send_do_view_change();
    }

    fn reset_leadership(&mut self) {
        self.pipeline_acks.clear();
        self.pulse_pending = false;
        self.inflight.clear();
        self.header_repair_starts_from = None;
        while let Some(request) = self.queue.pop_front() {
            if !matches!(request.client, Addr::Replica { .. }) {
                self.reject(&request, RejectReason::NotPrimary);
            }
        }
        if self.my_vote <= self.view {
            self.voting = false;
            self.my_vote = self.view;
        }
    }

    fn held(&self, h: &Header) -> Held {
        if self.wal.has(h.op, h.checksum) {
            return Held::Present;
        }
        let slot = self.wal.slot(h.op);
        if slot.uncertain || slot.lost.is_some_and(|lost| lost.checksum == h.checksum) {
            Held::Unknown
        } else {
            Held::Nack
        }
    }

    fn top_headers(&self) -> Vec<Header> {
        let count = usize_from(self.config.pipeline_max) + 1;
        let mut headers: Vec<Header> = self.log.values().rev().take(count).copied().collect();
        headers.reverse();
        headers
    }

    fn build_do_view_change(&self) -> DoViewChange {
        let count = usize_from(self.config.pipeline_max) + 1;
        let mut headers: Vec<(Header, Held)> = Vec::new();
        let mut expect: Option<u64> = None;
        for (&op, h) in self.chain.iter().rev() {
            if headers.len() == count || expect.is_some_and(|e| e != op) {
                break;
            }
            headers.push((*h, self.held(h)));
            expect = Some(op - 1);
        }
        if expect.is_none_or(|e| e == self.head_op) {
            for h in self.log.values().rev() {
                if headers.len() == count {
                    break;
                }
                headers.push((*h, Held::Present));
            }
        }
        headers.reverse();
        let top = headers.last().expect("the log always holds its checkpoint header, so one header is collected").0;
        DoViewChange {
            view: self.view,
            log_view: self.log_view,
            headers,
            top_op: top.op,
            top_checksum: top.checksum,
            commit_min: self.executed_op,
            commit_max: self.committed_op,
            checkpoint_op: self.checkpoint_op(),
        }
    }

    fn send_do_view_change(&mut self) {
        if self.status != Status::ViewChange || self.elect_log_chosen_and_durable {
            return;
        }
        let primary = self.primary_of(self.view);
        if primary == self.config.replica {
            if !self.dvcs.contains_key(&primary) {
                let dvc = self.build_do_view_change();
                self.dvcs.insert(primary, dvc);
            }
        } else {
            let dvc = self.build_do_view_change();
            self.send(primary, Message::DoViewChange(dvc));
        }
    }

    fn on_do_view_change(&mut self, from: u8, dvc: DoViewChange) {
        if dvc.view < self.view || self.primary_of(dvc.view) != self.config.replica {
            return;
        }
        let headers: Vec<Header> = dvc.headers.iter().map(|(h, _)| *h).collect();
        if !valid_headers(self.config.cluster, &headers) || dvc.top_op != headers[headers.len() - 1].op {
            return;
        }
        if dvc.view > self.view {
            let cannot_lead_without_knowing_our_own_log = self.status == Status::RecoveringHead;
            if cannot_lead_without_knowing_our_own_log {
                return;
            }
            let view_the_sender_joined_on_a_majority_say_so = dvc.view;
            self.enter_view_change(view_the_sender_joined_on_a_majority_say_so);
        }
        match self.status {
            Status::Normal => self.send_start_view(from),
            Status::ViewChange => {
                self.dvcs.insert(from, dvc);
                self.try_elect();
            }
            Status::RecoveringHead => {}
        }
    }

    fn try_elect(&mut self) {
        let me = self.config.replica;
        if self.status != Status::ViewChange || self.primary_of(self.view) != me {
            return;
        }
        if self.dvcs.len() < self.majority() || !self.dvcs.contains_key(&me) {
            return;
        }
        if !self.elect_log_chosen_and_durable {
            let log_holding_every_op_that_could_have_committed = self
                .dvcs
                .values()
                .max_by_key(|d| (d.log_view, d.top_op, d.headers.len()))
                .expect("a majority of DoViewChange messages are in, so dvcs is not empty");
            let headers: Vec<Header> =
                log_holding_every_op_that_could_have_committed.headers.iter().map(|(h, _)| *h).collect();
            let commit_max = self
                .dvcs
                .values()
                .map(|d| d.commit_max)
                .max()
                .expect("a majority of DoViewChange messages are in, so dvcs is not empty");
            let top = headers[headers.len() - 1].op;
            assert!(
                commit_max <= top,
                "replica {me}: the log chosen for view {} ends at {top}, below committed op {commit_max}: {:?}",
                self.view,
                self.dvc_summary()
            );

            self.answer_for_this_log_durably_before_touching_it(&headers);
            self.elect_committed_through = top.saturating_sub(self.config.pipeline_max).max(commit_max);
            self.install(&headers, true);
            self.committed_op = self.committed_op.max(commit_max);
            self.elect_log_chosen_and_durable = true;
        }
        self.move_canonical_prepares_into_durable_log();
        self.execute_committed_ops();
        if self.status != Status::ViewChange {
            return;
        }

        let first_op_whose_prepare_cannot_be_found =
            self.chain.values().find(|h| h.op > self.elect_committed_through && !self.have_body(h)).copied();
        if let Some(h) = first_op_whose_prepare_cannot_be_found {
            let header_below_is_known_so_our_claimed_top_stays_true =
                h.op - 1 <= self.head_op || self.chain.contains_key(&(h.op - 1));
            let nacks = self.dvcs.values().filter(|d| dvc_shows_its_sender_never_acked(d, &h)).count();
            let too_few_acks_to_have_committed = nacks >= self.nack_quorum();
            if header_below_is_known_so_our_claimed_top_stays_true && too_few_acks_to_have_committed {
                self.chain.split_off(&h.op);
                self.prepares_ahead_of_turn.split_off(&h.op);
                self.move_canonical_prepares_into_durable_log();
            }
        }
        if self.chain.is_empty() {
            self.start_view();
        } else {
            self.repair();
        }
    }

    fn answer_for_this_log_durably_before_touching_it(&mut self, headers: &[Header]) {
        self.log_view = self.view;
        self.superblock.state.log_view = self.view;
        self.superblock.state.view_headers = headers.to_vec();
        self.superblock.write(&mut self.dev);
    }

    fn dvc_summary(&self) -> Vec<DvcSummary> {
        self.dvcs
            .iter()
            .map(|(r, d)| DvcSummary {
                replica: *r,
                log_view: d.log_view,
                top_op: d.top_op,
                commit_max: d.commit_max,
                headers: d.headers.iter().map(|(h, held)| (h.op, h.view, *held)).collect(),
            })
            .collect()
    }

    fn start_view(&mut self) {
        self.superblock.state.view_headers = self.top_headers();
        self.superblock.write(&mut self.dev);
        self.committed_op = self.committed_op.max(self.head_op.saturating_sub(self.config.pipeline_max));
        assert!(
            self.committed_op <= self.head_op,
            "replica {}: commit_max {} > op {} floor {} view {} dvcs {:?}",
            self.config.replica,
            self.committed_op,
            self.head_op,
            self.elect_committed_through,
            self.view,
            self.dvc_summary()
        );
        self.status = Status::Normal;
        self.voting = false;
        self.my_vote = self.view;
        self.dvcs.clear();
        let me = self.config.replica;
        self.pipeline_acks = (self.committed_op + 1..=self.head_op).map(|op| (op, 1u32 << me)).collect();
        self.progress_at = self.ticks;
        self.heartbeat_at = self.ticks + self.config.timing.heartbeat;
        let resend_the_uncommitted_suffix_next_tick = self.ticks;
        self.prepare_resend_at = resend_the_uncommitted_suffix_next_tick;
        self.emit(Event::BecamePrimary { view: self.view });
        let sv = self.start_view_message();
        self.broadcast(Message::StartView(sv));
        self.execute_committed_ops();
        self.prepare_queued_work_while_pipeline_has_room();
    }

    fn start_view_message(&self) -> StartView {
        StartView {
            view: self.view,
            headers: self.top_headers(),
            commit_max: self.committed_op,
            checkpoint: self.superblock.state.checkpoint,
        }
    }

    fn send_start_view(&mut self, to: u8) {
        let sv = self.start_view_message();
        self.send(to, Message::StartView(sv));
    }

    fn request_start_view(&mut self, view: u32) {
        if self.ticks < self.start_view_requested_at {
            return;
        }
        self.start_view_requested_at = self.ticks + self.config.timing.resend;
        let primary = self.primary_of(view);
        self.send(primary, Message::RequestStartView { view });
    }

    fn on_start_view(&mut self, from: u8, sv: StartView) {
        if sv.view < self.view || from != self.primary_of(sv.view) {
            return;
        }
        if sv.view == self.view && self.status == Status::Normal {
            return;
        }
        if !valid_headers(self.config.cluster, &sv.headers) {
            return;
        }
        let new_log_view = sv.view > self.log_view;
        let persist_before_touching_the_log = new_log_view || sv.view > self.view;
        self.view = sv.view;
        self.log_view = sv.view;
        if persist_before_touching_the_log {
            let st = &mut self.superblock.state;
            st.view = sv.view;
            if new_log_view {
                st.log_view = sv.view;
                st.view_headers = sv.headers.clone();
            }
            self.superblock.write(&mut self.dev);
        }
        self.reset_leadership();
        self.dvcs.clear();
        self.elect_log_chosen_and_durable = false;
        self.install(&sv.headers, new_log_view);
        self.status = Status::Normal;
        self.voting = false;
        self.my_vote = self.view;
        self.arm_election();
        self.committed_op = self.committed_op.max(sv.commit_max);
        self.emit(Event::BecameBackup { view: self.view });

        let already_held_of_the_uncommitted_suffix: Vec<(u64, u128)> =
            self.log.range(sv.commit_max + 1..).map(|(&op, h)| (op, h.checksum)).collect();
        for (op, checksum) in already_held_of_the_uncommitted_suffix {
            self.send(from, Message::PrepareOk { view: self.view, op, checksum });
        }
        self.progress();
    }

    fn request(&mut self, kind: u8, op: u64, to: Option<u8>, message: Message) {
        if self.requested.contains_key(&(kind, op)) {
            return;
        }
        self.requested.insert((kind, op), self.ticks);
        let to = to.unwrap_or_else(|| self.random_peer());
        self.send(to, message);
    }

    fn likely_holder_of_prepare(&mut self, h: &Header) -> Option<u8> {
        if self.status == Status::ViewChange {
            let me = self.config.replica;
            let holders: Vec<u8> = self
                .dvcs
                .iter()
                .filter(|(r, d)| {
                    **r != me && d.headers.iter().any(|(x, held)| x.checksum == h.checksum && *held == Held::Present)
                })
                .map(|(r, _)| *r)
                .collect();
            if !holders.is_empty() {
                return Some(holders[self.prng.index(holders.len())]);
            }
            return None;
        }
        let primary = self.primary_of(self.view);
        let ask_the_primary_half_the_time = primary != self.config.replica && self.prng.bool();
        ask_the_primary_half_the_time.then_some(primary)
    }

    fn repair(&mut self) {
        if self.replica_count() == 1 || !self.appending() {
            return;
        }
        self.request_headers_filling_gaps_in_chain();
        self.request_the_next_missing_prepares();
    }

    fn request_headers_filling_gaps_in_chain(&mut self) {
        let mut gaps: Vec<(u64, u128, u64)> = Vec::new();
        let mut expect = self.head_op + 1;
        for (&n, h) in &self.chain {
            if n > expect {
                gaps.push((n - 1, h.parent, expect));
                if gaps.len() == 2 {
                    break;
                }
            }
            expect = n + 1;
        }
        for (op, checksum, floor) in gaps {
            self.request(REQUEST_HEADERS, op, None, Message::RequestHeaders { op, checksum, floor });
        }
        if let Some((op, checksum)) = self.header_repair_starts_from {
            if op <= self.head_op || self.chain.contains_key(&op) {
                self.header_repair_starts_from = None;
            } else {
                let floor = self.head_op + 1;
                self.request(REQUEST_HEADERS, op, None, Message::RequestHeaders { op, checksum, floor });
            }
        }
    }

    fn request_the_next_missing_prepares(&mut self) {
        let mut missing: Vec<Header> = Vec::new();
        for (expect, (&n, h)) in (self.head_op + 1..).zip(self.chain.iter()) {
            if n != expect || missing.len() == REPAIR_PREPARES {
                break;
            }
            if !self.have_body(h) {
                missing.push(*h);
            }
        }
        for h in missing {
            let to = self.likely_holder_of_prepare(&h);
            self.request(REQUEST_PREPARE, h.op, to, Message::RequestPrepare { op: h.op, checksum: h.checksum });
        }
    }

    fn sync_target_message(&self) -> Message {
        Message::SyncTarget { view: self.view, checkpoint: self.superblock.state.checkpoint }
    }

    fn on_request_headers(&mut self, from: u8, op: u64, checksum: u128, floor: u64) {
        let mut headers = Vec::new();
        let (mut op, mut checksum) = (op, checksum);
        while headers.len() < HEADERS_PER_MESSAGE && op >= floor {
            let Some(h) = self.find_header(op, checksum) else { break };
            headers.push(h);
            if op == 0 {
                break;
            }
            op -= 1;
            checksum = h.parent;
        }
        if !headers.is_empty() {
            self.send(from, Message::Headers { headers });
        } else if op <= self.checkpoint_op() && self.lsm.is_some() {
            let message = self.sync_target_message();
            self.send(from, message);
        }
    }

    fn on_headers(&mut self, mut headers: Vec<Header>) {
        headers.sort_unstable_by_key(|h| std::cmp::Reverse(h.op));
        let mut learned = false;
        for h in headers {
            if !h.valid() || h.cluster != self.config.cluster || h.op <= self.head_op {
                continue;
            }
            let anchored = self.header_repair_starts_from == Some((h.op, h.checksum));
            let linked = self.chain.get(&(h.op + 1)).is_some_and(|above| above.parent == h.checksum);
            let hash_links_to_something_canonical = anchored || linked;
            if !hash_links_to_something_canonical {
                continue;
            }
            if anchored {
                self.header_repair_starts_from = None;
            }
            match self.chain.get(&h.op) {
                Some(known) => assert_eq!(
                    known.checksum, h.checksum,
                    "replica {}: two canonical headers for op {}",
                    self.config.replica, h.op
                ),
                None => {
                    self.chain.insert(h.op, h);
                    learned = true;
                }
            }
        }
        if learned {
            self.progress();
        }
    }

    fn on_request_prepare(&mut self, from: u8, op: u64, checksum: u128) {
        if let Some(header) = self.find_header(op, checksum)
            && let Some(body) = self.body_of(&header)
        {
            return self.send(from, Message::Prepare(Prepare { header, body }));
        }
        if op <= self.checkpoint_op() && self.lsm.is_some() {
            let message = self.sync_target_message();
            self.send(from, message);
        }
    }

    fn storage_fault(&mut self) {
        self.counters.storage_faults += 1;
        self.emit(Event::StorageFault);
        assert!(
            self.replica_count() > 1,
            "replica {}: storage is damaged and there is no other replica",
            self.config.replica
        );
        self.lsm = None;
        self.executed_op = self.checkpoint_op();
        self.wanted.clear();
        self.blocks_pending = 0;
        self.blocks_requested_at = 0;
        self.repair_storage();
    }

    fn repair_storage(&mut self) {
        if self.lsm.is_some() {
            return;
        }
        if self.wanted.is_empty() {
            let checkpoint = self.superblock.state.checkpoint;
            let grid = self.config.layout.grid();
            let missing = match checkpoint.lsm {
                Some(reference) => missing_blocks(&mut self.dev, &grid, &reference, BLOCKS_WANTED),
                None => Vec::new(),
            };
            if missing.is_empty() {
                self.dev.sync();
                if self.open_storage() {
                    self.progress();
                }
                return;
            }
            self.wanted = missing.into_iter().map(|block| (block.address, block)).collect();
            self.blocks_pending = 0;
        }
        if self.blocks_pending == 0 || self.ticks >= self.blocks_requested_at {
            self.blocks_requested_at = self.ticks + self.config.timing.resend;
            let blocks: Vec<BlockRef> = self.wanted.values().take(BLOCKS_PER_REQUEST).copied().collect();
            self.blocks_pending = blocks.len();
            let to = self.random_peer();
            self.send(to, Message::RequestBlocks { blocks });
        }
    }

    fn on_request_blocks(&mut self, from: u8, blocks: Vec<BlockRef>) {
        let grid = self.config.layout.grid();
        let mut unserved = false;
        for block in blocks.into_iter().take(BLOCKS_PER_REQUEST) {
            if block.address == 0 || block.address > grid.block_count {
                continue;
            }
            match read_block_raw(&mut self.dev, &grid, block.address) {
                Some((found, bytes)) if found == block => self.send(from, Message::Block { block, bytes }),
                _ => unserved = true,
            }
        }
        let requested_checkpoint_is_gone_here = unserved;
        if requested_checkpoint_is_gone_here && self.lsm.is_some() {
            let offer_the_current_checkpoint = self.sync_target_message();
            self.send(from, offer_the_current_checkpoint);
        }
    }

    fn on_block(&mut self, block: BlockRef, bytes: Vec<u8>) {
        if self.lsm.is_some() || self.wanted.get(&block.address) != Some(&block) {
            return;
        }
        let grid = self.config.layout.grid();
        if !write_block_raw(&mut self.dev, &grid, block, &bytes) {
            return;
        }
        self.wanted.remove(&block.address);
        self.counters.blocks_repaired += 1;
        self.blocks_pending = self.blocks_pending.saturating_sub(1);
        if self.blocks_pending == 0 {
            self.repair_storage();
        }
    }

    fn on_sync_target(&mut self, view: u32, target: CheckpointInfo) {
        let catch_up_with_the_view_first_to_know_the_checkpoint_is_on_our_log = view > self.view;
        if catch_up_with_the_view_first_to_know_the_checkpoint_is_on_our_log {
            return self.request_start_view(view);
        }
        if self.log_view != self.view || self.status == Status::RecoveringHead {
            return;
        }
        let target_is_not_on_our_log = self.log.get(&target.op()).is_none_or(|h| h.checksum != target.header.checksum);
        let a_primary_keeps_its_whole_log = self.is_primary();
        if a_primary_keeps_its_whole_log && target_is_not_on_our_log {
            return;
        }
        if target.op() <= self.checkpoint_op() || target.lsm.is_none() {
            return;
        }
        if !target.header.valid() || target.header.cluster != self.config.cluster {
            return;
        }
        let our_own_log_can_get_us_there = self.lsm.is_some() && target.op() <= self.head_op;
        if our_own_log_can_get_us_there {
            return;
        }
        self.adopt_checkpoint(target);
    }

    fn adopt_checkpoint(&mut self, target: CheckpointInfo) {
        let op = target.op();
        match self.log.get(&op) {
            Some(mine) => {
                assert_eq!(
                    mine.checksum, target.header.checksum,
                    "replica {}: sync target at op {} is not on the log",
                    self.config.replica, op
                );
                self.log = self.log.split_off(&op);
            }
            None => {
                self.log.clear();
                self.log.insert(op, target.header);
                self.head_op = op;
            }
        }
        self.superblock.state.checkpoint = target;
        self.superblock.write(&mut self.dev);
        self.lsm = None;
        self.wanted.clear();
        self.blocks_pending = 0;
        self.blocks_requested_at = 0;
        self.executed_op = op;
        self.committed_op = self.committed_op.max(op);
        let head = self.head_op;
        self.chain.retain(|n, _| *n > head);
        self.prepares_ahead_of_turn.retain(|n, _| *n > head);
        self.recent_prepare_bodies.retain(|n, _| *n > op);
        self.pipeline_acks.clear();
        self.counters.state_syncs += 1;
        self.emit(Event::SyncStarted { op });
        self.repair_storage();
    }
}
