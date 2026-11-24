//! Everything replicas say to each other and to their clients.

use crate::header::Header;
use serde::{Deserialize, Serialize};
use wfe_core::Addr;
use wfe_lsm::{BlockRef, CheckpointRef};

/// What travels between processes: consensus traffic, or a packet for the
/// layer above (the state machine's own protocol).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[expect(
    clippy::large_enum_variant,
    reason = "packets move by value through every queue; boxing Message would change constructors in the gateway, engine, server and sim crates"
)]
pub enum Packet<A> {
    Vsr(Message),
    App(A),
}

/// One client request. Requests are not sessioned: a retried request may be
/// executed twice, so the state machine makes them idempotent where it
/// matters (the id is passed down for that).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub request_id: u128,
    /// Where the reply goes.
    pub client: Addr,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reply {
    pub request_id: u128,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RejectReason {
    /// Ask the primary of `view` instead.
    NotPrimary,
    /// The request queue is full; retry shortly.
    Busy,
    /// The request cannot fit in a prepare.
    TooLarge,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Prepare {
    pub header: Header,
    #[serde(with = "serde_bytes")]
    pub body: Vec<u8>,
}

/// A durable snapshot of the state machine as of one committed op.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointInfo {
    /// The header of the last op the snapshot includes: where the log
    /// continues from.
    pub header: Header,
    /// `None` only for the empty state at op 0.
    pub lsm: Option<CheckpointRef>,
}

impl CheckpointInfo {
    pub fn op(&self) -> u64 {
        self.header.op
    }

    /// A value that is equal across replicas iff their checkpoints are.
    pub fn id(&self) -> u128 {
        self.lsm.map_or(0, |c| c.manifest.checksum) ^ self.header.checksum
    }
}

/// What a replica can say about one of its log entries during a view change.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Held {
    /// The prepare is durable here.
    Present,
    /// This replica is certain it never durably held the prepare.
    Nack,
    /// It may have held it and lost it (storage fault): no information.
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DoViewChange {
    pub view: u32,
    /// The latest view in which the sender's log was made canonical.
    pub log_view: u32,
    /// The top of the sender's log as of `log_view`, ascending and contiguous.
    /// Covers every op that might not be committed, plus one that is.
    pub headers: Vec<(Header, Held)>,
    /// The top of the sender's log, also when `headers` is empty.
    pub top_op: u64,
    pub top_checksum: u128,
    pub commit_min: u64,
    pub commit_max: u64,
    pub checkpoint_op: u64,
}

/// The primary's heartbeat. Names the commit point so backups can find the chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Commit {
    pub view: u32,
    pub commit: u64,
    pub commit_checksum: u128,
    pub checkpoint_op: u64,
    pub checkpoint_id: u128,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartView {
    pub view: u32,
    /// The top of the canonical log, ascending and contiguous.
    pub headers: Vec<Header>,
    pub commit_max: u64,
    pub checkpoint: CheckpointInfo,
}

#[derive(Clone, Debug, Serialize, Deserialize, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Message {
    Request(Request),
    Reply(Reply),
    Reject {
        request_id: u128,
        view: u32,
        reason: RejectReason,
    },

    Prepare(Prepare),
    PrepareOk {
        view: u32,
        op: u64,
        checksum: u128,
    },
    Commit(Commit),

    /// "I think the primary of `view - 1` is gone." A vote, not a promise.
    /// A vote for `view`; `joined` once the sender has durably entered it.
    StartViewChange {
        view: u32,
        joined: bool,
    },
    DoViewChange(DoViewChange),
    StartView(StartView),
    RequestStartView {
        view: u32,
    },

    /// Headers from `(op, checksum)` walking down the hash chain to `floor`.
    RequestHeaders {
        op: u64,
        checksum: u128,
        floor: u64,
    },
    Headers {
        headers: Vec<Header>,
    },
    RequestPrepare {
        op: u64,
        checksum: u128,
    },

    RequestBlocks {
        blocks: Vec<BlockRef>,
    },
    Block {
        block: BlockRef,
        #[serde(with = "serde_bytes")]
        bytes: Vec<u8>,
    },
    RequestSyncTarget,
    /// "I cannot serve that any more; this is my checkpoint." `view` is the
    /// sender's view: a checkpoint is only adopted from a view the receiver
    /// has already caught up with.
    SyncTarget {
        view: u32,
        checkpoint: CheckpointInfo,
    },
}

impl Message {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}
