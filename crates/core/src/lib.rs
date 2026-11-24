//! Deterministic foundations shared by every WFE crate.
//!
//! Nothing in here reads a clock, spawns a thread, or draws entropy from the
//! operating system (the one exception is [`device::FileDevice`], the
//! production block device). That is what lets the whole system run inside
//! the simulator.

pub mod api;
pub mod checksum;
pub mod codec;
pub mod device;
pub mod error;
pub mod ids;
pub mod keys;
pub mod payload;
pub mod prng;

pub use checksum::{Checksum, checksum};
pub use error::{Error, ErrorCode, Result};
pub use ids::{Addr, CellId, Id128, OpNumber, ReplicaIndex, ShardId, Timestamp, View};
pub use payload::Payload;
pub use prng::{Prng, Ratio};
