//! Deterministic simulation of WFE.
//!
//! Everything a run does is a function of its seed: the simulated disks,
//! network and clock live here, and the code under test is the same code the
//! production runtime drives.

pub mod device;
pub mod findings;
pub mod net;

pub use device::SimDevice;
pub use net::{Net, NetOptions};
pub mod fuzz;
pub mod history;
pub mod workload;
pub mod world;
