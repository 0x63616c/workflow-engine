//! What an operator of WFE does next.
//!
//! Each planner takes what was asked for and what was last observed, and
//! returns the next actions. Planners keep nothing between calls, read no
//! clock and do no I/O, so a caller that crashes, repeats itself or acts on
//! an old observation can simply observe and plan again. The Kubernetes
//! operator, the CLI and the simulator all drive these same functions.

pub mod roles;
pub mod workers;

pub use roles::{CellView, LinkView, Other, RoleStep, plan_roles};
pub use workers::{
    FleetBuild, RampStep, RolloutPolicy, RolloutStep, RolloutStrategy, SunsetPolicy, SunsetStep, WorkersAction,
    WorkersInput, WorkersPlan, plan_rollout, plan_sunset, plan_workers,
};
