//! The single list of workspace crates and what each one is allowed to do.
//!
//! The tests in `tests/` hold the rest of the repo to this list.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// Runs inside a replica. Bound by the root `clippy.toml` determinism bans, and its
    /// dependency tree must not contain any crate in [`FORBIDDEN_IN_REPLICATED`].
    Replicated,
    /// Must replay exactly from a seed, so it is bound by the root `clippy.toml`
    /// determinism bans, but it may depend on I/O crates (the simulator drives the SDK).
    Deterministic,
    /// Does real I/O. Has its own `clippy.toml`, which replaces the root determinism bans.
    Io,
}

pub const CRATES: &[(&str, Kind)] = &[
    ("wfe-core", Kind::Replicated),
    ("wfe-lsm", Kind::Replicated),
    ("wfe-vsr", Kind::Replicated),
    ("wfe-engine", Kind::Replicated),
    ("wfe-gateway", Kind::Replicated),
    ("wfe-control", Kind::Deterministic),
    ("wfe-sim", Kind::Deterministic),
    ("wfe-rules", Kind::Deterministic),
    ("wfe-codec", Kind::Io),
    ("wfe-sdk", Kind::Io),
    ("wfe-server", Kind::Io),
    ("wfe-cli", Kind::Io),
    ("wfe-ui", Kind::Io),
    ("wfe-operator", Kind::Io),
];

pub const FORBIDDEN_IN_REPLICATED: &[&str] = &[
    "tokio",
    "mio",
    "axum",
    "hyper",
    "reqwest",
    "ureq",
    "kube",
    "rand",
    "getrandom",
    "anyhow",
    "wfe-codec",
    "wfe-sdk",
    "wfe-server",
    "wfe-cli",
    "wfe-ui",
    "wfe-operator",
    "wfe-sim",
];

pub fn kind_of(package: &str) -> Option<Kind> {
    CRATES.iter().find(|(name, _)| *name == package).map(|(_, kind)| *kind)
}
