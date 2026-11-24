//! The shard's key space.

use crate::chasm::ExecKey;
use wfe_core::api::{DeploymentVersion, TaskKind};
use wfe_core::keys::{Key, KeyReader};
use wfe_core::{Id128, Timestamp};

const EXEC: u8 = 0x10;
const LOCAL: u8 = 0x11;
const CURRENT: u8 = 0x12;
const TIMER: u8 = 0x20;
const QUEUE: u8 = 0x21;
const OUTBOX: u8 = 0x22;
const CHANGE: u8 = 0x30;
const VISIBILITY: u8 = 0x40;
const PINNED: u8 = 0x41;
const SHARD: u8 = 0x50;
const ROUTE: u8 = 0x51;

fn exec_key(prefix: u8, key: &ExecKey) -> Key {
    Key::new(prefix).u8(key.arch).str(&key.id).u128(key.run.0)
}

/// The root node; every other node's key is this plus its path.
pub fn exec(key: &ExecKey) -> Vec<u8> {
    exec_key(EXEC, key).into_vec()
}

pub fn exec_arch_prefix(arch: u8) -> Vec<u8> {
    Key::new(EXEC).u8(arch).into_vec()
}

pub fn local(key: &ExecKey) -> Vec<u8> {
    exec_key(LOCAL, key).into_vec()
}

const WRITTEN_BY_THIS_MODULE: &str = "keys under this prefix are only written by this module, so they always parse";

pub fn local_prefix() -> Vec<u8> {
    vec![LOCAL]
}

pub fn parse_local(key: &[u8]) -> ExecKey {
    let mut r = KeyReader::new(&key[1..]);
    ExecKey {
        arch: r.u8().expect(WRITTEN_BY_THIS_MODULE),
        id: r.str().expect(WRITTEN_BY_THIS_MODULE),
        run: Id128(r.u128().expect(WRITTEN_BY_THIS_MODULE)),
    }
}

/// The latest run of a business id.
pub fn current(arch: u8, id: &str) -> Vec<u8> {
    Key::new(CURRENT).u8(arch).str(id).into_vec()
}

pub fn current_prefix(arch: u8) -> Vec<u8> {
    Key::new(CURRENT).u8(arch).into_vec()
}

pub fn timer(at: Timestamp, key: &ExecKey) -> Vec<u8> {
    Key::new(TIMER).u64(at.micros()).u8(key.arch).str(&key.id).u128(key.run.0).into_vec()
}

pub fn timer_prefix() -> Vec<u8> {
    vec![TIMER]
}

pub fn parse_timer(key: &[u8]) -> (Timestamp, ExecKey) {
    let mut r = KeyReader::new(&key[1..]);
    let at = Timestamp(r.u64().expect(WRITTEN_BY_THIS_MODULE));
    (
        at,
        ExecKey {
            arch: r.u8().expect(WRITTEN_BY_THIS_MODULE),
            id: r.str().expect(WRITTEN_BY_THIS_MODULE),
            run: Id128(r.u128().expect(WRITTEN_BY_THIS_MODULE)),
        },
    )
}

pub fn version_tag(version: Option<&DeploymentVersion>) -> String {
    version.map(|v| format!("{}\u{1}{}", v.deployment, v.build_id)).unwrap_or_default()
}

/// Rows of one queue, kind and class: unpinned (`pinned: None`) or pinned to
/// one version.
pub fn queue_prefix(queue: &str, kind: TaskKind, pinned: Option<&DeploymentVersion>) -> Vec<u8> {
    Key::new(QUEUE).str(queue).u8(kind as u8).u8(u8::from(pinned.is_some())).str(&version_tag(pinned)).into_vec()
}

pub fn queue(queue: &str, kind: TaskKind, pinned: Option<&DeploymentVersion>, seq: u64) -> Vec<u8> {
    Key(queue_prefix(queue, kind, pinned)).u64(seq).into_vec()
}

pub fn outbox(key: &ExecKey) -> Vec<u8> {
    exec_key(OUTBOX, key).into_vec()
}

pub fn outbox_prefix() -> Vec<u8> {
    vec![OUTBOX]
}

pub fn parse_outbox(key: &[u8]) -> ExecKey {
    parse_local(key)
}

pub fn change(seq: u64) -> Vec<u8> {
    Key::new(CHANGE).u64(seq).into_vec()
}

pub fn parse_change(key: &[u8]) -> u64 {
    KeyReader::new(&key[1..]).u64().expect(WRITTEN_BY_THIS_MODULE)
}

pub fn visibility(arch: u8, suffix: &[u8]) -> Vec<u8> {
    Key::new(VISIBILITY).u8(arch).raw(suffix).into_vec()
}

pub fn visibility_prefix(arch: u8) -> Vec<u8> {
    Key::new(VISIBILITY).u8(arch).into_vec()
}

pub fn pinned_count(version: &DeploymentVersion) -> Vec<u8> {
    Key::new(PINNED).str(&version.deployment).str(&version.build_id).into_vec()
}

pub fn shard_state() -> Vec<u8> {
    vec![SHARD]
}

pub fn route(deployment: &str) -> Vec<u8> {
    Key::new(ROUTE).str(deployment).into_vec()
}

pub fn route_prefix() -> Vec<u8> {
    vec![ROUTE]
}
