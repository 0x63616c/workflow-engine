//! The worker-deployment archetype: worker versioning's control plane.
//!
//! One execution per deployment, on the shard its name hashes to. It owns
//! the list of versions (registered when their workers first poll) and the
//! routing config (current version, ramping version and percentage).
//!
//! It replicates to other cells like any execution, so a standby knows the
//! routing and which versions still have workflows pinned to them.
//!
//! Its side effects are what make routing cell-wide, and only the active
//! cell runs them:
//!
//! - **Propagate**: every shard needs the routing config to match tasks to
//!   pollers. The home shard's primary sends it until each shard has
//!   acknowledged the current revision.
//! - **Drainage**: a version that stops being current or ramping is
//!   *draining* while open workflows are still pinned to it. A timer
//!   periodically asks every shard for its count; at zero the version is
//!   *drained* and its workers can be retired.

use crate::chasm::{LogicalTask, Summary, Tx};
use crate::shard::DeploymentRouting;
use crate::{Fail, Fallible, decode_stored};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use wfe_core::api::*;
use wfe_core::codec::encode;
use wfe_core::keys::Key;
use wfe_core::{Error, ShardId, Timestamp};
use wfe_lsm::StorageError;

const STATE: &str = "d";

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub status: VersionStatus,
    pub task_queues: BTreeSet<String>,
    pub created_at: Timestamp,
    pub status_changed_at: Timestamp,
    pub pinned_open: u64,
    pub drainage_checked_at: Timestamp,
    pub last_poll_at: Timestamp,
    /// Generation of the drainage check in flight.
    pub drain_nonce: u64,
    /// The generation whose count has been asked for.
    pub drain_asked: u64,
    /// The generation whose count has been received.
    pub drain_counted: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepState {
    pub created_at: Timestamp,
    pub routing: RoutingConfig,
    pub versions: BTreeMap<String, Version>,
    /// Routing revision each shard has acknowledged.
    pub acked: BTreeMap<ShardId, u64>,
    pub deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DepTask {
    Propagate { revision: u64 },
    DrainCheck { build_id: String, nonce: u64 },
    DrainCount { build_id: String, nonce: u64 },
}

fn load(tx: &mut Tx) -> Fallible<DepState> {
    match tx.get::<DepState>(STATE)? {
        Some(st) if !st.deleted => Ok(st),
        _ => Err(Fail::Api(Error::not_found(format!("deployment {} not found", tx.key.id)))),
    }
}

fn changed(tx: &mut Tx, st: &mut DepState) {
    st.routing.revision += 1;
    tx.add_task(None, None, true, &DepTask::Propagate { revision: st.routing.revision });
}

fn set_status(tx: &mut Tx, st: &mut DepState, build_id: &str, status: VersionStatus) {
    let interval = tx.shard.cfg.drainage_interval_ms;
    let now = tx.now;
    let version = st.versions.get_mut(build_id).expect("callers pass a build id they just looked up in st.versions");
    if version.status == status {
        return;
    }
    version.status = status;
    version.status_changed_at = now;
    if status == VersionStatus::Draining {
        version.drain_nonce += 1;
        let nonce = version.drain_nonce;
        tx.add_task(
            Some(now.add_ms(interval)),
            None,
            false,
            &DepTask::DrainCheck { build_id: build_id.to_string(), nonce },
        );
    }
}

fn retire(tx: &mut Tx, st: &mut DepState, build_id: &str) {
    if st.routing.current.as_deref() != Some(build_id) && st.routing.ramping.as_deref() != Some(build_id) {
        set_status(tx, st, build_id, VersionStatus::Draining);
    }
}

fn recreate_keeping_routing_revision(deleted: &DepState, now: Timestamp) -> DepState {
    DepState {
        created_at: now,
        routing: RoutingConfig { revision: deleted.routing.revision, ..RoutingConfig::default() },
        ..DepState::default()
    }
}

/// Deployments replicate between cells, but which shards have the routing
/// and how many workflows are pinned are facts about one cell: the cell
/// that becomes active sends its shards the routing and counts again.
pub fn resume_on_promoted_cell(tx: &mut Tx) -> Result<(), StorageError> {
    let Some(mut st) = tx.get::<DepState>(STATE)? else { return Ok(()) };
    let interval = tx.shard.cfg.drainage_interval_ms;
    let now = tx.now;
    for (build_id, version) in &mut st.versions {
        if version.status == VersionStatus::Draining {
            version.drain_nonce += 1;
            let check = DepTask::DrainCheck { build_id: build_id.clone(), nonce: version.drain_nonce };
            tx.add_task(Some(now.add_ms(interval)), None, false, &check);
        }
    }
    changed(tx, &mut st);
    tx.put(STATE, &st)
}

pub fn register(tx: &mut Tx, build_id: &str, task_queue: &str) -> Fallible<()> {
    Error::require_fields(
        "RegisterWorker",
        &[("deployment", &tx.key.id), ("build_id", build_id), ("task_queue", task_queue)],
    )
    .map_err(Fail::Api)?;
    let mut st = match tx.get::<DepState>(STATE)? {
        Some(st) if !st.deleted => st,
        Some(deleted) => recreate_keeping_routing_revision(&deleted, tx.now),
        None => DepState { created_at: tx.now, ..DepState::default() },
    };
    let now = tx.now;
    let version = st.versions.entry(build_id.to_string()).or_insert_with(|| Version {
        created_at: now,
        status_changed_at: now,
        ..Version::default()
    });
    version.last_poll_at = now;
    if version.task_queues.insert(task_queue.to_string()) {
        changed(tx, &mut st);
    }
    tx.put(STATE, &st)?;
    Ok(())
}

fn require(st: &DepState, build_id: &str) -> Fallible<()> {
    if !st.versions.contains_key(build_id) {
        return Err(Fail::Api(Error::precondition(format!(
            "version {build_id} is not registered: no worker of that build has polled yet"
        ))));
    }
    Ok(())
}

pub fn set_current(tx: &mut Tx, build_id: &Option<String>) -> Fallible<()> {
    let mut st = load(tx)?;
    if st.routing.current == *build_id {
        return Ok(());
    }
    if let Some(build_id) = build_id {
        require(&st, build_id)?;
    }
    let previous = st.routing.current.take();
    st.routing.current = build_id.clone();
    st.routing.current_changed_at = tx.now;
    if st.routing.ramping == *build_id && build_id.is_some() {
        st.routing.ramping = None;
        st.routing.ramp_percent = 0;
        st.routing.ramping_changed_at = tx.now;
    }
    if let Some(build_id) = build_id {
        set_status(tx, &mut st, build_id, VersionStatus::Current);
    }
    if let Some(previous) = previous {
        retire(tx, &mut st, &previous);
    }
    changed(tx, &mut st);
    tx.put(STATE, &st)?;
    Ok(())
}

pub fn set_ramping(tx: &mut Tx, build_id: &Option<String>, percent: u8) -> Fallible<()> {
    let mut st = load(tx)?;
    if percent > 100 {
        return Err(Fail::Api(Error::invalid(format!(
            "SetRampingVersion percent is {percent}: it is the share of new workflows sent to the ramping \
             version, so pass 0 to 100"
        ))));
    }
    let percent = if build_id.is_some() { percent } else { 0 };
    if st.routing.ramping == *build_id && st.routing.ramp_percent == percent {
        return Ok(());
    }
    if let Some(build_id) = build_id {
        require(&st, build_id)?;
        if st.routing.current.as_ref() == Some(build_id) {
            return Err(Fail::Api(Error::precondition(format!(
                "version {build_id} is already current, so it cannot also ramp: ramp a different build, or \
                 clear the ramp with no build id"
            ))));
        }
    }
    let previous = st.routing.ramping.take();
    st.routing.ramping = build_id.clone();
    st.routing.ramp_percent = percent;
    st.routing.ramping_changed_at = tx.now;
    if let Some(build_id) = build_id {
        set_status(tx, &mut st, build_id, VersionStatus::Ramping);
    }
    if let Some(previous) = previous {
        retire(tx, &mut st, &previous);
    }
    changed(tx, &mut st);
    tx.put(STATE, &st)?;
    Ok(())
}

pub fn delete_version(tx: &mut Tx, build_id: &str) -> Fallible<()> {
    let mut st = load(tx)?;
    let Some(version) = st.versions.get(build_id) else { return Ok(()) };
    if !matches!(version.status, VersionStatus::Inactive | VersionStatus::Drained) {
        return Err(Fail::Api(Error::precondition(format!(
            "version {build_id} is {:?}; only inactive or drained versions can be deleted: make another \
             version current (or clear its ramp) and wait for its pinned workflows to close",
            version.status
        ))));
    }
    st.versions.remove(build_id);
    changed(tx, &mut st);
    tx.put(STATE, &st)?;
    Ok(())
}

pub fn delete(tx: &mut Tx) -> Fallible<()> {
    let mut st = load(tx)?;
    if !st.versions.is_empty() {
        let versions: Vec<&str> = st.versions.keys().map(String::as_str).collect();
        return Err(Fail::Api(Error::precondition(format!(
            "deployment {} still has versions {}: delete each with DeleteVersion first",
            tx.key.id,
            versions.join(", ")
        ))));
    }
    st.deleted = true;
    st.routing = RoutingConfig { revision: st.routing.revision, ..RoutingConfig::default() };
    changed(tx, &mut st);
    tx.put(STATE, &st)?;
    Ok(())
}

fn info(tx: &Tx, st: &DepState) -> DeploymentInfo {
    let shard_count = tx.shard.cfg.shard_count;
    DeploymentInfo {
        name: tx.key.id.clone(),
        created_at: st.created_at,
        routing: st.routing.clone(),
        versions: st
            .versions
            .iter()
            .map(|(build_id, v)| VersionInfo {
                build_id: build_id.clone(),
                status: v.status,
                task_queues: v.task_queues.iter().cloned().collect(),
                created_at: v.created_at,
                status_changed_at: v.status_changed_at,
                pinned_open: v.pinned_open,
                drainage_checked_at: v.drainage_checked_at,
                last_poll_at: v.last_poll_at,
            })
            .collect(),
        propagated_shards: u32::try_from(
            (0..shard_count).filter(|s| st.acked.get(s).copied().unwrap_or(0) >= st.routing.revision).count(),
        )
        .expect("at most shard_count shards are counted, and shard_count is a u32"),
        shard_count,
    }
}

pub fn describe(tx: &mut Tx) -> Fallible<DeploymentInfo> {
    let st = load(tx)?;
    Ok(info(tx, &st))
}

/// What shards need to know.
pub fn routing(tx: &mut Tx) -> Result<Option<DeploymentRouting>, StorageError> {
    Ok(tx.get::<DepState>(STATE)?.map(|st| DeploymentRouting {
        deployment: tx.key.id.clone(),
        task_queues: st.versions.values().flat_map(|v| v.task_queues.iter().cloned()).collect(),
        routing: st.routing,
        deleted: st.deleted,
    }))
}

/// Shards that still have to acknowledge `revision`.
pub fn unacked(tx: &mut Tx) -> Result<Vec<ShardId>, StorageError> {
    let Some(st) = tx.get::<DepState>(STATE)? else { return Ok(Vec::new()) };
    let revision = st.routing.revision;
    Ok((0..tx.shard.cfg.shard_count).filter(|s| st.acked.get(s).copied().unwrap_or(0) < revision).collect())
}

pub fn acked(tx: &mut Tx, shard: ShardId, revision: u64) -> Result<(), StorageError> {
    let Some(mut st) = tx.get::<DepState>(STATE)? else { return Ok(()) };
    let entry = st.acked.entry(shard).or_insert(0);
    if *entry < revision {
        *entry = revision;
        tx.put(STATE, &st)?;
    }
    Ok(())
}

/// The cell-wide count of open workflows pinned to a draining version.
pub fn drainage_result(tx: &mut Tx, build_id: &str, nonce: u64, count: u64) -> Result<(), StorageError> {
    let Some(mut st) = tx.get::<DepState>(STATE)? else { return Ok(()) };
    let interval = tx.shard.cfg.drainage_interval_ms;
    let now = tx.now;
    let Some(version) = st.versions.get_mut(build_id) else { return Ok(()) };
    if version.status != VersionStatus::Draining || version.drain_nonce != nonce || version.drain_counted >= nonce {
        return Ok(());
    }
    version.drain_counted = nonce;
    version.pinned_open = count;
    version.drainage_checked_at = now;
    if count == 0 {
        version.status = VersionStatus::Drained;
        version.status_changed_at = now;
    } else {
        version.drain_nonce += 1;
        let nonce = version.drain_nonce;
        tx.add_task(
            Some(now.add_ms(interval)),
            None,
            false,
            &DepTask::DrainCheck { build_id: build_id.to_string(), nonce },
        );
    }
    tx.put(STATE, &st)
}

pub fn validate(tx: &mut Tx, task: &LogicalTask) -> Result<bool, StorageError> {
    let Some(st) = tx.get::<DepState>(STATE)? else { return Ok(false) };
    let draining = |build_id: &str| st.versions.get(build_id).filter(|v| v.status == VersionStatus::Draining);
    Ok(match decode_stored::<DepTask>(&task.body) {
        DepTask::Propagate { revision } => {
            revision == st.routing.revision
                && (0..tx.shard.cfg.shard_count).any(|s| st.acked.get(&s).copied().unwrap_or(0) < revision)
        }
        DepTask::DrainCheck { build_id, nonce } => {
            draining(&build_id).is_some_and(|v| v.drain_nonce == nonce && v.drain_asked < nonce)
        }
        DepTask::DrainCount { build_id, nonce } => {
            draining(&build_id).is_some_and(|v| v.drain_nonce == nonce && v.drain_counted < nonce)
        }
    })
}

/// The drainage timer fired: ask every shard for its count.
pub fn fire(tx: &mut Tx, task: &LogicalTask) -> Result<(), StorageError> {
    match decode_stored::<DepTask>(&task.body) {
        DepTask::DrainCheck { build_id, nonce } => {
            let mut st: DepState =
                tx.get(STATE)?.expect("validate() returned true only because the deployment state exists");
            st.versions
                .get_mut(&build_id)
                .expect("validate() returned true only because this version is draining")
                .drain_asked = nonce;
            tx.put(STATE, &st)?;
            tx.add_task(None, None, true, &DepTask::DrainCount { build_id, nonce });
            Ok(())
        }
        DepTask::Propagate { .. } | DepTask::DrainCount { .. } => unreachable!("only drainage checks are timers"),
    }
}

/// Pending side effects, for the primary.
pub fn outbound(tx: &mut Tx) -> Vec<DepTask> {
    tx.meta.tasks.iter().filter(|t| t.outbound).map(|t| decode_stored(&t.body)).collect()
}

pub fn summary(tx: &mut Tx) -> Result<Summary, StorageError> {
    let Some(st) = tx.get::<DepState>(STATE)? else { return Ok(Summary::default()) };
    if st.deleted {
        return Ok(Summary::default());
    }
    let suffix = Key(Vec::new()).str(&tx.key.id).into_vec();
    Ok(Summary { visibility: Some((suffix, encode(&info(tx, &st)))), open: false, counted: None })
}
