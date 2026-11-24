//! Shard-wide state: counters, the cell's role, replication links and the
//! worker-versioning routes propagated from deployments.

use crate::{decode_stored, keys};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use wfe_core::api::{CellRef, DeploymentVersion, RoutingConfig};
use wfe_core::codec::encode;
use wfe_core::{CellId, ShardId, Timestamp};
use wfe_lsm::StorageError;
use wfe_vsr::Store;

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub cell: CellId,
    pub shard: ShardId,
    pub shard_count: u32,
    pub replica_count: u8,
    /// Most bytes of history or replicated state in one message.
    pub page_bytes_max: usize,
    pub workflow_task_timeout_ms: u64,
    pub activity_start_to_close_ms: u64,
    /// How often a draining version recounts its pinned workflows.
    pub drainage_interval_ms: u64,
    /// How often the primary retries pending side effects.
    pub outbound_interval_ms: u64,
    /// Executions shipped concurrently per replication link.
    pub replication_window: usize,
}

impl EngineConfig {
    pub fn new(cell: CellId, shard: ShardId, shard_count: u32, replica_count: u8) -> EngineConfig {
        EngineConfig {
            cell,
            shard,
            shard_count,
            replica_count,
            page_bytes_max: 256 * 1024,
            workflow_task_timeout_ms: 10_000,
            activity_start_to_close_ms: 60_000,
            drainage_interval_ms: 5_000,
            outbound_interval_ms: 200,
            replication_window: 8,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Link {
    pub target: CellRef,
    /// Every change at or below this has been acknowledged by the target.
    pub acked_seq: u64,
    pub paused: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardState {
    pub active: bool,
    pub epoch: u64,
    pub change_seq: u64,
    pub queue_seq: u64,
    pub executions: u64,
    pub open_workflows: u64,
    pub replicated_in: u64,
    pub links: BTreeMap<CellId, Link>,
}

impl Default for ShardState {
    fn default() -> ShardState {
        ShardState {
            active: true,
            epoch: 1,
            change_seq: 0,
            queue_seq: 0,
            executions: 0,
            open_workflows: 0,
            replicated_in: 0,
            links: BTreeMap::new(),
        }
    }
}

/// A deployment's routing as every shard knows it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRouting {
    pub deployment: String,
    pub routing: RoutingConfig,
    pub task_queues: BTreeSet<String>,
    pub deleted: bool,
}

pub struct Shard {
    pub cfg: EngineConfig,
    pub state: ShardState,
    saved: ShardState,
    pub routes: BTreeMap<String, DeploymentRouting>,
    cached_next_timer: Option<Option<Timestamp>>,
}

impl Shard {
    pub fn new(cfg: EngineConfig) -> Shard {
        Shard {
            cfg,
            state: ShardState::default(),
            saved: ShardState::default(),
            routes: BTreeMap::new(),
            cached_next_timer: None,
        }
    }

    pub fn load(&mut self, store: &mut Store) -> Result<(), StorageError> {
        self.state = store.get(&keys::shard_state())?.map(|b| decode_stored(&b)).unwrap_or_default();
        self.saved = self.state.clone();
        self.routes.clear();
        let prefix = keys::route_prefix();
        for (_, value) in store.scan(&prefix, &wfe_core::keys::prefix_end(&prefix), usize::MAX, false)? {
            let route: DeploymentRouting = decode_stored(&value);
            self.routes.insert(route.deployment.clone(), route);
        }
        self.cached_next_timer = None;
        self.refresh_timer(store)
    }

    pub fn save(&mut self, store: &mut Store) -> Result<(), StorageError> {
        if self.state != self.saved {
            store.put(&keys::shard_state(), &encode(&self.state))?;
            self.saved = self.state.clone();
        }
        Ok(())
    }

    pub fn timer_added(&mut self, at: Timestamp) {
        if let Some(next) = &mut self.cached_next_timer
            && next.is_none_or(|n| at < n)
        {
            *next = Some(at);
        }
    }

    /// Forget the cached wake-up time (after timers ran or rows went away).
    pub fn timer_invalidate(&mut self) {
        self.cached_next_timer = None;
    }

    pub fn refresh_timer(&mut self, store: &mut Store) -> Result<(), StorageError> {
        if self.cached_next_timer.is_none() {
            let prefix = keys::timer_prefix();
            let first = store.scan(&prefix, &wfe_core::keys::prefix_end(&prefix), 1, false)?;
            self.cached_next_timer = Some(first.first().map(|(key, _)| keys::parse_timer(key).0));
        }
        Ok(())
    }

    pub fn next_timer(&self) -> Option<Timestamp> {
        self.cached_next_timer.flatten()
    }

    /// Deleted deployments stay as tombstones so that stale news about them
    /// is recognized as stale.
    pub fn set_route(&mut self, store: &mut Store, route: DeploymentRouting) -> Result<(), StorageError> {
        store.put(&keys::route(&route.deployment), &encode(&route))?;
        self.routes.insert(route.deployment.clone(), route);
        Ok(())
    }

    /// The version an unpinned task of `queue` should run on: `None` means
    /// unversioned workers.
    pub fn target(&self, queue: &str, ramp: u8) -> Option<DeploymentVersion> {
        let governing = self
            .routes
            .values()
            .filter(|r| {
                !r.deleted
                    && r.task_queues.contains(queue)
                    && (r.routing.current.is_some() || r.routing.ramping.is_some())
            })
            .max_by_key(|r| (r.routing.current_changed_at.max(r.routing.ramping_changed_at), &r.deployment))?;
        let routing = &governing.routing;
        let build = match &routing.ramping {
            Some(ramping) if ramp < routing.ramp_percent => Some(ramping),
            _ => routing.current.as_ref(),
        };
        build.map(|b| DeploymentVersion::new(&governing.deployment, b))
    }
}
