//! The workflow engine: the deterministic state machine each shard
//! replicates.
//!
//! Built on the execution framework in [`chasm`], with two archetypes:
//! [`workflow`] (event-history workflows driven by workers) and
//! [`deployment`] (worker versioning). Everything a shard does is one of:
//!
//! - an [`Op`] applied from the log: an API call or the write-back of a side
//!   effect;
//! - a pulse: timers that are due, run in log order with the log's clock;
//! - a side effect performed by the primary outside the log
//!   ([`Engine::drive`]): telling other shards about routing changes,
//!   counting pinned workflows, shipping changed executions to another cell.
//!
//! Requests can be applied more than once (the transport retries), so every
//! operation is idempotent.

pub mod chasm;
pub mod deployment;
pub mod keys;
pub mod shard;
pub mod workflow;

pub use shard::{DeploymentRouting, EngineConfig, Link, Shard, ShardState};

use chasm::{DEPLOYMENT, ExecKey, Hooks, Local, LogicalTask, Meta, Node, Summary, TaskRef, Tx, WORKFLOW};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use wfe_core::api::*;
use wfe_core::codec::{decode, encode};
use wfe_core::ids::shard_for;
use wfe_core::keys::prefix_end;
use wfe_core::{Addr, CellId, Error, Id128, ShardId, Timestamp};
use wfe_lsm::StorageError;
use wfe_vsr::{Effects, OpContext, StateMachine, Store};

#[derive(Debug)]
pub enum Fail {
    Storage(StorageError),
    Api(Error),
}

impl From<StorageError> for Fail {
    fn from(e: StorageError) -> Fail {
        Fail::Storage(e)
    }
}

impl From<Error> for Fail {
    fn from(e: Error) -> Fail {
        Fail::Api(e)
    }
}

pub type Fallible<T> = Result<T, Fail>;

pub(crate) fn decode_stored<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
    decode(bytes).unwrap_or_else(|e| {
        panic!("bytes this state machine stored no longer decode as {}: {e}", std::any::type_name::<T>())
    })
}

/// One entry of a shard's log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Api(ApiRequest),
    ApplyRouting(DeploymentRouting),
    RoutingAcked { deployment: String, shard: ShardId, revision: u64 },
    DrainageResult { version: DeploymentVersion, nonce: u64, count: u64 },
    ApplyReplication(Chunk),
    ReplicationShipped { cell: CellId, items: Vec<(ExecKey, u64)> },
}

/// Part of an execution's state on its way to another cell.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Chunk {
    pub source_cell: CellId,
    pub source_shard: ShardId,
    pub key: ExecKey,
    /// The execution's position in the source's change index: names the
    /// state being shipped.
    pub seq: u64,
    /// The chunk covers the node paths in `(after, through]`. Chunks are
    /// addressed by path, not by number, because where a transfer is cut
    /// into chunks is not stable (it depends on what the source knows the
    /// target already has).
    pub after: String,
    pub through: String,
    /// The last chunk carries the root node.
    pub last: bool,
    pub nodes: Vec<(String, Node)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProbeQueue {
    pub task_queue: String,
    pub kind: TaskKind,
    pub version: Option<DeploymentVersion>,
}

/// Messages between engines, and between engines and gateways, outside the
/// log.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EnginePacket {
    /// A gateway asks whether long polls have something to wait for.
    Probe {
        id: u64,
        queues: Vec<ProbeQueue>,
        workflows: Vec<(String, Option<Id128>)>,
    },
    /// Indexes into the probe's lists: queues with a task, closed workflows.
    ProbeResult {
        id: u64,
        shard: ShardId,
        queues: Vec<u32>,
        workflows: Vec<u32>,
    },
    Routing(DeploymentRouting),
    RoutingAck {
        deployment: String,
        shard: ShardId,
        revision: u64,
    },
    CountPinned {
        version: DeploymentVersion,
        nonce: u64,
    },
    PinnedCount {
        version: DeploymentVersion,
        nonce: u64,
        shard: ShardId,
        count: u64,
    },
    Replicate(Chunk),
    /// The target holds state `seq` of `key` through path `through`.
    ReplicateAck {
        cell: CellId,
        key: ExecKey,
        seq: u64,
        through: String,
        done: bool,
    },
}

/// What a shard has received of an execution from one source cell.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Received {
    pub seq: u64,
    /// Node paths up to here have arrived.
    pub through: String,
    pub done: bool,
    /// Something in this (possibly superseded) transfer was newer than
    /// what was here.
    pub taken: bool,
}

impl Received {
    fn is_next_chunk(&self, chunk: &Chunk) -> bool {
        (chunk.seq == self.seq && !self.done && chunk.after == self.through)
            || (chunk.seq > self.seq && chunk.after.is_empty())
    }
}

struct Registry;

impl Hooks for Registry {
    fn validate(&self, tx: &mut Tx, task: &LogicalTask) -> Result<bool, StorageError> {
        match tx.key.arch {
            WORKFLOW => workflow::validate(tx, task),
            DEPLOYMENT => deployment::validate(tx, task),
            _ => Ok(false),
        }
    }
    fn summary(&self, tx: &mut Tx) -> Result<Summary, StorageError> {
        match tx.key.arch {
            WORKFLOW => workflow::summary(tx),
            DEPLOYMENT => deployment::summary(tx),
            _ => Ok(Summary::default()),
        }
    }
}

fn fire(tx: &mut Tx, task: &LogicalTask) -> Result<(), StorageError> {
    match tx.key.arch {
        WORKFLOW => workflow::fire(tx, task),
        DEPLOYMENT => deployment::fire(tx, task),
        _ => Ok(()),
    }
}

struct Transfer {
    seq: u64,
    last_acked_path: String,
}

pub struct Engine {
    shard: Shard,
    primary: PrimaryVolatile,
}

#[derive(Default)]
struct PrimaryVolatile {
    drive_at: Timestamp,
    proposed: BTreeSet<Vec<u8>>,
    drainage: BTreeMap<DeploymentVersion, DrainageTally>,
    transfers: BTreeMap<CellId, BTreeMap<ExecKey, Transfer>>,
    shipped: BTreeMap<CellId, BTreeMap<ExecKey, u64>>,
}

#[derive(Default)]
struct DrainageTally {
    nonce: u64,
    counts: BTreeMap<ShardId, u64>,
    proposed: bool,
}

impl DrainageTally {
    fn record(&mut self, nonce: u64, shard: ShardId, count: u64, shard_count: u32) -> Option<u64> {
        if nonce > self.nonce {
            *self = Self { nonce, ..Self::default() };
        }
        if nonce < self.nonce || self.proposed {
            return None;
        }
        self.counts.insert(shard, count);
        if u32::try_from(self.counts.len()) != Ok(shard_count) {
            return None;
        }
        self.proposed = true;
        Some(std::mem::take(&mut self.counts).into_values().sum())
    }
}

impl PrimaryVolatile {
    fn drive_on_next_tick(&mut self) {
        self.drive_at = Timestamp::ZERO;
    }
}

fn run_id_for_start_request(workflow_id: &str, request_id: Id128) -> Id128 {
    Id128::derive(&[workflow_id.as_bytes(), &request_id.0.to_le_bytes()])
}

fn probe_index(i: usize) -> u32 {
    u32::try_from(i).expect("a probe lists far fewer than 2^32 queues or workflows")
}

fn progress_this_transfer_delivered(received: Received, chunk_seq: u64) -> (String, bool) {
    if received.seq == chunk_seq { (received.through, received.done) } else { (String::new(), false) }
}

fn deployment_key(name: &str) -> ExecKey {
    ExecKey { arch: DEPLOYMENT, id: name.to_string(), run: Id128::ZERO }
}

impl Engine {
    pub fn new(cfg: EngineConfig) -> Engine {
        Engine { shard: Shard::new(cfg), primary: PrimaryVolatile::default() }
    }

    pub fn shard(&self) -> &Shard {
        &self.shard
    }

    fn writable(&self) -> Fallible<()> {
        if !self.shard.state.active {
            return Err(Fail::Api(Error::not_active("this cell is a standby: it only accepts replication")));
        }
        Ok(())
    }

    fn resolve(&mut self, store: &mut Store, id: &str, run: Option<Id128>) -> Fallible<ExecKey> {
        let run = match run {
            Some(run) => run,
            None => match store.get(&keys::current(WORKFLOW, id))? {
                Some(bytes) => decode_stored::<(Id128, Timestamp)>(&bytes).0,
                None => return Err(Fail::Api(Error::not_found(format!("workflow {id} not found")))),
            },
        };
        Ok(ExecKey { arch: WORKFLOW, id: id.to_string(), run })
    }

    fn on_workflow<T>(
        &mut self,
        store: &mut Store,
        now: Timestamp,
        id: &str,
        run: Option<Id128>,
        f: impl FnOnce(&mut Tx) -> Fallible<T>,
    ) -> Fallible<T> {
        let key = self.resolve(store, id, run)?;
        let mut tx = Tx::open(store, &mut self.shard, now, key)?;
        let out = f(&mut tx)?;
        tx.commit(&Registry, false)?;
        Ok(out)
    }

    fn on_deployment<T>(
        &mut self,
        store: &mut Store,
        now: Timestamp,
        name: &str,
        f: impl FnOnce(&mut Tx) -> Fallible<T>,
    ) -> Fallible<T> {
        let mut tx = Tx::open(store, &mut self.shard, now, deployment_key(name))?;
        let out = f(&mut tx)?;
        tx.commit(&Registry, false)?;
        Ok(out)
    }

    fn start_workflow(
        &mut self,
        store: &mut Store,
        now: Timestamp,
        req: &StartWorkflow,
        run_id: Option<Id128>,
        continued_from: Option<Id128>,
    ) -> Fallible<ApiResponse> {
        Error::require_fields(
            "StartWorkflow",
            &[
                ("workflow_id", &req.workflow_id),
                ("workflow_type", &req.workflow_type),
                ("task_queue", &req.task_queue),
            ],
        )
        .map_err(Fail::Api)?;
        let pointer = keys::current(WORKFLOW, &req.workflow_id);
        if run_id.is_none() && !req.request_id.is_zero() {
            let retried_start_run = run_id_for_start_request(&req.workflow_id, req.request_id);
            let key = ExecKey { arch: WORKFLOW, id: req.workflow_id.clone(), run: retried_start_run };
            if Tx::open(store, &mut self.shard, now, key)?.exists() {
                return Ok(ApiResponse::Started { run_id: retried_start_run, already_started: false });
            }
        }
        if let Some(bytes) = store.get(&pointer)? {
            let (run, _) = decode_stored::<(Id128, Timestamp)>(&bytes);
            let key = ExecKey { arch: WORKFLOW, id: req.workflow_id.clone(), run };
            let mut tx = Tx::open(store, &mut self.shard, now, key)?;
            if let Some(st) = workflow::peek(&mut tx)? {
                if !req.request_id.is_zero() && st.start_request == req.request_id {
                    return Ok(ApiResponse::Started { run_id: run, already_started: false });
                }
                if !st.status.is_closed() && continued_from.is_none() {
                    return match req.id_conflict {
                        IdConflictPolicy::UseExisting => {
                            Ok(ApiResponse::Started { run_id: run, already_started: true })
                        }
                        IdConflictPolicy::Fail => Err(Fail::Api(Error::already_exists(format!(
                            "workflow {} is already running",
                            req.workflow_id
                        )))),
                    };
                }
            }
        }
        let run = run_id.unwrap_or_else(|| {
            if req.request_id.is_zero() {
                Id128::derive(&[
                    req.workflow_id.as_bytes(),
                    &now.micros().to_le_bytes(),
                    &self.shard.state.change_seq.to_le_bytes(),
                ])
            } else {
                run_id_for_start_request(&req.workflow_id, req.request_id)
            }
        });
        let key = ExecKey { arch: WORKFLOW, id: req.workflow_id.clone(), run };
        let mut tx = Tx::open(store, &mut self.shard, now, key)?;
        if tx.exists() {
            return Ok(ApiResponse::Started { run_id: run, already_started: true });
        }
        workflow::start(&mut tx, req, continued_from)?;
        tx.commit(&Registry, false)?;
        store.put(&pointer, &encode(&(run, now)))?;
        Ok(ApiResponse::Started { run_id: run, already_started: false })
    }

    fn candidates(
        shard: &Shard,
        store: &mut Store,
        queue: &str,
        kind: TaskKind,
        version: Option<&DeploymentVersion>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, TaskRef)>, StorageError> {
        let mut out = Vec::new();
        if version.is_some() {
            let prefix = keys::queue_prefix(queue, kind, version);
            for (key, value) in store.scan(&prefix, &prefix_end(&prefix), limit, false)? {
                out.push((key, decode_stored::<TaskRef>(&value)));
            }
        }
        if out.len() < limit {
            let prefix = keys::queue_prefix(queue, kind, None);
            for (key, value) in store.scan(&prefix, &prefix_end(&prefix), 64, false)? {
                let task: TaskRef = decode_stored(&value);
                if shard.target(queue, task.ramp).as_ref() == version {
                    out.push((key, task));
                    if out.len() >= limit {
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    fn poll(
        &mut self,
        store: &mut Store,
        now: Timestamp,
        queue: &str,
        kind: TaskKind,
        worker: &WorkerInfo,
    ) -> Fallible<ApiResponse> {
        self.writable()?;
        for (row, task_ref) in Self::candidates(&self.shard, store, queue, kind, worker.version.as_ref(), 8)? {
            let mut tx = Tx::open(store, &mut self.shard, now, task_ref.key.clone())?;
            let Some(task) = tx.meta.tasks.iter().find(|t| t.id == task_ref.task_id).cloned() else {
                panic!("queue row {row:?} has no logical task");
            };
            let started = match kind {
                TaskKind::Workflow => {
                    workflow::start_wft(&mut tx, &task, worker).map(|t| ApiResponse::WorkflowTask(Some(Box::new(t))))
                }
                TaskKind::Activity => workflow::start_activity(&mut tx, &task, worker)
                    .map(|t| ApiResponse::ActivityTask(Some(Box::new(t)))),
            };
            match started {
                Ok(response) => {
                    tx.commit(&Registry, false)?;
                    return Ok(response);
                }
                Err(Fail::Api(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(match kind {
            TaskKind::Workflow => ApiResponse::WorkflowTask(None),
            TaskKind::Activity => ApiResponse::ActivityTask(None),
        })
    }

    fn list_workflows(&mut self, store: &mut Store, filter: &ListFilter, limit: usize) -> Fallible<ApiResponse> {
        let prefix = keys::visibility_prefix(WORKFLOW);
        let end = prefix_end(&prefix);
        let limit = limit.clamp(1, 1000);
        let mut out = Vec::new();
        let mut cursor = prefix;
        let mut scanned = 0;
        while out.len() < limit && scanned < 20_000 {
            let rows = store.scan(&cursor, &end, 256, false)?;
            if rows.is_empty() {
                break;
            }
            scanned += rows.len();
            for (_, value) in &rows {
                let info: WorkflowInfo = decode_stored(value);
                let keep = filter.status.is_none_or(|s| s == info.status)
                    && filter.workflow_type.as_ref().is_none_or(|t| *t == info.workflow_type)
                    && filter.id_prefix.as_ref().is_none_or(|p| info.workflow_id.starts_with(p.as_str()));
                if keep && out.len() < limit {
                    out.push(info);
                }
            }
            let Some((last_key, _)) = rows.last() else { break };
            cursor = last_key.clone();
            cursor.push(0);
        }
        Ok(ApiResponse::Workflows(out))
    }

    fn shard_info(&mut self, store: &mut Store, ctx: &OpContext) -> Fallible<ShardInfo> {
        let state = &self.shard.state;
        let mut replication = Vec::new();
        for link in state.links.values() {
            let start = keys::change(link.acked_seq + 1);
            let pending = store.scan(&start, &keys::change(u64::MAX), 10_000, false)?.len() as u64;
            replication.push(ReplicationStatus {
                target: link.target.clone(),
                shard: self.shard.cfg.shard,
                acked_seq: link.acked_seq,
                head_seq: state.change_seq,
                pending,
                paused: link.paused,
            });
        }
        Ok(ShardInfo {
            shard: self.shard.cfg.shard,
            active: state.active,
            epoch: state.epoch,
            executions: state.executions,
            open_workflows: state.open_workflows,
            change_seq: state.change_seq,
            op: ctx.op,
            timestamp: ctx.timestamp,
            replication,
            replicated_in: state.replicated_in,
        })
    }

    fn refresh_rows(&mut self, store: &mut Store, now: Timestamp) -> Result<(), StorageError> {
        let prefix = keys::local_prefix();
        let end = prefix_end(&prefix);
        let mut cursor = prefix;
        loop {
            let rows = store.scan(&cursor, &end, 256, false)?;
            if rows.is_empty() {
                break;
            }
            for (key, _) in &rows {
                let exec = keys::parse_local(key);
                let mut tx = Tx::open(store, &mut self.shard, now, exec)?;
                let a_deployment_on_the_newly_active_cell = tx.key.arch == DEPLOYMENT && tx.shard.state.active;
                if a_deployment_on_the_newly_active_cell {
                    deployment::resume_on_promoted_cell(&mut tx)?;
                    tx.commit(&Registry, false)?;
                } else {
                    let summary = Registry.summary(&mut tx)?;
                    tx.finish(summary, false)?;
                }
            }
            let Some((last_key, _)) = rows.last() else { break };
            cursor = last_key.clone();
            cursor.push(0);
        }
        self.shard.timer_invalidate();
        Ok(())
    }

    fn api(&mut self, store: &mut Store, ctx: &OpContext, request: ApiRequest) -> Fallible<ApiResponse> {
        let now = ctx.timestamp;
        match request {
            ApiRequest::StartWorkflow(req) => {
                self.writable()?;
                self.start_workflow(store, now, &req, None, None)
            }
            ApiRequest::SignalWorkflow { workflow_id, run_id, name, input, request_id } => {
                self.writable()?;
                self.on_workflow(store, now, &workflow_id, run_id, |tx| {
                    workflow::signal(tx, &name, &input, request_id)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::CancelWorkflow { workflow_id, run_id, reason } => {
                self.writable()?;
                self.on_workflow(store, now, &workflow_id, run_id, |tx| workflow::cancel(tx, &reason))?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::TerminateWorkflow { workflow_id, run_id, reason } => {
                self.writable()?;
                self.on_workflow(store, now, &workflow_id, run_id, |tx| workflow::terminate(tx, &reason))?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::SetVersioningOverride { workflow_id, run_id, value } => {
                self.writable()?;
                self.on_workflow(store, now, &workflow_id, run_id, |tx| workflow::set_override(tx, &value))?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::DescribeWorkflow { workflow_id, run_id }
            | ApiRequest::GetResult { workflow_id, run_id, .. } => {
                let d = self.on_workflow(store, now, &workflow_id, run_id, workflow::describe)?;
                Ok(ApiResponse::Workflow(Box::new(d)))
            }
            ApiRequest::GetHistory { workflow_id, run_id, after_event_id, limit } => {
                let (events, next_event_id) = self.on_workflow(store, now, &workflow_id, run_id, |tx| {
                    workflow::history(tx, after_event_id, limit as usize)
                })?;
                Ok(ApiResponse::History { events, next_event_id })
            }
            ApiRequest::ListWorkflows { filter, limit } => self.list_workflows(store, &filter, limit as usize),

            ApiRequest::PollWorkflowTask { task_queue, worker, .. } => {
                self.poll(store, now, &task_queue, TaskKind::Workflow, &worker)
            }
            ApiRequest::PollActivityTask { task_queue, worker, .. } => {
                self.poll(store, now, &task_queue, TaskKind::Activity, &worker)
            }
            ApiRequest::CompleteWorkflowTask { token, commands, worker, behavior } => {
                self.writable()?;
                let followup = self.on_workflow(store, now, &token.workflow_id, Some(token.run_id), |tx| {
                    workflow::complete_wft(tx, &token, &commands, &worker, behavior)
                })?;
                if let workflow::Followup::ContinueAsNew { run_id, request } = followup {
                    self.start_workflow(store, now, &request, Some(run_id), Some(token.run_id))?;
                }
                Ok(ApiResponse::Ok)
            }
            ApiRequest::FailWorkflowTask { token, failure } => {
                self.writable()?;
                self.on_workflow(store, now, &token.workflow_id, Some(token.run_id), |tx| {
                    workflow::fail_wft(tx, &token, &failure)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::CompleteActivity { token, result } => {
                self.writable()?;
                self.on_workflow(store, now, &token.workflow_id, Some(token.run_id), |tx| {
                    workflow::complete_activity(tx, &token, &result)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::FailActivity { token, failure } => {
                self.writable()?;
                self.on_workflow(store, now, &token.workflow_id, Some(token.run_id), |tx| {
                    workflow::fail_activity(tx, &token, &failure)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::HeartbeatActivity { token, details } => {
                self.writable()?;
                let cancel_requested = self.on_workflow(store, now, &token.workflow_id, Some(token.run_id), |tx| {
                    workflow::heartbeat(tx, &token, &details)
                })?;
                Ok(ApiResponse::Heartbeat { cancel_requested })
            }

            ApiRequest::RegisterWorker { version, task_queue } => {
                self.writable()?;
                self.on_deployment(store, now, &version.deployment, |tx| {
                    deployment::register(tx, &version.build_id, &task_queue)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::SetCurrentVersion { deployment, build_id } => {
                self.writable()?;
                self.on_deployment(store, now, &deployment, |tx| deployment::set_current(tx, &build_id))?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::SetRampingVersion { deployment, build_id, percent } => {
                self.writable()?;
                self.on_deployment(store, now, &deployment, |tx| deployment::set_ramping(tx, &build_id, percent))?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::DeleteVersion { version } => {
                self.writable()?;
                self.on_deployment(store, now, &version.deployment, |tx| {
                    deployment::delete_version(tx, &version.build_id)
                })?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::DeleteDeployment { deployment } => {
                self.writable()?;
                self.on_deployment(store, now, &deployment, deployment::delete)?;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::DescribeDeployment { deployment } => {
                let info = self.on_deployment(store, now, &deployment, deployment::describe)?;
                Ok(ApiResponse::Deployment(Box::new(info)))
            }
            ApiRequest::ListDeployments => {
                let prefix = keys::visibility_prefix(DEPLOYMENT);
                let rows = store.scan(&prefix, &prefix_end(&prefix), 10_000, false)?;
                Ok(ApiResponse::Deployments(rows.iter().map(|(_, v)| decode_stored(v)).collect()))
            }

            ApiRequest::DescribeCell => Ok(ApiResponse::Shard(Box::new(self.shard_info(store, ctx)?))),
            ApiRequest::CreateReplication { target } => {
                if target.cell == self.shard.cfg.cell || target.shards == 0 || target.replicas == 0 {
                    return Err(Fail::Api(Error::invalid(format!(
                        "CreateReplication target is cell {} with {} shards and {} replicas; this is cell {}: \
                         replicate to a different cell with at least one shard and one replica",
                        target.cell, target.shards, target.replicas, self.shard.cfg.cell
                    ))));
                }
                let link = self.shard.state.links.entry(target.cell).or_insert(Link {
                    target: target.clone(),
                    acked_seq: 0,
                    paused: false,
                });
                link.target = target;
                Ok(ApiResponse::Ok)
            }
            ApiRequest::DeleteReplication { cell } => {
                self.shard.state.links.remove(&cell);
                self.primary.transfers.remove(&cell);
                self.primary.shipped.remove(&cell);
                Ok(ApiResponse::Ok)
            }
            ApiRequest::PauseReplication { cell, paused } => {
                match self.shard.state.links.get_mut(&cell) {
                    Some(link) => link.paused = paused,
                    None => return Err(Fail::Api(Error::not_found(format!("no replication to cell {cell}")))),
                }
                Ok(ApiResponse::Ok)
            }
            ApiRequest::SetRole { active, epoch } => {
                let state = &mut self.shard.state;
                if epoch < state.epoch || (epoch == state.epoch && active != state.active) {
                    return Err(Fail::Api(Error::precondition(format!(
                        "SetRole asked for epoch {epoch} but the cell is at epoch {}: a role change needs an epoch \
                         above {}",
                        state.epoch, state.epoch
                    ))));
                }
                let changed = state.active != active;
                state.epoch = epoch;
                state.active = active;
                if changed {
                    self.refresh_rows(store, now)?;
                }
                Ok(ApiResponse::Ok)
            }
        }
    }

    fn apply_replication(&mut self, store: &mut Store, now: Timestamp, chunk: Chunk) -> Result<(), StorageError> {
        let mut tx = Tx::open(store, &mut self.shard, now, chunk.key.clone())?;
        let received = tx.local.received.get(&chunk.source_cell).cloned().unwrap_or_default();
        if !received.is_next_chunk(&chunk) {
            return Ok(());
        }
        let interrupted_transfer_already_took = !received.done && received.taken;
        let mut taken = interrupted_transfer_already_took;
        tx.shard.state.change_seq += 1;
        let mut took = false;
        for (path, node) in chunk.nodes {
            took |= tx.upsert_replicated(&path, node)?;
        }
        if !took {
            tx.shard.state.change_seq -= 1;
        }
        taken |= took;
        let progress = Received { seq: chunk.seq, through: chunk.through.clone(), done: chunk.last, taken };
        tx.local.received.insert(chunk.source_cell, progress);
        if !chunk.last {
            return tx.store.put(&keys::local(&chunk.key), &encode(&tx.local));
        }
        if taken && !took {
            tx.shard.state.change_seq += 1;
        }
        let summary = Registry.summary(&mut tx)?;
        let created_at = tx.meta.created_at;
        tx.finish(summary, taken)?;
        tx.store.put(&keys::local(&chunk.key), &encode(&tx.local))?;
        if taken {
            self.shard.state.replicated_in += 1;
            if chunk.key.arch == WORKFLOW {
                let pointer = keys::current(WORKFLOW, &chunk.key.id);
                let mine: Option<(Id128, Timestamp)> = store.get(&pointer)?.map(|b| decode_stored(&b));
                if mine.is_none_or(|(run, at)| (at, run) < (created_at, chunk.key.run)) {
                    store.put(&pointer, &encode(&(chunk.key.run, created_at)))?;
                }
            }
        }
        Ok(())
    }

    fn replication_shipped(
        &mut self,
        store: &mut Store,
        cell: CellId,
        items: Vec<(ExecKey, u64)>,
    ) -> Result<(), StorageError> {
        for (key, seq) in items {
            let row = keys::local(&key);
            let Some(mut local) = store.get(&row)?.map(|b| decode_stored::<Local>(&b)) else { continue };
            let shipped = local.shipped.entry(cell).or_insert(0);
            if *shipped < seq {
                *shipped = seq;
                store.put(&row, &encode(&local))?;
            }
        }
        self.advance_link_cursor_past_delivered(store, cell)
    }

    fn advance_link_cursor_past_delivered(&mut self, store: &mut Store, cell: CellId) -> Result<(), StorageError> {
        let Some(link) = self.shard.state.links.get(&cell) else { return Ok(()) };
        let mut acked = link.acked_seq;
        let rows = store.scan(&keys::change(acked + 1), &keys::change(u64::MAX), 256, false)?;
        let exhausted = rows.len() < 256;
        let mut all = true;
        for (row, value) in rows {
            let seq = keys::parse_change(&row);
            let key: ExecKey = decode_stored(&value);
            let local: Local = store.get(&keys::local(&key))?.map(|b| decode_stored(&b)).unwrap_or_default();
            if local.shipped.get(&cell).copied().unwrap_or(0) < seq {
                all = false;
                break;
            }
            acked = seq;
        }
        if all && exhausted {
            acked = self.shard.state.change_seq;
        }
        if let Some(link) = self.shard.state.links.get_mut(&cell) {
            link.acked_seq = acked;
        }
        Ok(())
    }

    fn execute(&mut self, store: &mut Store, ctx: &OpContext, op: Op) -> Fallible<ApiResponse> {
        let now = ctx.timestamp;
        match op {
            Op::Api(request) => return self.api(store, ctx, request),
            Op::ApplyRouting(route) => {
                let known = self.shard.routes.get(&route.deployment).map_or(0, |r| r.routing.revision);
                if known < route.routing.revision {
                    self.shard.set_route(store, route)?;
                }
            }
            Op::RoutingAcked { .. } | Op::DrainageResult { .. } if !self.shard.state.active => {}
            Op::RoutingAcked { deployment, shard, revision } => {
                let mut tx = Tx::open(store, &mut self.shard, now, deployment_key(&deployment))?;
                deployment::acked(&mut tx, shard, revision)?;
                tx.commit(&Registry, false)?;
            }
            Op::DrainageResult { version, nonce, count } => {
                let mut tx = Tx::open(store, &mut self.shard, now, deployment_key(&version.deployment))?;
                deployment::drainage_result(&mut tx, &version.build_id, nonce, count)?;
                tx.commit(&Registry, false)?;
            }
            Op::ApplyReplication(chunk) => self.apply_replication(store, now, chunk)?,
            Op::ReplicationShipped { cell, items } => self.replication_shipped(store, cell, items)?,
        }
        Ok(ApiResponse::Ok)
    }

    fn propose_once(&mut self, fx: &mut Effects<EnginePacket>, op: Op) {
        let body = encode(&op);
        if self.primary.proposed.insert(body.clone()) {
            fx.propose.push(body);
        }
    }

    fn broadcast(
        &self,
        fx: &mut Effects<EnginePacket>,
        cell: CellId,
        shard: ShardId,
        replicas: u8,
        packet: &EnginePacket,
    ) {
        for replica in 0..replicas {
            fx.send.push((Addr::replica(cell, shard, replica), packet.clone()));
        }
    }

    fn drive_deployments(
        &mut self,
        store: &mut Store,
        now: Timestamp,
        fx: &mut Effects<EnginePacket>,
    ) -> Result<(), StorageError> {
        let prefix = keys::outbox_prefix();
        let cfg = self.shard.cfg.clone();
        for (row, _) in store.scan(&prefix, &prefix_end(&prefix), 64, false)? {
            let key = keys::parse_outbox(&row);
            if key.arch != DEPLOYMENT {
                continue;
            }
            let mut tx = Tx::open(store, &mut self.shard, now, key.clone())?;
            let tasks = deployment::outbound(&mut tx);
            let route = deployment::routing(&mut tx)?;
            let unacked = deployment::unacked(&mut tx)?;
            drop(tx);
            for task in tasks {
                match task {
                    deployment::DepTask::Propagate { .. } => {
                        let Some(route) = &route else { continue };
                        for shard in &unacked {
                            self.broadcast(
                                fx,
                                cfg.cell,
                                *shard,
                                cfg.replica_count,
                                &EnginePacket::Routing(route.clone()),
                            );
                        }
                    }
                    deployment::DepTask::DrainCount { build_id, nonce } => {
                        let version = DeploymentVersion::new(&key.id, build_id);
                        for shard in 0..cfg.shard_count {
                            let packet = EnginePacket::CountPinned { version: version.clone(), nonce };
                            self.broadcast(fx, cfg.cell, shard, cfg.replica_count, &packet);
                        }
                    }
                    deployment::DepTask::DrainCheck { .. } => {}
                }
            }
        }
        Ok(())
    }

    fn build_chunk(
        &self,
        store: &mut Store,
        key: &ExecKey,
        seq: u64,
        since: u64,
        after: &str,
    ) -> Result<Chunk, StorageError> {
        let budget = self.shard.cfg.page_bytes_max;
        let prefix = keys::exec(key);
        let end = prefix_end(&prefix);
        let mut cursor = prefix.clone();
        cursor.extend_from_slice(after.as_bytes());
        cursor.push(0);
        let mut nodes = Vec::new();
        let mut bytes = 0;
        let mut through = after.to_string();
        let mut last = false;
        'scan: loop {
            let rows = store.scan(&cursor, &end, 128, false)?;
            if rows.is_empty() {
                last = true;
                break;
            }
            for (row, value) in &rows {
                let node: Node = decode_stored(value);
                let path = String::from_utf8(row[prefix.len()..].to_vec())
                    .expect("node paths are stored from &str, so they are valid UTF-8");
                if node.seq > since {
                    if !nodes.is_empty() && bytes + value.len() > budget {
                        break 'scan;
                    }
                    bytes += value.len();
                    nodes.push((path.clone(), node));
                }
                through = path;
            }
            let Some((last_key, _)) = rows.last() else { break };
            cursor = last_key.clone();
            cursor.push(0);
        }
        if last {
            let root: Node = decode_stored(
                &store.get(&prefix)?.expect("an execution with nodes under its prefix always has its root node"),
            );
            nodes.push((String::new(), root));
        }
        let cfg = &self.shard.cfg;
        Ok(Chunk {
            source_cell: cfg.cell,
            source_shard: cfg.shard,
            key: key.clone(),
            seq,
            after: after.to_string(),
            through,
            last,
            nodes,
        })
    }

    fn drive_replication_even_on_standby(
        &mut self,
        store: &mut Store,
        fx: &mut Effects<EnginePacket>,
    ) -> Result<(), StorageError> {
        let links: Vec<Link> = self.shard.state.links.values().filter(|l| !l.paused).cloned().collect();
        for link in links {
            let cell = link.target.cell;
            let shipped = self.primary.shipped.entry(cell).or_default();
            let window = self.shard.cfg.replication_window;
            let rows = store.scan(&keys::change(link.acked_seq + 1), &keys::change(u64::MAX), window * 8, false)?;
            let mut live: BTreeMap<ExecKey, (u64, u64)> = BTreeMap::new();
            let mut persist = Vec::new();
            for (row, value) in rows {
                let seq = keys::parse_change(&row);
                let key: ExecKey = decode_stored(&value);
                let local: Local = store.get(&keys::local(&key))?.map(|b| decode_stored(&b)).unwrap_or_default();
                let since = local.shipped.get(&cell).copied().unwrap_or(0);
                if since >= seq {
                    shipped.remove(&key);
                    continue;
                }
                if shipped.get(&key).is_some_and(|s| *s >= seq) {
                    persist.push((key, seq));
                    continue;
                }
                if live.len() < window {
                    live.insert(key, (seq, since));
                }
            }
            if !persist.is_empty() || link.acked_seq < self.shard.state.change_seq && live.is_empty() {
                let op = Op::ReplicationShipped { cell, items: persist };
                let body = encode(&op);
                if self.primary.proposed.insert(body.clone()) {
                    fx.propose.push(body);
                }
            }
            let transfers = self.primary.transfers.entry(cell).or_default();
            transfers.retain(|key, t| live.get(key).is_some_and(|(seq, _)| *seq == t.seq));
            let mut out = Vec::new();
            for (key, (seq, since)) in &live {
                let transfers = self.primary.transfers.entry(cell).or_default();
                let after = transfers.get(key).map(|t| t.last_acked_path.clone()).unwrap_or_default();
                out.push(self.build_chunk(store, key, *seq, *since, &after)?);
                self.primary
                    .transfers
                    .entry(cell)
                    .or_default()
                    .insert(key.clone(), Transfer { seq: *seq, last_acked_path: after });
            }
            for chunk in out {
                let shard = shard_for(&chunk.key.id, link.target.shards);
                self.broadcast(fx, cell, shard, link.target.replicas, &EnginePacket::Replicate(chunk));
            }
        }
        Ok(())
    }
}

impl StateMachine for Engine {
    type Packet = EnginePacket;

    fn open(&mut self, store: &mut Store) -> Result<(), StorageError> {
        self.primary.proposed.clear();
        self.primary.drainage.clear();
        self.primary.transfers.clear();
        self.primary.shipped.clear();
        self.primary.drive_at = Timestamp::ZERO;
        self.shard.load(store)
    }

    fn apply(&mut self, store: &mut Store, ctx: &OpContext, body: &[u8]) -> Result<Vec<u8>, StorageError> {
        let result: ApiResult = match decode::<Op>(body) {
            Err(e) => Err(Error::invalid(format!("undecodable operation: {e}"))),
            Ok(op) => match self.execute(store, ctx, op) {
                Ok(response) => Ok(response),
                Err(Fail::Api(e)) => Err(e),
                Err(Fail::Storage(e)) => return Err(e),
            },
        };
        self.shard.refresh_timer(store)?;
        self.shard.save(store)?;
        Ok(encode(&result))
    }

    fn pulse(&mut self, store: &mut Store, ctx: &OpContext) -> Result<(), StorageError> {
        let now = ctx.timestamp;
        let prefix = keys::timer_prefix();
        let end = wfe_core::keys::Key(prefix.clone()).u64(now.micros().saturating_add(1)).into_vec();
        let due = store.scan(&prefix, &end, 64, false)?;
        for (row, _) in due {
            let (_, key) = keys::parse_timer(&row);
            let mut tx = Tx::open(store, &mut self.shard, now, key)?;
            for _ in 0..256 {
                let next = tx
                    .meta
                    .tasks
                    .iter()
                    .filter(|t| t.at.is_some_and(|at| at <= now))
                    .min_by_key(|t| (t.at, t.id))
                    .cloned();
                let Some(task) = next else { break };
                if Registry.validate(&mut tx, &task)? {
                    fire(&mut tx, &task)?;
                    assert!(
                        !Registry.validate(&mut tx, &task)?,
                        "a task that ran must no longer be valid: {:?} of {:?}",
                        task.id,
                        tx.key
                    );
                }
                tx.remove_task(task.id);
            }
            tx.commit(&Registry, true)?;
        }
        self.shard.timer_invalidate();
        self.shard.refresh_timer(store)?;
        self.shard.save(store)
    }

    fn pulse_at(&self) -> Option<Timestamp> {
        self.shard.next_timer()
    }

    fn drive(&mut self, store: &mut Store, now: Timestamp, fx: &mut Effects<EnginePacket>) -> Result<(), StorageError> {
        if now < self.primary.drive_at {
            return Ok(());
        }
        self.primary.drive_at = now.add_ms(self.shard.cfg.outbound_interval_ms);
        self.primary.proposed.clear();
        if self.shard.state.active {
            self.drive_deployments(store, now, fx)?;
        }
        self.drive_replication_even_on_standby(store, fx)?;
        Ok(())
    }

    fn on_packet(
        &mut self,
        store: Option<&mut Store>,
        primary: bool,
        from: Addr,
        packet: EnginePacket,
        _now: Timestamp,
        fx: &mut Effects<EnginePacket>,
    ) -> Result<(), StorageError> {
        let (Some(store), true) = (store, primary) else { return Ok(()) };
        let me = self.shard.cfg.shard;
        match packet {
            EnginePacket::Probe { id, queues, workflows } => {
                let mut ready = Vec::new();
                if self.shard.state.active {
                    for (i, q) in queues.iter().enumerate() {
                        if !Self::candidates(&self.shard, store, &q.task_queue, q.kind, q.version.as_ref(), 1)?
                            .is_empty()
                        {
                            ready.push(probe_index(i));
                        }
                    }
                }
                let mut wake_to_read = Vec::new();
                for (i, (workflow_id, run_id)) in workflows.iter().enumerate() {
                    let Ok(key) = self.resolve(store, workflow_id, *run_id) else {
                        wake_to_read.push(probe_index(i));
                        continue;
                    };
                    match store.get(&keys::exec(&key))? {
                        Some(bytes) => {
                            let root: Node = decode_stored(&bytes);
                            if decode_stored::<Meta>(&root.data).closed {
                                wake_to_read.push(probe_index(i));
                            }
                        }
                        None => wake_to_read.push(probe_index(i)),
                    }
                }
                let result = EnginePacket::ProbeResult { id, shard: me, queues: ready, workflows: wake_to_read };
                fx.send.push((from, result));
            }
            EnginePacket::ProbeResult { .. } => {}
            EnginePacket::Routing(route) => {
                let known = self.shard.routes.get(&route.deployment).map_or(0, |r| r.routing.revision);
                if known >= route.routing.revision {
                    let ack = EnginePacket::RoutingAck { deployment: route.deployment, shard: me, revision: known };
                    fx.send.push((from, ack));
                } else {
                    self.propose_once(fx, Op::ApplyRouting(route));
                }
            }
            EnginePacket::RoutingAck { deployment, shard, revision } => {
                self.propose_once(fx, Op::RoutingAcked { deployment, shard, revision });
            }
            EnginePacket::CountPinned { version, nonce } => {
                let count: u64 = store.get(&keys::pinned_count(&version))?.map(|b| decode_stored(&b)).unwrap_or(0);
                fx.send.push((from, EnginePacket::PinnedCount { version, nonce, shard: me, count }));
            }
            EnginePacket::PinnedCount { version, nonce, shard, count } => {
                let tally = self.primary.drainage.entry(version.clone()).or_default();
                if let Some(count) = tally.record(nonce, shard, count, self.shard.cfg.shard_count) {
                    self.propose_once(fx, Op::DrainageResult { version, nonce, count });
                }
            }
            EnginePacket::Replicate(chunk) => {
                let local: Local = store.get(&keys::local(&chunk.key))?.map(|b| decode_stored(&b)).unwrap_or_default();
                let received = local.received.get(&chunk.source_cell).cloned().unwrap_or_default();
                if received.is_next_chunk(&chunk) {
                    self.propose_once(fx, Op::ApplyReplication(chunk));
                } else {
                    let (through, done) = progress_this_transfer_delivered(received, chunk.seq);
                    let ack = EnginePacket::ReplicateAck {
                        cell: self.shard.cfg.cell,
                        key: chunk.key,
                        seq: chunk.seq,
                        through,
                        done,
                    };
                    fx.send.push((from, ack));
                }
            }
            EnginePacket::ReplicateAck { cell, key, seq, through, done } => {
                let Some(cell_transfers) = self.primary.transfers.get_mut(&cell) else { return Ok(()) };
                let Some(transfer) = cell_transfers.get_mut(&key) else { return Ok(()) };
                let stale_ack = transfer.seq != seq || (!done && through <= transfer.last_acked_path);
                if stale_ack {
                    return Ok(());
                }
                if done {
                    cell_transfers.remove(&key);
                    self.primary.shipped.entry(cell).or_default().insert(key, seq);
                } else {
                    transfer.last_acked_path = through;
                }
                self.primary.drive_on_next_tick();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::DrainageTally;

    #[test]
    fn a_drainage_round_sums_one_count_per_shard_once() {
        let mut tally = DrainageTally::default();
        assert_eq!(tally.record(1, 0, 2, 2), None);
        assert_eq!(tally.record(1, 0, 2, 2), None);
        assert_eq!(tally.record(1, 1, 3, 2), Some(5));
        assert_eq!(tally.record(1, 1, 3, 2), None);
        assert_eq!(tally.record(1, 0, 2, 2), None);
    }

    #[test]
    fn late_counts_never_fill_in_a_newer_round() {
        let mut tally = DrainageTally::default();
        assert_eq!(tally.record(1, 0, 0, 2), None);
        assert_eq!(tally.record(1, 1, 0, 2), Some(0));
        assert_eq!(tally.record(1, 0, 0, 2), None);
        assert_eq!(tally.record(2, 0, 0, 2), None);
        assert_eq!(tally.record(1, 1, 0, 2), None);
        assert_eq!(tally.record(2, 1, 1, 2), Some(1));
    }
}
