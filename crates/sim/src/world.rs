//! The whole system in one deterministic process.
//!
//! A [`World`] is one or two cells (each shards x replicas of the real
//! engine on simulated disks), their gateways, a fleet of SDK workers and a
//! driver that starts workflows and administers the cells, all joined by a
//! simulated network. A seed decides the topology, the workload and every
//! fault; the same seed replays the same run.
//!
//! While it runs, and once more after the faults stop, the world checks
//! that:
//!
//! - replicas of a shard commit the same log and hold the same state;
//! - every workflow ends the way its input says it must, with a
//!   well-formed history, and exactly the signals that were acknowledged;
//! - worker versioning holds: pinned workflows stay on their version,
//!   auto-upgrading ones only move forward, old versions drain;
//! - a standby cell converges to the same workflows as the active one, and
//!   after a failover the new active cell finishes everything;
//! - the controller (`wfe-control`, the code the Kubernetes operator and
//!   the CLI run) gets every rollout and failover the seed asks for to its
//!   end, while it is interrupted, repeated and fed old observations.

use crate::workload::{self, Input, KINDS, Kind, out};
use crate::{Net, NetOptions, SimDevice};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use wfe_codec::{Chain, Codec, MemoryStore};
use wfe_control::{
    CellView, FleetBuild, Other, RampStep, RoleStep, RolloutPolicy, RolloutStrategy, SunsetPolicy, WorkersAction,
    WorkersInput, plan_roles, plan_workers,
};
use wfe_core::api::*;
use wfe_core::checksum::Hasher;
use wfe_core::{Addr, CellId, ErrorCode, Id128, Payload, Prng, Ratio, Timestamp};
use wfe_engine::{Engine, EngineConfig, EnginePacket};
use wfe_gateway::{Gateway, GatewayConfig};
use wfe_lsm::LsmOptions;
use wfe_sdk::{WorkerCore, WorkerOptions};
use wfe_vsr::{Event as VsrEvent, Layout, Packet, Replica, ReplicaConfig, Timing};

const SIM_STORE: &str =
    "a simulated replica's store is in memory and was written by this engine, so it reads and decodes";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Options {
    pub seed: u64,
    /// Ticks of workload and faults (10 ms each) before the world settles.
    pub ticks: u64,
    pub cells: u8,
    pub shards: u32,
    pub replicas: u8,
    pub faults: bool,
    /// Fail over from the first cell to the second mid-run.
    pub failover: bool,
    /// Workflows open at once.
    pub concurrency: usize,
}

impl Options {
    /// The topology and scenario a seed stands for.
    pub fn for_seed(seed: u64, ticks: u64) -> Options {
        let mut prng = Prng::new(seed ^ 0x005e_ed0f_7e57);
        let cells = if prng.chance(1, 2) { 2 } else { 1 };
        Options {
            seed,
            ticks,
            cells,
            shards: u32::try_from(prng.range(1, 3)).expect("prng.range(1, 3) is 1 or 2, which fits in u32"),
            replicas: 3,
            faults: !prng.chance(1, 10),
            failover: cells == 2 && prng.chance(2, 3),
            concurrency: usize::try_from(prng.range(4, 24))
                .expect("prng.range(4, 24) is below 24, which fits in usize"),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "ticks={} cells={} shards={} replicas={} faults={} failover={} concurrency={}",
            self.ticks, self.cells, self.shards, self.replicas, self.faults, self.failover, self.concurrency
        )
    }
}

/// What a run did. Everything here is a function of the options.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Report {
    pub ticks: u64,
    pub sim_ms: u64,
    pub workflows: u64,
    pub by_kind: BTreeMap<String, u64>,
    pub by_status: BTreeMap<String, u64>,
    pub events: u64,
    pub ops: u64,
    pub messages: u64,
    pub messages_dropped: u64,
    pub crashes: u64,
    pub partitions: u64,
    pub gateway_restarts: u64,
    pub worker_restarts: u64,
    pub view_changes: u64,
    pub workflow_tasks: u64,
    pub activities: u64,
    pub replicated: u64,
    pub failovers: u64,
    #[serde(default)]
    pub rollouts: u64,
    #[serde(default)]
    pub builds_retired: u64,
    #[serde(default)]
    pub controller_rounds: u64,
    #[serde(default)]
    pub controller_interruptions: u64,
    #[serde(default)]
    pub controller_stale_plans: u64,
    pub claim_checks: u64,
    pub checks: BTreeMap<String, u64>,
    /// Hash of everything delivered: equal for equal options.
    pub digest: String,
}

#[derive(Clone)]
pub enum SimMsg {
    Cluster(Packet<EnginePacket>),
    Client(ClientMessage),
}

pub fn now(tick: u64) -> Timestamp {
    Timestamp::from_millis(1_000_000 + tick * 10)
}

const DRIVER: u64 = 1;
const RPC: u64 = 2;
const WORKER_BASE: u64 = 100;
const GATEWAYS: u16 = 2;
const CLOSED_BEFORE_THE_STOP: ErrorCode = ErrorCode::FailedPrecondition;

fn random_gateway(prng: &mut Prng) -> u16 {
    u16::try_from(prng.below(u64::from(GATEWAYS))).expect("a value below GATEWAYS, a u16, fits in u16")
}

struct Slot {
    replica: Option<Box<Replica<Engine, SimDevice>>>,
    device: Option<SimDevice>,
    restart_at: u64,
}

struct Cell {
    id: CellId,
    slots: Vec<Vec<Slot>>,
    gateways: Vec<Gateway>,
}

struct SimWorker {
    opts: WorkerOptions,
    core: WorkerCore,
    incarnation: u64,
    jobs: Vec<(u64, u64, Result<Payload, Failure>)>,
    build: Option<(&'static str, &'static str)>,
    running: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Action {
    Start,
    Signal,
    Stop,
    Probe,
}

struct Model {
    id: String,
    kind: Kind,
    input: Input,
    queue: &'static str,
    plain: Payload,
    encoded: Payload,
    request_id: Id128,
    started: bool,
    signals_acked: u64,
    stop_at: u64,
    stopped: bool,
    closed: Option<WorkflowDescription>,
    inflight: Option<u64>,
    next_at: u64,
}

#[derive(Clone, Copy, Debug)]
enum Intent {
    Target(&'static str, &'static str),
    Active(usize),
}

#[derive(Debug)]
struct Wanted {
    target: Option<&'static str>,
    targets_so_far: BTreeSet<&'static str>,
    rollout: RolloutPolicy,
    sunset: SunsetPolicy,
    fleet: Vec<FleetBuild>,
}

const WORKERS_PER_BUILD: u32 = 2;
const DEPLOYMENTS: [(&str, &[&str]); 2] = [("pin", &["b1", "b2", "b3"]), ("auto", &["b1", "b2"])];
const THE_NEXT_ROUND_RETRIES: [ErrorCode; 3] =
    [ErrorCode::NotFound, ErrorCode::FailedPrecondition, ErrorCode::NotActive];
const EVERY_DEPLOYMENT_IS_WANTED: &str = "World::new gives every deployment in DEPLOYMENTS a wanted entry";

fn scenario(opts: &Options) -> (VecDeque<(u64, Intent)>, BTreeMap<&'static str, Wanted>, u64) {
    let mut prng = Prng::new(opts.seed ^ 0x0c0f_fee0_5ce2_a210);
    let t = opts.ticks;
    let mut intents: Vec<(u64, Intent)> = Vec::new();
    let mut wanted = BTreeMap::new();
    for (deployment, builds) in DEPLOYMENTS {
        intents.push((0, Intent::Target(deployment, prng.pick(builds))));
        for _ in 0..prng.range(1, 6) {
            let at = prng.range(t / 10, (t * 9 / 10).max(t / 10 + 1));
            intents.push((at, Intent::Target(deployment, prng.pick(builds))));
        }
        let strategy = if prng.chance(1, 4) { RolloutStrategy::AllAtOnce } else { RolloutStrategy::Progressive };
        let mut percent = 0;
        let steps = (0..prng.range(1, 4))
            .map(|_| {
                percent = (percent + prng.range(5, 45)).min(100);
                RampStep {
                    ramp_percent: u8::try_from(percent).expect("a ramp percent is capped at 100, which fits in u8"),
                    pause_ms: prng.range(300, 3_000),
                }
            })
            .collect();
        let scaledown_after_ms = prng.range(500, 4_000);
        wanted.insert(
            deployment,
            Wanted {
                target: None,
                targets_so_far: BTreeSet::new(),
                rollout: RolloutPolicy { strategy, steps },
                sunset: SunsetPolicy {
                    scaledown_after_ms,
                    delete_after_ms: scaledown_after_ms + prng.range(500, 4_000),
                },
                fleet: Vec::new(),
            },
        );
    }
    if opts.failover {
        let mut times: Vec<u64> =
            (0..prng.range(1, 4)).map(|_| prng.range(t / 5, (t * 85 / 100).max(t / 5 + 1))).collect();
        times.sort_unstable();
        for (i, at) in times.into_iter().enumerate() {
            intents.push((at, Intent::Active(if i.is_multiple_of(2) { 1 } else { 0 })));
        }
    }
    intents.sort_by_key(|(at, _)| *at);
    (intents.into(), wanted, prng.range(15, 60))
}

pub struct World {
    pub opts: Options,
    prng: Prng,
    tick: u64,
    net: Net<SimMsg>,
    cells: Vec<Cell>,
    workers: Vec<SimWorker>,
    codec: Chain,
    objects: Arc<MemoryStore>,
    active: usize,
    models: Vec<Model>,
    calls: BTreeMap<u64, (usize, Action, u64)>,
    next_call: u64,
    rpc_result: Option<(u64, ApiResult)>,
    intents: VecDeque<(u64, Intent)>,
    wanted: BTreeMap<&'static str, Wanted>,
    wanted_active: usize,
    round_period: u64,
    next_round_at: u64,
    stale: Vec<(&'static str, WorkersAction)>,
    committed: BTreeMap<(usize, u32), BTreeMap<u64, u128>>,
    heal_at: u64,
    settling: bool,
    digest: Hasher,
    report: Report,
}

#[derive(Clone, Copy)]
enum Compare {
    Fingerprint,
    Contents,
}

fn cluster_id(cell: CellId, shard: u32) -> u128 {
    0xCE11_0000_0000 + (u128::from(cell) << 32) + u128::from(shard)
}

impl World {
    fn replica_config(&self, cell: CellId, shard: u32, replica: u8) -> ReplicaConfig {
        ReplicaConfig {
            cluster: cluster_id(cell, shard),
            cell,
            shard,
            replica,
            replica_count: self.opts.replicas,
            layout: Layout {
                slot_count: 64,
                message_size_max: 16 * 1024,
                grid_block_size: 4096,
                grid_block_count: 8192,
            },
            lsm: LsmOptions {
                memtable_bytes_max: 24 * 1024,
                l0_tables_max: 3,
                level_growth: 4,
                table_data_blocks_max: 8,
                cache_blocks: 32,
            },
            checkpoint_interval: 16,
            pipeline_max: 4,
            request_queue_max: 32,
            timing: Timing::default(),
        }
    }

    fn engine(&self, cell: CellId, shard: u32) -> Engine {
        let mut cfg = EngineConfig::new(cell, shard, self.opts.shards, self.opts.replicas);
        cfg.page_bytes_max = 3 * 1024;
        cfg.workflow_task_timeout_ms = 2_000;
        cfg.activity_start_to_close_ms = 3_000;
        cfg.drainage_interval_ms = 1_000;
        cfg.outbound_interval_ms = 100;
        cfg.replication_window = 4;
        Engine::new(cfg)
    }

    fn gateway(&mut self, cell: CellId, gateway: u16) -> Gateway {
        let mut cfg = GatewayConfig::new(cell, gateway, self.opts.shards, self.opts.replicas);
        cfg.request_timeout_ms = 4_000;
        cfg.resend_ms = 300;
        cfg.fanout_timeout_ms = 2_000;
        cfg.register_ms = 5_000;
        Gateway::new(cfg, self.prng.next_u64())
    }

    fn worker_core(&mut self, opts: &WorkerOptions) -> WorkerCore {
        let mut core = WorkerCore::new(opts.clone(), Arc::new(self.codec.clone()), self.prng.next_u64());
        workload::register(&mut core);
        core
    }

    pub fn new(opts: Options) -> World {
        let mut prng = Prng::new(opts.seed);
        let lossy = opts.faults;
        let net = Net::new(
            NetOptions {
                delay_min: 1,
                delay_max: prng.range(2, 10),
                drop: Ratio::percent(if lossy { prng.below(6) } else { 0 }),
                duplicate: Ratio::percent(if lossy { prng.below(4) } else { 0 }),
            },
            prng.fork(),
        );
        let objects = Arc::new(MemoryStore::default());
        let codec = Chain::standard(objects.clone(), 256, 512);
        let (intents, wanted, round_period) = scenario(&opts);
        let mut world = World {
            opts: opts.clone(),
            prng,
            tick: 0,
            net,
            cells: Vec::new(),
            workers: Vec::new(),
            codec,
            objects,
            active: 0,
            models: Vec::new(),
            calls: BTreeMap::new(),
            next_call: 0,
            rpc_result: None,
            intents,
            wanted,
            wanted_active: 0,
            round_period,
            next_round_at: 0,
            stale: Vec::new(),
            committed: BTreeMap::new(),
            heal_at: 0,
            settling: false,
            digest: Hasher::new(),
            report: Report::default(),
        };
        for c in 0..opts.cells {
            let id = CellId::from(c) + 1;
            let mut slots = Vec::new();
            for shard in 0..opts.shards {
                let mut row = Vec::new();
                for replica in 0..opts.replicas {
                    let config = world.replica_config(id, shard, replica);
                    let mut dev = SimDevice::new();
                    Replica::<Engine, SimDevice>::format(&mut dev, &config);
                    let seed = world.prng.next_u64();
                    let r = Replica::open(dev, config, world.engine(id, shard), seed, now(0))
                        .expect("a device just formatted with this config always opens");
                    row.push(Slot { replica: Some(Box::new(r)), device: None, restart_at: 0 });
                }
                slots.push(row);
            }
            let gateways = (0..GATEWAYS).map(|g| world.gateway(id, g)).collect();
            world.cells.push(Cell { id, slots, gateways });
        }
        let mut fleet: Vec<(WorkerOptions, Option<(&'static str, &'static str)>)> = Vec::new();
        let base = |queue: &str, name: String| {
            let mut o = WorkerOptions::new(queue, name);
            o.poll_wait_ms = 1_500;
            o.request_timeout_ms = 3_000;
            o.error_backoff_ms = 200;
            o.workflow_polls = 2;
            o.activity_polls = 2;
            o
        };
        for i in 0..3 {
            fleet.push((base("main", format!("main-{i}")), None));
        }
        for (deployment, builds) in DEPLOYMENTS {
            let behavior =
                if deployment == "pin" { VersioningBehavior::Pinned } else { VersioningBehavior::AutoUpgrade };
            for build in builds {
                for i in 0..WORKERS_PER_BUILD {
                    let options = base(&format!("q-{deployment}"), format!("{deployment}-{build}-{i}"))
                        .versioned(DeploymentVersion::new(deployment, *build), behavior);
                    fleet.push((options, Some((deployment, *build))));
                }
            }
        }
        for (opts, build) in fleet {
            let core = world.worker_core(&opts);
            let running = build.is_none();
            world.workers.push(SimWorker { opts, core, incarnation: 1, jobs: Vec::new(), build, running });
        }
        world
    }

    fn cell_index(&self, cell: CellId) -> Option<usize> {
        let i = (cell as usize).checked_sub(1)?;
        (i < self.cells.len()).then_some(i)
    }

    fn dispatch(&mut self, from: Addr, to: Addr, msg: SimMsg) {
        let at = now(self.tick);
        match (to, msg) {
            (Addr::Replica { cell, shard, replica }, SimMsg::Cluster(packet)) => {
                self.digest
                    .update_u64(self.tick)
                    .update_u64((u64::from(cell) << 40) | (u64::from(shard) << 8) | u64::from(replica));
                let Some(c) = self.cell_index(cell) else { return };
                let Some(slot) = self.cells[c].slots.get_mut(shard as usize).and_then(|s| s.get_mut(replica as usize))
                else {
                    return;
                };
                if let Some(r) = slot.replica.as_mut() {
                    r.on_packet(from, packet, at);
                }
            }
            (Addr::Gateway { cell, gateway }, msg) => {
                self.digest.update_u64(self.tick).update_u64((1 << 60) | (u64::from(cell) << 20) | u64::from(gateway));
                let Some(c) = self.cell_index(cell) else { return };
                let Some(g) = self.cells[c].gateways.get_mut(gateway as usize) else { return };
                match msg {
                    SimMsg::Cluster(packet) => g.on_cluster(from, packet, at),
                    SimMsg::Client(message) => g.on_client(from, message, at),
                }
            }
            (Addr::Client { id }, SimMsg::Client(ClientMessage::Response { id: call, result })) => {
                self.digest.update_u64(self.tick).update_u64((2 << 60) | id).update_u64(call);
                match id {
                    DRIVER => self.on_driver_response(call, result),
                    RPC => self.rpc_result = Some((call, result)),
                    _ => {
                        let Some(w) = usize::try_from(id - WORKER_BASE).ok().and_then(|w| self.workers.get_mut(w))
                        else {
                            return;
                        };
                        if w.running && call >> 32 == w.incarnation {
                            w.core.on_response(call & 0xffff_ffff, result, at);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    fn step(&mut self) {
        self.tick += 1;
        let tick = self.tick;
        let at = now(tick);
        for (from, to, msg) in self.net.deliver(tick) {
            self.dispatch(from, to, msg);
        }
        for c in 0..self.cells.len() {
            let cell = self.cells[c].id;
            for shard in 0..self.opts.shards {
                for replica in 0..self.opts.replicas {
                    let slot = &mut self.cells[c].slots[shard as usize][replica as usize];
                    let Some(r) = slot.replica.as_mut() else {
                        if slot.restart_at <= tick {
                            self.restart(c, shard, replica);
                        }
                        continue;
                    };
                    r.tick(at);
                    let outbox = r.take_outbox();
                    let events = r.take_events();
                    let from = Addr::replica(cell, shard, replica);
                    for (to, packet) in outbox {
                        self.net.send(tick, from, to, SimMsg::Cluster(packet));
                    }
                    for event in events {
                        match event {
                            VsrEvent::Committed { op, checksum, .. } => {
                                let log = self.committed.entry((c, shard)).or_default();
                                let known = *log.entry(op).or_insert_with(|| {
                                    self.report.ops += 1;
                                    checksum
                                });
                                assert_eq!(
                                    known, checksum,
                                    "cell {cell} shard {shard} replica {replica} committed a different op {op}"
                                );
                                while log.len() > 4096 {
                                    log.pop_first();
                                }
                            }
                            VsrEvent::ViewChangeStarted { .. } => self.report.view_changes += 1,
                            VsrEvent::Checkpointed { .. }
                            | VsrEvent::BecamePrimary { .. }
                            | VsrEvent::BecameBackup { .. }
                            | VsrEvent::SyncStarted { .. }
                            | VsrEvent::StorageOpened { .. }
                            | VsrEvent::WalFault { .. }
                            | VsrEvent::StorageFault => {}
                        }
                    }
                }
            }
            for g in 0..self.cells[c].gateways.len() {
                let gateway = &mut self.cells[c].gateways[g];
                gateway.tick(at);
                let from = gateway.addr();
                let cluster = gateway.take_cluster();
                let client = gateway.take_client();
                for (to, packet) in cluster {
                    self.net.send(tick, from, to, SimMsg::Cluster(packet));
                }
                for (to, message) in client {
                    self.net.send(tick, from, to, SimMsg::Client(message));
                }
            }
        }
        let cell = self.cells[self.active].id;
        for w in 0..self.workers.len() {
            let worker = &mut self.workers[w];
            if !worker.running {
                continue;
            }
            let mut i = 0;
            while i < worker.jobs.len() {
                if worker.jobs[i].0 <= tick {
                    let (_, job, result) = worker.jobs.swap_remove(i);
                    worker.core.activity_done(job, result, at);
                } else {
                    i += 1;
                }
            }
            worker.core.tick(at);
            let from = Addr::client(WORKER_BASE + w as u64);
            for (id, request) in worker.core.take_requests() {
                let to = Addr::gateway(cell, random_gateway(&mut self.prng));
                let message = ClientMessage::Request { id: (worker.incarnation << 32) | id, request };
                self.net.send(tick, from, to, SimMsg::Client(message));
            }
            for job in worker.core.take_activities() {
                let (result, extra) = workload::run_activity(&job.task);
                worker.jobs.push((tick + extra + self.prng.range(1, 12), job.id, result));
            }
        }
        self.drive();
        if self.opts.faults && !self.settling {
            self.faults();
        }
        if tick.is_multiple_of(256) {
            self.check_replicas(false, Compare::Fingerprint);
        }
    }

    fn restart(&mut self, c: usize, shard: u32, replica: u8) {
        let cell = self.cells[c].id;
        let config = self.replica_config(cell, shard, replica);
        let engine = self.engine(cell, shard);
        let seed = self.prng.next_u64();
        let slot = &mut self.cells[c].slots[shard as usize][replica as usize];
        let Some(dev) = slot.device.take() else { return };
        let r = Replica::open(dev, config, engine, seed, now(self.tick))
            .unwrap_or_else(|e| panic!("cell {cell} shard {shard} replica {replica} cannot restart: {e}"));
        slot.replica = Some(Box::new(r));
    }

    fn faults(&mut self) {
        let tick = self.tick;
        if self.prng.chance(1, 900) {
            let c = self.prng.index(self.cells.len());
            let shard = usize::try_from(self.prng.below(u64::from(self.opts.shards)))
                .expect("a shard index is below opts.shards, a u32, so it fits in usize");
            let row = &mut self.cells[c].slots[shard];
            if row.iter().all(|s| s.replica.is_some()) {
                let slot = &mut row[self.prng.index(self.opts.replicas as usize)];
                let mut dev =
                    slot.replica.take().expect("every replica in this row was just checked to be up").into_device();
                dev.crash(&mut self.prng);
                slot.device = Some(dev);
                slot.restart_at = tick + self.prng.range(20, 600);
                self.report.crashes += 1;
            }
        }
        if self.heal_at != 0 && tick >= self.heal_at {
            self.net.heal();
            self.heal_at = 0;
        }
        if self.heal_at == 0 && self.prng.chance(1, 1500) {
            self.split_one_cell_or_the_cells_apart();
        }
        if self.prng.chance(1, 2500) {
            let c = self.prng.index(self.cells.len());
            let g = random_gateway(&mut self.prng);
            let cell = self.cells[c].id;
            self.cells[c].gateways[g as usize] = self.gateway(cell, g);
            self.report.gateway_restarts += 1;
        }
        let w = self.prng.index(self.workers.len());
        if self.prng.chance(1, 700) && self.workers[w].running {
            let opts = self.workers[w].opts.clone();
            let stats = self.workers[w].core.stats.clone();
            self.report.workflow_tasks += stats.workflow_tasks;
            self.report.activities += stats.activities;
            let core = self.worker_core(&opts);
            let worker = &mut self.workers[w];
            worker.core = core;
            worker.incarnation += 1;
            worker.jobs.clear();
            self.report.worker_restarts += 1;
        }
    }

    fn split_one_cell_or_the_cells_apart(&mut self) {
        {
            let mut a = Vec::new();
            let mut b = Vec::new();
            let whole_cells = self.cells.len() == 2 && self.prng.chance(1, 4);
            let victim = self.prng.index(self.cells.len());
            for (c, cell) in self.cells.iter().enumerate() {
                for shard in 0..self.opts.shards {
                    for replica in 0..self.opts.replicas {
                        let addr = Addr::replica(cell.id, shard, replica);
                        let left = if whole_cells { c == 0 } else { c == victim && self.prng.chance(1, 3) };
                        if left { a.push(addr) } else { b.push(addr) }
                    }
                }
                for g in 0..GATEWAYS {
                    let left = if whole_cells { c == 0 } else { c == victim && self.prng.chance(1, 4) };
                    if left { a.push(Addr::gateway(cell.id, g)) } else { b.push(Addr::gateway(cell.id, g)) }
                }
            }
            self.net.partition(&a, &b);
            self.heal_at = self.tick + self.prng.range(50, 500);
            self.report.partitions += 1;
        }
    }

    fn check_replicas(&mut self, all: bool, compare: Compare) -> bool {
        let mut settled = true;
        for cell in &mut self.cells {
            for (shard, row) in cell.slots.iter_mut().enumerate() {
                let mut seen: BTreeMap<u64, (u128, u8)> = BTreeMap::new();
                for (replica, slot) in row.iter_mut().enumerate() {
                    let Some(r) = slot.replica.as_mut() else {
                        settled = false;
                        continue;
                    };
                    let commit = r.status().commit_min;
                    let digest = match compare {
                        Compare::Fingerprint => r.with_store(|_, store| store.lsm.fingerprint()),
                        Compare::Contents => r.read_store(|_, store| {
                            let rows = store.scan(&[], &[0xff; 4], usize::MAX, false)?;
                            let mut h = Hasher::new();
                            for (k, v) in rows {
                                h.update_u64(k.len() as u64).update(&k).update_u64(v.len() as u64).update(&v);
                            }
                            Ok(h.finish())
                        }),
                    };
                    let Some(digest) = digest else {
                        settled = false;
                        continue;
                    };
                    match seen.get(&commit) {
                        Some((known, other)) => assert_eq!(
                            *known, digest,
                            "cell {} shard {shard}: replicas {other} and {replica} hold different state at op {commit}",
                            cell.id
                        ),
                        None => {
                            let replica = u8::try_from(replica).expect("replica indices come from opts.replicas, a u8");
                            seen.insert(commit, (digest, replica));
                        }
                    }
                }
                settled &= seen.len() == 1;
            }
        }
        *self.report.checks.entry("replica state comparisons".into()).or_insert(0) += 1;
        settled || !all
    }

    fn send_driver(&mut self, client: u64, cell: usize, request: ApiRequest) -> u64 {
        self.next_call += 1;
        let to = Addr::gateway(self.cells[cell].id, random_gateway(&mut self.prng));
        let message = ClientMessage::Request { id: self.next_call, request };
        self.net.send(self.tick, Addr::client(client), to, SimMsg::Client(message));
        self.next_call
    }

    fn new_model(&mut self) {
        let kind = *self.prng.pick(&KINDS);
        let n = match kind {
            Kind::Sum => self.prng.range(1, 5),
            Kind::Fan => self.prng.range(1, 6),
            Kind::Signals => self.prng.range(1, 4),
            Kind::Countdown => self.prng.range(1, 3),
            Kind::Flaky => self.prng.range(0, 2),
            Kind::Race
            | Kind::Marker
            | Kind::Cancelable
            | Kind::Terminated
            | Kind::Doom
            | Kind::TimerCancel
            | Kind::Blob => 0,
        };
        let input = Input { n, base: self.prng.below(1000) };
        let plain = match kind {
            Kind::Blob if self.prng.bool() => Payload::raw(vec![input.base.to_le_bytes()[0]; 1500]),
            Kind::Blob => {
                let mut noise = vec![0u8; 900];
                self.prng.fill(&mut noise);
                Payload::raw(noise)
            }
            Kind::Sum
            | Kind::Fan
            | Kind::Signals
            | Kind::Race
            | Kind::Countdown
            | Kind::Marker
            | Kind::Cancelable
            | Kind::Terminated
            | Kind::Flaky
            | Kind::Doom
            | Kind::TimerCancel => out(&input),
        };
        let encoded = self
            .codec
            .encode(plain.clone())
            .expect("the standard codec chain over an in-memory object store always encodes");
        let id = format!("wf-{}", self.models.len());
        let queue = *self.prng.pick(&["main", "main", "q-pin", "q-auto"]);
        self.models.push(Model {
            request_id: Id128::derive(&[id.as_bytes(), b"start"]),
            id,
            kind,
            input,
            queue,
            plain,
            encoded,
            started: false,
            signals_acked: 0,
            stop_at: self.tick + self.prng.range(50, 800),
            stopped: false,
            closed: None,
            inflight: None,
            next_at: 0,
        });
        *self.report.by_kind.entry(format!("{kind:?}")).or_insert(0) += 1;
        self.report.workflows += 1;
    }

    fn drive(&mut self) {
        let tick = self.tick;
        let open = self.models.iter().filter(|m| m.closed.is_none()).count();
        if !self.settling && open < self.opts.concurrency && self.prng.chance(1, 6) {
            self.new_model();
        }
        let expired: Vec<u64> =
            self.calls.iter().filter(|(_, (_, _, deadline))| *deadline <= tick).map(|(id, _)| *id).collect();
        for id in expired {
            let (m, _, _) = self.calls.remove(&id).expect("expired call ids were just collected from self.calls");
            self.models[m].inflight = None;
        }
        for m in 0..self.models.len() {
            let model = &self.models[m];
            if model.closed.is_some() || model.inflight.is_some() || model.next_at > tick {
                continue;
            }
            let id = model.id.clone();
            let wants_stop =
                matches!(model.kind, Kind::Cancelable | Kind::Terminated) && !model.stopped && tick >= model.stop_at;
            let (action, request) = if !model.started {
                let request = ApiRequest::StartWorkflow(StartWorkflow {
                    workflow_id: id,
                    workflow_type: model.kind.workflow_type().to_string(),
                    task_queue: model.queue.to_string(),
                    input: model.encoded.clone(),
                    request_id: model.request_id,
                    ..StartWorkflow::default()
                });
                (Action::Start, request)
            } else if model.kind == Kind::Signals && model.signals_acked < model.input.n {
                let i = model.signals_acked + 1;
                let request = ApiRequest::SignalWorkflow {
                    request_id: Id128::derive(&[id.as_bytes(), b"signal", &i.to_le_bytes()]),
                    workflow_id: id,
                    run_id: None,
                    name: "add".into(),
                    input: out(&i),
                };
                (Action::Signal, request)
            } else if wants_stop && model.kind == Kind::Cancelable {
                (Action::Stop, ApiRequest::CancelWorkflow { workflow_id: id, run_id: None, reason: "sim".into() })
            } else if wants_stop {
                (Action::Stop, ApiRequest::TerminateWorkflow { workflow_id: id, run_id: None, reason: "sim".into() })
            } else {
                (Action::Probe, ApiRequest::GetResult { workflow_id: id, run_id: None, wait_ms: 1_000 })
            };
            let call = self.send_driver(DRIVER, self.active, request);
            self.calls.insert(call, (m, action, tick + 700));
            self.models[m].inflight = Some(call);
        }
    }

    fn on_driver_response(&mut self, call: u64, result: ApiResult) {
        let Some((m, action, _)) = self.calls.remove(&call) else { return };
        let tick = self.tick;
        let model = &mut self.models[m];
        model.inflight = None;
        match (action, result) {
            (Action::Start, Ok(ApiResponse::Started { .. })) => model.started = true,
            (Action::Signal, Ok(_)) => model.signals_acked += 1,
            (Action::Stop, Ok(_)) => model.stopped = true,
            (Action::Probe, Ok(ApiResponse::Workflow(d))) => {
                let a_standby_showed_the_old_run_before_its_successor_arrived =
                    d.info.status == WorkflowStatus::ContinuedAsNew;
                if d.info.status.is_closed() && !a_standby_showed_the_old_run_before_its_successor_arrived {
                    *self.report.by_status.entry(d.info.status.as_str().to_string()).or_insert(0) += 1;
                    model.closed = Some(*d);
                } else {
                    model.next_at = tick + 10;
                }
            }
            (_, Err(e)) if e.code.is_retryable() || e.code == ErrorCode::NotActive => model.next_at = tick + 30,
            (Action::Stop, Err(e)) if e.code == CLOSED_BEFORE_THE_STOP => model.stopped = true,
            (action, other) => panic!("{action:?} of {} ({:?}) answered {other:?}", model.id, model.kind),
        }
    }

    fn rpc(&mut self, cell: usize, request: ApiRequest) -> ApiResult {
        for _ in 0..300 {
            let call = self.send_driver(RPC, cell, request.clone());
            let deadline = self.tick + 800;
            let result = loop {
                self.step();
                if self.rpc_result.as_ref().is_some_and(|(id, _)| *id == call) {
                    break self.rpc_result.take().map(|(_, r)| r);
                }
                if self.tick >= deadline {
                    break None;
                }
            };
            match result {
                Some(Err(e)) if e.code.is_retryable() => self.idle(20),
                Some(result) => return result,
                None => {}
            }
        }
        panic!("cell {} never answered {}", self.cells[cell].id, request.name());
    }

    fn rpc_ok(&mut self, cell: usize, request: ApiRequest, tolerate: &[ErrorCode]) -> ApiResponse {
        for _ in 0..600 {
            match self.rpc(cell, request.clone()) {
                Ok(response) => return response,
                Err(e) if tolerate.contains(&e.code) => self.idle(25),
                Err(e) => panic!("{} on cell {} failed: {e:?}", request.name(), self.cells[cell].id),
            }
        }
        panic!("{} on cell {} never succeeded", request.name(), self.cells[cell].id);
    }

    fn idle(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.step();
        }
    }

    fn cell_ref(&self, cell: usize) -> CellRef {
        CellRef {
            cell: self.cells[cell].id,
            shards: self.opts.shards,
            replicas: self.opts.replicas,
            addresses: Vec::new(),
        }
    }

    fn drain(&mut self, cell: usize) {
        let start = self.tick;
        loop {
            if let Ok(ApiResponse::Cell(info)) = self.rpc(cell, ApiRequest::DescribeCell) {
                let complete = info.shards.len() == self.opts.shards as usize;
                if complete
                    && info
                        .shards
                        .iter()
                        .all(|s| s.replication.iter().all(|r| r.pending == 0 && r.acked_seq >= r.head_seq))
                {
                    return;
                }
                assert!(
                    self.tick < start + 60_000,
                    "cell {} did not finish replicating: {:?}",
                    info.cell,
                    info.shards.iter().map(|s| (s.shard, s.change_seq, &s.replication)).collect::<Vec<_>>()
                );
            }
            self.idle(20);
        }
    }

    fn call(&mut self, cell: usize, request: ApiRequest) -> Option<ApiResult> {
        let call = self.send_driver(RPC, cell, request);
        let deadline = self.tick + 300;
        loop {
            self.step();
            if self.rpc_result.as_ref().is_some_and(|(id, _)| *id == call) {
                return self.rpc_result.take().map(|(_, result)| result);
            }
            if self.tick >= deadline {
                return None;
            }
        }
    }

    fn controller_call(&mut self, cell: usize, request: ApiRequest) -> bool {
        let name = request.name();
        match self.call(cell, request) {
            Some(Ok(_)) => true,
            None => false,
            Some(Err(e)) if e.code.is_retryable() || THE_NEXT_ROUND_RETRIES.contains(&e.code) => false,
            Some(Err(e)) => panic!("the controller's {name} on cell {} failed: {e:?}", self.cells[cell].id),
        }
    }

    fn observe_cell(&mut self, cell: usize) -> Option<CellInfo> {
        if let Some(Ok(ApiResponse::Cell(info))) = self.call(cell, ApiRequest::DescribeCell) {
            Some(*info)
        } else {
            None
        }
    }

    fn reconcile_roles(&mut self) -> bool {
        if self.cells.len() < 2 {
            return true;
        }
        let (want_at, other_at) = (self.wanted_active, 1 - self.wanted_active);
        let Some(want_info) = self.observe_cell(want_at) else { return false };
        let other_info = self.observe_cell(other_at);
        let want = CellView::of(&want_info);
        if want.is_active() {
            self.active = want_at;
        }
        let other_view = other_info.as_ref().map(CellView::of);
        let other = other_view.as_ref().map_or(Other::Unreachable, Other::Seen);
        let index_of = |cell: CellId| if cell == want.cell { want_at } else { other_at };
        match plan_roles(&want, other, false) {
            RoleStep::Done => return true,
            RoleStep::Wait(_) | RoleStep::Blocked(_) => {}
            RoleStep::Demote { cell, epoch } => {
                self.controller_call(index_of(cell), ApiRequest::SetRole { active: false, epoch });
            }
            RoleStep::Promote { cell, epoch } => {
                if self.controller_call(index_of(cell), ApiRequest::SetRole { active: true, epoch }) {
                    self.report.failovers += 1;
                }
            }
            RoleStep::CreateReplication { on, target } => {
                let target = self.cell_ref(index_of(target));
                self.controller_call(index_of(on), ApiRequest::CreateReplication { target });
            }
            RoleStep::DeleteReplication { on, target } => {
                self.controller_call(index_of(on), ApiRequest::DeleteReplication { cell: target });
            }
        }
        false
    }

    fn run_workers(&mut self, deployment: &'static str, build: &str, replicas: u32) {
        let created_ms = now(self.tick).millis();
        let fleet = &mut self.wanted.get_mut(deployment).expect(EVERY_DEPLOYMENT_IS_WANTED).fleet;
        match fleet.iter_mut().find(|b| b.build == build) {
            Some(known) => known.replicas = replicas,
            None => fleet.push(FleetBuild { build: build.to_string(), replicas, created_ms }),
        }
        for w in 0..self.workers.len() {
            if self.workers[w].running || self.workers[w].build != Some((deployment, build)) {
                continue;
            }
            let opts = self.workers[w].opts.clone();
            let stats = self.workers[w].core.stats.clone();
            self.report.workflow_tasks += stats.workflow_tasks;
            self.report.activities += stats.activities;
            let core = self.worker_core(&opts);
            let worker = &mut self.workers[w];
            worker.core = core;
            worker.incarnation += 1;
            worker.jobs.clear();
            worker.running = true;
        }
    }

    fn stop_workers(&mut self, deployment: &'static str, build: &str, delete: bool) {
        let fleet = &mut self.wanted.get_mut(deployment).expect(EVERY_DEPLOYMENT_IS_WANTED).fleet;
        if delete {
            fleet.retain(|b| b.build != build);
        } else {
            fleet.iter_mut().filter(|b| b.build == build).for_each(|b| b.replicas = 0);
        }
        for worker in self.workers.iter_mut().filter(|w| w.build == Some((deployment, build))) {
            worker.running = false;
            worker.jobs.clear();
        }
    }

    fn apply_workers_action(&mut self, deployment: &'static str, action: WorkersAction) {
        let cell = self.active;
        match action {
            WorkersAction::Run { build, replicas } | WorkersAction::ScaleUp { build, replicas } => {
                self.run_workers(deployment, &build, replicas);
            }
            WorkersAction::SetRamp { build, percent } => {
                let request =
                    ApiRequest::SetRampingVersion { deployment: deployment.into(), build_id: Some(build), percent };
                self.controller_call(cell, request);
            }
            WorkersAction::SetCurrent { build } => {
                let request = ApiRequest::SetCurrentVersion { deployment: deployment.into(), build_id: Some(build) };
                if self.controller_call(cell, request) {
                    self.report.rollouts += 1;
                }
            }
            WorkersAction::ClearRamp => {
                let request =
                    ApiRequest::SetRampingVersion { deployment: deployment.into(), build_id: None, percent: 0 };
                self.controller_call(cell, request);
            }
            WorkersAction::ScaleToZero { build } => self.stop_workers(deployment, &build, false),
            WorkersAction::Remove { build } => {
                self.stop_workers(deployment, &build, true);
                self.controller_call(
                    cell,
                    ApiRequest::DeleteVersion { version: DeploymentVersion::new(deployment, build) },
                );
                self.report.builds_retired += 1;
            }
        }
    }

    fn reconcile_workers(&mut self, deployment: &'static str) -> bool {
        let Some(target) = self.wanted[deployment].target else { return true };
        let info = match self.call(self.active, ApiRequest::DescribeDeployment { deployment: deployment.into() }) {
            Some(Ok(ApiResponse::Deployment(info))) => Some(*info),
            Some(Err(e)) if e.code == ErrorCode::NotFound => None,
            Some(Ok(_) | Err(_)) | None => return false,
        };
        let interfere = self.opts.faults && !self.settling;
        let clock_error_ms = if interfere { self.prng.below(4_000) } else { 2_000 };
        let now_ms = (now(self.tick).millis() + clock_error_ms).saturating_sub(2_000);
        let wanted = &self.wanted[deployment];
        let plan = plan_workers(&WorkersInput {
            target,
            replicas: WORKERS_PER_BUILD,
            rollout: &wanted.rollout,
            sunset: &wanted.sunset,
            info: info.as_ref(),
            fleet: &wanted.fleet,
            now_ms,
        });
        let only_the_target_runs =
            wanted.fleet.len() == 1 && wanted.fleet[0].build == target && wanted.fleet[0].replicas > 0;
        let the_cell_routes_to_the_target = info.as_ref().is_some_and(|i| {
            i.routing.current.as_deref() == Some(target)
                && i.routing.ramping.is_none()
                && i.propagated_shards == i.shard_count
                && i.versions.iter().all(|v| match v.status {
                    VersionStatus::Current => v.build_id == target,
                    VersionStatus::Drained | VersionStatus::Inactive => true,
                    VersionStatus::Ramping | VersionStatus::Draining => false,
                })
        });
        let settled =
            plan.actions.len() == 1 && plan.message.is_empty() && only_the_target_runs && the_cell_routes_to_the_target;
        if interfere && self.prng.chance(1, 12) {
            self.stale = plan.actions.into_iter().map(|action| (deployment, action)).collect();
            self.report.controller_stale_plans += 1;
            return false;
        }
        for action in plan.actions {
            if interfere && self.prng.chance(1, 25) {
                self.report.controller_interruptions += 1;
                return false;
            }
            self.apply_workers_action(deployment, action);
        }
        if interfere && !self.stale.is_empty() && self.prng.chance(1, 3) {
            for (deployment, action) in std::mem::take(&mut self.stale) {
                self.apply_workers_action(deployment, action);
            }
        }
        settled
    }

    fn controller_round(&mut self) -> bool {
        self.report.controller_rounds += 1;
        let mut settled = self.reconcile_roles();
        for (deployment, _) in DEPLOYMENTS {
            settled &= self.reconcile_workers(deployment);
        }
        settled
    }

    fn operate(&mut self) {
        while self.intents.front().is_some_and(|(at, _)| self.settling || *at <= self.tick) {
            let (_, intent) = self.intents.pop_front().expect("the loop condition just saw a front intent");
            match intent {
                Intent::Target(deployment, build) => {
                    let wanted = self.wanted.get_mut(deployment).expect(EVERY_DEPLOYMENT_IS_WANTED);
                    wanted.target = Some(build);
                    wanted.targets_so_far.insert(build);
                }
                Intent::Active(cell) => self.wanted_active = cell,
            }
        }
        if self.tick >= self.next_round_at {
            self.controller_round();
            self.next_round_at = self.tick + self.round_period;
        }
    }

    fn converge(&mut self) {
        let start = self.tick;
        while !self.controller_round() {
            if self.tick > start + 40_000 {
                let mut cells = Vec::new();
                for c in 0..self.cells.len() {
                    cells.push(self.observe_cell(c).map(|info| CellView::of(&info)));
                }
                let mut deployments = Vec::new();
                for (deployment, _) in DEPLOYMENTS {
                    deployments
                        .push(self.call(self.active, ApiRequest::DescribeDeployment { deployment: deployment.into() }));
                }
                panic!(
                    "the controller did not settle with cell index {} wanted active\nwanted: {:#?}\ncells: {cells:#?}\ndeployments: {deployments:#?}",
                    self.wanted_active, self.wanted
                );
            }
            self.idle(self.round_period);
        }
        self.check("controller settled");
    }

    fn history(&mut self, cell: usize, workflow_id: &str, run_id: Option<Id128>) -> Vec<Event> {
        let mut events = Vec::new();
        let mut after = 0;
        loop {
            let request = ApiRequest::GetHistory {
                workflow_id: workflow_id.to_string(),
                run_id,
                after_event_id: after,
                limit: 50,
            };
            let response = self.rpc_ok(cell, request, &[]);
            let ApiResponse::History { events: page, next_event_id } = response else {
                panic!("history of {workflow_id}: {response:?}");
            };
            events.extend(page);
            if next_event_id == 0 {
                return events;
            }
            after = next_event_id;
        }
    }

    fn check(&mut self, name: &str) {
        *self.report.checks.entry(name.to_string()).or_insert(0) += 1;
    }

    fn check_workflow(&mut self, m: usize) {
        let (id, kind, input, plain, queue, signals) = {
            let model = &self.models[m];
            (model.id.clone(), model.kind, model.input.clone(), model.plain.clone(), model.queue, model.signals_acked)
        };
        let mut d =
            self.models[m].closed.clone().expect("check_workflow runs only after settle saw every model closed");
        if let Some(result) = d.result.take() {
            d.result = Some(self.codec.decode(result).unwrap_or_else(|e| panic!("{id}: result does not decode: {e}")));
        }
        if let Err(e) = workload::check_outcome(kind, &input, &plain, &d) {
            panic!("{id}: {e}");
        }
        self.check("workflow outcomes");
        let events = self.history(self.active, &id, Some(d.info.run_id));
        self.report.events += events.len() as u64;
        if let Err(e) = workload::check_history(&events, true) {
            panic!("{id}: {e}\n{}", render(&events));
        }
        assert_eq!(events.len() as u64, d.info.history_length, "{id}: history length");
        self.check("history rules");
        if kind == Kind::Signals {
            let received =
                events.iter().filter(|e| matches!(e.attrs, EventAttrs::SignalReceived { .. })).count() as u64;
            assert_eq!(received, signals, "{id}: signals in history against signals acknowledged\n{}", render(&events));
            self.check("signals exactly once");
        }
        let versions = workload::task_versions(&events);
        let builds: Vec<&str> = versions.iter().map(|v| v.as_ref().map_or("", |v| v.build_id.as_str())).collect();
        match queue {
            "main" => assert!(versions.iter().all(Option::is_none), "{id}: unversioned workflow ran on {versions:?}"),
            "q-pin" => {
                assert!(
                    versions.iter().all(|v| v.as_ref().is_some_and(|v| v.deployment == "pin"))
                        && builds.windows(2).all(|w| w[0] == w[1])
                        && builds.iter().all(|b| self.wanted["pin"].targets_so_far.contains(b)),
                    "{id}: pinned workflow ran on {versions:?}\n{}",
                    render(&events)
                );
            }
            _ => {
                assert!(
                    versions.iter().all(|v| v.as_ref().is_some_and(|v| v.deployment == "auto"))
                        && builds.iter().all(|b| self.wanted["auto"].targets_so_far.contains(b)),
                    "{id}: auto-upgrade workflow ran on {versions:?}\n{}",
                    render(&events)
                );
            }
        }
        self.check("versioning rules");
        if self.cells.len() == 2 {
            let other = 1 - self.active;
            let response =
                self.rpc_ok(other, ApiRequest::DescribeWorkflow { workflow_id: id.clone(), run_id: None }, &[]);
            let ApiResponse::Workflow(theirs) = response else {
                panic!("{id}: DescribeWorkflow on the standby answered {response:?}")
            };
            let mine =
                self.models[m].closed.clone().expect("check_workflow runs only after settle saw every model closed");
            let theirs_summary =
                (theirs.info.run_id, theirs.info.status, &theirs.result, &theirs.failure, theirs.info.history_length);
            let mine_summary =
                (mine.info.run_id, mine.info.status, &mine.result, &mine.failure, mine.info.history_length);
            if theirs_summary != mine_summary {
                let debug = self.debug_execution(&id, mine.info.run_id);
                panic!(
                    "{id}: the standby cell has a different workflow\n  standby: {theirs_summary:?}\n  active:  {mine_summary:?}\n{debug}"
                );
            }
            let replicated = self.history(other, &id, Some(mine.info.run_id));
            if replicated != events {
                let debug = self.debug_execution(&id, mine.info.run_id);
                panic!(
                    "{id}: the standby cell has a different history\n{}\n--\n{}\n{debug}",
                    render(&events),
                    render(&replicated)
                );
            }
            self.check("replicated workflows");
        }
    }

    fn debug_execution(&mut self, id: &str, run: Id128) -> String {
        use wfe_engine::chasm::{ExecKey, Local, Node, WORKFLOW};
        use wfe_engine::keys;
        let key = ExecKey { arch: WORKFLOW, id: id.to_string(), run };
        let shard = wfe_core::ids::shard_for(id, self.opts.shards) as usize;
        let mut out = String::new();
        for cell in &mut self.cells {
            for slot in &mut cell.slots[shard] {
                let Some(r) = slot.replica.as_mut() else { continue };
                if !r.is_primary() {
                    continue;
                }
                let status = r.status();
                let state = r.state_machine().shard().state.clone();
                let detail = r.with_store(|_, store| {
                    let local: Option<Local> = store
                        .get(&keys::local(&key))
                        .expect(SIM_STORE)
                        .map(|b| wfe_core::codec::decode(&b).expect(SIM_STORE));
                    let prefix = keys::exec(&key);
                    let nodes: Vec<String> = store
                        .scan(&prefix, &wfe_core::keys::prefix_end(&prefix), 1000, false)
                        .expect(SIM_STORE)
                        .into_iter()
                        .map(|(k, v)| {
                            let node: Node = wfe_core::codec::decode(&v).expect(SIM_STORE);
                            format!(
                                "{}@{:?}/seq{}",
                                String::from_utf8_lossy(&k[prefix.len()..]),
                                node.updated,
                                node.seq
                            )
                        })
                        .collect();
                    format!("local {local:?}\n  nodes {nodes:?}")
                });
                out += &format!(
                    "cell {} shard {shard} replica {} op {}: active {} epoch {} change_seq {} links {:?}\n  {}\n",
                    cell.id,
                    status.replica,
                    status.commit_min,
                    state.active,
                    state.epoch,
                    state.change_seq,
                    state.links,
                    detail.unwrap_or_default()
                );
            }
        }
        out
    }

    fn settle(&mut self) {
        self.settling = true;
        self.net.heal();
        self.heal_at = 0;
        self.net.options.drop = Ratio::percent(0);
        self.net.options.duplicate = Ratio::percent(0);
        for c in 0..self.cells.len() {
            for shard in 0..self.opts.shards {
                for replica in 0..self.opts.replicas {
                    self.restart(c, shard, replica);
                }
            }
        }
        self.operate();
        let start = self.tick;
        while self.models.iter().any(|m| m.closed.is_none()) {
            self.step();
            self.operate();
            if self.tick > start + 60_000 {
                let stuck: Vec<String> =
                    self.models.iter().filter(|m| m.closed.is_none()).map(|m| m.id.clone()).collect();
                let id = stuck[0].clone();
                let d = self.rpc(self.active, ApiRequest::DescribeWorkflow { workflow_id: id.clone(), run_id: None });
                let run = d.as_ref().ok().and_then(|d| match d {
                    ApiResponse::Workflow(d) => Some(d.info.run_id),
                    ApiResponse::Ok
                    | ApiResponse::Started { .. }
                    | ApiResponse::History { .. }
                    | ApiResponse::Workflows(_)
                    | ApiResponse::WorkflowTask(_)
                    | ApiResponse::ActivityTask(_)
                    | ApiResponse::Heartbeat { .. }
                    | ApiResponse::Deployment(_)
                    | ApiResponse::Deployments(_)
                    | ApiResponse::Cell(_)
                    | ApiResponse::Shard(_) => None,
                });
                let events = if run.is_some() { self.history(self.active, &id, run) } else { Vec::new() };
                let mut deployments = Vec::new();
                for (deployment, _) in DEPLOYMENTS {
                    let request = ApiRequest::DescribeDeployment { deployment: deployment.into() };
                    deployments.push(self.call(self.active, request));
                }
                panic!(
                    "{} workflows never finished: {stuck:?}\nfirst: {d:#?}\n{}\nactive cell index {}\nwanted: {:?}\ndeployments: {deployments:?}",
                    stuck.len(),
                    render(&events),
                    self.active,
                    self.wanted
                );
            }
        }
        self.converge();
        if self.cells.len() == 2 {
            self.drain(self.active);
            for (deployment, _) in DEPLOYMENTS {
                let request = ApiRequest::DescribeDeployment { deployment: deployment.into() };
                let mine = self.rpc_ok(self.active, request.clone(), &[]);
                let theirs = self.rpc_ok(1 - self.active, request, &[]);
                let (ApiResponse::Deployment(mine), ApiResponse::Deployment(theirs)) = (mine, theirs) else {
                    panic!("DescribeDeployment of {deployment} did not answer with a deployment on both cells")
                };
                assert_eq!(theirs.routing, mine.routing, "{deployment}: the standby cell has a different routing");
                self.check("replicated deployments");
            }
        }
        for m in 0..self.models.len() {
            self.check_workflow(m);
        }
        let start = self.tick;
        while !self.check_replicas(true, Compare::Contents) {
            assert!(self.tick < start + 20_000, "replicas did not converge");
            self.idle(50);
        }
    }

    pub fn run(mut self) -> Report {
        while self.tick < self.opts.ticks {
            self.step();
            self.operate();
        }
        self.settle();
        let mut report = std::mem::take(&mut self.report);
        report.ticks = self.tick;
        report.sim_ms = self.tick * 10;
        report.messages = self.net.sent;
        report.messages_dropped = self.net.dropped;
        report.claim_checks = self.objects.len() as u64;
        for worker in &self.workers {
            report.workflow_tasks += worker.core.stats.workflow_tasks;
            report.activities += worker.core.stats.activities;
        }
        for cell in &mut self.cells {
            for row in &mut cell.slots {
                if let Some(r) = row[0].replica.as_mut() {
                    report.replicated += r.state_machine().shard().state.replicated_in;
                }
            }
        }
        report.digest = format!("{:032x}", self.digest.finish());
        report
    }
}

fn render(events: &[Event]) -> String {
    events.iter().map(|e| format!("  {:>3} {} {:?}", e.id, e.time, e.attrs)).collect::<Vec<_>>().join("\n")
}

/// Run one seed.
pub fn run(opts: Options) -> Report {
    World::new(opts).run()
}
