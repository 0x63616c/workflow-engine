#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_macros,
    clippy::unwrap_used,
    reason = "test harness: seeds come from env vars for replay, traces print, and a panic in a helper is a test failure"
)]

use std::collections::BTreeMap;
use wfe_core::checksum::Hasher;
use wfe_core::device::SECTOR_SIZE;
use wfe_core::{Addr, Prng, Ratio, Timestamp};
use wfe_lsm::{LsmOptions, StorageError};

const REPLICA_INDEX: &str = "replica indexes are below REPLICAS, which is a u8";
use wfe_sim::{Net, NetOptions, SimDevice};
use wfe_vsr::message::RejectReason;
use wfe_vsr::superblock::{SUPERBLOCK_COPIES, SUPERBLOCK_COPY_SIZE};
use wfe_vsr::{
    Event, Layout, Message, OpContext, Packet, Replica, ReplicaConfig, Request, StateMachine, Status, Store, Timing,
};

const META: &[u8] = b"\0meta";

#[derive(Default)]
struct Kv {
    digest: u128,
    applied: u64,
    pulses: u64,
    next_pulse: u64,
}

impl Kv {
    fn save(&self, store: &mut Store) -> Result<(), StorageError> {
        let mut meta = Vec::new();
        meta.extend_from_slice(&self.digest.to_le_bytes());
        meta.extend_from_slice(&self.applied.to_le_bytes());
        meta.extend_from_slice(&self.pulses.to_le_bytes());
        meta.extend_from_slice(&self.next_pulse.to_le_bytes());
        store.put(META, &meta)
    }
}

impl StateMachine for Kv {
    type Packet = ();

    fn open(&mut self, store: &mut Store) -> Result<(), StorageError> {
        *self = Kv::default();
        if let Some(meta) = store.get(META)? {
            self.digest = u128::from_le_bytes(meta[0..16].try_into().unwrap());
            self.applied = u64::from_le_bytes(meta[16..24].try_into().unwrap());
            self.pulses = u64::from_le_bytes(meta[24..32].try_into().unwrap());
            self.next_pulse = u64::from_le_bytes(meta[32..40].try_into().unwrap());
        }
        Ok(())
    }

    fn apply(&mut self, store: &mut Store, ctx: &OpContext, body: &[u8]) -> Result<Vec<u8>, StorageError> {
        let count = match store.get(body)? {
            Some(value) => u64::from_le_bytes(value[0..8].try_into().unwrap()) + 1,
            None => 1,
        };
        self.digest = Hasher::new()
            .update_u128(self.digest)
            .update(body)
            .update_u64(ctx.op)
            .update_u64(ctx.timestamp.micros())
            .update_u64(count)
            .finish();
        self.applied += 1;
        if count % 7 == 0 {
            store.delete(body);
        } else {
            let mut value = count.to_le_bytes().to_vec();
            value
                .resize(8 + usize::try_from(self.digest % 700).expect("x % 700 fits in usize"), count.to_le_bytes()[0]);
            store.put(body, &value)?;
        }
        if self.next_pulse == 0 {
            self.next_pulse = ctx.timestamp.micros() + 300_000;
        }
        self.save(store)?;
        Ok(count.to_le_bytes().to_vec())
    }

    fn pulse(&mut self, store: &mut Store, ctx: &OpContext) -> Result<(), StorageError> {
        if self.next_pulse != 0 && ctx.timestamp.micros() >= self.next_pulse {
            self.pulses += 1;
            self.next_pulse = ctx.timestamp.micros() + 300_000;
            self.digest = Hasher::new().update_u128(self.digest).update_u64(self.pulses).finish();
            self.save(store)?;
        }
        Ok(())
    }

    fn pulse_at(&self) -> Option<Timestamp> {
        (self.next_pulse != 0).then_some(Timestamp(self.next_pulse))
    }
}

const REPLICAS: u8 = 3;
const CLUSTER: u128 = 0xC0FFEE;

fn config(replica: u8) -> ReplicaConfig {
    ReplicaConfig {
        cluster: CLUSTER,
        cell: 1,
        shard: 0,
        replica,
        replica_count: REPLICAS,
        layout: Layout { slot_count: 32, message_size_max: 8192, grid_block_size: 4096, grid_block_count: 1024 },
        lsm: LsmOptions {
            memtable_bytes_max: 6 * 1024,
            l0_tables_max: 2,
            level_growth: 3,
            table_data_blocks_max: 3,
            cache_blocks: 8,
        },
        checkpoint_interval: 8,
        pipeline_max: 4,
        request_queue_max: 16,
        timing: Timing::default(),
    }
}

struct Client {
    request_id: u128,
    key: Vec<u8>,
    target: u8,
    sent_at: u64,
}

struct Sim {
    prng: Prng,
    tick: u64,
    net: Net<Packet<()>>,
    up: Vec<Option<Replica<Kv, SimDevice>>>,
    down: Vec<Option<SimDevice>>,
    clients: Vec<Client>,
    committed: BTreeMap<u64, u128>,
    replies: u64,
    crashes: u64,
    corruptions: u64,
    view_max: u32,
    trace: bool,
    trace_views: bool,
    digests: BTreeMap<u64, (u128, u64, u64)>,
}

fn now(tick: u64) -> Timestamp {
    Timestamp::from_millis(1_000_000 + tick * 10)
}

impl Sim {
    fn new(seed: u64) -> Sim {
        let mut prng = Prng::new(seed);
        let net = Net::new(
            NetOptions {
                delay_min: 1,
                delay_max: prng.range(2, 12),
                drop: Ratio::percent(prng.below(8)),
                duplicate: Ratio::percent(prng.below(5)),
            },
            prng.fork(),
        );
        let mut up = Vec::new();
        for replica in 0..REPLICAS {
            let mut dev = SimDevice::new();
            Replica::<Kv, SimDevice>::format(&mut dev, &config(replica));
            up.push(Some(Replica::open(dev, config(replica), Kv::default(), prng.next_u64(), now(0)).unwrap()));
        }
        let clients = (0..4)
            .map(|_| Client { request_id: prng.next_u128() | 1, key: b"k0".to_vec(), target: 0, sent_at: 0 })
            .collect();
        Sim {
            prng,
            tick: 0,
            net,
            up,
            down: (0..REPLICAS).map(|_| None).collect(),
            clients,
            committed: BTreeMap::new(),
            replies: 0,
            crashes: 0,
            corruptions: 0,
            view_max: 0,
            trace: false,
            trace_views: std::env::var("WFE_TRACE_VIEWS").is_ok(),
            digests: BTreeMap::new(),
        }
    }

    fn addr(replica: u8) -> Addr {
        Addr::replica(1, 0, replica)
    }

    fn send_request(&mut self, client: usize) {
        let c = &mut self.clients[client];
        c.sent_at = self.tick;
        let request = Request { request_id: c.request_id, client: Addr::client(client as u64), body: c.key.clone() };
        let to = Self::addr(c.target);
        self.net.send(self.tick, Addr::client(client as u64), to, Packet::Vsr(Message::Request(request)));
    }

    fn on_client_packet(&mut self, client: usize, packet: Packet<()>) {
        match packet {
            Packet::Vsr(Message::Reply(reply)) if reply.request_id == self.clients[client].request_id => {
                self.replies += 1;
                let key = format!("k{}", self.prng.below(40)).into_bytes();
                let c = &mut self.clients[client];
                c.request_id = self.prng.next_u128() | 1;
                c.key = key;
                self.send_request(client);
            }
            Packet::Vsr(Message::Reject { request_id, view, reason })
                if request_id == self.clients[client].request_id && reason == RejectReason::NotPrimary =>
            {
                self.clients[client].target = u8::try_from(view % u32::from(REPLICAS)).expect(REPLICA_INDEX);
            }
            Packet::Vsr(_) | Packet::App(_) => {}
        }
    }

    fn crash(&mut self, replica: usize) {
        if let Some(r) = self.up[replica].take() {
            let mut dev = r.into_device();
            dev.crash(&mut self.prng);
            if self.trace_views {
                println!("t{} r{replica}: crash", self.tick);
            }
            self.down[replica] = Some(dev);
            self.crashes += 1;
        }
    }

    fn restart(&mut self, replica: usize) {
        if let Some(dev) = self.down[replica].take() {
            let seed = self.prng.next_u64();
            let r = Replica::open(
                dev,
                config(u8::try_from(replica).expect(REPLICA_INDEX)),
                Kv::default(),
                seed,
                now(self.tick),
            )
            .unwrap_or_else(|e| panic!("replica {replica} cannot restart: {e}"));
            if self.trace_views {
                println!("t{} r{replica}: restart {:?}", self.tick, r.status());
            }
            self.up[replica] = Some(r);
        }
    }

    fn corrupt(&mut self) {
        let layout = config(0).layout;
        let sector = self.prng.below(layout.total_size() / SECTOR_SIZE as u64);
        let offset = sector * SECTOR_SIZE as u64;
        let victim = usize::try_from(wfe_core::checksum::route_hash(&sector.to_le_bytes()) % u64::from(REPLICAS))
            .expect(REPLICA_INDEX);
        if offset < SUPERBLOCK_COPIES * SUPERBLOCK_COPY_SIZE && offset / SUPERBLOCK_COPY_SIZE != 2 {
            return;
        }
        let dev = match self.up[victim].as_mut() {
            Some(r) => r.device_mut(),
            None => self.down[victim].as_mut().unwrap(),
        };
        if offset < dev.len() {
            dev.corrupt_sector(offset, &mut self.prng);
            self.corruptions += 1;
        }
    }

    fn step(&mut self, chaos: bool) {
        self.tick += 1;
        let tick = self.tick;
        for (from, to, packet) in self.net.deliver(tick) {
            match to {
                Addr::Replica { replica, .. } => {
                    if let Some(r) = self.up[replica as usize].as_mut() {
                        r.on_packet(from, packet, now(tick));
                    }
                }
                Addr::Client { id } => {
                    self.on_client_packet(usize::try_from(id).expect("client ids are small indexes"), packet)
                }
                Addr::Gateway { .. } | Addr::ObjectStore { .. } => {}
            }
        }
        for replica in 0..REPLICAS as usize {
            let Some(r) = self.up[replica].as_mut() else { continue };
            r.tick(now(tick));
            let outbox = r.take_outbox();
            let events = r.take_events();
            self.view_max = self.view_max.max(r.view());
            for (to, packet) in outbox {
                if self.trace
                    && let Packet::Vsr(m) = &packet
                {
                    println!("t{tick} r{replica} -> {to}: {m:?}");
                }
                if self.trace_views {
                    match &packet {
                        Packet::Vsr(Message::DoViewChange(d)) => println!(
                            "t{tick} r{replica} -> {to}: DVC view {} log_view {} top {} commit {}..{} cp {} {:?}",
                            d.view,
                            d.log_view,
                            d.top_op,
                            d.commit_min,
                            d.commit_max,
                            d.checkpoint_op,
                            d.headers.iter().map(|(h, held)| (h.op, h.view, *held)).collect::<Vec<_>>()
                        ),
                        Packet::Vsr(Message::StartView(v)) => println!(
                            "t{tick} r{replica} -> {to}: SV view {} commit {} {:?}",
                            v.view,
                            v.commit_max,
                            v.headers.iter().map(|h| (h.op, h.view)).collect::<Vec<_>>()
                        ),
                        Packet::Vsr(Message::SyncTarget { view, checkpoint }) => {
                            println!("t{tick} r{replica} -> {to}: SyncTarget view {view} op {}", checkpoint.op())
                        }
                        Packet::Vsr(_) | Packet::App(_) => {}
                    }
                }
                self.net.send(tick, Self::addr(u8::try_from(replica).expect(REPLICA_INDEX)), to, packet);
            }
            let r = self.up[replica].as_mut().unwrap();
            let commit_min = r.status().commit_min;
            if let Some(state) = r.with_store(|sm, _| (sm.digest, sm.applied, sm.pulses)) {
                let known = *self.digests.entry(commit_min).or_insert(state);
                assert_eq!(
                    known, state,
                    "replica {replica} diverged at op {commit_min}: replicas that executed the same ops must hold the same state"
                );
                while self.digests.len() > 4096 {
                    self.digests.pop_first();
                }
            }
            for event in events {
                if self.trace_views && !matches!(event, Event::Committed { .. } | Event::Checkpointed { .. }) {
                    println!("t{tick} r{replica}: {event:?}");
                }
                if let Event::Committed { op, checksum, .. } = event {
                    let known = *self.committed.entry(op).or_insert(checksum);
                    assert_eq!(known, checksum, "replica {replica} committed a different op {op}");
                }
            }
        }
        for client in 0..self.clients.len() {
            if tick >= self.clients[client].sent_at + 120 {
                if self.clients[client].sent_at != 0 {
                    self.clients[client].target =
                        u8::try_from(self.prng.below(u64::from(REPLICAS))).expect(REPLICA_INDEX);
                }
                self.send_request(client);
            }
        }
        if !chaos {
            return;
        }
        if self.prng.chance(1, 1200) {
            let downs = self.down.iter().filter(|d| d.is_some()).count();
            if downs == 0 || self.prng.chance(1, 5) {
                let replica = self.prng.index(REPLICAS as usize);
                self.crash(replica);
            }
        }
        for replica in 0..REPLICAS as usize {
            if self.down[replica].is_some() && self.prng.chance(1, 250) {
                self.restart(replica);
            }
        }
        if self.prng.chance(1, 1500) {
            let isolated = u8::try_from(self.prng.below(u64::from(REPLICAS))).expect(REPLICA_INDEX);
            let others: Vec<Addr> = (0..REPLICAS).filter(|r| *r != isolated).map(Self::addr).collect();
            if self.prng.bool() {
                self.net.partition(&[Self::addr(isolated)], &others);
            } else {
                let deafened = Self::addr(isolated);
                for talker in others {
                    self.net.cut(talker, deafened);
                }
            }
        }
        if self.net.is_partitioned() && self.prng.chance(1, 400) {
            self.net.heal();
        }
        if self.prng.chance(1, 300) {
            self.corrupt();
        }
    }

    fn converged(&self) -> bool {
        let statuses: Vec<_> = self.up.iter().map(|r| r.as_ref().unwrap().status()).collect();
        let head = statuses.iter().map(|s| s.commit_min).max().unwrap();
        statuses.iter().filter(|s| s.primary).count() == 1
            && statuses.iter().all(|s| {
                s.status == Status::Normal
                    && !s.storage_repairing
                    && s.commit_min + 8 >= head
                    && s.view == statuses[0].view
            })
    }

    fn run(seed: u64, ticks: u64) -> (u64, u64, u64, u32) {
        let mut sim = Sim::new(seed);
        for _ in 0..ticks {
            sim.step(true);
        }
        sim.net.heal();
        sim.net.options.drop = Ratio::ZERO;
        for replica in 0..REPLICAS as usize {
            sim.restart(replica);
        }
        let mut settled = 0;
        let mut replies_before = sim.replies;
        for i in 0.. {
            sim.step(false);
            if sim.converged() {
                settled += 1;
            } else {
                settled = 0;
                replies_before = sim.replies;
            }
            if settled > 1000 && sim.replies > replies_before + 20 {
                break;
            }
            if i > 60_000 && !sim.trace {
                sim.trace = std::env::var("WFE_TRACE").is_ok();
            }
            if i > 60_060 {
                let statuses: Vec<String> = sim
                    .up
                    .iter()
                    .map(|r| {
                        let s = r.as_ref().unwrap().status();
                        format!(
                            "r{} {:?}{} view {}/{} op {} commit {}..{} checkpoint {} missing {} repairing {} pipeline {} c {:?}",
                            s.replica,
                            s.status,
                            if s.primary { " primary" } else { "" },
                            s.view,
                            s.log_view,
                            s.op,
                            s.commit_min,
                            s.commit_max,
                            s.checkpoint_op,
                            s.log_missing,
                            s.storage_repairing,
                            s.pipeline,
                            s.counters
                        )
                    })
                    .collect();
                panic!("seed {seed}: no convergence after healing:\n{}", statuses.join("\n"));
            }
        }
        assert!(sim.digests.len() > 1, "seed {seed}: nothing was compared");
        (sim.replies, sim.crashes, sim.corruptions, sim.view_max)
    }
}

#[test]
fn survives_chaos() {
    let seeds: u64 = std::env::var("WFE_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(24);
    let start: u64 = std::env::var("WFE_SEED").ok().and_then(|s| s.parse().ok()).unwrap_or(1);
    let ticks: u64 = std::env::var("WFE_TICKS").ok().and_then(|s| s.parse().ok()).unwrap_or(20_000);
    for seed in start..start + seeds {
        let (replies, crashes, corruptions, view) =
            wfe_sim::findings::recorded("vsr-kv", seed, &format!("ticks={ticks}"), || Sim::run(seed, ticks));
        println!("seed {seed}: {replies} replies, {crashes} crashes, {corruptions} corruptions, view {view}");
    }
}
