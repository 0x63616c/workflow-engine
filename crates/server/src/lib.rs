//! A WFE node.
//!
//! One process hosts one replica of every shard of a cell (in production,
//! node `i` of `replicas` nodes; in development one process can host them
//! all) together with a gateway. The deterministic core is exactly what the
//! simulator runs; this crate gives it files, sockets and a clock:
//!
//! - a single core thread owns the replicas and the gateway, and steps
//!   them on packets, API calls and a 10 ms tick;
//! - cluster traffic is length-prefixed MessagePack frames over TCP, to
//!   peers of this cell and (for replication) of other cells;
//! - clients use HTTP: `POST /api` with a JSON `ApiRequest`.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::mpsc as std_mpsc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};
use wfe_core::api::*;
use wfe_core::codec::{decode, encode};
use wfe_core::device::FileDevice;
use wfe_core::{Addr, CellId, Error, Timestamp};
use wfe_engine::{Engine, EngineConfig, EnginePacket};
use wfe_gateway::{Gateway, GatewayConfig};
use wfe_lsm::LsmOptions;
use wfe_vsr::{Layout, Packet, Replica, ReplicaConfig, Timing};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub cell: CellId,
    pub shards: u32,
    pub replicas: u8,
    /// This node's index among the cell's nodes. It hosts replica `node`
    /// of every shard, and gateway `node`.
    pub node: u8,
    /// Host every replica in this one process (development).
    pub all_in_one: bool,
    pub data_dir: PathBuf,
    /// Cluster (TCP) and client (HTTP) listen addresses.
    pub listen: SocketAddr,
    pub http: SocketAddr,
    /// Cluster addresses of this cell's nodes, by node index.
    pub peers: Vec<String>,
    /// Cluster addresses of other cells' nodes, for replication. Cells not
    /// listed here are reached at the addresses their replication link or
    /// their own frames carry.
    pub cells: BTreeMap<CellId, Vec<String>>,
    /// How other cells reach this cell's nodes, in node order. Empty:
    /// `peers`, or `listen` for an all-in-one node.
    pub advertise: Vec<String>,
    /// Data file capacity for each replica, in bytes.
    pub data_file_bytes: u64,
}

impl NodeConfig {
    /// One process, the whole cell, on local ports.
    pub fn dev(
        cell: CellId,
        shards: u32,
        replicas: u8,
        data_dir: PathBuf,
        listen: SocketAddr,
        http: SocketAddr,
    ) -> NodeConfig {
        NodeConfig {
            cell,
            shards,
            replicas,
            node: 0,
            all_in_one: true,
            data_dir,
            listen,
            http,
            peers: Vec::new(),
            cells: BTreeMap::new(),
            advertise: Vec::new(),
            data_file_bytes: 1 << 30,
        }
    }

    /// The cluster addresses this cell tells other cells.
    pub fn advertised(&self) -> Vec<String> {
        if !self.advertise.is_empty() {
            self.advertise.clone()
        } else if !self.peers.is_empty() {
            self.peers.clone()
        } else {
            vec![self.listen.to_string()]
        }
    }

    fn hosts(&self, replica: u8) -> bool {
        self.all_in_one || replica == self.node
    }

    fn replica_config(&self, shard: u32, replica: u8) -> ReplicaConfig {
        const BLOCK: u32 = 64 * 1024;
        ReplicaConfig {
            cluster: 0x7765_6674_0000_0000_0000_0000 + (u128::from(self.cell) << 32) + u128::from(shard),
            cell: self.cell,
            shard,
            replica,
            replica_count: self.replicas,
            layout: Layout {
                slot_count: 256,
                message_size_max: 1024 * 1024,
                grid_block_size: BLOCK,
                grid_block_count: (self.data_file_bytes / u64::from(BLOCK)).max(1024),
            },
            lsm: LsmOptions {
                memtable_bytes_max: 8 << 20,
                l0_tables_max: 4,
                level_growth: 8,
                table_data_blocks_max: 256,
                cache_blocks: 1024,
            },
            checkpoint_interval: 128,
            pipeline_max: 16,
            request_queue_max: 512,
            timing: Timing::default(),
        }
    }
}

/// What a node reports about itself (`GET /status`).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct NodeStatus {
    pub cell: CellId,
    pub node: u8,
    pub shards: u32,
    pub replicas: u8,
    pub hosted: Vec<ReplicaView>,
    pub gateway_calls: u64,
    pub gateway_open_calls: u64,
    pub uptime_ms: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicaView {
    pub shard: u32,
    pub replica: u8,
    pub status: String,
    pub primary: bool,
    pub view: u32,
    pub op: u64,
    pub commit: u64,
    pub checkpoint_op: u64,
    pub repairing: bool,
}

#[derive(Serialize, Deserialize)]
struct Frame {
    from: Addr,
    to: Addr,
    packet: Packet<EnginePacket>,
    #[serde(default)]
    origin: Vec<String>,
}

enum Input {
    Packet(Addr, Addr, Packet<EnginePacket>),
    Origin(CellId, Vec<String>),
    Api(ApiRequest, oneshot::Sender<ApiResult>),
    Status(oneshot::Sender<NodeStatus>),
}

fn wall_clock() -> Timestamp {
    Timestamp::from_micros(
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX)),
    )
}

struct MonotonicClock(Timestamp);

impl MonotonicClock {
    fn now(&mut self) -> Timestamp {
        self.0 = self.0.max(wall_clock());
        self.0
    }
}

struct Core {
    config: NodeConfig,
    replicas: Vec<Replica<Engine, FileDevice>>,
    gateway: Gateway,
    clock: MonotonicClock,
    local: VecDeque<(Addr, Addr, Packet<EnginePacket>)>,
    links: BTreeMap<String, mpsc::Sender<Vec<u8>>>,
    cell_addresses_learned_at_runtime: BTreeMap<CellId, Vec<String>>,
    advertised: Vec<String>,
    waiters: BTreeMap<u64, oneshot::Sender<ApiResult>>,
    next_call: u64,
    started: Instant,
    ticks: u64,
    runtime: tokio::runtime::Handle,
}

impl Core {
    fn open(config: NodeConfig, runtime: tokio::runtime::Handle) -> anyhow::Result<Core> {
        std::fs::create_dir_all(&config.data_dir)?;
        let mut clock = MonotonicClock(Timestamp::ZERO);
        let mut replicas = Vec::new();
        let seed = wall_clock().micros() ^ (u64::from(std::process::id()) << 32);
        for shard in 0..config.shards {
            for replica in (0..config.replicas).filter(|r| config.hosts(*r)) {
                let rc = config.replica_config(shard, replica);
                let path = config.data_dir.join(format!("cell{}-shard{shard}-replica{replica}.wfe", config.cell));
                let fresh = !FileDevice::exists(&path);
                let mut dev = FileDevice::open(&path, true)?;
                if fresh {
                    Replica::<Engine, FileDevice>::format(&mut dev, &rc);
                    tracing::info!(?path, "formatted data file");
                }
                let engine = Engine::new(EngineConfig::new(config.cell, shard, config.shards, config.replicas));
                let r = Replica::open(dev, rc, engine, seed.wrapping_add(replicas.len() as u64), clock.now())
                    .map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))?;
                replicas.push(r);
            }
        }
        let mut gateway_config =
            GatewayConfig::new(config.cell, u16::from(config.node), config.shards, config.replicas);
        gateway_config.addresses = config.advertised();
        let gateway = Gateway::new(gateway_config, seed.rotate_left(17) | 1);
        let advertised = config.advertised();
        Ok(Core {
            config,
            replicas,
            gateway,
            clock,
            local: VecDeque::new(),
            links: BTreeMap::new(),
            cell_addresses_learned_at_runtime: BTreeMap::new(),
            advertised,
            waiters: BTreeMap::new(),
            next_call: 0,
            started: Instant::now(),
            ticks: 0,
            runtime,
        })
    }

    fn route(&self, to: Addr) -> Option<Option<String>> {
        let (cell, index) = match to {
            Addr::Replica { cell, replica, .. } => (cell, replica as usize),
            Addr::Gateway { cell, gateway } => (cell, gateway as usize),
            Addr::Client { .. } | Addr::ObjectStore { .. } => return None,
        };
        if cell == self.config.cell {
            let here = match to {
                Addr::Replica { replica, .. } => self.config.hosts(replica),
                Addr::Gateway { .. } | Addr::Client { .. } | Addr::ObjectStore { .. } => {
                    index == self.config.node as usize
                }
            };
            if here {
                return Some(None);
            }
            return self.config.peers.get(index).cloned().map(Some);
        }
        let nodes = self.config.cells.get(&cell).or_else(|| self.cell_addresses_learned_at_runtime.get(&cell))?;
        if nodes.is_empty() {
            return None;
        }
        Some(Some(nodes[index % nodes.len()].clone()))
    }

    fn learn_from_links(&mut self) {
        for replica in &self.replicas {
            for link in replica.state_machine().shard().state.links.values() {
                let target = &link.target;
                if !target.addresses.is_empty()
                    && self.cell_addresses_learned_at_runtime.get(&target.cell) != Some(&target.addresses)
                {
                    tracing::info!(cell = target.cell, addresses = ?target.addresses, "replication target addresses");
                    self.cell_addresses_learned_at_runtime.insert(target.cell, target.addresses.clone());
                }
            }
        }
    }

    fn send(&mut self, from: Addr, to: Addr, packet: Packet<EnginePacket>) {
        match self.route(to) {
            Some(None) => self.local.push_back((from, to, packet)),
            Some(Some(address)) => {
                let other_cell =
                    !matches!(to, Addr::Replica { cell, .. } | Addr::Gateway { cell, .. } if cell == self.config.cell);
                let origin = if other_cell { self.advertised.clone() } else { Vec::new() };
                let frame = encode(&Frame { from, to, packet, origin });
                let link = self.links.entry(address.clone()).or_insert_with(|| {
                    let (tx, rx) = mpsc::channel(4096);
                    self.runtime.spawn(link(address, rx));
                    tx
                });
                if link.try_send(frame).is_err() {
                    tracing::debug!("dropped a frame for a slow or dead peer; the protocols resend it");
                }
            }
            None => {}
        }
    }

    fn deliver(&mut self, from: Addr, to: Addr, packet: Packet<EnginePacket>) {
        let now = self.clock.now();
        match to {
            Addr::Replica { cell, shard, replica } if cell == self.config.cell => {
                if let Some(r) =
                    self.replicas.iter_mut().find(|r| r.config().shard == shard && r.config().replica == replica)
                {
                    r.on_packet(from, packet, now);
                }
            }
            Addr::Gateway { cell, gateway } if cell == self.config.cell && gateway == u16::from(self.config.node) => {
                self.gateway.on_cluster(from, packet, now);
            }
            Addr::Replica { .. } | Addr::Gateway { .. } | Addr::Client { .. } | Addr::ObjectStore { .. } => {}
        }
    }

    fn flush(&mut self) {
        loop {
            for i in 0..self.replicas.len() {
                let from = {
                    let c = self.replicas[i].config();
                    Addr::replica(c.cell, c.shard, c.replica)
                };
                for (to, packet) in self.replicas[i].take_outbox() {
                    self.send(from, to, packet);
                }
                self.replicas[i].take_events();
            }
            let from = self.gateway.addr();
            for (to, packet) in self.gateway.take_cluster() {
                self.send(from, to, packet);
            }
            for (_, message) in self.gateway.take_client() {
                if let ClientMessage::Response { id, result } = message
                    && let Some(waiter) = self.waiters.remove(&id)
                    && waiter.send(result).is_err()
                {
                    tracing::debug!("a caller stopped waiting before its result arrived");
                }
            }
            if self.local.is_empty() {
                return;
            }
            while let Some((from, to, packet)) = self.local.pop_front() {
                self.deliver(from, to, packet);
            }
        }
    }

    fn status(&mut self) -> NodeStatus {
        NodeStatus {
            cell: self.config.cell,
            node: self.config.node,
            shards: self.config.shards,
            replicas: self.config.replicas,
            hosted: self
                .replicas
                .iter()
                .map(|r| {
                    let s = r.status();
                    ReplicaView {
                        shard: r.config().shard,
                        replica: s.replica,
                        status: format!("{:?}", s.status).to_lowercase(),
                        primary: s.primary,
                        view: s.view,
                        op: s.op,
                        commit: s.commit_min,
                        checkpoint_op: s.checkpoint_op,
                        repairing: s.storage_repairing,
                    }
                })
                .collect(),
            gateway_calls: self.gateway.stats.calls,
            gateway_open_calls: self.gateway.stats.open_calls as u64,
            uptime_ms: u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
        }
    }

    fn run(mut self, inputs: std_mpsc::Receiver<Input>) {
        const TICK: Duration = Duration::from_millis(10);
        let mut next_tick = Instant::now();
        loop {
            let wait = next_tick.saturating_duration_since(Instant::now());
            match inputs.recv_timeout(wait) {
                Ok(Input::Packet(from, to, packet)) => self.deliver(from, to, packet),
                Ok(Input::Origin(cell, addresses)) => {
                    if cell != self.config.cell && self.cell_addresses_learned_at_runtime.get(&cell) != Some(&addresses)
                    {
                        tracing::info!(cell, ?addresses, "learned another cell's addresses");
                        self.cell_addresses_learned_at_runtime.insert(cell, addresses);
                    }
                }
                Ok(Input::Api(request, waiter)) => {
                    self.next_call += 1;
                    self.waiters.insert(self.next_call, waiter);
                    let now = self.clock.now();
                    let message = ClientMessage::Request { id: self.next_call, request };
                    self.gateway.on_client(Addr::client(self.next_call), message, now);
                }
                Ok(Input::Status(reply)) => {
                    if reply.send(self.status()).is_err() {
                        tracing::debug!("a status caller stopped waiting before its reply arrived");
                    }
                }
                Err(std_mpsc::RecvTimeoutError::Timeout) => {}
                Err(std_mpsc::RecvTimeoutError::Disconnected) => return,
            }
            if Instant::now() >= next_tick {
                next_tick += TICK;
                let fell_behind_so_skip_missed_ticks_instead_of_bursting = next_tick < Instant::now();
                if fell_behind_so_skip_missed_ticks_instead_of_bursting {
                    next_tick = Instant::now() + TICK;
                }
                let now = self.clock.now();
                for r in &mut self.replicas {
                    r.tick(now);
                }
                self.gateway.tick(now);
                self.ticks += 1;
                if self.ticks.is_multiple_of(50) {
                    self.learn_from_links();
                }
                self.waiters.retain(|_, w| !w.is_closed());
            }
            self.flush();
        }
    }
}

fn drop_frames_queued_while_disconnected(frames: &mut mpsc::Receiver<Vec<u8>>) {
    while frames.try_recv().is_ok() {}
}

const A_PEER_THAT_TAKES_LONGER_THAN_THIS_IS_GONE: Duration = Duration::from_secs(2);

async fn connected_peer(address: &str) -> Option<tokio::net::TcpStream> {
    match tokio::time::timeout(A_PEER_THAT_TAKES_LONGER_THAN_THIS_IS_GONE, tokio::net::TcpStream::connect(address))
        .await
    {
        Ok(Ok(stream)) => Some(stream),
        Ok(Err(_)) | Err(_) => None,
    }
}

async fn link(address: String, mut frames: mpsc::Receiver<Vec<u8>>) {
    loop {
        let Some(mut stream) = connected_peer(&address).await else {
            tokio::time::sleep(Duration::from_millis(250)).await;
            drop_frames_queued_while_disconnected(&mut frames);
            continue;
        };
        if let Err(error) = stream.set_nodelay(true) {
            tracing::warn!(%address, %error, "could not disable Nagle's algorithm on a peer link; frames may be delayed");
        }
        let (mut from_peer, mut to_peer) = stream.split();
        let mut nothing_is_ever_sent_back = [0u8; 64];
        loop {
            tokio::select! {
                frame = frames.recv() => {
                    let Some(frame) = frame else { return };
                    let mut buf = Vec::with_capacity(frame.len() + 4);
                    let frame_len = u32::try_from(frame.len()).expect("encoded frames are far smaller than 4 GiB");
                    buf.extend_from_slice(&frame_len.to_le_bytes());
                    buf.extend_from_slice(&frame);
                    let written =
                        tokio::time::timeout(A_PEER_THAT_TAKES_LONGER_THAN_THIS_IS_GONE, to_peer.write_all(&buf)).await;
                    let the_peer_stopped_reading_or_went_away = !matches!(written, Ok(Ok(())));
                    if the_peer_stopped_reading_or_went_away {
                        break;
                    }
                }
                read = from_peer.read(&mut nothing_is_ever_sent_back) => {
                    let the_peer_closed_the_connection = !matches!(read, Ok(n) if n > 0);
                    if the_peer_closed_the_connection {
                        break;
                    }
                }
            }
        }
        drop_frames_queued_while_disconnected(&mut frames);
    }
}

async fn accept(listener: tokio::net::TcpListener, inputs: std_mpsc::Sender<Input>) {
    loop {
        let Ok((mut stream, _)) = listener.accept().await else { continue };
        let inputs = inputs.clone();
        tokio::spawn(async move {
            let mut len = [0u8; 4];
            loop {
                if stream.read_exact(&mut len).await.is_err() {
                    return;
                }
                let len = u32::from_le_bytes(len) as usize;
                if len > 8 << 20 {
                    return;
                }
                let mut buf = vec![0u8; len];
                if stream.read_exact(&mut buf).await.is_err() {
                    return;
                }
                let Ok(frame) = decode::<Frame>(&buf) else { return };
                if !frame.origin.is_empty()
                    && let Addr::Replica { cell, .. } | Addr::Gateway { cell, .. } = frame.from
                    && inputs.send(Input::Origin(cell, frame.origin)).is_err()
                {
                    return;
                }
                if inputs.send(Input::Packet(frame.from, frame.to, frame.packet)).is_err() {
                    return;
                }
            }
        });
    }
}

/// A handle on a running node, for in-process callers.
#[derive(Clone)]
pub struct NodeHandle {
    inputs: std_mpsc::Sender<Input>,
    pub http: SocketAddr,
    pub cluster: SocketAddr,
}

impl NodeHandle {
    pub async fn call(&self, request: ApiRequest) -> ApiResult {
        let (tx, rx) = oneshot::channel();
        self.inputs
            .send(Input::Api(request, tx))
            .map_err(|e| Error::unavailable(format!("the node is shutting down: {e}")))?;
        rx.await.unwrap_or_else(|_| Err(Error::unavailable("the node dropped the call")))
    }

    pub async fn status(&self) -> Option<NodeStatus> {
        let (tx, rx) = oneshot::channel();
        self.inputs.send(Input::Status(tx)).ok()?;
        rx.await.ok()
    }
}

/// Start a node: open its data files, listen, serve. Returns once it is
/// listening; the node runs until the process exits.
pub async fn start(config: NodeConfig) -> anyhow::Result<NodeHandle> {
    use axum::extract::State;
    use axum::routing::{get, post};
    use axum::{Json, Router};

    let cluster = tokio::net::TcpListener::bind(config.listen).await?;
    let http = tokio::net::TcpListener::bind(config.http).await?;
    let handle_addrs = (http.local_addr()?, cluster.local_addr()?);
    let (inputs, receiver) = std_mpsc::channel();
    let runtime = tokio::runtime::Handle::current();
    let core_config = config.clone();
    let core = tokio::task::spawn_blocking(move || Core::open(core_config, runtime)).await??;
    std::thread::Builder::new().name("wfe-core".into()).spawn(move || core.run(receiver))?;
    tokio::spawn(accept(cluster, inputs.clone()));

    let handle = NodeHandle { inputs, http: handle_addrs.0, cluster: handle_addrs.1 };
    let app = Router::new()
        .route(
            "/api",
            post(|State(node): State<NodeHandle>, Json(request): Json<ApiRequest>| async move {
                Json(node.call(request).await)
            }),
        )
        .route("/status", get(|State(node): State<NodeHandle>| async move { Json(node.status().await) }))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(handle.clone());
    tokio::spawn(async move {
        if let Err(e) = axum::serve(http, app).await {
            tracing::error!("http server stopped: {e}");
        }
    });
    tracing::info!(cell = config.cell, node = config.node, http = %handle.http, cluster = %handle.cluster, "node started");
    Ok(handle)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_link_reconnects_when_its_peer_closes_even_with_nothing_to_send() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("a loopback port is free");
        let address = listener.local_addr().expect("a bound listener has an address").to_string();
        let (_frames_never_sent, frames) = mpsc::channel(1);
        tokio::spawn(link(address, frames));
        let (first, _) = listener.accept().await.expect("the link dials the address it was given");
        drop(first);
        let reconnected = tokio::time::timeout(Duration::from_secs(5), listener.accept()).await;
        assert!(reconnected.is_ok(), "the link kept a connection its peer had closed");
    }
}
