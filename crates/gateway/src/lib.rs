//! The gateway: a cell's stateless front door.
//!
//! Clients speak [`ClientMessage`] to a gateway; the gateway turns each
//! [`ApiRequest`] into requests on the right shards and assembles the
//! answer:
//!
//! - most calls go to the one shard their workflow id (or deployment name)
//!   hashes to;
//! - listings and cell administration fan out to every shard;
//! - long polls wait outside the log: the gateway probes shard primaries
//!   for "is there something for this poller", and only then submits the
//!   poll that takes the task.
//!
//! Like everything else it is a deterministic actor: packets and ticks in,
//! packets out. It keeps nothing that cannot be lost; a client whose
//! gateway restarts retries.

use std::collections::{BTreeMap, BTreeSet};
use wfe_core::api::*;
use wfe_core::codec::{decode, encode};
use wfe_core::ids::shard_for;
use wfe_core::{Addr, CellId, Error, ErrorCode, Id128, ShardId, Timestamp};
use wfe_engine::{EnginePacket, Op, ProbeQueue};
use wfe_vsr::message::RejectReason;
use wfe_vsr::{Message, Packet, Request};

pub type ClusterPacket = Packet<EnginePacket>;

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub cell: CellId,
    pub gateway: u16,
    pub shard_count: u32,
    pub replica_count: u8,
    /// How long a call may take before the client is told to retry.
    pub request_timeout_ms: u64,
    /// Resend an unanswered shard request, to the next replica, this often.
    pub resend_ms: u64,
    pub probe_ms: u64,
    /// Fan-out calls answer with what they have after this long.
    pub fanout_timeout_ms: u64,
    /// Re-register a polling worker version this often.
    pub register_ms: u64,
    /// Cluster addresses of the cell's nodes, reported by `DescribeCell`.
    pub addresses: Vec<String>,
}

impl GatewayConfig {
    pub fn new(cell: CellId, gateway: u16, shard_count: u32, replica_count: u8) -> GatewayConfig {
        GatewayConfig {
            cell,
            gateway,
            shard_count,
            replica_count,
            request_timeout_ms: 10_000,
            resend_ms: 400,
            probe_ms: 50,
            fanout_timeout_ms: 3_000,
            register_ms: 30_000,
            addresses: Vec::new(),
        }
    }
}

enum Kind {
    Single,
    Fanout { waiting: BTreeSet<ShardId>, answers: BTreeMap<ShardId, ApiResult>, give_up_at: Timestamp },
    Poll { queue: ProbeQueue, wait_until: Timestamp, submitted: bool },
    Result { workflow_id: String, run_id: Option<Id128>, shard: ShardId, wait_until: Timestamp, submitted: bool },
}

struct Call {
    client: Option<(Addr, u64)>,
    request: ApiRequest,
    deadline: Timestamp,
    kind: Kind,
}

struct ShardRequest {
    call: u64,
    shard: ShardId,
    body: Vec<u8>,
    target: u8,
    sent_at: Timestamp,
}

#[derive(Clone, Debug, Default)]
pub struct GatewayStats {
    pub calls: u64,
    pub calls_failed: u64,
    pub shard_requests: u64,
    pub resends: u64,
    pub probes: u64,
    pub open_calls: usize,
}

pub struct Gateway {
    cfg: GatewayConfig,
    me: Addr,
    now: Timestamp,
    request_id_high_bits: u64,
    counter: u64,
    primaries: Vec<u8>,
    calls: BTreeMap<u64, Call>,
    requests: BTreeMap<u128, ShardRequest>,
    probe_at: Timestamp,
    probe_id: u64,
    probe_queues: Vec<Vec<u64>>,
    probe_workflows: Vec<u64>,
    registered: BTreeMap<(DeploymentVersion, String), Timestamp>,
    out_cluster: Vec<(Addr, ClusterPacket)>,
    out_client: Vec<(Addr, ClientMessage)>,
    pub stats: GatewayStats,
}

impl Gateway {
    pub fn new(cfg: GatewayConfig, nonce: u64) -> Gateway {
        Gateway {
            me: Addr::gateway(cfg.cell, cfg.gateway),
            primaries: vec![0; cfg.shard_count as usize],
            cfg,
            now: Timestamp::ZERO,
            request_id_high_bits: nonce,
            counter: 0,
            calls: BTreeMap::new(),
            requests: BTreeMap::new(),
            probe_at: Timestamp::ZERO,
            probe_id: 0,
            probe_queues: Vec::new(),
            probe_workflows: Vec::new(),
            registered: BTreeMap::new(),
            out_cluster: Vec::new(),
            out_client: Vec::new(),
            stats: GatewayStats::default(),
        }
    }

    pub fn addr(&self) -> Addr {
        self.me
    }

    pub fn config(&self) -> &GatewayConfig {
        &self.cfg
    }

    pub fn take_cluster(&mut self) -> Vec<(Addr, ClusterPacket)> {
        std::mem::take(&mut self.out_cluster)
    }

    pub fn take_client(&mut self) -> Vec<(Addr, ClientMessage)> {
        std::mem::take(&mut self.out_client)
    }

    /// The replica each shard's requests currently go to.
    pub fn primaries(&self) -> &[u8] {
        &self.primaries
    }

    fn next_id(&mut self) -> u64 {
        self.counter += 1;
        self.counter
    }

    fn respond(&mut self, call: u64, result: ApiResult) {
        let Some(call) = self.calls.remove(&call) else { return };
        if result.is_err() {
            self.stats.calls_failed += 1;
        }
        if let Some((client, id)) = call.client {
            self.out_client.push((client, ClientMessage::Response { id, result }));
        }
    }

    fn submit(&mut self, call: u64, shard: ShardId, request: ApiRequest) {
        let counter = self.next_id();
        let request_id = (u128::from(self.request_id_high_bits) << 64) | u128::from(counter);
        let body = encode(&Op::Api(request));
        let target = self.primaries[shard as usize];
        self.requests.insert(request_id, ShardRequest { call, shard, body, target, sent_at: self.now });
        self.send(request_id);
    }

    fn send(&mut self, request_id: u128) {
        let request =
            self.requests.get_mut(&request_id).expect("send is only called for a request id that is in self.requests");
        request.sent_at = self.now;
        let to = Addr::replica(self.cfg.cell, request.shard, request.target);
        let message = Message::Request(Request { request_id, client: self.me, body: request.body.clone() });
        self.out_cluster.push((to, Packet::Vsr(message)));
        self.stats.shard_requests += 1;
    }

    fn shard_of(&self, id: &str) -> ShardId {
        shard_for(id, self.cfg.shard_count)
    }

    fn register(&mut self, worker: &WorkerInfo, task_queue: &str) {
        let Some(version) = &worker.version else { return };
        let key = (version.clone(), task_queue.to_string());
        if self.registered.get(&key).is_some_and(|at| self.now < at.add_ms(self.cfg.register_ms)) {
            return;
        }
        self.registered.insert(key, self.now);
        let request = ApiRequest::RegisterWorker { version: version.clone(), task_queue: task_queue.to_string() };
        let call = self.next_id();
        let shard = self.shard_of(&version.deployment);
        self.calls.insert(
            call,
            Call {
                client: None,
                request: request.clone(),
                deadline: self.now.add_ms(self.cfg.request_timeout_ms),
                kind: Kind::Single,
            },
        );
        self.submit(call, shard, request);
    }

    pub fn on_client(&mut self, from: Addr, message: ClientMessage, now: Timestamp) {
        self.now = self.now.max(now);
        let ClientMessage::Request { id, request } = message else { return };
        self.stats.calls += 1;
        let call = self.next_id();
        let timeout = self.cfg.request_timeout_ms;
        let mut entry = Call {
            client: Some((from, id)),
            request: request.clone(),
            deadline: self.now.add_ms(timeout),
            kind: Kind::Single,
        };
        let all: BTreeSet<ShardId> = (0..self.cfg.shard_count).collect();
        match &request {
            ApiRequest::StartWorkflow(r) => self.route(call, entry, &r.workflow_id.clone()),
            ApiRequest::SignalWorkflow { workflow_id, .. }
            | ApiRequest::CancelWorkflow { workflow_id, .. }
            | ApiRequest::TerminateWorkflow { workflow_id, .. }
            | ApiRequest::DescribeWorkflow { workflow_id, .. }
            | ApiRequest::GetHistory { workflow_id, .. }
            | ApiRequest::SetVersioningOverride { workflow_id, .. } => self.route(call, entry, &workflow_id.clone()),
            ApiRequest::CompleteWorkflowTask { token, .. }
            | ApiRequest::FailWorkflowTask { token, .. }
            | ApiRequest::CompleteActivity { token, .. }
            | ApiRequest::FailActivity { token, .. }
            | ApiRequest::HeartbeatActivity { token, .. } => self.route(call, entry, &token.workflow_id.clone()),
            ApiRequest::RegisterWorker { version, .. } | ApiRequest::DeleteVersion { version } => {
                self.route(call, entry, &version.deployment.clone())
            }
            ApiRequest::SetCurrentVersion { deployment, .. }
            | ApiRequest::SetRampingVersion { deployment, .. }
            | ApiRequest::DescribeDeployment { deployment }
            | ApiRequest::DeleteDeployment { deployment } => self.route(call, entry, &deployment.clone()),
            ApiRequest::GetResult { workflow_id, run_id, wait_ms } => {
                let shard = self.shard_of(workflow_id);
                if *wait_ms == 0 {
                    self.calls.insert(call, entry);
                    self.submit(call, shard, request);
                } else {
                    let wait_until = self.now.add_ms(*wait_ms);
                    entry.deadline = wait_until.add_ms(timeout);
                    entry.kind = Kind::Result {
                        workflow_id: workflow_id.clone(),
                        run_id: *run_id,
                        shard,
                        wait_until,
                        submitted: false,
                    };
                    self.calls.insert(call, entry);
                    self.probe_at = Timestamp::ZERO;
                }
            }
            ApiRequest::PollWorkflowTask { task_queue, worker, wait_ms, .. }
            | ApiRequest::PollActivityTask { task_queue, worker, wait_ms, .. } => {
                self.register(worker, task_queue);
                let kind = if matches!(request, ApiRequest::PollWorkflowTask { .. }) {
                    TaskKind::Workflow
                } else {
                    TaskKind::Activity
                };
                let wait_until = self.now.add_ms(*wait_ms);
                entry.deadline = wait_until.add_ms(timeout);
                entry.kind = Kind::Poll {
                    queue: ProbeQueue { task_queue: task_queue.clone(), kind, version: worker.version.clone() },
                    wait_until,
                    submitted: false,
                };
                self.calls.insert(call, entry);
                self.probe_at = Timestamp::ZERO;
            }
            ApiRequest::ListWorkflows { .. }
            | ApiRequest::ListDeployments
            | ApiRequest::DescribeCell
            | ApiRequest::CreateReplication { .. }
            | ApiRequest::DeleteReplication { .. }
            | ApiRequest::PauseReplication { .. }
            | ApiRequest::SetRole { .. } => {
                entry.kind = Kind::Fanout {
                    waiting: all.clone(),
                    answers: BTreeMap::new(),
                    give_up_at: self.now.add_ms(self.cfg.fanout_timeout_ms),
                };
                self.calls.insert(call, entry);
                for shard in all {
                    self.submit(call, shard, request.clone());
                }
            }
        }
    }

    fn route(&mut self, call: u64, entry: Call, id: &str) {
        let shard = self.shard_of(id);
        let request = entry.request.clone();
        self.calls.insert(call, entry);
        self.submit(call, shard, request);
    }

    pub fn on_cluster(&mut self, from: Addr, packet: ClusterPacket, now: Timestamp) {
        self.now = self.now.max(now);
        match packet {
            Packet::Vsr(Message::Reply(reply)) => {
                let Some(request) = self.requests.remove(&reply.request_id) else { return };
                if let Addr::Replica { replica, .. } = from {
                    self.primaries[request.shard as usize] = replica;
                }
                let result: ApiResult =
                    decode(&reply.body).unwrap_or_else(|e| Err(Error::internal(format!("undecodable reply: {e}"))));
                self.on_answer(request.call, request.shard, result);
            }
            Packet::Vsr(Message::Reject { request_id, view, reason }) => {
                let Some(request) = self.requests.get_mut(&request_id) else { return };
                match reason {
                    RejectReason::NotPrimary => {
                        let hinted = u8::try_from(view % u32::from(self.cfg.replica_count))
                            .expect("a remainder of the u8 replica_count fits in a u8");
                        let Addr::Replica { replica, .. } = from else { return };
                        let hint_is_news = hinted != replica && hinted != request.target;
                        if hint_is_news {
                            request.target = hinted;
                            self.primaries[request.shard as usize] = hinted;
                            self.send(request_id);
                        }
                    }
                    RejectReason::Busy => {}
                    RejectReason::TooLarge => {
                        let request = self
                            .requests
                            .remove(&request_id)
                            .expect("the Reject handler found this request id in self.requests above");
                        let error = Error::new(
                            ErrorCode::ResourceExhausted,
                            "the request is too large; store large payloads through the payload codec",
                        );
                        self.on_answer(request.call, request.shard, Err(error));
                    }
                }
            }
            Packet::App(EnginePacket::ProbeResult { id, shard, queues, workflows }) => {
                if let Addr::Replica { replica, .. } = from {
                    self.primaries[shard as usize] = replica;
                }
                if id != self.probe_id {
                    return;
                }
                let mut ready = Vec::new();
                for index in queues {
                    if let Some(calls) = self.probe_queues.get(index as usize) {
                        let one_idle_poller_per_ready_queue = calls.iter().copied().find(|c| {
                            matches!(self.calls.get(c), Some(Call { kind: Kind::Poll { submitted: false, .. }, .. }))
                        });
                        ready.extend(one_idle_poller_per_ready_queue);
                    }
                }
                for index in workflows {
                    ready.extend(self.probe_workflows.get(index as usize).copied());
                }
                for call in ready {
                    let Some(entry) = self.calls.get_mut(&call) else { continue };
                    match &mut entry.kind {
                        Kind::Poll { submitted, .. } if !*submitted => {
                            *submitted = true;
                            let request = entry.request.clone();
                            self.submit(call, shard, request);
                        }
                        Kind::Result { submitted, shard: home, .. } if !*submitted && *home == shard => {
                            *submitted = true;
                            let request = entry.request.clone();
                            self.submit(call, shard, request);
                        }
                        Kind::Single | Kind::Fanout { .. } | Kind::Poll { .. } | Kind::Result { .. } => {}
                    }
                }
            }
            Packet::Vsr(_) | Packet::App(_) => {}
        }
    }

    fn on_answer(&mut self, call: u64, shard: ShardId, result: ApiResult) {
        let Some(entry) = self.calls.get_mut(&call) else { return };
        match &mut entry.kind {
            Kind::Single | Kind::Result { .. } => self.respond(call, result),
            Kind::Poll { submitted, wait_until, .. } => {
                let empty = matches!(result, Ok(ApiResponse::WorkflowTask(None)) | Ok(ApiResponse::ActivityTask(None)));
                let another_poller_took_it = empty && self.now < *wait_until;
                if another_poller_took_it {
                    *submitted = false;
                } else {
                    self.respond(call, result);
                }
            }
            Kind::Fanout { waiting, answers, .. } => {
                waiting.remove(&shard);
                answers.insert(shard, result);
                if waiting.is_empty() {
                    self.finish_fanout(call);
                }
            }
        }
    }

    fn finish_fanout(&mut self, call: u64) {
        let Some(entry) = self.calls.get_mut(&call) else { return };
        let Kind::Fanout { waiting, answers, .. } = &mut entry.kind else { return };
        let answers = std::mem::take(answers);
        let missing: Vec<ShardId> = waiting.iter().copied().collect();
        let request = entry.request.clone();
        self.requests.retain(|_, r| r.call != call);
        let first_error = answers.values().find_map(|a| a.as_ref().err().cloned());
        let result = match request {
            ApiRequest::DescribeCell => Ok(ApiResponse::Cell(Box::new(CellInfo {
                cell: self.cfg.cell,
                shard_count: self.cfg.shard_count,
                replica_count: self.cfg.replica_count,
                addresses: self.cfg.addresses.clone(),
                shards: answers
                    .into_values()
                    .filter_map(|a| match a {
                        Ok(ApiResponse::Shard(info)) => Some(*info),
                        _ => None,
                    })
                    .collect(),
            }))),
            _ if !missing.is_empty() => Err(Error::unavailable(format!(
                "shards {missing:?} did not answer; the call may have partly applied, retry it"
            ))),
            _ if let Some(error) = first_error => Err(error),
            ApiRequest::ListWorkflows { limit, .. } => {
                let mut all: Vec<WorkflowInfo> = answers
                    .into_values()
                    .flat_map(|a| match a {
                        Ok(ApiResponse::Workflows(list)) => list,
                        _ => Vec::new(),
                    })
                    .collect();
                all.sort_by(|a, b| (b.start_time, &a.workflow_id).cmp(&(a.start_time, &b.workflow_id)));
                all.truncate(limit.clamp(1, 1000) as usize);
                Ok(ApiResponse::Workflows(all))
            }
            ApiRequest::ListDeployments => {
                let mut all: Vec<DeploymentInfo> = answers
                    .into_values()
                    .flat_map(|a| match a {
                        Ok(ApiResponse::Deployments(list)) => list,
                        _ => Vec::new(),
                    })
                    .collect();
                all.sort_by(|a, b| a.name.cmp(&b.name));
                Ok(ApiResponse::Deployments(all))
            }
            ApiRequest::CreateReplication { .. }
            | ApiRequest::DeleteReplication { .. }
            | ApiRequest::PauseReplication { .. }
            | ApiRequest::SetRole { .. } => Ok(ApiResponse::Ok),
            ApiRequest::StartWorkflow(_)
            | ApiRequest::SignalWorkflow { .. }
            | ApiRequest::CancelWorkflow { .. }
            | ApiRequest::TerminateWorkflow { .. }
            | ApiRequest::DescribeWorkflow { .. }
            | ApiRequest::GetHistory { .. }
            | ApiRequest::GetResult { .. }
            | ApiRequest::SetVersioningOverride { .. }
            | ApiRequest::PollWorkflowTask { .. }
            | ApiRequest::CompleteWorkflowTask { .. }
            | ApiRequest::FailWorkflowTask { .. }
            | ApiRequest::PollActivityTask { .. }
            | ApiRequest::CompleteActivity { .. }
            | ApiRequest::FailActivity { .. }
            | ApiRequest::HeartbeatActivity { .. }
            | ApiRequest::RegisterWorker { .. }
            | ApiRequest::SetCurrentVersion { .. }
            | ApiRequest::SetRampingVersion { .. }
            | ApiRequest::DescribeDeployment { .. }
            | ApiRequest::DeleteVersion { .. }
            | ApiRequest::DeleteDeployment { .. } => Err(Error::internal(format!(
                "gateway bug: {} reached finish_fanout, but only the requests on_client sends as Kind::Fanout can",
                request.name()
            ))),
        };
        self.respond(call, result);
    }

    fn probe(&mut self) {
        let mut queues: Vec<ProbeQueue> = Vec::new();
        let mut queue_calls: Vec<Vec<u64>> = Vec::new();
        let mut workflows = Vec::new();
        let mut workflow_calls = Vec::new();
        let mut workflow_shards = Vec::new();
        for (id, call) in &self.calls {
            match &call.kind {
                Kind::Poll { queue, submitted: false, .. } => match queues.iter().position(|q| q == queue) {
                    Some(i) => queue_calls[i].push(*id),
                    None => {
                        queues.push(queue.clone());
                        queue_calls.push(vec![*id]);
                    }
                },
                Kind::Result { workflow_id, run_id, shard, submitted: false, .. } => {
                    workflows.push((workflow_id.clone(), *run_id));
                    workflow_calls.push(*id);
                    workflow_shards.push(*shard);
                }
                Kind::Single | Kind::Fanout { .. } | Kind::Poll { .. } | Kind::Result { .. } => {}
            }
        }
        self.probe_id += 1;
        self.probe_queues = queue_calls;
        self.probe_workflows = workflow_calls;
        if queues.is_empty() && workflows.is_empty() {
            return;
        }
        self.stats.probes += 1;
        let no_such_workflow_keeps_indexes_aligned = (String::new(), Some(Id128(1)));
        for shard in 0..self.cfg.shard_count {
            let mine: Vec<(String, Option<Id128>)> = workflows
                .iter()
                .zip(&workflow_shards)
                .map(|(w, s)| if *s == shard { w.clone() } else { no_such_workflow_keeps_indexes_aligned.clone() })
                .collect();
            let relevant = workflow_shards.contains(&shard);
            if queues.is_empty() && !relevant {
                continue;
            }
            let packet = EnginePacket::Probe { id: self.probe_id, queues: queues.clone(), workflows: mine };
            for replica in 0..self.cfg.replica_count {
                self.out_cluster.push((Addr::replica(self.cfg.cell, shard, replica), Packet::App(packet.clone())));
            }
        }
    }

    pub fn tick(&mut self, now: Timestamp) {
        self.now = self.now.max(now);
        let now = self.now;
        self.resend_unanswered_to_next_replica(now);
        self.answer_calls_past_their_deadline(now);
        if now >= self.probe_at {
            self.probe_at = now.add_ms(self.cfg.probe_ms);
            self.probe();
        }
        self.stats.open_calls = self.calls.len();
    }

    fn resend_unanswered_to_next_replica(&mut self, now: Timestamp) {
        let stale: Vec<u128> = self
            .requests
            .iter()
            .filter(|(_, r)| now >= r.sent_at.add_ms(self.cfg.resend_ms))
            .map(|(id, _)| *id)
            .collect();
        for id in stale {
            if !self.calls.contains_key(&self.requests[&id].call) {
                self.requests.remove(&id);
                continue;
            }
            let request = self
                .requests
                .get_mut(&id)
                .expect("stale ids come from self.requests and are only removed by the branch above");
            let believed = self.primaries[request.shard as usize];
            request.target =
                if believed != request.target { believed } else { (request.target + 1) % self.cfg.replica_count };
            self.stats.resends += 1;
            self.send(id);
        }
    }

    fn answer_calls_past_their_deadline(&mut self, now: Timestamp) {
        let mut expired = Vec::new();
        let mut partial = Vec::new();
        let mut finished_waiting = Vec::new();
        for (id, call) in &self.calls {
            match &call.kind {
                Kind::Fanout { give_up_at, .. } if now >= *give_up_at => partial.push(*id),
                Kind::Poll { wait_until, submitted: false, .. } if now >= *wait_until => finished_waiting.push(*id),
                Kind::Result { wait_until, submitted: false, .. } if now >= *wait_until => finished_waiting.push(*id),
                Kind::Single | Kind::Fanout { .. } | Kind::Poll { .. } | Kind::Result { .. }
                    if now >= call.deadline =>
                {
                    expired.push(*id)
                }
                Kind::Single | Kind::Fanout { .. } | Kind::Poll { .. } | Kind::Result { .. } => {}
            }
        }
        for id in partial {
            self.finish_fanout(id);
        }
        for id in finished_waiting {
            let entry = self
                .calls
                .get_mut(&id)
                .expect("finished_waiting ids come from self.calls, and only fan-out calls were answered since");
            match &mut entry.kind {
                Kind::Poll { queue, .. } => {
                    let empty = match queue.kind {
                        TaskKind::Workflow => ApiResponse::WorkflowTask(None),
                        TaskKind::Activity => ApiResponse::ActivityTask(None),
                    };
                    self.respond(id, Ok(empty));
                }
                Kind::Result { submitted, shard, .. } => {
                    *submitted = true;
                    let (shard, ask_where_it_stands_now) = (*shard, entry.request.clone());
                    self.submit(id, shard, ask_where_it_stands_now);
                }
                Kind::Single | Kind::Fanout { .. } => {}
            }
        }
        for id in expired {
            self.requests.retain(|_, r| r.call != id);
            self.respond(id, Err(Error::new(ErrorCode::DeadlineExceeded, "the cell did not answer in time")));
        }
    }
}
