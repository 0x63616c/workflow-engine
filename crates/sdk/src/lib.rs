//! The WFE Rust SDK.
//!
//! ```no_run
//! use wfe_sdk::prelude::*;
//!
//! async fn greet(ctx: WfContext, input: Payload) -> WorkflowResult {
//!     let name: String = input.to_json().map_err(|e| Failure::new(e.to_string()))?;
//!     let greeting = ctx.call("say_hello", Payload::json(&name).unwrap()).await?;
//!     ctx.timer(1_000).await;
//!     Ok(greeting)
//! }
//!
//! # async fn run() -> Result<(), wfe_sdk::Error> {
//! let client = Client::connect("http://127.0.0.1:7400");
//! let mut worker = Worker::new(client.clone(), WorkerOptions::new("greetings", "worker-1"));
//! worker.workflow("greet", greet);
//! worker.activity("say_hello", |_ctx, input: Payload| async move {
//!     let name: String = input.to_json().map_err(|e| Failure::new(e.to_string()))?;
//!     Ok(Payload::json(&format!("hello, {name}")).unwrap())
//! });
//! tokio::spawn(worker.run());
//!
//! let run = client.start("greet", "greet-1", "greetings", Payload::json("wfe").unwrap()).await?;
//! let done = client.result("greet-1", Some(run), 30_000).await?;
//! # Ok(()) }
//! ```
//!
//! A workflow takes one [`Payload`] and returns one: put a struct in it, and
//! add fields to the struct as the workflow grows.

pub mod core;
pub mod workflow;

pub use core::{ActivityJob, WorkerCore, WorkerOptions, WorkerStats};
pub use wfe_codec as codec;
pub use wfe_core::api;
pub use wfe_core::{Error, ErrorCode, Id128, Payload, Timestamp};
pub use workflow::{
    Either, WfContext, WorkflowError, WorkflowFn, WorkflowResult, join, join_all, replay, select, workflow_fn,
};

pub mod prelude {
    pub use crate::workflow::{Either, WfContext, WorkflowError, WorkflowResult, join, join_all, select};
    pub use crate::{ActivityContext, Client, Worker, WorkerOptions};
    pub use wfe_core::Payload;
    pub use wfe_core::api::{ActivityOptions, DeploymentVersion, Failure, RetryPolicy, VersioningBehavior};
}

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use wfe_codec::{Codec, Identity};
use wfe_core::api::*;

type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

/// How requests reach a cell.
pub trait Transport: Send + Sync + 'static {
    fn call(&self, request: ApiRequest) -> BoxFuture<ApiResult>;
}

/// The server's HTTP API: `POST /api` with a JSON [`ApiRequest`].
///
/// Given several node addresses (comma separated), it stays on one until a
/// request cannot reach it, then moves to the next.
pub struct HttpTransport {
    urls: Vec<String>,
    current: Arc<AtomicUsize>,
    http: reqwest::Client,
}

impl HttpTransport {
    pub fn new(base: &str) -> HttpTransport {
        let urls: Vec<String> = base
            .split(',')
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .map(|u| format!("{}/api", u.trim_end_matches('/')))
            .collect();
        HttpTransport { urls, current: Arc::new(AtomicUsize::new(0)), http: reqwest::Client::new() }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Hold {
    WorkerPoll { wait_ms: u64 },
    ResultWait { wait_ms: u64 },
    Immediate,
}

impl Hold {
    fn of(request: &ApiRequest) -> Hold {
        match request {
            ApiRequest::PollWorkflowTask { wait_ms, .. } | ApiRequest::PollActivityTask { wait_ms, .. } => {
                Hold::WorkerPoll { wait_ms: *wait_ms }
            }
            ApiRequest::GetResult { wait_ms, .. } => Hold::ResultWait { wait_ms: *wait_ms },
            ApiRequest::StartWorkflow(_)
            | ApiRequest::SignalWorkflow { .. }
            | ApiRequest::CancelWorkflow { .. }
            | ApiRequest::TerminateWorkflow { .. }
            | ApiRequest::DescribeWorkflow { .. }
            | ApiRequest::GetHistory { .. }
            | ApiRequest::ListWorkflows { .. }
            | ApiRequest::SetVersioningOverride { .. }
            | ApiRequest::CompleteWorkflowTask { .. }
            | ApiRequest::FailWorkflowTask { .. }
            | ApiRequest::CompleteActivity { .. }
            | ApiRequest::FailActivity { .. }
            | ApiRequest::HeartbeatActivity { .. }
            | ApiRequest::RegisterWorker { .. }
            | ApiRequest::SetCurrentVersion { .. }
            | ApiRequest::SetRampingVersion { .. }
            | ApiRequest::DescribeDeployment { .. }
            | ApiRequest::ListDeployments
            | ApiRequest::DeleteVersion { .. }
            | ApiRequest::DeleteDeployment { .. }
            | ApiRequest::DescribeCell
            | ApiRequest::CreateReplication { .. }
            | ApiRequest::DeleteReplication { .. }
            | ApiRequest::PauseReplication { .. }
            | ApiRequest::SetRole { .. } => Hold::Immediate,
        }
    }

    fn wait_ms(self) -> u64 {
        match self {
            Hold::WorkerPoll { wait_ms } | Hold::ResultWait { wait_ms } => wait_ms,
            Hold::Immediate => 0,
        }
    }
}

impl Transport for HttpTransport {
    fn call(&self, request: ApiRequest) -> BoxFuture<ApiResult> {
        let wait_ms = Hold::of(&request).wait_ms();
        if self.urls.is_empty() {
            return Box::pin(async { Err(Error::invalid("no server address")) });
        }
        let index = self.current.load(Ordering::Relaxed);
        let url = &self.urls[index % self.urls.len()];
        let send = self.http.post(url).timeout(Duration::from_millis(wait_ms + 30_000)).json(&request).send();
        let current = self.current.clone();
        Box::pin(async move {
            let response = match send.await {
                Ok(response) => response,
                Err(e) => {
                    move_to_next_node(&current, index);
                    return Err(Error::unavailable(e.to_string()));
                }
            };
            response.json::<ApiResult>().await.map_err(|e| Error::unavailable(format!("unreadable response: {e}")))?
        })
    }
}

fn move_to_next_node(current: &AtomicUsize, failed: usize) {
    if current.compare_exchange(failed, failed + 1, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        tracing::debug!(failed, "another request already moved off this node");
    }
}

fn now() -> Timestamp {
    Timestamp::from_micros(
        SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| {
            u64::try_from(d.as_micros()).expect("microseconds since 1970 fit in u64 until the year 586524")
        }),
    )
}

macro_rules! expect_response {
    ($response:expr, $shape:pat => $value:expr) => {
        match $response {
            $shape => Ok($value),
            other => unexpected(other),
        }
    };
}

fn unexpected<T>(response: ApiResponse) -> Result<T, Error> {
    Err(Error::internal(format!(
        "the server answered {response:?}, which is not an answer to this request; \
         check that client and server run the same API version"
    )))
}

/// A connection to a cell.
#[derive(Clone)]
pub struct Client {
    transport: Arc<dyn Transport>,
    codec: Arc<dyn Codec>,
    nonce: u64,
    counter: Arc<AtomicU64>,
}

impl Client {
    /// To a server's HTTP address, such as `http://127.0.0.1:7400`, or
    /// several of a cell's nodes separated by commas.
    pub fn connect(url: &str) -> Client {
        Client::with_transport(Arc::new(HttpTransport::new(url)))
    }

    pub fn with_transport(transport: Arc<dyn Transport>) -> Client {
        let time = now().micros();
        let nonce = splitmix_nonce(time, u64::from(std::process::id()));
        Client { transport, codec: Arc::new(Identity), nonce, counter: Arc::new(AtomicU64::new(0)) }
    }

    /// Run payloads through `codec` (compression, claim check) on the way
    /// in and out. Workers built from this client use it too.
    pub fn with_codec(mut self, codec: impl Codec + 'static) -> Client {
        self.codec = Arc::new(codec);
        self
    }

    pub fn codec(&self) -> Arc<dyn Codec> {
        self.codec.clone()
    }

    fn request_id(&self) -> Id128 {
        Id128((u128::from(self.nonce) << 64) | (u128::from(self.counter.fetch_add(1, Ordering::Relaxed)) + 1))
    }

    /// Any API call. Retries while the cell is unavailable.
    pub async fn call(&self, request: ApiRequest) -> ApiResult {
        let mut delay = 100;
        for _ in 0..6 {
            match self.transport.call(request.clone()).await {
                Err(e) if e.code.is_retryable() => {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    delay = (delay * 2).min(2_000);
                }
                other => return other,
            }
        }
        self.transport.call(request).await
    }

    /// One attempt, no retries. For worker polls: the worker core polls again on its own.
    pub async fn call_once(&self, request: ApiRequest) -> ApiResult {
        self.transport.call(request).await
    }

    async fn encode(&self, payload: Payload) -> Result<Payload, Error> {
        let codec = self.codec.clone();
        tokio::task::spawn_blocking(move || codec.encode(payload))
            .await
            .map_err(|e| Error::internal(e.to_string()))?
            .map_err(|e| Error::invalid(e.to_string()))
    }

    async fn decode(&self, payload: Payload) -> Result<Payload, Error> {
        let codec = self.codec.clone();
        tokio::task::spawn_blocking(move || codec.decode(payload))
            .await
            .map_err(|e| Error::internal(e.to_string()))?
            .map_err(|e| Error::invalid(e.to_string()))
    }

    /// Start a workflow; returns its run id.
    pub async fn start(
        &self,
        workflow_type: &str,
        workflow_id: &str,
        task_queue: &str,
        input: Payload,
    ) -> Result<Id128, Error> {
        self.start_with(StartWorkflow {
            workflow_id: workflow_id.to_string(),
            workflow_type: workflow_type.to_string(),
            task_queue: task_queue.to_string(),
            input,
            ..StartWorkflow::default()
        })
        .await
    }

    pub async fn start_with(&self, mut request: StartWorkflow) -> Result<Id128, Error> {
        request.input = self.encode(request.input).await?;
        if request.request_id.is_zero() {
            request.request_id = self.request_id();
        }
        expect_response!(self.call(ApiRequest::StartWorkflow(request)).await?, ApiResponse::Started { run_id, .. } => run_id)
    }

    pub async fn signal(&self, workflow_id: &str, name: &str, input: Payload) -> Result<(), Error> {
        let request = ApiRequest::SignalWorkflow {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            name: name.to_string(),
            input: self.encode(input).await?,
            request_id: self.request_id(),
        };
        self.call(request).await.map(|_| ())
    }

    pub async fn cancel(&self, workflow_id: &str, reason: &str) -> Result<(), Error> {
        let request = ApiRequest::CancelWorkflow {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            reason: reason.to_string(),
        };
        self.call(request).await.map(|_| ())
    }

    pub async fn terminate(&self, workflow_id: &str, reason: &str) -> Result<(), Error> {
        let request = ApiRequest::TerminateWorkflow {
            workflow_id: workflow_id.to_string(),
            run_id: None,
            reason: reason.to_string(),
        };
        self.call(request).await.map(|_| ())
    }

    async fn decode_description(&self, mut d: WorkflowDescription) -> Result<WorkflowDescription, Error> {
        d.input = self.decode(d.input).await?;
        if let Some(result) = d.result.take() {
            d.result = Some(self.decode(result).await?);
        }
        Ok(d)
    }

    pub async fn describe(&self, workflow_id: &str, run_id: Option<Id128>) -> Result<WorkflowDescription, Error> {
        let request = ApiRequest::DescribeWorkflow { workflow_id: workflow_id.to_string(), run_id };
        let d = expect_response!(self.call(request).await?, ApiResponse::Workflow(d) => *d)?;
        self.decode_description(d).await
    }

    /// Wait up to `wait_ms` for the workflow to close, following
    /// continue-as-new. The description says how it stands.
    pub async fn result(
        &self,
        workflow_id: &str,
        run_id: Option<Id128>,
        wait_ms: u64,
    ) -> Result<WorkflowDescription, Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        let mut run_id = run_id;
        loop {
            let left = deadline.saturating_duration_since(tokio::time::Instant::now()).as_millis();
            let left = u64::try_from(left).unwrap_or(u64::MAX);
            let request =
                ApiRequest::GetResult { workflow_id: workflow_id.to_string(), run_id, wait_ms: left.min(20_000) };
            let d = expect_response!(self.call(request).await?, ApiResponse::Workflow(d) => *d)?;
            if let (WorkflowStatus::ContinuedAsNew, Some(next)) = (d.info.status, d.continued_as) {
                run_id = Some(next);
                continue;
            }
            if d.info.status.is_closed() || left == 0 {
                return self.decode_description(d).await;
            }
        }
    }

    pub async fn history(&self, workflow_id: &str, run_id: Option<Id128>) -> Result<Vec<Event>, Error> {
        let mut all = Vec::new();
        let mut after = 0;
        loop {
            let request = ApiRequest::GetHistory {
                workflow_id: workflow_id.to_string(),
                run_id,
                after_event_id: after,
                limit: 1000,
            };
            let (events, next_event_id) = expect_response!(
                self.call(request).await?,
                ApiResponse::History { events, next_event_id } => (events, next_event_id)
            )?;
            all.extend(events);
            if next_event_id == 0 {
                return Ok(all);
            }
            after = next_event_id;
        }
    }

    pub async fn list(&self, filter: ListFilter, limit: u32) -> Result<Vec<WorkflowInfo>, Error> {
        expect_response!(self.call(ApiRequest::ListWorkflows { filter, limit }).await?, ApiResponse::Workflows(list) => list)
    }

    pub async fn set_versioning_override(
        &self,
        workflow_id: &str,
        value: Option<VersioningOverride>,
    ) -> Result<(), Error> {
        let request = ApiRequest::SetVersioningOverride { workflow_id: workflow_id.to_string(), run_id: None, value };
        self.call(request).await.map(|_| ())
    }

    /// Route new workflows (and auto-upgrading ones) to this build.
    pub async fn set_current_version(&self, deployment: &str, build_id: Option<&str>) -> Result<(), Error> {
        let request =
            ApiRequest::SetCurrentVersion { deployment: deployment.to_string(), build_id: build_id.map(String::from) };
        self.call(request).await.map(|_| ())
    }

    /// Send `percent` of new workflows to this build.
    pub async fn set_ramping_version(
        &self,
        deployment: &str,
        build_id: Option<&str>,
        percent: u8,
    ) -> Result<(), Error> {
        let request = ApiRequest::SetRampingVersion {
            deployment: deployment.to_string(),
            build_id: build_id.map(String::from),
            percent,
        };
        self.call(request).await.map(|_| ())
    }

    pub async fn describe_deployment(&self, deployment: &str) -> Result<DeploymentInfo, Error> {
        let request = ApiRequest::DescribeDeployment { deployment: deployment.to_string() };
        expect_response!(self.call(request).await?, ApiResponse::Deployment(d) => *d)
    }

    /// Wait until every shard routes by the deployment's latest routing
    /// change (they learn of it asynchronously), or `wait_ms` passes.
    /// Workflows started before that may still go to the previous version.
    pub async fn routing_propagated(&self, deployment: &str, wait_ms: u64) -> Result<DeploymentInfo, Error> {
        let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
        loop {
            let info = self.describe_deployment(deployment).await?;
            if info.propagated_shards >= info.shard_count || tokio::time::Instant::now() >= deadline {
                return Ok(info);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn list_deployments(&self) -> Result<Vec<DeploymentInfo>, Error> {
        expect_response!(self.call(ApiRequest::ListDeployments).await?, ApiResponse::Deployments(list) => list)
    }

    pub async fn describe_cell(&self) -> Result<CellInfo, Error> {
        expect_response!(self.call(ApiRequest::DescribeCell).await?, ApiResponse::Cell(cell) => *cell)
    }
}

impl Client {
    pub async fn set_role(&self, active: bool, epoch: u64) -> Result<(), Error> {
        self.call(ApiRequest::SetRole { active, epoch }).await.map(|_| ())
    }

    /// Start (or resume) shipping this cell's workflows to `target`.
    pub async fn create_replication(&self, target: CellRef) -> Result<(), Error> {
        self.call(ApiRequest::CreateReplication { target }).await.map(|_| ())
    }

    pub async fn delete_replication(&self, cell: wfe_core::CellId) -> Result<(), Error> {
        self.call(ApiRequest::DeleteReplication { cell }).await.map(|_| ())
    }

    pub async fn pause_replication(&self, cell: wfe_core::CellId, paused: bool) -> Result<(), Error> {
        self.call(ApiRequest::PauseReplication { cell, paused }).await.map(|_| ())
    }

    pub async fn delete_version(&self, deployment: &str, build_id: &str) -> Result<(), Error> {
        let version = DeploymentVersion::new(deployment, build_id);
        self.call(ApiRequest::DeleteVersion { version }).await.map(|_| ())
    }

    pub async fn delete_deployment(&self, deployment: &str) -> Result<(), Error> {
        self.call(ApiRequest::DeleteDeployment { deployment: deployment.to_string() }).await.map(|_| ())
    }
}

/// How a cell stands, summed over its shards.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CellSummary {
    pub cell: wfe_core::CellId,
    pub shard_count: u32,
    pub replica_count: u8,
    /// Cluster addresses of the cell's nodes.
    #[serde(default)]
    pub addresses: Vec<String>,
    /// Shards that answered.
    pub shards_up: u32,
    /// `active`, `standby`, or `mixed` while a role change is in progress.
    pub role: String,
    pub epoch: u64,
    pub executions: u64,
    pub open_workflows: u64,
    pub replicated_in: u64,
    /// Per target cell: `(executions still to ship, paused)`.
    pub replication: std::collections::BTreeMap<wfe_core::CellId, (u64, bool)>,
}

impl CellSummary {
    pub fn of(info: &CellInfo) -> CellSummary {
        let active = info.shards.iter().filter(|s| s.active).count();
        let role = match active {
            0 if !info.shards.is_empty() => "standby",
            n if n == info.shards.len() && n > 0 => "active",
            0 => "unknown",
            _ => "mixed",
        };
        let mut replication = std::collections::BTreeMap::new();
        for link in info.shards.iter().flat_map(|s| &s.replication) {
            let entry = replication.entry(link.target.cell).or_insert((0, false));
            entry.0 += link.pending;
            entry.1 |= link.paused;
        }
        CellSummary {
            cell: info.cell,
            shard_count: info.shard_count,
            replica_count: info.replica_count,
            addresses: info.addresses.clone(),
            shards_up: u32::try_from(info.shards.len()).expect("a cell reports at most shard_count (a u32) shards"),
            role: role.to_string(),
            epoch: info.shards.iter().map(|s| s.epoch).max().unwrap_or(0),
            executions: info.shards.iter().map(|s| s.executions).sum(),
            open_workflows: info.shards.iter().map(|s| s.open_workflows).sum(),
            replicated_in: info.shards.iter().map(|s| s.replicated_in).sum(),
            replication,
        }
    }

    pub fn cell_ref(&self) -> CellRef {
        CellRef {
            cell: self.cell,
            shards: self.shard_count,
            replicas: self.replica_count,
            addresses: self.addresses.clone(),
        }
    }
}

/// Move the active role from one cell to another.
///
/// The orderly way: demote `from`, wait until it has shipped everything to
/// `to`, promote `to`, and turn replication around so `from` follows.
/// With `force` (the old cell is gone), `to` is promoted without waiting;
/// workflows `from` had not shipped yet are missing until it comes back.
pub async fn failover(from: &Client, to: &Client, force: bool, wait_ms: u64) -> Result<(), Error> {
    use wfe_control::{CellView, Other, RoleStep, plan_roles};
    let deadline = tokio::time::Instant::now() + Duration::from_millis(wait_ms);
    loop {
        let to_info = to.describe_cell().await?;
        let from_info = match from.describe_cell().await {
            Ok(info) => Some(info),
            Err(_) if force => None,
            Err(e) => return Err(e),
        };
        let want = CellView::of(&to_info);
        let other_view = from_info.as_ref().map(CellView::of);
        let other = other_view.as_ref().map_or(Other::Unreachable, Other::Seen);
        let client_of = |cell: wfe_core::CellId| if cell == want.cell { to } else { from };
        let ref_of = |cell: wfe_core::CellId| match &from_info {
            Some(info) if cell != want.cell => CellSummary::of(info).cell_ref(),
            Some(_) | None => CellSummary::of(&to_info).cell_ref(),
        };
        match plan_roles(&want, other, force) {
            RoleStep::Done => return Ok(()),
            RoleStep::Wait(_) if force && want.is_active() => return Ok(()),
            RoleStep::Wait(why) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(Error::deadline(format!(
                        "the failover to cell {} did not finish in {wait_ms} ms: {why}; run it again to continue, or force",
                        want.cell
                    )));
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            RoleStep::Blocked(why) => return Err(Error::precondition(why)),
            RoleStep::Demote { cell, epoch } => client_of(cell).set_role(false, epoch).await?,
            RoleStep::Promote { cell, epoch } => client_of(cell).set_role(true, epoch).await?,
            RoleStep::CreateReplication { on, target } => client_of(on).create_replication(ref_of(target)).await?,
            RoleStep::DeleteReplication { on, target } => client_of(on).delete_replication(target).await?,
        }
    }
}

fn splitmix_nonce(a: u64, b: u64) -> u64 {
    let mut x = a ^ b.rotate_left(32) ^ 0x9e37_79b9_7f4a_7c15;
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// What an activity knows about its invocation.
#[derive(Clone)]
pub struct ActivityContext {
    pub task: Arc<ActivityTask>,
    job: u64,
    events: mpsc::UnboundedSender<WorkerEvent>,
}

impl ActivityContext {
    pub fn attempt(&self) -> u32 {
        self.task.token.attempt
    }

    /// Progress recorded by an earlier attempt.
    pub fn heartbeat_details(&self) -> Option<&Payload> {
        self.task.heartbeat_details.as_ref()
    }

    /// Tell the cell this activity is alive, optionally with progress the
    /// next attempt can resume from.
    pub fn heartbeat(&self, details: Option<Payload>) {
        deliver(&self.events, WorkerEvent::Heartbeat(self.job, details));
    }
}

type ActivityFn = Arc<dyn Fn(ActivityContext, Payload) -> BoxFuture<Result<Payload, Failure>> + Send + Sync>;

fn deliver(events: &mpsc::UnboundedSender<WorkerEvent>, event: WorkerEvent) {
    if events.send(event).is_err() {
        tracing::debug!("worker loop has stopped; dropping an event");
    }
}

enum WorkerEvent {
    Response(u64, ApiResult),
    ActivityDone(u64, Result<Payload, Failure>),
    Heartbeat(u64, Option<Payload>),
    Stop,
}

/// Stops a running [`Worker`].
#[derive(Clone)]
pub struct WorkerHandle {
    events: mpsc::UnboundedSender<WorkerEvent>,
}

impl WorkerHandle {
    /// Stop polling; the worker's `run` returns once work in hand is done.
    pub fn stop(&self) {
        deliver(&self.events, WorkerEvent::Stop);
    }
}

/// Polls a task queue and runs workflows and activities.
pub struct Worker {
    client: Client,
    core: WorkerCore,
    activities: BTreeMap<String, ActivityFn>,
    events: mpsc::UnboundedSender<WorkerEvent>,
    inbox: mpsc::UnboundedReceiver<WorkerEvent>,
}

impl Worker {
    pub fn new(client: Client, mut options: WorkerOptions) -> Worker {
        let (events, inbox) = mpsc::unbounded_channel();
        let activity_polls = std::mem::take(&mut options.activity_polls);
        let mut core = WorkerCore::new(
            options,
            client.codec(),
            client.nonce ^ client.counter.fetch_add(1 << 32, Ordering::Relaxed),
        );
        core.set_activity_polls(0, activity_polls);
        Worker { client, core, activities: BTreeMap::new(), events, inbox }
    }

    pub fn workflow<F, Fut>(&mut self, workflow_type: &str, f: F) -> &mut Worker
    where
        F: Fn(WfContext, Payload) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = WorkflowResult> + 'static,
    {
        self.core.register_workflow(workflow_type, workflow_fn(f));
        self
    }

    pub fn activity<F, Fut>(&mut self, activity_type: &str, f: F) -> &mut Worker
    where
        F: Fn(ActivityContext, Payload) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Payload, Failure>> + Send + 'static,
    {
        self.activities.insert(activity_type.to_string(), Arc::new(move |ctx, input| Box::pin(f(ctx, input))));
        self.core.enable_activities();
        self
    }

    pub fn handle(&self) -> WorkerHandle {
        WorkerHandle { events: self.events.clone() }
    }

    pub fn stats(&self) -> &WorkerStats {
        &self.core.stats
    }

    /// Run until stopped.
    pub async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut stopping = false;
        loop {
            tokio::select! {
                _ = interval.tick() => {}
                event = self.inbox.recv() => match event {
                    Some(WorkerEvent::Response(id, result)) => self.core.on_response(id, result, now()),
                    Some(WorkerEvent::ActivityDone(job, result)) => self.core.activity_done(job, result, now()),
                    Some(WorkerEvent::Heartbeat(job, details)) => self.core.heartbeat(job, details, now()),
                    Some(WorkerEvent::Stop) | None => {
                        stopping = true;
                        self.core.stop();
                    }
                },
            }
            self.core.tick(now());
            for (id, request) in self.core.take_requests() {
                let (client, events) = (self.client.clone(), self.events.clone());
                tokio::spawn(async move {
                    let result = match Hold::of(&request) {
                        Hold::WorkerPoll { .. } => client.call_once(request).await,
                        Hold::ResultWait { .. } | Hold::Immediate => client.call(request).await,
                    };
                    deliver(&events, WorkerEvent::Response(id, result));
                });
            }
            for job in self.core.take_activities() {
                let events = self.events.clone();
                let Some(f) = self.activities.get(&job.task.activity_type).cloned() else {
                    let failure = Failure::new(format!(
                        "activity type {} is not registered on this worker",
                        job.task.activity_type
                    ))
                    .kind("not_registered");
                    deliver(&events, WorkerEvent::ActivityDone(job.id, Err(failure)));
                    continue;
                };
                let mut task = job.task;
                let input = std::mem::take(&mut task.input);
                let ctx = ActivityContext { task: Arc::new(task), job: job.id, events: events.clone() };
                tokio::spawn(async move {
                    let result = match tokio::spawn(f(ctx, input)).await {
                        Ok(result) => result,
                        Err(panicked) => Err(Failure::new(format!("activity panicked: {panicked}")).kind("panic")),
                    };
                    deliver(&events, WorkerEvent::ActivityDone(job.id, result));
                });
            }
            if stopping && self.core.is_idle() {
                return;
            }
        }
    }
}
