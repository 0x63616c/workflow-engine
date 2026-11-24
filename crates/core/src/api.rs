//! The WFE API: one request enum, one response enum.
//!
//! Every way into a cell (SDK, CLI, UI, operator) sends an [`ApiRequest`] to
//! a gateway and gets a `Result<ApiResponse, Error>` back. The same types are
//! MessagePack on the cluster wire and JSON over HTTP.
//!
//! Workflows, activities and signals each carry exactly one [`Payload`] in
//! and one out. New fields are added with `#[serde(default)]`.

use crate::{CellId, Error, Id128, Payload, ShardId, Timestamp};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// One build of the workers of one deployment.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DeploymentVersion {
    pub deployment: String,
    pub build_id: String,
}

impl DeploymentVersion {
    pub fn new(deployment: impl Into<String>, build_id: impl Into<String>) -> DeploymentVersion {
        DeploymentVersion { deployment: deployment.into(), build_id: build_id.into() }
    }
}

impl std::fmt::Display for DeploymentVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.deployment, self.build_id)
    }
}

/// How a workflow moves between versions; declared by the worker each time
/// it completes a workflow task.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningBehavior {
    /// The worker is unversioned.
    #[default]
    Unspecified,
    /// Stay on the version that ran the first workflow task.
    Pinned,
    /// Follow the deployment's current (or ramping) version.
    AutoUpgrade,
}

/// An operator's override of a workflow's own versioning behavior.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningOverride {
    Pinned(DeploymentVersion),
    AutoUpgrade,
}

/// What a worker says about itself when it polls.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerInfo {
    pub identity: String,
    /// `None` for an unversioned worker.
    #[serde(default)]
    pub version: Option<DeploymentVersion>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionStatus {
    /// Registered by a poller, never routed to.
    #[default]
    Inactive,
    Current,
    Ramping,
    /// No longer receives new workflows; pinned workflows are still running.
    Draining,
    /// No open workflow is pinned to it: its workers can be shut down.
    Drained,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionInfo {
    pub build_id: String,
    pub status: VersionStatus,
    pub task_queues: Vec<String>,
    pub created_at: Timestamp,
    #[serde(default)]
    pub status_changed_at: Timestamp,
    /// Open workflows pinned to this version, as of `drainage_checked_at`.
    #[serde(default)]
    pub pinned_open: u64,
    #[serde(default)]
    pub drainage_checked_at: Timestamp,
    #[serde(default)]
    pub last_poll_at: Timestamp,
}

/// Where new and auto-upgrading workflows of a deployment go.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingConfig {
    /// Bumped on every change; shards apply the highest they have seen.
    pub revision: u64,
    /// `None`: unversioned workers.
    pub current: Option<String>,
    pub current_changed_at: Timestamp,
    pub ramping: Option<String>,
    /// Percent of workflows (by workflow id) sent to `ramping`.
    pub ramp_percent: u8,
    pub ramping_changed_at: Timestamp,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentInfo {
    pub name: String,
    pub created_at: Timestamp,
    pub routing: RoutingConfig,
    pub versions: Vec<VersionInfo>,
    /// Shards that have applied `routing.revision`.
    pub propagated_shards: u32,
    pub shard_count: u32,
}

/// Serialized PascalCase (stored in histories and on the wire); shown and
/// parsed snake_case (`as_str`, `parse`) in the CLI, UI and error messages.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    strum::IntoStaticStr,
    strum::EnumString,
    strum::VariantArray,
)]
#[strum(serialize_all = "snake_case")]
pub enum WorkflowStatus {
    #[default]
    Running,
    Completed,
    Failed,
    Canceled,
    Terminated,
    TimedOut,
    ContinuedAsNew,
}

impl WorkflowStatus {
    pub fn is_closed(self) -> bool {
        self != WorkflowStatus::Running
    }
    pub fn as_str(self) -> &'static str {
        self.into()
    }
    pub fn parse(s: &str) -> Option<WorkflowStatus> {
        s.parse().ok()
    }
}

/// Why something failed. Carried in histories, so it only grows.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Failure {
    pub message: String,
    /// Application-defined category (`"timeout"`, `"canceled"`, ...).
    #[serde(default)]
    pub kind: String,
    #[serde(default)]
    pub non_retryable: bool,
    #[serde(default)]
    pub details: Option<Payload>,
}

impl Failure {
    pub fn new(message: impl Into<String>) -> Failure {
        Failure { message: message.into(), ..Failure::default() }
    }
    pub fn kind(mut self, kind: impl Into<String>) -> Failure {
        self.kind = kind.into();
        self
    }
    pub fn non_retryable(mut self) -> Failure {
        self.non_retryable = true;
        self
    }
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.kind.is_empty() { write!(f, "{}", self.message) } else { write!(f, "{}: {}", self.kind, self.message) }
    }
}

/// Activity retry policy. Integers only: it lives in replicated state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub initial_interval_ms: u64,
    /// Multiplier per attempt, in percent (200 doubles).
    pub backoff_percent: u32,
    pub maximum_interval_ms: u64,
    /// Zero means unlimited.
    pub maximum_attempts: u32,
}

impl Default for RetryPolicy {
    fn default() -> RetryPolicy {
        RetryPolicy {
            initial_interval_ms: 1_000,
            backoff_percent: 200,
            maximum_interval_ms: 100_000,
            maximum_attempts: 0,
        }
    }
}

impl RetryPolicy {
    /// Delay before attempt `attempt + 1`, after `attempt` attempts failed.
    pub fn delay_ms(&self, attempt: u32) -> u64 {
        let mut delay = self.initial_interval_ms.max(1);
        for _ in 1..attempt.min(64) {
            delay = delay.saturating_mul(u64::from(self.backoff_percent.max(100))) / 100;
            if delay >= self.maximum_interval_ms {
                break;
            }
        }
        delay.min(self.maximum_interval_ms.max(self.initial_interval_ms))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeoutKind {
    ScheduleToClose,
    StartToClose,
    Heartbeat,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityOptions {
    pub activity_type: String,
    /// Defaults to the workflow's task queue.
    #[serde(default)]
    pub task_queue: Option<String>,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    /// Total time allowed across retries; zero means no limit.
    #[serde(default)]
    pub schedule_to_close_ms: u64,
    /// Time allowed per attempt; zero means the server default.
    #[serde(default)]
    pub start_to_close_ms: u64,
    /// Zero disables heartbeat timeouts.
    #[serde(default)]
    pub heartbeat_ms: u64,
}

/// What a workflow decides to do, in the order it decided.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Command {
    ScheduleActivity {
        options: ActivityOptions,
        input: Payload,
    },
    StartTimer {
        fire_after_ms: u64,
    },
    /// `started_id` is the event id of the timer's `TimerStarted`.
    CancelTimer {
        started_id: u64,
    },
    RecordMarker {
        name: String,
        details: Payload,
    },
    CompleteWorkflow {
        result: Payload,
    },
    FailWorkflow {
        failure: Failure,
    },
    CancelWorkflow,
    ContinueAsNew {
        input: Payload,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// Position in the history, from 1.
    pub id: u64,
    pub time: Timestamp,
    pub attrs: EventAttrs,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
pub enum EventAttrs {
    WorkflowStarted {
        workflow_type: String,
        task_queue: String,
        input: Payload,
        #[serde(default)]
        execution_timeout_ms: u64,
        #[serde(default)]
        continued_from: Option<Id128>,
    },
    WorkflowTaskScheduled {
        attempt: u32,
    },
    WorkflowTaskStarted {
        scheduled_id: u64,
        identity: String,
        version: Option<DeploymentVersion>,
    },
    WorkflowTaskCompleted {
        scheduled_id: u64,
        started_id: u64,
        version: Option<DeploymentVersion>,
        behavior: VersioningBehavior,
    },
    WorkflowTaskFailed {
        scheduled_id: u64,
        started_id: u64,
        failure: Failure,
    },
    WorkflowTaskTimedOut {
        scheduled_id: u64,
        started_id: u64,
    },
    ActivityScheduled {
        options: ActivityOptions,
        input: Payload,
    },
    /// Written together with the activity's closing event.
    ActivityStarted {
        scheduled_id: u64,
        attempt: u32,
        identity: String,
    },
    ActivityCompleted {
        scheduled_id: u64,
        result: Payload,
    },
    ActivityFailed {
        scheduled_id: u64,
        failure: Failure,
    },
    ActivityTimedOut {
        scheduled_id: u64,
        kind: TimeoutKind,
    },
    TimerStarted {
        fire_after_ms: u64,
    },
    TimerFired {
        started_id: u64,
    },
    TimerCanceled {
        started_id: u64,
    },
    MarkerRecorded {
        name: String,
        details: Payload,
    },
    SignalReceived {
        name: String,
        input: Payload,
    },
    CancelRequested {
        reason: String,
    },
    VersioningOverrideChanged {
        value: Option<VersioningOverride>,
    },
    WorkflowCompleted {
        result: Payload,
    },
    WorkflowFailed {
        failure: Failure,
    },
    WorkflowCanceled,
    WorkflowTerminated {
        reason: String,
    },
    WorkflowTimedOut,
    WorkflowContinuedAsNew {
        new_run_id: Id128,
        input: Payload,
    },
}

impl EventAttrs {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowVersioning {
    pub behavior: VersioningBehavior,
    /// The version that completed the latest workflow task.
    pub version: Option<DeploymentVersion>,
    #[serde(default, rename = "override")]
    pub override_: Option<VersioningOverride>,
}

impl WorkflowVersioning {
    /// The version this workflow may only run on, if any.
    pub fn pinned(&self) -> Option<&DeploymentVersion> {
        match &self.override_ {
            Some(VersioningOverride::Pinned(v)) => Some(v),
            Some(VersioningOverride::AutoUpgrade) => None,
            None if self.behavior == VersioningBehavior::Pinned => self.version.as_ref(),
            None => None,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowInfo {
    pub workflow_id: String,
    pub run_id: Id128,
    pub workflow_type: String,
    pub task_queue: String,
    pub status: WorkflowStatus,
    pub start_time: Timestamp,
    #[serde(default)]
    pub close_time: Timestamp,
    pub history_length: u64,
    #[serde(default)]
    pub versioning: WorkflowVersioning,
    #[serde(default)]
    pub shard: ShardId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PendingActivity {
    pub scheduled_id: u64,
    pub activity_type: String,
    pub task_queue: String,
    pub attempt: u32,
    /// `scheduled`, `started` or `backoff`.
    pub state: String,
    #[serde(default)]
    pub last_failure: Option<Failure>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowDescription {
    pub info: WorkflowInfo,
    pub input: Payload,
    #[serde(default)]
    pub result: Option<Payload>,
    #[serde(default)]
    pub failure: Option<Failure>,
    #[serde(default)]
    pub pending_activities: Vec<PendingActivity>,
    #[serde(default)]
    pub pending_timers: u64,
    /// `none`, `scheduled` or `started`.
    #[serde(default)]
    pub workflow_task: String,
    #[serde(default)]
    pub cancel_requested: bool,
    /// The run this one continued as, if it did.
    #[serde(default)]
    pub continued_as: Option<Id128>,
}

/// What to do when the workflow id already has a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdConflictPolicy {
    /// Fail if a run is open; start a new run if the last one closed.
    #[default]
    Fail,
    /// Return the open run instead of failing.
    UseExisting,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartWorkflow {
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input: Payload,
    /// Makes a retried start return the same run.
    #[serde(default)]
    pub request_id: Id128,
    #[serde(default)]
    pub execution_timeout_ms: u64,
    #[serde(default)]
    pub id_conflict: IdConflictPolicy,
    #[serde(default)]
    pub versioning_override: Option<VersioningOverride>,
}

/// Opaque to workers: names the task being worked on.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskToken {
    pub workflow_id: String,
    pub run_id: Id128,
    pub scheduled_id: u64,
    pub attempt: u32,
    /// The `WorkflowTaskStarted` event id; zero for activities.
    #[serde(default)]
    pub started_id: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskKind {
    Workflow,
    Activity,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkflowTask {
    pub token: TaskToken,
    pub workflow_type: String,
    /// The history from event 1, or its first page (see `next_event_id`).
    pub history: Vec<Event>,
    /// Non-zero when the history continues: fetch from here.
    #[serde(default)]
    pub next_event_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActivityTask {
    pub token: TaskToken,
    pub workflow_type: String,
    pub activity_type: String,
    pub input: Payload,
    #[serde(default)]
    pub heartbeat_details: Option<Payload>,
    #[serde(default)]
    pub start_to_close_ms: u64,
    #[serde(default)]
    pub heartbeat_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListFilter {
    #[serde(default)]
    pub status: Option<WorkflowStatus>,
    #[serde(default)]
    pub workflow_type: Option<String>,
    /// Only ids starting with this.
    #[serde(default)]
    pub id_prefix: Option<String>,
}

/// Another cell, as a replication target.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellRef {
    pub cell: CellId,
    pub shards: u32,
    pub replicas: u8,
    /// Cluster addresses of the cell's nodes, in node order, when the
    /// caller knows them. Nodes use these to reach the cell; a deployment
    /// that configures its cells statically can leave them empty.
    #[serde(default)]
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationStatus {
    pub target: CellRef,
    pub shard: ShardId,
    /// Highest change shipped and acknowledged.
    pub acked_seq: u64,
    /// Latest change on this shard.
    pub head_seq: u64,
    /// Executions that still have to be shipped.
    pub pending: u64,
    #[serde(default)]
    pub paused: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardInfo {
    pub shard: ShardId,
    /// The cell accepts writes when active; a standby only takes replication.
    pub active: bool,
    /// Bumped at every failover; the higher epoch wins a conflict.
    pub epoch: u64,
    pub executions: u64,
    pub open_workflows: u64,
    pub change_seq: u64,
    pub op: u64,
    pub timestamp: Timestamp,
    pub replication: Vec<ReplicationStatus>,
    /// Changes applied from other cells.
    #[serde(default)]
    pub replicated_in: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellInfo {
    pub cell: CellId,
    pub shard_count: u32,
    pub replica_count: u8,
    /// Shards that answered; missing ones are unavailable.
    pub shards: Vec<ShardInfo>,
    /// Cluster addresses of the cell's nodes (see [`CellRef::addresses`]).
    #[serde(default)]
    pub addresses: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
pub enum ApiRequest {
    StartWorkflow(StartWorkflow),
    SignalWorkflow {
        workflow_id: String,
        run_id: Option<Id128>,
        name: String,
        input: Payload,
        request_id: Id128,
    },
    CancelWorkflow {
        workflow_id: String,
        run_id: Option<Id128>,
        reason: String,
    },
    TerminateWorkflow {
        workflow_id: String,
        run_id: Option<Id128>,
        reason: String,
    },
    DescribeWorkflow {
        workflow_id: String,
        run_id: Option<Id128>,
    },
    /// Events with id above `after_event_id`.
    GetHistory {
        workflow_id: String,
        run_id: Option<Id128>,
        after_event_id: u64,
        limit: u32,
    },
    /// Wait up to `wait_ms` for the workflow to close, then describe it.
    /// Follows continue-as-new to the latest run when `run_id` is `None`.
    GetResult {
        workflow_id: String,
        run_id: Option<Id128>,
        wait_ms: u64,
    },
    ListWorkflows {
        filter: ListFilter,
        limit: u32,
    },
    SetVersioningOverride {
        workflow_id: String,
        run_id: Option<Id128>,
        value: Option<VersioningOverride>,
    },

    PollWorkflowTask {
        task_queue: String,
        worker: WorkerInfo,
        wait_ms: u64,
        request_id: Id128,
    },
    CompleteWorkflowTask {
        token: TaskToken,
        commands: Vec<Command>,
        worker: WorkerInfo,
        behavior: VersioningBehavior,
    },
    FailWorkflowTask {
        token: TaskToken,
        failure: Failure,
    },
    PollActivityTask {
        task_queue: String,
        worker: WorkerInfo,
        wait_ms: u64,
        request_id: Id128,
    },
    CompleteActivity {
        token: TaskToken,
        result: Payload,
    },
    FailActivity {
        token: TaskToken,
        failure: Failure,
    },
    HeartbeatActivity {
        token: TaskToken,
        details: Option<Payload>,
    },

    /// Sent by gateways when a new version starts polling a queue.
    RegisterWorker {
        version: DeploymentVersion,
        task_queue: String,
    },
    /// `build_id: None` routes to unversioned workers.
    SetCurrentVersion {
        deployment: String,
        build_id: Option<String>,
    },
    /// `build_id: None` stops ramping.
    SetRampingVersion {
        deployment: String,
        build_id: Option<String>,
        percent: u8,
    },
    DescribeDeployment {
        deployment: String,
    },
    ListDeployments,
    DeleteVersion {
        version: DeploymentVersion,
    },
    DeleteDeployment {
        deployment: String,
    },

    DescribeCell,
    /// Start (or resume) replicating this cell to `target`.
    CreateReplication {
        target: CellRef,
    },
    DeleteReplication {
        cell: CellId,
    },
    PauseReplication {
        cell: CellId,
        paused: bool,
    },
    /// Make this cell active (accept writes) or standby. `epoch` must grow.
    SetRole {
        active: bool,
        epoch: u64,
    },
}

impl ApiRequest {
    pub fn name(&self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ApiResponse {
    Ok,
    Started {
        run_id: Id128,
        already_started: bool,
    },
    Workflow(Box<WorkflowDescription>),
    History {
        events: Vec<Event>,
        next_event_id: u64,
    },
    Workflows(Vec<WorkflowInfo>),
    /// `None`: nothing arrived before the poll timed out.
    WorkflowTask(Option<Box<WorkflowTask>>),
    ActivityTask(Option<Box<ActivityTask>>),
    Heartbeat {
        cancel_requested: bool,
    },
    Deployment(Box<DeploymentInfo>),
    Deployments(Vec<DeploymentInfo>),
    Cell(Box<CellInfo>),
    Shard(Box<ShardInfo>),
}

pub type ApiResult = Result<ApiResponse, Error>;

/// The client side of a gateway connection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClientMessage {
    Request { id: u64, request: ApiRequest },
    Response { id: u64, result: ApiResult },
}

/// Counts by status, for dashboards.
pub fn count_by_status(workflows: &[WorkflowInfo]) -> BTreeMap<&'static str, u64> {
    let mut counts = BTreeMap::new();
    for w in workflows {
        *counts.entry(w.status.as_str()).or_insert(0) += 1;
    }
    counts
}
