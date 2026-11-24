//! The worker as a deterministic actor.
//!
//! [`WorkerCore`] decides what a worker does: which polls to keep open,
//! how a workflow task turns into commands (by [`replay`]), when an
//! activity runs. It performs no I/O: it emits [`ApiRequest`]s and activity
//! jobs and is told the outcomes. The async [`crate::Worker`] and the
//! simulator both drive this same code.

use crate::workflow::{WorkflowFn, WorkflowInfo, replay};
use std::collections::BTreeMap;
use std::sync::Arc;
use wfe_codec::Codec;
use wfe_core::api::*;
use wfe_core::{Error, Id128, Payload, Timestamp};

#[derive(Clone, Debug)]
pub struct WorkerOptions {
    pub task_queue: String,
    pub identity: String,
    /// `None` for an unversioned worker.
    pub version: Option<DeploymentVersion>,
    /// What this build's workflows do when a newer version becomes current.
    pub behavior: VersioningBehavior,
    pub workflow_polls: usize,
    pub activity_polls: usize,
    /// Activities running at once.
    pub activity_slots: usize,
    pub poll_wait_ms: u64,
    /// Give up on an unanswered request after this long.
    pub request_timeout_ms: u64,
    /// Pause after a failed poll.
    pub error_backoff_ms: u64,
}

impl WorkerOptions {
    pub fn new(task_queue: impl Into<String>, identity: impl Into<String>) -> WorkerOptions {
        WorkerOptions {
            task_queue: task_queue.into(),
            identity: identity.into(),
            version: None,
            behavior: VersioningBehavior::Unspecified,
            workflow_polls: 2,
            activity_polls: 2,
            activity_slots: 16,
            poll_wait_ms: 10_000,
            request_timeout_ms: 20_000,
            error_backoff_ms: 500,
        }
    }

    pub fn versioned(mut self, version: DeploymentVersion, behavior: VersioningBehavior) -> WorkerOptions {
        self.version = Some(version);
        self.behavior = behavior;
        self
    }

    /// Versioned by the environment: `WFE_DEPLOYMENT` and `WFE_BUILD_ID`,
    /// which the Kubernetes operator sets in the workers it runs. Without
    /// both, the options are unchanged.
    pub fn versioned_from_env(self, behavior: VersioningBehavior) -> WorkerOptions {
        let var = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        match (var("WFE_DEPLOYMENT"), var("WFE_BUILD_ID")) {
            (Some(deployment), Some(build_id)) => {
                self.versioned(DeploymentVersion::new(deployment, build_id), behavior)
            }
            _ => self,
        }
    }
}

enum Pending {
    WorkflowPoll,
    ActivityPoll,
    History(Box<WorkflowTask>),
    Respond,
}

/// An activity the driver must run, then report with
/// [`WorkerCore::activity_done`].
#[derive(Clone, Debug)]
pub struct ActivityJob {
    pub id: u64,
    pub task: ActivityTask,
}

#[derive(Clone, Debug, Default)]
pub struct WorkerStats {
    pub workflow_tasks: u64,
    pub workflow_task_failures: u64,
    pub activities: u64,
    pub errors: u64,
}

pub struct WorkerCore {
    opts: WorkerOptions,
    workflows: BTreeMap<String, WorkflowFn>,
    codec: Arc<dyn Codec>,
    nonce: u64,
    next_id: u64,
    pending: BTreeMap<u64, (Pending, Timestamp)>,
    out: Vec<(u64, ApiRequest)>,
    jobs: Vec<ActivityJob>,
    running: BTreeMap<u64, TaskToken>,
    backoff_until: Timestamp,
    stopping: bool,
    activity_polls_enabled: usize,
    pub stats: WorkerStats,
}

impl WorkerCore {
    /// `nonce` makes this worker's request ids unique.
    pub fn new(opts: WorkerOptions, codec: Arc<dyn Codec>, nonce: u64) -> WorkerCore {
        WorkerCore {
            opts,
            workflows: BTreeMap::new(),
            codec,
            nonce,
            next_id: 0,
            pending: BTreeMap::new(),
            out: Vec::new(),
            jobs: Vec::new(),
            running: BTreeMap::new(),
            backoff_until: Timestamp::ZERO,
            stopping: false,
            activity_polls_enabled: 0,
            stats: WorkerStats::default(),
        }
    }

    pub fn options(&self) -> &WorkerOptions {
        &self.opts
    }

    pub fn register_workflow(&mut self, workflow_type: impl Into<String>, f: WorkflowFn) {
        self.workflows.insert(workflow_type.into(), f);
    }

    /// Poll for activities with this many polls, once enabled.
    pub fn set_activity_polls(&mut self, now: usize, when_enabled: usize) {
        self.opts.activity_polls = now;
        self.activity_polls_enabled = when_enabled;
    }

    /// Activities are registered: start polling for them.
    pub fn enable_activities(&mut self) {
        self.opts.activity_polls = self.activity_polls_enabled.max(self.opts.activity_polls);
    }

    /// Stop polling; work in hand is finished.
    pub fn stop(&mut self) {
        self.stopping = true;
    }

    pub fn is_idle(&self) -> bool {
        self.pending.is_empty() && self.running.is_empty() && self.jobs.is_empty()
    }

    fn worker(&self) -> WorkerInfo {
        WorkerInfo { identity: self.opts.identity.clone(), version: self.opts.version.clone() }
    }

    fn request(&mut self, pending: Pending, request: ApiRequest, now: Timestamp) {
        self.next_id += 1;
        self.pending.insert(self.next_id, (pending, now));
        self.out.push((self.next_id, request));
    }

    fn request_id(&mut self) -> Id128 {
        self.next_id += 1;
        Id128((u128::from(self.nonce) << 64) | u128::from(self.next_id))
    }

    pub fn take_requests(&mut self) -> Vec<(u64, ApiRequest)> {
        std::mem::take(&mut self.out)
    }

    pub fn take_activities(&mut self) -> Vec<ActivityJob> {
        std::mem::take(&mut self.jobs)
    }

    fn forget_requests_whose_answer_never_came(&mut self, now: Timestamp) {
        let timeout = self.opts.request_timeout_ms + self.opts.poll_wait_ms;
        let before = self.pending.len();
        self.pending.retain(|_, (_, sent)| now < sent.add_ms(timeout));
        self.stats.errors += u64::try_from(before - self.pending.len()).expect("a count of pending requests fits u64");
    }

    pub fn tick(&mut self, now: Timestamp) {
        self.forget_requests_whose_answer_never_came(now);
        if self.stopping || now < self.backoff_until {
            return;
        }
        let workflow_polls = self.pending.values().filter(|(p, _)| matches!(p, Pending::WorkflowPoll)).count();
        if !self.workflows.is_empty() {
            for _ in workflow_polls..self.opts.workflow_polls {
                let request = ApiRequest::PollWorkflowTask {
                    task_queue: self.opts.task_queue.clone(),
                    worker: self.worker(),
                    wait_ms: self.opts.poll_wait_ms,
                    request_id: self.request_id(),
                };
                self.request(Pending::WorkflowPoll, request, now);
            }
        }
        let activity_polls = self.pending.values().filter(|(p, _)| matches!(p, Pending::ActivityPoll)).count();
        let free = self.opts.activity_slots.saturating_sub(self.running.len() + self.jobs.len() + activity_polls);
        for _ in 0..self.opts.activity_polls.saturating_sub(activity_polls).min(free) {
            let request = ApiRequest::PollActivityTask {
                task_queue: self.opts.task_queue.clone(),
                worker: self.worker(),
                wait_ms: self.opts.poll_wait_ms,
                request_id: self.request_id(),
            };
            self.request(Pending::ActivityPoll, request, now);
        }
    }

    pub fn on_response(&mut self, id: u64, result: ApiResult, now: Timestamp) {
        let Some((pending, _)) = self.pending.remove(&id) else { return };
        let response = match result {
            Ok(response) => response,
            Err(_) => {
                self.stats.errors += 1;
                if !matches!(pending, Pending::Respond) {
                    self.backoff_until = now.add_ms(self.opts.error_backoff_ms);
                }
                return;
            }
        };
        match (pending, response) {
            (Pending::WorkflowPoll, ApiResponse::WorkflowTask(Some(task))) => self.on_workflow_task(*task, now),
            (Pending::History(mut task), ApiResponse::History { events, next_event_id }) => {
                task.history.extend(events);
                task.next_event_id = next_event_id;
                self.on_workflow_task(*task, now);
            }
            (Pending::ActivityPoll, ApiResponse::ActivityTask(Some(mut task))) => {
                self.next_id += 1;
                let id = self.next_id;
                self.running.insert(id, task.token.clone());
                match self.decode(std::mem::take(&mut task.input)) {
                    Ok(input) => {
                        task.input = input;
                        if let Some(details) = task.heartbeat_details.take() {
                            task.heartbeat_details = self.decode(details).ok();
                        }
                        self.jobs.push(ActivityJob { id, task: *task });
                    }
                    Err(failure) => self.activity_done(id, Err(failure), now),
                }
            }
            _ => {}
        }
    }

    fn decode(&self, payload: Payload) -> Result<Payload, Failure> {
        self.codec.decode(payload).map_err(|e| Failure::new(e.to_string()).kind("codec"))
    }

    fn encode(&self, payload: Payload) -> Result<Payload, Failure> {
        self.codec.encode(payload).map_err(|e| Failure::new(e.to_string()).kind("codec"))
    }

    fn on_workflow_task(&mut self, mut task: WorkflowTask, now: Timestamp) {
        if task.next_event_id != 0 {
            let request = ApiRequest::GetHistory {
                workflow_id: task.token.workflow_id.clone(),
                run_id: Some(task.token.run_id),
                after_event_id: task.next_event_id,
                limit: 1000,
            };
            return self.request(Pending::History(Box::new(task)), request, now);
        }
        self.stats.workflow_tasks += 1;
        let token = task.token.clone();
        let outcome = self.decide(&mut task);
        let request = match outcome {
            Ok(commands) => ApiRequest::CompleteWorkflowTask {
                token,
                commands,
                worker: self.worker(),
                behavior: self.opts.behavior,
            },
            Err(failure) => {
                self.stats.workflow_task_failures += 1;
                ApiRequest::FailWorkflowTask { token, failure }
            }
        };
        self.request(Pending::Respond, request, now);
    }

    fn decide(&self, task: &mut WorkflowTask) -> Result<Vec<Command>, Failure> {
        let Some(workflow) = self.workflows.get(&task.workflow_type) else {
            return Err(Failure::new(format!("workflow type {} is not registered on this worker", task.workflow_type))
                .kind("not_registered"));
        };
        let mut task_queue = String::new();
        for event in &mut task.history {
            match &mut event.attrs {
                EventAttrs::WorkflowStarted { input, task_queue: queue, .. } => {
                    task_queue = queue.clone();
                    *input = self.decode(std::mem::take(input))?;
                }
                EventAttrs::ActivityCompleted { result, .. } => *result = self.decode(std::mem::take(result))?,
                EventAttrs::SignalReceived { input, .. } => *input = self.decode(std::mem::take(input))?,
                EventAttrs::MarkerRecorded { details, .. } => *details = self.decode(std::mem::take(details))?,
                EventAttrs::WorkflowTaskScheduled { .. }
                | EventAttrs::WorkflowTaskStarted { .. }
                | EventAttrs::WorkflowTaskCompleted { .. }
                | EventAttrs::WorkflowTaskFailed { .. }
                | EventAttrs::WorkflowTaskTimedOut { .. }
                | EventAttrs::ActivityScheduled { .. }
                | EventAttrs::ActivityStarted { .. }
                | EventAttrs::ActivityFailed { .. }
                | EventAttrs::ActivityTimedOut { .. }
                | EventAttrs::TimerStarted { .. }
                | EventAttrs::TimerFired { .. }
                | EventAttrs::TimerCanceled { .. }
                | EventAttrs::CancelRequested { .. }
                | EventAttrs::VersioningOverrideChanged { .. }
                | EventAttrs::WorkflowCompleted { .. }
                | EventAttrs::WorkflowFailed { .. }
                | EventAttrs::WorkflowCanceled
                | EventAttrs::WorkflowTerminated { .. }
                | EventAttrs::WorkflowTimedOut
                | EventAttrs::WorkflowContinuedAsNew { .. } => {}
            }
        }
        let info = WorkflowInfo {
            workflow_id: task.token.workflow_id.clone(),
            run_id: task.token.run_id,
            workflow_type: task.workflow_type.clone(),
            task_queue,
        };
        let mut commands = replay(workflow, info, &task.history)?;
        for command in &mut commands {
            match command {
                Command::ScheduleActivity { input, .. } | Command::ContinueAsNew { input } => {
                    *input = self.encode(std::mem::take(input))?
                }
                Command::RecordMarker { details, .. } => *details = self.encode(std::mem::take(details))?,
                Command::CompleteWorkflow { result } => *result = self.encode(std::mem::take(result))?,
                Command::StartTimer { .. }
                | Command::CancelTimer { .. }
                | Command::FailWorkflow { .. }
                | Command::CancelWorkflow => {}
            }
        }
        Ok(commands)
    }

    /// The driver finished running an activity.
    pub fn activity_done(&mut self, job: u64, result: Result<Payload, Failure>, now: Timestamp) {
        let Some(token) = self.running.remove(&job) else { return };
        self.stats.activities += 1;
        let request = match result.and_then(|payload| self.encode(payload)) {
            Ok(result) => ApiRequest::CompleteActivity { token, result },
            Err(failure) => ApiRequest::FailActivity { token, failure },
        };
        self.request(Pending::Respond, request, now);
    }

    /// Report progress of a running activity.
    pub fn heartbeat(&mut self, job: u64, details: Option<Payload>, now: Timestamp) {
        let Some(token) = self.running.get(&job).cloned() else { return };
        let details = details.and_then(|d| self.encode(d).ok());
        self.request(Pending::Respond, ApiRequest::HeartbeatActivity { token, details }, now);
    }
}

/// A transport-level failure, as an API error.
pub fn unavailable(message: impl Into<String>) -> Error {
    Error::unavailable(message)
}
