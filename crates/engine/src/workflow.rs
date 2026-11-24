//! The workflow archetype.
//!
//! Nodes of a workflow execution:
//!
//! - `w`: [`WfState`], the mutable summary;
//! - `h/<event id>`: one history [`Event`] each, append-only;
//! - `a/<scheduled event id>`: one pending activity each;
//! - `t/<started event id>`: one pending timer each.
//!
//! Its logical tasks ([`WfTask`]) carry the generation they were created for
//! (event id, attempt, deadline) and are valid only while the state still
//! says the same thing.

use crate::chasm::{LogicalTask, QueueSlot, Summary, Tx};
use crate::{Fail, Fallible, decode_stored};
use serde::{Deserialize, Serialize};
use wfe_core::api::*;
use wfe_core::codec::encode;
use wfe_core::keys::Key;
use wfe_core::{Error, Id128, Payload, Timestamp};
use wfe_lsm::StorageError;

const STATE: &str = "w";

fn hist(id: u64) -> String {
    format!("h/{id:016x}")
}
fn act_path(id: u64) -> String {
    format!("a/{id:016x}")
}
fn timer_path(id: u64) -> String {
    format!("t/{id:016x}")
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Wft {
    #[default]
    None,
    Backoff {
        attempt: u32,
    },
    Scheduled {
        scheduled_id: u64,
        attempt: u32,
    },
    Started {
        scheduled_id: u64,
        started_id: u64,
        attempt: u32,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WfState {
    pub workflow_type: String,
    pub task_queue: String,
    pub status: WorkflowStatus,
    pub start_time: Timestamp,
    pub close_time: Timestamp,
    /// The id the next event gets.
    pub next_event_id: u64,
    pub result: Option<Payload>,
    pub failure: Option<Failure>,
    pub wft: Wft,
    /// Events have arrived that no workflow task has seen.
    pub unseen: bool,
    pub last_completed_wft: u64,
    pub versioning: WorkflowVersioning,
    pub cancel_requested: bool,
    pub start_request: Id128,
    /// Recent signal request ids, for deduplication.
    pub signal_requests: Vec<Id128>,
    pub continued_as: Option<Id128>,
    pub pending_timers: u64,
    pub pending_activities: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum ActPhase {
    Scheduled,
    Started,
    Backoff,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActState {
    pub scheduled_id: u64,
    pub options: ActivityOptions,
    pub input: Payload,
    pub attempt: u32,
    pub phase: ActPhase,
    pub expires_at: Option<Timestamp>,
    pub heartbeat_details: Option<Payload>,
    pub heartbeat_deadline: Option<Timestamp>,
    pub last_failure: Option<Failure>,
    pub identity: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WfTask {
    /// A workflow task waits in the queue.
    Dispatch {
        scheduled_id: u64,
        attempt: u32,
    },
    WftTimeout {
        scheduled_id: u64,
        attempt: u32,
    },
    WftRetry {
        attempt: u32,
    },
    ActDispatch {
        scheduled_id: u64,
        attempt: u32,
    },
    ActTimeout {
        scheduled_id: u64,
        attempt: u32,
        kind: TimeoutKind,
        deadline: Timestamp,
    },
    ActRetry {
        scheduled_id: u64,
        attempt: u32,
    },
    Timer {
        started_id: u64,
    },
    ExecutionTimeout,
}

fn load(tx: &mut Tx) -> Fallible<WfState> {
    tx.get::<WfState>(STATE)?.ok_or_else(|| Fail::Api(Error::not_found(format!("workflow {} not found", tx.key.id))))
}

fn running(id: &str, st: &WfState) -> Fallible<()> {
    if st.status.is_closed() {
        return Err(Fail::Api(Error::precondition(format!(
            "workflow {id} is {}; only a running workflow accepts this request: start a new run, or read \
             the outcome with GetResult",
            st.status.as_str()
        ))));
    }
    Ok(())
}

fn append(tx: &mut Tx, st: &mut WfState, attrs: EventAttrs) -> Result<u64, StorageError> {
    let id = st.next_event_id;
    st.next_event_id += 1;
    tx.put(&hist(id), &Event { id, time: tx.now, attrs })?;
    Ok(id)
}

fn slot(tx: &Tx, st: &WfState, queue: &str, kind: TaskKind) -> QueueSlot {
    QueueSlot {
        queue: queue.to_string(),
        kind,
        pinned: st.versioning.pinned().cloned(),
        ramp: (wfe_core::checksum::route_hash(tx.key.id.as_bytes()) % 100) as u8,
    }
}

fn schedule_wft(tx: &mut Tx, st: &mut WfState, attempt: u32) -> Result<(), StorageError> {
    if st.status.is_closed() || !matches!(st.wft, Wft::None | Wft::Backoff { .. }) {
        return Ok(());
    }
    let scheduled_id = append(tx, st, EventAttrs::WorkflowTaskScheduled { attempt })?;
    st.wft = Wft::Scheduled { scheduled_id, attempt };
    let slot = slot(tx, st, &st.task_queue, TaskKind::Workflow);
    tx.add_task(None, Some(slot), false, &WfTask::Dispatch { scheduled_id, attempt });
    Ok(())
}

fn notify(tx: &mut Tx, st: &mut WfState) -> Result<(), StorageError> {
    st.unseen = true;
    if st.wft == Wft::None {
        schedule_wft(tx, st, 1)?;
    }
    Ok(())
}

fn close(tx: &mut Tx, st: &mut WfState, status: WorkflowStatus, attrs: EventAttrs) -> Result<(), StorageError> {
    append(tx, st, attrs)?;
    st.status = status;
    st.close_time = tx.now;
    st.wft = Wft::None;
    tx.meta.closed = true;
    Ok(())
}

pub fn start(tx: &mut Tx, req: &StartWorkflow, continued_from: Option<Id128>) -> Fallible<()> {
    let mut st = WfState {
        workflow_type: req.workflow_type.clone(),
        task_queue: req.task_queue.clone(),
        start_time: tx.now,
        next_event_id: 1,
        start_request: req.request_id,
        versioning: WorkflowVersioning { override_: req.versioning_override.clone(), ..Default::default() },
        ..WfState::default()
    };
    append(
        tx,
        &mut st,
        EventAttrs::WorkflowStarted {
            workflow_type: req.workflow_type.clone(),
            task_queue: req.task_queue.clone(),
            input: req.input.clone(),
            execution_timeout_ms: req.execution_timeout_ms,
            continued_from,
        },
    )?;
    if req.execution_timeout_ms > 0 {
        tx.add_task(Some(tx.now.add_ms(req.execution_timeout_ms)), None, false, &WfTask::ExecutionTimeout);
    }
    schedule_wft(tx, &mut st, 1)?;
    tx.put(STATE, &st)?;
    Ok(())
}

/// `Some(request id)` of the start if the execution exists.
pub fn peek(tx: &mut Tx) -> Result<Option<WfState>, StorageError> {
    tx.get::<WfState>(STATE)
}

pub fn signal(tx: &mut Tx, name: &str, input: &Payload, request_id: Id128) -> Fallible<()> {
    let mut st = load(tx)?;
    let retry_of_delivered_signal = !request_id.is_zero() && st.signal_requests.contains(&request_id);
    if retry_of_delivered_signal {
        return Ok(());
    }
    running(&tx.key.id, &st)?;
    if !request_id.is_zero() {
        st.signal_requests.push(request_id);
        if st.signal_requests.len() > 32 {
            st.signal_requests.remove(0);
        }
    }
    append(tx, &mut st, EventAttrs::SignalReceived { name: name.to_string(), input: input.clone() })?;
    notify(tx, &mut st)?;
    tx.put(STATE, &st)?;
    Ok(())
}

pub fn cancel(tx: &mut Tx, reason: &str) -> Fallible<()> {
    let mut st = load(tx)?;
    running(&tx.key.id, &st)?;
    if !st.cancel_requested {
        st.cancel_requested = true;
        append(tx, &mut st, EventAttrs::CancelRequested { reason: reason.to_string() })?;
        notify(tx, &mut st)?;
        tx.put(STATE, &st)?;
    }
    Ok(())
}

pub fn terminate(tx: &mut Tx, reason: &str) -> Fallible<()> {
    let mut st = load(tx)?;
    if st.status.is_closed() {
        return Ok(());
    }
    close(tx, &mut st, WorkflowStatus::Terminated, EventAttrs::WorkflowTerminated { reason: reason.to_string() })?;
    st.failure = Some(Failure::new(reason).kind("terminated"));
    tx.put(STATE, &st)?;
    Ok(())
}

pub fn set_override(tx: &mut Tx, value: &Option<VersioningOverride>) -> Fallible<()> {
    let mut st = load(tx)?;
    running(&tx.key.id, &st)?;
    if st.versioning.override_ == *value {
        return Ok(());
    }
    st.versioning.override_ = value.clone();
    append(tx, &mut st, EventAttrs::VersioningOverrideChanged { value: value.clone() })?;
    requeue_waiting_tasks_under_new_routing(tx, &st);
    tx.put(STATE, &st)?;
    Ok(())
}

fn info(tx: &Tx, st: &WfState) -> WorkflowInfo {
    WorkflowInfo {
        workflow_id: tx.key.id.clone(),
        run_id: tx.key.run,
        workflow_type: st.workflow_type.clone(),
        task_queue: st.task_queue.clone(),
        status: st.status,
        start_time: st.start_time,
        close_time: st.close_time,
        history_length: st.next_event_id - 1,
        versioning: st.versioning.clone(),
        shard: tx.shard.cfg.shard,
    }
}

pub fn describe(tx: &mut Tx) -> Fallible<WorkflowDescription> {
    let st = load(tx)?;
    let first: Event = tx.get(&hist(1))?.expect("every workflow history starts with event 1, written when it started");
    let EventAttrs::WorkflowStarted { input, .. } = first.attrs else { panic!("event 1 is the start") };
    let activities: Vec<(String, ActState)> = tx.list("a/", "", 1000)?;
    Ok(WorkflowDescription {
        info: info(tx, &st),
        input,
        result: st.result.clone(),
        failure: st.failure.clone(),
        pending_activities: if st.status.is_closed() {
            Vec::new()
        } else {
            activities
                .into_iter()
                .map(|(_, a)| PendingActivity {
                    scheduled_id: a.scheduled_id,
                    task_queue: a.options.task_queue.clone().unwrap_or_else(|| st.task_queue.clone()),
                    activity_type: a.options.activity_type,
                    attempt: a.attempt,
                    state: <&'static str>::from(a.phase).to_string(),
                    last_failure: a.last_failure,
                })
                .collect()
        },
        pending_timers: if st.status.is_closed() { 0 } else { st.pending_timers },
        workflow_task: <&'static str>::from(&st.wft).to_string(),
        cancel_requested: st.cancel_requested,
        continued_as: st.continued_as,
    })
}

/// Events after `after`, up to `limit` and the page size. Returns the id to
/// continue from, or zero at the end.
pub fn history(tx: &mut Tx, after: u64, limit: usize) -> Fallible<(Vec<Event>, u64)> {
    let st = load(tx)?;
    let budget = tx.shard.cfg.page_bytes_max;
    let limit = limit.clamp(1, 1000);
    let rows: Vec<(String, Event)> = tx.list("h/", &hist(after), limit)?;
    let mut events = Vec::new();
    let mut bytes = 0;
    for (_, event) in rows {
        bytes += encode(&event).len();
        if !events.is_empty() && bytes > budget {
            break;
        }
        events.push(event);
    }
    let last = events.last().map_or(after, |e| e.id);
    let next = if last + 1 < st.next_event_id { last } else { 0 };
    Ok((events, next))
}

fn token_error(what: &str) -> Fail {
    Fail::Api(Error::stale(format!("{what} is no longer current")))
}

/// A worker takes the workflow task that `task` dispatches.
pub fn start_wft(tx: &mut Tx, task: &LogicalTask, worker: &WorkerInfo) -> Fallible<WorkflowTask> {
    let mut st = load(tx)?;
    let Wft::Scheduled { scheduled_id, attempt } = st.wft else { return Err(token_error("workflow task")) };
    let _ = task;
    let started_id = append(
        tx,
        &mut st,
        EventAttrs::WorkflowTaskStarted {
            scheduled_id,
            identity: worker.identity.clone(),
            version: worker.version.clone(),
        },
    )?;
    st.wft = Wft::Started { scheduled_id, started_id, attempt };
    st.unseen = false;
    let timeout = tx.shard.cfg.workflow_task_timeout_ms;
    tx.add_task(Some(tx.now.add_ms(timeout)), None, false, &WfTask::WftTimeout { scheduled_id, attempt });
    tx.put(STATE, &st)?;
    let (history, next_event_id) = history(tx, 0, 1000)?;
    Ok(WorkflowTask {
        token: TaskToken { workflow_id: tx.key.id.clone(), run_id: tx.key.run, scheduled_id, attempt, started_id },
        workflow_type: st.workflow_type,
        history,
        next_event_id,
    })
}

/// What the engine must do after a workflow task completed.
pub enum Followup {
    None,
    /// Start the next run of a workflow that continued as new.
    ContinueAsNew {
        run_id: Id128,
        request: Box<StartWorkflow>,
    },
}

pub fn complete_wft(
    tx: &mut Tx,
    token: &TaskToken,
    commands: &[Command],
    worker: &WorkerInfo,
    behavior: VersioningBehavior,
) -> Fallible<Followup> {
    let mut st = load(tx)?;
    let retried_completion = st.last_completed_wft == token.scheduled_id;
    if retried_completion {
        return Ok(Followup::None);
    }
    let Wft::Started { scheduled_id, started_id, attempt } = st.wft else { return Err(token_error("workflow task")) };
    if scheduled_id != token.scheduled_id || attempt != token.attempt || started_id != token.started_id {
        return Err(token_error("workflow task"));
    }
    let terminal = commands.iter().position(|c| {
        matches!(
            c,
            Command::CompleteWorkflow { .. }
                | Command::FailWorkflow { .. }
                | Command::CancelWorkflow
                | Command::ContinueAsNew { .. }
        )
    });
    if let Some(i) = terminal.filter(|i| i + 1 != commands.len()) {
        return Err(Fail::Api(Error::invalid(format!(
            "command {i} of {} closes the workflow but is not last: CompleteWorkflow, FailWorkflow, \
             CancelWorkflow and ContinueAsNew must be the final command, so drop the commands after it",
            commands.len()
        ))));
    }
    let closing_without_seeing_new_events = terminal.is_some() && st.unseen;
    if closing_without_seeing_new_events {
        append(
            tx,
            &mut st,
            EventAttrs::WorkflowTaskFailed {
                scheduled_id,
                started_id,
                failure: Failure::new("new events arrived before the workflow closed").kind("unhandled_events"),
            },
        )?;
        st.wft = Wft::None;
        schedule_wft(tx, &mut st, 1)?;
        tx.put(STATE, &st)?;
        return Ok(Followup::None);
    }

    let behavior = if worker.version.is_some() { behavior } else { VersioningBehavior::Unspecified };
    append(
        tx,
        &mut st,
        EventAttrs::WorkflowTaskCompleted { scheduled_id, started_id, version: worker.version.clone(), behavior },
    )?;
    st.last_completed_wft = scheduled_id;
    st.wft = Wft::None;
    st.versioning.version = worker.version.clone();
    st.versioning.behavior = behavior;

    let mut followup = Followup::None;
    for command in commands {
        match command {
            Command::ScheduleActivity { options, input } => {
                if options.activity_type.is_empty() {
                    return Err(Fail::Api(Error::invalid(
                        "ScheduleActivity has an empty activity_type: set it to the name a worker registered",
                    )));
                }
                let id = append(
                    tx,
                    &mut st,
                    EventAttrs::ActivityScheduled { options: options.clone(), input: input.clone() },
                )?;
                let expires_at =
                    (options.schedule_to_close_ms > 0).then(|| tx.now.add_ms(options.schedule_to_close_ms));
                let act = ActState {
                    scheduled_id: id,
                    options: options.clone(),
                    input: input.clone(),
                    attempt: 1,
                    phase: ActPhase::Scheduled,
                    expires_at,
                    heartbeat_details: None,
                    heartbeat_deadline: None,
                    last_failure: None,
                    identity: String::new(),
                };
                tx.put(&act_path(id), &act)?;
                st.pending_activities += 1;
                dispatch_activity(tx, &st, &act);
                if let Some(deadline) = expires_at {
                    tx.add_task(
                        Some(deadline),
                        None,
                        false,
                        &WfTask::ActTimeout {
                            scheduled_id: id,
                            attempt: 0,
                            kind: TimeoutKind::ScheduleToClose,
                            deadline,
                        },
                    );
                }
            }
            Command::StartTimer { fire_after_ms } => {
                let id = append(tx, &mut st, EventAttrs::TimerStarted { fire_after_ms: *fire_after_ms })?;
                let at = tx.now.add_ms(*fire_after_ms);
                tx.put(&timer_path(id), &at)?;
                st.pending_timers += 1;
                tx.add_task(Some(at), None, false, &WfTask::Timer { started_id: id });
            }
            Command::CancelTimer { started_id } => {
                if tx.get::<Timestamp>(&timer_path(*started_id))?.is_some() {
                    tx.delete(&timer_path(*started_id))?;
                    st.pending_timers -= 1;
                    append(tx, &mut st, EventAttrs::TimerCanceled { started_id: *started_id })?;
                }
            }
            Command::RecordMarker { name, details } => {
                append(tx, &mut st, EventAttrs::MarkerRecorded { name: name.clone(), details: details.clone() })?;
            }
            Command::CompleteWorkflow { result } => {
                st.result = Some(result.clone());
                close(
                    tx,
                    &mut st,
                    WorkflowStatus::Completed,
                    EventAttrs::WorkflowCompleted { result: result.clone() },
                )?;
            }
            Command::FailWorkflow { failure } => {
                st.failure = Some(failure.clone());
                close(tx, &mut st, WorkflowStatus::Failed, EventAttrs::WorkflowFailed { failure: failure.clone() })?;
            }
            Command::CancelWorkflow => {
                st.failure = Some(Failure::new("workflow canceled").kind("canceled"));
                close(tx, &mut st, WorkflowStatus::Canceled, EventAttrs::WorkflowCanceled)?;
            }
            Command::ContinueAsNew { input } => {
                let run_id = Id128::derive(&[&tx.key.run.0.to_le_bytes(), b"continue-as-new"]);
                st.continued_as = Some(run_id);
                close(
                    tx,
                    &mut st,
                    WorkflowStatus::ContinuedAsNew,
                    EventAttrs::WorkflowContinuedAsNew { new_run_id: run_id, input: input.clone() },
                )?;
                let first: Event =
                    tx.get(&hist(1))?.expect("every workflow history starts with event 1, written when it started");
                let EventAttrs::WorkflowStarted { execution_timeout_ms, .. } = first.attrs else {
                    panic!("event 1 of every workflow history is WorkflowStarted")
                };
                followup = Followup::ContinueAsNew {
                    run_id,
                    request: Box::new(StartWorkflow {
                        workflow_id: tx.key.id.clone(),
                        workflow_type: st.workflow_type.clone(),
                        task_queue: st.task_queue.clone(),
                        input: input.clone(),
                        request_id: run_id,
                        execution_timeout_ms,
                        id_conflict: IdConflictPolicy::Fail,
                        versioning_override: st.versioning.override_.clone(),
                    }),
                };
            }
        }
    }
    if st.unseen {
        schedule_wft(tx, &mut st, 1)?;
    }
    tx.put(STATE, &st)?;
    Ok(followup)
}

fn backoff_ms_so_a_workflow_bug_never_spins(failed_attempt: u32) -> u64 {
    (u64::from(failed_attempt) * 1_000).min(10_000)
}

fn requeue_waiting_tasks_under_new_routing(tx: &mut Tx, st: &WfState) {
    let waiting: Vec<LogicalTask> = tx.meta.tasks.iter().filter(|t| t.queue.is_some()).cloned().collect();
    for task in waiting {
        let Some(old) = task.queue else { continue };
        tx.remove_task(task.id);
        let slot = slot(tx, st, &old.queue, old.kind);
        let body: WfTask = decode_stored(&task.body);
        tx.add_task(None, Some(slot), false, &body);
    }
}

pub fn fail_wft(tx: &mut Tx, token: &TaskToken, failure: &Failure) -> Fallible<()> {
    let mut st = load(tx)?;
    let Wft::Started { scheduled_id, started_id, attempt } = st.wft else { return Err(token_error("workflow task")) };
    if scheduled_id != token.scheduled_id || attempt != token.attempt {
        return Err(token_error("workflow task"));
    }
    append(tx, &mut st, EventAttrs::WorkflowTaskFailed { scheduled_id, started_id, failure: failure.clone() })?;
    st.wft = Wft::Backoff { attempt: attempt + 1 };
    let delay = backoff_ms_so_a_workflow_bug_never_spins(attempt);
    tx.add_task(Some(tx.now.add_ms(delay)), None, false, &WfTask::WftRetry { attempt: attempt + 1 });
    tx.put(STATE, &st)?;
    Ok(())
}

fn dispatch_activity(tx: &mut Tx, st: &WfState, act: &ActState) {
    let queue = act.options.task_queue.clone().unwrap_or_else(|| st.task_queue.clone());
    let slot = slot(tx, st, &queue, TaskKind::Activity);
    tx.add_task(None, Some(slot), false, &WfTask::ActDispatch { scheduled_id: act.scheduled_id, attempt: act.attempt });
}

/// A worker takes the activity task that `task` dispatches.
pub fn start_activity(tx: &mut Tx, task: &LogicalTask, worker: &WorkerInfo) -> Fallible<ActivityTask> {
    let st = load(tx)?;
    let WfTask::ActDispatch { scheduled_id, attempt } = decode_stored(&task.body) else {
        return Err(token_error("activity task"));
    };
    let Some(mut act) = tx.get::<ActState>(&act_path(scheduled_id))? else { return Err(token_error("activity task")) };
    if act.phase != ActPhase::Scheduled || act.attempt != attempt {
        return Err(token_error("activity task"));
    }
    act.phase = ActPhase::Started;
    act.identity = worker.identity.clone();
    let start_to_close = if act.options.start_to_close_ms > 0 {
        act.options.start_to_close_ms
    } else {
        tx.shard.cfg.activity_start_to_close_ms
    };
    let deadline = tx.now.add_ms(start_to_close);
    tx.add_task(
        Some(deadline),
        None,
        false,
        &WfTask::ActTimeout { scheduled_id, attempt, kind: TimeoutKind::StartToClose, deadline },
    );
    if act.options.heartbeat_ms > 0 {
        let deadline = tx.now.add_ms(act.options.heartbeat_ms);
        act.heartbeat_deadline = Some(deadline);
        tx.add_task(
            Some(deadline),
            None,
            false,
            &WfTask::ActTimeout { scheduled_id, attempt, kind: TimeoutKind::Heartbeat, deadline },
        );
    }
    tx.put(&act_path(scheduled_id), &act)?;
    Ok(ActivityTask {
        token: TaskToken { workflow_id: tx.key.id.clone(), run_id: tx.key.run, scheduled_id, attempt, started_id: 0 },
        workflow_type: st.workflow_type,
        activity_type: act.options.activity_type,
        input: act.input,
        heartbeat_details: act.heartbeat_details,
        start_to_close_ms: start_to_close,
        heartbeat_ms: act.options.heartbeat_ms,
    })
}

fn started_activity(tx: &mut Tx, st: &WfState, token: &TaskToken) -> Fallible<ActState> {
    running(&tx.key.id, st)?;
    match tx.get::<ActState>(&act_path(token.scheduled_id))? {
        Some(act) if act.phase == ActPhase::Started && act.attempt == token.attempt => Ok(act),
        _ => Err(Fail::Api(Error::not_found(format!(
            "activity {} attempt {} is no longer running (it completed, timed out or was retried): drop this \
             result; the current attempt reports its own",
            token.scheduled_id, token.attempt
        )))),
    }
}

fn finish_activity(tx: &mut Tx, st: &mut WfState, act: &ActState, attrs: EventAttrs) -> Result<(), StorageError> {
    append(
        tx,
        st,
        EventAttrs::ActivityStarted {
            scheduled_id: act.scheduled_id,
            attempt: act.attempt,
            identity: act.identity.clone(),
        },
    )?;
    append(tx, st, attrs)?;
    tx.delete(&act_path(act.scheduled_id))?;
    st.pending_activities -= 1;
    notify(tx, st)
}

pub fn complete_activity(tx: &mut Tx, token: &TaskToken, result: &Payload) -> Fallible<()> {
    let mut st = load(tx)?;
    let act = started_activity(tx, &st, token)?;
    finish_activity(
        tx,
        &mut st,
        &act,
        EventAttrs::ActivityCompleted { scheduled_id: act.scheduled_id, result: result.clone() },
    )?;
    tx.put(STATE, &st)?;
    Ok(())
}

fn attempt_failed(
    tx: &mut Tx,
    st: &mut WfState,
    mut act: ActState,
    failure: Failure,
    timeout: Option<TimeoutKind>,
) -> Result<(), StorageError> {
    let policy = act.options.retry.clone().unwrap_or_default();
    let retry_at = tx.now.add_ms(policy.delay_ms(act.attempt));
    let retry = !failure.non_retryable
        && (policy.maximum_attempts == 0 || act.attempt < policy.maximum_attempts)
        && act.expires_at.is_none_or(|expires| retry_at < expires);
    if retry {
        act.phase = ActPhase::Backoff;
        act.attempt += 1;
        act.last_failure = Some(failure);
        act.heartbeat_deadline = None;
        tx.add_task(
            Some(retry_at),
            None,
            false,
            &WfTask::ActRetry { scheduled_id: act.scheduled_id, attempt: act.attempt },
        );
        tx.put(&act_path(act.scheduled_id), &act)
    } else {
        let attrs = match timeout {
            Some(kind) => EventAttrs::ActivityTimedOut { scheduled_id: act.scheduled_id, kind },
            None => EventAttrs::ActivityFailed { scheduled_id: act.scheduled_id, failure },
        };
        finish_activity(tx, st, &act, attrs)
    }
}

pub fn fail_activity(tx: &mut Tx, token: &TaskToken, failure: &Failure) -> Fallible<()> {
    let mut st = load(tx)?;
    let act = started_activity(tx, &st, token)?;
    attempt_failed(tx, &mut st, act, failure.clone(), None)?;
    tx.put(STATE, &st)?;
    Ok(())
}

/// Returns whether the workflow wants the activity to stop.
pub fn heartbeat(tx: &mut Tx, token: &TaskToken, details: &Option<Payload>) -> Fallible<bool> {
    let st = load(tx)?;
    let mut act = started_activity(tx, &st, token)?;
    if details.is_some() {
        act.heartbeat_details = details.clone();
    }
    if act.options.heartbeat_ms > 0 {
        let deadline = tx.now.add_ms(act.options.heartbeat_ms);
        act.heartbeat_deadline = Some(deadline);
        tx.add_task(
            Some(deadline),
            None,
            false,
            &WfTask::ActTimeout {
                scheduled_id: act.scheduled_id,
                attempt: act.attempt,
                kind: TimeoutKind::Heartbeat,
                deadline,
            },
        );
    }
    tx.put(&act_path(act.scheduled_id), &act)?;
    Ok(st.cancel_requested)
}

pub fn validate(tx: &mut Tx, task: &LogicalTask) -> Result<bool, StorageError> {
    let Some(st) = tx.get::<WfState>(STATE)? else { return Ok(false) };
    if st.status.is_closed() {
        return Ok(false);
    }
    Ok(match decode_stored::<WfTask>(&task.body) {
        WfTask::Dispatch { scheduled_id, attempt } => st.wft == Wft::Scheduled { scheduled_id, attempt },
        WfTask::WftTimeout { scheduled_id, attempt } => {
            matches!(st.wft, Wft::Started { scheduled_id: s, attempt: a, .. } if s == scheduled_id && a == attempt)
        }
        WfTask::WftRetry { attempt } => st.wft == Wft::Backoff { attempt },
        WfTask::ActDispatch { scheduled_id, attempt } => tx
            .get::<ActState>(&act_path(scheduled_id))?
            .is_some_and(|a| a.phase == ActPhase::Scheduled && a.attempt == attempt),
        WfTask::ActTimeout { scheduled_id, attempt, kind, deadline } => {
            tx.get::<ActState>(&act_path(scheduled_id))?.is_some_and(|a| match kind {
                TimeoutKind::ScheduleToClose => true,
                TimeoutKind::StartToClose => a.phase == ActPhase::Started && a.attempt == attempt,
                TimeoutKind::Heartbeat => {
                    a.phase == ActPhase::Started && a.attempt == attempt && a.heartbeat_deadline == Some(deadline)
                }
            })
        }
        WfTask::ActRetry { scheduled_id, attempt } => tx
            .get::<ActState>(&act_path(scheduled_id))?
            .is_some_and(|a| a.phase == ActPhase::Backoff && a.attempt == attempt),
        WfTask::Timer { started_id } => tx.get::<Timestamp>(&timer_path(started_id))?.is_some(),
        WfTask::ExecutionTimeout => true,
    })
}

/// Run a timer task that is due and valid.
pub fn fire(tx: &mut Tx, task: &LogicalTask) -> Result<(), StorageError> {
    let mut st: WfState = tx.get(STATE)?.expect("validate() returned true only because the workflow state exists");
    match decode_stored::<WfTask>(&task.body) {
        WfTask::WftTimeout { scheduled_id, .. } => {
            let Wft::Started { started_id, attempt, .. } = st.wft else { unreachable!() };
            append(tx, &mut st, EventAttrs::WorkflowTaskTimedOut { scheduled_id, started_id })?;
            st.wft = Wft::None;
            schedule_wft(tx, &mut st, attempt + 1)?;
        }
        WfTask::WftRetry { attempt } => schedule_wft(tx, &mut st, attempt)?,
        WfTask::ActTimeout { scheduled_id, kind, .. } => {
            let act: ActState =
                tx.get(&act_path(scheduled_id))?.expect("validate() returned true only because this activity exists");
            match kind {
                TimeoutKind::ScheduleToClose => {
                    finish_activity(tx, &mut st, &act, EventAttrs::ActivityTimedOut { scheduled_id, kind })?
                }
                TimeoutKind::StartToClose | TimeoutKind::Heartbeat => {
                    let failure = Failure::new(format!("activity timed out ({kind:?})")).kind("timeout");
                    attempt_failed(tx, &mut st, act, failure, Some(kind))?
                }
            }
        }
        WfTask::ActRetry { scheduled_id, .. } => {
            let mut act: ActState =
                tx.get(&act_path(scheduled_id))?.expect("validate() returned true only because this activity exists");
            act.phase = ActPhase::Scheduled;
            tx.put(&act_path(scheduled_id), &act)?;
            dispatch_activity(tx, &st, &act);
        }
        WfTask::Timer { started_id } => {
            tx.delete(&timer_path(started_id))?;
            st.pending_timers -= 1;
            append(tx, &mut st, EventAttrs::TimerFired { started_id })?;
            notify(tx, &mut st)?;
        }
        WfTask::ExecutionTimeout => {
            st.failure = Some(Failure::new("workflow execution timed out").kind("timeout"));
            close(tx, &mut st, WorkflowStatus::TimedOut, EventAttrs::WorkflowTimedOut)?;
        }
        WfTask::Dispatch { .. } | WfTask::ActDispatch { .. } => unreachable!("queue tasks have no timer"),
    }
    tx.put(STATE, &st)
}

pub fn summary(tx: &mut Tx) -> Result<Summary, StorageError> {
    let Some(st) = tx.get::<WfState>(STATE)? else { return Ok(Summary::default()) };
    let open = !st.status.is_closed();
    let newest_first = u64::MAX - st.start_time.micros();
    let suffix = Key(Vec::new()).u64(newest_first).str(&tx.key.id).u128(tx.key.run.0).into_vec();
    Ok(Summary {
        visibility: Some((suffix, encode(&info(tx, &st)))),
        open,
        counted: if open { st.versioning.pinned().cloned() } else { None },
    })
}
