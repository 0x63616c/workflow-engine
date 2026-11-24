//! The workload the simulator runs: workflows, activities, and what each
//! workflow must end up as.
//!
//! Every workflow's outcome is a function of its input, whatever faults it
//! lives through; that is what the oracle in [`crate::world`] checks.

use serde::{Deserialize, Serialize};
use wfe_core::Payload;
use wfe_core::api::*;
use wfe_sdk::workflow::{Either, WfContext, WorkflowError, WorkflowResult, join_all, select, workflow_fn};
use wfe_sdk::{WorkerCore, WorkflowFn};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Kind {
    /// `n` activities in sequence.
    Sum,
    /// `n` activities at once, and a timer.
    Fan,
    /// Waits for `n` signals.
    Signals,
    /// A timer against a slow activity.
    Race,
    /// Continues as new `n` times.
    Countdown,
    /// A recorded side effect.
    Marker,
    /// Waits to be canceled.
    Cancelable,
    /// Waits; gets terminated.
    Terminated,
    /// An activity that fails `n` times first.
    Flaky,
    /// An activity that fails for good.
    Doom,
    /// Cancels a timer.
    TimerCancel,
    /// A large payload, through the codec.
    Blob,
}

pub const KINDS: [Kind; 12] = [
    Kind::Sum,
    Kind::Fan,
    Kind::Signals,
    Kind::Race,
    Kind::Countdown,
    Kind::Marker,
    Kind::Cancelable,
    Kind::Terminated,
    Kind::Flaky,
    Kind::Doom,
    Kind::TimerCancel,
    Kind::Blob,
];

impl Kind {
    pub fn workflow_type(self) -> &'static str {
        match self {
            Kind::Sum => "sum",
            Kind::Fan => "fan",
            Kind::Signals => "signals",
            Kind::Race => "race",
            Kind::Countdown => "countdown",
            Kind::Marker => "marker",
            Kind::Cancelable | Kind::Terminated => "wait",
            Kind::Flaky => "flaky",
            Kind::Doom => "doom",
            Kind::TimerCancel => "timer_cancel",
            Kind::Blob => "blob",
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Input {
    pub n: u64,
    pub base: u64,
}

pub fn out<T: Serialize + ?Sized>(value: &T) -> Payload {
    Payload::json(value).expect("simulator payloads are plain serde types, which always encode as JSON")
}

fn failed(e: impl ToString) -> WorkflowError {
    WorkflowError::Failed(Failure::new(e.to_string()).kind("bad_payload").non_retryable())
}

fn num(payload: &Payload) -> Result<u64, WorkflowError> {
    payload.to_json::<u64>().map_err(failed)
}

fn input(payload: &Payload) -> Result<Input, WorkflowError> {
    payload.to_json::<Input>().map_err(failed)
}

fn activity(activity_type: &str) -> ActivityOptions {
    ActivityOptions {
        activity_type: activity_type.to_string(),
        retry: Some(RetryPolicy {
            initial_interval_ms: 100,
            backoff_percent: 200,
            maximum_interval_ms: 1_000,
            maximum_attempts: 0,
        }),
        start_to_close_ms: 3_000,
        ..ActivityOptions::default()
    }
}

async fn sum(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    let mut x = i.base;
    for _ in 0..i.n {
        x = num(&ctx.activity(activity("add_one"), out(&x)).await?)?;
    }
    Ok(out(&x))
}

async fn fan(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    let branches = (0..i.n).map(|k| ctx.activity(activity("square"), out(&k))).collect();
    let timer = ctx.timer(50);
    let mut total = 0;
    for result in join_all(branches).await {
        total += num(&result?)?;
    }
    timer.await;
    Ok(out(&total))
}

async fn signals(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    let mut total = 0;
    for _ in 0..i.n {
        total += num(&ctx.signal("add").await)?;
    }
    Ok(out(&total))
}

async fn race(ctx: WfContext, _: Payload) -> WorkflowResult {
    match select(ctx.timer(300), ctx.activity(activity("slow"), Payload::null())).await {
        Either::Left(()) => Ok(out("timer")),
        Either::Right(result) => result.map_err(WorkflowError::from),
    }
}

async fn countdown(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    if i.n == 0 {
        return Ok(out("done"));
    }
    let next = num(&ctx.activity(activity("add_one"), out(&i.base)).await?)?;
    Err(WorkflowError::ContinueAsNew(out(&Input { n: i.n - 1, base: next })))
}

async fn marker(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    let picked = ctx.side_effect("pick", || out(&(i.base * 7)));
    ctx.timer(30).await;
    let result = ctx.activity(activity("add_one"), picked).await?;
    Ok(result)
}

async fn wait(ctx: WfContext, _: Payload) -> WorkflowResult {
    match select(ctx.cancelled(), ctx.timer(120_000)).await {
        Either::Left(()) => Err(WorkflowError::Canceled),
        Either::Right(()) => Ok(out("timeout")),
    }
}

async fn flaky(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    Ok(ctx.activity(activity("flaky"), out(&i.n)).await?)
}

async fn doom(ctx: WfContext, _: Payload) -> WorkflowResult {
    Ok(ctx.activity(activity("doom"), Payload::null()).await?)
}

async fn timer_cancel(ctx: WfContext, payload: Payload) -> WorkflowResult {
    let i = input(&payload)?;
    let timer = ctx.timer(60_000);
    let x = ctx.activity(activity("add_one"), out(&i.base)).await?;
    timer.cancel();
    Ok(x)
}

async fn blob(ctx: WfContext, payload: Payload) -> WorkflowResult {
    Ok(ctx.activity(activity("echo"), payload).await?)
}

pub fn workflows() -> Vec<(&'static str, WorkflowFn)> {
    vec![
        ("sum", workflow_fn(sum)),
        ("fan", workflow_fn(fan)),
        ("signals", workflow_fn(signals)),
        ("race", workflow_fn(race)),
        ("countdown", workflow_fn(countdown)),
        ("marker", workflow_fn(marker)),
        ("wait", workflow_fn(wait)),
        ("flaky", workflow_fn(flaky)),
        ("doom", workflow_fn(doom)),
        ("timer_cancel", workflow_fn(timer_cancel)),
        ("blob", workflow_fn(blob)),
    ]
}

pub fn register(core: &mut WorkerCore) {
    for (name, f) in workflows() {
        core.register_workflow(name, f);
    }
}

/// What an activity does. `(result, how many extra ticks it takes)`.
pub fn run_activity(task: &ActivityTask) -> (Result<Payload, Failure>, u64) {
    let number = || task.input.to_json::<u64>().map_err(|e| Failure::new(e.to_string()).non_retryable());
    match task.activity_type.as_str() {
        "add_one" => (number().map(|x| out(&(x + 1))), 0),
        "square" => (number().map(|x| out(&(x * x))), 0),
        "slow" => (Ok(out("activity")), 25),
        "flaky" => match number() {
            Ok(k) if u64::from(task.token.attempt) <= k => (Err(Failure::new("not yet").kind("flaky")), 0),
            other => (other.map(|k| out(&k)), 0),
        },
        "doom" => (Err(Failure::new("doomed").kind("doom").non_retryable()), 0),
        "echo" => (Ok(task.input.clone()), 0),
        other => (Err(Failure::new(format!("no activity {other}")).non_retryable()), 0),
    }
}

/// Is this how a workflow of `kind` may end?
pub fn check_outcome(kind: Kind, i: &Input, plain_input: &Payload, d: &WorkflowDescription) -> Result<(), String> {
    use WorkflowStatus::*;
    let status = d.info.status;
    let result = d.result.as_ref();
    let number = result.and_then(|r| r.to_json::<u64>().ok());
    let text = result.and_then(|r| r.to_json::<String>().ok());
    let long_wait_won_the_race_against_a_fault_delayed_cancel =
        status == Completed && text.as_deref() == Some("timeout");
    let ok = match kind {
        Kind::Sum => status == Completed && number == Some(i.base + i.n),
        Kind::Fan => status == Completed && number == Some((0..i.n).map(|k| k * k).sum()),
        Kind::Signals => status == Completed && number == Some(i.n * (i.n + 1) / 2),
        Kind::Race => status == Completed && matches!(text.as_deref(), Some("timer" | "activity")),
        Kind::Countdown => status == Completed && text.as_deref() == Some("done"),
        Kind::Marker => status == Completed && number == Some(i.base * 7 + 1),
        Kind::Cancelable => status == Canceled || long_wait_won_the_race_against_a_fault_delayed_cancel,
        Kind::Terminated => status == Terminated || long_wait_won_the_race_against_a_fault_delayed_cancel,
        Kind::Flaky => status == Completed && number == Some(i.n),
        Kind::Doom => status == Failed && d.failure.as_ref().is_some_and(|f| f.kind == "doom"),
        Kind::TimerCancel => status == Completed && number == Some(i.base + 1),
        Kind::Blob => status == Completed && result == Some(plain_input),
    };
    if ok { Ok(()) } else { Err(format!("{kind:?} {i:?} ended {status:?} result {result:?} failure {:?}", d.failure)) }
}

/// Structural rules every history obeys.
pub fn check_history(events: &[Event], closed: bool) -> Result<(), String> {
    use EventAttrs::*;
    use std::collections::BTreeMap;
    if events.is_empty() {
        return Err("empty history".into());
    }
    if !matches!(events[0].attrs, WorkflowStarted { .. }) {
        return Err("history does not begin with WorkflowStarted".into());
    }
    #[derive(PartialEq, Debug)]
    enum Task {
        None,
        Scheduled(u64),
        Started(u64, u64),
    }
    let mut task = Task::None;
    let mut closed_by_scheduled_activity: BTreeMap<u64, bool> = BTreeMap::new();
    let mut resolved_and_due_by_started_timer: BTreeMap<u64, (bool, u64)> = BTreeMap::new();
    let mut time = events[0].time;
    for (i, e) in events.iter().enumerate() {
        if e.id != i as u64 + 1 {
            return Err(format!("event {} is at position {}", e.id, i + 1));
        }
        if e.time < time {
            return Err(format!("event {} goes back in time", e.id));
        }
        time = e.time;
        let is_last = i + 1 == events.len();
        let closes = matches!(
            e.attrs,
            WorkflowCompleted { .. }
                | WorkflowFailed { .. }
                | WorkflowCanceled
                | WorkflowTerminated { .. }
                | WorkflowTimedOut
                | WorkflowContinuedAsNew { .. }
        );
        if closes != (is_last && closed) {
            return Err(format!(
                "event {} ({}): a closing event is the last event of a closed workflow",
                e.id,
                e.attrs.name()
            ));
        }
        let wrong = |what: &str| Err(format!("event {} ({}): {what}", e.id, e.attrs.name()));
        match &e.attrs {
            WorkflowStarted { .. } if i != 0 => return wrong("a second start"),
            WorkflowTaskScheduled { .. } => {
                if task != Task::None {
                    return wrong("a workflow task is already open");
                }
                task = Task::Scheduled(e.id);
            }
            WorkflowTaskStarted { scheduled_id, .. } => {
                if task != Task::Scheduled(*scheduled_id) {
                    return wrong("starts a task that is not the scheduled one");
                }
                task = Task::Started(*scheduled_id, e.id);
            }
            WorkflowTaskCompleted { scheduled_id, started_id, .. }
            | WorkflowTaskFailed { scheduled_id, started_id, .. }
            | WorkflowTaskTimedOut { scheduled_id, started_id } => {
                if task != Task::Started(*scheduled_id, *started_id) {
                    return wrong("closes a task that is not the started one");
                }
                task = Task::None;
            }
            ActivityScheduled { .. } => {
                closed_by_scheduled_activity.insert(e.id, false);
            }
            ActivityStarted { scheduled_id, .. } => {
                if closed_by_scheduled_activity.get(scheduled_id) != Some(&false) {
                    return wrong("starts an activity that is not open");
                }
            }
            ActivityCompleted { scheduled_id, .. }
            | ActivityFailed { scheduled_id, .. }
            | ActivityTimedOut { scheduled_id, .. } => match closed_by_scheduled_activity.get_mut(scheduled_id) {
                Some(done) if !*done => *done = true,
                _ => return wrong("closes an activity that is not open"),
            },
            TimerStarted { fire_after_ms } => {
                resolved_and_due_by_started_timer.insert(e.id, (false, e.time.add_ms(*fire_after_ms).micros()));
            }
            TimerFired { started_id } => match resolved_and_due_by_started_timer.get_mut(started_id) {
                Some((done, due)) if !*done => {
                    if e.time.micros() < *due {
                        return wrong("fires early");
                    }
                    *done = true;
                }
                _ => return wrong("fires a timer that is not running"),
            },
            TimerCanceled { started_id } => match resolved_and_due_by_started_timer.get_mut(started_id) {
                Some((done, _)) if !*done => *done = true,
                _ => return wrong("cancels a timer that is not running"),
            },
            WorkflowStarted { .. }
            | MarkerRecorded { .. }
            | SignalReceived { .. }
            | CancelRequested { .. }
            | VersioningOverrideChanged { .. }
            | WorkflowCompleted { .. }
            | WorkflowFailed { .. }
            | WorkflowCanceled
            | WorkflowTerminated { .. }
            | WorkflowTimedOut
            | WorkflowContinuedAsNew { .. } => {}
        }
    }
    Ok(())
}

/// The versions that completed a history's workflow tasks, in order.
pub fn task_versions(events: &[Event]) -> Vec<Option<DeploymentVersion>> {
    events
        .iter()
        .filter_map(|e| match &e.attrs {
            EventAttrs::WorkflowTaskCompleted { version, .. } => Some(version.clone()),
            EventAttrs::WorkflowStarted { .. }
            | EventAttrs::WorkflowTaskScheduled { .. }
            | EventAttrs::WorkflowTaskStarted { .. }
            | EventAttrs::WorkflowTaskFailed { .. }
            | EventAttrs::WorkflowTaskTimedOut { .. }
            | EventAttrs::ActivityScheduled { .. }
            | EventAttrs::ActivityStarted { .. }
            | EventAttrs::ActivityCompleted { .. }
            | EventAttrs::ActivityFailed { .. }
            | EventAttrs::ActivityTimedOut { .. }
            | EventAttrs::TimerStarted { .. }
            | EventAttrs::TimerFired { .. }
            | EventAttrs::TimerCanceled { .. }
            | EventAttrs::MarkerRecorded { .. }
            | EventAttrs::SignalReceived { .. }
            | EventAttrs::CancelRequested { .. }
            | EventAttrs::VersioningOverrideChanged { .. }
            | EventAttrs::WorkflowCompleted { .. }
            | EventAttrs::WorkflowFailed { .. }
            | EventAttrs::WorkflowCanceled
            | EventAttrs::WorkflowTerminated { .. }
            | EventAttrs::WorkflowTimedOut
            | EventAttrs::WorkflowContinuedAsNew { .. } => None,
        })
        .collect()
}
