//! Writing workflows, and replaying them.
//!
//! A workflow is an `async fn(WfContext, Payload) -> WorkflowResult`. It
//! never does I/O itself: everything it awaits comes from its
//! [`WfContext`] (activities, timers, signals), and resolves from the
//! workflow's event history. That makes it deterministic, which is what
//! lets [`replay`] rebuild its state from the history on any worker, any
//! number of times, and get the same decisions.
//!
//! The futures here do not use wakers: the workflow is polled once per
//! workflow task, after the task's events have been applied. Combine them
//! with [`join`], [`join_all`] and [`select`], which poll every branch;
//! combinators that rely on wake-ups (`FuturesUnordered`, channels, real
//! timers) do not work inside a workflow.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use wfe_core::api::*;
use wfe_core::{Id128, Payload, Timestamp};

/// How a workflow ends, other than by returning a result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkflowError {
    Failed(Failure),
    /// The workflow honors a cancellation request.
    Canceled,
    /// Close this run and start a fresh one with a new input and an empty
    /// history.
    ContinueAsNew(Payload),
}

impl From<Failure> for WorkflowError {
    fn from(failure: Failure) -> WorkflowError {
        WorkflowError::Failed(failure)
    }
}

pub type WorkflowResult = Result<Payload, WorkflowError>;
pub type WorkflowFuture = Pin<Box<dyn Future<Output = WorkflowResult>>>;
pub type WorkflowFn = Arc<dyn Fn(WfContext, Payload) -> WorkflowFuture + Send + Sync>;

/// Wrap an `async fn` as a registrable workflow.
pub fn workflow_fn<F, Fut>(f: F) -> WorkflowFn
where
    F: Fn(WfContext, Payload) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WorkflowResult> + 'static,
{
    Arc::new(move |ctx, input| Box::pin(f(ctx, input)))
}

#[derive(Clone, Debug, Default)]
pub struct WorkflowInfo {
    pub workflow_id: String,
    pub run_id: Id128,
    pub workflow_type: String,
    pub task_queue: String,
}

#[derive(Default)]
struct ActivitySlot {
    event_id: Option<u64>,
    result: Option<Result<Payload, Failure>>,
}

#[derive(Default)]
struct TimerSlot {
    event_id: Option<u64>,
    fired: bool,
    canceled: bool,
}

#[derive(Default)]
struct Inner {
    info: WorkflowInfo,
    now: Timestamp,
    replaying: bool,
    activities: Vec<ActivitySlot>,
    activity_by_event: BTreeMap<u64, usize>,
    timers: Vec<TimerSlot>,
    timer_by_event: BTreeMap<u64, usize>,
    signals: BTreeMap<String, VecDeque<Payload>>,
    cancel_requested: bool,
    recorded_markers: VecDeque<(String, Payload)>,
    commands: Vec<Command>,
    slot_created_by_command: Vec<Created>,
}

#[derive(Clone, Copy)]
enum Created {
    Activity(usize),
    Timer(usize),
    Other,
}

/// A workflow's window on the world.
#[derive(Clone)]
pub struct WfContext {
    inner: Rc<RefCell<Inner>>,
}

impl WfContext {
    pub fn info(&self) -> WorkflowInfo {
        self.inner.borrow().info.clone()
    }

    /// The workflow's clock: the time of the workflow task being processed.
    pub fn now(&self) -> Timestamp {
        self.inner.borrow().now
    }

    /// True while re-running decisions that are already in the history.
    /// Use only to silence logging.
    pub fn is_replaying(&self) -> bool {
        self.inner.borrow().replaying
    }

    /// Run an activity and wait for its result.
    pub fn activity(&self, options: ActivityOptions, input: Payload) -> ActivityFuture {
        let mut inner = self.inner.borrow_mut();
        let slot = inner.activities.len();
        inner.activities.push(ActivitySlot::default());
        inner.commands.push(Command::ScheduleActivity { options, input });
        inner.slot_created_by_command.push(Created::Activity(slot));
        ActivityFuture { inner: self.inner.clone(), slot }
    }

    /// An activity by type name, with default options.
    pub fn call(&self, activity_type: &str, input: Payload) -> ActivityFuture {
        self.activity(ActivityOptions { activity_type: activity_type.to_string(), ..ActivityOptions::default() }, input)
    }

    /// A durable timer.
    pub fn timer(&self, ms: u64) -> TimerFuture {
        let mut inner = self.inner.borrow_mut();
        let slot = inner.timers.len();
        inner.timers.push(TimerSlot::default());
        inner.commands.push(Command::StartTimer { fire_after_ms: ms });
        inner.slot_created_by_command.push(Created::Timer(slot));
        TimerFuture { inner: self.inner.clone(), slot }
    }

    /// The next signal named `name` that has not been received yet.
    pub fn signal(&self, name: &str) -> SignalFuture {
        SignalFuture { inner: self.inner.clone(), name: name.to_string() }
    }

    /// A signal named `name` if one is waiting.
    pub fn try_signal(&self, name: &str) -> Option<Payload> {
        self.inner.borrow_mut().signals.get_mut(name).and_then(VecDeque::pop_front)
    }

    pub fn is_cancel_requested(&self) -> bool {
        self.inner.borrow().cancel_requested
    }

    /// Resolves when cancellation of the workflow is requested.
    pub fn cancelled(&self) -> CancelFuture {
        CancelFuture { inner: self.inner.clone() }
    }

    /// Record the result of a non-deterministic computation (a random
    /// number, a wall-clock read, a feature flag). `f` runs once; replays
    /// return the recorded value.
    pub fn side_effect(&self, name: &str, f: impl FnOnce() -> Payload) -> Payload {
        let mut inner = self.inner.borrow_mut();
        let details = match inner.recorded_markers.pop_front() {
            Some((_, details)) => details,
            None => f(),
        };
        inner.commands.push(Command::RecordMarker { name: name.to_string(), details: details.clone() });
        inner.slot_created_by_command.push(Created::Other);
        details
    }
}

pub struct ActivityFuture {
    inner: Rc<RefCell<Inner>>,
    slot: usize,
}

impl Future for ActivityFuture {
    type Output = Result<Payload, Failure>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.borrow_mut().activities[self.slot].result.take() {
            Some(result) => Poll::Ready(result),
            None => Poll::Pending,
        }
    }
}

pub struct TimerFuture {
    inner: Rc<RefCell<Inner>>,
    slot: usize,
}

impl TimerFuture {
    /// Stop the timer. Awaiting it afterwards never resolves.
    ///
    /// A timer started in this same activation has no event yet, so the cell
    /// is not told: it fires unobserved.
    pub fn cancel(&self) {
        let mut inner = self.inner.borrow_mut();
        let slot = &mut inner.timers[self.slot];
        if slot.fired || slot.canceled {
            return;
        }
        slot.canceled = true;
        if let Some(started_id) = slot.event_id {
            inner.commands.push(Command::CancelTimer { started_id });
            inner.slot_created_by_command.push(Created::Other);
        }
    }
}

impl Future for TimerFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        let inner = self.inner.borrow();
        let slot = &inner.timers[self.slot];
        if slot.fired && !slot.canceled { Poll::Ready(()) } else { Poll::Pending }
    }
}

pub struct SignalFuture {
    inner: Rc<RefCell<Inner>>,
    name: String,
}

impl Future for SignalFuture {
    type Output = Payload;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Payload> {
        match self.inner.borrow_mut().signals.get_mut(&self.name).and_then(VecDeque::pop_front) {
            Some(payload) => Poll::Ready(payload),
            None => Poll::Pending,
        }
    }
}

pub struct CancelFuture {
    inner: Rc<RefCell<Inner>>,
}

impl Future for CancelFuture {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.inner.borrow().cancel_requested { Poll::Ready(()) } else { Poll::Pending }
    }
}

/// Wait for every future; results in order.
pub fn join_all<F: Future + Unpin>(futures: Vec<F>) -> JoinAll<F> {
    let results = futures.iter().map(|_| None).collect();
    JoinAll { futures: futures.into_iter().map(Some).collect(), results }
}

pub struct JoinAll<F: Future> {
    futures: Vec<Option<F>>,
    results: Vec<Option<F::Output>>,
}

impl<F: Future> Unpin for JoinAll<F> {}

impl<F: Future + Unpin> Future for JoinAll<F> {
    type Output = Vec<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        for (future, result) in this.futures.iter_mut().zip(this.results.iter_mut()) {
            if let Some(f) = future
                && let Poll::Ready(value) = Pin::new(f).poll(cx)
            {
                *result = Some(value);
                *future = None;
            }
        }
        if this.results.iter().all(Option::is_some) {
            Poll::Ready(this.results.iter_mut().filter_map(Option::take).collect())
        } else {
            Poll::Pending
        }
    }
}

/// Wait for both.
pub async fn join<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    let (mut ra, mut rb) = (None, None);
    std::future::poll_fn(move |cx| {
        if ra.is_none()
            && let Poll::Ready(v) = a.as_mut().poll(cx)
        {
            ra = Some(v);
        }
        if rb.is_none()
            && let Poll::Ready(v) = b.as_mut().poll(cx)
        {
            rb = Some(v);
        }
        match (ra.take(), rb.take()) {
            (Some(a), Some(b)) => Poll::Ready((a, b)),
            (a, b) => {
                ra = a;
                rb = b;
                Poll::Pending
            }
        }
    })
    .await
}

pub enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Whichever finishes first; `a` wins a tie.
pub async fn select<A: Future, B: Future>(a: A, b: B) -> Either<A::Output, B::Output> {
    let mut a = Box::pin(a);
    let mut b = Box::pin(b);
    std::future::poll_fn(move |cx| {
        if let Poll::Ready(v) = a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(v));
        }
        if let Poll::Ready(v) = b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(v));
        }
        Poll::Pending
    })
    .await
}

fn nondeterminism(message: String) -> Failure {
    Failure::new(message).kind("nondeterminism").non_retryable()
}

fn apply_inbound(inner: &mut Inner, event: &Event) {
    match &event.attrs {
        EventAttrs::ActivityCompleted { scheduled_id, result } => {
            if let Some(slot) = inner.activity_by_event.get(scheduled_id) {
                inner.activities[*slot].result = Some(Ok(result.clone()));
            }
        }
        EventAttrs::ActivityFailed { scheduled_id, failure } => {
            if let Some(slot) = inner.activity_by_event.get(scheduled_id) {
                inner.activities[*slot].result = Some(Err(failure.clone()));
            }
        }
        EventAttrs::ActivityTimedOut { scheduled_id, kind } => {
            if let Some(slot) = inner.activity_by_event.get(scheduled_id) {
                let failure = Failure::new(format!("activity timed out ({kind:?})")).kind("timeout");
                inner.activities[*slot].result = Some(Err(failure));
            }
        }
        EventAttrs::TimerFired { started_id } => {
            if let Some(slot) = inner.timer_by_event.get(started_id) {
                inner.timers[*slot].fired = true;
            }
        }
        EventAttrs::TimerCanceled { started_id } => {
            if let Some(slot) = inner.timer_by_event.get(started_id) {
                inner.timers[*slot].canceled = true;
            }
        }
        EventAttrs::SignalReceived { name, input } => {
            inner.signals.entry(name.clone()).or_default().push_back(input.clone());
        }
        EventAttrs::CancelRequested { .. } => inner.cancel_requested = true,
        EventAttrs::WorkflowStarted { .. }
        | EventAttrs::WorkflowTaskScheduled { .. }
        | EventAttrs::WorkflowTaskStarted { .. }
        | EventAttrs::WorkflowTaskCompleted { .. }
        | EventAttrs::WorkflowTaskFailed { .. }
        | EventAttrs::WorkflowTaskTimedOut { .. }
        | EventAttrs::ActivityScheduled { .. }
        | EventAttrs::ActivityStarted { .. }
        | EventAttrs::TimerStarted { .. }
        | EventAttrs::MarkerRecorded { .. }
        | EventAttrs::VersioningOverrideChanged { .. }
        | EventAttrs::WorkflowCompleted { .. }
        | EventAttrs::WorkflowFailed { .. }
        | EventAttrs::WorkflowCanceled
        | EventAttrs::WorkflowTerminated { .. }
        | EventAttrs::WorkflowTimedOut
        | EventAttrs::WorkflowContinuedAsNew { .. } => {}
    }
}

fn is_command_event(attrs: &EventAttrs) -> bool {
    matches!(
        attrs,
        EventAttrs::ActivityScheduled { .. } | EventAttrs::TimerStarted { .. } | EventAttrs::MarkerRecorded { .. }
    )
}

/// Run `workflow` against `history` and return the commands of its latest
/// workflow task (the one whose `WorkflowTaskStarted` ends the history).
pub fn replay(workflow: &WorkflowFn, info: WorkflowInfo, history: &[Event]) -> Result<Vec<Command>, Failure> {
    let Some(Event { attrs: EventAttrs::WorkflowStarted { input, .. }, .. }) = history.first() else {
        return Err(nondeterminism("history does not begin with WorkflowStarted".into()));
    };
    let inner = Rc::new(RefCell::new(Inner { info, ..Inner::default() }));
    let ctx = WfContext { inner: inner.clone() };
    let mut future = workflow(ctx, input.clone());
    let mut outcome: Option<WorkflowResult> = None;
    let mut cx = Context::from_waker(Waker::noop());

    let mut completion_index_by_started_id: BTreeMap<u64, usize> = BTreeMap::new();
    let mut abandoned_started_ids: Vec<u64> = Vec::new();
    for (i, event) in history.iter().enumerate() {
        if let EventAttrs::WorkflowTaskCompleted { started_id, .. } = &event.attrs {
            completion_index_by_started_id.insert(*started_id, i);
        }
        if let EventAttrs::WorkflowTaskFailed { started_id, .. } | EventAttrs::WorkflowTaskTimedOut { started_id, .. } =
            &event.attrs
        {
            abandoned_started_ids.push(*started_id);
        }
    }

    let mut not_yet_shown_to_workflow: Vec<&Event> = Vec::new();
    let mut last_commands = Vec::new();
    for event in history {
        let EventAttrs::WorkflowTaskStarted { .. } = &event.attrs else {
            not_yet_shown_to_workflow.push(event);
            continue;
        };
        let decisions_were_never_recorded = abandoned_started_ids.contains(&event.id);
        if decisions_were_never_recorded {
            continue;
        }
        let recorded: Option<Vec<&Event>> = completion_index_by_started_id.get(&event.id).map(|at| {
            history[at + 1..]
                .iter()
                .take_while(|e| {
                    is_command_event(&e.attrs)
                        || is_close(&e.attrs)
                        || matches!(e.attrs, EventAttrs::TimerCanceled { .. })
                })
                .collect()
        });
        {
            let mut inner = inner.borrow_mut();
            inner.now = event.time;
            inner.replaying = recorded.is_some();
            inner.commands.clear();
            inner.slot_created_by_command.clear();
            inner.recorded_markers = recorded
                .iter()
                .flatten()
                .filter_map(|e| {
                    if let EventAttrs::MarkerRecorded { name, details } = &e.attrs {
                        Some((name.clone(), details.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            for event in not_yet_shown_to_workflow.drain(..) {
                apply_inbound(&mut inner, event);
            }
        }
        if outcome.is_none()
            && let Poll::Ready(result) = future.as_mut().poll(&mut cx)
        {
            outcome = Some(result);
        }
        let mut inner = inner.borrow_mut();
        let Some(recorded) = recorded else {
            last_commands = std::mem::take(&mut inner.commands);
            if let Some(result) = &outcome {
                last_commands.push(match result {
                    Ok(result) => Command::CompleteWorkflow { result: result.clone() },
                    Err(WorkflowError::Failed(failure)) => Command::FailWorkflow { failure: failure.clone() },
                    Err(WorkflowError::Canceled) => Command::CancelWorkflow,
                    Err(WorkflowError::ContinueAsNew(input)) => Command::ContinueAsNew { input: input.clone() },
                });
            }
            continue;
        };
        let created: Vec<(Command, Created)> = inner
            .commands
            .iter()
            .cloned()
            .zip(inner.slot_created_by_command.iter().copied())
            .filter(|(c, _)| !matches!(c, Command::CancelTimer { .. }))
            .collect();
        let events: Vec<&&Event> = recorded.iter().filter(|e| is_command_event(&e.attrs)).collect();
        if created.len() != events.len() {
            return Err(nondeterminism(format!(
                "workflow task at event {} recorded {} commands, the code now produces {}",
                event.id,
                events.len(),
                created.len()
            )));
        }
        for ((command, slot), event) in created.iter().zip(events) {
            let matches = match (command, &event.attrs) {
                (
                    Command::ScheduleActivity { options, .. },
                    EventAttrs::ActivityScheduled { options: recorded, .. },
                ) => options.activity_type == recorded.activity_type,
                (Command::StartTimer { .. }, EventAttrs::TimerStarted { .. }) => true,
                (Command::RecordMarker { name, .. }, EventAttrs::MarkerRecorded { name: recorded, .. }) => {
                    name == recorded
                }
                _ => false,
            };
            if !matches {
                return Err(nondeterminism(format!(
                    "event {} is {}, but the code now produces {command:?}",
                    event.id,
                    event.attrs.name()
                )));
            }
            match slot {
                Created::Activity(i) => {
                    inner.activities[*i].event_id = Some(event.id);
                    inner.activity_by_event.insert(event.id, *i);
                }
                Created::Timer(i) => {
                    inner.timers[*i].event_id = Some(event.id);
                    inner.timer_by_event.insert(event.id, *i);
                }
                Created::Other => {}
            }
        }
    }
    Ok(last_commands)
}

fn is_close(attrs: &EventAttrs) -> bool {
    matches!(
        attrs,
        EventAttrs::WorkflowCompleted { .. }
            | EventAttrs::WorkflowFailed { .. }
            | EventAttrs::WorkflowCanceled
            | EventAttrs::WorkflowContinuedAsNew { .. }
    )
}
