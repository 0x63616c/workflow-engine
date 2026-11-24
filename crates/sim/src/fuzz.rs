//! The always-on fuzzer: simulate new seeds forever, on every core.
//!
//! Each worker thread takes the next seed, runs the whole-system
//! simulation, and adds the outcome to shared statistics. Failures go to
//! the findings log with the source revision that produced them. A
//! [`FuzzStatus`] snapshot is what the management UI shows.
#![expect(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    reason = "the fuzzer is the non-deterministic driver around the replayed world: it reads the wall clock, runs \
              worker threads that share statistics through a Mutex, and appends to a log file. Each seed it runs \
              is still replayed deterministically by world::run."
)]

use crate::findings::{self, Finding};
use crate::history::{self, Minute, OpenMinute};
use crate::world::{self, Options, Report};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

const TYPEABLE_SEED_LIMIT: u64 = 1_000_000_000_000;
const RECENT_RUNS_KEPT: usize = 100;
const RECENT_FAILURES_KEPT: usize = 50;
const MINUTES_KEPT: usize = 60;

#[derive(Clone, Debug)]
pub struct FuzzConfig {
    pub threads: usize,
    /// Simulated ticks (10 ms) of workload per run.
    pub ticks: u64,
    /// `None`: start from a random seed.
    pub first_seed: Option<u64>,
    /// `None`: run until stopped.
    pub max_runs: Option<u64>,
    /// Append one JSON line per run here.
    pub log: Option<PathBuf>,
    /// Append one JSON line per minute here (see [`crate::history`]), and
    /// reload the last hour of runs per minute from it on start.
    pub history: Option<PathBuf>,
}

impl Default for FuzzConfig {
    fn default() -> FuzzConfig {
        FuzzConfig {
            threads: std::thread::available_parallelism().map_or(1, |n| n.get()),
            ticks: 12_000,
            first_seed: None,
            max_runs: None,
            log: None,
            history: None,
        }
    }
}

/// One finished simulation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RunRecord {
    pub seed: u64,
    /// Unix seconds when it finished.
    pub time: u64,
    pub options: Options,
    pub ok: bool,
    pub wall_ms: u64,
    #[serde(default)]
    pub failure: Option<String>,
    #[serde(default)]
    pub report: Option<Report>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Tally {
    pub runs: u64,
    pub failures: u64,
}

/// A snapshot of the fuzzer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FuzzStatus {
    pub started_at: u64,
    pub now: u64,
    pub revision: String,
    pub dirty: bool,
    pub threads: usize,
    pub ticks: u64,
    pub first_seed: u64,
    pub running: bool,
    pub runs: u64,
    pub failures: u64,
    /// Simulated time covered, and the wall time it took (summed over
    /// threads).
    pub sim_ms: u64,
    pub wall_ms: u64,
    pub totals: Report,
    /// Runs and failures by scenario (`2 cells, failover`...).
    pub scenarios: BTreeMap<String, Tally>,
    pub in_flight: Vec<u64>,
    /// Newest first.
    pub recent: Vec<RunRecord>,
    pub recent_failures: Vec<RunRecord>,
    /// Runs finished in each of the last sixty minutes, oldest first.
    pub per_minute: Vec<u64>,
    pub findings: Vec<Finding>,
    /// The last error writing the per-run log (`FuzzConfig::log`); fuzzing goes on without it.
    #[serde(default)]
    pub log_error: Option<String>,
    /// The last error appending to the history (`FuzzConfig::history`); fuzzing goes on without it.
    #[serde(default)]
    pub history_error: Option<String>,
}

#[derive(Default)]
struct State {
    status: FuzzStatus,
    in_flight: BTreeMap<usize, u64>,
    recent: VecDeque<RunRecord>,
    recent_failures: VecDeque<RunRecord>,
    minutes: VecDeque<(u64, u64)>,
    history: Option<PathBuf>,
    session: Minute,
    open_minute: Option<OpenMinute>,
}

impl State {
    fn add_to_minute(&mut self, minute: u64, seed: u64, wall_ms: u64, scenario: String, report: Option<&Report>) {
        if self.open_minute.as_ref().is_some_and(|open| open.minute.minute != minute) {
            self.close_minute();
        }
        let session = &self.session;
        self.open_minute
            .get_or_insert_with(|| OpenMinute::new(session, minute))
            .add_run(seed, wall_ms, scenario, report);
    }

    fn close_minute(&mut self) {
        let Some(open) = self.open_minute.take() else { return };
        let Some(path) = &self.history else { return };
        if let Err(e) = history::append(path, &open.close()) {
            self.status.history_error = Some(format!("appending to {}: {e}", path.display()));
        }
    }
}

fn minutes_on_record(path: &Path) -> VecDeque<(u64, u64)> {
    let since = (unix() / 60).saturating_sub(MINUTES_KEPT as u64);
    let mut runs: BTreeMap<u64, u64> = BTreeMap::new();
    for m in history::load_recent(path).into_iter().filter(|m| m.minute > since) {
        *runs.entry(m.minute).or_insert(0) += m.runs;
    }
    runs.into_iter().collect()
}

pub struct Fuzzer {
    state: Arc<Mutex<State>>,
    stop: Arc<AtomicBool>,
    threads: Vec<std::thread::JoinHandle<()>>,
}

fn unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs())
}

fn random_first_seed() -> u64 {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs().wrapping_mul(1_000_000_000).wrapping_add(u64::from(d.subsec_nanos())));
    (nanos ^ (u64::from(std::process::id()) << 40)) % TYPEABLE_SEED_LIMIT
}

fn locked<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().expect(
        "a fuzz worker panicked while holding a fuzzer lock; simulations run outside the locks, so that panic \
         (printed above) is a fuzzer bug",
    )
}

fn open_log(path: &Path) -> std::fs::File {
    let opened = path
        .parent()
        .map_or(Ok(()), std::fs::create_dir_all)
        .and_then(|()| std::fs::OpenOptions::new().create(true).append(true).open(path));
    opened.unwrap_or_else(|e| {
        panic!("the fuzz run log {} cannot be opened for appending: {e}. Pass a writable log path.", path.display())
    })
}

fn scenario(o: &Options) -> String {
    let cells = if o.cells == 1 { "1 cell".to_string() } else { format!("{} cells", o.cells) };
    let mut s = format!("{cells}, {} shard{}", o.shards, if o.shards == 1 { "" } else { "s" });
    if o.failover {
        s += ", failover";
    }
    if !o.faults {
        s += ", no faults";
    }
    s
}

pub(crate) fn add(total: &mut Report, r: &Report) {
    total.ticks += r.ticks;
    total.sim_ms += r.sim_ms;
    total.workflows += r.workflows;
    total.events += r.events;
    total.ops += r.ops;
    total.messages += r.messages;
    total.messages_dropped += r.messages_dropped;
    total.crashes += r.crashes;
    total.partitions += r.partitions;
    total.gateway_restarts += r.gateway_restarts;
    total.worker_restarts += r.worker_restarts;
    total.view_changes += r.view_changes;
    total.workflow_tasks += r.workflow_tasks;
    total.activities += r.activities;
    total.replicated += r.replicated;
    total.failovers += r.failovers;
    total.rollouts += r.rollouts;
    total.builds_retired += r.builds_retired;
    total.controller_rounds += r.controller_rounds;
    total.controller_interruptions += r.controller_interruptions;
    total.controller_stale_plans += r.controller_stale_plans;
    total.claim_checks += r.claim_checks;
    for (map, from) in
        [(&mut total.by_kind, &r.by_kind), (&mut total.by_status, &r.by_status), (&mut total.checks, &r.checks)]
    {
        for (k, v) in from {
            *map.entry(k.clone()).or_insert(0) += v;
        }
    }
}

impl Fuzzer {
    pub fn start(config: FuzzConfig) -> Fuzzer {
        let first_seed = config.first_seed.unwrap_or_else(random_first_seed);
        let (revision, dirty, _) = findings::revision();
        let started_at = unix();
        let session = Minute {
            revision: revision.clone(),
            dirty,
            session: started_at,
            first_seed,
            threads: config.threads,
            ticks: config.ticks,
            ..Minute::default()
        };
        let state = Arc::new(Mutex::new(State {
            minutes: config.history.as_deref().map(minutes_on_record).unwrap_or_default(),
            history: config.history.clone(),
            session,
            status: FuzzStatus {
                started_at,
                revision,
                dirty,
                threads: config.threads,
                ticks: config.ticks,
                first_seed,
                running: true,
                ..FuzzStatus::default()
            },
            ..State::default()
        }));
        let stop = Arc::new(AtomicBool::new(false));
        let next = Arc::new(AtomicU64::new(0));
        let log = Arc::new(Mutex::new(config.log.as_deref().map(open_log)));
        let threads = (0..config.threads)
            .map(|thread| {
                let (state, stop, next, log, config) =
                    (state.clone(), stop.clone(), next.clone(), log.clone(), config.clone());
                std::thread::Builder::new()
                    .name(format!("fuzz-{thread}"))
                    .stack_size(16 << 20)
                    .spawn(move || {
                        while !stop.load(Ordering::Relaxed) {
                            let n = next.fetch_add(1, Ordering::Relaxed);
                            if config.max_runs.is_some_and(|max| n >= max) {
                                break;
                            }
                            let seed = first_seed.wrapping_add(n);
                            let options = Options::for_seed(seed, config.ticks);
                            locked(&state).in_flight.insert(thread, seed);
                            let started = Instant::now();
                            let outcome = findings::catch(|| world::run(options.clone()));
                            let wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                            let record = match outcome {
                                Ok(report) => RunRecord {
                                    seed,
                                    time: unix(),
                                    options,
                                    ok: true,
                                    wall_ms,
                                    failure: None,
                                    report: Some(report),
                                },
                                Err(failure) => {
                                    let _one_findings_writer_at_a_time = locked(&log);
                                    let finding = findings::record("world", seed, &options.describe(), &failure);
                                    RunRecord {
                                        seed,
                                        time: unix(),
                                        options,
                                        ok: false,
                                        wall_ms,
                                        failure: Some(finding.failure),
                                        report: None,
                                    }
                                }
                            };
                            let log_error = locked(&log).as_mut().and_then(|file| {
                                let line = serde_json::to_string(&record).expect(
                                    "a RunRecord holds only strings, integers, bools and maps with string keys, \
                                     which always serialize to JSON",
                                );
                                writeln!(file, "{line}").err().map(|e| format!("writing run {seed}: {e}"))
                            });
                            let mut state = locked(&state);
                            if log_error.is_some() {
                                state.status.log_error = log_error;
                            }
                            state.in_flight.remove(&thread);
                            let s = &mut state.status;
                            s.runs += 1;
                            s.wall_ms += wall_ms;
                            let scenario = scenario(&record.options);
                            let tally = s.scenarios.entry(scenario.clone()).or_default();
                            tally.runs += 1;
                            match &record.report {
                                Some(report) => {
                                    s.sim_ms += report.sim_ms;
                                    add(&mut s.totals, report);
                                }
                                None => {
                                    s.failures += 1;
                                    tally.failures += 1;
                                }
                            }
                            let minute = record.time / 60;
                            state.add_to_minute(minute, seed, wall_ms, scenario, record.report.as_ref());
                            match state.minutes.back_mut() {
                                Some((m, count)) if *m == minute => *count += 1,
                                _ => state.minutes.push_back((minute, 1)),
                            }
                            while state.minutes.len() > MINUTES_KEPT {
                                state.minutes.pop_front();
                            }
                            if !record.ok {
                                state.recent_failures.push_front(record.clone());
                                state.recent_failures.truncate(RECENT_FAILURES_KEPT);
                            }
                            state.recent.push_front(record);
                            state.recent.truncate(RECENT_RUNS_KEPT);
                        }
                    })
                    .expect("the OS can start a fuzz worker thread with a 16 MiB stack")
            })
            .collect();
        Fuzzer { state, stop, threads }
    }

    pub fn status(&self) -> FuzzStatus {
        let state = locked(&self.state);
        let mut status = state.status.clone();
        status.now = unix();
        status.running = !self.stop.load(Ordering::Relaxed) && self.threads.iter().any(|t| !t.is_finished());
        status.in_flight = state.in_flight.values().copied().collect();
        status.recent = state.recent.iter().cloned().collect();
        status.recent_failures = state.recent_failures.iter().cloned().collect();
        let this_minute = status.now / 60;
        status.per_minute = (0..60)
            .map(|i| {
                let minute = this_minute - 59 + i;
                state.minutes.iter().find(|(m, _)| *m == minute).map_or(0, |(_, n)| *n)
            })
            .collect();
        drop(state);
        status.findings = findings::load();
        status
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Append the minute still collecting runs to the history. Call once the
    /// workers have finished; `join` does.
    pub fn flush_history(&self) {
        locked(&self.state).close_minute();
    }

    /// Wait for the worker threads (after `stop`, or when `max_runs` is
    /// reached).
    pub fn join(mut self) -> FuzzStatus {
        for thread in self.threads.drain(..) {
            if let Err(panic) = thread.join() {
                std::panic::resume_unwind(panic);
            }
        }
        self.flush_history();
        self.status()
    }
}
