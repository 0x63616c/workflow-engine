//! The fuzzer's long-term record (`wfe dst run --history`): one JSON line
//! per minute of fuzzing, appended as each minute ends and when the fuzzer
//! stops. A restart keeps the record, and the runs-per-minute chart is
//! reloaded from it.
//!
//! Each line names its revision and its session (one fuzzer process, which
//! runs one consecutive range of seeds), so the file answers, for any
//! revision: which seeds passed, how fast they ran, and how many faults and
//! checks they exercised. [`sessions`] folds the minutes into that summary.
#![expect(
    clippy::disallowed_methods,
    reason = "the history is the fuzzer's non-deterministic shell: it reads and appends a file. Nothing replayed \
              from a seed calls it."
)]

use crate::fuzz::{Tally, add};
use crate::world::Report;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

const TAIL_BYTES: u64 = 4 << 20;

/// One minute of one fuzzer session.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Minute {
    /// Unix seconds / 60.
    pub minute: u64,
    pub revision: String,
    pub dirty: bool,
    /// Unix seconds when the session (one fuzzer process) started.
    pub session: u64,
    /// The session's first seed; it runs `first_seed`, `first_seed + 1`, ...
    pub first_seed: u64,
    pub threads: usize,
    pub ticks: u64,
    pub runs: u64,
    pub failures: u64,
    /// Wall time of the runs that finished this minute, summed over threads.
    pub wall_ms: u64,
    pub sim_ms: u64,
    pub wall_ms_p50: u64,
    pub wall_ms_p95: u64,
    pub wall_ms_max: u64,
    pub slowest_seed: u64,
    /// The lowest and highest seed that finished this minute.
    pub seed_min: u64,
    pub seed_max: u64,
    pub failed_seeds: Vec<u64>,
    /// Faults injected, work done and checks run, summed over passing runs.
    pub totals: Report,
    pub scenarios: BTreeMap<String, Tally>,
}

#[derive(Default)]
pub(crate) struct OpenMinute {
    pub(crate) minute: Minute,
    walls: Vec<(u64, u64)>,
}

impl OpenMinute {
    pub(crate) fn new(template: &Minute, minute: u64) -> OpenMinute {
        OpenMinute { minute: Minute { minute, seed_min: u64::MAX, ..template.clone() }, walls: Vec::new() }
    }

    pub(crate) fn add_run(&mut self, seed: u64, wall_ms: u64, scenario: String, report: Option<&Report>) {
        let m = &mut self.minute;
        m.runs += 1;
        m.wall_ms += wall_ms;
        m.seed_min = m.seed_min.min(seed);
        m.seed_max = m.seed_max.max(seed);
        let tally = m.scenarios.entry(scenario).or_default();
        tally.runs += 1;
        match report {
            Some(report) => {
                m.sim_ms += report.sim_ms;
                add(&mut m.totals, report);
            }
            None => {
                m.failures += 1;
                tally.failures += 1;
                m.failed_seeds.push(seed);
            }
        }
        self.walls.push((wall_ms, seed));
    }

    pub(crate) fn close(mut self) -> Minute {
        self.walls.sort_unstable();
        let at = |per_cent: usize| {
            self.walls.get((self.walls.len() * per_cent / 100).min(self.walls.len().saturating_sub(1)))
        };
        let m = &mut self.minute;
        m.wall_ms_p50 = at(50).map_or(0, |w| w.0);
        m.wall_ms_p95 = at(95).map_or(0, |w| w.0);
        (m.wall_ms_max, m.slowest_seed) = self.walls.last().copied().unwrap_or_default();
        if m.runs == 0 {
            m.seed_min = 0;
        }
        self.minute
    }
}

pub fn append(path: &Path, minute: &Minute) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let line = serde_json::to_string(minute).map_err(std::io::Error::other)?;
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    writeln!(file, "{line}")
}

/// Every minute on record. A missing file is an empty history; lines that
/// do not parse (a write cut short by a crash) are skipped.
pub fn load(path: &Path) -> Vec<Minute> {
    parse(&std::fs::read_to_string(path).unwrap_or_default())
}

/// The minutes in the last few MiB of the file: enough for the last hour.
pub fn load_recent(path: &Path) -> Vec<Minute> {
    let Ok(mut file) = std::fs::File::open(path) else { return Vec::new() };
    let len = file.metadata().map_or(0, |m| m.len());
    let from = len.saturating_sub(TAIL_BYTES);
    let mut text = String::new();
    if file.seek(SeekFrom::Start(from)).is_err() || file.read_to_string(&mut text).is_err() {
        return Vec::new();
    }
    let whole_lines = if from == 0 { text.as_str() } else { text.split_once('\n').map_or("", |(_, rest)| rest) };
    parse(whole_lines)
}

fn parse(text: &str) -> Vec<Minute> {
    text.lines().filter_map(|line| serde_json::from_str(line).ok()).collect()
}

/// One fuzzer session, folded from its minutes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Session {
    pub session: u64,
    pub revision: String,
    pub dirty: bool,
    pub threads: usize,
    pub ticks: u64,
    pub first_seed: u64,
    /// The highest seed that finished. Every seed from `first_seed` up to
    /// it ran, except the few in flight when the session stopped.
    pub last_seed: u64,
    /// Unix minutes of the first and last minute on record.
    pub from_minute: u64,
    pub to_minute: u64,
    pub runs: u64,
    pub failures: u64,
    pub failed_seeds: Vec<u64>,
    pub wall_ms: u64,
    pub sim_ms: u64,
    pub wall_ms_max: u64,
    pub slowest_seed: u64,
    pub totals: Report,
    pub scenarios: BTreeMap<String, Tally>,
}

/// Sessions in the order they started.
pub fn sessions(minutes: &[Minute]) -> Vec<Session> {
    let mut by_session: BTreeMap<(u64, u64), Session> = BTreeMap::new();
    for m in minutes {
        let s = by_session.entry((m.session, m.first_seed)).or_insert_with(|| Session {
            session: m.session,
            revision: m.revision.clone(),
            dirty: m.dirty,
            threads: m.threads,
            ticks: m.ticks,
            first_seed: m.first_seed,
            from_minute: m.minute,
            ..Session::default()
        });
        s.from_minute = s.from_minute.min(m.minute);
        s.to_minute = s.to_minute.max(m.minute);
        s.last_seed = s.last_seed.max(m.seed_max);
        s.runs += m.runs;
        s.failures += m.failures;
        s.failed_seeds.extend(&m.failed_seeds);
        s.wall_ms += m.wall_ms;
        s.sim_ms += m.sim_ms;
        if m.wall_ms_max > s.wall_ms_max {
            (s.wall_ms_max, s.slowest_seed) = (m.wall_ms_max, m.slowest_seed);
        }
        add(&mut s.totals, &m.totals);
        for (name, tally) in &m.scenarios {
            let t = s.scenarios.entry(name.clone()).or_default();
            t.runs += tally.runs;
            t.failures += tally.failures;
        }
    }
    by_session.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minute(session: u64, minute: u64, seeds: std::ops::Range<u64>, failed: &[u64]) -> Minute {
        let template = Minute { session, first_seed: 100 * session, revision: "abc".into(), ..Minute::default() };
        let mut open = OpenMinute::new(&template, minute);
        for seed in seeds {
            let report = Report { sim_ms: 1_000, crashes: 1, ..Report::default() };
            let ok = !failed.contains(&seed);
            open.add_run(seed, seed, "1 cell, 1 shard".into(), ok.then_some(&report));
        }
        open.close()
    }

    #[test]
    fn sessions_fold_their_minutes() {
        let minutes = [minute(1, 10, 100..120, &[]), minute(2, 10, 200..205, &[203]), minute(1, 11, 120..140, &[131])];
        assert_eq!(minutes[0].wall_ms_p50, 110);
        assert_eq!(minutes[0].wall_ms_p95, 119);
        assert_eq!((minutes[0].wall_ms_max, minutes[0].slowest_seed), (119, 119));
        let sessions = sessions(&minutes);
        assert_eq!(sessions.len(), 2);
        let first = &sessions[0];
        assert_eq!((first.first_seed, first.last_seed, first.runs, first.failures), (100, 139, 40, 1));
        assert_eq!((first.from_minute, first.to_minute), (10, 11));
        assert_eq!(first.failed_seeds, vec![131]);
        assert_eq!(first.totals.crashes, 39);
        assert_eq!(first.scenarios["1 cell, 1 shard"].runs, 40);
        assert_eq!((sessions[1].runs, sessions[1].failed_seeds.clone()), (5, vec![203]));
    }

    #[test]
    fn a_line_cut_short_by_a_crash_is_skipped() {
        let whole = serde_json::to_string(&minute(1, 10, 100..102, &[])).unwrap();
        let text = format!("{whole}\n{}", &whole[..whole.len() / 2]);
        assert_eq!(parse(&text).len(), 1);
    }
}
