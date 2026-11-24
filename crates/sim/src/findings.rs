//! A permanent record of everything simulation catches.
//!
//! Every failing run appends one JSON line to `findings/dst.jsonl` at the
//! workspace root (override with `WFE_FINDINGS`): the suite, seed and
//! parameters needed to replay it, the failure, and the exact source it ran
//! against. Fixes are noted on the record later (`status`, `note`); records
//! are never deleted, so the file doubles as a regression seed corpus.
//!
//! This module is shell code: it reads the clock and runs `git`. Nothing in
//! a simulation depends on it.
#![expect(
    clippy::disallowed_methods,
    reason = "findings is the non-deterministic shell around a simulation: it reads the wall clock, the environment \
              and files, and runs git. Nothing replayed from a seed calls it."
)]

use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::io::Write;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// Seconds since the Unix epoch.
    pub time: u64,
    pub suite: String,
    pub seed: u64,
    /// Whatever else is needed to replay the run (ticks, options...).
    pub params: String,
    /// The panic message and location.
    pub failure: String,
    /// `git rev-parse HEAD`, or `unborn` before the first commit.
    pub revision: String,
    /// The working tree differed from `revision`.
    pub dirty: bool,
    /// Hash of every source file as it was, so a dirty tree is still named.
    pub source: String,
    /// `open`, or `fixed` once understood.
    pub status: String,
    #[serde(default)]
    pub note: String,
}

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn path() -> PathBuf {
    std::env::var_os("WFE_FINDINGS").map(PathBuf::from).unwrap_or_else(|| workspace_root().join("findings/dst.jsonl"))
}

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).current_dir(workspace_root()).output().ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `(revision, dirty, source hash)` of the workspace.
pub fn revision() -> (String, bool, String) {
    let revision = git(&["rev-parse", "HEAD"]).unwrap_or_else(|| "unborn".into());
    let dirty = git(&["status", "--porcelain"]).is_none_or(|s| !s.is_empty());
    let mut hasher = wfe_core::checksum::Hasher::new();
    if let Some(files) = git(&["ls-files", "-co", "--exclude-standard"]) {
        for file in files.lines().filter(|f| !f.starts_with("findings/")) {
            hasher.update(file.as_bytes());
            hasher.update(&std::fs::read(workspace_root().join(file)).unwrap_or_default());
        }
    }
    (revision, dirty, format!("{:032x}", hasher.finish()))
}

pub fn load() -> Vec<Finding> {
    let text = std::fs::read_to_string(path()).unwrap_or_default();
    text.lines().filter_map(|line| serde_json::from_str(line).ok()).collect()
}

/// Append a finding, unless the same failure of the same run on the same
/// source is already on record.
pub fn record(suite: &str, seed: u64, params: &str, failure: &str) -> Finding {
    let (revision, dirty, source) = revision();
    let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let finding = Finding {
        time,
        suite: suite.into(),
        seed,
        params: params.into(),
        failure: failure.chars().take(2000).collect(),
        revision,
        dirty,
        source,
        status: "open".into(),
        note: String::new(),
    };
    let known = load().into_iter().any(|f| {
        f.suite == finding.suite && f.seed == finding.seed && f.params == finding.params && f.source == finding.source
    });
    if !known {
        let path = path();
        if let Err(e) = append(&path, &finding) {
            panic!(
                "{suite} seed {seed} failed, but the finding could not be appended to {}: {e}\n\
                 Set WFE_FINDINGS to a writable file and rerun the seed. The failure was:\n{failure}",
                path.display()
            );
        }
    }
    finding
}

fn append(path: &Path, finding: &Finding) -> std::io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut file = std::fs::OpenOptions::new().create(true).append(true).open(path)?;
    let line = serde_json::to_string(finding)
        .expect("a Finding holds only strings, integers and a bool, which always serialize to JSON");
    writeln!(file, "{line}")
}

thread_local! {
    static LAST_PANIC: RefCell<Option<String>> = const { RefCell::new(None) };
}

fn install_hook() {
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        let previous = panic::take_hook();
        panic::set_hook(Box::new(move |info| {
            LAST_PANIC.with(|last| *last.borrow_mut() = Some(info.to_string()));
            previous(info);
        }));
    });
}

/// Run one simulation; `Err` carries the failure if it panicked.
pub fn catch<R>(run: impl FnOnce() -> R) -> Result<R, String> {
    install_hook();
    LAST_PANIC.with(|last| *last.borrow_mut() = None);
    panic::catch_unwind(AssertUnwindSafe(run)).map_err(|payload| {
        LAST_PANIC.with(|last| last.borrow_mut().take()).unwrap_or_else(|| payload_message(payload.as_ref()))
    })
}

fn payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic whose payload is not a string".into())
}

/// Run one simulation, record a failure, and let it propagate.
pub fn recorded<R>(suite: &str, seed: u64, params: &str, run: impl FnOnce() -> R) -> R {
    match catch(run) {
        Ok(result) => result,
        Err(failure) => {
            let finding = record(suite, seed, params, &failure);
            panic!("{suite} seed {seed} ({params}) failed, recorded in {}:\n{}", path().display(), finding.failure);
        }
    }
}
