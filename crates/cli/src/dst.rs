use crate::output::{Output, duration_ms, time};
use crate::{Done, Failed, usage};
use clap::Subcommand;
use serde_json::json;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use wfe_core::{Error, Timestamp};
use wfe_sim::findings;
use wfe_sim::fuzz::{FuzzConfig, FuzzStatus, Fuzzer};
use wfe_sim::history::{self, Session};
use wfe_sim::world::{self, Options};

#[derive(Subcommand)]
pub(crate) enum DstCommand {
    /// Simulate seed after seed on every core.
    ///
    /// This is what the always-on box runs. Failures land in the findings log, and `--serve` exposes the
    /// live status for the management UI.
    Run {
        /// Defaults to the number of cores.
        #[arg(long)]
        threads: Option<usize>,
        /// Simulated ticks (10 ms each) of workload per run.
        #[arg(long, default_value_t = 12_000)]
        ticks: u64,
        /// The first seed; random when omitted.
        #[arg(long)]
        seed: Option<u64>,
        /// Stop after this many runs; run forever when omitted.
        #[arg(long)]
        runs: Option<u64>,
        /// Serve the live status as JSON here (`GET /status`).
        #[arg(long)]
        serve: Option<SocketAddr>,
        /// Append one JSON line per run to this file.
        #[arg(long)]
        log: Option<PathBuf>,
        /// Append one JSON line per minute to this file: the long-term record `wfe dst history` reads, and
        /// where the runs-per-minute chart is reloaded from after a restart.
        #[arg(long, env = "WFE_DST_HISTORY")]
        history: Option<PathBuf>,
        /// Seconds between progress lines on stderr (0: none).
        #[arg(long, default_value_t = 30)]
        progress: u64,
    },
    /// Run one seed again and print its report.
    Replay {
        #[arg(long)]
        seed: u64,
        #[arg(long, default_value_t = 12_000)]
        ticks: u64,
    },
    /// Every fuzzer session on record: its revision, the seeds it ran, throughput, failures and faults injected.
    History {
        /// The file `wfe dst run --history` appends to.
        #[arg(long, env = "WFE_DST_HISTORY")]
        history: PathBuf,
        /// Only sessions of revisions starting with this.
        #[arg(long)]
        revision: Option<String>,
    },
    /// Everything simulation has caught, with the revision that failed.
    Findings {
        /// Only the ones not marked fixed.
        #[arg(long)]
        open: bool,
    },
    /// The status of a running `wfe dst run --serve`.
    Status {
        #[arg(long, env = "WFE_DST_URL", default_value = "http://127.0.0.1:7600")]
        dst_url: String,
    },
}

fn speedup(sim_ms: u64, wall_ms: u64) -> f64 {
    if wall_ms == 0 { 0.0 } else { sim_ms as f64 / wall_ms as f64 }
}

async fn interrupted() {
    #[cfg(unix)]
    {
        let Ok(mut terminate) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) else {
            return std::future::pending().await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    if tokio::signal::ctrl_c().await.is_err() {
        std::future::pending::<()>().await;
    }
}

fn summary(s: &FuzzStatus) -> serde_json::Value {
    json!({
        "running": s.running,
        "revision": s.revision,
        "dirty": s.dirty,
        "threads": s.threads,
        "ticks": s.ticks,
        "first_seed": s.first_seed,
        "runs": s.runs,
        "failures": s.failures,
        "simulated_ms": s.sim_ms,
        "wall_ms": s.wall_ms,
        "speedup": (speedup(s.sim_ms, s.wall_ms) * 10.0).round() / 10.0,
        "scenarios": s.scenarios,
        "totals": s.totals,
        "in_flight": s.in_flight,
        "failed_seeds": s.recent_failures.iter().map(|r| json!({
            "seed": r.seed,
            "failure": r.failure,
            "replay": format!("wfe dst replay --seed {} --ticks {}", r.seed, r.options.ticks),
        })).collect::<Vec<_>>(),
    })
}

pub(crate) async fn run(command: &DstCommand) -> Done {
    match command {
        DstCommand::Run { threads, ticks, seed, runs, serve, log, history, progress } => {
            let mut config = FuzzConfig {
                ticks: *ticks,
                first_seed: *seed,
                max_runs: *runs,
                log: log.clone(),
                history: history.clone(),
                ..FuzzConfig::default()
            };
            if let Some(threads) = threads {
                config.threads = (*threads).max(1);
            }
            let fuzzer = Arc::new(Fuzzer::start(config));
            if let Some(addr) = serve {
                use axum::routing::get;
                let listener =
                    tokio::net::TcpListener::bind(addr).await.map_err(|e| usage(format!("--serve {addr}: {e}")))?;
                let state = fuzzer.clone();
                let app = axum::Router::new()
                    .route(
                        "/status",
                        get(move || {
                            let state = state.clone();
                            async move {
                                axum::Json(
                                    tokio::task::spawn_blocking(move || state.status()).await.unwrap_or_default(),
                                )
                            }
                        }),
                    )
                    .route("/healthz", get(|| async { "ok" }));
                eprintln!(
                    "{}",
                    json!({ "serving": format!("http://{}", listener.local_addr().map_err(anyhow::Error::from)?) })
                );
                tokio::spawn(async move {
                    if let Err(error) = axum::serve(listener, app).await {
                        tracing::error!(%error, "the fuzzer status server stopped");
                    }
                });
            }
            let mut last_progress = std::time::Instant::now();
            let mut interrupt = std::pin::pin!(interrupted());
            loop {
                tokio::select! {
                    _ = &mut interrupt => {
                        fuzzer.stop();
                        eprintln!("{}", json!({ "stopping": "waiting for runs in flight" }));
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                }
                let f = fuzzer.clone();
                let status = tokio::task::spawn_blocking(move || f.status()).await.map_err(anyhow::Error::from)?;
                if !status.running {
                    break;
                }
                if *progress > 0 && last_progress.elapsed() >= Duration::from_secs(*progress) {
                    last_progress = std::time::Instant::now();
                    eprintln!(
                        "{}",
                        json!({ "runs": status.runs, "failures": status.failures, "in_flight": status.in_flight })
                    );
                }
            }
            let status_once_every_thread_finished_its_seed = loop {
                let f = fuzzer.clone();
                let status = tokio::task::spawn_blocking(move || f.status()).await.map_err(anyhow::Error::from)?;
                if !status.running && status.in_flight.is_empty() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            fuzzer.flush_history();
            let status = status_once_every_thread_finished_its_seed;
            if status.failures > 0 {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&summary(&status)).expect("a serde_json::Value always serializes")
                );
                return Err(Failed(
                    1,
                    Error::new(
                        wfe_core::ErrorCode::Internal,
                        format!("{} of {} runs failed", status.failures, status.runs),
                    ),
                ));
            }
            Ok(Output::json(summary(&status)))
        }
        DstCommand::Replay { seed, ticks } => {
            let options = Options::for_seed(*seed, *ticks);
            let params = options.describe();
            let run_options = options.clone();
            let started = std::time::Instant::now();
            let outcome = tokio::task::spawn_blocking(move || {
                std::thread::Builder::new()
                    .stack_size(16 << 20)
                    .spawn(move || findings::catch(|| world::run(run_options)))
                    .expect("one more thread with a 16 MiB stack can start unless the OS is out of resources")
                    .join()
                    .unwrap_or_else(|_| Err("the simulation thread died".into()))
            })
            .await
            .map_err(anyhow::Error::from)?;
            let wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            match outcome {
                Ok(report) => Ok(Output::json(
                    json!({ "ok": true, "seed": seed, "options": options, "wall_ms": wall_ms, "report": report }),
                )),
                Err(failure) => {
                    let finding = findings::record("world", *seed, &params, &failure);
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            &json!({ "ok": false, "seed": seed, "options": options, "finding": finding })
                        )
                        .expect("a serde_json::Value always serializes")
                    );
                    Err(Failed(1, Error::new(wfe_core::ErrorCode::Internal, format!("seed {seed} failed: {failure}"))))
                }
            }
        }
        DstCommand::History { history, revision } => {
            let sessions: Vec<Session> = history::sessions(&history::load(history))
                .into_iter()
                .filter(|s| revision.as_ref().is_none_or(|r| s.revision.starts_with(r.as_str())))
                .collect();
            let rows = sessions
                .iter()
                .map(|s| {
                    vec![
                        time(Timestamp::from_millis(s.session * 1_000)),
                        format!(
                            "{}{}",
                            s.revision.chars().take(10).collect::<String>(),
                            if s.dirty { "+" } else { "" }
                        ),
                        format!("{}..={}", s.first_seed, s.last_seed),
                        s.runs.to_string(),
                        s.failures.to_string(),
                        duration_ms(s.sim_ms),
                        format!("{:.1}x", speedup(s.sim_ms, s.wall_ms)),
                        s.totals.crashes.to_string(),
                        s.totals.partitions.to_string(),
                        s.totals.failovers.to_string(),
                    ]
                })
                .collect();
            Ok(Output::json(&sessions).with_table(
                vec![
                    "STARTED",
                    "REVISION",
                    "SEEDS",
                    "RUNS",
                    "FAILED",
                    "SIMULATED",
                    "SPEEDUP",
                    "CRASHES",
                    "PARTITIONS",
                    "FAILOVERS",
                ],
                rows,
            ))
        }
        DstCommand::Findings { open } => {
            let all: Vec<_> = findings::load().into_iter().filter(|f| !*open || f.status != "fixed").collect();
            let rows = all
                .iter()
                .map(|f| {
                    let first_line = f.failure.lines().next().unwrap_or("").chars().take(90).collect::<String>();
                    let revision = format!(
                        "{}{}",
                        f.revision.chars().take(10).collect::<String>(),
                        if f.dirty { "+" } else { "" }
                    );
                    vec![
                        time(Timestamp::from_millis(f.time * 1_000)),
                        f.suite.clone(),
                        f.seed.to_string(),
                        f.status.clone(),
                        revision,
                        first_line,
                    ]
                })
                .collect();
            Ok(Output::json(&all).with_table(vec!["WHEN", "SUITE", "SEED", "STATUS", "REVISION", "FAILURE"], rows))
        }
        DstCommand::Status { dst_url } => {
            let url = format!("{}/status", dst_url.trim_end_matches('/'));
            let response = reqwest::get(&url).await.map_err(|e| Error::unavailable(format!("{url}: {e}")))?;
            let status: FuzzStatus = response.json().await.map_err(|e| Error::unavailable(format!("{url}: {e}")))?;
            let mut rows = vec![
                vec!["running".to_string(), status.running.to_string()],
                vec![
                    "revision".to_string(),
                    format!("{}{}", status.revision, if status.dirty { " (dirty)" } else { "" }),
                ],
                vec!["uptime".to_string(), duration_ms(status.now.saturating_sub(status.started_at) * 1_000)],
                vec!["runs".to_string(), status.runs.to_string()],
                vec!["failures".to_string(), status.failures.to_string()],
                vec!["simulated".to_string(), duration_ms(status.sim_ms)],
                vec!["cpu time".to_string(), duration_ms(status.wall_ms)],
            ];
            for (name, tally) in &status.scenarios {
                rows.push(vec![format!("scenario: {name}"), format!("{} runs, {} failed", tally.runs, tally.failures)]);
            }
            Ok(Output::json(summary(&status)).with_table(vec!["", ""], rows))
        }
    }
}
