use crate::App;
use crate::view::{self, Flash, compact, count, duration_ms, page, pill, tile, when};
use axum::extract::{Query, State};
use maud::{Markup, html};
use std::time::Duration;
use wfe_core::Timestamp;
use wfe_sim::fuzz::{FuzzStatus, RunRecord};
use wfe_sim::world::Options;

fn scenario(o: &Options) -> String {
    let mut s = format!("{} cell{} · {}×{}", o.cells, if o.cells == 1 { "" } else { "s" }, o.shards, o.replicas);
    if o.failover {
        s += " · failover";
    }
    if !o.faults {
        s += " · no faults";
    }
    s
}

fn replay(r: &RunRecord) -> String {
    format!("wfe dst replay --seed {} --ticks {}", r.seed, r.options.ticks)
}

fn hhmm(unix_minute: u64) -> String {
    let m = unix_minute % (24 * 60);
    format!("{:02}:{:02} UTC", m / 60, m % 60)
}

fn throughput(status: &FuzzStatus) -> Markup {
    const W: f64 = 720.0;
    const H: f64 = 96.0;
    let values = &status.per_minute;
    let n = values.len().max(1) as f64;
    let max = values.iter().copied().max().unwrap_or(0).max(1);
    let step = W / n;
    let this_minute = status.now / 60;
    html! {
        div.panel.chart {
            div.title { "Runs finished per minute" }
            div.subtitle { "Last 60 minutes · peak " (count(max)) " per minute" }
            svg viewBox=(format!("0 0 {W} {}", H + 16.0)) role="img" aria-label="Runs finished per minute over the last hour" {
                @for (i, v) in values.iter().enumerate() {
                    @let x = i as f64 * step;
                    @let h = if *v == 0 { 1.0 } else { (*v as f64 / max as f64 * (H - 4.0)).max(3.0) };
                    @let minute = this_minute + 1 + i as u64 - values.len() as u64;
                    @let r = h.min(3.0);
                    @let (left, right, top) = (x + 1.0, x + step - 1.0, H - h);
                    g data-tip=(format!("{} — {} run{}", hhmm(minute), count(*v), if *v == 1 { "" } else { "s" })) {
                        rect.hit x=(x) y="0" width=(step) height=(H) {}
                        path.bar.zero[*v == 0] d=(format!(
                            "M{left},{H} V{} Q{left},{top} {},{top} H{} Q{right},{top} {right},{} V{H} Z",
                            top + r, left + r, right - r, top + r
                        )) {}
                    }
                }
                line.axis x1="0" x2=(W) y1=(H) y2=(H) {}
                text x="0" y=(H + 13.0) { "60 min ago" }
                text x=(W) y=(H + 13.0) text-anchor="end" { "now" }
            }
        }
    }
}

fn counts_table(title: &str, key: &str, rows: &[(String, u64)], comparable: bool) -> Markup {
    let max = rows.iter().map(|(_, n)| *n).max().unwrap_or(0).max(1);
    html! {
        div.panel.scroll {
            table {
                thead { tr { th { (title) } th.num { (key) } @if comparable { th {} } } }
                tbody {
                    @for (name, n) in rows {
                        tr {
                            td { (name) }
                            td.num data-tip=(count(*n)) { (compact(*n)) }
                            @if comparable { td style="width:40%" { div.meter { div.track { div.fill style=(format!("width:{:.1}%", *n as f64 / max as f64 * 100.0)) {} } } } }
                        }
                    }
                    @if rows.is_empty() { tr { td colspan="3" { span.muted { "Nothing yet." } } } }
                }
            }
        }
    }
}

fn dashboard_body(url: &str, s: &FuzzStatus) -> Markup {
    let uptime_ms = s.now.saturating_sub(s.started_at) * 1_000;
    let recent_minutes: Vec<u64> = s.per_minute.iter().rev().skip(1).take(5).copied().collect();
    let too_young_for_five_whole_minutes = uptime_ms < 6 * 60_000;
    let (per_minute, per_minute_note) = if too_young_for_five_whole_minutes {
        (s.runs as f64 / (uptime_ms.max(1_000) as f64 / 60_000.0), "since it started")
    } else {
        (
            recent_minutes.iter().sum::<u64>() as f64 / recent_minutes.len().max(1) as f64,
            "average of the last 5 minutes",
        )
    };
    let speedup = if s.wall_ms == 0 { 0.0 } else { s.sim_ms as f64 / s.wall_ms as f64 };
    let open_findings = s.findings.iter().filter(|f| f.status != "fixed").count();
    let t = &s.totals;
    let faults = vec![
        ("Replica crashes".to_string(), t.crashes),
        ("Network partitions".to_string(), t.partitions),
        ("Messages dropped".to_string(), t.messages_dropped),
        ("View changes".to_string(), t.view_changes),
        ("Gateway restarts".to_string(), t.gateway_restarts),
        ("Worker restarts".to_string(), t.worker_restarts),
        ("Cell failovers".to_string(), t.failovers),
        ("Controller interrupted mid-plan".to_string(), t.controller_interruptions),
        ("Controller acted on a stale plan".to_string(), t.controller_stale_plans),
    ];
    let work = vec![
        ("Workflows run".to_string(), t.workflows),
        ("History events".to_string(), t.events),
        ("Workflow tasks".to_string(), t.workflow_tasks),
        ("Activities".to_string(), t.activities),
        ("Replicated log entries".to_string(), t.ops),
        ("Messages delivered".to_string(), t.messages),
        ("Changes replicated between cells".to_string(), t.replicated),
        ("Payloads offloaded to object storage".to_string(), t.claim_checks),
        ("Controller rounds".to_string(), t.controller_rounds),
        ("Worker builds rolled out".to_string(), t.rollouts),
        ("Worker builds retired".to_string(), t.builds_retired),
    ];
    let pairs = |map: &std::collections::BTreeMap<String, u64>| {
        let mut rows: Vec<(String, u64)> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
        rows.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
        rows
    };
    html! {
        div data-live="dst" {
            div.head {
                h1 { "Simulation" }
                @if s.running { (pill("good", "running")) } @else { (pill("warn", "stopped")) }
                @if s.failures > 0 { (pill("bad", &format!("{} failing seed{}", s.failures, if s.failures == 1 { "" } else { "s" }))) }
            }
            p.lead {
                "Every run builds a whole deployment in one thread — cells, shards, replicas, gateways and SDK workers on a simulated "
                "network and disk — then drives workflows through crashes, partitions and failovers and checks the outcome. "
                "A seed fully determines a run, so any failure replays exactly."
            }
            div.muted {
                "Source " span.mono { (s.revision.chars().take(12).collect::<String>()) }
                @if s.dirty { " (uncommitted changes)" }
                " · " (s.threads) " threads · " (count(s.ticks)) " ticks per run · seeds from " span.mono { (s.first_seed) }
                " · " span.mono { (url) }
            }
            div.tiles {
                (tile("Runs", compact(s.runs), &format!("{} in total", count(s.runs))))
                div class=(if s.failures > 0 { "tile alarm" } else { "tile" }) {
                    div.label { "Failures" }
                    div.value { (count(s.failures)) }
                    div.note { @if s.failures == 0 { "every check passed" } @else { "see below to replay" } }
                }
                (tile("Runs per minute", format!("{per_minute:.0}"), per_minute_note))
                (tile("Simulated time", duration_ms(s.sim_ms), &format!("{speedup:.0}× real time per core")))
                (tile("Uptime", duration_ms(uptime_ms), &format!("{} of CPU", duration_ms(s.wall_ms))))
                div class=(if open_findings > 0 { "tile alarm" } else { "tile" }) {
                    div.label { "Open findings" }
                    div.value { (open_findings) }
                    div.note { (s.findings.len()) " caught so far" }
                }
            }
            (throughput(s))

            @if !s.recent_failures.is_empty() {
                h2 { "Failing seeds" }
                div.panel.scroll {
                    table {
                        thead { tr { th { "Seed" } th { "Scenario" } th { "Finished" } th { "Failure" } th { "Replay" } } }
                        tbody {
                            @for r in &s.recent_failures {
                                tr {
                                    td.id { (r.seed) }
                                    td.tight { (scenario(&r.options)) }
                                    td.tight { (when(Timestamp::from_millis(r.time * 1_000))) }
                                    td.wrap { pre.block { (r.failure.as_deref().unwrap_or("")) } }
                                    td { code { (replay(r)) } }
                                }
                            }
                        }
                    }
                }
            }

            h2 { "In flight" span.muted { "seeds being simulated right now" } }
            div.seeds {
                @for seed in &s.in_flight { span.seed { (seed) } }
                @if s.in_flight.is_empty() { span.muted { "None." } }
            }

            h2 { "What the runs went through" }
            div.split {
                (counts_table("Faults injected", "Count", &faults, false))
                (counts_table("Work done", "Count", &work, false))
            }

            h2 { "Checks" span.muted { "how many times each invariant was verified, and held" } }
            div.split {
                (counts_table("Invariant", "Verified", &pairs(&t.checks), false))
                div {
                    div.panel.scroll {
                        table {
                            thead { tr { th { "Scenario" } th.num { "Runs" } th.num { "Failures" } } }
                            tbody {
                                @for (name, tally) in &s.scenarios {
                                    tr {
                                        td { (name) }
                                        td.num { (count(tally.runs)) }
                                        td.num { @if tally.failures > 0 { (pill("bad", &tally.failures.to_string())) } @else { "0" } }
                                    }
                                }
                                @if s.scenarios.is_empty() { tr { td colspan="3" { span.muted { "Nothing yet." } } } }
                            }
                        }
                    }
                }
            }

            h2 { "Workflows exercised" }
            div.split {
                (counts_table("By kind", "Workflows", &pairs(&t.by_kind), true))
                (counts_table("By outcome", "Workflows", &pairs(&t.by_status), true))
            }

            h2 { "Recent runs" }
            div.panel.scroll {
                table {
                    thead { tr {
                        th { "Seed" } th { "Scenario" } th { "Result" } th.num { "Took" } th.num { "Speed" } th.num { "Workflows" }
                        th.num { "Events" } th.num { "Crashes" } th.num { "Partitions" } th.num { "View changes" } th { "Digest" }
                    } }
                    tbody {
                        @for r in s.recent.iter().take(40) {
                            tr {
                                td.id data-tip=(replay(r)) { (r.seed) }
                                td.tight { (scenario(&r.options)) }
                                td { @if r.ok { (pill("good", "passed")) } @else { (pill("bad", "failed")) } }
                                td.num.tight { (duration_ms(r.wall_ms)) }
                                @match &r.report {
                                    Some(report) => {
                                        td.num.tight { (format!("{:.0}×", report.sim_ms as f64 / r.wall_ms.max(1) as f64)) }
                                        td.num { (count(report.workflows)) }
                                        td.num { (count(report.events)) }
                                        td.num { (report.crashes) }
                                        td.num { (report.partitions) }
                                        td.num { (report.view_changes) }
                                        td.id.muted { (report.digest.chars().take(12).collect::<String>()) }
                                    }
                                    None => td colspan="7" { span.muted { "—" } },
                                }
                            }
                        }
                        @if s.recent.is_empty() { tr { td colspan="11" { span.muted { "The first runs are still going." } } } }
                    }
                }
            }

            h2 { "Findings log" span.muted { "everything simulation has ever caught, with the source that failed" } }
            div.panel.scroll {
                table {
                    thead { tr { th { "Caught" } th { "Suite" } th { "Seed" } th { "Status" } th { "Revision" } th { "Failure and fix" } } }
                    tbody {
                        @for f in s.findings.iter().rev() {
                            tr {
                                td.tight { (when(Timestamp::from_millis(f.time * 1_000))) }
                                td { (f.suite) }
                                td.id data-tip=(f.params) { (f.seed) }
                                td { @if f.status == "fixed" { (pill("good", "fixed")) } @else { (pill("bad", &f.status)) } }
                                td.id.tight { (f.revision.chars().take(10).collect::<String>()) @if f.dirty { "+" } }
                                td.wrap {
                                    details {
                                        summary { (f.failure.lines().next().unwrap_or("").chars().take(140).collect::<String>()) }
                                        pre.block { (f.failure) }
                                    }
                                    @if !f.note.is_empty() { div style="margin-top:6px" { span.muted { "Fix: " } (f.note) } }
                                }
                            }
                        }
                        @if s.findings.is_empty() { tr { td colspan="6" { span.muted { "Nothing caught yet." } } } }
                    }
                }
            }
        }
    }
}

pub(crate) async fn dashboard(State(app): State<App>, Query(flash): Query<Flash>) -> Markup {
    let nav = app.nav("dst");
    let how = html! {
        p { "Start a fuzzer on any machine and point the UI at it:" }
        pre.block { "wfe dst run --serve 0.0.0.0:7600\nwfe ui --dst http://<that machine>:7600" }
    };
    let Some(url) = &app.dst else {
        let body = html! {
            h1 { "Simulation" }
            p.lead { "No simulation server is configured." }
            div.panel.pad { (how) }
        };
        return page("Simulation", &nav, &flash, body);
    };
    let status = async {
        let response = app
            .http
            .get(format!("{url}/status"))
            .timeout(Duration::from_secs(8))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        response.json::<FuzzStatus>().await.map_err(|e| e.to_string())
    }
    .await;
    let body = match status {
        Ok(status) => dashboard_body(url, &status),
        Err(e) => html! {
            div data-live="dst" {
                div.head { h1 { "Simulation" } (pill("bad", "unreachable")) }
                div.flash.bad role="alert" { "✕ " span.mono { (url) } " did not answer: " (e) }
                div.panel.pad { (how) }
            }
        },
    };
    let _ = view::empty;
    page("Simulation", &nav, &flash, body)
}
