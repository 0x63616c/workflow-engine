use crate::view::{self, Flash, action, count, enc, error_box, page, pill, role_pill, tile};
use crate::{App, Cell, done, not_found};
use axum::extract::{Form, Path, Query, State};
use axum::response::{IntoResponse, Response};
use maud::{Markup, html};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use wfe_core::api::{ApiRequest, CellInfo};
use wfe_core::{CellId, Error};
use wfe_sdk::CellSummary;

type Described = Vec<(Arc<Cell>, Result<CellInfo, Error>)>;

fn names(all: &Described) -> BTreeMap<CellId, String> {
    all.iter().filter_map(|(cell, info)| Some((info.as_ref().ok()?.cell, cell.name.clone()))).collect()
}

fn cell_label(names: &BTreeMap<CellId, String>, id: CellId) -> Markup {
    match names.get(&id) {
        Some(name) => html! { a href=(format!("/cells/{}", enc(name))) { (name) } },
        None => html! { "cell " (id) " " span.muted { "(not in this UI)" } },
    }
}

fn link_state(pending: u64, paused: bool) -> Markup {
    if paused {
        pill("warn", "paused")
    } else if pending == 0 {
        pill("good", "in sync")
    } else {
        pill("info", &format!("{} to ship", count(pending)))
    }
}

fn lag(info: &CellInfo, target: CellId) -> u64 {
    info.shards
        .iter()
        .flat_map(|s| &s.replication)
        .filter(|r| r.target.cell == target)
        .map(|r| r.head_seq.saturating_sub(r.acked_seq))
        .sum()
}

fn links_table(all: &Described, only: Option<&str>, back: &str) -> Markup {
    let names = names(all);
    let mut rows = Vec::new();
    for (cell, info) in all {
        let Ok(info) = info else { continue };
        if only.is_some_and(|name| name != cell.name) {
            continue;
        }
        for (target, (pending, paused)) in CellSummary::of(info).replication {
            rows.push((cell.name.clone(), info, target, pending, paused));
        }
    }
    html! {
        div.panel.scroll {
            @if rows.is_empty() {
                (view::empty("Nothing is replicating yet."))
            } @else {
                table {
                    thead { tr { th { "From" } th { "To" } th { "State" } th.num { "Executions to ship" } th.num { "Changes behind" } th {} } }
                    tbody {
                        @for (from, info, target, pending, paused) in &rows {
                            @let url = format!("/cells/{}/replication", enc(from));
                            @let target_text = target.to_string();
                            tr {
                                td { a href=(format!("/cells/{}", enc(from))) { (from) } }
                                td { (cell_label(&names, *target)) }
                                td { (link_state(*pending, *paused)) }
                                td.num { (count(*pending)) }
                                td.num { (count(lag(info, *target))) }
                                td.actions {
                                    @if *paused {
                                        (action(&url, "Resume", "small", &[("cell", &target_text), ("op", "resume"), ("back", back)], None))
                                    } @else {
                                        (action(&url, "Pause", "small", &[("cell", &target_text), ("op", "pause"), ("back", back)], None))
                                    }
                                    (action(&url, "Remove", "small danger", &[("cell", &target_text), ("op", "delete"), ("back", back)],
                                        Some("Stop replicating to this cell? It keeps what it has.")))
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn cell_card(cell: &Cell, info: &Result<CellInfo, Error>, names: &BTreeMap<CellId, String>) -> Markup {
    let href = format!("/cells/{}", enc(&cell.name));
    match info {
        Err(e) => html! {
            div.card.down {
                h3 { a href=(href) { (cell.name) } (pill("bad", "unreachable")) }
                p.muted { (e.message) }
                p.muted.mono { (cell.urls.join(", ")) }
            }
        },
        Ok(info) => {
            let s = CellSummary::of(info);
            html! {
                div.card {
                    h3 {
                        a href=(href) { (cell.name) }
                        (role_pill(&s.role))
                        @if s.shards_up < s.shard_count { (pill("bad", &format!("{} of {} shards down", s.shard_count - s.shards_up, s.shard_count))) }
                    }
                    div.muted { "cell " (s.cell) " · epoch " (s.epoch) }
                    div.facts {
                        div.fact { div.label { "Shards" } div.value { (s.shards_up) "/" (s.shard_count) } }
                        div.fact { div.label { "Replicas" } div.value { (s.replica_count) } }
                        div.fact { div.label { "Open workflows" } div.value { (count(s.open_workflows)) } }
                        div.fact { div.label { "Executions" } div.value { (count(s.executions)) } }
                        div.fact { div.label { "Replicated in" } div.value { (count(s.replicated_in)) } }
                    }
                    @if !s.replication.is_empty() {
                        div.links {
                            @for (target, (pending, paused)) in &s.replication {
                                div { "→ " (cell_label(names, *target)) " " (link_state(*pending, *paused)) }
                            }
                        }
                    }
                    div.foot {
                        a href=(format!("{href}/workflows")) { "Workflows" }
                        a href=(format!("{href}/deployments")) { "Deployments" }
                    }
                }
            }
        }
    }
}

pub(crate) async fn overview(State(app): State<App>, Query(flash): Query<Flash>) -> Markup {
    let all = app.describe_all().await;
    let names = names(&all);
    let body = html! {
        h1 { "Cells" }
        p.lead {
            "A cell is one sharded, replicated cluster. One cell is active and takes writes; "
            "the others are standbys that receive its workflows and can take over."
        }
        div.cards data-live="cells" {
            @for (cell, info) in &all { (cell_card(cell, info, &names)) }
        }
        h2 { "Replication" }
        div data-live="links" { (links_table(&all, None, "/")) }
        @if all.len() > 1 {
            div.split style="margin-top:16px" {
                div.panel.pad {
                    form.row method="post" action="/replicate" {
                        label { "Replicate" select name="from" { @for (c, _) in &all { option { (c.name) } } } }
                        label { "to" select name="to" { @for (c, _) in all.iter().rev() { option { (c.name) } } } }
                        button.primary type="submit" { "Start replicating" }
                    }
                }
                div.panel.pad {
                    form.row method="post" action="/failover" data-confirm="Move the active role? Writes to the old cell stop." {
                        label { "Fail over from" select name="from" { @for (c, _) in &all { option { (c.name) } } } }
                        label { "to" select name="to" { @for (c, _) in all.iter().rev() { option { (c.name) } } } }
                        label.check data-tip="Promote without waiting for the old cell. Use when it is gone; workflows it had not shipped are missing until it returns." {
                            input type="checkbox" name="force" value="1"; "Force"
                        }
                        button.danger type="submit" { "Fail over" }
                    }
                }
            }
        }
    };
    page("Cells", &app.nav("cells"), &flash, body)
}

pub(crate) async fn detail(State(app): State<App>, Path(name): Path<String>, Query(flash): Query<Flash>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let (all, nodes) = tokio::join!(app.describe_all(), cell.nodes());
    let names = names(&all);
    let info = all
        .iter()
        .find(|(c, _)| c.name == name)
        .map(|(_, info)| info.clone())
        .unwrap_or_else(|| Err(Error::internal("missing")));
    let back = format!("/cells/{}", enc(&name));
    let body = html! {
        @match &info {
            Err(e) => {
                div.head { h1 { (name) } (pill("bad", "unreachable")) }
                (error_box("The cell did not answer", e))
            }
            Ok(info) => {
                @let s = CellSummary::of(info);
                div data-live="head" {
                    div.head {
                        h1 { (name) }
                        (role_pill(&s.role))
                        span.muted { "cell " (s.cell) " · epoch " (s.epoch) " · " (s.shard_count) " shards × " (s.replica_count) " replicas" }
                    }
                    div.tiles {
                        (tile("Shards up", format!("{}/{}", s.shards_up, s.shard_count), ""))
                        (tile("Open workflows", count(s.open_workflows), ""))
                        (tile("Executions", count(s.executions), "open and closed"))
                        (tile("Replicated in", count(s.replicated_in), "changes from other cells"))
                    }
                }
                div.panel.pad {
                    form.row method="post" action=(format!("{back}/role")) data-confirm="Change this cell's role?" {
                        span { "Role: a standby rejects writes and only takes replication. To move the active role safely, use " a href="/" { "fail over" } "." }
                        button type="submit" name="role" value="active" { "Make active" }
                        button type="submit" name="role" value="standby" { "Make standby" }
                    }
                }
                h2 { "Shards" }
                div.panel.scroll data-live="shards" {
                    table {
                        thead { tr {
                            th { "Shard" } th { "Role" } th.num { "Epoch" } th.num { "Open" } th.num { "Executions" }
                            th.num { "Log position" } th.num { "Changes" } th.num { "Replicated in" } th { "Replicating to" }
                        } }
                        tbody {
                            @for shard in 0..info.shard_count {
                                @match info.shards.iter().find(|s| s.shard == shard) {
                                    Some(sh) => tr {
                                        td { (sh.shard) }
                                        td { (role_pill(if sh.active { "active" } else { "standby" })) }
                                        td.num { (sh.epoch) }
                                        td.num { (count(sh.open_workflows)) }
                                        td.num { (count(sh.executions)) }
                                        td.num { (count(sh.op)) }
                                        td.num { (count(sh.change_seq)) }
                                        td.num { (count(sh.replicated_in)) }
                                        td {
                                            @for r in &sh.replication {
                                                div { (cell_label(&names, r.target.cell)) " " (link_state(r.pending, r.paused)) " "
                                                    span.muted { "acked " (count(r.acked_seq)) " of " (count(r.head_seq)) } }
                                            }
                                            @if sh.replication.is_empty() { span.muted { "—" } }
                                        }
                                    },
                                    None => tr { td { (shard) } td colspan="8" { (pill("bad", "no answer")) } },
                                }
                            }
                        }
                    }
                }
            }
        }
        h2 { "Nodes" span.muted { "each node's own view of the replicas it hosts" } }
        div data-live="nodes" {
            @for (url, status) in &nodes {
                div.panel.scroll style="margin-bottom:12px" {
                    @match status {
                        None => div.empty { span.mono { (url) } " " (pill("bad", "no answer")) },
                        Some(node) => {
                            table {
                                thead { tr {
                                    th { "Node " (node.node) " · " span.mono { (url) } }
                                    th { "Replica" } th { "State" } th { "Role" } th.num { "View" } th.num { "Op" } th.num { "Commit" } th.num { "Checkpoint" }
                                } }
                                tbody {
                                    @for r in &node.hosted {
                                        tr {
                                            td { "shard " (r.shard) }
                                            td { (r.replica) }
                                            td {
                                                (pill(if r.status == "normal" { "good" } else { "warn" }, &r.status))
                                                @if r.repairing { " " (pill("warn", "repairing")) }
                                            }
                                            td { @if r.primary { (pill("info", "primary")) } @else { span.muted { "backup" } } }
                                            td.num { (r.view) }
                                            td.num { (count(r.op)) }
                                            td.num { (count(r.commit)) }
                                            td.num { (count(r.checkpoint_op)) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        h2 { "Replication from this cell" }
        div data-live="links" { (links_table(&all, Some(&name), &back)) }
        @if all.len() > 1 {
            div.panel.pad style="margin-top:12px" {
                form.row method="post" action="/replicate" {
                    input type="hidden" name="from" value=(name);
                    input type="hidden" name="back" value=(back);
                    label { "Replicate this cell to" select name="to" { @for (c, _) in all.iter().filter(|(c, _)| c.name != name) { option { (c.name) } } } }
                    button.primary type="submit" { "Start replicating" }
                }
            }
        }
    };
    page(&name, &app.cell_nav(&name, "overview"), &flash, body).into_response()
}

fn back_or(back: &Option<String>, default: &str) -> String {
    let same_site_path = |b: &&str| b.starts_with('/') && !b.starts_with("//");
    back.as_deref().filter(same_site_path).unwrap_or(default).to_string()
}

#[derive(Deserialize)]
pub(crate) struct Pair {
    from: String,
    to: String,
    #[serde(default)]
    force: Option<String>,
    #[serde(default)]
    back: Option<String>,
}

pub(crate) async fn replicate(State(app): State<App>, Form(form): Form<Pair>) -> Response {
    let back = back_or(&form.back, "/");
    let outcome = async {
        if form.from == form.to {
            return Err(Error::invalid("a cell cannot replicate to itself"));
        }
        let from = app.cell(&form.from).ok_or_else(|| Error::not_found(format!("no cell named {}", form.from)))?;
        let to = app.cell(&form.to).ok_or_else(|| Error::not_found(format!("no cell named {}", form.to)))?;
        let target = CellSummary::of(&to.describe().await?);
        from.api(ApiRequest::CreateReplication { target: target.cell_ref() }).await?;
        Ok(format!("{} now replicates to {}", form.from, form.to))
    }
    .await;
    done(&back, outcome)
}

pub(crate) async fn failover(State(app): State<App>, Form(form): Form<Pair>) -> Response {
    let back = back_or(&form.back, "/");
    let outcome = async {
        if form.from == form.to {
            return Err(Error::invalid("pick two different cells"));
        }
        let from = app.cell(&form.from).ok_or_else(|| Error::not_found(format!("no cell named {}", form.from)))?;
        let to = app.cell(&form.to).ok_or_else(|| Error::not_found(format!("no cell named {}", form.to)))?;
        wfe_sdk::failover(&from.client(), &to.client(), form.force.is_some(), 30_000).await?;
        Ok(format!("{} is now active; {} follows it", form.to, form.from))
    }
    .await;
    done(&back, outcome)
}

#[derive(Deserialize)]
pub(crate) struct LinkAction {
    cell: CellId,
    op: String,
    #[serde(default)]
    back: Option<String>,
}

pub(crate) async fn replication(
    State(app): State<App>,
    Path(name): Path<String>,
    Form(form): Form<LinkAction>,
) -> Response {
    let back = back_or(&form.back, &format!("/cells/{}", enc(&name)));
    let outcome = async {
        let cell = app.cell(&name).ok_or_else(|| Error::not_found(format!("no cell named {name}")))?;
        let (request, message) = match form.op.as_str() {
            "pause" => (ApiRequest::PauseReplication { cell: form.cell, paused: true }, "Replication paused"),
            "resume" => (ApiRequest::PauseReplication { cell: form.cell, paused: false }, "Replication resumed"),
            "delete" => (ApiRequest::DeleteReplication { cell: form.cell }, "Replication removed"),
            other => return Err(Error::invalid(format!("unknown action {other}"))),
        };
        cell.api(request).await?;
        Ok(message.to_string())
    }
    .await;
    done(&back, outcome)
}

#[derive(Deserialize)]
pub(crate) struct RoleForm {
    role: String,
}

pub(crate) async fn role(State(app): State<App>, Path(name): Path<String>, Form(form): Form<RoleForm>) -> Response {
    let back = format!("/cells/{}", enc(&name));
    let outcome = async {
        let cell = app.cell(&name).ok_or_else(|| Error::not_found(format!("no cell named {name}")))?;
        let epoch = CellSummary::of(&cell.describe().await?).epoch + 1;
        cell.api(ApiRequest::SetRole { active: form.role == "active", epoch }).await?;
        Ok(format!("{name} is now {} at epoch {epoch}", if form.role == "active" { "active" } else { "standby" }))
    }
    .await;
    done(&back, outcome)
}
