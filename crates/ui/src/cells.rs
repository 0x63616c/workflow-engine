use crate::view::{
    self, Action, Confirm, Flash, action, count, enc, error_box, head, icon, icons, page, pill, role_pill, stat,
};
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
        Some(name) => html! { a.link href=(format!("/cells/{}", enc(name))) { (name) } },
        None => html! { span data-tip="Not managed by this UI" { "Cell " (id) } },
    }
}

fn link_state(pending: u64, paused: bool) -> Markup {
    if paused {
        pill("warn", "Paused")
    } else if pending == 0 {
        pill("good", "In sync")
    } else {
        pill("info pulse", &format!("{} to ship", count(pending)))
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

fn links_table(all: &Described, only: Option<&str>, back: &str) -> Option<Markup> {
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
    if rows.is_empty() {
        return None;
    }
    Some(html! {
        div.panel.scroll {
            table {
                thead { tr { th { "Link" } th { "State" } th.num { "To ship" } th.num { "Changes behind" } th {} } }
                tbody {
                    @for (from, info, target, pending, paused) in &rows {
                        @let url = format!("/cells/{}/replication", enc(from));
                        @let target_text = target.to_string();
                        @let (op, tip, glyph) = if *paused { ("resume", "Resume", icons::PLAY) } else { ("pause", "Pause", icons::PAUSE) };
                        tr {
                            td {
                                span.route {
                                    a.link href=(format!("/cells/{}", enc(from))) { (from) }
                                    (icon(icons::ARROW))
                                    (cell_label(&names, *target))
                                }
                            }
                            td { (link_state(*pending, *paused)) }
                            td.num { (count(*pending)) }
                            td.num { (count(lag(info, *target))) }
                            td.actions {
                                (action(&Action {
                                    url: &url,
                                    fields: &[("cell", &target_text), ("op", op), ("back", back)],
                                    class: "icon-btn",
                                    tip: Some(tip),
                                    confirm: None,
                                }, icon(glyph)))
                                (action(&Action {
                                    url: &url,
                                    fields: &[("cell", &target_text), ("op", "delete"), ("back", back)],
                                    class: "icon-btn danger",
                                    tip: Some("Remove"),
                                    confirm: Some(Confirm {
                                        title: "Stop replicating?",
                                        body: "The target keeps everything it already has.",
                                        cta: "Stop replicating",
                                        danger: true,
                                    }),
                                }, icon(icons::TRASH)))
                            }
                        }
                    }
                }
            }
        }
    })
}

fn cell_card(cell: &Cell, info: &Result<CellInfo, Error>, names: &BTreeMap<CellId, String>) -> Markup {
    let href = format!("/cells/{}", enc(&cell.name));
    match info {
        Err(e) => html! {
            div.card.down {
                div.card-head {
                    h3 { a.stretch href=(href) { (cell.name) } }
                    (pill("bad", "Unreachable"))
                }
                div.muted { (e.message) }
                div.faint.mono { (cell.urls.join(", ")) }
            }
        },
        Ok(info) => {
            let s = CellSummary::of(info);
            html! {
                div.card {
                    div.card-head {
                        h3 { a.stretch href=(href) { (cell.name) } }
                        @if s.shards_up < s.shard_count { (pill("bad", &format!("{} shards down", s.shard_count - s.shards_up))) }
                        (role_pill(&s.role))
                    }
                    div.facts {
                        div.fact { div.label { "Shards" } div.value { (s.shards_up) small { " / " (s.shard_count) } } }
                        div.fact { div.label { "Open" } div.value { (count(s.open_workflows)) } }
                        div.fact { div.label { "Executions" } div.value { (count(s.executions)) } }
                    }
                    @if !s.replication.is_empty() {
                        div.card-links {
                            @for (target, (pending, paused)) in &s.replication {
                                div { (icon(icons::ARROW)) span.grow { (cell_label(names, *target)) } (link_state(*pending, *paused)) }
                            }
                        }
                    }
                    div.card-foot {
                        a.btn.small href=(format!("{href}/workflows")) { "Workflows" }
                        a.btn.small href=(format!("{href}/deployments")) { "Deployments" }
                    }
                }
            }
        }
    }
}

fn cell_options(all: &Described, selected: Option<&str>, except: Option<&str>) -> Markup {
    html! {
        @for (c, _) in all {
            @if except != Some(c.name.as_str()) {
                option selected[selected == Some(c.name.as_str())] { (c.name) }
            }
        }
    }
}

fn first_with_role<'a>(all: &'a Described, role: &str) -> Option<&'a str> {
    all.iter()
        .find(|(_, info)| info.as_ref().is_ok_and(|info| CellSummary::of(info).role == role))
        .map(|(cell, _)| cell.name.as_str())
}

fn replicate_dialog(all: &Described, from: Option<&str>, back: &str) -> Markup {
    let active = first_with_role(all, "active");
    let standby = first_with_role(all, "standby");
    view::dialog(
        "replicate",
        html! { "Replicate a cell" },
        "/replicate",
        html! {
            input type="hidden" name="back" value=(back);
            @if let Some(from) = from {
                input type="hidden" name="from" value=(from);
                label.field { "To" select name="to" autofocus { (cell_options(all, standby, Some(from))) } }
            } @else {
                div.field-row {
                    label.field { "From" select name="from" autofocus { (cell_options(all, active, None)) } }
                    label.field { "To" select name="to" { (cell_options(all, standby.or_else(|| all.iter().map(|(c, _)| c.name.as_str()).find(|n| Some(*n) != active)), None)) } }
                }
            }
        },
        html! { button.btn.primary type="submit" { "Start replicating" } },
    )
}

fn failover_dialog(all: &Described) -> Markup {
    let active = first_with_role(all, "active");
    let standby = first_with_role(all, "standby");
    view::dialog(
        "failover",
        html! { "Fail over" },
        "/failover",
        html! {
            p { "The new cell takes over writes. The old one becomes its standby." }
            div.field-row {
                label.field { "From" select name="from" autofocus { (cell_options(all, active, None)) } }
                label.field { "To" select name="to" { (cell_options(all, standby.or_else(|| all.iter().map(|(c, _)| c.name.as_str()).find(|n| Some(*n) != active)), None)) } }
            }
            label.check {
                input type="checkbox" name="force" value="1";
                span { "Force" small { "Don't wait for the old cell. Anything it hadn't shipped is missing until it returns." } }
            }
        },
        html! { button.btn.danger.solid type="submit" { "Fail over" } },
    )
}

pub(crate) async fn overview(State(app): State<App>, Query(flash): Query<Flash>) -> Markup {
    let all = app.describe_all().await;
    let names = names(&all);
    let many = all.len() > 1;
    let links = links_table(&all, None, "/");
    let body = html! {
        (head(html! { h1 { "Cells" } }, html! {}, html! {
            @if many {
                button.btn type="button" data-open="failover" { (icon(icons::SWAP)) "Fail over" }
                button.btn.primary type="button" data-open="replicate" { (icon(icons::PLUS)) "Replicate" }
            }
        }))
        div.cards {
            @for (cell, info) in &all { (cell_card(cell, info, &names)) }
        }
        @if let Some(links) = links {
            h2 { "Replication" }
            (links)
        }
        @if many {
            (replicate_dialog(&all, None, "/"))
            (failover_dialog(&all))
        }
    };
    page("Cells", &app.nav("cells"), &flash, body)
}

fn role_action(back: &str, role: &'static str) -> Markup {
    let url = format!("{back}/role");
    let confirm = if role == "active" {
        Confirm {
            title: "Make this cell active?",
            body: "It starts taking writes. To move the role without losing writes, fail over instead.",
            cta: "Make active",
            danger: false,
        }
    } else {
        Confirm {
            title: "Make this cell a standby?",
            body: "It stops taking writes and only receives replication.",
            cta: "Make standby",
            danger: true,
        }
    };
    action(
        &Action { url: &url, fields: &[("role", role)], class: "btn", tip: None, confirm: Some(confirm) },
        html! { (confirm.cta) },
    )
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
    let many = all.len() > 1;
    let body = html! {
        @match &info {
            Err(e) => {
                (head(html! { h1 { (name) } (pill("bad", "Unreachable")) }, html! {}, html! {}))
                (error_box("The cell did not answer", e))
            }
            Ok(info) => {
                @let s = CellSummary::of(info);
                (head(
                    html! { h1 { (name) } (role_pill(&s.role)) },
                    html! {
                        span { "Cell " (s.cell) }
                        span.sep { (s.shard_count) " shards × " (s.replica_count) " replicas" }
                        span.sep { "Epoch " (s.epoch) }
                    },
                    html! {
                        @if s.role != "active" { (role_action(&back, "active")) }
                        @if s.role != "standby" { (role_action(&back, "standby")) }
                        @if many { button.btn.primary type="button" data-open="replicate" { (icon(icons::PLUS)) "Replicate" } }
                    },
                ))
                div.stats {
                    div.stat { div.label { "Shards up" } div.value { (s.shards_up) small { " / " (s.shard_count) } } }
                    (stat("Open workflows", count(s.open_workflows), ""))
                    (stat("Executions", count(s.executions), ""))
                    (stat("Replicated in", count(s.replicated_in), ""))
                }
                h2 { "Shards" }
                div.panel.scroll {
                    table {
                        thead { tr {
                            th { "Shard" } th { "Role" } th.num { "Open" } th.num { "Executions" }
                            th.num { "Log position" } th.num { "Changes" } th.num { "Replicated in" } th { "Replicating to" }
                        } }
                        tbody {
                            @for shard in 0..info.shard_count {
                                @match info.shards.iter().find(|s| s.shard == shard) {
                                    Some(sh) => tr {
                                        td { (sh.shard) }
                                        td { (role_pill(if sh.active { "active" } else { "standby" })) }
                                        td.num { (count(sh.open_workflows)) }
                                        td.num { (count(sh.executions)) }
                                        td.num { (count(sh.op)) }
                                        td.num { (count(sh.change_seq)) }
                                        td.num { (count(sh.replicated_in)) }
                                        td {
                                            @for r in &sh.replication {
                                                div.route data-tip=(format!("Acknowledged {} of {}", count(r.acked_seq), count(r.head_seq))) {
                                                    (cell_label(&names, r.target.cell)) (link_state(r.pending, r.paused))
                                                }
                                            }
                                            @if sh.replication.is_empty() { span.faint { "—" } }
                                        }
                                    },
                                    None => tr { td { (shard) } td colspan="7" { (pill("bad", "No answer")) } },
                                }
                            }
                        }
                    }
                }
            }
        }
        h2 { "Nodes" }
        div.stack {
            @for (url, status) in &nodes {
                div.panel {
                    @match status {
                        None => div.panel-head { h3.mono { (url) } (pill("bad", "No answer")) },
                        Some(node) => {
                            div.panel-head { h3 { "Node " (node.node) } span.faint.mono { (url) } }
                            div.scroll {
                                table {
                                    thead { tr {
                                        th { "Shard" } th { "Replica" } th { "State" } th { "Role" }
                                        th.num { "View" } th.num { "Op" } th.num { "Commit" } th.num { "Checkpoint" }
                                    } }
                                    tbody {
                                        @for r in &node.hosted {
                                            tr {
                                                td { (r.shard) }
                                                td { (r.replica) }
                                                td {
                                                    (pill(if r.status == "normal" { "good" } else { "warn" }, &view::sentence(&r.status)))
                                                    @if r.repairing { " " (pill("warn pulse", "Repairing")) }
                                                }
                                                td { @if r.primary { (pill("info", "Primary")) } @else { span.muted { "Backup" } } }
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
        }
        @if let Some(links) = links_table(&all, Some(&name), &back) {
            h2 { "Replication" }
            (links)
        }
        @if many { (replicate_dialog(&all, Some(&name), &back)) }
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
