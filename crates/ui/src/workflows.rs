use crate::view::{self, Flash, count, duration_ms, enc, error_box, page, payload, pill, tile, when, workflow_pill};
use crate::{App, done, not_found, unexpected};
use axum::extract::{Form, Path, Query, State};
use axum::response::{IntoResponse, Response};
use maud::{Markup, html};
use serde::Deserialize;
use wfe_core::api::*;
use wfe_core::{Error, Id128, Payload};

fn workflow_href(cell: &str, id: &str, run: Option<Id128>) -> String {
    let run = run.map(|r| format!("&run={r}")).unwrap_or_default();
    format!("/cells/{}/workflow?id={}{run}", enc(cell), enc(id))
}

fn version_text(v: &WorkflowVersioning) -> Markup {
    html! {
        @match &v.version { Some(version) => span.mono { (version) }, None => span.muted { "unversioned" } }
        @match &v.override_ {
            Some(VersioningOverride::Pinned(p)) => { " " (pill("warn", &format!("pinned to {p}"))) }
            Some(VersioningOverride::AutoUpgrade) => { " " (pill("warn", "auto-upgrade override")) }
            None => {}
        }
    }
}

#[derive(Deserialize, Default)]
pub(crate) struct ListQuery {
    #[serde(default)]
    status: String,
    #[serde(default, rename = "type")]
    workflow_type: String,
    #[serde(default)]
    prefix: String,
    #[serde(flatten)]
    flash: Flash,
}

const LIST_LIMIT: u32 = 1_000;

pub(crate) async fn list(State(app): State<App>, Path(name): Path<String>, Query(q): Query<ListQuery>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let some = |s: &String| (!s.trim().is_empty()).then(|| s.trim().to_string());
    let unfiltered_status_so_the_chips_can_count_every_status = None;
    let filter = ListFilter {
        status: unfiltered_status_so_the_chips_can_count_every_status,
        workflow_type: some(&q.workflow_type),
        id_prefix: some(&q.prefix),
    };
    let result = match cell.api(ApiRequest::ListWorkflows { filter, limit: LIST_LIMIT }).await {
        Ok(ApiResponse::Workflows(list)) => Ok(list),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let base = format!("/cells/{}/workflows", enc(&name));
    let with_status = |status: &str| {
        format!("{base}?status={status}&type={}&prefix={}", enc(q.workflow_type.trim()), enc(q.prefix.trim()))
    };
    let wanted = WorkflowStatus::parse(&q.status);
    let body = html! {
        div.head { h1 { "Workflows" } }
        details.drawer {
            summary { "Start a workflow" }
            div {
                form.stack method="post" action=(format!("{base}/start")) {
                    div.row.form style="display:flex;gap:10px;flex-wrap:wrap" {
                        label { "Workflow type" input type="text" name="workflow_type" required placeholder="greet"; }
                        label { "Task queue" input type="text" name="task_queue" required placeholder="demo"; }
                        label { "Workflow id (optional)" input type="text" name="workflow_id" placeholder="generated"; }
                    }
                    label { "Input (one JSON value)" textarea name="input" placeholder="{\"name\": \"wfe\"}" {} }
                    div { button.primary type="submit" { "Start" } }
                }
            }
        }
        form.row method="get" action=(base) style="margin:16px 0 4px" {
            input type="hidden" name="status" value=(q.status);
            label { "Type" input type="text" name="type" value=(q.workflow_type); }
            label { "Id starts with" input type="text" name="prefix" value=(q.prefix); }
            button type="submit" { "Filter" }
        }
        @match &result {
            Err(e) => (error_box("Could not list workflows", e)),
            Ok(workflows) => {
                @let counts = count_by_status(workflows);
                div data-live="list" {
                    div.chips {
                        a.chip.on[wanted.is_none()] href=(with_status("")) { "All" b { (count(workflows.len() as u64)) } }
                        @for (status, n) in &counts {
                            a.chip.on[q.status == *status] href=(with_status(status)) { (status.replace('_', " ")) b { (count(*n)) } }
                        }
                    }
                    @let shown: Vec<&WorkflowInfo> = workflows.iter().filter(|w| wanted.is_none_or(|s| w.status == s)).collect();
                    div.panel.scroll {
                        @if shown.is_empty() {
                            (view::empty("No workflows match."))
                        } @else {
                            table {
                                thead { tr {
                                    th { "Workflow id" } th { "Type" } th { "Status" } th { "Task queue" } th { "Version" }
                                    th { "Started" } th { "Ran for" } th.num { "Events" } th.num { "Shard" }
                                } }
                                tbody {
                                    @for w in &shown {
                                        tr {
                                            td.id.wrap { a href=(workflow_href(&name, &w.workflow_id, Some(w.run_id))) { (w.workflow_id) } }
                                            td { (w.workflow_type) }
                                            td { (workflow_pill(w.status)) }
                                            td { (w.task_queue) }
                                            td { (version_text(&w.versioning)) }
                                            td.tight { (when(w.start_time)) }
                                            td.tight {
                                                @if w.close_time.is_zero() { span.muted { (duration_ms(view::now().since_ms(w.start_time))) " so far" } }
                                                @else { (duration_ms(w.close_time.since_ms(w.start_time))) }
                                            }
                                            td.num { (w.history_length) }
                                            td.num { (w.shard) }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    @if u32::try_from(workflows.len()).map_or(true, |n| n >= LIST_LIMIT) {
                        p.muted { "Showing the newest " (LIST_LIMIT) " per shard; narrow the filter to see others." }
                    }
                }
            }
        }
    };
    page("Workflows", &app.cell_nav(&name, "workflows"), &q.flash, body).into_response()
}

fn json_payload(text: &str) -> Result<Payload, Error> {
    if text.trim().is_empty() {
        return Ok(Payload::null());
    }
    let value: serde_json::Value =
        serde_json::from_str(text).map_err(|e| Error::invalid(format!("the input is not JSON: {e}")))?;
    Ok(Payload::json(&value).expect("a JSON value serializes"))
}

fn request_id() -> Id128 {
    let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
    Id128(nanos.wrapping_mul(0x9e37_79b9_7f4a_7c15_f39c_c060_5ced_c835) | 1)
}

#[derive(Deserialize)]
pub(crate) struct StartForm {
    workflow_type: String,
    task_queue: String,
    #[serde(default)]
    workflow_id: String,
    #[serde(default)]
    input: String,
}

pub(crate) async fn start(State(app): State<App>, Path(name): Path<String>, Form(form): Form<StartForm>) -> Response {
    let list = format!("/cells/{}/workflows", enc(&name));
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let request_id = request_id();
    let workflow_id = match form.workflow_id.trim() {
        "" => format!("{}-{:08x}", form.workflow_type.trim(), request_id.0 & 0xffff_ffff),
        id => id.to_string(),
    };
    let outcome = async {
        let input = json_payload(&form.input)?;
        let request = StartWorkflow {
            workflow_id: workflow_id.clone(),
            workflow_type: form.workflow_type.trim().to_string(),
            task_queue: form.task_queue.trim().to_string(),
            input,
            request_id,
            ..StartWorkflow::default()
        };
        let response = cell.api(ApiRequest::StartWorkflow(request)).await?;
        if let ApiResponse::Started { run_id, .. } = response { Ok(run_id) } else { Err(unexpected(&response)) }
    }
    .await;
    match outcome {
        Ok(run_id) => done(&workflow_href(&name, &workflow_id, Some(run_id)), Ok("Workflow started".into())),
        Err(e) => done(&list, Err(e)),
    }
}

#[derive(Deserialize)]
pub(crate) struct DetailQuery {
    id: String,
    #[serde(default)]
    run: Option<String>,
    #[serde(flatten)]
    flash: Flash,
}

async fn history(cell: &crate::Cell, id: &str, run: Option<Id128>) -> Result<Vec<Event>, Error> {
    let mut events: Vec<Event> = Vec::new();
    loop {
        let after_event_id = events.last().map_or(0, |e| e.id);
        let request = ApiRequest::GetHistory { workflow_id: id.to_string(), run_id: run, after_event_id, limit: 1_000 };
        let response = cell.api(request).await?;
        let ApiResponse::History { events: page, next_event_id } = response else {
            return Err(unexpected(&response));
        };
        let empty = page.is_empty();
        events.extend(page);
        if next_event_id == 0 || empty || events.len() >= 20_000 {
            return Ok(events);
        }
    }
}

fn event_kind(attrs: &EventAttrs) -> &'static str {
    match attrs {
        EventAttrs::WorkflowCompleted { .. } | EventAttrs::ActivityCompleted { .. } => "good",
        EventAttrs::WorkflowFailed { .. }
        | EventAttrs::ActivityFailed { .. }
        | EventAttrs::WorkflowTaskFailed { .. } => "bad",
        EventAttrs::WorkflowTimedOut
        | EventAttrs::ActivityTimedOut { .. }
        | EventAttrs::WorkflowTaskTimedOut { .. } => "serious",
        EventAttrs::WorkflowCanceled
        | EventAttrs::WorkflowTerminated { .. }
        | EventAttrs::CancelRequested { .. }
        | EventAttrs::TimerCanceled { .. } => "warn",
        EventAttrs::WorkflowStarted { .. }
        | EventAttrs::SignalReceived { .. }
        | EventAttrs::WorkflowContinuedAsNew { .. } => "info",
        EventAttrs::WorkflowTaskScheduled { .. }
        | EventAttrs::WorkflowTaskStarted { .. }
        | EventAttrs::WorkflowTaskCompleted { .. }
        | EventAttrs::ActivityScheduled { .. }
        | EventAttrs::ActivityStarted { .. }
        | EventAttrs::TimerStarted { .. }
        | EventAttrs::TimerFired { .. }
        | EventAttrs::MarkerRecorded { .. }
        | EventAttrs::VersioningOverrideChanged { .. } => "muted",
    }
}

fn event_fields(attrs: &EventAttrs) -> Vec<(String, serde_json::Value)> {
    use serde_json::Value;
    let value = wfe_core::payload::readable(serde_json::to_value(attrs).unwrap_or_default());
    let Value::Object(outer) = value else { return Vec::new() };
    let Some(Value::Object(fields)) = outer.into_iter().next().map(|(_, v)| v) else { return Vec::new() };
    let mut out = Vec::new();
    for (key, value) in fields {
        match (key.as_str(), value) {
            ("options", Value::Object(options)) => {
                for (k, v) in options {
                    let dull = matches!(&v, Value::Null) || v == 0 || v == "";
                    if !dull {
                        out.push((k, v));
                    }
                }
            }
            (_, Value::Null) => {}
            (_, value) if value == 0 && key.ends_with("_ms") => {}
            (_, value) => out.push((key, value)),
        }
    }
    out
}

pub(crate) async fn detail(State(app): State<App>, Path(name): Path<String>, Query(q): Query<DetailQuery>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let run = q.run.as_deref().and_then(Id128::parse);
    let described = match cell.api(ApiRequest::DescribeWorkflow { workflow_id: q.id.clone(), run_id: run }).await {
        Ok(ApiResponse::Workflow(d)) => Ok(*d),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let nav = app.cell_nav(&name, "workflows");
    let d = match described {
        Ok(d) => d,
        Err(e) => {
            let body = html! { div.head { h1.mono { (q.id) } } (error_box("Could not load the workflow", &e)) };
            return page(&q.id, &nav, &q.flash, body).into_response();
        }
    };
    let events = history(&cell, &q.id, Some(d.info.run_id)).await;
    let here = workflow_href(&name, &q.id, Some(d.info.run_id));
    let act = format!("/cells/{}/workflow/act", enc(&name));
    let run_text = d.info.run_id.to_string();
    let open = !d.info.status.is_closed();
    let hidden = html! {
        input type="hidden" name="id" value=(q.id);
        input type="hidden" name="run" value=(run_text);
    };
    let body = html! {
        div data-live="head" {
            div.head {
                h1.mono { (d.info.workflow_id) }
                (workflow_pill(d.info.status))
                @if d.cancel_requested && open { (pill("warn", "cancel requested")) }
            }
            div.muted { (d.info.workflow_type) " · run " span.mono { (d.info.run_id) } }
            @if let Some(next) = d.continued_as {
                p { "Continued as a new run: " a.mono href=(workflow_href(&name, &q.id, Some(next))) { (next) } }
            }
            div.tiles {
                (tile("Started", format!("{} ago", duration_ms(view::now().since_ms(d.info.start_time))), &d.info.start_time.rfc3339()))
                (tile(if open { "Running for" } else { "Ran for" },
                    duration_ms(if open { view::now() } else { d.info.close_time }.since_ms(d.info.start_time)), ""))
                (tile("Events", count(d.info.history_length), ""))
                (tile("Task queue", &d.info.task_queue, &format!("shard {}", d.info.shard)))
                (tile("Workflow task", if d.workflow_task.is_empty() { "none" } else { &d.workflow_task }, ""))
                (tile("Pending", format!("{} / {}", d.pending_activities.len(), d.pending_timers), "activities / timers"))
            }
        }
        @if open {
            div.split {
                div.panel.pad {
                    form.stack method="post" action=(act) {
                        (hidden)
                        input type="hidden" name="op" value="signal";
                        label { "Signal name" input type="text" name="name" required placeholder="approve"; }
                        label { "Input (one JSON value, optional)" textarea name="input" style="min-height:44px" {} }
                        div { button.primary type="submit" { "Send signal" } }
                    }
                }
                div.panel.pad {
                    form.stack method="post" action=(act) {
                        (hidden)
                        label { "Reason (optional)" input type="text" name="reason"; }
                        div {
                            button type="submit" name="op" value="cancel" data-tip="Ask the workflow to stop; it may clean up first." { "Request cancel" }
                            " "
                            button.danger type="submit" name="op" value="terminate" data-tip="Stop it now. The workflow gets no chance to clean up." { "Terminate" }
                        }
                    }
                    form.row method="post" action=(act) style="margin-top:16px" {
                        (hidden)
                        input type="hidden" name="op" value="override";
                        label { "Version override" select name="mode" {
                            option value="pin" { "Pin to version" }
                            option value="auto" { "Auto-upgrade" }
                            option value="clear" { "Clear override" }
                        } }
                        label { "deployment:build" input type="text" name="version" placeholder="shop:v2"; }
                        button type="submit" { "Apply" }
                    }
                }
            }
        }
        div.split style="margin-top:16px" data-live="io" {
            div.panel.pad {
                dl.kv {
                    dt { "Input" } dd { (payload(&d.input)) }
                    @if let Some(result) = &d.result { dt { "Result" } dd { (payload(result)) } }
                    @if let Some(failure) = &d.failure {
                        dt { "Failure" }
                        dd { (pill("bad", if failure.kind.is_empty() { "failed" } else { &failure.kind })) " " (failure.message)
                            @if let Some(details) = &failure.details { (payload(details)) } }
                    }
                }
            }
            div.panel.pad {
                dl.kv {
                    dt { "Version" } dd { (version_text(&d.info.versioning)) }
                    dt { "Behavior" } dd { (match d.info.versioning.behavior {
                        VersioningBehavior::Pinned => "pinned: stays on the version that started it",
                        VersioningBehavior::AutoUpgrade => "auto-upgrade: follows the deployment's current version",
                        VersioningBehavior::Unspecified => "unspecified (unversioned worker)",
                    }) }
                    @if !d.info.close_time.is_zero() { dt { "Closed" } dd { (when(d.info.close_time)) } }
                }
            }
        }
        @if !d.pending_activities.is_empty() {
            h2 { "Pending activities" }
            div.panel.scroll data-live="pending" {
                table {
                    thead { tr { th.num { "Event" } th { "Activity" } th { "State" } th.num { "Attempt" } th { "Task queue" } th { "Last failure" } } }
                    tbody {
                        @for a in &d.pending_activities {
                            tr {
                                td.num { (a.scheduled_id) }
                                td { (a.activity_type) }
                                td { (pill(match a.state.as_str() { "started" => "info", "backoff" => "warn", _ => "muted" }, &a.state)) }
                                td.num { (a.attempt) }
                                td { (a.task_queue) }
                                td.wrap { @match &a.last_failure { Some(f) => (f.to_string()), None => span.muted { "—" } } }
                            }
                        }
                    }
                }
            }
        }
        h2 { "History" }
        div.panel.scroll data-live="history" {
            @match &events {
                Err(e) => (error_box("Could not load the history", e)),
                Ok(events) => {
                    @let first = events.first().map_or(d.info.start_time, |e| e.time);
                    table {
                        thead { tr { th.num { "#" } th { "At" } th { "Event" } th { "Details" } } }
                        tbody {
                            @for e in events {
                                tr {
                                    td.num.muted { (e.id) }
                                    td.tight.mono data-tip=(e.time.rfc3339()) { "+" (format!("{:.3}", e.time.micros().saturating_sub(first.micros()) as f64 / 1e6)) "s" }
                                    td.tight { (pill(event_kind(&e.attrs), e.attrs.name())) }
                                    td.wrap {
                                        @for (key, value) in event_fields(&e.attrs) {
                                            @if value.is_object() || value.is_array() {
                                                div { span.muted { (key) } pre.json { (serde_json::to_string_pretty(&value).unwrap_or_default()) } }
                                            } @else {
                                                span style="margin-right:14px" { span.muted { (key) " " } span.mono { (view::inline_json(&value)) } }
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
        p.muted { a href=(here) { "Permalink to this run" } }
    };
    page(&q.id, &nav, &q.flash, body).into_response()
}

#[derive(Deserialize)]
pub(crate) struct ActForm {
    id: String,
    #[serde(default)]
    run: Option<String>,
    op: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    input: String,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    version: String,
}

pub(crate) async fn act(State(app): State<App>, Path(name): Path<String>, Form(form): Form<ActForm>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let run_id = form.run.as_deref().and_then(Id128::parse);
    let back = workflow_href(&name, &form.id, run_id);
    let workflow_id = form.id.clone();
    let outcome = async {
        let (request, message) = match form.op.as_str() {
            "signal" => (
                ApiRequest::SignalWorkflow {
                    workflow_id,
                    run_id,
                    name: form.name.trim().to_string(),
                    input: json_payload(&form.input)?,
                    request_id: request_id(),
                },
                format!("Signal {} sent", form.name.trim()),
            ),
            "cancel" => (
                ApiRequest::CancelWorkflow { workflow_id, run_id, reason: form.reason.clone() },
                "Cancel requested".to_string(),
            ),
            "terminate" => (
                ApiRequest::TerminateWorkflow { workflow_id, run_id, reason: form.reason.clone() },
                "Workflow terminated".to_string(),
            ),
            "override" => {
                let value = match form.mode.as_str() {
                    "pin" => {
                        let (deployment, build_id) = form
                            .version
                            .trim()
                            .split_once(':')
                            .filter(|(d, b)| !d.is_empty() && !b.is_empty())
                            .ok_or_else(|| Error::invalid("pinning takes a version as deployment:build"))?;
                        Some(VersioningOverride::Pinned(DeploymentVersion::new(deployment, build_id)))
                    }
                    "auto" => Some(VersioningOverride::AutoUpgrade),
                    _ => None,
                };
                (
                    ApiRequest::SetVersioningOverride { workflow_id, run_id, value },
                    "Version override updated".to_string(),
                )
            }
            other => return Err(Error::invalid(format!("unknown action {other}"))),
        };
        cell.api(request).await?;
        Ok(message)
    }
    .await;
    done(&back, outcome)
}
