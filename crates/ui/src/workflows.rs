use crate::view::{
    self, Flash, code_block, copy_button, count, duration_ms, enc, error_box, head, icon, icons, page, payload, pill,
    sentence, stat, when, workflow_pill,
};
use crate::{App, done, not_found, unexpected};
use axum::extract::{Form, Path, Query, State};
use axum::response::{IntoResponse, Response};
use maud::{Markup, html};
use serde::Deserialize;
use wfe_core::api::*;
use wfe_core::{Error, Id128, Payload, Timestamp};

fn workflow_href(cell: &str, id: &str, run: Option<Id128>) -> String {
    let run = run.map(|r| format!("&run={r}")).unwrap_or_default();
    format!("/cells/{}/workflow?id={}{run}", enc(cell), enc(id))
}

fn version_text(v: &WorkflowVersioning) -> Markup {
    html! {
        @match &v.version { Some(version) => span.mono { (version) }, None => span.faint { "Unversioned" } }
        @match &v.override_ {
            Some(VersioningOverride::Pinned(p)) => { " " (pill("warn", &format!("Pinned to {p}"))) }
            Some(VersioningOverride::AutoUpgrade) => { " " (pill("warn", "Auto-upgrade")) }
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

fn start_dialog(base: &str) -> Markup {
    view::dialog(
        "start",
        html! { "Start a workflow" },
        &format!("{base}/start"),
        html! {
            div.field-row {
                label.field { "Type" input type="text" name="workflow_type" required autofocus placeholder="greet" autocomplete="off" data-1p-ignore; }
                label.field { "Task queue" input type="text" name="task_queue" required placeholder="demo" autocomplete="off" data-1p-ignore; }
            }
            label.field { span { "ID " small { "optional" } } input type="text" name="workflow_id" placeholder="Generated" autocomplete="off" data-1p-ignore; }
            label.field { span { "Input " small { "JSON, optional" } } textarea name="input" spellcheck="false" placeholder="{\"name\": \"Ada\"}" {} }
        },
        html! { button.btn.primary type="submit" { "Start workflow" } },
    )
}

pub(crate) async fn list(State(app): State<App>, Path(name): Path<String>, Query(q): Query<ListQuery>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let some = |s: &String| (!s.trim().is_empty()).then(|| s.trim().to_string());
    let unfiltered_status_so_the_tabs_can_count_every_status = None;
    let filter = ListFilter {
        status: unfiltered_status_so_the_tabs_can_count_every_status,
        workflow_type: some(&q.workflow_type),
        id_prefix: some(&q.prefix),
    };
    let filtered = filter.workflow_type.is_some() || filter.id_prefix.is_some();
    let result = match cell.api(ApiRequest::ListWorkflows { filter, limit: LIST_LIMIT }).await {
        Ok(ApiResponse::Workflows(list)) => Ok(list),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let base = format!("/cells/{}/workflows", enc(&name));
    let with_status = |status: &str| {
        let mut href = base.clone();
        for (key, value) in [("status", status), ("type", q.workflow_type.trim()), ("prefix", q.prefix.trim())] {
            if !value.is_empty() {
                href += if href.contains('?') { "&" } else { "?" };
                href += &format!("{key}={}", enc(value));
            }
        }
        href
    };
    let wanted = WorkflowStatus::parse(&q.status);
    let start_button =
        html! { button.btn.primary type="button" data-open="start" { (icon(icons::PLUS)) "Start workflow" } };
    let body = html! {
        (head(html! { h1 { "Workflows" } }, html! {}, start_button.clone()))
        @match &result {
            Err(e) => (error_box("Could not list workflows", e)),
            Ok(workflows) => {
                @let counts = count_by_status(workflows);
                @let shown: Vec<&WorkflowInfo> = workflows.iter().filter(|w| wanted.is_none_or(|s| w.status == s)).collect();
                div.toolbar {
                    div.tabs {
                        a.tab.on[wanted.is_none()] href=(with_status("")) { "All" b { (count(workflows.len() as u64)) } }
                        @for (status, n) in &counts {
                            a.tab.on[q.status == *status] href=(with_status(status)) { (sentence(status)) b { (count(*n)) } }
                        }
                    }
                    form.filters method="get" action=(base) data-auto {
                        input type="hidden" name="status" value=(q.status);
                        div.search {
                            (icon(icons::SEARCH))
                            input type="text" name="prefix" value=(q.prefix) placeholder="Search by ID" data-search autocomplete="off" data-1p-ignore aria-label="Search by ID";
                        }
                        input.short type="text" name="type" value=(q.workflow_type) placeholder="Type" autocomplete="off" data-1p-ignore aria-label="Type";
                    }
                }
                div.panel {
                    @if shown.is_empty() {
                        @if filtered || wanted.is_some() {
                            (view::empty(icons::SEARCH, "No matches", html! { a.btn href=(base) { "Clear filters" } }))
                        } @else {
                            (view::empty(icons::WORKFLOW, "No workflows yet", start_button))
                        }
                    } @else {
                        div.scroll {
                            table {
                                thead { tr {
                                    th { "ID" } th { "Status" } th { "Type" } th { "Task queue" } th { "Version" }
                                    th { "Started" } th.num { "Duration" }
                                } }
                                tbody {
                                    @for w in &shown {
                                        @let href = workflow_href(&name, &w.workflow_id, Some(w.run_id));
                                        tr data-href=(href) {
                                            td.id.wrap { a href=(href) { (w.workflow_id) } }
                                            td { (workflow_pill(w.status)) }
                                            td { (w.workflow_type) }
                                            td.muted { (w.task_queue) }
                                            td { (version_text(&w.versioning)) }
                                            td.tight.muted { (when(w.start_time)) }
                                            td.num.tight {
                                                @if w.close_time.is_zero() { (duration_ms(view::now().since_ms(w.start_time))) }
                                                @else { (duration_ms(w.close_time.since_ms(w.start_time))) }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        @if u32::try_from(workflows.len()).map_or(true, |n| n >= LIST_LIMIT) {
                            div.panel-foot { "Showing the newest " (count(u64::from(LIST_LIMIT))) " per shard. Search to find older ones." }
                        }
                    }
                }
            }
        }
        (start_dialog(&base))
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

fn spaced(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push(' ');
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

struct Span {
    label: String,
    note: String,
    glyph: &'static str,
    tone: &'static str,
    start: Timestamp,
    end: Option<Timestamp>,
    event: u64,
    instant: bool,
}

fn spans(events: &[Event]) -> Vec<Span> {
    let mut spans: Vec<Span> = Vec::new();
    let close = |spans: &mut Vec<Span>, opened_by: u64, at: Timestamp, tone: &'static str| {
        if let Some(span) = spans.iter_mut().find(|s| s.event == opened_by && !s.instant) {
            span.end = Some(at);
            span.tone = tone;
        }
    };
    let instant = |label: &str, tone: &'static str, e: &Event| Span {
        label: label.to_string(),
        note: String::new(),
        glyph: icons::SEND,
        tone,
        start: e.time,
        end: Some(e.time),
        event: e.id,
        instant: true,
    };
    for e in events {
        match &e.attrs {
            EventAttrs::ActivityScheduled { options, .. } => spans.push(Span {
                label: options.activity_type.clone(),
                note: String::new(),
                glyph: icons::BOLT,
                tone: "info",
                start: e.time,
                end: None,
                event: e.id,
                instant: false,
            }),
            EventAttrs::ActivityStarted { scheduled_id, attempt, .. } => {
                if let Some(span) = spans.iter_mut().find(|s| s.event == *scheduled_id).filter(|_| *attempt > 1) {
                    span.note = format!("{attempt} attempts");
                }
            }
            EventAttrs::ActivityCompleted { scheduled_id, .. } => close(&mut spans, *scheduled_id, e.time, "good"),
            EventAttrs::ActivityFailed { scheduled_id, .. } => close(&mut spans, *scheduled_id, e.time, "bad"),
            EventAttrs::ActivityTimedOut { scheduled_id, .. } => close(&mut spans, *scheduled_id, e.time, "serious"),
            EventAttrs::TimerStarted { fire_after_ms } => spans.push(Span {
                label: "Timer".to_string(),
                note: duration_ms(*fire_after_ms),
                glyph: icons::CLOCK,
                tone: "info",
                start: e.time,
                end: None,
                event: e.id,
                instant: false,
            }),
            EventAttrs::TimerFired { started_id } => close(&mut spans, *started_id, e.time, "muted"),
            EventAttrs::TimerCanceled { started_id } => close(&mut spans, *started_id, e.time, "warn"),
            EventAttrs::SignalReceived { name, .. } => spans.push(instant(name, "info", e)),
            EventAttrs::CancelRequested { .. } => spans.push(instant("Cancel requested", "warn", e)),
            EventAttrs::WorkflowStarted { .. }
            | EventAttrs::WorkflowTaskScheduled { .. }
            | EventAttrs::WorkflowTaskStarted { .. }
            | EventAttrs::WorkflowTaskCompleted { .. }
            | EventAttrs::WorkflowTaskFailed { .. }
            | EventAttrs::WorkflowTaskTimedOut { .. }
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
    spans
}

const GANTT_ROWS: usize = 200;

fn gantt(info: &WorkflowInfo, spans: &[Span], start: Timestamp, end: Timestamp) -> Markup {
    let total_ms = end.since_ms(start).max(1);
    let percent = |t: Timestamp| (t.since_ms(start) as f64 / total_ms as f64 * 100.0).clamp(0.0, 100.0);
    let root_tone = if info.status.is_closed() { "root" } else { "root open" };
    html! {
        div.panel.gantt {
            div.gantt-axis {
                span {}
                div.gantt-ticks {
                    @for quarter in 0..=4u64 {
                        span style=(format!("left:{}%", quarter * 25)) {
                            @if quarter == 0 { "0" } @else { (duration_ms(total_ms * quarter / 4)) }
                        }
                    }
                }
                span {}
            }
            div.gantt-row {
                span.gantt-label { (icon(icons::WORKFLOW)) b { (info.workflow_type) } }
                span.gantt-track { span class=(format!("gantt-bar {root_tone}")) style="left:0%;width:100%" {} }
                span.gantt-dur { (duration_ms(total_ms)) }
            }
            @for s in spans.iter().take(GANTT_ROWS) {
                @let left = percent(s.start);
                @let width = percent(s.end.unwrap_or(end)) - left;
                @let class = format!("gantt-bar {}{}{}", s.tone, if s.end.is_none() { " open" } else { "" }, if s.instant { " point" } else { "" });
                a.gantt-row href=(format!("#e{}", s.event)) data-tip=(format!("Event #{} at +{}", s.event, duration_ms(s.start.since_ms(start)))) {
                    span.gantt-label {
                        (icon(s.glyph))
                        b { (s.label) }
                        @if !s.note.is_empty() { small { (s.note) } }
                    }
                    span.gantt-track {
                        @if s.instant {
                            span class=(class) style=(format!("left:{left:.3}%")) {}
                        } @else {
                            span class=(class) style=(format!("left:{left:.3}%;width:{width:.3}%")) {}
                        }
                    }
                    span.gantt-dur {
                        @if s.instant { "+" (duration_ms(s.start.since_ms(start))) }
                        @else { (duration_ms(s.end.unwrap_or(end).since_ms(s.start))) }
                    }
                }
            }
            @if spans.len() > GANTT_ROWS {
                div.gantt-more { "Showing the first " (GANTT_ROWS) " of " (count(u64::try_from(spans.len()).unwrap_or(u64::MAX))) }
            }
        }
    }
}

fn timeline(events: &[Event], first: wfe_core::Timestamp) -> Markup {
    html! {
        ol.timeline {
            @for e in events {
                li id=(format!("e{}", e.id)) class=(format!("ev {}", event_kind(&e.attrs))) {
                    span.ev-dot {}
                    div.ev-main {
                        span.ev-name { (spaced(e.attrs.name())) }
                        @let (nested, scalars): (Vec<_>, Vec<_>) =
                            event_fields(&e.attrs).into_iter().partition(|(_, value)| value.is_object() || value.is_array());
                        @for (key, value) in &scalars {
                            span.ev-field { (key.replace('_', " ")) " " span.mono { (view::inline_json(value)) } }
                        }
                        @for (key, value) in &nested {
                            details.ev-json {
                                summary { (sentence(key)) }
                                (code_block(&serde_json::to_string_pretty(value).unwrap_or_default()))
                            }
                        }
                    }
                    span.ev-time data-tip=(e.time.rfc3339()) {
                        b { "#" (e.id) }
                        "+" (format!("{:.3}", e.time.micros().saturating_sub(first.micros()) as f64 / 1e6)) "s"
                    }
                }
            }
        }
    }
}

fn workflow_dialogs(act: &str, id: &str, run: &str) -> Markup {
    let target = html! {
        input type="hidden" name="id" value=(id);
        input type="hidden" name="run" value=(run);
    };
    html! {
        (view::dialog("signal", html! { "Send a signal" }, act, html! {
            (target)
            input type="hidden" name="op" value="signal";
            label.field { "Name" input type="text" name="name" required autofocus placeholder="approve" autocomplete="off" data-1p-ignore; }
            label.field { span { "Input " small { "JSON, optional" } } textarea name="input" spellcheck="false" {} }
        }, html! { button.btn.primary type="submit" { "Send signal" } }))
        (view::dialog("cancel", html! { "Cancel this workflow?" }, act, html! {
            (target)
            input type="hidden" name="op" value="cancel";
            p { "The workflow is asked to stop and can clean up first." }
            label.field { span { "Reason " small { "optional" } } input type="text" name="reason" autofocus autocomplete="off" data-1p-ignore; }
        }, html! { button.btn.primary type="submit" { "Request cancel" } }))
        (view::dialog("terminate", html! { "Terminate this workflow?" }, act, html! {
            (target)
            input type="hidden" name="op" value="terminate";
            p { "It stops now, with no chance to clean up." }
            label.field { span { "Reason " small { "optional" } } input type="text" name="reason" autofocus autocomplete="off" data-1p-ignore; }
        }, html! { button.btn.danger.solid type="submit" { "Terminate" } }))
        (view::dialog("override", html! { "Version override" }, act, html! {
            (target)
            input type="hidden" name="op" value="override";
            div.seg {
                label { input type="radio" name="mode" value="pin" checked; span { "Pin" } }
                label { input type="radio" name="mode" value="auto"; span { "Auto-upgrade" } }
                label { input type="radio" name="mode" value="clear"; span { "None" } }
            }
            label.field data-show-when="mode=pin" { "Version" input type="text" name="version" placeholder="shop:v2" autocomplete="off" data-1p-ignore; }
        }, html! { button.btn.primary type="submit" { "Apply" } }))
    }
}

pub(crate) async fn detail(State(app): State<App>, Path(name): Path<String>, Query(q): Query<DetailQuery>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let run = q.run.as_deref().and_then(Id128::parse);
    let described = match cell.api(ApiRequest::DescribeWorkflow { workflow_id: q.id.clone(), run_id: run }).await {
        Ok(ApiResponse::Workflow(d)) => Ok(*d),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let nav = app.cell_nav(&name, "workflows").leaf(&q.id);
    let d = match described {
        Ok(d) => d,
        Err(e) => {
            let body = html! {
                (head(html! { h1.mono { (q.id) } }, html! {}, html! {}))
                (error_box("Could not load the workflow", &e))
            };
            return page(&q.id, &nav, &q.flash, body).into_response();
        }
    };
    let events = history(&cell, &q.id, Some(d.info.run_id)).await;
    let act = format!("/cells/{}/workflow/act", enc(&name));
    let run_text = d.info.run_id.to_string();
    let open = !d.info.status.is_closed();
    let elapsed = if open { view::now() } else { d.info.close_time }.since_ms(d.info.start_time);
    let body = html! {
        (head(
            html! {
                h1.mono { (d.info.workflow_id) }
                (copy_button(&d.info.workflow_id))
                (workflow_pill(d.info.status))
                @if d.cancel_requested && open { (pill("warn", "Cancel requested")) }
            },
            html! {
                span { (d.info.workflow_type) }
                span.sep.mono data-tip="Run ID" { (run_text) }
                (copy_button(&run_text))
            },
            html! {
                @if open {
                    button.btn type="button" data-open="signal" { (icon(icons::SEND)) "Signal" }
                    button.btn type="button" data-open="cancel" { "Cancel" }
                    (view::menu(html! {
                        button.menu-item type="button" data-open="override" { (icon(icons::LAYERS)) "Version override" }
                        button.menu-item.danger type="button" data-open="terminate" { (icon(icons::X)) "Terminate" }
                    }))
                }
            },
        ))
        @if let Some(next) = d.continued_as {
            div.alert.info {
                (icon(icons::ARROW))
                div { "Continued as a new run: " a.link.mono href=(workflow_href(&name, &q.id, Some(next))) { (next) } }
            }
        }
        div.stats {
            div.stat { div.label { "Started" } div.value data-tip=(d.info.start_time.rfc3339()) { (view::ago(d.info.start_time)) } }
            (stat(if open { "Running for" } else { "Ran for" }, duration_ms(elapsed), ""))
            (stat("Events", count(d.info.history_length), ""))
            (stat("Pending activities", d.pending_activities.len(), ""))
            (stat("Pending timers", d.pending_timers, ""))
        }
        @if let Ok(events) = &events {
            @let spans = spans(events);
            @if !spans.is_empty() {
                @let start = events.first().map_or(d.info.start_time, |e| e.time);
                @let end = if open { view::now() } else { events.last().map_or(d.info.close_time, |e| e.time) };
                h2 { "Timeline" span.count { (spans.len()) } }
                (gantt(&d.info, &spans, start, end))
            }
        }
        div.split.wide-left {
            div.stack {
                div.panel.pad { div.panel-title { "Input" } (payload(&d.input)) }
                @if let Some(result) = &d.result {
                    div.panel.pad { div.panel-title { "Result" } (payload(result)) }
                }
                @if let Some(failure) = &d.failure {
                    div.panel.pad {
                        div.panel-title { "Failure" @if !failure.kind.is_empty() { (pill("bad", &failure.kind)) } }
                        div { (failure.message) }
                        @if let Some(details) = &failure.details { div style="margin-top:10px" { (payload(details)) } }
                    }
                }
            }
            div.panel.pad {
                dl.kv {
                    dt { "Task queue" } dd { (d.info.task_queue) }
                    dt { "Shard" } dd { (d.info.shard) }
                    dt { "Version" } dd { (version_text(&d.info.versioning)) }
                    @match d.info.versioning.behavior {
                        VersioningBehavior::Pinned => { dt { "Upgrades" } dd { "Pinned" } }
                        VersioningBehavior::AutoUpgrade => { dt { "Upgrades" } dd { "Automatic" } }
                        VersioningBehavior::Unspecified => {}
                    }
                    @if !matches!(d.workflow_task.as_str(), "" | "none") { dt { "Workflow task" } dd { (sentence(&d.workflow_task)) } }
                    @if !d.info.close_time.is_zero() { dt { "Closed" } dd { (when(d.info.close_time)) } }
                }
            }
        }
        @if !d.pending_activities.is_empty() {
            h2 { "Pending activities" span.count { (d.pending_activities.len()) } }
            div.panel.scroll {
                table {
                    thead { tr { th { "Activity" } th { "State" } th.num { "Attempt" } th { "Task queue" } th { "Last failure" } } }
                    tbody {
                        @for a in &d.pending_activities {
                            tr {
                                td { (a.activity_type) " " span.faint { "#" (a.scheduled_id) } }
                                td { (pill(match a.state.as_str() { "started" => "info pulse", "backoff" => "warn", _ => "muted" }, &sentence(&a.state))) }
                                td.num { (a.attempt) }
                                td.muted { (a.task_queue) }
                                td.wrap { @match &a.last_failure { Some(f) => (f.to_string()), None => span.faint { "—" } } }
                            }
                        }
                    }
                }
            }
        }
        h2 { "History" span.count { (count(d.info.history_length)) } }
        div.panel {
            @match &events {
                Err(e) => div.pad { (error_box("Could not load the history", e)) },
                Ok(events) => (timeline(events, events.first().map_or(d.info.start_time, |e| e.time))),
            }
        }
        @if open { (workflow_dialogs(&act, &q.id, &run_text)) }
    };
    page(&q.id, &nav.polling(open), &q.flash, body).into_response()
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
