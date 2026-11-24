use maud::{DOCTYPE, Markup, PreEscaped, html};
use wfe_core::api::{VersionStatus, WorkflowStatus};
use wfe_core::{Payload, Timestamp};

pub(crate) const CSS: &str = include_str!("style.css");
pub(crate) const JS: &str = include_str!("live.js");

#[derive(Clone, Default)]
pub(crate) struct Nav {
    /// `cells`, `dst`.
    pub section: &'static str,
    /// The cell in view, and which of its tabs.
    pub cell: Option<String>,
    pub tab: &'static str,
}

#[derive(Clone, Default, serde::Deserialize)]
pub(crate) struct Flash {
    #[serde(default)]
    pub ok: Option<String>,
    #[serde(default)]
    pub err: Option<String>,
}

pub(crate) fn page(title: &str, nav: &Nav, flash: &Flash, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · WFE" }
                link rel="stylesheet" href="/assets/style.css";
            }
            body {
                header.top {
                    a.brand href="/" { span.mark { "▚" } "WFE" }
                    nav {
                        a.on[nav.section == "cells"] href="/" { "Cells" }
                        a.on[nav.section == "dst"] href="/dst" { "Simulation" }
                    }
                    span.live title="This page refreshes itself" { span.beat {} "live" }
                }
                @if let Some(cell) = &nav.cell {
                    div.sub {
                        a.crumb href="/" { "Cells" }
                        span.sep { "/" }
                        strong { (cell) }
                        nav.tabs {
                            a.on[nav.tab == "overview"] href=(format!("/cells/{}", enc(cell))) { "Overview" }
                            a.on[nav.tab == "workflows"] href=(format!("/cells/{}/workflows", enc(cell))) { "Workflows" }
                            a.on[nav.tab == "deployments"] href=(format!("/cells/{}/deployments", enc(cell))) { "Deployments" }
                        }
                    }
                }
                main {
                    @if let Some(message) = &flash.ok {
                        div.flash.good role="status" { "✓ " (message) }
                    }
                    @if let Some(message) = &flash.err {
                        div.flash.bad role="alert" { "✕ " (message) }
                    }
                    (body)
                }
                div #tip hidden {}
                script { (PreEscaped(JS)) }
            }
        }
    }
}

pub(crate) fn enc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => out.push(b as char),
            _ => out += &format!("%{b:02X}"),
        }
    }
    out
}

pub(crate) fn pill(kind: &str, label: &str) -> Markup {
    let glyph = match kind {
        "good" => "●",
        "info" => "◐",
        "warn" => "▲",
        "serious" => "◆",
        "bad" => "✕",
        _ => "○",
    };
    html! { span class=(format!("pill {kind}")) { span.glyph aria-hidden="true" { (glyph) } (label) } }
}

pub(crate) fn workflow_pill(status: WorkflowStatus) -> Markup {
    let kind = match status {
        WorkflowStatus::Running => "info",
        WorkflowStatus::Completed => "good",
        WorkflowStatus::Failed => "bad",
        WorkflowStatus::TimedOut => "serious",
        WorkflowStatus::Canceled | WorkflowStatus::Terminated => "warn",
        WorkflowStatus::ContinuedAsNew => "muted",
    };
    pill(kind, &status.as_str().replace('_', " "))
}

pub(crate) fn version_pill(status: VersionStatus) -> Markup {
    let (kind, label) = match status {
        VersionStatus::Current => ("good", "current"),
        VersionStatus::Ramping => ("info", "ramping"),
        VersionStatus::Draining => ("warn", "draining"),
        VersionStatus::Drained => ("muted", "drained"),
        VersionStatus::Inactive => ("muted", "inactive"),
    };
    pill(kind, label)
}

pub(crate) fn role_pill(role: &str) -> Markup {
    let kind = match role {
        "active" => "good",
        "standby" => "info",
        "mixed" => "warn",
        _ => "bad",
    };
    pill(kind, role)
}

pub(crate) fn tile(label: &str, value: impl std::fmt::Display, note: &str) -> Markup {
    html! {
        div.tile {
            div.label { (label) }
            div.value { (value) }
            @if !note.is_empty() { div.note { (note) } }
        }
    }
}

pub(crate) fn count(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

pub(crate) fn compact(n: u64) -> String {
    match n {
        0..10_000 => count(n),
        10_000..1_000_000 => format!("{:.1}k", n as f64 / 1e3),
        1_000_000..1_000_000_000 => format!("{:.1}M", n as f64 / 1e6),
        _ => format!("{:.1}B", n as f64 / 1e9),
    }
}

pub(crate) fn duration_ms(ms: u64) -> String {
    match ms {
        0..1_000 => format!("{ms} ms"),
        1_000..60_000 => format!("{}.{} s", ms / 1_000, ms % 1_000 / 100),
        60_000..3_600_000 => format!("{}m {}s", ms / 60_000, ms % 60_000 / 1_000),
        3_600_000..86_400_000 => format!("{}h {}m", ms / 3_600_000, ms % 3_600_000 / 60_000),
        _ => format!("{}d {}h", ms / 86_400_000, ms % 86_400_000 / 3_600_000),
    }
}

pub(crate) fn now() -> Timestamp {
    Timestamp::from_micros(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX)),
    )
}

pub(crate) fn when(t: Timestamp) -> Markup {
    if t.is_zero() {
        return html! { span.muted { "—" } };
    }
    let ago = now().since_ms(t);
    html! { span data-tip=(t.rfc3339()) { (duration_ms(ago)) " ago" } }
}

pub(crate) fn payload(p: &Payload) -> Markup {
    match p.encoding() {
        Some(wfe_core::payload::ENCODING_NULL) => html! { span.muted { "null" } },
        Some(wfe_core::payload::ENCODING_JSON) if p.metadata.len() == 1 => match p.to_json::<serde_json::Value>() {
            Ok(value) => html! { pre.json { (serde_json::to_string_pretty(&value).unwrap_or_default()) } },
            Err(_) => html! { span.muted { (p.data.len()) " bytes of invalid JSON" } },
        },
        _ => {
            let meta: Vec<String> = p.metadata.iter().map(|(k, v)| format!("{k}={v}")).collect();
            html! { span.muted { (count(p.data.len() as u64)) " bytes · " (meta.join(" · ")) } }
        }
    }
}

pub(crate) fn inline_json(value: &serde_json::Value) -> String {
    if let serde_json::Value::String(s) = value { s.clone() } else { value.to_string() }
}

pub(crate) fn empty(message: &str) -> Markup {
    html! { div.empty { (message) } }
}

pub(crate) fn error_box(what: &str, error: &wfe_core::Error) -> Markup {
    html! {
        div.flash.bad role="alert" {
            "✕ " (what) ": " (error.message) " "
            span.muted { "(" (error.code.as_str()) ")" }
        }
    }
}

pub(crate) fn action(action: &str, label: &str, class: &str, fields: &[(&str, &str)], confirm: Option<&str>) -> Markup {
    html! {
        form.inline method="post" action=(action) data-confirm=[confirm] {
            @for (name, value) in fields {
                input type="hidden" name=(name) value=(value);
            }
            button class=(class) type="submit" { (label) }
        }
    }
}
