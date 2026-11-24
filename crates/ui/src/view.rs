use maud::{DOCTYPE, Markup, PreEscaped, html};
use wfe_core::api::{VersionStatus, WorkflowStatus};
use wfe_core::{Payload, Timestamp};

pub(crate) const CSS: &str = include_str!("style.css");
pub(crate) const JS: &str = include_str!("app.js");

const THEME_BEFORE_FIRST_PAINT: &str = r#"(function(){var t;try{t=localStorage.getItem("wfe-theme")}catch(e){}document.documentElement.dataset.theme=t||(matchMedia("(prefers-color-scheme: dark)").matches?"dark":"light")})()"#;

pub(crate) mod icons {
    pub(crate) const LOGO: &str = r#"<path d="M5 7c4 0 4 10 7 10s3-10 7-10"/><path d="M5 17c4 0 4-10 7-10"/>"#;
    pub(crate) const GRID: &str = r#"<rect x="3.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="3.5" width="7" height="7" rx="1.5"/><rect x="3.5" y="13.5" width="7" height="7" rx="1.5"/><rect x="13.5" y="13.5" width="7" height="7" rx="1.5"/>"#;
    pub(crate) const CELL: &str = r#"<path d="M12 2.8 20 7.4v9.2l-8 4.6-8-4.6V7.4z"/>"#;
    pub(crate) const WORKFLOW: &str =
        r#"<circle cx="6" cy="6" r="2.5"/><circle cx="18" cy="18" r="2.5"/><path d="M6 8.5V13a5 5 0 0 0 5 5h4.5"/>"#;
    pub(crate) const LAYERS: &str = r#"<path d="m12 3 9 5-9 5-9-5z"/><path d="m3 12.5 9 5 9-5"/>"#;
    pub(crate) const PULSE: &str = r#"<path d="M3 12h4l3-7 4 14 3-7h4"/>"#;
    pub(crate) const PLUS: &str = r#"<path d="M12 5v14M5 12h14"/>"#;
    pub(crate) const X: &str = r#"<path d="M6 6l12 12M18 6 6 18"/>"#;
    pub(crate) const CHECK: &str = r#"<path d="m5 12.5 4.5 4.5L19 7.5"/>"#;
    pub(crate) const COPY: &str =
        r#"<rect x="9" y="9" width="11" height="11" rx="2"/><path d="M5 15V6a2 2 0 0 1 2-2h8"/>"#;
    pub(crate) const CHEVRON: &str = r#"<path d="m9 6 6 6-6 6"/>"#;
    pub(crate) const MORE: &str =
        r#"<circle cx="5" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/>"#;
    pub(crate) const SUN: &str = r#"<circle cx="12" cy="12" r="4"/><path d="M12 2.5v2M12 19.5v2M2.5 12h2M19.5 12h2M5.3 5.3l1.4 1.4M17.3 17.3l1.4 1.4M5.3 18.7l1.4-1.4M17.3 6.7l1.4-1.4"/>"#;
    pub(crate) const MOON: &str = r#"<path d="M20 14.5A8 8 0 0 1 9.5 4a8 8 0 1 0 10.5 10.5z"/>"#;
    pub(crate) const SEARCH: &str = r#"<circle cx="11" cy="11" r="6.5"/><path d="m16 16 4.5 4.5"/>"#;
    pub(crate) const PAUSE: &str = r#"<path d="M9 5v14M15 5v14"/>"#;
    pub(crate) const PLAY: &str = r#"<path d="M7 4.5v15l12-7.5z"/>"#;
    pub(crate) const TRASH: &str =
        r#"<path d="M4 7h16M10 11v6M14 11v6M6 7l1 12a2 2 0 0 0 2 2h6a2 2 0 0 0 2-2l1-12M9 7V4h6v3"/>"#;
    pub(crate) const ALERT: &str = r#"<path d="M12 4 2.5 20h19z"/><path d="M12 10v4M12 17v.5"/>"#;
    pub(crate) const MENU: &str = r#"<path d="M4 7h16M4 12h16M4 17h16"/>"#;
    pub(crate) const ARROW: &str = r#"<path d="M5 12h14M13 6l6 6-6 6"/>"#;
    pub(crate) const SWAP: &str = r#"<path d="M4 8h14M14 4l4 4-4 4M20 16H6M10 12l-4 4 4 4"/>"#;
    pub(crate) const SELECT: &str = r#"<path d="m8 9 4-4 4 4M8 15l4 4 4-4"/>"#;
    pub(crate) const SERVER: &str = r#"<rect x="3.5" y="4" width="17" height="6.5" rx="1.5"/><rect x="3.5" y="13.5" width="17" height="6.5" rx="1.5"/><path d="M7 7.25h.01M7 16.75h.01"/>"#;
    pub(crate) const LIST: &str = r#"<path d="M8 6h12M8 12h12M8 18h12M4 6h.01M4 12h.01M4 18h.01"/>"#;
    pub(crate) const SHIELD: &str =
        r#"<path d="M12 3 5 6v5c0 4.5 3 8 7 10 4-2 7-5.5 7-10V6z"/><path d="m9 12 2 2 4-4"/>"#;
    pub(crate) const FLAG: &str = r#"<path d="M5 21V4M5 4h11l-2 4 2 4H5"/>"#;
    pub(crate) const SEND: &str = r#"<path d="M21 3 10 14M21 3l-7 18-4-7-7-4z"/>"#;
}

pub(crate) fn icon(paths: &str) -> Markup {
    html! {
        svg.icon viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" {
            (PreEscaped(paths))
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct Nav {
    pub section: &'static str,
    pub cells: Vec<String>,
    pub cell: Option<String>,
    pub chosen: Option<String>,
    pub tab: &'static str,
    pub leaf: Option<String>,
    pub polls_for_changes: bool,
}

impl Nav {
    pub(crate) fn leaf(mut self, leaf: &str) -> Nav {
        self.leaf = Some(leaf.to_string());
        self
    }

    pub(crate) fn tab(mut self, tab: &'static str) -> Nav {
        self.tab = tab;
        self
    }

    pub(crate) fn polling(mut self, polls_for_changes: bool) -> Nav {
        self.polls_for_changes = polls_for_changes;
        self
    }

    fn crumbs(&self) -> Vec<(String, String)> {
        let mut crumbs = Vec::new();
        if let Some(cell) = &self.cell {
            let base = format!("/cells/{}", enc(cell));
            crumbs.push((cell.clone(), base.clone()));
            let (label, path) = match self.tab {
                "workflows" => ("Workflows", "/workflows"),
                "deployments" => ("Deployments", "/deployments"),
                _ => ("Cluster", ""),
            };
            crumbs.push((label.to_string(), format!("{base}{path}")));
        } else if self.section == "cells" {
            crumbs.push(("Cells".to_string(), "/".to_string()));
        } else if self.section == "replication" {
            crumbs.push(("Replication".to_string(), "/replication".to_string()));
        } else if self.section == "dst" {
            crumbs.push(("Simulation".to_string(), "/dst".to_string()));
            if !self.tab.is_empty() {
                crumbs.push((sentence(self.tab), format!("/dst/{}", self.tab)));
            }
        }
        if let Some(leaf) = &self.leaf {
            crumbs.push((leaf.clone(), String::new()));
        }
        crumbs
    }
}

#[derive(Clone, Default, serde::Deserialize)]
pub(crate) struct Flash {
    #[serde(default)]
    pub ok: Option<String>,
    #[serde(default)]
    pub err: Option<String>,
}

fn item(on: bool, href: &str, glyph: &str, label: &str) -> Markup {
    html! { a.item.on[on] href=(href) { (icon(glyph)) span { (label) } } }
}

fn switcher(nav: &Nav, chosen: &str) -> Markup {
    let same_page = match (nav.cell.is_some(), nav.tab) {
        (true, "deployments") => "/deployments",
        (true, "overview") => "",
        _ => "/workflows",
    };
    html! {
        @if nav.cells.len() > 1 {
            details.menu.switcher {
                summary.switch aria-label="Switch cell" { (icon(icons::CELL)) span { (chosen) } (icon(icons::SELECT)) }
                div.menu-list {
                    @for name in &nav.cells {
                        a.menu-item href=(format!("/cells/{}{same_page}", enc(name))) {
                            (icon(icons::CELL)) span { (name) }
                            @if name.as_str() == chosen { span.check { (icon(icons::CHECK)) } }
                        }
                    }
                }
            }
        } @else {
            div.switcher.switch { (icon(icons::CELL)) span { (chosen) } }
        }
    }
}

fn sidebar(nav: &Nav) -> Markup {
    let in_cell = |tab: &str| nav.cell.is_some() && nav.tab == tab;
    let in_dst = |tab: &str| nav.section == "dst" && nav.tab == tab;
    html! {
        aside.side #side data-cell=[nav.cell.as_deref()] {
            a.brand href="/" { span.logo { (icon(icons::LOGO)) } "WorkflowEngine" }
            nav.side-nav {
                @if let Some(chosen) = &nav.chosen {
                    @let base = format!("/cells/{}", enc(chosen));
                    (switcher(nav, chosen))
                    (item(in_cell("workflows"), &format!("{base}/workflows"), icons::WORKFLOW, "Workflows"))
                    (item(in_cell("deployments"), &format!("{base}/deployments"), icons::LAYERS, "Deployments"))
                    (item(in_cell("overview"), &base, icons::SERVER, "Cluster"))
                }
                div.side-label { "Platform" }
                (item(nav.section == "cells" && nav.cell.is_none(), "/", icons::GRID, "Cells"))
                (item(nav.section == "replication", "/replication", icons::SWAP, "Replication"))
                div.side-label { "Simulation" }
                (item(in_dst(""), "/dst", icons::PULSE, "Overview"))
                (item(in_dst("failures"), "/dst/failures", icons::ALERT, "Failures"))
                (item(in_dst("runs"), "/dst/runs", icons::LIST, "Runs"))
                (item(in_dst("coverage"), "/dst/coverage", icons::SHIELD, "Coverage"))
                (item(in_dst("findings"), "/dst/findings", icons::FLAG, "Findings"))
            }
        }
    }
}

fn top_bar(nav: &Nav) -> Markup {
    let crumbs = nav.crumbs();
    html! {
        header.bar #bar {
            button.icon-btn.side-toggle type="button" data-side-toggle aria-label="Menu" { (icon(icons::MENU)) }
            nav.crumbs aria-label="Breadcrumb" {
                @for (i, (label, href)) in crumbs.iter().enumerate() {
                    @if i > 0 { (icon(icons::CHEVRON)) }
                    @if i + 1 == crumbs.len() { span.here { (label) } } @else { a href=(href) { (label) } }
                }
            }
            div.bar-end {
                span.live { span.beat {} span.on { "Live" } span.off { "Reconnecting" } }
                button.icon-btn type="button" data-theme-toggle aria-label="Switch theme" data-tip="Switch theme" {
                    span.moon { (icon(icons::MOON)) }
                    span.sun { (icon(icons::SUN)) }
                }
            }
        }
    }
}

pub(crate) fn page(title: &str, nav: &Nav, flash: &Flash, body: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " · WorkflowEngine" }
                script { (PreEscaped(THEME_BEFORE_FIRST_PAINT)) }
                link rel="stylesheet" href="/assets/style.css";
                script defer src="/assets/app.js" {}
            }
            body {
                div #progress .progress {}
                (sidebar(nav))
                div.main {
                    (top_bar(nav))
                    main #content .enter data-live[nav.polls_for_changes] { (body) }
                }
                div #toasts aria-live="polite" {
                    @if let Some(message) = &flash.ok { (toast("good", "status", message)) }
                    @if let Some(message) = &flash.err { (toast("bad", "alert", message)) }
                }
                div #tip role="tooltip" hidden {}
                template #skeleton {
                    div.sk.sk-title {}
                    div.sk-stats { div.sk {} div.sk {} div.sk {} div.sk {} }
                    div.sk-table { div.sk {} div.sk {} div.sk {} div.sk {} div.sk {} div.sk {} }
                }
                dialog #confirm aria-labelledby="confirm-title" {
                    div.dialog-box {
                        header.dialog-head { h2 #confirm-title {} }
                        div.dialog-body { p #confirm-body {} }
                        footer.dialog-foot {
                            button.btn type="button" data-close { "Cancel" }
                            button.btn.primary #confirm-ok type="button" { "Confirm" }
                        }
                    }
                }
            }
        }
    }
}

fn toast(kind: &str, role: &str, message: &str) -> Markup {
    html! {
        div class=(format!("toast {kind}")) role=(role) {
            span { (message) }
            button.toast-x type="button" aria-label="Dismiss" { "×" }
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

pub(crate) fn head(title: Markup, meta: Markup, actions: Markup) -> Markup {
    html! {
        div.head {
            div.head-main {
                div.head-title { (title) }
                @if !meta.0.is_empty() { div.head-meta { (meta) } }
            }
            @if !actions.0.is_empty() { div.head-actions { (actions) } }
        }
    }
}

pub(crate) fn sentence(words: &str) -> String {
    let spaced = words.replace('_', " ");
    let mut chars = spaced.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

pub(crate) fn pill(kind: &str, label: &str) -> Markup {
    html! { span class=(format!("pill {kind}")) { span.dot {} (label) } }
}

pub(crate) fn workflow_pill(status: WorkflowStatus) -> Markup {
    let kind = match status {
        WorkflowStatus::Running => "info pulse",
        WorkflowStatus::Completed => "good",
        WorkflowStatus::Failed => "bad",
        WorkflowStatus::TimedOut => "serious",
        WorkflowStatus::Canceled | WorkflowStatus::Terminated => "warn",
        WorkflowStatus::ContinuedAsNew => "muted",
    };
    pill(kind, &sentence(status.as_str()))
}

pub(crate) fn version_pill(status: VersionStatus) -> Markup {
    let (kind, label) = match status {
        VersionStatus::Current => ("good", "Current"),
        VersionStatus::Ramping => ("info pulse", "Ramping"),
        VersionStatus::Draining => ("warn", "Draining"),
        VersionStatus::Drained => ("muted", "Drained"),
        VersionStatus::Inactive => ("muted", "Inactive"),
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
    pill(kind, &sentence(role))
}

pub(crate) fn stat(label: &str, value: impl std::fmt::Display, note: &str) -> Markup {
    html! {
        div.stat {
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

pub(crate) fn ago(t: Timestamp) -> String {
    let ms = now().since_ms(t);
    if ms < 5_000 {
        "just now".to_string()
    } else {
        format!("{} ago", duration_ms(ms / 1_000 * 1_000).replace(".0 s", "s"))
    }
}

pub(crate) fn when(t: Timestamp) -> Markup {
    if t.is_zero() {
        return html! { span.faint { "—" } };
    }
    html! { span data-tip=(t.rfc3339()) { (ago(t)) } }
}

pub(crate) fn copy_button(text: &str) -> Markup {
    html! {
        button.icon-btn.copy.small type="button" data-copy=(text) aria-label="Copy" {
            (icon(icons::COPY)) (icon(icons::CHECK))
        }
    }
}

pub(crate) fn code_block(text: &str) -> Markup {
    html! {
        div.code-wrap {
            pre.code { (text) }
            (copy_button(text))
        }
    }
}

pub(crate) fn payload(p: &Payload) -> Markup {
    match p.encoding() {
        Some(wfe_core::payload::ENCODING_NULL) => html! { span.faint { "None" } },
        Some(wfe_core::payload::ENCODING_JSON) if p.metadata.len() == 1 => match p.to_json::<serde_json::Value>() {
            Ok(value) => code_block(&serde_json::to_string_pretty(&value).unwrap_or_default()),
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

pub(crate) fn empty(icon_paths: &str, title: &str, more: Markup) -> Markup {
    html! {
        div.empty {
            div.empty-icon { (icon(icon_paths)) }
            strong { (title) }
            (more)
        }
    }
}

pub(crate) fn error_box(what: &str, error: &wfe_core::Error) -> Markup {
    html! {
        div.alert role="alert" {
            (icon(icons::ALERT))
            div { strong { (what) } div.muted { (error.message) " (" (error.code.as_str()) ")" } }
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Confirm<'a> {
    pub title: &'a str,
    pub body: &'a str,
    pub cta: &'a str,
    pub danger: bool,
}

pub(crate) struct Action<'a> {
    pub url: &'a str,
    pub fields: &'a [(&'a str, &'a str)],
    pub class: &'a str,
    pub tip: Option<&'a str>,
    pub confirm: Option<Confirm<'a>>,
}

pub(crate) fn action(a: &Action<'_>, label: Markup) -> Markup {
    html! {
        form.inline method="post" action=(a.url)
            data-confirm=[a.confirm.map(|c| c.title)]
            data-confirm-body=[a.confirm.map(|c| c.body)]
            data-confirm-cta=[a.confirm.map(|c| c.cta)]
            data-danger[a.confirm.is_some_and(|c| c.danger)] {
            @for (name, value) in a.fields {
                input type="hidden" name=(name) value=(value);
            }
            button class=(a.class) type="submit" data-tip=[a.tip] aria-label=[a.tip] { (label) }
        }
    }
}

pub(crate) fn dialog(id: &str, title: Markup, action: &str, fields: Markup, submit: Markup) -> Markup {
    html! {
        dialog id=(id) aria-labelledby=(format!("{id}-title")) {
            form.dialog-box method="post" action=(action) {
                header.dialog-head {
                    h2 id=(format!("{id}-title")) { (title) }
                    button.icon-btn type="button" data-close aria-label="Close" { (icon(icons::X)) }
                }
                div.dialog-body {
                    div.dialog-error role="alert" hidden {}
                    (fields)
                }
                footer.dialog-foot {
                    button.btn type="button" data-close { "Cancel" }
                    (submit)
                }
            }
        }
    }
}

pub(crate) fn menu(items: Markup) -> Markup {
    html! {
        details.menu {
            summary.btn aria-label="More actions" { (icon(icons::MORE)) }
            div.menu-list { (items) }
        }
    }
}
