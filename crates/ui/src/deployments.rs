use crate::view::{
    self, Action, Confirm, Flash, action, code_block, count, enc, error_box, head, icon, icons, page, pill,
    version_pill, when,
};
use crate::{App, done, not_found, unexpected};
use axum::extract::{Form, Path, Query, State};
use axum::response::{IntoResponse, Response};
use maud::{Markup, html};
use serde::Deserialize;
use wfe_core::Error;
use wfe_core::api::*;

fn share_of_new_workflows(status: VersionStatus, routing: &RoutingConfig) -> u8 {
    let ramp = routing.ramp_percent.min(100);
    match status {
        VersionStatus::Ramping => ramp,
        VersionStatus::Current if routing.ramping.is_some() => 100 - ramp,
        VersionStatus::Current => 100,
        VersionStatus::Inactive | VersionStatus::Draining | VersionStatus::Drained => 0,
    }
}

fn traffic(routing: &RoutingConfig) -> Markup {
    let ramp = if routing.ramping.is_some() { routing.ramp_percent.min(100) } else { 0 };
    let rest = 100 - ramp;
    html! {
        div.traffic {
            div.traffic-bar {
                div.none[routing.current.is_none()] style=(format!("width:{rest}%")) {}
                div.ramp style=(format!("width:{ramp}%")) {}
            }
            div.traffic-legend {
                span {
                    i.none[routing.current.is_none()] {}
                    @match &routing.current { Some(build) => span.mono { (build) }, None => span { "Unversioned workers" } }
                    b { (rest) "%" }
                }
                @if let Some(build) = &routing.ramping {
                    span { i.ramp {} span.mono { (build) } b { (ramp) "%" } }
                }
            }
        }
    }
}

fn deployment_panel(act: &str, d: &DeploymentInfo) -> Markup {
    let r = &d.routing;
    html! {
        div.panel {
            div.panel-head {
                h3 { (d.name) }
                @if d.propagated_shards < d.shard_count {
                    (pill("info pulse", &format!("Routing on {} of {} shards", d.propagated_shards, d.shard_count)))
                }
                span.grow {}
                @if r.ramping.is_some() {
                    (action(&Action {
                        url: act,
                        fields: &[("deployment", &d.name), ("op", "unramp")],
                        class: "btn small",
                        tip: None,
                        confirm: None,
                    }, html! { "Stop ramp" }))
                }
                (view::menu(html! {
                    @if r.current.is_some() {
                        (action(&Action {
                            url: act,
                            fields: &[("deployment", &d.name), ("op", "unversioned")],
                            class: "menu-item",
                            tip: None,
                            confirm: Some(Confirm {
                                title: "Route to unversioned workers?",
                                body: "New workflows stop going to the current build.",
                                cta: "Route to unversioned",
                                danger: false,
                            }),
                        }, html! { (icon(icons::SWAP)) "Route to unversioned" }))
                    }
                    (action(&Action {
                        url: act,
                        fields: &[("deployment", &d.name), ("op", "delete")],
                        class: "menu-item danger",
                        tip: None,
                        confirm: Some(Confirm {
                            title: "Delete this deployment?",
                            body: "It must have no versions in use.",
                            cta: "Delete deployment",
                            danger: true,
                        }),
                    }, html! { (icon(icons::TRASH)) "Delete deployment" }))
                }))
            }
            (traffic(r))
            div.scroll {
                table {
                    thead { tr {
                        th { "Build" } th { "Status" } th { "New workflows" } th.num { "Pinned open" }
                        th { "Task queues" } th { "Registered" } th {}
                    } }
                    tbody {
                        @for v in &d.versions {
                            @let share = share_of_new_workflows(v.status, r);
                            @let ramp_fill = serde_json::json!({
                                "deployment": d.name,
                                "build": v.build_id,
                                "percent": if v.status == VersionStatus::Ramping { r.ramp_percent } else { 10 },
                            });
                            tr {
                                td.id { (v.build_id) }
                                td { span data-tip=(format!("Since {}", v.status_changed_at.rfc3339())) { (version_pill(v.status)) } }
                                td { div.meter { div.track { div.fill style=(format!("width:{share}%")) {} } span { (share) "%" } } }
                                td.num { span data-tip=(format!("Counted {}", v.drainage_checked_at.rfc3339())) { (count(v.pinned_open)) } }
                                td.muted { (v.task_queues.join(", ")) }
                                td.tight.muted { (when(v.created_at)) }
                                td.actions {
                                    @if v.status != VersionStatus::Current {
                                        button.btn.small type="button" data-open="ramp" data-fill=(ramp_fill.to_string()) { "Ramp" }
                                        (action(&Action {
                                            url: act,
                                            fields: &[("deployment", &d.name), ("op", "current"), ("build", &v.build_id)],
                                            class: "btn small",
                                            tip: None,
                                            confirm: Some(Confirm {
                                                title: "Make this build current?",
                                                body: "New and auto-upgrading workflows go to it.",
                                                cta: "Make current",
                                                danger: false,
                                            }),
                                        }, html! { "Make current" }))
                                    }
                                    @if matches!(v.status, VersionStatus::Drained | VersionStatus::Inactive) {
                                        (action(&Action {
                                            url: act,
                                            fields: &[("deployment", &d.name), ("op", "delete_version"), ("build", &v.build_id)],
                                            class: "icon-btn danger",
                                            tip: Some("Delete"),
                                            confirm: Some(Confirm {
                                                title: "Delete this version?",
                                                body: "No open workflow is pinned to it.",
                                                cta: "Delete version",
                                                danger: true,
                                            }),
                                        }, icon(icons::TRASH)))
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

fn ramp_dialog(act: &str) -> Markup {
    view::dialog(
        "ramp",
        html! { "Ramp " span.mono data-slot="build" {} },
        act,
        html! {
            input type="hidden" name="deployment";
            input type="hidden" name="build";
            input type="hidden" name="op" value="ramp";
            p { "Share of new workflows sent to this build." }
            div.range {
                input type="range" name="percent" min="0" max="100" value="10" autofocus aria-label="Percent";
                output { "10%" }
            }
        },
        html! { button.btn.primary type="submit" { "Set ramp" } },
    )
}

pub(crate) async fn list(State(app): State<App>, Path(name): Path<String>, Query(flash): Query<Flash>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let result = match cell.api(ApiRequest::ListDeployments).await {
        Ok(ApiResponse::Deployments(list)) => Ok(list),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let act = format!("/cells/{}/deployments/act", enc(&name));
    let body = html! {
        (head(html! { h1 { "Deployments" } }, html! {}, html! {}))
        @match &result {
            Err(e) => (error_box("Could not list deployments", e)),
            Ok(deployments) if deployments.is_empty() => {
                div.panel {
                    (view::empty(icons::LAYERS, "No deployments yet", html! {
                        span { "Start a versioned worker to create one." }
                        (code_block("wfe worker --deployment shop --build-id v1"))
                    }))
                }
            }
            Ok(deployments) => {
                div.stack { @for d in deployments { (deployment_panel(&act, d)) } }
                (ramp_dialog(&act))
            }
        }
    };
    page("Deployments", &app.cell_nav(&name, "deployments"), &flash, body).into_response()
}

#[derive(Deserialize)]
pub(crate) struct ActForm {
    deployment: String,
    op: String,
    #[serde(default)]
    build: String,
    #[serde(default)]
    percent: Option<u8>,
}

pub(crate) async fn act(State(app): State<App>, Path(name): Path<String>, Form(form): Form<ActForm>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let back = format!("/cells/{}/deployments", enc(&name));
    let deployment = form.deployment.clone();
    let outcome = async {
        let (request, message) = match form.op.as_str() {
            "current" => (
                ApiRequest::SetCurrentVersion { deployment, build_id: Some(form.build.clone()) },
                format!("{} is now the current version of {}", form.build, form.deployment),
            ),
            "unversioned" => (
                ApiRequest::SetCurrentVersion { deployment, build_id: None },
                format!("{} now routes to unversioned workers", form.deployment),
            ),
            "ramp" => {
                let percent = form.percent.unwrap_or(0);
                if percent > 100 {
                    return Err(Error::invalid("a ramp is 0 to 100 percent"));
                }
                (
                    ApiRequest::SetRampingVersion { deployment, build_id: Some(form.build.clone()), percent },
                    format!("{} takes {percent}% of new workflows", form.build),
                )
            }
            "unramp" => {
                (ApiRequest::SetRampingVersion { deployment, build_id: None, percent: 0 }, "Ramp stopped".to_string())
            }
            "delete_version" => (
                ApiRequest::DeleteVersion { version: DeploymentVersion::new(deployment, form.build.clone()) },
                format!("Version {} deleted", form.build),
            ),
            "delete" => {
                (ApiRequest::DeleteDeployment { deployment }, format!("Deployment {} deleted", form.deployment))
            }
            other => return Err(Error::invalid(format!("unknown action {other}"))),
        };
        cell.api(request).await?;
        Ok(message)
    }
    .await;
    done(&back, outcome)
}
