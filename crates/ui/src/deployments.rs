use crate::view::{self, Flash, action, count, enc, error_box, page, pill, version_pill, when};
use crate::{App, done, not_found, unexpected};
use axum::extract::{Form, Path, Query, State};
use axum::response::{IntoResponse, Response};
use maud::html;
use serde::Deserialize;
use wfe_core::Error;
use wfe_core::api::*;

pub(crate) async fn list(State(app): State<App>, Path(name): Path<String>, Query(flash): Query<Flash>) -> Response {
    let Some(cell) = app.cell(&name) else { return not_found(&app, &format!("There is no cell named {name}.")) };
    let result = match cell.api(ApiRequest::ListDeployments).await {
        Ok(ApiResponse::Deployments(list)) => Ok(list),
        Ok(other) => Err(unexpected(&other)),
        Err(e) => Err(e),
    };
    let act = format!("/cells/{}/deployments/act", enc(&name));
    let body = html! {
        div.head { h1 { "Deployments" } }
        p.lead {
            "A deployment is a set of workers that ships together; each build of it is a version. "
            "New workflows go to the current version, or to the ramping version for a share of them. "
            "Pinned workflows finish on the version they started on, so a version is safe to shut down once it has drained."
        }
        @match &result {
            Err(e) => (error_box("Could not list deployments", e)),
            Ok(deployments) if deployments.is_empty() => {
                div.panel { (view::empty("No versioned worker has polled this cell yet. Start one with: wfe worker --deployment shop --build-id v1")) }
            }
            Ok(deployments) => {
                @for d in deployments {
                    @let r = &d.routing;
                    h2 { (d.name) }
                    div.panel data-live=(format!("deployment-{}", d.name)) {
                        div.pad style="padding:14px 16px;display:flex;gap:18px;flex-wrap:wrap;align-items:center;border-bottom:1px solid var(--line)" {
                            span { span.muted { "Current " } @match &r.current { Some(b) => strong.mono { (b) }, None => span { "unversioned workers" } } }
                            @if let Some(b) = &r.ramping {
                                span { span.muted { "Ramping " } strong.mono { (b) } " at " (r.ramp_percent) "%" }
                                (action(&act, "Stop ramp", "small", &[("deployment", &d.name), ("op", "unramp")], None))
                            }
                            @if d.propagated_shards >= d.shard_count {
                                (pill("good", "routing on every shard"))
                            } @else {
                                (pill("info", &format!("routing on {} of {} shards", d.propagated_shards, d.shard_count)))
                            }
                            @if r.current.is_some() {
                                (action(&act, "Route to unversioned", "small", &[("deployment", &d.name), ("op", "unversioned")],
                                    Some("Send new workflows to unversioned workers?")))
                            }
                            (action(&act, "Delete deployment", "small danger", &[("deployment", &d.name), ("op", "delete")],
                                Some("Delete this deployment? It must have no versions in use.")))
                        }
                        div.scroll {
                            table {
                                thead { tr {
                                    th { "Build" } th { "Status" } th { "Share of new workflows" } th.num { "Pinned open" }
                                    th { "Task queues" } th { "Registered" } th { "Since" } th {}
                                } }
                                tbody {
                                    @for v in &d.versions {
                                        @let share = match v.status {
                                            VersionStatus::Ramping => r.ramp_percent.min(100),
                                            VersionStatus::Current if r.ramping.is_some() => 100 - r.ramp_percent.min(100),
                                            VersionStatus::Current => 100,
                                            VersionStatus::Inactive | VersionStatus::Draining | VersionStatus::Drained => 0,
                                        };
                                        tr {
                                            td.id { (v.build_id) }
                                            td { (version_pill(v.status)) }
                                            td {
                                                div.meter { div.track { div.fill style=(format!("width:{share}%")) {} } span { (share) "%" } }
                                            }
                                            td.num data-tip=(format!("counted {}", v.drainage_checked_at.rfc3339())) { (count(v.pinned_open)) }
                                            td { (v.task_queues.join(", ")) }
                                            td.tight { (when(v.created_at)) }
                                            td.tight { (when(v.status_changed_at)) }
                                            td.actions {
                                                @if v.status != VersionStatus::Current {
                                                    (action(&act, "Make current", "small", &[("deployment", &d.name), ("op", "current"), ("build", &v.build_id)],
                                                        Some("Send new and auto-upgrading workflows to this build?")))
                                                    form.inline method="post" action=(act) {
                                                        input type="hidden" name="deployment" value=(d.name);
                                                        input type="hidden" name="op" value="ramp";
                                                        input type="hidden" name="build" value=(v.build_id);
                                                        input.narrow type="number" name="percent" min="0" max="100" value=(if v.status == VersionStatus::Ramping { r.ramp_percent } else { 10 }) aria-label="Ramp percent";
                                                        " "
                                                        button.small type="submit" { "Ramp %" }
                                                    }
                                                }
                                                @if matches!(v.status, VersionStatus::Drained | VersionStatus::Inactive) {
                                                    (action(&act, "Delete", "small danger", &[("deployment", &d.name), ("op", "delete_version"), ("build", &v.build_id)],
                                                        Some("Forget this version?")))
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
