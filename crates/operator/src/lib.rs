//! The WFE Kubernetes operator.
//!
//! Three controllers, one per custom resource (see [`crd`]):
//!
//! - **Cell**: runs a cell's nodes as a StatefulSet behind two services,
//!   reports the cell's health, and moves the active role between cells
//!   when their `spec.role`s say so.
//! - **CellReplication**: keeps one cell shipping its changes to another.
//! - **WorkerDeployment**: runs each build of a worker as its own
//!   Deployment, moves new workflows to a new build by the rollout
//!   strategy, and retires builds once no workflow is pinned to them.
//!
//! What to do next is decided by `wfe-control`, the same code the
//! simulator runs; [`plan`] says what Kubernetes objects that needs. This
//! module fetches, asks, and applies.

pub mod crd;
pub mod plan;

use crd::*;
use futures::StreamExt;
use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use k8s_openapi::api::core::v1::Service;
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{Event as Finalize, finalizer};
use kube::runtime::watcher;
use kube::{Client, CustomResourceExt, Resource, ResourceExt};
use serde::Serialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use wfe_control::{FleetBuild, RolloutPolicy, SunsetPolicy, WorkersAction, WorkersInput, plan_workers};
use wfe_core::ErrorCode;
use wfe_sdk::CellSummary;

const MANAGER: &str = "wfe-operator";
const FINALIZER: &str = "wfe.dev/replication";
/// Promote a cell even though the active cell cannot be reached.
pub const ANNOTATION_FORCE: &str = "wfe.dev/force-failover";

#[derive(Clone, Debug)]
pub struct OperatorConfig {
    /// Only this namespace; every namespace when `None`.
    pub namespace: Option<String>,
    /// The image cells run when their spec names none.
    pub image: String,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("kubernetes: {0}")]
    Kube(#[from] kube::Error),
    #[error("cell: {0}")]
    Cell(#[from] wfe_core::Error),
    #[error("{0}")]
    Other(String),
}

type Result<T, E = Error> = std::result::Result<T, E>;

struct Context {
    client: Client,
    config: OperatorConfig,
}

fn drop_formats_kubernetes_does_not_know(schema: &mut serde_json::Value) {
    if let Some(object) = schema.as_object_mut() {
        let unsigned = object.get("format").and_then(|f| f.as_str()).is_some_and(|f| f.starts_with("uint"));
        if unsigned {
            object.remove("format");
        }
        object.values_mut().for_each(drop_formats_kubernetes_does_not_know);
    } else if let Some(items) = schema.as_array_mut() {
        items.iter_mut().for_each(drop_formats_kubernetes_does_not_know);
    }
}

/// The custom resource definitions, as a `kubectl apply -f -` document.
pub fn crds() -> anyhow::Result<String> {
    let mut list = json!({
        "apiVersion": "v1",
        "kind": "List",
        "items": [Cell::crd(), CellReplication::crd(), WorkerDeployment::crd()],
    });
    drop_formats_kubernetes_does_not_know(&mut list);
    Ok(serde_json::to_string_pretty(&list)? + "\n")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

fn namespace_of<K: Resource>(resource: &K) -> String {
    resource.meta().namespace.clone().unwrap_or_else(|| "default".into())
}

async fn apply<K>(api: &Api<K>, object: &K) -> Result<K>
where
    K: Resource + Clone + std::fmt::Debug + Serialize + serde::de::DeserializeOwned,
{
    let name = object.meta().name.clone().unwrap_or_default();
    Ok(api.patch(&name, &PatchParams::apply(MANAGER).force(), &Patch::Apply(object)).await?)
}

async fn set_status<K, S>(api: &Api<K>, name: &str, status: &S) -> Result<()>
where
    K: Resource + Clone + std::fmt::Debug + serde::de::DeserializeOwned,
    S: Serialize,
{
    api.patch_status(name, &PatchParams::default(), &Patch::Merge(json!({ "status": status }))).await?;
    Ok(())
}

fn wfe(cell: &str, namespace: &str) -> wfe_sdk::Client {
    wfe_sdk::Client::connect(&plan::cell_url(cell, namespace))
}

async fn reconcile_role(cell: &Cell, summary: &CellSummary, ctx: &Context) -> Result<String> {
    let (name, namespace) = (cell.name_any(), namespace_of(cell));
    let me = wfe(&name, &namespace);
    let want_active = cell.spec.role == CellRole::Active;
    let partial_view = summary.shards_up < summary.shard_count;
    if partial_view {
        return Ok(String::new());
    }
    match (want_active, summary.role.as_str()) {
        (true, "active") | (false, "standby") => Ok(String::new()),
        (false, _) if summary.epoch <= 1 => {
            me.set_role(false, summary.epoch + 1).await?;
            Ok("started as a standby".into())
        }
        (false, _) => Ok("still active: set the cell that takes over to role Active to fail over".into()),
        (true, _) => {
            let cells: Api<Cell> = Api::namespaced(ctx.client.clone(), &namespace);
            let mut unreachable = Vec::new();
            let mut active = Vec::new();
            let mut epoch = summary.epoch;
            for other in cells.list(&ListParams::default()).await?.items.into_iter().filter(|c| c.name_any() != name) {
                let other_name = other.name_any();
                match wfe(&other_name, &namespace).describe_cell().await {
                    Ok(info) => {
                        let s = CellSummary::of(&info);
                        epoch = epoch.max(s.epoch);
                        if s.role != "standby" {
                            active.push(other);
                        }
                    }
                    Err(_) => unreachable.push(other_name),
                }
            }
            let forced = cell.annotations().get(ANNOTATION_FORCE).is_some_and(|v| v == "true");
            match active.as_slice() {
                [] if unreachable.is_empty() || forced => {
                    me.set_role(true, epoch + 1).await?;
                    Ok(if unreachable.is_empty() {
                        "promoted: no other cell was active".into()
                    } else {
                        format!("promoted by force without {}", unreachable.join(", "))
                    })
                }
                [] => Ok(format!(
                    "not promoted: {} cannot be reached and may be active; annotate this cell {ANNOTATION_FORCE}=true to promote anyway",
                    unreachable.join(", ")
                )),
                [from] if from.spec.role == CellRole::Standby => {
                    let from_name = from.name_any();
                    wfe_sdk::failover(&wfe(&from_name, &namespace), &me, false, 60_000).await?;
                    Ok(format!("took over from {from_name}"))
                }
                [from] => Ok(format!(
                    "not promoted: {} is active and its spec says Active; set it to Standby to fail over",
                    from.name_any()
                )),
                many => Ok(format!("not promoted: {} cells are active", many.len())),
            }
        }
    }
}

async fn reconcile_cell(cell: Arc<Cell>, ctx: Arc<Context>) -> Result<Action> {
    let (name, namespace) = (cell.name_any(), namespace_of(&*cell));
    let (nodes, clients, set) = plan::cell_objects(&cell, &ctx.config.image);
    let services: Api<Service> = Api::namespaced(ctx.client.clone(), &namespace);
    apply(&services, &nodes).await?;
    apply(&services, &clients).await?;
    let set = apply(&Api::<StatefulSet>::namespaced(ctx.client.clone(), &namespace), &set).await?;
    let ready = set.status.as_ref().and_then(|s| s.ready_replicas).unwrap_or(0);

    let mut status = CellStatus {
        url: plan::cell_url(&name, &namespace),
        nodes: format!("{ready}/{}", cell.spec.replicas),
        observed_generation: cell.metadata.generation.unwrap_or(0),
        ..CellStatus::default()
    };
    match wfe(&name, &namespace).describe_cell().await {
        Err(e) => {
            status.phase = if ready == 0 { "Starting" } else { "Unavailable" }.into();
            status.shards = format!("0/{}", cell.spec.shards);
            status.message = e.message;
        }
        Ok(info) => {
            let summary = CellSummary::of(&info);
            status.phase = if summary.shards_up == summary.shard_count { "Ready" } else { "Degraded" }.into();
            status.shards = format!("{}/{}", summary.shards_up, summary.shard_count);
            status.open_workflows = summary.open_workflows;
            status.executions = summary.executions;
            status.message = match reconcile_role(&cell, &summary, &ctx).await {
                Ok(message) => message,
                Err(e) => format!("role: {e}"),
            };
            let after = match status.message.is_empty() {
                true => summary,
                false => wfe(&name, &namespace).describe_cell().await.map(|i| CellSummary::of(&i)).unwrap_or(summary),
            };
            status.role = after.role;
            status.epoch = after.epoch;
        }
    }
    let settled = status.phase == "Ready" && status.message.is_empty();
    set_status(&Api::<Cell>::namespaced(ctx.client.clone(), &namespace), &name, &status).await?;
    Ok(Action::requeue(Duration::from_secs(if settled { 15 } else { 5 })))
}

async fn apply_replication(link: &CellReplication, ctx: &Context) -> Result<Action> {
    let (name, namespace) = (link.name_any(), namespace_of(link));
    let cells: Api<Cell> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut status = CellReplicationStatus { state: "Pending".into(), ..CellReplicationStatus::default() };
    let outcome: Result<()> = async {
        for cell in [&link.spec.source, &link.spec.target] {
            if cells.get_opt(cell).await?.is_none() {
                return Err(Error::Other(format!("there is no Cell named {cell}")));
            }
        }
        let source = wfe(&link.spec.source, &namespace);
        let target = CellSummary::of(&wfe(&link.spec.target, &namespace).describe_cell().await?);
        let mut now = CellSummary::of(&source.describe_cell().await?);
        if !now.replication.contains_key(&target.cell) {
            source.create_replication(target.cell_ref()).await?;
            now = CellSummary::of(&source.describe_cell().await?);
        }
        if let Some((_, paused)) = now.replication.get(&target.cell)
            && *paused != link.spec.paused
        {
            source.pause_replication(target.cell, link.spec.paused).await?;
        }
        let pending = now.replication.get(&target.cell).map_or(0, |r| r.0);
        status.pending = pending;
        status.state = match (link.spec.paused, pending) {
            (true, _) => "Paused",
            (false, 0) => "InSync",
            (false, _) => "CatchingUp",
        }
        .into();
        Ok(())
    }
    .await;
    if let Err(e) = &outcome {
        status.message = e.to_string();
    }
    set_status(&Api::<CellReplication>::namespaced(ctx.client.clone(), &namespace), &name, &status).await?;
    Ok(Action::requeue(Duration::from_secs(if status.state == "InSync" { 15 } else { 5 })))
}

async fn cleanup_replication(link: &CellReplication, ctx: &Context) -> Result<Action> {
    let namespace = namespace_of(link);
    let cells: Api<Cell> = Api::namespaced(ctx.client.clone(), &namespace);
    let source = cells.get_opt(&link.spec.source).await?;
    let target = cells.get_opt(&link.spec.target).await?;
    if let (Some(source), Some(target)) = (source, target)
        && source.metadata.deletion_timestamp.is_none()
    {
        match wfe(&link.spec.source, &namespace).delete_replication(target.spec.cell_id).await {
            Ok(()) => {}
            Err(e) if e.code == ErrorCode::NotFound => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(Action::await_change())
}

async fn reconcile_replication(link: Arc<CellReplication>, ctx: Arc<Context>) -> Result<Action> {
    let api: Api<CellReplication> = Api::namespaced(ctx.client.clone(), &namespace_of(&*link));
    finalizer(&api, FINALIZER, link, |event| async {
        match event {
            Finalize::Apply(link) => apply_replication(&link, &ctx).await,
            Finalize::Cleanup(link) => cleanup_replication(&link, &ctx).await,
        }
    })
    .await
    .map_err(|e| Error::Other(e.to_string()))
}

async fn reconcile_workers(workers: Arc<WorkerDeployment>, ctx: Arc<Context>) -> Result<Action> {
    let (name, namespace) = (workers.name_any(), namespace_of(&*workers));
    let target = plan::target_build(&workers);
    let deployment = plan::deployment_name(&workers);
    let url = plan::cell_url(&workers.spec.cell, &namespace);
    let objects: Api<Deployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let api: Api<WorkerDeployment> = Api::namespaced(ctx.client.clone(), &namespace);
    let mut status = WorkerDeploymentStatus {
        target_build_id: target.clone(),
        observed_generation: workers.metadata.generation.unwrap_or(0),
        ..WorkerDeploymentStatus::default()
    };

    let cell = wfe(&workers.spec.cell, &namespace);
    let info = match cell.describe_deployment(&deployment).await {
        Ok(info) => Some(info),
        Err(e) if e.code == ErrorCode::NotFound => None,
        Err(e) => {
            apply(&objects, &plan::workers_object(&workers, &target, workers.spec.replicas, &url)).await?;
            status.message = format!("cell {}: {}", workers.spec.cell, e.message);
            set_status(&api, &name, &status).await?;
            return Ok(Action::requeue(Duration::from_secs(5)));
        }
    };

    let owned = objects.list(&ListParams::default().labels(&format!("{}={name}", plan::LABEL_WORKERS))).await?;
    let mut fleet: Vec<FleetBuild> = owned
        .iter()
        .filter_map(|object| {
            Some(FleetBuild {
                build: object.annotations().get(plan::LABEL_BUILD)?.clone(),
                replicas: u32::try_from(object.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0)).unwrap_or(0),
                created_ms: object
                    .metadata
                    .creation_timestamp
                    .as_ref()
                    .map_or(0, |t| u64::try_from(t.0.as_millisecond()).unwrap_or(0)),
            })
        })
        .collect();
    let wanted = WorkersInput {
        target: &target,
        replicas: u32::try_from(workers.spec.replicas).unwrap_or(0),
        rollout: &RolloutPolicy::from(&workers.spec.rollout),
        sunset: &SunsetPolicy::from(&workers.spec.sunset),
        info: info.as_ref(),
        fleet: &fleet,
        now_ms: now_ms(),
    };
    let planned = plan_workers(&wanted);
    status.message = planned.message;
    for action in planned.actions {
        match action {
            WorkersAction::Run { build, replicas } => {
                let replicas = i32::try_from(replicas).unwrap_or(i32::MAX);
                apply(&objects, &plan::workers_object(&workers, &build, replicas, &url)).await?;
                match fleet.iter_mut().find(|b| b.build == build) {
                    Some(known) => known.replicas = u32::try_from(replicas).unwrap_or(0),
                    None => fleet.push(FleetBuild {
                        build,
                        replicas: u32::try_from(replicas).unwrap_or(0),
                        created_ms: now_ms(),
                    }),
                }
            }
            WorkersAction::ScaleUp { build, replicas } => {
                let patch = Patch::Merge(json!({ "spec": { "replicas": replicas } }));
                objects.patch(&plan::workers_object_name(&workers, &build), &PatchParams::default(), &patch).await?;
                fleet.iter_mut().filter(|b| b.build == build).for_each(|b| b.replicas = replicas);
                tracing::info!(workers = %name, %build, "scaled a build back up: it is needed again");
            }
            WorkersAction::SetRamp { build, percent } => {
                cell.set_ramping_version(&deployment, Some(&build), percent).await?;
                status.message = format!("ramping {build} to {percent}%");
            }
            WorkersAction::SetCurrent { build } => {
                cell.set_current_version(&deployment, Some(&build)).await?;
                status.message = format!("made {build} current");
            }
            WorkersAction::ClearRamp => {
                cell.set_ramping_version(&deployment, None, 0).await?;
                status.message = "ended the ramp of an abandoned rollout".into();
            }
            WorkersAction::ScaleToZero { build } => {
                let patch = Patch::Merge(json!({ "spec": { "replicas": 0 } }));
                objects.patch(&plan::workers_object_name(&workers, &build), &PatchParams::default(), &patch).await?;
                fleet.iter_mut().filter(|b| b.build == build).for_each(|b| b.replicas = 0);
                tracing::info!(workers = %name, %build, "scaled a drained build to zero");
            }
            WorkersAction::Remove { build } => {
                objects.delete(&plan::workers_object_name(&workers, &build), &DeleteParams::default()).await?;
                if let Err(still_in_use) = cell.delete_version(&deployment, &build).await {
                    tracing::info!(workers = %name, %build, reason = %still_in_use.message, "the cell keeps the version");
                }
                fleet.retain(|b| b.build != build);
                tracing::info!(workers = %name, %build, "deleted a drained build");
            }
        }
    }
    for build in fleet {
        let version = info.as_ref().and_then(|i| i.versions.iter().find(|v| v.build_id == build.build));
        status.builds.push(BuildStatus {
            status: version.map_or("unregistered".into(), |v| format!("{:?}", v.status).to_lowercase()),
            pinned_open: version.map_or(0, |v| v.pinned_open),
            replicas: i32::try_from(build.replicas).unwrap_or(i32::MAX),
            build_id: build.build,
        });
    }
    status.builds.sort_by(|a, b| a.build_id.cmp(&b.build_id));

    if let Ok(info) = cell.describe_deployment(&deployment).await {
        status.current_build_id = info.routing.current.unwrap_or_default();
        status.ramping_build_id = info.routing.ramping.unwrap_or_default();
        status.ramp_percent = info.routing.ramp_percent;
    }
    let settled = status.current_build_id == target && status.builds.len() == 1;
    set_status(&api, &name, &status).await?;
    Ok(Action::requeue(Duration::from_secs(if settled { 30 } else { 5 })))
}

fn on_error<K: Resource<DynamicType = ()>>(resource: Arc<K>, error: &Error, _: Arc<Context>) -> Action {
    tracing::warn!(kind = %K::kind(&()), name = %resource.meta().name.clone().unwrap_or_default(), %error, "reconcile failed");
    Action::requeue(Duration::from_secs(5))
}

/// Run the controllers until the process is stopped.
pub async fn run(config: OperatorConfig) -> anyhow::Result<()> {
    let already_installed_by_the_host_process =
        rustls::crypto::aws_lc_rs::default_provider().install_default().is_err();
    tracing::debug!(already_installed_by_the_host_process, "TLS provider for the Kubernetes client: aws-lc-rs");
    let client = Client::try_default().await?;
    let ctx = Arc::new(Context { client: client.clone(), config: config.clone() });
    fn api<K>(client: &Client, namespace: &Option<String>) -> Api<K>
    where
        K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
        K::DynamicType: Default,
    {
        match namespace {
            Some(namespace) => Api::namespaced(client.clone(), namespace),
            None => Api::all(client.clone()),
        }
    }
    let ns = &config.namespace;
    let watch = watcher::Config::default;
    tracing::info!(namespace = ?ns, image = %config.image, "operator started");

    let cells = Controller::new(api::<Cell>(&client, ns), watch())
        .owns(api::<StatefulSet>(&client, ns), watch())
        .shutdown_on_signal()
        .run(reconcile_cell, on_error, ctx.clone())
        .for_each(|_| async {});
    let links = Controller::new(api::<CellReplication>(&client, ns), watch())
        .shutdown_on_signal()
        .run(reconcile_replication, on_error, ctx.clone())
        .for_each(|_| async {});
    let workers = Controller::new(api::<WorkerDeployment>(&client, ns), watch())
        .owns(api::<Deployment>(&client, ns), watch())
        .shutdown_on_signal()
        .run(reconcile_workers, on_error, ctx.clone())
        .for_each(|_| async {});
    tokio::join!(cells, links, workers);
    Ok(())
}
