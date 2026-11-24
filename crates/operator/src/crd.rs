//! The custom resources.
//!
//! - [`Cell`]: one sharded, replicated WFE cluster.
//! - [`CellReplication`]: one cell following another.
//! - [`WorkerDeployment`]: versioned workers, rolled out and retired by the
//!   state of the workflows that run on them.

use k8s_openapi::api::core::v1::{PodTemplateSpec, ResourceRequirements};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const GROUP: &str = "wfe.dev";

fn default_shards() -> u32 {
    4
}
fn default_replicas() -> u8 {
    3
}
fn default_storage() -> String {
    "10Gi".into()
}
fn default_data_file_mb() -> u64 {
    1024
}
fn default_one() -> i32 {
    1
}

fn any_object(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({ "type": "object", "x-kubernetes-preserve-unknown-fields": true })
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum CellRole {
    /// Takes writes.
    #[default]
    Active,
    /// Only receives replication; can be promoted.
    Standby,
}

/// A WFE cell: `shards` consensus groups, each on the same `replicas`
/// nodes. The operator runs the nodes as a StatefulSet.
#[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "wfe.dev",
    version = "v1alpha1",
    kind = "Cell",
    namespaced,
    status = "CellStatus",
    shortname = "wcell",
    printcolumn = r#"{"name":"Cell","type":"integer","jsonPath":".spec.cellId"}"#,
    printcolumn = r#"{"name":"Role","type":"string","jsonPath":".status.role"}"#,
    printcolumn = r#"{"name":"Shards","type":"string","jsonPath":".status.shards"}"#,
    printcolumn = r#"{"name":"Nodes","type":"string","jsonPath":".status.nodes"}"#,
    printcolumn = r#"{"name":"Open","type":"integer","jsonPath":".status.openWorkflows"}"#,
    printcolumn = r#"{"name":"Phase","type":"string","jsonPath":".status.phase"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct CellSpec {
    /// The cell's id: unique among cells that replicate to each other.
    /// Cannot change.
    pub cell_id: u32,
    /// Consensus groups. Cannot change (yet): workflows are placed by hash.
    #[serde(default = "default_shards")]
    pub shards: u32,
    /// Nodes; every shard has one replica on each. Use 3 or 5.
    #[serde(default = "default_replicas")]
    pub replicas: u8,
    /// The image to run; the operator's default when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<String>,
    /// The role this cell should have. Changing two cells' roles together
    /// (one to `Standby`, its follower to `Active`) fails over between them.
    #[serde(default)]
    pub role: CellRole,
    /// Size of each node's volume. It must hold `shards` data files.
    #[serde(default = "default_storage")]
    pub storage: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_class_name: Option<String>,
    /// Capacity of each replica's data file, in mebibytes.
    #[serde(default = "default_data_file_mb")]
    pub data_file_mb: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "any_object")]
    pub resources: Option<ResourceRequirements>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CellStatus {
    /// `Starting`, `Ready`, `Degraded` (some shards unavailable) or
    /// `Unavailable`.
    #[serde(default)]
    pub phase: String,
    /// `active`, `standby` or `mixed`, as the cell reports it.
    #[serde(default)]
    pub role: String,
    #[serde(default)]
    pub epoch: u64,
    /// `<available>/<total>`.
    #[serde(default)]
    pub shards: String,
    /// `<ready>/<total>`.
    #[serde(default)]
    pub nodes: String,
    #[serde(default)]
    pub open_workflows: u64,
    #[serde(default)]
    pub executions: u64,
    /// Where clients and workers reach the cell from inside the cluster.
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub observed_generation: i64,
}

/// `source` ships every workflow change to `target`. Both name `Cell`s in
/// this namespace.
#[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "wfe.dev",
    version = "v1alpha1",
    kind = "CellReplication",
    namespaced,
    status = "CellReplicationStatus",
    shortname = "wrepl",
    printcolumn = r#"{"name":"Source","type":"string","jsonPath":".spec.source"}"#,
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".spec.target"}"#,
    printcolumn = r#"{"name":"State","type":"string","jsonPath":".status.state"}"#,
    printcolumn = r#"{"name":"Pending","type":"integer","jsonPath":".status.pending"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct CellReplicationSpec {
    pub source: String,
    pub target: String,
    #[serde(default)]
    pub paused: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CellReplicationStatus {
    /// `InSync`, `CatchingUp`, `Paused` or `Pending` (not established).
    #[serde(default)]
    pub state: String,
    /// Executions the source still has to ship.
    #[serde(default)]
    pub pending: u64,
    #[serde(default)]
    pub message: String,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum RolloutStrategy {
    /// Make a new build current as soon as its workers poll.
    #[default]
    AllAtOnce,
    /// Ramp a new build through `steps`, then make it current.
    Progressive,
    /// Run the new build's workers; a person moves traffic.
    Manual,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct RampStep {
    /// Share of new workflows the new build takes at this step.
    pub ramp_percent: u8,
    /// How long to hold the step before the next one.
    pub pause_seconds: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Rollout {
    #[serde(default)]
    pub strategy: RolloutStrategy,
    #[serde(default)]
    pub steps: Vec<RampStep>,
}

fn default_scaledown() -> u64 {
    300
}
fn default_delete() -> u64 {
    3_600
}

/// What happens to a build's workers once no workflow needs them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct Sunset {
    /// Scale a drained build to zero after this long.
    #[serde(default = "default_scaledown")]
    pub scaledown_after_seconds: u64,
    /// Delete a drained build's workers and forget the version after this long.
    #[serde(default = "default_delete")]
    pub delete_after_seconds: u64,
}

impl Default for Sunset {
    fn default() -> Sunset {
        Sunset { scaledown_after_seconds: default_scaledown(), delete_after_seconds: default_delete() }
    }
}

/// Workers of one WFE deployment. Every change to the pod template is a
/// new build: the operator runs it next to the old ones, moves new
/// workflows to it by the rollout strategy, and retires old builds once
/// the workflows pinned to them have finished.
///
/// The operator sets `WFE_URL`, `WFE_DEPLOYMENT` and `WFE_BUILD_ID` in
/// every container.
#[derive(CustomResource, Clone, Debug, PartialEq, Serialize, Deserialize, JsonSchema)]
#[kube(
    group = "wfe.dev",
    version = "v1alpha1",
    kind = "WorkerDeployment",
    namespaced,
    status = "WorkerDeploymentStatus",
    shortname = "wwd",
    printcolumn = r#"{"name":"Cell","type":"string","jsonPath":".spec.cell"}"#,
    printcolumn = r#"{"name":"Target","type":"string","jsonPath":".status.targetBuildId"}"#,
    printcolumn = r#"{"name":"Current","type":"string","jsonPath":".status.currentBuildId"}"#,
    printcolumn = r#"{"name":"Ramping","type":"string","jsonPath":".status.rampingBuildId"}"#,
    printcolumn = r#"{"name":"Ramp %","type":"integer","jsonPath":".status.rampPercent"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDeploymentSpec {
    /// The `Cell` (in this namespace) the workers poll.
    pub cell: String,
    /// The WFE deployment name; the resource's name when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    /// The build id; a hash of the pod template when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    /// Workers of the target build (and of builds still draining).
    #[serde(default = "default_one")]
    pub replicas: i32,
    #[schemars(schema_with = "any_object")]
    pub template: PodTemplateSpec,
    #[serde(default)]
    pub rollout: Rollout,
    #[serde(default)]
    pub sunset: Sunset,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct BuildStatus {
    pub build_id: String,
    /// The version's status in the cell (`current`, `ramping`, `draining`,
    /// `drained`, `inactive`), or `unregistered` before its workers poll.
    pub status: String,
    /// Open workflows pinned to the build.
    #[serde(default)]
    pub pinned_open: u64,
    #[serde(default)]
    pub replicas: i32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerDeploymentStatus {
    #[serde(default)]
    pub target_build_id: String,
    #[serde(default)]
    pub current_build_id: String,
    #[serde(default)]
    pub ramping_build_id: String,
    #[serde(default)]
    pub ramp_percent: u8,
    #[serde(default)]
    pub builds: Vec<BuildStatus>,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub observed_generation: i64,
}
