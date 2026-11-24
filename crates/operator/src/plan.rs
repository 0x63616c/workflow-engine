//! What Kubernetes objects a resource needs, as pure functions. What a
//! rollout or a failover does next is decided in `wfe-control`, which the
//! simulator drives too.

use crate::crd::*;
use k8s_openapi::api::apps::v1::{Deployment, DeploymentSpec, StatefulSet, StatefulSetSpec};
use k8s_openapi::api::core::v1::{
    Container, ContainerPort, EnvVar, EnvVarSource, HTTPGetAction, ObjectFieldSelector, PersistentVolumeClaim,
    PersistentVolumeClaimSpec, PodSpec, PodTemplateSpec, Probe, Service, ServicePort, ServiceSpec, VolumeMount,
    VolumeResourceRequirements,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::{Resource, ResourceExt};
use std::collections::BTreeMap;
use wfe_control::{RolloutPolicy, SunsetPolicy};

pub const HTTP_PORT: i32 = 7400;
pub const CLUSTER_PORT: i32 = 7401;
pub const LABEL_CELL: &str = "wfe.dev/cell";
pub const LABEL_WORKERS: &str = "wfe.dev/worker-deployment";
const START_A_MAJORITY_TOGETHER: &str = "Parallel";
const POD_ORDINAL: &str = "metadata.labels['apps.kubernetes.io/pod-index']";
pub const LABEL_BUILD: &str = "wfe.dev/build-id";

fn owner<K: Resource<DynamicType = ()>>(resource: &K) -> Vec<OwnerReference> {
    resource.controller_owner_ref(&()).into_iter().collect()
}

fn labels(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    let mut map: BTreeMap<String, String> = pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    map.insert("app.kubernetes.io/managed-by".into(), "wfe-operator".into());
    map
}

/// The headless service that names a cell's nodes.
pub fn nodes_service_name(cell: &str) -> String {
    format!("{cell}-nodes")
}

/// Where clients reach a cell from inside the cluster.
pub fn cell_url(cell: &str, namespace: &str) -> String {
    format!("http://{cell}.{namespace}.svc:{HTTP_PORT}")
}

/// The cluster addresses of a cell's nodes, in node order.
pub fn cell_peers(cell: &str, namespace: &str, replicas: u8) -> Vec<String> {
    (0..replicas).map(|i| format!("{cell}-{i}.{}.{namespace}.svc:{CLUSTER_PORT}", nodes_service_name(cell))).collect()
}

/// The services and the StatefulSet that run a cell.
pub fn cell_objects(cell: &Cell, default_image: &str) -> (Service, Service, StatefulSet) {
    let name = cell.name_any();
    let namespace = cell.namespace().unwrap_or_else(|| "default".into());
    let spec = &cell.spec;
    let selector = labels(&[(LABEL_CELL, &name)]);
    let meta = |object: String| ObjectMeta {
        name: Some(object),
        namespace: Some(namespace.clone()),
        labels: Some(selector.clone()),
        owner_references: Some(owner(cell)),
        ..ObjectMeta::default()
    };
    let port = |name: &str, port: i32| ServicePort {
        name: Some(name.into()),
        port,
        target_port: Some(IntOrString::Int(port)),
        ..ServicePort::default()
    };

    let nodes_found_before_ready = Some(true);
    let nodes = Service {
        metadata: meta(nodes_service_name(&name)),
        spec: Some(ServiceSpec {
            cluster_ip: Some("None".into()),
            publish_not_ready_addresses: nodes_found_before_ready,
            selector: Some(selector.clone()),
            ports: Some(vec![port("cluster", CLUSTER_PORT), port("http", HTTP_PORT)]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };
    let clients = Service {
        metadata: meta(name.clone()),
        spec: Some(ServiceSpec {
            selector: Some(selector.clone()),
            ports: Some(vec![port("http", HTTP_PORT)]),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    };

    let args: Vec<String> = [
        "server".to_string(),
        format!("--cell={}", spec.cell_id),
        format!("--shards={}", spec.shards),
        format!("--replicas={}", spec.replicas),
        "--data-dir=/data".to_string(),
        format!("--data-file-mb={}", spec.data_file_mb),
        format!("--listen=0.0.0.0:{CLUSTER_PORT}"),
        format!("--http=0.0.0.0:{HTTP_PORT}"),
        format!("--peers={}", cell_peers(&name, &namespace, spec.replicas).join(",")),
    ]
    .into();
    let container = Container {
        name: "wfe".into(),
        image: Some(spec.image.clone().unwrap_or_else(|| default_image.to_string())),
        args: Some(args),
        env: Some(vec![EnvVar {
            name: "WFE_NODE".into(),
            value_from: Some(EnvVarSource {
                field_ref: Some(ObjectFieldSelector {
                    field_path: POD_ORDINAL.into(),
                    ..ObjectFieldSelector::default()
                }),
                ..EnvVarSource::default()
            }),
            ..EnvVar::default()
        }]),
        ports: Some(vec![
            ContainerPort { name: Some("http".into()), container_port: HTTP_PORT, ..ContainerPort::default() },
            ContainerPort { name: Some("cluster".into()), container_port: CLUSTER_PORT, ..ContainerPort::default() },
        ]),
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/healthz".into()),
                port: IntOrString::Int(HTTP_PORT),
                ..HTTPGetAction::default()
            }),
            period_seconds: Some(5),
            ..Probe::default()
        }),
        volume_mounts: Some(vec![VolumeMount {
            name: "data".into(),
            mount_path: "/data".into(),
            ..VolumeMount::default()
        }]),
        resources: spec.resources.clone(),
        ..Container::default()
    };
    let set = StatefulSet {
        metadata: meta(name.clone()),
        spec: Some(StatefulSetSpec {
            service_name: Some(nodes_service_name(&name)),
            replicas: Some(i32::from(spec.replicas)),
            pod_management_policy: Some(START_A_MAJORITY_TOGETHER.into()),
            selector: LabelSelector { match_labels: Some(selector.clone()), ..LabelSelector::default() },
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta { labels: Some(selector.clone()), ..ObjectMeta::default() }),
                spec: Some(PodSpec {
                    containers: vec![container],
                    termination_grace_period_seconds: Some(10),
                    ..PodSpec::default()
                }),
            },
            volume_claim_templates: Some(vec![PersistentVolumeClaim {
                metadata: ObjectMeta { name: Some("data".into()), ..ObjectMeta::default() },
                spec: Some(PersistentVolumeClaimSpec {
                    access_modes: Some(vec!["ReadWriteOnce".into()]),
                    storage_class_name: spec.storage_class_name.clone(),
                    resources: Some(VolumeResourceRequirements {
                        requests: Some([("storage".to_string(), Quantity(spec.storage.clone()))].into()),
                        ..VolumeResourceRequirements::default()
                    }),
                    ..PersistentVolumeClaimSpec::default()
                }),
                ..PersistentVolumeClaim::default()
            }]),
            ..StatefulSetSpec::default()
        }),
        ..StatefulSet::default()
    };
    (nodes, clients, set)
}

/// The WFE deployment a `WorkerDeployment` manages.
pub fn deployment_name(workers: &WorkerDeployment) -> String {
    workers.spec.deployment.clone().unwrap_or_else(|| workers.name_any())
}

/// The build the spec asks for: named, or derived from the pod template so
/// that any change to it is a new version.
pub fn target_build(workers: &WorkerDeployment) -> String {
    if let Some(build) = workers.spec.build_id.as_ref().filter(|b| !b.is_empty()) {
        return build.clone();
    }
    let template = serde_json::to_vec(&workers.spec.template).unwrap_or_default();
    format!("{:032x}", wfe_core::checksum(&template))[..10].to_string()
}

/// A build id as part of a Kubernetes object name.
pub fn dns_label(text: &str) -> String {
    let mut out: String =
        text.to_ascii_lowercase().chars().map(|c| if c.is_ascii_alphanumeric() { c } else { '-' }).collect();
    out = out.trim_matches('-').to_string();
    out.truncate(40);
    if out.is_empty() { "build".into() } else { out }
}

pub fn workers_object_name(workers: &WorkerDeployment, build: &str) -> String {
    format!("{}-{}", workers.name_any(), dns_label(build))
}

/// The Kubernetes Deployment that runs one build's workers.
pub fn workers_object(workers: &WorkerDeployment, build: &str, replicas: i32, cell_url: &str) -> Deployment {
    let name = workers.name_any();
    let selector = labels(&[(LABEL_WORKERS, &name), (LABEL_BUILD, &dns_label(build))]);
    let mut template = workers.spec.template.clone();
    let meta = template.metadata.get_or_insert_with(ObjectMeta::default);
    meta.labels.get_or_insert_with(BTreeMap::new).extend(selector.clone());
    if let Some(pod) = template.spec.as_mut() {
        let injected = [
            ("WFE_URL", cell_url.to_string()),
            ("WFE_DEPLOYMENT", deployment_name(workers)),
            ("WFE_BUILD_ID", build.to_string()),
        ];
        for container in &mut pod.containers {
            let env = container.env.get_or_insert_with(Vec::new);
            env.retain(|e| !injected.iter().any(|(k, _)| e.name == *k));
            env.extend(injected.iter().map(|(k, v)| EnvVar {
                name: k.to_string(),
                value: Some(v.clone()),
                ..EnvVar::default()
            }));
        }
    }
    Deployment {
        metadata: ObjectMeta {
            name: Some(workers_object_name(workers, build)),
            namespace: workers.namespace(),
            labels: Some(selector.clone()),
            annotations: Some([(LABEL_BUILD.to_string(), build.to_string())].into()),
            owner_references: Some(owner(workers)),
            ..ObjectMeta::default()
        },
        spec: Some(DeploymentSpec {
            replicas: Some(replicas),
            selector: LabelSelector { match_labels: Some(selector), ..LabelSelector::default() },
            template,
            ..DeploymentSpec::default()
        }),
        ..Deployment::default()
    }
}

impl From<&Rollout> for RolloutPolicy {
    fn from(rollout: &Rollout) -> RolloutPolicy {
        RolloutPolicy {
            strategy: match rollout.strategy {
                crate::crd::RolloutStrategy::AllAtOnce => wfe_control::RolloutStrategy::AllAtOnce,
                crate::crd::RolloutStrategy::Progressive => wfe_control::RolloutStrategy::Progressive,
                crate::crd::RolloutStrategy::Manual => wfe_control::RolloutStrategy::Manual,
            },
            steps: rollout
                .steps
                .iter()
                .map(|s| wfe_control::RampStep {
                    ramp_percent: s.ramp_percent,
                    pause_ms: s.pause_seconds.saturating_mul(1_000),
                })
                .collect(),
        }
    }
}

impl From<&Sunset> for SunsetPolicy {
    fn from(sunset: &Sunset) -> SunsetPolicy {
        SunsetPolicy {
            scaledown_after_ms: sunset.scaledown_after_seconds.saturating_mul(1_000),
            delete_after_ms: sunset.delete_after_seconds.saturating_mul(1_000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell() -> Cell {
        let mut cell = Cell::new(
            "east",
            CellSpec {
                cell_id: 7,
                shards: 4,
                replicas: 3,
                image: None,
                role: CellRole::Active,
                storage: "10Gi".into(),
                storage_class_name: None,
                data_file_mb: 512,
                resources: None,
            },
        );
        cell.metadata.namespace = Some("prod".into());
        cell.metadata.uid = Some("uid-1".into());
        cell
    }

    #[test]
    fn cell_objects_wire_the_nodes_together() {
        let (nodes, clients, set) = cell_objects(&cell(), "wfe:test");
        assert_eq!(nodes.metadata.name.as_deref(), Some("east-nodes"));
        assert_eq!(nodes.spec.as_ref().unwrap().cluster_ip.as_deref(), Some("None"));
        assert_eq!(clients.metadata.name.as_deref(), Some("east"));
        let spec = set.spec.unwrap();
        assert_eq!(spec.replicas, Some(3));
        assert_eq!(spec.service_name.as_deref(), Some("east-nodes"));
        let container = &spec.template.spec.unwrap().containers[0];
        assert_eq!(container.image.as_deref(), Some("wfe:test"));
        let args = container.args.clone().unwrap();
        assert!(args.contains(&"--cell=7".to_string()));
        assert!(args.contains(
            &"--peers=east-0.east-nodes.prod.svc:7401,east-1.east-nodes.prod.svc:7401,east-2.east-nodes.prod.svc:7401".to_string()
        ));
        assert_eq!(set.metadata.owner_references.unwrap()[0].name, "east");
    }

    #[test]
    fn worker_objects_carry_the_build() {
        let template: PodTemplateSpec = serde_json::from_value(serde_json::json!({
            "spec": { "containers": [{ "name": "worker", "image": "shop:1", "env": [{ "name": "WFE_URL", "value": "stale" }] }] }
        }))
        .unwrap();
        let mut workers = WorkerDeployment::new(
            "shop",
            WorkerDeploymentSpec {
                cell: "east".into(),
                deployment: None,
                build_id: None,
                replicas: 2,
                template,
                rollout: Rollout::default(),
                sunset: Sunset::default(),
            },
        );
        workers.metadata.namespace = Some("prod".into());
        workers.metadata.uid = Some("uid-2".into());
        let build = target_build(&workers);
        assert_eq!(build.len(), 10);
        let mut changed = workers.clone();
        changed.spec.template.spec.as_mut().unwrap().containers[0].image = Some("shop:2".into());
        assert_ne!(target_build(&changed), build, "a template change is a new build");
        changed.spec.build_id = Some("v2".into());
        assert_eq!(target_build(&changed), "v2");

        let object = workers_object(&workers, "V2_rc.1", 2, "http://east.prod.svc:7400");
        assert_eq!(object.metadata.name.as_deref(), Some("shop-v2-rc-1"));
        let pod = object.spec.unwrap().template.spec.unwrap();
        let env: BTreeMap<String, String> =
            pod.containers[0].env.clone().unwrap().into_iter().map(|e| (e.name, e.value.unwrap_or_default())).collect();
        assert_eq!(env["WFE_URL"], "http://east.prod.svc:7400");
        assert_eq!(env["WFE_DEPLOYMENT"], "shop");
        assert_eq!(env["WFE_BUILD_ID"], "V2_rc.1");
        assert_eq!(env.len(), 3);
    }
}
