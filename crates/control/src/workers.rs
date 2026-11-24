//! Rolling a worker deployment to a new build and retiring the old ones.

use wfe_core::api::{DeploymentInfo, VersionStatus};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RolloutStrategy {
    /// Make a new build current as soon as its workers poll.
    #[default]
    AllAtOnce,
    /// Ramp a new build through the steps, then make it current.
    Progressive,
    /// Run the new build's workers; a person moves traffic.
    Manual,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RampStep {
    /// Share of new workflows the new build takes at this step.
    pub ramp_percent: u8,
    /// How long to hold the step before the next one.
    pub pause_ms: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RolloutPolicy {
    pub strategy: RolloutStrategy,
    pub steps: Vec<RampStep>,
}

/// What happens to a build's workers once no workflow needs them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SunsetPolicy {
    pub scaledown_after_ms: u64,
    pub delete_after_ms: u64,
}

/// What a rollout does next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RolloutStep {
    /// Nothing to do: the target is current, or a person decides.
    Hold,
    Wait(String),
    SetRamp(u8),
    SetCurrent,
    /// The target is current, but an abandoned rollout still ramps another build.
    ClearRamp,
}

/// `now_ms` and the cell's timestamps need to agree only roughly: a
/// difference shortens or lengthens a pause, it never skips a safety check.
pub fn plan_rollout(rollout: &RolloutPolicy, target: &str, info: Option<&DeploymentInfo>, now_ms: u64) -> RolloutStep {
    let Some(info) = info.filter(|i| i.versions.iter().any(|v| v.build_id == target)) else {
        return RolloutStep::Wait(format!("waiting for a worker of build {target} to poll"));
    };
    let routing = &info.routing;
    if routing.current.as_deref() == Some(target) {
        return if routing.ramping.is_some() { RolloutStep::ClearRamp } else { RolloutStep::Hold };
    }
    let nothing_to_protect_by_ramping = routing.current.is_none();
    match rollout.strategy {
        RolloutStrategy::Manual => RolloutStep::Hold,
        RolloutStrategy::AllAtOnce => RolloutStep::SetCurrent,
        RolloutStrategy::Progressive if nothing_to_protect_by_ramping => RolloutStep::SetCurrent,
        RolloutStrategy::Progressive => {
            let Some(first) = rollout.steps.first() else { return RolloutStep::SetCurrent };
            if routing.ramping.as_deref() != Some(target) {
                return RolloutStep::SetRamp(first.ramp_percent.min(100));
            }
            let percent_of = |step: &RampStep| step.ramp_percent.min(100);
            let first_step_reaching_the_reported_ramp =
                rollout.steps.iter().position(|s| percent_of(s) >= routing.ramp_percent);
            let Some(index) = first_step_reaching_the_reported_ramp else {
                return RolloutStep::SetCurrent;
            };
            let step = &rollout.steps[index];
            let ramp_was_moved_by_hand = percent_of(step) != routing.ramp_percent;
            if ramp_was_moved_by_hand {
                return RolloutStep::SetRamp(percent_of(step));
            }
            let held_ms = now_ms.saturating_sub(routing.ramping_changed_at.millis());
            if held_ms < step.pause_ms {
                let left = (step.pause_ms - held_ms).div_ceil(1_000);
                return RolloutStep::Wait(format!("holding {}% for {left} more seconds", percent_of(step)));
            }
            let next_step_that_raises_the_ramp =
                rollout.steps[index + 1..].iter().find(|s| percent_of(s) > routing.ramp_percent);
            match next_step_that_raises_the_ramp {
                Some(next) => RolloutStep::SetRamp(percent_of(next)),
                None => RolloutStep::SetCurrent,
            }
        }
    }
}

/// What to do with the workers of a build that is not the target.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SunsetStep {
    /// The cell routes to the build or open workflows are pinned to it.
    Needed,
    /// Nothing needs it, but not for long enough to act yet.
    Keep,
    ScaleToZero,
    Delete,
}

/// A build's workers are needed while the cell still routes to it or open
/// workflows are pinned to it. `created_ms` is when its workers were
/// created, for a build the cell has never heard of.
pub fn plan_sunset(
    sunset: &SunsetPolicy,
    build: &str,
    info: Option<&DeploymentInfo>,
    created_ms: u64,
    now_ms: u64,
) -> SunsetStep {
    let version = info.and_then(|i| i.versions.iter().find(|v| v.build_id == build));
    let idle_since = match version {
        Some(v) => match v.status {
            VersionStatus::Current | VersionStatus::Ramping | VersionStatus::Draining => return SunsetStep::Needed,
            VersionStatus::Drained | VersionStatus::Inactive => v.status_changed_at.millis().max(v.created_at.millis()),
        },
        None => created_ms,
    };
    let idle_ms = now_ms.saturating_sub(idle_since);
    if idle_ms >= sunset.delete_after_ms {
        SunsetStep::Delete
    } else if idle_ms >= sunset.scaledown_after_ms {
        SunsetStep::ScaleToZero
    } else {
        SunsetStep::Keep
    }
}

/// One build's workers as they exist now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetBuild {
    pub build: String,
    pub replicas: u32,
    pub created_ms: u64,
}

pub struct WorkersInput<'a> {
    /// The build that should end up current.
    pub target: &'a str,
    pub replicas: u32,
    pub rollout: &'a RolloutPolicy,
    pub sunset: &'a SunsetPolicy,
    /// What the cell reports; `None` when it has never heard of the deployment.
    pub info: Option<&'a DeploymentInfo>,
    pub fleet: &'a [FleetBuild],
    pub now_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkersAction {
    /// Have this many workers of the build running.
    Run {
        build: String,
        replicas: u32,
    },
    /// Bring back the workers of an older build that is needed again.
    ScaleUp {
        build: String,
        replicas: u32,
    },
    SetRamp {
        build: String,
        percent: u8,
    },
    SetCurrent {
        build: String,
    },
    /// Stop sending a share of new workflows to the ramping build.
    ClearRamp,
    ScaleToZero {
        build: String,
    },
    /// Delete the build's workers and ask the cell to forget the version.
    Remove {
        build: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WorkersPlan {
    /// In the order to apply them. Every action can be repeated safely.
    pub actions: Vec<WorkersAction>,
    /// What the rollout is waiting for, if anything.
    pub message: String,
}

pub fn plan_workers(input: &WorkersInput<'_>) -> WorkersPlan {
    let target = input.target.to_string();
    let mut plan = WorkersPlan {
        actions: vec![WorkersAction::Run { build: target.clone(), replicas: input.replicas }],
        message: String::new(),
    };
    match plan_rollout(input.rollout, input.target, input.info, input.now_ms) {
        RolloutStep::Hold => {}
        RolloutStep::Wait(why) => plan.message = why,
        RolloutStep::SetRamp(percent) => plan.actions.push(WorkersAction::SetRamp { build: target, percent }),
        RolloutStep::SetCurrent => plan.actions.push(WorkersAction::SetCurrent { build: target }),
        RolloutStep::ClearRamp => plan.actions.push(WorkersAction::ClearRamp),
    }
    for old in input.fleet.iter().filter(|b| b.build != input.target) {
        match plan_sunset(input.sunset, &old.build, input.info, old.created_ms, input.now_ms) {
            SunsetStep::Needed if old.replicas == 0 => {
                plan.actions.push(WorkersAction::ScaleUp { build: old.build.clone(), replicas: input.replicas });
            }
            SunsetStep::Needed | SunsetStep::Keep => {}
            SunsetStep::ScaleToZero if old.replicas == 0 => {}
            SunsetStep::ScaleToZero => plan.actions.push(WorkersAction::ScaleToZero { build: old.build.clone() }),
            SunsetStep::Delete => plan.actions.push(WorkersAction::Remove { build: old.build.clone() }),
        }
    }
    let needed_but_gone = input.info.into_iter().flat_map(|i| &i.versions).find(|v| {
        v.build_id != input.target
            && matches!(v.status, VersionStatus::Current | VersionStatus::Ramping | VersionStatus::Draining)
            && !input.fleet.iter().any(|b| b.build == v.build_id)
    });
    if let Some(version) = needed_but_gone {
        plan.message = format!(
            "build {} is {:?} with {} open pinned workflows but has no workers; run workers of that build again",
            version.build_id, version.status, version.pinned_open
        );
    }
    plan
}

#[cfg(test)]
mod tests {
    use super::*;
    use wfe_core::Timestamp;
    use wfe_core::api::{RoutingConfig, VersionInfo};

    fn info(
        current: Option<&str>,
        ramping: Option<(&str, u8)>,
        ramp_changed_ms: u64,
        versions: &[(&str, VersionStatus, u64)],
    ) -> DeploymentInfo {
        DeploymentInfo {
            name: "shop".into(),
            routing: RoutingConfig {
                current: current.map(str::to_string),
                ramping: ramping.map(|r| r.0.to_string()),
                ramp_percent: ramping.map_or(0, |r| r.1),
                ramping_changed_at: Timestamp::from_millis(ramp_changed_ms),
                ..RoutingConfig::default()
            },
            versions: versions
                .iter()
                .map(|(build, status, changed)| VersionInfo {
                    build_id: build.to_string(),
                    status: *status,
                    status_changed_at: Timestamp::from_millis(*changed),
                    ..VersionInfo::default()
                })
                .collect(),
            ..DeploymentInfo::default()
        }
    }

    fn progressive() -> RolloutPolicy {
        RolloutPolicy {
            strategy: RolloutStrategy::Progressive,
            steps: vec![
                RampStep { ramp_percent: 10, pause_ms: 60_000 },
                RampStep { ramp_percent: 50, pause_ms: 120_000 },
            ],
        }
    }

    const SUNSET: SunsetPolicy = SunsetPolicy { scaledown_after_ms: 60_000, delete_after_ms: 600_000 };

    #[test]
    fn waits_for_the_build_to_poll() {
        let all = RolloutPolicy::default();
        assert!(matches!(plan_rollout(&all, "v2", None, 0), RolloutStep::Wait(_)));
        let known = info(Some("v1"), None, 0, &[("v1", VersionStatus::Current, 0)]);
        assert!(matches!(plan_rollout(&all, "v2", Some(&known), 0), RolloutStep::Wait(_)));
    }

    #[test]
    fn all_at_once_and_manual() {
        let known = info(Some("v1"), None, 0, &[("v1", VersionStatus::Current, 0), ("v2", VersionStatus::Inactive, 0)]);
        assert_eq!(plan_rollout(&RolloutPolicy::default(), "v2", Some(&known), 0), RolloutStep::SetCurrent);
        let manual = RolloutPolicy { strategy: RolloutStrategy::Manual, steps: vec![] };
        assert_eq!(plan_rollout(&manual, "v2", Some(&known), 0), RolloutStep::Hold);
        assert_eq!(plan_rollout(&RolloutPolicy::default(), "v1", Some(&known), 0), RolloutStep::Hold);
    }

    #[test]
    fn progressive_walks_the_steps() {
        let r = progressive();
        let versions = [("v1", VersionStatus::Current, 0), ("v2", VersionStatus::Inactive, 0)];
        let start = info(Some("v1"), None, 0, &versions);
        assert_eq!(plan_rollout(&r, "v2", Some(&start), 1_000), RolloutStep::SetRamp(10));
        let at_10 = info(Some("v1"), Some(("v2", 10)), 1_000, &versions);
        assert!(matches!(plan_rollout(&r, "v2", Some(&at_10), 30_000), RolloutStep::Wait(_)));
        assert_eq!(plan_rollout(&r, "v2", Some(&at_10), 61_000), RolloutStep::SetRamp(50));
        let at_50 = info(Some("v1"), Some(("v2", 50)), 61_000, &versions);
        assert!(matches!(plan_rollout(&r, "v2", Some(&at_50), 100_000), RolloutStep::Wait(_)));
        assert_eq!(plan_rollout(&r, "v2", Some(&at_50), 181_000), RolloutStep::SetCurrent);
        let at_30 = info(Some("v1"), Some(("v2", 30)), 0, &versions);
        assert_eq!(plan_rollout(&r, "v2", Some(&at_30), 0), RolloutStep::SetRamp(50));
        let other = info(Some("v1"), Some(("v0", 30)), 0, &versions);
        assert_eq!(plan_rollout(&r, "v2", Some(&other), 0), RolloutStep::SetRamp(10));
        let at_90 = info(Some("v1"), Some(("v2", 90)), 0, &versions);
        assert_eq!(plan_rollout(&r, "v2", Some(&at_90), 0), RolloutStep::SetCurrent);
    }

    #[test]
    fn going_back_to_the_current_build_ends_the_abandoned_ramp() {
        let versions = [("v1", VersionStatus::Current, 0), ("v2", VersionStatus::Ramping, 0)];
        let mid_ramp = info(Some("v1"), Some(("v2", 10)), 0, &versions);
        assert_eq!(plan_rollout(&progressive(), "v1", Some(&mid_ramp), 0), RolloutStep::ClearRamp);
    }

    #[test]
    fn steps_that_do_not_raise_the_ramp_are_skipped() {
        let repeated = RolloutPolicy {
            strategy: RolloutStrategy::Progressive,
            steps: vec![
                RampStep { ramp_percent: 100, pause_ms: 1_000 },
                RampStep { ramp_percent: 100, pause_ms: 1_000 },
                RampStep { ramp_percent: 40, pause_ms: 1_000 },
                RampStep { ramp_percent: 250, pause_ms: 1_000 },
            ],
        };
        let versions = [("v1", VersionStatus::Current, 0), ("v2", VersionStatus::Ramping, 0)];
        let at_100 = info(Some("v1"), Some(("v2", 100)), 0, &versions);
        assert_eq!(plan_rollout(&repeated, "v2", Some(&at_100), 5_000), RolloutStep::SetCurrent);
    }

    #[test]
    fn the_first_build_of_a_deployment_does_not_ramp() {
        let fresh = info(None, None, 0, &[("v1", VersionStatus::Inactive, 0)]);
        assert_eq!(plan_rollout(&progressive(), "v1", Some(&fresh), 0), RolloutStep::SetCurrent);
    }

    #[test]
    fn sunset_keeps_what_workflows_need() {
        let d = info(
            Some("v3"),
            None,
            0,
            &[
                ("v1", VersionStatus::Drained, 10_000),
                ("v2", VersionStatus::Draining, 10_000),
                ("v3", VersionStatus::Current, 0),
            ],
        );
        assert_eq!(plan_sunset(&SUNSET, "v2", Some(&d), 0, 10_000_000), SunsetStep::Needed);
        assert_eq!(plan_sunset(&SUNSET, "v1", Some(&d), 0, 20_000), SunsetStep::Keep);
        assert_eq!(plan_sunset(&SUNSET, "v1", Some(&d), 0, 70_000), SunsetStep::ScaleToZero);
        assert_eq!(plan_sunset(&SUNSET, "v1", Some(&d), 0, 610_000), SunsetStep::Delete);
        assert_eq!(plan_sunset(&SUNSET, "v0", Some(&d), 5_000, 30_000), SunsetStep::Keep);
        assert_eq!(plan_sunset(&SUNSET, "v0", None, 5_000, 700_000), SunsetStep::Delete);
    }

    #[test]
    fn a_needed_build_with_no_workers_is_scaled_back_up() {
        let d = info(Some("v2"), None, 0, &[("v1", VersionStatus::Draining, 0), ("v2", VersionStatus::Current, 0)]);
        let fleet = [
            FleetBuild { build: "v1".into(), replicas: 0, created_ms: 0 },
            FleetBuild { build: "v2".into(), replicas: 2, created_ms: 0 },
        ];
        let plan = plan_workers(&WorkersInput {
            target: "v2",
            replicas: 2,
            rollout: &RolloutPolicy::default(),
            sunset: &SUNSET,
            info: Some(&d),
            fleet: &fleet,
            now_ms: 10_000_000,
        });
        assert_eq!(
            plan.actions,
            vec![
                WorkersAction::Run { build: "v2".into(), replicas: 2 },
                WorkersAction::ScaleUp { build: "v1".into(), replicas: 2 },
            ]
        );
    }

    #[test]
    fn the_plan_runs_the_target_and_retires_the_rest() {
        let d = info(
            Some("v1"),
            None,
            0,
            &[("v0", VersionStatus::Drained, 0), ("v1", VersionStatus::Current, 0), ("v2", VersionStatus::Inactive, 0)],
        );
        let fleet = [
            FleetBuild { build: "v0".into(), replicas: 2, created_ms: 0 },
            FleetBuild { build: "v1".into(), replicas: 2, created_ms: 0 },
            FleetBuild { build: "v2".into(), replicas: 2, created_ms: 0 },
        ];
        let plan = plan_workers(&WorkersInput {
            target: "v2",
            replicas: 3,
            rollout: &RolloutPolicy::default(),
            sunset: &SUNSET,
            info: Some(&d),
            fleet: &fleet,
            now_ms: 70_000,
        });
        assert_eq!(
            plan.actions,
            vec![
                WorkersAction::Run { build: "v2".into(), replicas: 3 },
                WorkersAction::SetCurrent { build: "v2".into() },
                WorkersAction::ScaleToZero { build: "v0".into() },
            ]
        );
    }
}
