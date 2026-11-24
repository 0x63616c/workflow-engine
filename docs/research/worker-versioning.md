# Worker Versioning: Temporal's design, precise semantics, and a minimal faithful subset

Research date: 2026-11-23. Scope: Temporal's current Worker Versioning (Worker Deployments and Worker Deployment
Versions with `Pinned` and `AutoUpgrade` versioning behaviors) and the `temporal-worker-controller` Kubernetes
controller. The deprecated "version sets" and "assignment/redirect rules" designs are covered only in
[Appendix A](#appendix-a-why-the-older-designs-were-replaced).

## Sources and conventions

Source was read from shallow clones at these commits. Paths below are relative to the repo root.

| Tag | Repo | Commit | Date |
| --- | --- | --- | --- |
| `[api]` | `temporalio/api` | `a4032d0f31dc` | 2026-11-23 |
| `[srv]` | `temporalio/temporal` | `4197dfdd52eb` | 2026-11-23 |
| `[twc]` | `temporalio/temporal-worker-controller` | `0cc78584b18b` | 2026-11-19 |

Docs pages used:

- <https://docs.temporal.io/worker-versioning> (concepts, statuses, inheritance)
- <https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning> (production guide)
- <https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/roll-out-and-pin>
- <https://docs.temporal.io/production-deployment/worker-deployments/worker-versioning/upgrade-on-continue-as-new>
- <https://docs.temporal.io/production-deployment/worker-deployments/recover-pinned-workflows>
- <https://docs.temporal.io/production-deployment/worker-deployments/kubernetes-controller>
- <https://docs.temporal.io/cli/worker> and <https://docs.temporal.io/cli/workflow> (CLI reference)
- <https://docs.temporal.io/evaluate/cloud/limits> (limits)
- <https://docs.temporal.io/encyclopedia/worker-versioning-legacy> (deprecated design)

Confidence markers:

- No marker: verified by reading the cited proto or source.
- **(docs)**: stated by the docs, not cross-checked in source.
- **(inferred)**: my reading of how pieces fit together, not a statement found verbatim in source or docs.

Terminology: WFT = workflow task, AT = activity task, TQ = task queue, CaN = continue-as-new.

---

## 1. Data model

### 1.1 Identity

- **Worker Deployment**: a named logical service. Identified by `deployment_name` within a namespace.
- **Worker Deployment Version**: `(deployment_name, build_id)`. Proto: `WorkerDeploymentVersion {build_id = 1;
  deployment_name = 2}` (`[api] temporal/api/deployment/v1/message.proto`).
- A version's **task queue membership** is the set of `(task queue name, task queue type)` pairs that a worker
  with that version has ever polled. Membership is tracked per TQ type (workflow, activity, Nexus) and is what
  routing consults (section 3).

Validation (`[srv] common/worker_versioning/worker_versioning.go`, `ValidateDeploymentVersionFields` around
line 610 and `DeploymentFromCapabilities` around line 192):

- Both fields non-empty.
- Each field length `<= (maxIDLengthLimit - 39) / 2` (the 39 comes from the system workflow ID prefix).
- `deployment_name` must not contain `.` or `:` (both are used as version-string delimiters: legacy
  `<deployment>.<build>`, current `<deployment>:<build>`).
- Neither field may start with `__` (reserved; the unversioned sentinel is `__unversioned__`).

### 1.2 What a worker declares

`WorkerDeploymentOptions` (`[api] temporal/api/deployment/v1/message.proto`):

| Field | Meaning |
| --- | --- |
| `deployment_name = 1` | Required when versioned. |
| `build_id = 2` | Required when versioned. |
| `worker_versioning_mode = 3` | `UNVERSIONED = 1` or `VERSIONED = 2`. Must be identical for all workers with the same `(deployment_name, build_id)` across all task queues. |

A poller counts as versioned if and only if `worker_versioning_mode == VERSIONED`. Otherwise it is unversioned
even if it sent a name and build ID (those are then only informational).

### 1.3 Routing config (per Worker Deployment)

`RoutingConfig` (`[api] temporal/api/deployment/v1/message.proto`):

| Field | Meaning |
| --- | --- |
| `current_deployment_version = 7` | The Current Version. `nil` means unversioned workers receive the non-ramped traffic. |
| `ramping_deployment_version = 9` | The Ramping Version. "Must always be different from `current_deployment_version` unless both are nil." `nil` with a non-zero percentage means "ramp to unversioned". |
| `ramping_version_percentage = 3` (float) | In `[0, 100]`. 100 means the ramping version gets all unpinned traffic but is still not Current. |
| `current_version_changed_time = 4` | Last time Current changed. |
| `ramping_version_changed_time = 5` | Last time the ramping version changed. Not updated when only the percentage changes. |
| `ramping_version_percentage_changed_time = 6` | Last time the percentage changed. Also updated when the ramping version changes, even if the percentage is the same. |
| `revision_number = 10` (int64) | "Monotonically increasing value which is incremented on every mutation to any field of this message to achieve eventual consistency between task queues and their partitions." |

Deprecated string forms (`current_version = 1`, `ramping_version = 2`) exist for older clients; ignore them.

`WorkerDeploymentInfo` (describe output): `name`, `version_summaries[]`, `create_time`, `routing_config`,
`last_modifier_identity`, `manager_identity`, `routing_config_update_state`
(`ROUTING_CONFIG_UPDATE_STATE_IN_PROGRESS = 1` / `COMPLETED = 2`, i.e. whether the latest routing config has
finished propagating to all task queues).

### 1.4 Version info

`WorkerDeploymentVersionInfo` (`[api] temporal/api/deployment/v1/message.proto`):

| Field | Meaning |
| --- | --- |
| `deployment_version = 11` | Identity. |
| `status = 14` | `WorkerDeploymentVersionStatus`, see 1.6. |
| `create_time = 3` | |
| `routing_changed_time = 4` | Last time this version's current/ramping/percentage state changed. |
| `current_since_time = 5` | Set while Current, else nil. |
| `ramping_since_time = 6` | Set while Ramping. "Updated when the version first starts ramping, not on each ramp change." |
| `ramp_percentage = 7` | Valid while Ramping. |
| `first_activation_time = 12` | First time it became Current or Ramping. |
| `last_current_time = 15` | Last time it was Current. |
| `last_deactivation_time = 13` | Last time it stopped being Current or Ramping. "Cleared if the version becomes current or ramping again." |
| `drainage_info = 9` | `VersionDrainageInfo {status = 1; last_changed_time = 2; last_checked_time = 3}`. Absent while Current or Ramping. |
| `metadata = 10` | `VersionMetadata {map<string, Payload> entries}`. |
| `last_modifier_identity = 17` | |
| `compute_config = 16` | Serverless worker scaling config. Out of scope. |

`DescribeWorkerDeploymentVersionResponse` also returns `version_task_queues[] {name, type, stats,
stats_by_priority_key}`.

### 1.5 Per-workflow versioning state

`WorkflowExecutionVersioningInfo` (`[api] temporal/api/workflow/v1/message.proto`):

| Field | Meaning |
| --- | --- |
| `behavior = 1` | `VERSIONING_BEHAVIOR_UNSPECIFIED = 0`, `PINNED = 1`, `AUTO_UPGRADE = 2`. Set from the last completed WFT. Unspecified means the workflow is unversioned. |
| `deployment_version = 7` | Version that completed the last WFT (or the inherited version before the first WFT completes). |
| `versioning_override = 3` | Operator override, see 3.9. |
| `version_transition = 6` | `DeploymentVersionTransition {deployment_version = 2}`. Present while the workflow is moving to another version. A nil target inside a present transition means "transitioning to unversioned". |
| `revision_number = 8` | Routing-config revision observed when the workflow last started a transition. Pinned and sticky tasks send 0. |
| `continue_as_new_initial_versioning_behavior = 9` | Carried from the CaN command for the first WFT of the new run. |

Two derived values drive everything (`[srv] service/history/workflow/util.go`, lines 217-262):

```text
effective_behavior(info):
    if info.version_transition != nil          -> AUTO_UPGRADE
    else if info.versioning_override != nil    -> override behavior
                                                   (pinned or pending one_time -> PINNED,
                                                    auto_upgrade -> AUTO_UPGRADE)
    else                                       -> info.behavior

effective_deployment(info):
    if info.version_transition != nil                    -> transition target (may be nil = unversioned)
    else if override is pinned or pending one_time       -> override target version
    else if effective_behavior != UNSPECIFIED
            or override is auto_upgrade                  -> info.deployment_version
    else                                                 -> nil (unversioned)
```

History events that carry versioning data (`[api] temporal/api/history/v1/message.proto`):

- `WorkflowExecutionStartedEventAttributes`: `versioning_override = 33`, `inherited_pinned_version = 37`,
  `inherited_auto_upgrade_info = 39` (mutually exclusive with 37), `declined_target_version_upgrade = 40`.
- `WorkflowTaskStartedEventAttributes.target_worker_deployment_version_changed = 9`.
- `WorkflowTaskCompletedEventAttributes`: `versioning_behavior = 8`, `worker_deployment_name = 10`,
  `deployment_version = 11`.
- `WorkflowExecutionOptionsUpdatedEventAttributes`: `versioning_override = 1`, `unset_versioning_override = 2`.
- `ContinueAsNewWorkflowExecutionCommandAttributes.initial_versioning_behavior = 16`,
  `StartChildWorkflowExecutionCommandAttributes.versioning_override = 19`.

Visibility search attributes maintained per workflow (`[srv] service/history/workflow/mutable_state_impl.go`,
around lines 3962-4238):

| Search attribute | Value |
| --- | --- |
| `TemporalWorkerDeployment` | Effective deployment name (override wins). |
| `TemporalWorkerDeploymentVersion` | `<deployment>:<build_id>` of the effective version (override wins). |
| `TemporalWorkflowVersioningBehavior` | `Pinned` / `AutoUpgrade` (override wins). |
| `TemporalUsedWorkerDeploymentVersions` | Keyword list; a version is appended each time it completes a WFT. An override alone does not add an entry. |
| `BuildIds` | Contains `pinned:<deployment>:<build_id>` for effectively pinned workflows (single entry, replaced on change), or `unversioned`. This is what drainage queries count (section 4). |

### 1.6 Version status and drainage enums

`WorkerDeploymentVersionStatus` (`[api] temporal/api/enums/v1/deployment.proto`):

| Value | Meaning |
| --- | --- |
| `CREATED = 6` | Created via `CreateWorkerDeploymentVersion`; the server has not seen a poller for it yet. |
| `INACTIVE = 1` | Exists (a worker polled), never been Current or Ramping. Never becomes Draining or Drained unless it is activated first. |
| `CURRENT = 2` | Is the deployment's Current Version. |
| `RAMPING = 3` | Is the deployment's Ramping Version. |
| `DRAINING = 4` | Was Current or Ramping, is no longer, and may still have open pinned workflows. |
| `DRAINED = 5` | Was Draining and has no open pinned workflows. May still be needed for queries on closed workflows. |

`VersionDrainageStatus`: `UNSPECIFIED = 0` (not applicable: never activated, or currently Current/Ramping),
`DRAINING = 1`, `DRAINED = 2`.

The transition diagram and timing rules are in section 4.

---

## 2. Worker side

### 2.1 What the worker sends

All in `[api] temporal/api/workflowservice/v1/request_response.proto`:

- **Every poll** carries `deployment_options` (`PollWorkflowTaskQueueRequest.deployment_options = 6`,
  `PollActivityTaskQueueRequest.deployment_options = 6`, `PollNexusTaskQueueRequest.deployment_options = 6`).
  The proto comment says these options "need to be sent to server in every poll".
- **Every WFT completion** carries `RespondWorkflowTaskCompletedRequest.deployment_options = 17` and
  `versioning_behavior = 16`. The behavior is the one the workflow type was registered with (or the worker's
  default versioning behavior). `UNSPECIFIED` "means versioning is not enabled in the worker".
- WFT failure and activity responses also carry `deployment_options` (for example
  `RespondWorkflowTaskFailedRequest.deployment_options = 10`), used for bookkeeping only.
- Eager workflow start: `StartWorkflowExecutionRequest.eager_worker_deployment_options = 28`.

Server-side handling of a completion (`[srv] service/history/api/respondworkflowtaskcompleted/api.go`,
lines 219-243):

- If the worker's mode is `UNVERSIONED`, the reported behavior is coerced to `UNSPECIFIED`.
- A behavior together with an unspecified or unknown mode is rejected: `InvalidArgument` "versioning behavior
  cannot be specified with an unspecified or unknown worker versioning mode".

SDK side **(docs)**: a versioned worker is configured with a deployment name, a build ID, "use versioning =
true", and optionally a default versioning behavior. Each workflow type can declare `Pinned` or `AutoUpgrade`
at registration. A versioned worker with a workflow type that has no behavior and no worker default fails at
registration, so in practice a versioned worker always reports a concrete behavior.

### 2.2 First poll registers the version

On a poll from a versioned worker whose version is not yet a member of the polled task queue
(`[srv] service/matching/physical_task_queue_manager.go`, lines 650-775):

1. Matching serializes registration per task queue.
2. If the task queue already has more than `MaxVersionsInTaskQueue` versions the poll fails with
   `ResourceExhausted`, cause `RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS`, "exceeding maximum number of
   Deployment Versions in this task queue (limit = %d)".
3. Matching calls the deployment subsystem's `RegisterTaskQueueWorker(namespace, deployment_name, build_id,
   task_queue, task_queue_type)`. This creates the Worker Deployment and the Version if they do not exist and
   adds the task queue to the version.
4. The poll blocks until the version shows up in the task queue's own routing data, then proceeds as a normal
   poll.

So there is no explicit "create version" step in the common path: polling is registration. The explicit
`CreateWorkerDeployment` / `CreateWorkerDeploymentVersion` RPCs exist for pre-creating versions that have no
pollers yet (status `CREATED`, used by serverless workers).

### 2.3 How a workflow's behavior and version get set and change

Verified in `[srv] service/history/workflow/workflow_task_state_machine.go` (`afterAddWorkflowTaskCompletedEvent`,
lines 1326-1444) and `mutable_state_impl.go`:

1. **Start.** A new workflow has no `behavior` and no `deployment_version` unless it inherits (3.7, 3.8) or was
   started with an override (3.9). With `inherited_pinned_version` it starts as `PINNED` on that version. With
   `inherited_auto_upgrade_info` it starts as `AUTO_UPGRADE` with `deployment_version = source version` and the
   source revision number.
2. **Each WFT completion** overwrites the base values with what the completing worker reported:
   - reported behavior `UNSPECIFIED` (unversioned worker): `behavior` unset and `deployment_version = nil`. The
     workflow is now unversioned.
   - otherwise: `behavior = reported behavior`, `deployment_version = completing worker's version`.
   - if a transition is pending and its target equals the completing worker's version, the transition is cleared
     (completed). If the completing worker is a different version (the WFT had already started when the
     transition began), the transition stays.
   - a `one_time` override whose target equals the completing version is cleared.
   - the completing version is appended to `TemporalUsedWorkerDeploymentVersions`.
   - if the transition completed, or the effective version or behavior changed, all not-yet-started activities
     are rescheduled (their dispatch stamp is bumped, so stale tasks in queues are dropped at start).
3. **Behavior can flip at every WFT.** The behavior is a property of the workflow type's registration in the
   build that completed the last WFT. An AutoUpgrade workflow that moves to a build where its type is registered
   Pinned becomes Pinned on that build from then on (until an override or a new build flips it back). A Pinned
   workflow cannot move on its own, so it can only stop being Pinned through an override.
4. **Sticky execution** is only set on completion if no transition is pending, or the pending transition's
   target equals the completing version. When a transition is pending at completion time, a new normal WFT is
   forced and is dispatched through the matching path (never handed back inline to the same worker)
   (`[srv] service/history/api/respondworkflowtaskcompleted/api.go`, lines 367-385 and 575-630).

---

## 3. Server-side routing rules

The rules are split between the component that owns task queues ("matching") and the component that owns
workflow state ("history"). Matching chooses which pollers may receive a task. History validates the choice
when the task starts and records the consequences.

### 3.1 Inputs

Per task queue and task queue type, matching holds (`[srv] proto/internal/temporal/server/api/persistence/v1/task_queues.proto`,
`DeploymentData`):

```text
deployments_data: map<deployment_name, WorkerDeploymentData>
WorkerDeploymentData {
    routing_config: RoutingConfig                  // copy of the deployment's routing config
    versions: map<build_id, WorkerDeploymentVersionData {revision_number, update_time, deleted, status}>
}
```

"A version is present here iff the task queue has ever been polled from the version."

Per task, history attaches a directive (`[srv] proto/internal/temporal/server/api/taskqueue/v1/message.proto`,
`TaskVersionDirective`; built by `MakeDirectiveForWorkflowTask` in
`[srv] common/worker_versioning/worker_versioning.go` line 268 and `MakeDirectiveForActivityTask` in
`[srv] service/history/worker_versioning_util.go` lines 200-237):

```text
directive {
    behavior:           workflow's effective behavior when the task was scheduled
    deployment_version: workflow's effective version when the task was scheduled
    revision_number:    workflow's versioning revision number
    use_ramping_version: bool (WFT only; activities always send false)
}
```

A workflow with effective behavior `UNSPECIFIED` (new, or unversioned) sends no behavior and no version.

### 3.2 Physical queues and pollers

Each (task queue, type) has one **default queue** plus one **version queue** per member version
(`[srv] service/matching/task_queue_partition_manager.go`, `PollTask` around line 824):

- An unversioned poller reads only the default queue.
- A versioned poller reads only its own version queue `(deployment_name, build_id)`.

### 3.3 Task queue's current and ramping version

`CalculateTaskQueueVersioningInfo` (`[srv] common/worker_versioning/worker_versioning.go` line 983). A task
queue can be a member of versions from several deployments (for example when it is being moved from one
deployment to another), so the task queue's effective routing is derived:

- A deployment's `routing_config.current_deployment_version` is a candidate only if this task queue is a
  member of that version (or it is nil). Among candidates, the one with the latest
  `current_version_changed_time` wins, and any versioned candidate beats any unversioned (nil) candidate
  regardless of time.
- Same for ramping, keyed on `ramping_version_percentage_changed_time`.

This membership filter is the reason for the "missing task queues" protection in 5.2: if version B becomes
Current but task queue `foo` was only ever polled by version A, then `foo` sees "no versioned current" and its
unpinned tasks go to unversioned pollers.

### 3.4 Target version of a workflow

Verbatim core (`[srv] common/worker_versioning/worker_versioning.go`, lines 865-978):

```go
func FindTargetDeploymentVersionAndRevisionNumberForWorkflowID(
    current, currentRevisionNumber, ramping, rampingPercentage, rampingRevisionNumber,
    workflowId, useRampingVersion) {
    if useRampingVersion && ramping != nil { return ramping, rampingRevisionNumber }
    if rampingPercentage <= 0 { return current, currentRevisionNumber
    } else if rampingPercentage == 100 { return ramping, rampingRevisionNumber }
    wfRampThreshold := calcRampThreshold(workflowId)
    if wfRampThreshold <= float64(rampingPercentage) { return ramping, rampingRevisionNumber }
    return current, currentRevisionNumber
}

func calcRampThreshold(id string) float64 {
    if id == "" { return rand.Float64() }
    h := farm.Fingerprint32([]byte(id))
    return 100 * (float64(h) / (float64(math.MaxUint32) + 1))
}
```

Precise consequences:

- **What is hashed: the workflow ID only** (FarmHash `Fingerprint32`), mapped to a threshold in `[0, 100)`.
  Not the run ID, not the namespace, not the deployment name, and no per-rollout salt.
- The mapping is **stable and monotonic**: a workflow ID with threshold `t` targets the ramping version for
  every percentage `>= t`. Raising the ramp from 5 to 20 keeps every workflow that was already ramped and adds
  more. Lowering it moves the highest-threshold workflows back to Current.
- The same workflow IDs are the "canaries" in every rollout and in every deployment (no salt). All runs of a
  workflow ID (CaN chain, retries, cron runs) land in the same bucket.
- Activities use their workflow's ID, so an activity on a task queue in the same deployment gets the same
  answer as its workflow.
- Tasks with no workflow ID (for example Nexus tasks) use a random draw. Quirk: `rand.Float64()` is in `[0, 1)`
  while the comparison is against a 0-100 percentage, so such tasks go to the ramping version for any
  percentage `>= 1`. Treat as an upstream bug; do not copy.
- The result may be nil (unversioned): no Current set, or ramp-to-unversioned.

The percentage applies to **all unpinned traffic**: first tasks of new workflows and every task of AutoUpgrade
workflows. It is not "percent of new workflow starts only".

### 3.5 Matching: where a task goes

`getPhysicalQueuesForAdd` (`[srv] service/matching/task_queue_partition_manager.go` line 2190) picks a
**backlog queue** (where the task is stored if nobody is waiting) and a **dispatch queue** (whose pollers may
receive it).

**Rule M1, pinned directive** (`directive.behavior == PINNED`, which includes pinned overrides and pending
one-time overrides):

- WFT, Nexus: backlog queue = dispatch queue = the version queue of `directive.deployment_version`. No
  fallback. If that version has no pollers the task waits (subject to the usual timeouts).
- AT: if the activity task queue is a member of the pinned version, same as above. If it is not, the activity
  is an **independent activity** and is routed as unpinned (rule M2).
- Sticky WFT: stays on the sticky queue (it is specific to one worker process anyway).
- Query on a pinned workflow: rejected with `FailedPrecondition` if the version's status is `DRAINED` and it
  has no pollers (`checkQueryBlackholed`).

**Rule M2, unpinned** (`behavior` is `AUTO_UPGRADE` or `UNSPECIFIED`): compute `target` per 3.3 and 3.4 using
the task's workflow ID.

- `target` is a version: backlog queue = **default queue**, dispatch queue = target's version queue. Source
  comment: "Unpinned tasks are written to the default queue for late binding, in case target version changes
  by the time they can be dispatched."
- `target` is nil: backlog and dispatch on the default queue, so unversioned pollers receive it.
- **Late binding**: every time a backlogged task is about to be dispatched, the target is recomputed
  (`AddSpooledTask`, line 984). An unpinned task is never committed to a version until a poller actually
  receives it.
- Sticky WFT of an unpinned workflow: if `directive.deployment_version != target`, matching returns
  `StickyWorkerUnavailable`, and history reschedules the WFT on the normal queue. This is how AutoUpgrade
  workflows with a warm sticky worker get pulled off the old version.
- Revision guard (`chooseTargetQueueByFlag`, line 2423; dynamic config
  `system.useRevisionNumberForWorkerVersioning`, default true): if the target's deployment name equals the
  task's deployment name and `target_revision < directive.revision_number`, this matching partition has a
  stale routing config, so keep the task on its own `directive.deployment_version` instead of sending the
  workflow backwards. Otherwise use the target.

Because unpinned tasks for a versioned target are stored in the default queue but only dispatched to the target
version's pollers, **unversioned pollers receive nothing from a task queue that has a Current Version** (unless
a ramp-to-unversioned is active). A stuck or absent Current Version therefore blocks the head of the shared
backlog; docs call this out: with a 50% ramp and all Current workers down, the ramping version is not
guaranteed its 50% "because Current Version Tasks block the queue" **(docs)**.

### 3.6 History: what happens when a task starts

**Workflow task start** (`[srv] service/history/api/recordworkflowtaskstarted/api.go`, lines 151-213):

- H1. Validate the directive against the workflow's present state (`ValidateTaskVersionDirective`): if the
  directive's behavior or version no longer equals the workflow's effective behavior or version, the task is
  obsolete and dropped (a replacement was already scheduled).
- H2. If the poller's version equals the workflow's effective version: start normally.
- H3. If it differs and the workflow is effectively Pinned: drop the task as obsolete.
- H4. If it differs and the workflow is not Pinned: start the task **and start a transition** to the poller's
  version (`StartDeploymentTransition`, `[srv] service/history/workflow/mutable_state_impl.go` line 10149):
  set `version_transition = {poller version}` (nil for an unversioned poller), clear stickiness, store the
  dispatch revision number in `versioning_info.revision_number`. From now on effective behavior is
  `AUTO_UPGRADE` and effective version is the transition target.
- H5. The transition is cleared only when a WFT is completed by the target version (2.3), or by an override
  change (3.9). A failed or timed-out WFT leaves the transition in place, and the retry is routed by matching
  again. (Verified by enumerating all writes to `VersionTransition` in `service/history/workflow`; there is no
  clear on WFT failure.) A later task that starts on yet another version replaces the transition target.

**Activity task start** (`[srv] service/history/api/recordactivitytaskstarted/api.go`, lines 186-249):

- A1. Same directive validation as H1.
- A2. If the workflow has a pending transition: reject with `ActivityStartDuringTransition`. Matching drops the
  task; history reschedules all pending activities when the transition completes. Proto comment: "Pending
  activities will not start new attempts during a transition."
- A3. If the poller's version equals the workflow's effective version: start.
- A4. If it differs and the workflow is Pinned: start (this is an independent activity of a pinned workflow).
- A5. If it differs and the workflow is not Pinned: compute where this workflow's own WFT would go right now
  (target of the **workflow's** task queue for this workflow ID, cached for `history.routingInfoCacheTTL`,
  default 1 s). If that equals the poller's version, or it is the same deployment and the activity's dispatch
  revision is strictly greater than the workflow task queue's revision, then: start a transition to the
  poller's version, **reject this activity start**, and schedule a new WFT. The workflow must complete a WFT
  on the new version before the activity runs there. Otherwise the activity is independently versioned and
  starts without touching the workflow.

A5 is what the enum comment means by "Workflows stuck on a backlogged activity will still auto-upgrade if their
Target Version changes" (`[api] temporal/api/enums/v1/workflow.proto`), and what the docs mean by "the Workflow
moves to the Target immediately before starting the Activity" **(docs)**.

### 3.7 Case-by-case summary

| Case | Result |
| --- | --- |
| First WFT of a new workflow, no override, nothing inherited | Directive has no behavior. Unpinned (M2): goes to the target of its task queue for its workflow ID (Current, or Ramping if the ID falls under the ramp). The poller's version differs from nil, so H4 starts a transition, and the first completion sets behavior and version. |
| First WFT with a pinned (or one-time) override at start | Effective behavior is Pinned on the override version from the start (M1). |
| First WFT with `inherited_pinned_version` | Pinned to that version (M1) even if it is not Current or Ramping. If the worker then reports AutoUpgrade for this type, the workflow becomes AutoUpgrade after that first task. |
| First WFT with `inherited_auto_upgrade_info` | Directive is AutoUpgrade with the source version and revision. Routed as unpinned (M2) to the current target; the source version and revision only feed the stale-partition guard so a child or CaN does not land on an older version than its parent already saw. |
| AutoUpgrade workflow, routing config changes mid-flight | Tasks already started finish where they are. The next WFT (or backlogged WFT, or AT per A5) is routed to the new target; H4 starts a transition; the workflow replays its history on the new build; on completion the workflow is on the new version. Pending activities wait for the transition. |
| Pinned workflow, routing config changes | Nothing changes. All tasks keep going to the pinned version's queues, including when that version is Draining or Drained. If no worker of that version polls, tasks wait. Optionally the next WFT started event carries `target_worker_deployment_version_changed = true` (3.8). |
| Ramping | Applied to every unpinned task by workflow-ID hash (3.4). An AutoUpgrade workflow whose ID falls under the ramp moves to the ramping version on its next task, and moves back if the ramp is lowered or removed. |
| Unversioned workers only, no Current set | Target is nil. Everything goes to the default queue and unversioned pollers. This is the day-0 state and behaves like no versioning. |
| Versioned workers polling, no Current set | Versions register as `INACTIVE` and receive nothing (except workflows explicitly pinned to them by override). Unversioned workers keep all traffic. |
| Unversioned to versioned | Set Current to version V. New workflows and existing unversioned workflows have unpinned directives, so their next task targets V; H4 transitions them; first completion on V assigns behavior and version. Ramp first to do this gradually. |
| Versioned to unversioned | `SetCurrent` with empty build ID (or ramp to unversioned with empty build ID and a percentage). AutoUpgrade workflows' next task targets nil, goes to an unversioned poller, transitions "to unversioned", and on completion `behavior` and `deployment_version` are cleared. Pinned workflows stay on their versions. |
| Activity, same task queue as the workflow | Pinned: same version as the workflow. AutoUpgrade: the workflow's target version; if the workflow is not there yet, it transitions first (A5). |
| Activity, different task queue, same deployment and version polls both | Same as above; both queues share one routing config, and the hash uses the workflow ID. |
| Activity, task queue not in the workflow's version (other deployment, or unversioned queue) | Independent activity: routed to the Current or Ramping version of the activity's own task queue (by workflow-ID hash), or to unversioned workers if that queue has no Current. Does not affect the workflow's version. |
| Queued unpinned tasks when Current changes | They sit in the default queue and are re-targeted at dispatch time, so they go to the new Current. Nothing is migrated or rewritten. |
| Queued pinned tasks when Current changes | Unaffected; they are stored in the pinned version's own queue. |
| Sticky WFT when target changes | Matching refuses the sticky dispatch (`StickyWorkerUnavailable`), history reschedules on the normal queue, then the normal rules apply. |
| WFT in flight on the old version when Current changes | It may complete normally. Its completion sets the workflow's version to the old version (the completing worker's). The following task is routed to the new target. |

### 3.8 Child workflows, continue-as-new, retries, cron

Proto comments on `WorkflowExecutionStartedEventAttributes` (`[api] temporal/api/history/v1/message.proto`)
plus the implementations in `[srv] service/history/transfer_queue_active_task_executor.go` (lines 1010-1099,
children), `[srv] service/history/workflow/mutable_state_impl.go` (lines 2775-3007, CaN) and
`[srv] service/history/workflow/retry.go` (lines 199-243, retry and cron).

"TQ compatible" below means: the new run's task queue equals the source run's task queue, or the new run's
task queue is a member of the version in question.

**Child workflow**

- An explicit `versioning_override` on the start-child command wins over all inheritance. An invalid override
  fails the command with `START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_INVALID_VERSIONING_OVERRIDE`.
- Parent effectively Pinned: child gets `inherited_pinned_version = parent's effective version` if the child is
  in the same namespace and TQ compatible. Otherwise the child starts like a new workflow.
- Parent has a pinned (or pending one-time) override: the child inherits the override under the same condition.
  The inherited override takes precedence over the inherited base version.
- Parent AutoUpgrade: child gets `inherited_auto_upgrade_info {source_deployment_version,
  source_deployment_revision_number}` if the source version is non-nil, the revision is non-zero, same
  namespace, and TQ compatible. This does not pin anything; the child's first task is still routed to the
  target (3.7). The CaN initial behavior is not passed to children.
- Docs summary: "A Pinned child stays pinned for its lifetime. An Auto-Upgrade child changes to Auto-Upgrade
  after its first task." **(docs)** That is, the inherited version only governs the first task; afterwards the
  child's own registered behavior decides.

**Continue-as-new**

- Previous run effectively Pinned and the CaN command's `initial_versioning_behavior` is unspecified: the new
  run gets `inherited_pinned_version` if TQ compatible. So a pinned workflow stays on its version across CaN by
  default.
- A pinned override is inherited under the same TQ condition, regardless of `initial_versioning_behavior`.
- Previous run AutoUpgrade, or Pinned with a non-empty `initial_versioning_behavior`: the new run gets
  `inherited_auto_upgrade_info {source version, source revision, continue_as_new_initial_versioning_behavior}`
  (only if the source revision is non-zero and no pinned version is inherited). The first task is routed as
  unpinned to the target.
- `ContinueAsNewVersioningBehavior` (`[api] temporal/api/enums/v1/workflow.proto`): `AUTO_UPGRADE = 1` (first
  task of the new run goes to the target version: Current, or Ramping per hash), `USE_RAMPING_VERSION = 2`
  (first task goes to the Ramping version if one exists regardless of percentage, else Current). The flag only
  applies until the new run completes its first WFT (`GetShouldUseRampingVersion`,
  `mutable_state_impl.go` line 10213). After the first WFT the run follows whatever behavior the new build
  reports, so a Pinned type re-pins itself on the new version. This is "upgrade on continue-as-new".
- **Target-changed notification** for Pinned workflows (dynamic config `system.enableSendTargetVersionChanged`,
  default true; `[srv] service/history/workflow/workflow_task_state_machine.go` lines 494-544): when a WFT
  starts for a workflow that is effectively Pinned with no override, and the task queue's target for this
  workflow ID differs from the pinned version, `WorkflowTaskStartedEventAttributes.
  target_worker_deployment_version_changed` is true. The SDK exposes it so workflow code can decide to
  continue-as-new with `initial_versioning_behavior = AUTO_UPGRADE`. It is lazy: a sleeping workflow learns it
  only on its next WFT **(docs)**. If the workflow continues-as-new while staying pinned, the declined target
  is recorded as `declined_target_version_upgrade {deployment_version, revision_number}` on the new run so it
  is not notified again for the same target.

**Retry (new run from a workflow retry policy)**

- Inherits the pinned version only if the failed run is effectively Pinned at retry time **and** itself started
  with `inherited_pinned_version`. A top-level pinned workflow that retries starts fresh on the target version.
- Previous run AutoUpgrade: passes `inherited_auto_upgrade_info` (including the CaN initial behavior so the
  retry's first task is routed the same way).
- A pinned override is always inherited (retries use the same task queue).

**Cron**

- Never inherits a base version or behavior. Each cron run starts on the target version.
- A pinned override is still inherited (same task queue). The docs page lists both statements and does not
  reconcile them; source resolves it as stated here (`retry.go` lines 199-203 for the override, the
  `initiator == RETRY` guard at line 215 for the base version).

### 3.9 Versioning override

`VersioningOverride` (`[api] temporal/api/workflow/v1/message.proto`), a oneof:

| Variant | Effect |
| --- | --- |
| `pinned {behavior = PINNED, version}` | Workflow is effectively Pinned to `version` until the override is removed. The base `behavior` / `deployment_version` keep being updated by completions underneath. |
| `auto_upgrade = true` | Workflow is effectively AutoUpgrade regardless of what workers report. |
| `one_time {target_deployment_version}` | Routes like Pinned to the target until one WFT completes there, then the override is cleared and the behavior reported by that completion applies. Used to move a pinned workflow once. |

Set at start (`StartWorkflowExecution`, `SignalWithStart`), on a start-child command, via
`UpdateWorkflowExecutionOptions {workflow_execution_options.versioning_override, update_mask}`, or atomically
with a reset. Remove by sending the mask path `versioning_override` with an empty value (CLI: behavior
`unspecified`).

Validation (`[srv] common/worker_versioning/worker_versioning.go` lines 750-860,
`[srv] service/history/api/updateworkflowoptions/api.go`):

- Structure: `auto_upgrade` must be `true`; `pinned` needs a behavior and a version ("must provide version if
  override is pinned."); `one_time` needs a target version.
- On update only: `pinned` with no version is allowed if the workflow is already effectively Pinned (the
  current version is filled in). Otherwise `FailedPrecondition` "must specify a specific pinned override
  version because workflow with id %v has behavior %s and is not yet pinned to any version".
- The override's version must be a member of the workflow's task queue: `FailedPrecondition` "Pinned version
  '<deployment>:<build>' is not present in task queue '<tq>' of type 'Workflow'". The version's status is not
  checked: pinning to a Draining or Drained version is allowed.
- Only whole-override replacement is supported: "versioning_override doesn't support partial updates".
- Workflow must be running (`ErrWorkflowCompleted` otherwise). No-op updates write no event.

Effects of applying or removing an override (`updateVersioningOverride`, `mutable_state_impl.go` line 6202):

- Writes `WorkflowExecutionOptionsUpdated`.
- If the effective version or behavior changed: discard any pending transition, clear stickiness, reschedule
  the pending WFT (so it is re-routed), update search attributes.
- Always reschedule pending activities.
- If the override targets a version that is `DRAINED` or `INACTIVE`, history sends a fire-and-forget
  **reactivation signal** to that version's state machine so it goes back to `DRAINING` and its drainage is
  recomputed (`[srv] service/history/api/worker_versioning_util.go`; dynamic config
  `history.enableVersionReactivationSignals`, default true). Not needed for `CURRENT`, `RAMPING`, `DRAINING`.

