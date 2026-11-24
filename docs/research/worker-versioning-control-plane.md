# Temporal Worker Versioning control plane: Worker Deployment and Deployment Version entity workflows

Source: Temporal server clone at HEAD `4197dfdd52eb338c932e64db209e3853fc7a032d`, read only; nothing was run or modified. There is no worker-versioning architecture doc under `docs/`.

Root `R` = `/private/tmp/claude-501/-Users-calum-coding-workflow-engine/5427790e-9027-41e8-8ebf-133cf7ce1a1f/scratchpad/research-versioning/temporal`

Citation short names (each is one absolute path):

| Short name | File |
|---|---|
| `workflow.go` | `R/service/worker/workerdeployment/workflow.go` (deployment workflow) |
| `version_workflow.go` | `R/service/worker/workerdeployment/version_workflow.go` |
| `client.go` | `R/service/worker/workerdeployment/client.go` |
| `util.go` | `R/service/worker/workerdeployment/util.go` |
| `activities.go` | `R/service/worker/workerdeployment/activities.go` |
| `version_activities.go` | `R/service/worker/workerdeployment/version_activities.go` |
| `fx.go` | `R/service/worker/workerdeployment/fx.go` |
| `message.proto` | `R/proto/internal/temporal/server/api/deployment/v1/message.proto` |
| `task_queues.proto` | `R/proto/internal/temporal/server/api/persistence/v1/task_queues.proto` |
| `request_response.proto` | `R/proto/internal/temporal/server/api/matchingservice/v1/request_response.proto` |
| `workflow_handler.go` | `R/service/frontend/workflow_handler.go` |
| `matching_engine.go` | `R/service/matching/matching_engine.go` |
| `physical_task_queue_manager.go` | `R/service/matching/physical_task_queue_manager.go` |
| `task_queue_partition_manager.go` | `R/service/matching/task_queue_partition_manager.go` |
| `user_data_manager.go` | `R/service/matching/user_data_manager.go` |
| `poller_history.go` | `R/service/matching/poller_history.go` |
| `worker_versioning.go` | `R/common/worker_versioning/worker_versioning.go` |
| `constants.go` | `R/common/dynamicconfig/constants.go` |
| `sadefs/constants.go` | `R/common/searchattribute/sadefs/constants.go` |
| `mutable_state_impl.go` | `R/service/history/workflow/mutable_state_impl.go` |
| `workflow_task_state_machine.go` | `R/service/history/workflow/workflow_task_state_machine.go` |
| `history/api/worker_versioning_util.go` | `R/service/history/api/worker_versioning_util.go` |

Unless stated otherwise, everything below describes the default mode: `matching.deploymentWorkflowVersion` = 2 (`VersionDataRevisionNumber`), called "async mode" in the code, with `matching.enableWorkerDeploymentVersionDemotionSignal` = false.

---

## 1. Entity model

**Workflows.** There are two entity workflows per namespace, both on `primitives.PerNSWorkerTaskQueue` (`util.go:436`).

| Entity | Workflow type | Workflow ID |
|---|---|---|
| Worker Deployment | `temporal-sys-worker-deployment-workflow` (`util.go:40`) | `temporal-sys-worker-deployment:<deploymentName>` (`util.go:164-166`) |
| Deployment Version | `temporal-sys-worker-deployment-version-workflow` (`util.go:39`) | `temporal-sys-worker-deployment-version:<deploymentName>:<buildID>` (`util.go:170-176`) |

- Start options (`util.go:425-445`): `WorkflowIdReusePolicy ALLOW_DUPLICATE`, `WorkflowIdConflictPolicy USE_EXISTING`, search attribute `TemporalNamespaceDivision = "TemporalWorkerDeployment"` (`util.go:43`), priority key 1 (`util.go:44`).
- Inputs: `WorkerDeploymentWorkflowArgs{namespace_name, namespace_id, deployment_name, state}` and `WorkerDeploymentVersionWorkflowArgs{namespace_name, namespace_id, version_state}` (`message.proto:162-174`).
- Listing deployments uses the visibility query `WorkflowType = 'temporal-sys-worker-deployment-workflow' AND TemporalNamespaceDivision = 'TemporalWorkerDeployment' AND ExecutionStatus = 'Running'` (`util.go:117-127`).

**Interaction surface** (`util.go:47-74`):
- Updates on the deployment workflow: `register-worker-in-deployment`, `create-deployment`, `create-deployment-version`, `set-current-version`, `set-ramping-version`, `delete-version`, `delete-deployment`, `set-manager-identity`.
- Updates on the version workflow: `register-task-queue-worker`, `sync-version-state`, `update-version-metadata`, `update-version-compute-config`, `delete-version`.
- Signals: `force-continue-as-new`, `sync-drainage-status`, `terminate-drainage`, `sync-version-summary`, `propagation-complete`, `reactivate-version`, `demote-version`.
- Queries: `describe-version`, `describe-deployment`, `create-request-id`.
- Memo key: `WorkerDeploymentMemo`.

**Version strings.**
- Internal state keys use the v0.31 form `<deployment_name>.<build_id>`; the public form is `<deployment_name>:<build_id>` (`worker_versioning.go:52-54`).
- The unversioned sentinel is `__unversioned__` (`worker_versioning.go:42`).
- The parser accepts either delimiter and splits at the earliest one (`worker_versioning.go:1206-1296`). This is why both characters are banned from deployment names but allowed in build IDs.

**`WorkerDeploymentLocalState`** (`message.proto:177-192`):
- `create_time`
- `routing_config` (public `RoutingConfig`)
- `versions`: map of version string to `WorkerDeploymentVersionSummary`
- `conflict_token`
- `last_modifier_identity`
- `sync_batch_size`
- `manager_identity`
- `propagating_revisions`: map of build_id to `{revision_numbers[]}`
- `create_request_id`

**`WorkerDeploymentVersionSummary`** (`message.proto:199-245`): `version`, `create_time`, `drainage_status` (deprecated), `drainage_info`, `routing_update_time`, `current_since_time`, `ramping_since_time`, `first_activation_time`, `last_deactivation_time`, `status`, `last_current_time`, `create_request_id`, `compute_config`, `compute_status`, `task_queue_family_summary` (bloom filter).

**`VersionLocalState`** (`message.proto:74-156`):
- `version` `{deployment_name, build_id}`
- `create_time`, `routing_update_time`, `current_since_time`, `ramping_since_time`, `ramp_percentage`
- `drainage_info` `{status, last_changed_time, last_checked_time}`
- `metadata`
- `started_deployment_workflow` (deprecated)
- `task_queue_families`: map of name to `{task_queues: map<int32 type, {}>}`
- `sync_batch_size`
- `first_activation_time`, `last_deactivation_time`, `status`, `revision_number`, `last_current_time`
- `last_modifier_identity`, `compute_config`, `compute_status`

**`RoutingConfig`** (public API message; fields seen through Go usage at `workflow.go:996-1006, 1497-1507`): `CurrentDeploymentVersion`, `CurrentVersion` (deprecated string), `RampingDeploymentVersion`, `RampingVersion` (deprecated string), `RampingVersionPercentage`, `CurrentVersionChangedTime`, `RampingVersionChangedTime`, `RampingVersionPercentageChangedTime`, `RevisionNumber`. Initial value is `{CurrentVersion: "__unversioned__"}` (`workflow.go:294`).

**Conflict token.**
- It is `time.Time.MarshalBinary()` of the workflow clock.
- It is set at creation (`workflow.go:296-298`) and overwritten by every accepted set-current (`workflow.go:1607`), set-ramping (`workflow.go:1040`), set-manager (`workflow.go:1343-1377`) and create-deployment.
- It is compared with `bytes.Equal`, and only when the caller supplied a non-nil token (`workflow.go:880-ish, 1384-ish`).
- Registration, create-version, delete-version and metadata updates do not touch it.
- It changes even when an accepted update is a routing no-op, for example the same version set by a different identity.

**`RoutingConfig.revision_number`.**
- It is a per-deployment monotonic counter, incremented by exactly 1 per set-current or set-ramping call that changes routing, in async mode only (`workflow.go:1010-1014, 1510-1514`).
- It is not incremented when only the modifier identity changes (`workflow.go:971, 1486`).
- Task queues accept a routing config only if its revision is strictly greater than the stored one (`matching_engine.go:2603-2609`).
- It also keys idempotent sync requests and propagation tracking (section 5).

**`VersionLocalState.revision_number`.**
- It is a per-version counter, incremented whenever version data changes; today that means a status change (`version_workflow.go:1383-1418`).
- Task queues ignore version data whose revision is lower than the stored one; equal is accepted (`matching_engine.go:2613-2623`).

---

## 2. Registration (first poller)

**Flow.**

1. **Matching.** A poll with deployment options reaches `ensureRegisteredInDeploymentVersion` (`physical_task_queue_manager.go:645-770`).
   - If `HasDeploymentVersion` is false for the task queue's user data (`worker_versioning.go:400-420`), registration is serialized per physical queue.
   - It checks the per-task-queue limit, calls `RegisterTaskQueueWorker` with identity `"matching service"`, then blocks the poll until the version appears in user data.
2. **Client** (`client.go:342-384`).
   - The update ID is `"_auto_create_" + Fingerprint64(buildId) + "-" + Fingerprint64(taskQueueName) + "-" + taskQueueType`, which deduplicates across partitions.
   - It runs update-with-start (`ExecuteMultiOperation`) on the deployment workflow with update `register-worker-in-deployment`.
   - If the deployment workflow is not running, it first compares a visibility count of deployments with the limit (`client.go:1504-1561`).
3. **Deployment workflow `handleRegisterWorker`** (`workflow.go:701-780`).
   - `addVersionToWorkerDeployment` adds the summary `{Version, CreateTime, Status: INACTIVE}` (`workflow.go:671-699`).
   - Activity `RegisterWorkerInVersion` runs update-with-start on the version workflow (initial status INACTIVE, `client.go:1586-1626`) with update `register-task-queue-worker`, passing the deployment's current `RoutingConfig` (`workflow.go:738-755`).
   - A CREATED summary becomes INACTIVE (`workflow.go:773-776`).
4. **Version workflow `handleRegisterWorker`** (`version_workflow.go:747-833`).
   - It adds `(name, type)` to `task_queue_families` and moves CREATED to INACTIVE (`version_workflow.go:820-823`).
   - `syncRegisteredTaskQueueAsync` (`version_workflow.go:1569-1595`) writes to that one task queue: `UpdateRoutingConfig` = deployment routing config, `UpsertVersionData{Status, RevisionNumber (not incremented), UpdateTime}`.
   - It does not wait for propagation and sends no `propagation-complete`.
5. The poller unblocks when matching sees the version in user data.

**Limits.**

| Dynamic config key | Default | Enforced at | Error |
|---|---|---|---|
| `matching.maxDeployments` | 100 (`constants.go:1647-1651`) | Before starting a new deployment workflow | `ResourceExhausted` `"reached maximum deployments in namespace (%d)"` |
| `matching.maxVersionsInDeployment` | 100 (`constants.go:1652-1656`; code fallback `defaultMaxVersions = 100`, `workflow.go:26`) | `addVersionToWorkerDeployment` and create-version | Tries auto-delete; else app error type `errTooManyVersions`, `"cannot add version %s since maximum number of versions (%d) have been registered in the deployment"` |
| `matching.maxTaskQueuesInDeploymentVersion` | 100 (`constants.go:1669-1673`) | Version `validateRegisterWorker` (`version_workflow.go:728-745`); counts task queue names (families), not (name, type) pairs | App error type `errMaxTaskQueuesInVersion`, `"maximum number of task queues (%d) have been registered in deployment"` |
| `matching.maxVersionsInTaskQueue` | 200 | Matching, before registering | `ResourceExhausted` `"exceeding maximum number of Deployment Versions in this task queue (limit = %d)"` |
| `limit.maxIDLength` | 1000 (`constants.go:560-565`) | Field length validation | See validation below |

- `errTooManyVersions` and `errMaxTaskQueuesInVersion` surface to the poller as `ResourceExhausted{Scope: NAMESPACE, Cause: RESOURCE_EXHAUSTED_CAUSE_WORKER_DEPLOYMENT_LIMITS}` (`client.go:386-406`).
- Other registration failures cause a 5s backoff (`deploymentRegisterErrorBackoff`, `physical_task_queue_manager.go:53`) and then `Unavailable("task queue is not ready to process polls from this deployment version, try again shortly")` (`physical_task_queue_manager.go:111`).
- A repeat registration returns app error `"task queue already exists in deployment version"` of type `errNoChange`, which is treated as success.

**Auto-deletion when the version limit is hit** (`tryDeleteVersion`, `workflow.go:1650-1666`).
- Versions are iterated in ascending `create_time` order, oldest first (`workflow.go:1887-1906`).
- Each is tried as a server delete with `{Identity: "try-delete-for-add-version", ServerDelete: true}` and `skip_drainage` false.
- The first success wins.
- A version is eligible if it is not current, not ramping, has drainage status other than DRAINING, and has no active pollers.
- If none is eligible: `FailedPrecondition("could not add version: too many versions in deployment and none are eligible for deletion")`.

**Validation** (`ValidateDeploymentVersionFields`, `worker_versioning.go:610-640`); all are `InvalidArgument`:
- `"%v cannot be empty"`.
- `"size of %v larger than the maximum allowed"` when `len > (maxIDLength - 39) / 2`, which is 480 characters each at defaults.
- `"worker deployment name cannot contain '.'"` and `"worker deployment name cannot contain ':'"`.
- `"%v cannot start with '__'"`, applied to both fields; this is what reserves `__unversioned__`.
- Field names in messages are `WorkerDeploymentName` and `BuildID`.
- Poll-time checks (`worker_versioning.go:195-205`): `"versioned worker must have deployment name"`, `"versioned worker must have build id"`, `"deployment name cannot contain ':' or '.'"`.

**Explicit creation.**
- `CreateWorkerDeployment` (`client.go:1169-1302`):
  - It queries `create-request-id` first. The same request ID returns the existing conflict token (idempotent).
  - An existing deployment with an `_auto_create_` request ID gives `AlreadyExists` with `ErrWorkerDeploymentAlreadyExists` plus `" (auto-created from worker polls)"`; any other gives plain `AlreadyExists`.
  - Otherwise it checks the deployment limit, then runs update-with-start `create-deployment` with UpdateId `"_create_" + requestID`.
- `CreateWorkerDeploymentVersion` (`client.go:1304-1374`, handler `workflow.go:574-669`):
  - It is a plain update `create-deployment-version` (UpdateId `"_create_version_" + requestID`), so the deployment must already exist; otherwise `NotFound` with `ErrWorkerDeploymentNotFound`.
  - The same request ID is idempotent; a different one gives `AlreadyExists` with `"Worker Deployment Version %q already exists"`.
  - It applies the same max-versions and auto-delete logic.
  - It starts the version workflow with status **CREATED** (`client.go:1411-1444`) and records the summary `{Status: CREATED, CreateRequestId}` (`workflow.go:656-662`).
- CREATED means the version exists but has never had a poller. It becomes INACTIVE on the first task queue registration, or goes straight to CURRENT or RAMPING if activated with `allow_no_pollers`.

---

## 3. SetWorkerDeploymentCurrentVersion

**Frontend** (`workflow_handler.go:4180-4227`), in order:
1. Request nil.
2. Namespace empty.
3. `system.enableDeploymentVersions` (default true) off: `PermissionDenied("Worker Deployment Versions are disabled on this namespace.")`.
4. `InvalidArgument("deployment name cannot be empty")`.
5. The version string is built from `build_id`; an empty build ID means `__unversioned__`.

**Client** (`client.go:822-929`):
- `InvalidArgument("invalid version string: ...")`.
- `InvalidArgument("invalid version string '%s' does not match deployment name '%s'")`.
- Deployment name validation as in section 2.
- With `allow_no_pollers`: the build ID is validated and the call is an update-with-start, so it creates the deployment if absent (subject to `matching.maxDeployments`).
- Without it: a plain update; workflow not found gives `NotFound("no Worker Deployment found with name '%s'; does your Worker Deployment have pollers?")`.

**Workflow validator** (`workflow.go:1379-1406`), in order:
1. Deployment deleted: `errDeploymentDeleted`, surfaced as `NotFound("Worker Deployment not found")` (`util.go:366-368`).
2. No change: `CurrentVersion == args.Version && LastModifierIdentity == args.Identity`. This is app error `"no change"` of type `errNoChange`, with the conflict token in details. The client turns it into success with `PreviousVersion = version` and the current token.
3. Conflict token supplied and unequal: `"conflict token mismatch"`, `FailedPrecondition`.
4. Version not in `Versions`, not `__unversioned__`, and not `AllowNoPollers`: `"version %s not found in deployment"` of type `errVersionNotFound`, surfaced as `NotFound("build ID '%s' not found in Worker Deployment '%s'")` (`client.go:413-425`).
5. `ManagerIdentity != "" && ManagerIdentity != args.Identity`: `FailedPrecondition` with `ErrManagerIdentityMismatch` (`util.go:101-112`).

**Handler** (`workflow.go:1408-1619`):
1. `preUpdateChecks`: deleted, or `ContinueAsNewSuggested` gives `errLongHistory`, which the client retries (`workflow.go:495-514`).
2. Take the lock and re-run the validator.
3. `updateTime = workflow.Now`.
4. `AllowNoPollers` and unknown version (`workflow.go:1447-1465`): add the summary (INACTIVE) and start the version workflow (CREATED). This is subject to the max-versions and auto-delete logic.
5. Missing task queue check (`workflow.go:1467-1482`). It runs only if `!IgnoreMissingTaskQueues && prevCurrent != "__unversioned__" && newCurrent != "__unversioned__"`. Failure is `FailedPrecondition` with `ErrCurrentVersionDoesNotHaveAllTaskQueues` (`util.go:101-112`).
6. The body runs only if `prevCurrent != newCurrent` (`workflow.go:1486`). Otherwise only the token and identity change.
   - Build the pending `RoutingConfig` with `CurrentVersionChangedTime = updateTime` and `RevisionNumber++` (`workflow.go:1497-1514`).
   - If the new current equals the existing ramping version, clear the ramp: `RampingVersion = ""`, `RampingDeploymentVersion = nil`, percentage 0, and both ramping timestamps set to `updateTime` (`workflow.go:1516-1523`).
   - **New current is synced first** (if versioned): `syncVersion(newCurrent, {RoutingUpdateTime, CurrentSinceTime: updateTime, RoutingConfig})`. Then the summary's drainage status is cleared.
   - **Then the previous current** (if versioned): `syncVersion(prevCurrent, {CurrentSinceTime: nil, RoutingConfig})`, carrying the same revision.
   - The pending config is committed to state last.
7. `ConflictToken = updateTime.MarshalBinary()`, `LastModifierIdentity = args.Identity`, memo updated. The response is `{PreviousVersion, ConflictToken}`.

**`ignore_missing_task_queues` exact rule** (`client.go:1888-2025`).
- The missing set is every `(name, type)` registered in the previous current version and absent from the new one.
- The call is rejected if any missing task queue is "expected". A missing task queue is not expected when either:
  - a. `DescribeTaskQueue.VersioningInfo != nil` and its `CurrentVersion` differs from the previous current version (it has moved to another deployment), or
  - b. the enhanced describe stats for the previous current version on that type show `TasksAddRate == 0 && ApproximateBacklogCount == 0`.
- Describe failures give `FailedPrecondition` `"Version %s not found in deployment"` or `"Failed to describe version %s while checking for missing task queues: %v"`.

**Edge cases.**
- **Set to unversioned.** There is no existence check, no missing task queue check, and no sync to a new version. Only the previous current is demoted; its task queues then have no current version and are implicitly unversioned (`workflow.go` comment before the demotion block). An existing ramp is kept.
- **`allow_no_pollers`.** The version becomes CURRENT with zero task queues. Task queues receive routing data only when pollers later register (section 2, step 4). The missing task queue check still applies and usually needs `ignore_missing_task_queues`.

**Effects on versions** (version workflow, `version_workflow.go:917-1072`).

| Field | New current | Previous current |
|---|---|---|
| `status` | CURRENT | DRAINING |
| `current_since_time` | `CurrentVersionChangedTime` | nil |
| `last_current_time` | `CurrentVersionChangedTime` | unchanged |
| `routing_update_time` | `CurrentVersionChangedTime` | max of the three routing-config timestamps |
| `ramping_since_time`, `ramp_percentage` | nil, 0 | nil, 0 |
| `first_activation_time` | set if nil | unchanged |
| `drainage_info` | nil | `{DRAINING, last_changed = last_checked = now}` if previously unspecified (`version_workflow.go:501-516`) |
| `last_deactivation_time` | nil | `args.RoutingUpdateTime` |
| `revision_number` | +1 if status changed | +1 |

**INFERRED hazard.** If the new-current sync succeeds and the previous-current sync fails, the handler returns an error without committing `RoutingConfig`. The new version already believes it is current and has propagated revision N+1. The demotion-signal path's comment "Commit routing config before demoting old version to avoid partial state on step 2 failure" acknowledges this.

---

## 4. SetWorkerDeploymentRampingVersion

**Frontend** (`workflow_handler.go:4229-4289`):
- `Percentage < 0 || > 100`: `InvalidArgument("Percentage must be between 0 and 100 (inclusive)")` (`workflow_handler.go:4274-4276`).
- Mapping of `build_id` and percentage:
  - Build ID set: ramp that version. Percentage 0 is a valid 0% ramp.
  - Empty build ID with percentage > 0: ramp to `__unversioned__`.
  - Empty build ID with percentage 0: `""`, meaning **unset the ramp**.

**Client** (`client.go:931-1047`): same pattern as set-current. `errNoChange` becomes success with `PreviousVersion = version` and `PreviousPercentage = percentage`.

**Workflow validator** (`workflow.go:870-908`), in order:
1. Deployment deleted.
2. No change: same version, same percentage, and same identity. App error `"version already ramping, no change"` of type `errNoChange`.
3. Conflict token mismatch: `"conflict token mismatch"`, `FailedPrecondition`.
4. `args.Version == CurrentVersion`, unless the version is `__unversioned__` with percentage 0: `"requested ramping version %s is already current"`, `FailedPrecondition`.
5. Unknown version, not unversioned or empty, and not `AllowNoPollers`: `"requested ramping version %s not found in deployment"` of type `errVersionNotFound`, surfaced as `NotFound`.
6. Manager identity mismatch: `FailedPrecondition`.

**Handler** (`workflow.go:912-1054`): `preUpdateChecks`, lock, re-validate, `AllowNoPollers` creation (`workflow.go:945-964`). The body runs only if the version or the percentage changed (`workflow.go:971`).

Timestamps (`workflow.go:976-987, 996-1006`):
- `RampingVersionPercentageChangedTime = now` always.
- `RampingVersionChangedTime = now` for a new ramping version or an unset; it is preserved on a percentage-only change.
- `RevisionNumber++`.

**Set ramp** (`setRamp`, `workflow.go:1096-1180`):
- The missing task queue check runs only when the ramping version changes, and only if `!IgnoreMissingTaskQueues && current != "__unversioned__" && newRamping != "__unversioned__"`. It compares the current version against the new ramping version. Failure is `FailedPrecondition` with `ErrRampingVersionDoesNotHaveAllTaskQueues`.
- A percentage-only change therefore skips the task queue check.
- The new ramping version is synced first.
- A ramp to unversioned has no version workflow, so its routing config is synced through the current version's workflow and task queues.
- Then, if a different versioned previous ramp existed, it is synced with the same config (`unsetPreviousRamp`, `workflow.go:1183-1215`) and becomes DRAINING.

**Unset ramp** (`unsetRamp`, `workflow.go:1057-1093`):
- The previous ramping version is synced; if the previous ramp was unversioned, the sync goes through the current version.
- The previous ramping version's summary drainage status is set to DRAINING.

**Effects on the ramping version workflow:**
- Status RAMPING.
- `ramping_since_time = RampingVersionChangedTime`, stable across percentage changes.
- `ramp_percentage` updated.
- `routing_update_time = RampingVersionPercentageChangedTime`.
- `first_activation_time` set if nil.
- `drainage_info` and `last_deactivation_time` cleared.
- On a percentage-only change, status is unchanged, so the version revision is not incremented; only the routing config revision moves.

Promoting the ramping version to current is done through set-current (section 3), which clears the ramp.

---

## 5. Propagation to task queues

**Mechanism.**
- `syncVersion` (`workflow.go:1668-1712`) is an activity that sends update `sync-version-state` to the version workflow, carrying the full `RoutingConfig`.
- The request ID is `SyncWorkerDeploymentVersion-<revision>`, which makes retries idempotent.
- The version workflow handler (`version_workflow.go:917-1014`) updates its own state and calls `syncTaskQueuesAsync` (`version_workflow.go:1383-1418`). The update returns without waiting for task queues.
- The API call therefore returns before task queues have the data; this is the asynchronous part.

**Background propagation** (`version_workflow.go:1421-1505`):
- Task queue families are batched by `sync_batch_size` (25, `client.go:2145-2152`).
- Per batch, activity `SyncDeploymentVersionUserData` (unlimited retries, `util.go:134-149`) calls matching `SyncDeploymentUserData` once per task queue name (`version_activities.go:52-117`).
- Then `CheckWorkerDeploymentUserDataPropagation` waits until every loaded partition has user-data version at or above the returned one (`user_data_manager.go:664-709`). Unloaded partitions are skipped.

**What lands in task queue user data** (`task_queues.proto:90-120`, `matching_engine.go:2590-2685`), per task queue type:
- `DeploymentData.deployments_data[deploymentName] = WorkerDeploymentData{routing_config, versions[buildId] -> WorkerDeploymentVersionData{revision_number, update_time, deleted, status}}`.
- A version is present iff the task queue has been polled from it.
- The write goes to the root workflow partition's user data.

**Consistency rules.**
- The routing config is replaced only if the incoming `revision_number` is strictly greater (`matching_engine.go:2603-2609`).
- Version data is skipped if the stored revision is greater; equal is accepted (`matching_engine.go:2613-2623`).
- An unchanged write returns `errUserDataUnmodified`.
- Because the new and old versions both push the same revision, the arrival order of the two does not matter.

**Completion tracking.**
- The deployment workflow appends the revision to `propagating_revisions[buildId]` when it starts a sync.
- The version workflow sends signal `propagation-complete{revision_number, build_id}` when its propagation finishes (`version_workflow.go:1535-1550`).
- The deployment workflow removes the entry (`workflow.go:207-251`).
- `routing_config_update_state` is `IN_PROGRESS` iff `len(PropagatingRevisions) > 0`, else `COMPLETED` (`client.go:1824-1828`).
- The version workflow will not continue-as-new or complete while `asyncPropagationsInProgress > 0` (`version_workflow.go:361-395`).

**Scope limitation (INFERRED).** A routing config is pushed only to task queues of the version workflows that were synced: the new and previous current, or the new and previous ramping. Task queues that belong only to an unaffected version keep an older revision.

**Flags.**
- `matching.deploymentWorkflowVersion` (`constants.go:1628-1632`, enum in `fx.go:24-36`): 0 = `InitialVersion` (synchronous, old per-version data format), 1 = `AsyncSetCurrentAndRamping`, 2 = `VersionDataRevisionNumber` (default).
- The value is read once per run through a side effect (`workflow.go:92-104`).
- `matching.enableWorkerDeploymentVersionDemotionSignal` = false (`constants.go:1637-1641`). When on, the previous version is demoted by a fire-and-forget `demote-version` signal after committing the routing config (`workflow.go:110-143, 1717-1746`).
- `system.useRevisionNumberForWorkerVersioning` = true (`constants.go:1100-1104`).

---

## 6. Drainage

**Query** (`client.go:1857-1886`): a visibility count of

`TemporalWorkerDeploymentVersion = '<deploymentName>:<buildId>' AND TemporalWorkflowVersioningBehavior = 'Pinned' AND ExecutionStatus = 'Running'`

Count 0 means `DRAINED`; otherwise `DRAINING`. Only running pinned workflows count. AutoUpgrade workflows are ignored.

**Timing.**

| Dynamic config key | Default |
|---|---|
| `matching.wv.VersionDrainageStatusVisibilityGracePeriod` | 3m (`constants.go:629-634`) |
| `matching.wv.VersionDrainageStatusRefreshInterval` | 3m (`constants.go:635-640`) |

The in-code fallbacks if the side-effect read fails are grace 3m and refresh 5m (`version_workflow.go:29-31`).

**Loop** (`refreshDrainageInfo`, `version_workflow.go:1122-1195`):
- It starts at the beginning of a run only if `drainage_info.status == DRAINING` (`version_workflow.go:266-268`).
- The first check waits the grace period; the first check is identified by `last_checked_time == last_changed_time`. Later checks wait the refresh interval. The wait is reduced by the time already elapsed.
- It runs activity `GetVersionDrainageStatus`, takes the lock, and re-checks that the status is still DRAINING.
- It updates `last_checked_time`. On a status change it sets `last_changed_time`, sets the version status, bumps the version revision, and pushes the status to task queues (`version_workflow.go:1238-1256`).
- It sends `sync-version-summary` to the deployment workflow, then continues-as-new. There is one check per run.

**Stops when:**
- the status becomes DRAINED,
- the version becomes current or ramping (`drainage_info` set to nil), or
- the version is deleted.

Once DRAINED, the version is never re-checked unless reactivated.

**Reactivation by routing.** Becoming CURRENT or RAMPING clears `drainage_info`. A later demotion restarts tracking from scratch.

**Reactivation by pinned override.**
- History calls `ReactivateVersionWorkflowIfPinned` (`history/api/worker_versioning_util.go:34-63`) after: start workflow with override, signal-with-start, reset with options, and `UpdateWorkflowExecutionOptions`. It runs after successful persistence and only for newly created runs.
- Conditions: `history.enableVersionReactivationSignals` (default true, `constants.go:3652-3658`), the override targets a version (Pinned or OneTime), and matching did not report the version as CURRENT, RAMPING or DRAINING (`ShouldSkipReactivation`, `worker_versioning.go:423-459`).
- Membership and status are cached for `history.versionMembershipCacheTTL` = 1s.
- It is fire-and-forget: signal `reactivate-version`, identity `"history-service"`. The request ID is a UUIDv5 of `"reactivation-signal:<version revision>"`, with a per-pod LRU dedup of size `worker.reactivationSignalDedupCacheMaxSize` = 10000 (`client.go:2074-2143`).
- Handler (`version_workflow.go:153-187`):
  - The signal is dropped if the version is deleted.
  - It acts only if status is **DRAINED or INACTIVE**: it sets `drainage_info = {DRAINING, now, now}`, sets status DRAINING, pushes to task queues, syncs the summary, and continues-as-new.
  - The loop then restarts with the grace period.
  - It does not set `last_deactivation_time`.

**How executions get the search attributes.** The history service's mutable state writes them and emits an upsert-visibility task when they change (`mutable_state_impl.go:3977-3991, 4240-4311`). Write points:
- At workflow start, if the effective behavior is not unspecified, for example a pinned override at start (`mutable_state_impl.go:3355-3361`).
- At every workflow task completion (`workflow_task_state_machine.go:1435`).
- When the versioning override changes (`mutable_state_impl.go:6314`).

Values (`mutable_state_impl.go:10073-10111`, `sadefs/constants.go:80-106, 184-187`):

| Search attribute | Type | Value |
|---|---|---|
| `TemporalWorkerDeploymentVersion` | Keyword | `<deployment>:<buildId>` of the override target if any, else the version of the last completed workflow task |
| `TemporalWorkerDeployment` | Keyword | Deployment name, same precedence |
| `TemporalWorkflowVersioningBehavior` | Keyword | Enum string of the override behavior if any, else the SDK-reported behavior; removed when unspecified |
| `TemporalUsedWorkerDeploymentVersions` | KeywordList | Every `<deployment>:<buildId>` that completed a workflow task; an override alone does not add an entry |
| `BuildIds` | KeywordList | Deprecated; gets `pinned:<version>` tags |

---

## 7. Deletion

**DeleteWorkerDeploymentVersion.** Client `client.go:1049-1115`; identity defaults to the request ID.

Deployment workflow validator (`validateDeleteVersion`, `workflow.go:1239-1262`), in order:
1. Deployment deleted: `NotFound`.
2. Version not in `Versions`: app error `"version not found in deployment"` of type `errVersionNotFound`. The client returns success (idempotent).
3. Version is current or ramping: `FailedPrecondition("version '%s' cannot be deleted since it is current or ramping")`.
4. Manager identity mismatch, unless `ServerDelete`: `FailedPrecondition`.

Then the handler (`workflow.go:1296-1324`) runs activity `DeleteWorkerDeploymentVersion` (`activities.go:148-206`), which sends update `delete-version` to the version workflow. A `NotFound` there is treated as success.

Version workflow handler (`version_workflow.go:530-632`), in order:
1. Already deleted: return (idempotent).
2. `!SkipDrainage && drainage_info.status == DRAINING`: `FailedPrecondition("version '%s' cannot be deleted since it is draining")` (`version_workflow.go:578-583`).
3. Active pollers: `FailedPrecondition("version '%s' cannot be deleted since it has active pollers")` (`version_workflow.go:586-590`). `skip_drainage` does not skip this check.
4. Set `deleteVersion = true`. Cancel in-flight propagations. Send `ForgetVersion` for all registered task queues and wait for propagation (`version_workflow.go:634-699`).
5. The workflow completes once propagations finish (`version_workflow.go:393-395`).

The deployment workflow then removes the summary and its `propagating_revisions` entry (`workflow.go:1264-1294`).

**Active pollers** (`CheckIfTaskQueuesHavePollers`, `version_activities.go:156-186`):
- For each registered task queue it calls `DescribeTaskQueue` in ENHANCED mode with `Versions.BuildIds = ["<name>:<buildId>"]` and `ReportPollers: true`.
- Any poller entry for the workflow, activity or nexus type counts.
- Entries come from a TTL cache with TTL `matching.PollerHistoryTTL` = 5m (`constants.go:615-620`; `physical_task_queue_manager.go:162, 539-545`; `poller_history.go:30-39`).
- The effective rule is "no poll in the last 5 minutes".

**Task queue cleanup** (`removeDeploymentVersions`, `matching_engine.go:4161-4198`):
- The build ID is removed from the task queue's version map, in both formats.
- The deployment's entry is removed from the task queue when no versions remain.

**Race.** If a poller registers after deletion is flagged but before the workflow closes, the version is revived with fresh INACTIVE state (`reviveDeleted`, `version_workflow.go:835-846`).

**DeleteWorkerDeployment** (`client.go:1117-1167`; `workflow.go:840-864`):
- The validator rejects if `len(Versions) > 0` with `serviceerror.NewFailedPrecondition("deployment has versions, can't be deleted")`.
- Otherwise the workflow completes.
- Workflow not found returns success.
- There is no manager identity check on this path.

**Automatic deletion.** The only automatic deletion is `tryDeleteVersion` when adding a version over the limit (section 2). I found no time-based garbage collection of versions or deployments.

---

## 8. Version status state machine

| From | To | Trigger |
|---|---|---|
| (none) | CREATED | `CreateWorkerDeploymentVersion`, or `allow_no_pollers` creation (`client.go:1411-1444`) |
| (none) | INACTIVE | First poller registration (`client.go:1586-1626`); also UNSPECIFIED at run start (`version_workflow.go:261-263`) |
| CREATED | INACTIVE | First task queue registered (`version_workflow.go:820-823`) |
| CREATED, INACTIVE, DRAINING, DRAINED | CURRENT | Routing config names it current (`version_workflow.go:1201-1217`) |
| CREATED, INACTIVE, DRAINING, DRAINED | RAMPING | Routing config names it ramping |
| RAMPING | CURRENT | Set-current on the ramping version |
| CURRENT, RAMPING | DRAINING | Was active and is now neither current nor ramping |
| DRAINING | DRAINED | Visibility count reaches 0 |
| DRAINED, INACTIVE | DRAINING | `reactivate-version` signal |
| Any except CURRENT, RAMPING | deleted | Delete with the section 7 checks; the workflow closes |
| deleted, still open | INACTIVE | New poller registers (`reviveDeleted`) |

CURRENT to RAMPING is not directly reachable, because the validator rejects ramping the current version.

**Timestamps.**

| Field | Rule |
|---|---|
| `create_time` | Set at first run start |
| `first_activation_time` | Set on the first CURRENT or RAMPING; never reset (`version_workflow.go:997-1001`; summary preserved at `workflow.go:253-272`) |
| `last_current_time` | Set to `CurrentVersionChangedTime` each time it becomes CURRENT |
| `current_since_time` | `CurrentVersionChangedTime` while CURRENT, else nil |
| `ramping_since_time` | `RampingVersionChangedTime` while RAMPING, else nil; stable across percentage changes |
| `last_deactivation_time` | Set on transition to DRAINING; cleared on reactivation to CURRENT or RAMPING |
| `routing_update_time` (public `RoutingChangedTime`, `client.go:1724`) | CURRENT: current-changed time. RAMPING: percentage-changed time. DRAINING: max of the three routing-config timestamps |

---

## 9. Other behavior

**UpdateWorkerDeploymentVersionMetadata.**
- Frontend: `InvalidArgument("At least one of remove_entries or upsert_entries must be provided")` (`workflow_handler.go:4493-4523`).
- Version workflow (`version_workflow.go:406-456`): upserts, then removes entries; sets `last_modifier_identity`.
- It takes no lock, has no manager identity check, and is not propagated to the deployment workflow or to task queues.

**SetWorkerDeploymentManager.**
- Frontend: `InvalidArgument("Identity is required")` (`workflow_handler.go:4436`).
- Workflow (`workflow.go:1326-1377`):
  - No change (same manager and same last modifier) gives `"no change"` of type `errNoChange`.
  - Conflict token mismatch gives `FailedPrecondition`.
  - Otherwise it sets `manager_identity`, `last_modifier_identity`, and a new conflict token.
- It does not check the existing manager, so anyone can take over.
- The manager check gates set-current, set-ramping and user delete-version only.

**Continue-as-new.**
- Both workflows continue-as-new after any handled update or signal once idle, carrying state as input.
- Deployment condition (`workflow.go:440-445`): no pending signals, all handlers finished, and `forceCAN || stateChanged || ContinueAsNewSuggested`.
- Version condition (`version_workflow.go:361-388`): the same, plus no propagations in progress.
- While `ContinueAsNewSuggested` is set, new updates are rejected with `errLongHistory` and the client retries.

**Serialization.**
- Each workflow has a single workflow-level lock, held for the whole of each update handler.
- Validators run before acceptance and again inside the lock.
- The version workflow has a second lock, with the documented ordering "always acquire `lock` before `computeConfigLock`" (`version_workflow.go:36-67`).

**Client retries.**
- Exponential backoff from 100ms, expiring after 1 minute (`client.go:233`).
- Retryable conditions are in `isRetryableUpdateError` (`util.go:385-423`).
- Exhausted retries surface as `ResourceExhausted{Cause: BUSY_WORKFLOW}` with `"too many requests issued to Worker Deployment '%s'. Please try again later"` (`client.go:1628-1694`).

**Activity options.** `StartToClose` 1m, initial interval 100ms, 5 attempts. Propagation activities have unlimited attempts (`util.go:134-149`).

**Summaries.** The version workflow pushes `sync-version-summary` to the deployment workflow on drainage changes (`version_workflow.go:1092-1120`). The deployment workflow memo carries the routing config and the latest, current and ramping summaries (`workflow.go:1862-1881`).

**Old deleted version cleanup.** `CleanupOldDeletedVersions` (`worker_versioning.go:1299-1348`) is deprecated. It removes version data flagged `deleted` that is older than 7 days, or earlier if over `matching.maxVersionsInTaskQueue`.

---

## Things I could not verify

- **Public API protos.** `go.temporal.io/api` is not vendored. `RoutingConfig` field numbers and the exact enum strings (`'Pinned'`, `'AutoUpgrade'`) are INFERRED from Go usage and the drainage query literal.
- **Validator error encoding.** That a validator-returned `serviceerror.FailedPrecondition` is encoded with failure type `"FailedPrecondition"` is INFERRED from the constant `errFailedPrecondition` and `convertUpdateFailure` (`util.go:349-378`). If it is not, `DeleteWorkerDeployment` with versions surfaces as `Internal` (`client.go:1162-1164`).
- **Poller aggregation.** I did not trace whether enhanced `DescribeTaskQueue` aggregates pollers across all partitions, or that each poll refreshes the TTL entry. The 5-minute window is INFERRED from the cache TTL.
- **Legacy signals.** I found no non-test sender for `sync-drainage-status` or `terminate-drainage`; they appear to be legacy.
- **Partial-failure behavior.** The set-current partial state described in section 3 is reasoned from the code, not observed.
- **`__unversioned__` to nil.** That `ExternalWorkerDeploymentVersionFromStringV31("__unversioned__")` returns nil is INFERRED from the reverse conversion.
- **Not covered.** Compute config and worker controller instance paths, the bloom-filter task queue family summary internals, and how matching uses the routing config to route tasks.
- **Line numbers.** Citations written with "-ish", and some ranges for `client.go` and `workflow_handler.go`, come from my earlier read and were not re-checked in the final pass.
