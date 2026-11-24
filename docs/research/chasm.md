# CHASM: Temporal's "Coordinated Heterogeneous Application State Machines"

Research notes for designing a Rust workflow engine "built using CHASM principles".

- **Source:** `github.com/temporalio/temporal`, shallow clone at commit
  `4197dfdd52eb338c932e64db209e3853fc7a032d` (committer date 2026-11-23).
- **Method:** read-only source reading. Nothing from the clone was built or run.
- **Paths** below are relative to the root of that repository.
- **Confidence labels:** unmarked statements were read directly in source at that commit.
  Statements marked **(inferred)** are my reading between the lines. Section 8 lists what I did
  not read or could not verify.

---

## 1. What CHASM is and the problem it solves

CHASM is a framework inside the Temporal server (package `chasm/`, engine in
`service/history/chasm_engine.go`) for building durable, sharded, replicated state machines
*other than* Workflows, on the same infrastructure Workflows use.

The official introduction (`docs/architecture/chasm.md`) frames the problem this way:

- Workflows are "too slow or heavyweight for some problems, unable to scale in every dimension
  (e.g. millions of signals, large payloads), and overly complex when a purpose-built solution
  would be simpler".
- CHASM treats "Workflow as just one Application State Machine (ASM) among many". An ASM
  "leverages Temporal infrastructure like sharding, routing, atomic storage, failure recovery
  without the full cost of a Workflow" and hides the distributed-systems details "behind a clean,
  typed API".

In concrete terms, a CHASM archetype is a *state-based* durable object, not an *event-sourced*
one. There is no history of events and no replay. Each execution is a tree of typed components
whose current state is persisted node by node, mutated in atomic transitions, and driven forward
by tasks written in the same transaction as the state (transactional outbox). The engine rejects
any CHASM transition that produces history events ("CHASM framework does not support events yet",
`service/history/chasm_engine.go`, `createNewExecutionWithUpdate`).

Libraries built on it in this commit (`chasm/lib/`): `scheduler`, `activity` (standalone
activities), `callback`, `nexusoperation`, `workflow` (the Workflow archetype itself, which keeps
its legacy mutable state and uses the tree for newer sub-features such as callbacks and signal
backlinks), `worker`, `flowcontrol`, `namespacereplication`, and `tests`.

CHASM does not have its own storage engine or its own service. It rides on the History service:

- A CHASM execution *is* a `MutableState` record whose `chasm_nodes` map is populated
  (`proto/internal/temporal/server/api/persistence/v1/workflow_mutable_state.proto`,
  `WorkflowMutableState.chasm_nodes = 12`). `BusinessID` is stored in the `WorkflowId` column.
- It uses the same shard routing (`common.WorkflowIDToHistoryShard(NamespaceID, BusinessID, N)`),
  execution cache and per-execution lock, current-run pointer, history task queues (transfer,
  timer, outbound, visibility), retention, and replication stack.
- Library gRPC services are registered on the History service's gRPC server
  (`service/history/service.go`, `s.chasmRegistry.RegisterServices(s.server)`), and an
  interceptor puts the engine into the request context (`chasm/interceptors.go`,
  `ChasmEngineInterceptor`).

---

## 2. Core concepts

### 2.1 Vocabulary

| Term | Meaning | Where |
| --- | --- | --- |
| ASM | A registered state machine type: Library + Component types + Tasks | `docs/architecture/chasm.md` |
| Library | Namespace grouping components, tasks, and gRPC/Nexus services | `chasm/library.go` |
| Registry | Global catalog of libraries, indexed by name, ID, and Go type | `chasm/registry.go` |
| Archetype | Fully qualified name of a *root* component type (`library.component`) | `chasm/registrable_component.go` |
| ArchetypeID | `uint32` = `farm.Fingerprint32(fqn)`; `0` is reserved (`UnspecifiedArchetypeID`) | `chasm/registry.go` (`GenerateTypeID`) |
| Execution | A runtime instance; identified by `ExecutionKey{NamespaceID, BusinessID, RunID}` | `chasm/ref.go` |
| Node | Unit of storage in the tree; four kinds: component, data, collection, pointer | `chasm.proto`, `chasm/tree.go` |
| Transition | One user function applied to the tree under the execution lock | `chasm/engine.go` |
| VersionedTransition (VT) | `{namespace_failover_version, transition_count}`; the logical clock | `hsm.proto` |
| ComponentRef | Serialized address plus consistency token for one component | `chasm/ref.go` |

### 2.2 Component

`chasm/component.go`:

```go
type Component interface {
    LifecycleState(Context) LifecycleState
    mustEmbedUnimplementedComponent()
}

type TerminableComponent interface {
    Component
    Terminate(MutableContext, TerminateComponentRequest) (TerminateComponentResponse, error)
}

type RootComponent interface {
    TerminableComponent
    ContextMetadata(Context) map[string]string
}
```

A component is a plain Go struct that embeds `chasm.UnimplementedComponent`. The framework
discovers its shape by reflection (`chasm/fields_iterator.go`):

- Exactly **one** struct field that implements `proto.Message` is the component's own state
  (zero or more than one is an error). It is serialized into the component node's data blob.
- Fields of type `chasm.Field[T]`, `chasm.Map[K, T]`, `chasm.ParentPtr[T]`, and `chasm.MSPointer`
  are framework-managed.
- Every other field is unmanaged and transient: in-memory only, never persisted (the registry
  logs a warning for them). Components use these for caches that are invalidated by a persisted
  token (for example `Scheduler.cacheConflictToken` in `chasm/lib/scheduler/scheduler.go`).
- A child node's name is the Go field name, or the value of a `name:"..."` struct tag. Names
  beginning with `__chasm` are reserved.

Lifecycle (`chasm/component.go`): `LifecycleStateRunning` and `LifecycleStatePaused` are open;
`LifecycleStateCompleted` and `LifecycleStateFailed` are closed (`IsClosed()` is
`s >= LifecycleStateCompleted`). Lifecycle is *derived*: the framework calls `LifecycleState` on
the component; there is no stored framework status per component.

### 2.3 Field, Map, ParentPtr, pointers

`chasm/field.go`, `chasm/map.go`, `chasm/parent_pointer.go`, `chasm/component_field_option.go`:

```go
type Field[T any] struct { Internal fieldInternal }   // exported only so reflection can set it

func NewDataField[D proto.Message](ctx MutableContext, d D) Field[D]
func NewComponentField[C Component](ctx MutableContext, c C, opts ...ComponentFieldOption) Field[C]
func ComponentPointerTo[C Component](ctx MutableContext, c C) Field[C]
func DataPointerTo[D proto.Message](ctx MutableContext, d D) Field[D]
func NewEmptyField[T any]() Field[T]                  // assigning this deletes the child node

func (f Field[T]) TryGet(Context) (T, bool)
func (f Field[T]) Get(Context) T                      // lazy deserialize; panics on framework errors

type Map[K string | int | int8 | int32 | int64 | uint | uint8 | uint32 | uint64 | bool, T any] map[K]Field[T]

type ParentPtr[T any] struct { Internal parentPtrInternal }
func (p ParentPtr[T]) Get(Context) T

func ComponentFieldDetached() ComponentFieldOption
func ComponentFieldEphemeral() ComponentFieldOption
```

- A `Field[T]` is a handle to a child **node**. `T` is either a proto message (data node) or a
  component (component subtree). A `Map` is a collection node whose children are fields.
- Every field and map entry is "persisted as separate nodes. Use a separate field for data that
  changes at a different frequency, is large, or is only read in certain operations"
  (`docs/architecture/chasm.md`). This is the main tool for controlling write amplification.
- Deserialization is lazy and per node. Deserializing a component creates field handles pointing
  at child nodes without decoding the children.
- **Getting a field with a `MutableContext` marks its node dirty**; the framework assumes the
  caller mutates what it fetched (`chasm/tree.go`, value states). There is no explicit `Set`.
- Pointers store a node path (`ChasmPointerAttributes.node_path`). `ParentPtr[T]` resolves to the
  nearest ancestor component (skipping map nodes); `T` may be an interface, which is how the
  reusable `Callback` component reads its parent as a `CompletionSource`
  (`chasm/lib/callback/component.go`).
- **Detached** components are exempt from the "closed ancestor blocks writes" access rule.
  **Ephemeral** components exist only in memory: never persisted or replicated, and cannot have
  tasks.
- `MSPointer` (`chasm/ms_pointer.go`) gives the Workflow component access to legacy mutable state.
  It exists for migration, not as a general feature.

### 2.4 Context and MutableContext

`chasm/context.go`:

```go
type Context interface {
    Ref(Component) ([]byte, error)   // ref to the component's state at the START of the transition
    Now(Component) time.Time         // the only legal clock inside a transition
    ExecutionKey() ExecutionKey
    ExecutionInfo() ExecutionInfo
    Path(Component) []string
    Logger() log.Logger
    NamespaceEntry() *namespace.Namespace
    Value(key any) any
    // plus metrics, request headers, links, user metadata, time-skipping getters
}

type MutableContext interface {
    Context
    AddTask(Component, TaskAttributes, any)
    // plus SetRequestLinks, SetUserMetadata, time-skipping configuration
}
```

- The context is not bound to a component; every method takes the component as an argument.
- `Now` is constant for the life of the context and "must be used to allow for framework support
  of pause and time skipping".
- A component created in the current transaction has no ref yet (it is not a node until
  structure sync).
- Operation intent travels on the Go context (`NewContextWithOperationIntent`):
  `OperationIntentProgress` is rejected against closed executions/components,
  `OperationIntentObserve` (also the default) is always allowed.

### 2.5 Engine API

`chasm/engine.go` defines the interface; `service/history/chasm_engine.go` implements it.

```go
type Engine interface {
    StartExecution(ctx, ComponentRef, func(MutableContext) (RootComponent, error), ...TransitionOption) (StartExecutionResult, error)
    UpdateWithStartExecution(ctx, ComponentRef, startFn, updateFn func(MutableContext, Component) error, ...TransitionOption) (EngineUpdateWithStartExecutionResult, error)
    UpdateComponent(ctx, ComponentRef, func(MutableContext, Component) error, ...TransitionOption) ([]byte, error)
    ReadComponent(ctx, ComponentRef, func(Context, Component) error, ...TransitionOption) error
    PollComponent(ctx, ComponentRef, func(Context, Component) (bool, error), ...TransitionOption) ([]byte, error)
    DeleteExecution(ctx, ComponentRef, DeleteExecutionRequest) error
    NotifyExecution(ExecutionKey)
}
```

Application code never calls the interface directly. It calls typed generic wrappers that pull
the engine from the context:

```go
func StartExecution[C RootComponent, I any](ctx, key ExecutionKey,
    startFn func(MutableContext, I) (C, error), input I, opts ...TransitionOption) (StartExecutionResult, error)

func UpdateWithStartExecution[C RootComponent, I, O any](ctx, key ExecutionKey,
    startFn func(MutableContext, I) (C, error),
    updateFn func(C, MutableContext, I) (O, error), input I, opts ...) (UpdateWithStartExecutionResult[O], error)

func UpdateComponent[C any, R []byte | ComponentRef, I, O any](ctx, ref R,
    updateFn func(C, MutableContext, I) (O, error), input I, opts ...) (O, []byte, error)

func ReadComponent[C any, R []byte | ComponentRef, I, O any](ctx, ref R,
    readFn func(C, Context, I) (O, error), input I, opts ...) (O, error)

func PollComponent[C any, R []byte | ComponentRef, I, O any](ctx, ref R,
    monotonicPredicate func(C, Context, I) (O, bool, error), input I, opts ...) (O, []byte, error)

func DeleteExecution[C RootComponent](ctx, key ExecutionKey, req DeleteExecutionRequest) error
```

The receiver-first shape (`func(C, MutableContext, I) (O, error)`) lets handlers pass Go method
expressions as transition functions: `chasm.UpdateComponent(ctx, ref, (*Scheduler).Update, req)`.

Options (`TransitionOptions`): `WithRequestID`, `WithBusinessIDPolicy(reuse, conflict)`,
`WithSpeculative`, `WithAssumeNew`, `WithRefConsistencyLevel`.

- `BusinessIDReusePolicy{AllowDuplicate, AllowDuplicateFailedOnly, RejectDuplicate}` governs
  starting when a *closed* run exists; `BusinessIDConflictPolicy{Fail, TerminateExisting,
  UseExisting}` governs starting when a *running* one exists. `TerminateExisting` is not
  implemented, with a comment that state-based replication cannot replicate close-plus-start
  transactionally (`handleConflictPolicy`).
- `RefConsistencyLevel{ExecutionLastUpdate (default), ComponentCreation, CurrentRun}`.

Engine behavior verified in `service/history/chasm_engine.go`:

- **StartExecution**: lock the current-execution slot, build a new `MutableState` with a fresh
  RunID, run `startFn`, `SetRootComponent`, close the transaction as a snapshot, persist with
  `CreateWorkflowModeBrandNew`. On a current-run conflict: same request ID returns the existing
  run (idempotent start); a running execution applies the conflict policy
  (`ExecutionAlreadyStartedError` or return the existing ref); a closed one applies the reuse
  policy and then creates with `CreateWorkflowModeUpdateCurrent`.
- **UpdateComponent**: take the execution lease (lock plus staleness check, section 5), dedup by
  request ID (`ErrRequestIDAlreadyUsed`), resolve the component from the ref, run `updateFn`,
  record the request ID, persist via `UpdateWorkflowExecutionAsActive`, and return a *new*
  serialized ref for the component (nil if the transition deleted it).
- **ReadComponent**: same lease, read-only context, release with no write.
- **PollComponent**: evaluate the predicate under the lock; if unsatisfied, subscribe to
  `ChasmNotifier` for the execution key *before* releasing the lock, then loop on notifications.
  Notifications are per execution, carry no payload (a closed channel), and the waiter
  re-evaluates under the lock (`service/history/chasm_notifier.go`). The predicate must be
  monotonic. Returns `(nil, nil, nil)` on server-side long-poll timeout.
- **UpdateWithStartExecution**: with `WithAssumeNew` it tries the create first; otherwise it
  loads the current run and either updates it (running), starts a new run (closed), or
  starts-and-updates (not found).
- **DeleteExecution**: if running on the active cluster, mark delete-after-close, `Terminate`,
  and add a delete task in one transaction; otherwise add the delete task directly.
- **Speculative transitions** (`WithSpeculative`): applied in memory only and not persisted
  until a later transition; lost on cache eviction, shard movement, or restart; not replicated;
  not allowed for `StartExecution`.
- User functions are wrapped with panic capture; a panic becomes an error, not a crash.

Request-ID idempotency for updates is stored in one of two places depending on a dynamic config
flag (`EnableChasmRequestIDDataNode`): a framework-owned data node `__chasm_requestIDs` under the
root (`chasm/request_ids.go`, bounded by max count and max age), or the legacy
`WorkflowExecutionState.request_ids` map.

### 2.6 ComponentRef

`chasm/ref.go`, serialized as `ChasmComponentRef` (`chasm.proto`):

```go
type ExecutionKey struct { NamespaceID, BusinessID, RunID string }

type ComponentRef struct {
    ExecutionKey
    archetypeID           ArchetypeID
    executionLastUpdateVT *VersionedTransition  // consistency token for the whole execution
    componentPath         []string
    componentInitialVT    *VersionedTransition  // identity of this incarnation of the node
    validationFn          func(NodeBackend, Context, Component, *Registry) error // task validation hook
}

func NewComponentRef[C Component](key ExecutionKey) ComponentRef   // root only
func (r *ComponentRef) Serialize(*Registry) ([]byte, error)
func DeserializeComponentRef([]byte) (ComponentRef, error)
```

- Without a ref, callers can only address the root of an execution by key. Refs to
  sub-components are minted by the framework (`ctx.Ref(component)`), serialized to opaque bytes,
  and handed out as tokens (task tokens, callback return addresses, long-poll tokens).
- An empty `RunID` means "the current run".
- `componentInitialVT` is omitted for the root (`refComponentInitialVT` in `chasm/tree.go`).

### 2.7 Library and Registry

`chasm/library.go`, `chasm/registry.go`, `chasm/registrable_component.go`,
`chasm/registrable_task.go`:

```go
type Library interface {
    Name() string
    Components() []*RegistrableComponent
    Tasks() []*RegistrableTask
    RegisterServices(*grpc.Server)
    NexusServices() []*nexus.Service
    NexusServiceProcessors() []*NexusServiceProcessor
    mustEmbedUnimplementedLibrary()
}

func NewRegistrableComponent[C Component](componentType string, opts ...RegistrableComponentOption) *RegistrableComponent
func NewRegistrablePureTask[C any, T any](taskType string, h PureTaskHandler[C, T], opts ...RegistrableTaskOption) *RegistrableTask
func NewRegistrableSideEffectTask[C any, T any](taskType string, h SideEffectTaskHandler[C, T], opts ...RegistrableTaskOption) *RegistrableTask
```

- Component options: `WithEphemeral`, `WithSingleCluster`, `WithDetached`, `WithRetention`,
  `WithBusinessIDAlias`, `WithSearchAttributes`, `WithContextValues`, `WithExecutionType`,
  `WithRecordPrincipals`. Task options: `WithTaskGroup`,
  `WithSingletonTask(Replace | Ignore)`, `WithTaskCountMetric`.
- The registry indexes components and tasks by fully qualified name, by ID, and by Go type. It
  rejects ID collisions, duplicate Go types, invalid names, and reserved field-name prefixes at
  registration time.
- Persisted data stores only numeric type IDs (`ChasmComponentAttributes.type_id`,
  `Task.type_id`). The ID is a hash of the name, so **names are persisted schema**.
- The task's component type parameter `C` may be an interface, so one task type can target any
  component that implements it.
- Each library also exports `NewNilLibrary()` so offline tools can register types and decode
  trees without handlers (`chasm/AGENTS.md`).
- The built-in `core` library registers `*Visibility` as `core.vis` (detached) and the
  side-effect task `visTask` (`chasm/library_core.go`).

### 2.8 Tasks, handlers, validators

`chasm/task.go`, `chasm/task_handler_base.go`:

```go
type TaskAttributes struct {
    ScheduledTime time.Time   // zero value (TaskScheduledTimeImmediate) = run as soon as possible
    Destination   string      // only for immediate tasks; routes to the outbound queue
}

type TaskInvocation struct { TaskAttributes; Attempt int }   // Attempt is not persisted

type TaskValidator[C any, T any] interface {
    Validate(Context, C, TaskInvocation, T) (bool, error)
}

type PureTaskHandler[C any, T any] interface {
    TaskValidator[C, T]
    Execute(MutableContext, C, TaskAttributes, T) error
}

type SideEffectTaskHandler[C any, T any] interface {
    TaskValidator[C, T]
    Execute(context.Context, ComponentRef, TaskAttributes, T) error
    Discard(context.Context, ComponentRef, TaskAttributes, T) error
}
```

The two handler signatures encode the whole execution model:

- A **pure** task handler receives the component and a `MutableContext`. It has no Go
  `context.Context`, so it cannot do I/O. It runs inside a transaction under the execution lock.
- A **side-effect** task handler receives a Go context and a `ComponentRef`, *not* the component.
  It runs outside the lock and can only touch state by calling `ReadComponent` /
  `UpdateComponent` with that ref, "the same API as any external caller"
  (`docs/architecture/chasm.md`).
- **Every** task type has a validator. `Validate` returning `(false, nil)` silently drops the
  task. Its two documented purposes are standby deduplication (the active cluster already did the
  work) and obsolescence (state moved on).
- The task payload `T` is a proto message (or a struct with at most one proto field), serialized
  with deterministic proto encoding.
- `Discard` is the standby-side hook for a side-effect task that has been pending too long; the
  base implementation returns `ErrTaskDiscarded`.

### 2.9 StateMachine / Transition helper

`chasm/statemachine.go`. Optional sugar; the engine does not depend on it.

```go
type StateMachine[S comparable] interface {
    StateMachineState() S
    SetStateMachineState(S)
}

type Transition[S comparable, SM StateMachine[S], E any] struct {
    Sources     []S
    Destination S
    apply       func(SM, MutableContext, E) error
}

func NewTransition[S comparable, SM StateMachine[S], E any](src []S, dst S, apply func(SM, MutableContext, E) error) Transition[S, SM, E]
func (t Transition[S, SM, E]) Possible(sm SM) bool
func (t Transition[S, SM, E]) Apply(sm SM, ctx MutableContext, event E) error
```

`Apply` returns `ErrInvalidTransition` (a FailedPrecondition) if the current state is not in
`Sources`; otherwise it runs `apply` and then sets the destination state. Transitions are
package-level values, one per event type.

### 2.10 Lifecycle and access rules

- When the **root** component's `LifecycleState` becomes closed, the execution closes: the
  framework maps it onto the execution state/status at transaction close
  (`closeTransactionHandleRootLifecycleChange` in `chasm/tree.go`: Completed maps to
  COMPLETED/COMPLETED, Failed to COMPLETED/FAILED, terminated to TERMINATED). The BusinessID
  becomes reusable, and a retention timer is generated
  (`closeTransactionGenerateChasmRetentionTask` in
  `service/history/workflow/mutable_state_impl.go`).
- **Access rule** (`validateAccess` in `chasm/tree.go`): for `OperationIntentProgress`, every
  non-detached ancestor of the target must be open (and not paused, when validating tasks). A
  violation returns NotFound ("CHASM tree is closed for writes"). Detached components skip the
  ancestor check, which is what lets Visibility and completion callbacks keep working after the
  root closes. Observe-intent operations are always allowed.
- Note from `chasm/lib/scheduler/scheduler.go` (`Scheduler.Update`): "UpdateComponent does not
  reject mutations on completed executions, so we must check explicitly here." The access rule
  only applies when the caller declares Progress intent (task execution always does).

### 2.11 Visibility

`chasm/visibility.go`, `service/history/visibility_queue_task_executor.go`:

- `Visibility` is an ordinary component with `Data *ChasmVisibilityData` and two data fields
  (`SA`, `Memo`) for user-supplied search attributes and memo. An archetype opts in by declaring
  `Visibility chasm.Field[*chasm.Visibility]` on its root; it must be an immediate child of the
  root, and the archetype must set `WithBusinessIDAlias`.
- The root may implement `VisibilitySearchAttributesProvider` and `VisibilityMemoProvider` to
  compute archetype-defined attributes from state. At transaction close the framework compares
  the computed values with the previous ones and, if they or the lifecycle changed, generates a
  visibility task (`closeTransactionForceUpdateVisibility`).
- The visibility task carries a `TransitionCount`; its validator is
  `task.TransitionCount == component.Data.TransitionCount`. Every update bumps the counter, so
  older visibility tasks invalidate themselves. This is the canonical "generation counter"
  validator.
- The visibility queue executor validates the task, reads the component by path, merges custom
  and component-provided attributes, and upserts the visibility record.

---

## 3. Persistence model

### 3.1 Node tree

`proto/internal/temporal/server/api/persistence/v1/chasm.proto`:

```proto
message ChasmNode {
  ChasmNodeMetadata metadata = 1;
  temporal.api.common.v1.DataBlob data = 2;
}

message ChasmNodeMetadata {
  VersionedTransition initial_versioned_transition = 1;      // when this node was created
  VersionedTransition last_update_versioned_transition = 2;  // when it last changed
  oneof attributes {
    ChasmComponentAttributes  component_attributes  = 11;
    ChasmDataAttributes       data_attributes       = 12;
    ChasmCollectionAttributes collection_attributes = 13;
    ChasmPointerAttributes    pointer_attributes    = 14;
  }
}

message ChasmComponentAttributes {
  message Task {
    uint32 type_id = 1;
    string destination = 2;
    google.protobuf.Timestamp scheduled_time = 3;
    DataBlob data = 4;
    VersionedTransition versioned_transition = 5;   // with the offset: unique logical task ID
    int64 versioned_transition_offset = 6;
    int32 physical_task_status = 7;                 // cluster-local, not replicated
  }
  uint32 type_id = 1;
  repeated Task side_effect_tasks = 2;   // insertion order
  repeated Task pure_tasks = 3;          // ordered by scheduled time
  bool detached = 4;
  // plus per-request metadata, user metadata, principals
}
```

The execution's whole tree is a flat `map<string, ChasmNode>` keyed by **encoded path**.

### 3.2 Paths

`chasm/path_encoder.go`: path segments are joined with `$`; a direct child of a collection node
is introduced with `#`; `\`, `$`, and every code point at or below `#` are escaped with `\`; the
root path is the empty string. The encoding is designed so a subtree is a contiguous key range
(prefix scan).

### 3.3 Physical storage

- SQL (`schema/postgresql/v12/temporal/schema.sql`): table `chasm_node_maps` with primary key
  `(shard_id, namespace_id, workflow_id, run_id, chasm_path)` and separate `metadata` and `data`
  blob columns. One row per node.
- Cassandra (`schema/cassandra/temporal/schema.cql`): a `chasm_node_map map<text, blob>` column
  on the execution row, updated per key
  (`common/persistence/cassandra/mutable_state_store.go`, `templateUpdateChasmNodeQuery`,
  `templateDeleteChasmNodeQuery`).
- Current-run pointer for non-workflow archetypes: SQL table `current_chasm_executions` with
  primary key `(shard_id, namespace_id, business_id, archetype_id)`. The same BusinessID can
  therefore exist independently under different archetypes.

### 3.4 Dirty tracking and what a transaction writes

In memory (`chasm/tree.go`), each `Node` holds its last serialized form plus a lazily decoded
value and a `valueState`:

`NeedDeserialize < Synced < NeedSerialize < NeedSyncStructure`

- Any access through a `MutableContext` marks a data node `NeedSerialize` and a component node
  `NeedSyncStructure`, and marks the subtree lineage dirty (`subtreeIsDirty`, propagated to
  ancestors and descendants).
- The tree accumulates two mutation sets: `mutation` (user data: replicated, advances the
  logical clock) and `systemMutation` (cluster-local bookkeeping such as physical task status:
  persisted but not replicated and no clock bump).

`Node.CloseTransaction(ctx, isActive) (NodesMutation, error)` runs, in order:

1. `executeImmediatePureTasks`: run immediate pure tasks added during the transition, looping
   until none remain (syncing structure between rounds).
2. `syncSubComponents`: reflect over dirty component values to create, replace, or delete child
   nodes. A field set to empty deletes its subtree; a removed map key deletes that subtree. New
   nodes get `InitialVersionedTransition = next VT`. Deletions are recorded only for nodes that
   were previously persisted.
3. `resolveDeferredPointers`.
4. `closeTransactionHandleRootLifecycleChange` (section 2.10; skipped for workflows).
5. `closeTransactionForceUpdateVisibility`.
6. `closeTransactionSerializeNodes`: serialize nodes in `NeedSerialize`, stamping
   `LastUpdateVersionedTransition = next VT`. If the skip-persistence flag is on and the
   deterministic bytes are unchanged (and there are no new tasks), the stamp is reverted and the
   node is not written.
7. `closeTransactionUpdateComponentTasks` (section 4).
8. Merge `systemMutation` into the returned mutation and reset per-transaction state.

`MutableStateImpl.closeTransaction` (`service/history/workflow/mutable_state_impl.go`) then, if
user state is dirty, appends to the transition history, records tombstones for deleted nodes,
generates the replication task, and builds the persistence mutation. A single atomic write
contains (`common/persistence/persistence_interface.go`, `InternalWorkflowMutation`):

- `UpsertChasmNodes map[string]InternalChasmNode` (only nodes changed in this transaction),
- `DeleteChasmNodes map[string]struct{}`,
- the execution info and execution state blobs (transition history, tombstone batches, request
  IDs, status),
- `Tasks map[tasks.Category][]InternalHistoryTask` (the outbox rows),
- a `Condition` and `DBRecordVersion` for optimistic concurrency.

Creation writes a full snapshot (`ChasmNodes`). The exact conditional-write mechanics are the
same ones workflows use; I did not trace them for CHASM specifically **(inferred)**.

Loading: `NewTreeFromDB(serializedNodes, ...)` builds the node skeleton from the map without
decoding any values. An empty map means a legacy workflow with no CHASM state.

---

## 4. Task model and execution semantics

### 4.1 Two levels: logical and physical tasks

- A **logical task** is an entry in a component node's metadata (`side_effect_tasks` or
  `pure_tasks`). It is part of replicated state.
- A **physical task** is a row in a History queue (`service/history/tasks/chasm_task.go`):
  - `ChasmTask` for one side-effect task. Carries `ChasmTaskInfo{component_initial_VT,
    component_last_update_VT, path, type_id, data, archetype_id, task_VT, task_VT_offset}`.
  - `ChasmTaskPure{WorkflowKey, VisibilityTimestamp, ArchetypeID}`: a bare timer wake-up for the
    whole execution. It names no component and no task.
- Physical tasks are a cluster-local, regenerable projection of logical tasks. The only link is
  `physical_task_status` on the logical task, which is explicitly not replicated.

`ctx.AddTask(component, attrs, payload)` (`Node.AddTask`): immediate pure tasks go to an
in-memory list executed in the same transaction; everything else is buffered in `newTasks`.

`closeTransactionUpdateComponentTasks` at commit:

1. For every component whose lineage is dirty, **re-validate all existing logical tasks** and
   delete the invalid ones (`closeTransactionCleanupInvalidTasks`). Tasks are never explicitly
   completed or cancelled; they disappear when their validator returns false.
2. For nodes updated in this transaction, validate each new task and append it with
   `versioned_transition = next VT` and an incrementing offset; apply singleton mode; keep pure
   tasks sorted by scheduled time (`closeTransactionHandleNewTasks`).
3. For each side-effect logical task without a physical task, emit a `ChasmTask`. Queue choice
   (`taskCategory`): visibility task type goes to the visibility queue; non-empty `Destination`
   to the outbound queue; immediate to the transfer queue; otherwise the timer queue.
4. Find the earliest pure task in the *entire tree* and ensure exactly one `ChasmTaskPure` timer
   exists for it (`closeTransactionGeneratePhysicalPureTask`); later physical pure timers are
   dropped via `DeletePhysicalCHASMPureTasks`. The status flip is recorded in `systemMutation`.

### 4.2 Pure task execution

Active cluster (`service/history/timer_queue_active_task_executor.go`,
`executeChasmPureTimerTask`; `timer_queue_task_executor_base.go`, `executeChasmPureTimers`):

1. Take the execution lock and load mutable state.
2. `tree.EachPureTask(referenceTime, cb)`: for every component whose earliest pure task is due,
   run each due task through `Node.ExecutePureTask`: validate, then execute with a
   `MutableContext`; an invalid task is a successful no-op.
3. If at least one task was processed, commit **once**. All due pure tasks across the tree run in
   one transaction.

For a scheduled pure task, after a successful `Execute` the framework validates again and raises
`TaskNotInvalidatedError` (metric always; terminal error to the DLQ when enabled) if the task is
still valid: "A scheduled pure task must be invalidated by its own successful execution,
otherwise the logical task stays in the tree with no physical task left to drive it"
(`chasm/tree.go`, `ExecutePureTask`).

An immediate pure task "runs immediately in the same transaction as the scheduling Transition"
(`docs/architecture/chasm.md`; `executeImmediatePureTasks`).

### 4.3 Side-effect task execution

Active cluster (`timer_queue_active_task_executor.go`, `transfer_queue_active_task_executor.go`,
`outbound_queue_active_task_executor.go`; `service/history/chasm_task_util.go`):

1. Take the lock and load mutable state. This runs shard-ownership validation
   (`validateTaskByClock`) and task-generation validation (`ndc_task_util.go`).
2. Get the tree, then **release the lock** ("The task's executor must do its own locking as
   needed, and additional mutable state validations will run at access time").
3. `Node.ExecuteSideEffectTask` builds a `ComponentRef` from the task info
   (`executionLastUpdateVT = component_last_update_VT`, path, initial VT) and attaches a
   `validationFn` that performs a zombie check, the task-generation clock check, and the task's
   own `Validate`.
4. The handler's `Execute(ctx, ref, attrs, payload)` runs with Progress intent. Typical shape:
   `ReadComponent` to copy out inputs, do the external call with no lock held, then
   `UpdateComponent` to record the outcome.
5. Each re-entry goes through `getExecutionLease` (staleness check) and `Node.Component(ctx,
   ref)`: path lookup, initial-VT match, access rule, then `validationFn`. If the task is no
   longer valid the engine returns NotFound (`errTaskNotValid`).

So validation of a side-effect task happens *at access time, under the lock*, on every re-entry,
not once up front.

### 4.4 Delivery guarantees and error handling

- **At-least-once.** Physical tasks are retried until they succeed or are dropped. The docs: "If
  the Validator or Executor returns an error, the framework retries the task until it succeeds
  or the Validator discards it. Tasks are processed in FIFO order in general, but strict ordering
  is not guaranteed."
- Error classification in `service/history/queues/executable.go`: NotFound and
  `ErrStaleReference` mean "invalid task, drop"; `ErrTaskDiscarded` is dropped; errors that
  implement `MaybeTerminalTaskError` (for example `UnprocessableTaskError`,
  `TaskNotInvalidatedError`) are terminal and go to the DLQ; the rest are retried.
- Exactly-once *effects* are the application's job, achieved by the validator plus idempotent
  external calls. Handlers in `chasm/lib` are written to tolerate duplicate execution (for
  example the scheduler's `recordExecuteResult` counts only newly started workflows so "a
  concurrent ExecuteTask's duplicate StartWorkflow can't inflate ActionCount").
- A subtle point for side-effect tasks: the external call can succeed and the following
  `UpdateComponent` can fail. The task is then retried and the external call repeats. Nothing in
  the framework prevents this.

### 4.5 Standby (passive cluster) behavior

`service/history/timer_queue_standby_task_executor.go`:

- Tasks never *execute* on standby. They are verified.
- Side-effect: `Node.ValidateSideEffectTask` returns `(isTaskInTree, isValidByComponent)`. It
  checks the node exists, the initial VT matches, the logical task with the same
  `(VT, offset)` is still present, and then the component validator. If either is false the
  physical task is dropped. Otherwise it is retried until replication removes the logical task.
  After `ChasmStandbyTaskDiscardDelay` (default 24h, per task type) the handler's `Discard` runs.
- Pure: any due pure task still present returns `ErrTaskRetry`.

Note the asymmetry: the structural "is the logical task still in the tree" check is in
`ValidateSideEffectTask`, which is used on the standby and visibility paths. On the active path I
only found the component validator (via `makeValidationFn`). Correctness on the active side
therefore rests on the validator.

---

## 5. Consistency, versioning, and replication

### 5.1 VersionedTransition and transition history

```proto
message VersionedTransition {
  int64 namespace_failover_version = 1;
  int64 transition_count = 2;
}
```

- Ordering (`common/persistence/transitionhistory/transition_history.go`, `Compare`): by
  failover version, then by transition count.
- `WorkflowExecutionInfo.transition_history` is a run-length list. `[{v:1,t:3},{v:2,t:5}]` means
  transitions 1 to 3 happened at failover version 1 and 4 to 5 at version 2.
- It advances by exactly one count per committed transaction that changed replicated state
  (`UpdatedTransitionHistory` in `service/history/workflow/state_transition_history.go`), on the
  active cluster only. A transaction that changes only cluster-local state does not advance it.
- Every node carries `initial_versioned_transition` and `last_update_versioned_transition`.

`StalenessCheck(history, refVT)` has three outcomes:

| Outcome | Meaning | Example against `[{v:1,t:3},{v:3,t:6}]` |
| --- | --- | --- |
| `nil` | ref VT is contained in the history | `{v:3,t:4}`, `{v:1,t:3}` |
| `ErrStaleState` | local state is *behind* the ref; reload | `{v:3,t:7}`, `{v:4,t:4}` |
| `ErrStaleReference` | the ref points at a state that never existed on this lineage | `{v:3,t:3}`, `{v:2,t:4}`, `{v:1,t:4}` |

### 5.2 What a ref check actually guarantees

`ChasmEngine.getExecutionLease` + `Node.IsStale` + `Node.Component`:

1. Lock the execution. Run `StalenessCheck(transitionHistory, ref.executionLastUpdateVT)`.
2. `ErrStaleState`: the cached state may be old, so reload from the database and check again.
   Still stale returns `Unavailable("stale state, please retry")` (replication lag after a forced
   failover, or data loss).
3. `ErrStaleReference`: fail the request; the ref is from a discarded branch.
4. Resolve the node by path. For non-root refs, `componentInitialVT` must equal the node's
   `InitialVersionedTransition`, else NotFound. This protects against delete-and-recreate at the
   same path.
5. Apply the access rule, then the ref's `validationFn` if any.

Important nuance: the default check is **"my state is at least as new as the state the ref was
minted from, on the same lineage"**. It is a read-your-writes guard, *not* a compare-and-swap. A
ref minted at transition 5 is accepted when the execution is at transition 9. The docs' phrasing
("guards against a stale callback updating a node that has already moved past the expected
state") reads stronger than the code; rejecting out-of-date callers is done by task validators
and by application-level tokens (for example the scheduler's `ConflictToken`).

`RefConsistencyLevel` relaxes the token: `ComponentCreation` substitutes the component's initial
VT; `CurrentRun` drops both VTs and the RunID (workflow archetype only).

`chasm.ExecutionStateChanged(component, ctx, refBytes)` (`chasm/transition_history.go`) compares
a ref's VT to the current VT; the activity library uses a serialized ref as its long-poll token
(`chasm/lib/activity/handler.go`).

### 5.3 Concurrency control summary

- One writer at a time per execution: an in-process lock in the execution cache, on the shard
  owner.
- Shard ownership fencing plus a conditional database write protect against split brain
  **(inferred for the write path; the task-side clock check `validateTaskByClock` is verified)**.
- Persistence failures map to retryable errors (`convertError`: condition failed and shard
  ownership lost become Unavailable).
- Idempotency for external callers is by request ID (start and update).

### 5.4 State-based replication

CHASM executions *require* state-based replication ("state-based replication not enabled for
chasm execution" is an internal error in `closeTransactionPrepareReplicationTasks`).

Source (active) side:

- Each committed transaction that advanced the VT enqueues one `SyncVersionedTransitionTask`
  carrying the new VT.
- When streaming to a target, `SyncStateRetrieverImpl.getSyncStateResult`
  (`service/history/replication/sync_state_retriever.go`) decides between a **mutation** and a
  **snapshot**. A mutation is possible when the target's last known VT is on the same lineage and
  the source's tombstone batches still cover everything after it.
  - Mutation: `tree.Snapshot(exclusiveMinVT)` returns every node whose
    `last_update_versioned_transition` is greater than the target's VT, plus the tombstone
    batches newer than it, plus the execution info/state.
  - Snapshot: the full mutable state, including all nodes.
- Deleted nodes are tracked as `StateMachineTombstone{chasm_node_path}` in per-VT tombstone
  batches on the execution info (`closeTransactionTrackTombstones`), capped in number. That cap
  is why a snapshot fallback exists.
- Speculative in-memory changes are never replicated; the retriever reloads from the database if
  the cached tree has pending speculative state.

Target (standby) side (`service/history/ndc/workflow_state_replicator.go`,
`ReplicateVersionedTransition`):

- `StalenessCheck(localHistory, sourceLastVT)`: `nil` means duplicate (already applied);
  `ErrStaleState` means local is behind, so apply the mutation or snapshot; `ErrStaleReference`
  means divergent lineage (handled by a separate path).
- `applyMutation` first verifies the mutation is a strict extension of local state, then
  `MutableState.ApplyMutation`: apply tombstones (`Node.ApplyMutation` with `DeletedNodes`),
  overwrite execution info/state, then `Node.ApplyMutation` with `UpdatedNodes`.
- `Node.ApplyMutation` replaces a node when its last-update VT differs, resets decoded values,
  and calls `carryOverTaskStatus`, which matches logical tasks by `(VT, offset)` to preserve the
  *local* `physical_task_status`. Newly arrived logical tasks have no physical task, so the next
  `CloseTransaction` generates them locally. This is how each cluster gets its own queue rows
  without replicating queue rows.
- The standby closes the transaction with a passive policy: no VT bump, no replication tasks.

After failover, the new active cluster already has the logical tasks and their locally generated
physical tasks; the validators decide which are still relevant.

---

## 6. Worked example

### 6.1 Callback: a small reusable component with a state machine

Files: `chasm/lib/callback/component.go`, `statemachine.go`, `tasks.go`, `library.go`.

**Component.** One proto state message plus a typed parent pointer:

```go
type Callback struct {
    chasm.UnimplementedComponent
    *callbackspb.CallbackState                       // the single proto field = persisted state
    CompletionSource chasm.ParentPtr[CompletionSource]
}

func (c *Callback) LifecycleState(_ chasm.Context) chasm.LifecycleState {
    switch c.Status {
    case CALLBACK_STATUS_SUCCEEDED: return chasm.LifecycleStateCompleted
    case CALLBACK_STATUS_FAILED:    return chasm.LifecycleStateFailed
    default:                        return chasm.LifecycleStateRunning
    }
}
func (c *Callback) StateMachineState() callbackspb.CallbackStatus     { return c.Status }
func (c *Callback) SetStateMachineState(s callbackspb.CallbackStatus) { c.Status = s }
```

**Transitions.** `STANDBY -> SCHEDULED -> {SUCCEEDED | FAILED | BACKING_OFF -> SCHEDULED}`:

```go
var TransitionScheduled = chasm.NewTransition(
    []callbackspb.CallbackStatus{CALLBACK_STATUS_STANDBY},
    CALLBACK_STATUS_SCHEDULED,
    func(cb *Callback, ctx chasm.MutableContext, _ EventScheduled) error {
        destination, err := callbackDestination(cb.GetCallback())
        if err != nil { return err }
        // side-effect task; Destination routes it to the outbound queue
        ctx.AddTask(cb, chasm.TaskAttributes{Destination: destination}, &callbackspb.InvocationTask{})
        return nil
    },
)

var TransitionAttemptFailed = chasm.NewTransition(
    []callbackspb.CallbackStatus{CALLBACK_STATUS_SCHEDULED},
    CALLBACK_STATUS_BACKING_OFF,
    func(cb *Callback, ctx chasm.MutableContext, e EventAttemptFailed) error {
        cb.recordAttempt(e.Time)                                  // Attempt++
        next := e.Time.Add(e.RetryPolicy.ComputeNextDelay(0, int(cb.Attempt), e.Err))
        cb.NextAttemptScheduleTime = timestamppb.New(next)
        // pure timer task
        ctx.AddTask(cb, chasm.TaskAttributes{ScheduledTime: next}, &callbackspb.BackoffTask{Attempt: cb.Attempt})
        return nil
    },
)
```

**Side-effect task** (does the network call):

```go
func (h *invocationTaskHandler) Validate(_ chasm.Context, cb *Callback, _ chasm.TaskInvocation, task *callbackspb.InvocationTask) (bool, error) {
    return cb.Attempt == task.Attempt && cb.Status == CALLBACK_STATUS_SCHEDULED, nil
}

func (h *invocationTaskHandler) Execute(ctx context.Context, ref chasm.ComponentRef, attrs chasm.TaskAttributes, task *callbackspb.InvocationTask) error {
    invokable, err := chasm.ReadComponent(ctx, ref, (*Callback).loadInvocationArgs, nil)   // lock, copy out, unlock
    if err != nil { return err }
    result := invokable.Invoke(callCtx, ns, h, task, attrs)                                // I/O with no lock held
    _, _, saveErr := chasm.UpdateComponent(ctx, ref, (*Callback).saveResult, saveResultInput{result: result, ...})
    return invokable.WrapError(result, saveErr)
}
```

`saveResult` applies `TransitionSucceeded`, `TransitionFailed`, or `TransitionAttemptFailed`.
Each changes `Status` or `Attempt`, so the invocation task's validator becomes false and the
logical task is removed at that commit.

**Pure task** (the backoff timer):

```go
func (h *backoffTaskHandler) Validate(_ chasm.Context, cb *Callback, _ chasm.TaskInvocation, task *callbackspb.BackoffTask) (bool, error) {
    return cb.Status == CALLBACK_STATUS_BACKING_OFF && cb.Attempt == task.Attempt, nil
}
func (h *backoffTaskHandler) Execute(ctx chasm.MutableContext, cb *Callback, _ chasm.TaskAttributes, _ *callbackspb.BackoffTask) error {
    return TransitionRescheduled.Apply(cb, ctx, EventRescheduled{})   // -> SCHEDULED, adds a new InvocationTask
}
```

The pattern to copy: **the task payload captures the generation it was created for (`Attempt`),
and the validator compares it to current state.** A retry loop needs no cancellation logic.

### 6.2 Scheduler: a full archetype with gRPC handlers

Files: `chasm/lib/scheduler/library.go`, `fx.go`, `handler.go`, `scheduler.go`, `generator.go`,
`generator_tasks.go`, `invoker_tasks.go`, `proto/v1/service.proto`.

**Tree shape:**

```go
type Scheduler struct {
    chasm.UnimplementedComponent
    *schedulerpb.SchedulerState                                    // root's own state

    LastCompletionResult chasm.Field[*schedulerpb.LastCompletionResult] // separate data node "to minimize write traffic"
    Generator   chasm.Field[*Generator]
    Invoker     chasm.Field[*Invoker]
    Backfillers chasm.Map[string, *Backfiller]
    EventLog    chasm.Field[*EventLog]
    Visibility  chasm.Field[*chasm.Visibility]

    cacheConflictToken int64                    // transient, not persisted
    compiledSpec       *scheduler.CompiledSpec  // transient cache
}
```

`Generator` and `Invoker` each hold `Scheduler chasm.ParentPtr[*Scheduler]`. Because a transition
"operates on the entire Execution", a `Generator` pure task freely reads and writes the parent
and the sibling `Invoker` in the same transaction (`GeneratorTaskHandler.Execute`).

**Registration** (`library.go`): the root is registered with
`chasm.NewRegistrableComponent[*Scheduler](chasm.SchedulerComponentName,
chasm.WithBusinessIDAlias("ScheduleId"), chasm.WithSearchAttributes(...),
chasm.WithContextValues(...))`; children with plain names (`"generator"`, `"invoker"`,
`"backfiller"`, `"eventlog"`); four pure tasks (`idle`, `generate`, `processBuffer`, `backfill`)
and three side-effect tasks (`callbacks`, `execute`, `migrateToWorkflow`).
`RegisterServices` registers the library's gRPC service. `fx.go` wires handlers with dependency
injection and calls `registry.Register(library)`.

**Routing** (`proto/v1/service.proto`): each RPC declares
`option (temporal.server.api.routing.v1.routing).business_id = "frontend_request.schedule_id";`
so the generated client can route to the owning shard.

**Handlers** (`handler.go`) are thin adapters from RPC to engine call:

```go
// create
_, err = chasm.StartExecution(ctx,
    chasm.ExecutionKey{NamespaceID: req.NamespaceId, BusinessID: req.FrontendRequest.ScheduleId},
    CreateScheduler, req, chasm.WithRequestID(req.FrontendRequest.RequestId))

// mutate
resp, _, err = chasm.UpdateComponent(ctx,
    chasm.NewComponentRef[*Scheduler](chasm.ExecutionKey{NamespaceID: ..., BusinessID: ...}),
    (*Scheduler).Update, req, chasm.WithRequestID(req.FrontendRequest.RequestId))
if errors.Is(err, chasm.ErrRequestIDAlreadyUsed) { /* retry of a request that already succeeded */ }

// read
resp, err = chasm.ReadComponent(ctx, chasm.NewComponentRef[*Scheduler](key),
    func(s *Scheduler, ctx chasm.Context, req *...) (*..., error) { return s.Describe(ctx, req, h.specBuilder) }, req)
```

The activity library shows the other two calls (`chasm/lib/activity/handler.go`):
`chasm.UpdateWithStartExecution(ctx, key, startFn, updateFn, req, append(startOpts,
chasm.WithAssumeNew())...)` for "start, or attach callbacks to the existing one, in one
transaction", and `chasm.PollComponent(ctx, ref, predicate, req)` for long polls where the
predicate uses `chasm.ExecutionStateChanged(a, ctx, token)`.

**Timer loop** (`generator.go`, `generator_tasks.go`): the generator arms a pure task at the next
wake-up time. `Execute` processes the time range `(LastProcessedTime, now]`, buffers starts on
the Invoker, advances the high-water mark, and arms the next task. `Validate` is
`scheduledTime > max(LastProcessedTime, LastProcessedTaskTime)`, so advancing the high-water mark
invalidates the task that just ran and any older duplicates. The code comment explains why a
second watermark exists: "An obsolete timer can execute without processing an action and must
still invalidate itself."

**Side-effect loop** (`invoker_tasks.go`, `InvokerExecuteTaskHandler`): `Validate` is "is there
still work buffered"; `Execute` does `ReadComponent` (deep-copying protos because they are used
"outside of the MS lock" and capturing `ctx.Ref(scheduler)` as a completion-callback return
address), starts/cancels/terminates workflows, then `UpdateComponent` to record results.

---

## 7. Principles to carry into a Rust design

1. **The unit of consistency is the execution, and a transition is a closure.** One execution =
   one single-writer aggregate keyed by `(namespace, business_id, run_id)`. The engine's whole
   write API is "load, lock, run this closure against typed state, commit atomically". Keep the
   surface that small: `start`, `update`, `read`, `poll`, `update_with_start`, `delete`.

2. **State-based, not event-sourced.** Persist current state; do not persist or replay a history.
   This removes determinism constraints from user code (only "no I/O inside a transition"
   remains) and makes storage proportional to state size, not activity.

3. **A tree of components, one row per node, addressed by an encoded path.** Let authors split
   state into fields by write frequency, size, and read pattern. Write only dirty nodes. Choose a
   path encoding where a subtree is a key range.

4. **Dirty tracking should be explicit and cheap.** CHASM marks a node dirty on any mutable
   access and then compares serialized bytes to skip no-op writes. In Rust, `get_mut()` (or a
   guard with `DerefMut`) is the natural dirty hook and the borrow checker enforces it. Keep the
   byte-compare skip as a safety net; it requires deterministic serialization.

5. **Transactional outbox, always.** State changes and the tasks they imply commit in one write.
   No task exists without the state that justifies it, and vice versa.

6. **Separate logical tasks from physical queue rows.** Logical tasks live in component metadata
   and are part of (replicated) state. Queue rows are a local, regenerable index over them, with
   a local "physical row created" flag. This gives you task refresh, replication, and failover
   without replicating queues. For timers, keep at most one wake-up row per execution (earliest
   deadline) and run every due timer in one transaction.

7. **Tasks are never cancelled or acknowledged; they are validated.** Every task type must
   provide `validate(state, task) -> bool`. Re-validate existing tasks whenever the owning
   lineage changes and drop the invalid ones at commit. Put a generation marker (attempt number,
   counter, high-water mark) in the task payload and compare it to state. Enforce the invariant
   CHASM learned the hard way: **a scheduled task that executes successfully must make its own
   validator false**; detect violations and dead-letter them.

8. **Encode pure vs side-effect in the type system.** Pure handler:
   `fn(&mut C, &mut MutableContext, &T) -> Result<()>`, synchronous, no I/O handle in scope, runs
   in-transaction under the lock. Side-effect handler:
   `async fn(&ComponentRef, &T) -> Result<()>`, never receives `&mut C`, runs outside the lock,
   and re-enters through the same engine API as external callers. Rust can make the "no I/O in a
   transition" rule a compile-time property; Go cannot.

9. **Side-effect shape is read, act, write.** Copy out what you need under the lock, do the
   external call unlocked, then write the outcome in a new transition that re-validates. Design
   for at-least-once: the external call must be idempotent (request IDs), and the write-back must
   tolerate duplicates.

10. **Give every transition a logical clock value and stamp it on nodes.** Keep
    `(epoch, count)` even for a single cluster: `count` increments once per committed
    state-changing transaction; `epoch` is reserved for ownership changes. Stamp `created_at` and
    `updated_at` VTs on every node. This one mechanism buys refs, staleness detection, delta
    replication, and cheap "has anything changed since token X" long polls.

11. **Refs are capability tokens with two checks.** `(execution key, archetype id, path,
    node-created VT, minted-at VT)`. Node-created VT rejects delete-and-recreate aliasing.
    Minted-at VT means "the state I act on must be at least this new" (reload if not, reject if
    on another lineage). Do not mistake it for optimistic concurrency; provide that separately
    (application conflict tokens or an explicit `expect_version` option).

12. **Stable numeric IDs derived from names; names are schema.** Type IDs are hashes of fully
    qualified names, collision-checked at startup. Field names appear in persisted paths.
    Renaming a component, task, or field is a data migration. Prefer a derive macro that requires
    explicit, stable names over inferring them from identifiers.

13. **Lifecycle is derived from state, and closing is hierarchical.** Each component reports
    open/closed from its own state. Root closed means execution closed (ID reusable, retention
    starts). Closed ancestors block progress on descendants unless the child is explicitly
    detached. Distinguish progress intent from observe intent.

14. **Idempotent entry points.** Start is idempotent by request ID; updates can be. ID reuse and
    conflict policies are engine concerns, not application concerns.

15. **Time comes from the context.** A fixed `now` per transition keeps transitions
    deterministic with respect to time and enables pause and time skipping later.

16. **Cross-cutting features are components.** Visibility is just a detached child component with
    a generation-counter task; request-ID dedup is a reserved data node. Build framework features
    out of the framework's own primitives.

17. **Long polls are execution-level notifications plus a monotonic predicate** re-evaluated
    under the lock. Subscribe before releasing the lock.

18. **Replicate state, not operations.** Because every node has an updated-at VT, a delta is "all
    nodes with VT greater than X plus tombstones newer than X"; fall back to a full snapshot when
    tombstones have been trimmed or lineages diverged. Keep cluster-local fields structurally
    separate so they never leak into replicated bytes.

Things I would deliberately *not* copy:

- Runtime reflection over structs and the exported-`Internal` field workaround. Use a derive
  macro and a trait.
- "Getting with a mutable context means dirty". Use `&` vs `&mut`.
- `Field.Get` panicking on framework errors. Return `Result`.
- Hosting the tree inside a legacy workflow record (`MSPointer`, workflow columns named
  `workflow_id`, dual request-ID stores). That is migration baggage.
- Access-rule enforcement that depends on the caller remembering to declare intent (see the
  scheduler's manual `Closed` check). Make progress intent the default for writes.
- Speculative in-memory transitions, until there is a measured need.

---

## 8. Verification notes

**Read in full or in the relevant parts** (all claims above trace to these):
`docs/architecture/chasm.md`; `chasm/AGENTS.md`; `chasm/component.go`, `context.go`,
`engine.go`, `field.go`, `map.go`, `parent_pointer.go`, `fields_iterator.go`, `ref.go`,
`library.go`, `library_core.go`, `registry.go`, `registrable_component.go`,
`registrable_task.go`, `task.go`, `task_handler_base.go`, `statemachine.go`, `errors.go`,
`path_encoder.go`, `constants.go`, `transition_history.go`, `request_ids.go`, `interceptors.go`,
`visibility.go` (struct, task handler, provider interfaces), `tree.go` (lines 1 to 4400);
`proto/.../persistence/v1/chasm.proto`; `service/history/chasm_engine.go`,
`chasm_task_util.go`, `chasm_notifier.go`, `tasks/chasm_task.go`;
`common/persistence/transitionhistory/transition_history.go`; chasm-related sections of
`service/history/workflow/mutable_state_impl.go`, `timer_queue_active_task_executor.go`,
`timer_queue_standby_task_executor.go`, `timer_queue_task_executor_base.go`,
`outbound_queue_active_task_executor.go`, `ndc_task_util.go`, `queues/executable.go`,
`visibility_queue_task_executor.go`, `replication/sync_state_retriever.go`,
`replication/raw_task_converter.go`, `ndc/workflow_state_replicator.go`
(`ReplicateVersionedTransition`, `applyMutation`, `applySnapshot`); SQL and Cassandra schema
files; `chasm/lib/scheduler` (library, fx, handler, root component, generator, invoker tasks),
`chasm/lib/callback` (component, statemachine, tasks), and parts of
`chasm/lib/activity/handler.go`.

**Not read or not verified:**

- No code was executed; no tests were run. Behavior is as written in source, not as observed.
- The conditional-write and shard-fencing path for CHASM writes was not traced end to end; I
  assume it is the workflow path (section 5.3).
- Transfer-queue and outbound-queue **standby** executors were not read; I assume they mirror the
  timer standby executor.
- DLQ mechanics, retry backoff, and the rule that turns repeated failures into DLQ writes were
  only skimmed (`queues/executable.go`).
- `executable_sync_versioned_transition_task.go`, `executable_verify_versioned_transition_task.go`,
  the divergent-lineage path in the state replicator, `workflow/task_refresher.go`, and
  `applySnapshotWhenWorkflowExist` were not read.
- The speculative-transition path was read at the level of docs, option comments, and the tree's
  decision functions, not the full engine path.
- Time skipping (`tree.go` after line 4400 and related context methods), `chasm/search_attribute.go`
  internals, `chasm/visibility_manager.go`, `chasm/detached_tree.go` (known only via
  `chasm/AGENTS.md`), Nexus integration files, and the `workflow`, `nexusoperation`, `worker`,
  `flowcontrol`, and `namespacereplication` libraries were not read.
- How the frontend client resolves the `routing.business_id` proto option to a shard was not
  read; only the option and the frontend handlers calling a generated client were seen.
- Several behaviors are behind dynamic config flags at this commit (`EnableChasm`,
  `EnableCHASMSkipPersistence`, `EnableChasmRequestIDDataNode`,
  `ChasmDLQScheduledPureTaskOnValidation`, and others). I describe the code paths, not which are
  enabled by default.
- CHASM is under active development; comments contain many TODOs. Details here are a snapshot of
  one commit.

**Public material.** A web search found little beyond mirrors of the in-repo architecture
document (which describe CHASM as an internal framework, not a user-facing API). I did not find
or rely on a public design document; the source is the authority for everything above.
