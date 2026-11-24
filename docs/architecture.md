# Architecture

The rule everything else follows from: **the core is deterministic and does
no I/O**. Replicas, gateways and SDK worker cores are state machines driven
by packets and ticks. They read no clock, spawn no threads and draw no
entropy. The production runtime gives them files, sockets and a clock; the
simulator gives them simulated ones and a seed. It is the same code either
way, which is what makes the simulation results mean something.

`wfe-rules` enforces the boundary: crates that run inside a replica cannot
depend on tokio, a random number generator, or any of the I/O crates, and
the root `clippy.toml` bans wall clocks and hash maps with random iteration
order in them.

```
 clients, workers (wfe-sdk; payload codecs run here)
        │  HTTP: POST /api
        ▼
 ┌──────────────────────────── cell ────────────────────────────┐
 │  gateway (one per node): routes by hash, parks long polls    │
 │        │                                                     │
 │  shard 0        shard 1        ...        shard N-1          │
 │  ┌─────────┐    ┌─────────┐                                  │
 │  │ VSR     │    │ VSR     │   each shard: `replicas` copies, │
 │  │ engine  │    │ engine  │   one per node                   │
 │  │ LSM     │    │ LSM     │                                  │
 │  └─────────┘    └─────────┘                                  │
 └──────────────────────────────────────────────────────────────┘
        │  changed executions, in chunks
        ▼
   another cell (standby)
```

## Storage: `wfe-lsm`

A log-structured merge tree over a grid of fixed-size, checksummed blocks in
one data file per replica. Its defining property is **physical
determinism**: the same sequence of operations produces byte-identical
blocks at identical addresses on every replica. So:

- one checksum (of the manifest, which covers every table, which covers
  every block) names a whole checkpoint;
- a corrupt block is repaired by asking any peer for `(address, checksum)`;
- a replica that has fallen too far behind adopts a peer's checkpoint block
  by block instead of replaying the log.

Compaction is paced by the log, not by a background thread, so it is part of
the deterministic state too.

## Consensus: `wfe-vsr`

Each shard is one Viewstamped Replication group, following the protocol as
TigerBeetle refined it:

- a hash-chained log of 128-byte headers; a write-ahead log with a redundant
  header ring, so recovery can tell a torn append from a lost prepare;
- view changes that never truncate an uncommitted operation without a quorum
  of nacks;
- repair by checksum from any peer, checkpoints, and state sync.

The log supplies the clock: every operation carries a timestamp chosen by
the primary, and state machines only ever see that time.

## The engine: `wfe-engine`

The state machine each shard replicates. It is built on a small execution
framework after Temporal's CHASM (`chasm.rs`):

- an **execution** is a tree of nodes addressed by path (`h/…` history
  events, `a/…` activities, `t/…` timers, `w` workflow state), owned by an
  **archetype**. There are two: `workflow` and `deployment`;
- everything that happens is a **transition**: open, read and write typed
  nodes, commit atomically;
- **logical tasks** are part of the execution's state. They are never
  cancelled, only validated: a task that no longer applies is dropped when
  it is looked at;
- **physical rows** (timer wake-ups, task-queue entries, the outbox) are a
  local index derived from the tasks. They are regenerated, never
  replicated;
- every commit stamps the nodes it wrote with a logical time and moves the
  execution in the shard's **change index**, which is what replication
  reads.

Side effects (telling other shards about a routing change, counting pinned
workflows, shipping executions to another cell) are done by the primary
outside the log and written back as ordinary operations, so they can be
repeated safely. Every operation is idempotent because the transport
retries.

### Workflows

Event-history workflows driven by workers, as in Temporal: a worker polls
for a workflow task, replays the history through the workflow function, and
answers with commands (schedule an activity, start a timer, complete).
Activities have retries with backoff, start-to-close and
schedule-to-close timeouts and heartbeats. A workflow takes one `Payload`
and returns one.

### Worker versioning

A **deployment** names a set of worker builds. A worker polls as
`(deployment, build id)`, and a workflow declares a behavior:

- **pinned**: stays on the build it started on until it finishes;
- **auto-upgrade**: moves to the deployment's current build at its next
  workflow task.

The deployment's routing has a **current** build and optionally a
**ramping** build that takes a percentage of new workflows. A build that is
neither is **draining** while open workflows are still pinned to it, then
**drained**. The deployment lives on one shard; its routing reaches the
others asynchronously, and `propagated_shards` says how far it has got.

Deciding what to do next (ramp further, make a build current, scale an old
build to zero, delete it) is `wfe-control`: planners that take what was
asked for and what a cell reports, and return the next actions. They keep
nothing between calls and do no I/O, so the operator, the CLI and the
simulator all run the same ones.

### Cell replication

A link from cell A to cell B makes every shard primary in A ship changed
executions to B. A transfer is the set of nodes changed since the last
acknowledged one, cut into chunks addressed by node path; the root node goes
last, so a partly received execution is never visible. A node is only taken
if its logical time is newer than what is there, which makes delivery
idempotent and lets replication run in both directions after a failover.

A cell is **active** or **standby**, with an epoch. A standby refuses writes
(`not_active`) and does not run timers or hand out tasks. An orderly
failover demotes the active cell, waits for the standby to catch up, and
promotes it at a higher epoch; a forced one skips the wait.

Deployments replicate like workflows, so a standby knows the routing and
which builds still have workflows pinned to them. Sending the routing to
the shards and counting pinned workflows are done by the active cell only,
and start again on the cell that is promoted.

## Gateway, server, SDK

- **`wfe-gateway`** turns each API request into requests on the right
  shards. Long polls wait at the gateway, which probes shard primaries and
  only submits the poll that takes a task once there is one.
- **`wfe-server`** runs one replica of every shard plus a gateway on a
  single core thread, with length-prefixed MessagePack over TCP between
  nodes and JSON over HTTP for clients (`POST /api`, `GET /status`,
  `GET /healthz`). Cells learn each other's addresses from the frames they
  exchange.
- **`wfe-sdk`** has the same split: a deterministic worker core that the
  simulator drives directly, and a thin tokio shell around it.
- **`wfe-codec`** runs in the SDK. The server never looks inside a payload.

## Scaling

Workflows are placed on shards by hash of their id; shards are independent
consensus groups, so throughput grows with the shard count. Nodes each hold
one replica of every shard. A cell's shard count is fixed when it is
created; to grow past it, add cells.
