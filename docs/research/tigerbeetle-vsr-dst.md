# TigerBeetle: VSR, storage, and deterministic simulation testing

Research notes for building a consensus-replicated deterministic state machine in Rust, with a
VOPR-style simulator and an always-on fuzzing fleet. Everything here was read from source; nothing
was built or run.

## 0. Provenance and how to read this

| What | Revision | Used for |
|---|---|---|
| `tigerbeetle/tigerbeetle` `main` HEAD | `a2a3c7064519e08233abe26c54a43fca2f9ad9bc` (2026-11-10, 0.18.0 era) | Sections 1 to 8, parts of 9 |
| tag `0.17.10` | `33fa353100aaeb20cce77e3e7c52e774a3b33c27` (2026-10-11) | Section 9 (CFO, devhub UI) |

Things that differ from what older docs, talks and blog posts say:

- **Message names changed at HEAD.** `docs/internals/vsr.md`: the code "uses `JoinView`/`View`/
  `ExitView` instead of `DoViewChange`/`StartView`/`StartViewChange`, to put the state (`View`)
  into the spotlight". This document uses the HEAD names. Mapping:

  | VRR paper / older TigerBeetle | HEAD command | Short |
  |---|---|---|
  | `start_view_change` (SVC) | `exit_view` | EV |
  | `do_view_change` (DVC) | `join_view` | JV |
  | `start_view` (SV) | `view` | View |
  | `request_start_view` (RSV) | `get_view` | |
  | `request_headers` / `request_prepare` / `request_reply` / `request_blocks` | `get_headers` / `get_prepare` / `get_reply` / `get_blocks` | |
  | `nack_prepare` | removed; nacks ride in the JV as a bitset | |

- **The CFO is gone from the public tree at HEAD.** `src/scripts/cfo.zig`,
  `src/scripts/cfo_supervisor.sh` and `src/devhub/` exist at 0.17.10 but not at HEAD (only
  `src/scripts/devhub.zig` remains). Section 9 is therefore read from the 0.17.10 tag.
- **File moves.** The simulator entry point is `src/vopr.zig` (there is no `src/simulator.zig`).
  The fuzz helper library is `src/stdx/testing/fuzz.zig`, the PRNG is `src/stdx/prng.zig`, and
  the workload and auditor are `src/state_machine/workload.zig` and
  `src/state_machine/auditor.zig`.

Conventions: paths are relative to the repo root; `fn_name` refers to a function in the file named
just before it. "(inferred)" marks a conclusion I drew rather than read. Line numbers are for the
HEAD revision above and will drift.

Production constants used throughout (`src/config.zig`, `ConfigCluster` defaults; the simulator
uses the much smaller `test_min` values in parentheses):

| Constant | Default | `test_min` |
|---|---|---|
| `journal_slot_count` | 1024 | 32 (`journal_slot_count_min` = 2 × headers per sector) |
| `pipeline_prepare_queue_max` | 8 | 4 |
| `lsm_compaction_ops` (one "bar") | 32 | 4 |
| `vsr_checkpoint_ops` (derived) | 976 | 20 |
| `message_size_max` | 1 MiB | minimal |
| `block_size` | 512 KiB | 4 KiB (one sector) |
| `replicas_max` / `standbys_max` | 6 / 6 | same |
| `clients_max` | 64 | 7 |
| header size | 256 bytes | same |

## 1. Replica architecture and message set

### 1.1 Shape of a replica

`src/vsr/replica.zig` (about 12,500 lines) defines `ReplicaType(StateMachine, MessageBus, Storage,
AOF)`. The replica is generic over its IO so the same code runs in production and in the
simulator. It owns, as plain struct fields (no heap allocation after init):

- `superblock` (`src/vsr/superblock.zig`): the durable root. Holds the VSR state (`view`,
  `log_view`, `commit_max`, checkpoint state, sync state) and the `view_headers`.
- `journal` (`src/vsr/journal.zig`): the WAL, two rings (redundant headers and prepares).
- `grid` (`src/vsr/grid.zig`) with its `free_set`: the block store under the LSM forest.
- `client_sessions` (`src/vsr/client_sessions.zig`) and `client_replies`
  (`src/vsr/client_replies.zig`): the VR client table and one durable reply slot per client.
- `state_machine`: the application; it owns the LSM forest.
- `clock` (`src/vsr/clock.zig`): Marzullo-based cluster time.
- `message_bus` and a `MessagePool` of reference-counted, fixed-size message buffers.
- `pipeline`: a tagged union. On a primary in normal status it is a `PipelineQueue` (a prepare
  queue of `pipeline_prepare_queue_max` plus a request queue of `pipeline_request_queue_max`).
  Everywhere else it is a `PipelineCache` (recent uncommitted prepares kept in memory to avoid WAL
  reads and to survive WAL corruption).
- repair and sync machinery: `journal_repair_message_budget`, `grid_repair_*`, `grid_scrubber`,
  `syncing` stage, `sync_tables`.

Primary selection is round-robin: `primary_index(view) = view % replica_count`. Standbys are
nodes with index `>= replica_count`: they receive prepares and commits, journal and execute them,
but never ack, vote, or serve repair (`ignore_repair_message`, `send_prepare_ok`).

Everything is single-threaded and callback driven. IO completions call back into the replica; the
replica never blocks. `tick()` is the only notion of time: it advances `clock`, `message_bus` and
`multiversion`, ticks every `Timeout`, then runs the handlers of fired timeouts in a fixed order.
A message a replica sends to itself goes into a one-slot `loopback_queue` that the sending code
path must flush before returning (`flush_loopback_queue`); `tick` asserts it is empty. This keeps
self-delivery synchronous and deterministic without reentrancy surprises.

Static allocation: `open()` calls `static_allocator.transition_from_init_to_static()` once setup
is done. Any later allocation is a bug that trips an assertion.

### 1.2 Status

`Status` (`replica.zig`):

| Status | Meaning |
|---|---|
| `recovering` | Start state while the superblock and journal are opened. A solo replica stays here until it has committed its whole journal. |
| `normal` | In a view with `view == log_view`; primary prepares, backups accept prepares. |
| `view_change` | `view > log_view` (or a new primary that has its JV quorum and is repairing). No prepares accepted, no prepare_oks sent. |
| `recovering_head` | Storage faults make the head op uncertain. The replica cannot vote (it cannot produce an honest JV) and cannot nack. It waits for a `view` message from a primary. It can still serve `get_prepare`/`get_headers`/`get_reply`. |

`view` and `log_view` are the two view numbers every design needs:

- `view`: highest view this replica has agreed to take part in.
- `log_view`: "the latest view in which this replica changed its head message", that is, the
  view where it was primary and got a JV quorum, or was a backup and processed a `view` message.
- Invariants: `view >= log_view`; `status == normal` implies `view == log_view`; both in-memory
  values are `>=` their durable superblock copies.

### 1.3 Message set

All messages share a 256-byte header (`src/vsr/message_header.zig`): `checksum` (u128,
AEGIS-128L MAC over the rest of the header), `checksum_body`, `cluster` (u128), `size`, `epoch`,
`view`, `release`, `protocol`, `command`, `replica`, then 128 bytes that depend on the command.
Bodies are padded with zeros to a sector so a `request` buffer can be turned into a `prepare` and
written to the WAL without copying.

| Command | Direction | Purpose |
|---|---|---|
| `ping` / `pong` | replica to replica | clock samples, release lists (upgrade), route and liveness info. Use the durable view. |
| `ping_client` / `pong_client` | client and replica | client learns the view |
| `request` | client to primary (backups forward) | carries `client`, `session`, `request` number, `parent` (checksum of the previous reply) |
| `prepare` | primary to all replicas and standbys | the log entry |
| `prepare_ok` | backup to current primary | durable ack, carries `checkpoint_id` and `commit_min` |
| `reply` | replica to client | result, `context` = reply checksum chain |
| `commit` | primary to all | heartbeat and commit number, `commit_checksum`, `checkpoint_op`, `checkpoint_id`, `timestamp_monotonic` |
| `exit_view` | replica to all | "I think the primary of `view` is dead" |
| `join_view` | replica to primary of new view | vote plus log suffix headers, `log_view`, nack and present bitsets |
| `view` | primary to all | checkpoint state plus log suffix headers; installs a view |
| `get_view` | replica to primary | ask for a `view` (carries a `nonce`) |
| `get_headers` / `headers` | any to any | header repair |
| `get_prepare` (answered with `prepare`) | any to any | prepare repair |
| `get_reply` (answered with `reply`) | any to any | client reply repair |
| `get_blocks` / `block` | any to any | grid repair and state sync |
| `eviction` | replica to client | session gone or client incompatible |

The `Prepare` header carries: `parent` (checksum of the previous prepare: the hash chain),
`request_checksum`, `checkpoint_id`, `client`, `op`, `commit`, `timestamp`, `request`,
`operation`, and the `view` and `release` in the common part.

Not implemented: the VRR recovery protocol (section 4.3 of the paper). "Instead, replicas persist
their VSR state to the superblock" (`docs/internals/vsr.md`). Reconfiguration exists only as a
stub operation.

### 1.4 Quorums

`vsr.quorums(replica_count)` in `src/vsr.zig` (Flexible Paxos; `quorum_replication_max = 3`):

| replica_count | 1 | 2 | 3 | 4 | 5 | 6 |
|---|---|---|---|---|---|---|
| replication | 1 | 2 | 2 | 2 | 3 | 3 |
| view_change | 1 | 2 | 2 | 3 | 3 | 4 |
| nack_prepare | 1 | 1 | 2 | 3 | 3 | 4 |
| upgrade | 1 | 2 | 3 | 4 | 5 | 6 |

Formulas: `replication = min(quorum_replication_max, ceil(n/2))` except `n == 2` gives 2;
`view_change = n - replication + 1` except `n == 2` gives 2;
`nack_prepare = n - replication + 1`. The asserted properties are the ones to copy:
`view_change + replication > n` (every view change sees every possibly committed op) and
`nack_prepare + replication > n` (that many nacks prove the op never reached a replication
quorum).

### 1.5 Timeouts

Set in `init` (milliseconds, converted to ticks with `tick_ms = 10`):

| Timeout | Value | Role |
|---|---|---|
| `ping_timeout` | 1000 | ping all replicas |
| `prepare_timeout` | 250, RTT adjusted, exponential backoff with jitter | primary resends the oldest unacked prepare to replicas that have not acked |
| `primary_abdicate_timeout` | 10000 | primary stops heartbeating if it cannot get acks |
| `commit_message_timeout` | 500 | primary heartbeat (`commit`) |
| `commit_fault` (`FaultDetector`) | 100 to 2000 | backup-side heartbeat health: green / yellow / red |
| `exit_view_window_timeout` | 5000 | window in which EVs are collected |
| `exit_view_message_timeout` | 500 | resend own EV |
| `view_change_status_timeout` | 5000 | stuck in view_change: send EV again (move to the next view) |
| `join_view_message_timeout` | 500 | resend JV |
| `get_view_message_timeout` | 1000 | backup in view_change asks the primary for a `view` |
| `journal_repair_timeout` | 100 with jitter | run `repair()` |
| `repair_sync_timeout` | 5000 | decide whether WAL repair is stuck and state sync is needed |
| `grid_repair_timeout` | 100 with jitter | send `get_blocks` |
| `grid_scrub_timeout` | 500, adaptive | background scrubbing |
| `pulse_timeout` | 100 | primary checks whether the state machine needs a pulse |
| `upgrade_timeout` | 5000 | primary checks whether the cluster can upgrade |
| `commit_stall_timeout` | 10, adaptive | primary backpressure |

## 2. Normal case, timestamps and pulses

### 2.1 Request admission (primary)

`on_request` and `ignore_request_message*` in `replica.zig`:

1. Drop unless `status == normal`, not a standby, and the client's view is not ahead of ours.
   A backup forwards a new request to the primary, or answers a duplicate itself from its client
   table.
2. Drop while a view change is being persisted (`view_durable_updating()`), and drop ordinary
   requests during an upgrade.
3. Session checks against the client table. Evict the client (`eviction` message with a reason)
   if its release is out of range, its session number is unknown (`no_session`), too low, or the
   request is malformed.
4. Duplicate detection: same `request` number and checksum as the stored entry means resend the
   stored reply. A new request must be exactly `entry.request + 1` and must carry
   `parent == entry.header.context`, which proves the client saw our last reply. At most one
   request in flight per client.
5. If the pipeline (prepare queue plus request queue) is full, drop. Clients retry.
6. Timestamp gate: the primary needs `clock.realtime_synchronized()`. If the clock is not
   synchronized the request is dropped and the abdication timer keeps running, so a primary with
   a bad clock steps down by silence.

### 2.2 Prepare (primary)

`primary_pipeline_prepare`:

```
prepare_timestamp = max(max(prepare_timestamp, commit_timestamp) + 1, realtime)
state_machine.prepare(operation, body)        // may advance prepare_timestamp per event
header = Prepare {
    parent        = journal.header(op).checksum,
    request_checksum,
    checkpoint_id = if op + 1 <= op_checkpoint_next { parent_checkpoint_id } else { checkpoint_id() },
    op            = op + 1,
    commit        = commit_max,
    timestamp     = prepare_timestamp,
    view, client, request, operation, release (from the request),
}
```

The request buffer is reused as the prepare. The primary pushes it on the prepare queue, starts
`prepare_timeout` and `primary_abdicate_timeout`, and calls its own `on_prepare`, so the primary
journals through exactly the same path as a backup.

Timestamps are therefore strictly monotonic across the log, assigned once by the primary, and
carried in the prepare. The state machine never reads a clock. `execute_op` asserts
`commit_timestamp < prepare.header.timestamp`. After a view change the new primary sets
`prepare_timestamp` to at least the maximum head timestamp seen in the JV quorum
(`primary_set_log_from_join_view_messages`), which keeps monotonicity across primaries even when
their wall clocks disagree.

### 2.3 Replication topology

`docs/internals/vsr.md` still describes ring/chain replication ("each replica, in a chain
beginning with the primary, ... forwards the prepare to the next replica"). The code at HEAD does
**star** replication: `replicate()` only sends when `status == normal and primary()`, and then to
all other replicas and standbys in parallel with the local WAL write. On `prepare_timeout` the
primary resends the oldest unacked prepare directly to each replica that has not acked. Treat the
doc as stale.

### 2.4 Prepare handling (any replica)

`on_prepare`:

1. A prepare from an older view, or from the current view with `op <= self.op`, is treated as a
   repair (`on_repair`): it can fill a dirty slot but never moves the head.
2. Backups call `advance_commit_max(header.commit)` and cache the message in the pipeline cache.
3. Bound on how far ahead a prepare may be: `op <= op_prepare_max`, or, if beyond it, at most
   `min(commit_min + journal_slot_count, op_checkpoint_next + journal_slot_count - 1)`. This is
   what stops the WAL from overwriting entries the current checkpoint still needs.
4. `header.checkpoint_id` must equal the replica's own id for that op range. A mismatch is
   `@panic("checkpoint diverged")`. This is a storage-determinism check on every prepare:
   `checkpoint_id` is the checksum of the previous checkpoint state, so a replica whose on-disk
   state differs by one bit crashes at the next prepare instead of silently diverging.
5. A gap between `self.op` and the new op is allowed in normal status
   (`jump_to_newer_op_in_normal_status`); the missing headers are repaired later. A parent
   mismatch within the same view is a panic (`panic_if_hash_chain_would_break_in_the_same_view`).
6. `self.op = header.op`; `journal.set_header_as_dirty(header)`; `write_prepare`. When the write
   completes, `send_prepare_ok`.

`send_prepare_ok` conditions (all must hold): `status == normal`; `header.op <= self.op`; not
syncing; `op <= op_prepare_ok_max()`; `journal.has_prepare(header)` (header present and slot
clean, so the prepare is durable); not a standby. The ack always goes to
`primary_index(self.view)`, never to the primary of the prepare's own view, and is stamped with
the current view. The send path additionally drops it until `log_view` is durable (section 3.6).

### 2.5 Commit

`on_prepare_ok` (primary): count unique acks per prepare; at exactly `quorum_replication` (the
primary's own ack counts, and only once its own WAL write completes) mark `ok_quorum_received`,
reset the abdication timer and call `commit_pipeline`. Commits are strictly in order from the head
of the prepare queue.

`on_commit` (backup): same view only; heartbeat deduplicated by `timestamp_monotonic`; if the
backup has a header for the commit op with a different checksum while its hash chain is valid, it
panics; otherwise `advance_commit_max` then `commit_journal`.

A backup commits `commit_min + 1` only when `valid_hash_chain()` holds, or when it is in normal
status and that header's view equals the head header's view (`commit_start_journal`).
`valid_hash_chain` requires `op >= op_repair_max`, and an unbroken parent chain from
`commit_min` (or checkpoint + 1) to the head. This is the rule that stops a backup from executing
an op from a forked suffix.

The commit path is a hand-written state machine, `CommitStage`:

```
idle -> start -> check_prepare -> prefetch -> stall -> reply_setup -> execute
     -> checkpoint_durable -> compact
     -> checkpoint_data { aof, state_machine, client_replies, client_sessions, grid }
     -> checkpoint_superblock -> idle
```

- `prefetch`: asynchronously load everything the op will read into memory, so `execute` is
  synchronous and deterministic.
- `stall` (primary only, `commit_stall`): backpressure. If backups lag, the primary delays its own
  commit by 10 ms per quarter checkpoint of lag (capped), and with probability 2/5 for one tick
  when lag is small, so that lagging replicas are not pushed into state sync.
- `execute` (`execute_op`): run the state machine, `commit_min += 1`, build the reply (header
  copies prepare fields, `context` chains the reply checksum), update the client table, write the
  reply to its `client_replies` slot. The reply is sent by the primary, and also by one backup
  chosen by `PRNG.from_seed(op)`.
- `compact`: one "beat" of LSM compaction per op (section 5).
- checkpoint stages run only at checkpoint triggers (section 5).

### 2.6 Heartbeats and abdication

- The primary sends `commit` every 500 ms when `commit_min == commit_max`.
- Backups track heartbeats in `commit_fault`. On red, a backup calls `send_exit_view()`.
- `primary_abdicate_timeout` (10 s without a fresh prepare_ok, or clock unsynchronized) sets
  `primary_abdicating = true`. While set the primary does not send heartbeats
  (`send_commit` returns early), so backups time out and elect someone else. It is cleared by the
  next prepare_ok. This handles the primary that can send but not receive.

### 2.7 Pulses

A pulse is how the state machine gets time-driven work (for example expiring pending transfers)
without reading a clock. `on_pulse_timeout` every 100 ms on the primary, when the pipeline is not
full and `pulse_enabled()`:

```
timestamp = max(prepare_timestamp, realtime)
if state_machine.pulse_needed(timestamp) {
    prepare_timestamp = timestamp
    send_request_pulse_to_self()      // operation = pulse, client = 0, no session, no reply
}
```

The same check runs after every commit in `commit_execute`. `pulse_enabled` is false when a pulse
is already in the pipeline, during an upgrade, during AOF recovery, or while a solo replica is
persisting its view. A pulse is an ordinary prepare: it is replicated, journaled and executed at
its op on every replica, so time-driven effects are part of the deterministic log. Upgrades use
the same self-request mechanism (`operation = upgrade`).

## 3. View change as implementable rules

Sources: `replica.zig` functions `send_exit_view`, `on_exit_view`,
`transition_to_view_change_status`, `update_join_view_headers`, `send_join_view`, `on_join_view`,
`JVQuorum`, `primary_set_log_from_join_view_messages`, `set_op_and_commit_max`, `repair`,
`primary_start_view_as_the_new_primary`, `create_view_message`, `on_view`, `jump_view`,
`view_durable_update`, `send_message_to_replica_base`; and `vsr.Headers` in `src/vsr.zig`.

### 3.1 Triggering: exit_view

EV runs in both normal and view_change status (the paper only runs SVC in view_change). It plays
the role of Raft's PreVote plus CheckQuorum: a single partitioned replica cannot drag the cluster
into a new view.

- R-EV1. A backup sends `exit_view(view)` to all replicas (and sets its own bit) when its
  heartbeat detector goes red, or when it has sat in view_change for
  `view_change_status_timeout`. It keeps resending every 500 ms while its bit is set.
- R-EV2. `on_exit_view`: ignore unless `message.view == self.view`, and ignore if standby,
  `recovering_head`, or syncing. Set the sender's bit. When an EV from another replica arrives,
  start the 5 s `exit_view_window_timeout`.
- R-EV3. When `quorum_view_change` bits are set (own bit included), call
  `transition_to_view_change_status(view + 1)`.
- R-EV4. On window timeout clear every bit except our own. Our own bit is cleared when we receive
  a `commit` heartbeat from the primary of the current view (`on_commit`).

### 3.2 Entering view_change

`transition_to_view_change_status(view_new)`:

- R-VC1. If we were in a state with `log_view == view` (normal, or recovering), first compute and
  store the JV headers (`update_join_view_headers`, rule R-JV1). If we were already in
  view_change with `log_view < view`, keep the JV headers we already have: they describe our log
  as of `log_view` and must not change.
- R-VC2. Set `view = view_new`. Start `view_durable_update()` (write `view`, `log_view`, and the
  headers to the superblock). Convert the pipeline queue to a cache. Reset EV and JV quorum state.
  Start `view_change_status_timeout`, `join_view_message_timeout`, and (for non-primaries)
  `get_view_message_timeout`.
- R-VC3. Send `join_view` to the primary of the new view. The send is dropped until the new
  `view` is durable (R-D1) and is retried by the timeout.
- R-VC4. Jumping: on any `join_view`, `exit_view`, `ping` or `pong` with a higher view,
  transition to view_change for that view. On a `prepare` or `commit` with a higher view (or the
  same view while we are in view_change), the view has evidently started, so send `get_view` to
  that view's primary rather than voting (`jump_view`).

### 3.3 What a join_view carries

Header fields (`send_join_view`): `view`, `log_view`, `checkpoint_op`, `op` (the head op of the
JV headers), `commit_min`, `nack_bitset: u128`, `present_bitset: u128`. Body: the JV headers,
high to low op, at most `pipeline_prepare_queue_max + 1` of them.

- R-JV1 (which headers). Starting from the head op and going down, include one entry per op and
  stop after the first op `<= commit_max` (so the list covers every possibly uncommitted op plus
  one committed anchor). For an op whose header is not known, include a **blank**
  (`vsr.Headers.jv_blank(op)`): a reserved header that says "I have no header here". The head
  entry is never blank (a replica that does not know its head is in `recovering_head` and does not
  vote). Blanks must not hide a chain break: all valid headers in one JV belong to one hash chain.
- R-JV2 (nack bit, per header). Set the nack bit if the replica can prove it never prepared that
  op in its `log_view`:
  1. no header for the op in the journal and the slot is not faulty; or
  2. the header matches but the slot is dirty and not faulty (the prepare was never written); or
  3. the journal holds a different header (different checksum) for that op. This is safe even if
     the slot is faulty.
- R-JV3 (cannot nack). Blank plus faulty slot means "I might have prepared this and lost it":
  no nack bit. This is the central change from textbook VRR. A corrupt slot is not evidence of
  absence.
- R-JV4 (present bit). Set if the replica holds the prepare itself:
  `prepare_inhabited[slot] && prepare_checksums[slot] == checksum`, or a write of exactly this
  prepare is in flight, or it is in the pipeline.
- R-JV5. `commit_min <= op`, `checkpoint_op <= commit_min`, `log_view < view`,
  `headers[0].view <= log_view` (`JVQuorum.verify_message`).
- R-JV6. The JV body must be byte-identical to the superblock's durable `view_headers` at the time
  it is sent (asserted in `send_message_to_replica_base`). A replica that restarts mid view change
  resends the same JV, built from the superblock, not from whatever its journal now looks like.

It is safe to advertise a header whose prepare has not been written yet: if the op is committed,
some other member of the quorum has it; if it is not, and nobody can supply it, the nack rule
discards it.

### 3.4 Choosing the log: JVQuorum

`JVQuorum.quorum_headers` is a pure function of the collected JVs. Port it as a pure function and
test it exhaustively.

```
if count(JVs) < quorum_view_change           -> awaiting_quorum

canonical   = JVs with the maximum log_view
op_head_max = max(jv.op for jv in canonical)
op_head_min = max over all JVs of:                       // "commit_max" of the quorum
                  op of the JV's last (lowest) header,
                  jv.op - pipeline_prepare_queue_max,    // saturating
                  jv.commit_min,
                  jv.headers[0].commit

for op in op_head_min ..= op_head_max:
    header_canonical = the valid (non-blank) header for op from any canonical JV, else null
    copies = 0; nacks = 0
    for jv in all JVs:
        if jv.op < op                         -> nacks += 1; continue   // never saw it
        if op is below the JV's lowest header -> nacks += 1; continue
        h = jv's header for op
        if h valid && present bit && h.checksum == header_canonical.checksum -> copies += 1
        if nack bit                                                 -> nacks += 1   // explicit
        else if h valid && header_canonical != null
                        && h.checksum != header_canonical.checksum  -> nacks += 1   // implicit
        else if h valid && header_canonical == null                 -> nacks += 1   // implicit

    if nacks >= quorum_nack_prepare:
        assert op > op_head_min            // never truncate the committed anchor
        head = op - 1; stop
    if header_canonical == null || copies == 0:
        if count(JVs) < replica_count  -> awaiting_repair    // wait for more JVs
        else                           -> complete_invalid   // every replica reported: stuck
    // else: op stays in the log, continue

head defaults to op_head_max
-> complete_valid(canonical headers from head down to op_head_min)
```

Rules worth stating separately:

- R-Q1 (canonical). Only JVs with the highest `log_view` define the log. Their valid headers must
  agree with each other (`JVQuorum.verify` asserts that any two JVs with equal `log_view` have
  equal checksums wherever both have a valid header). Headers from lower `log_view`s are
  "uncanonical": they may have been replaced in a later view.
- R-Q2 (truncation). An uncommitted op is dropped only when `quorum_nack_prepare` replicas prove
  they did not prepare it. Since `nack + replication > n`, the op cannot have reached a
  replication quorum, so it cannot have been committed, so dropping it is safe. Truncation always
  cuts the whole suffix from the first nacked op.
- R-Q3 (no guessing). If an op has no canonical header, or no replica in the quorum has the
  prepare, and there are not enough nacks, the primary does not decide. It waits for more JVs.
  With all replicas reporting and still no decision the view is `complete_invalid`: the code logs
  "deadlocked" and the view change timer moves the cluster to the next view. Availability is
  given up, safety is not.
- R-Q4. `op_head_min` is known to be committed (or at least outside the pipeline). It is never
  nacked and it is the anchor that guarantees the new primary has a head even if the entire
  pipeline is truncated.

How storage faults change classic VRR. In the paper the new primary takes the log of the replica
with the largest `(log_view, op)`. That is wrong once disks can lose or corrupt entries: the
"best" log may have a hole where a committed op used to be, and a replica that lost an op would,
by omission, vote that it never existed. TigerBeetle's version (Protocol-Aware Recovery's CTRL,
abbreviated) changes three things: (1) a replica distinguishes "definitely not prepared" (nack)
from "do not know" (blank, faulty) per op; (2) the head is chosen per op from the union of
canonical JVs, not per replica; (3) the new primary must then actually obtain every prepare up
to the chosen head before it starts the view (section 3.5).

### 3.5 Becoming primary

`on_join_view`, once `quorum_headers` returns `complete_valid`:

- R-P1 (forfeit). The would-be primary gives up the view if it is behind:
  `Checkpoint.durable(self.op_checkpoint_next, commit_max)` (the cluster has a durable checkpoint
  we lack), or some JV has a newer checkpoint than ours and `view - log_view < replica_count`
  (give each better-placed replica one chance). It then jumps straight to the next view whose
  primary reported the maximum checkpoint. Consequence: a new primary never has to state sync.
- R-P2 (install). `primary_set_log_from_join_view_messages`:
  - `prepare_timestamp = max(prepare_timestamp, max head timestamp over all JVs)`.
  - `set_op_and_commit_max(head.op, commit_max)`, which removes journal entries above the new
    head (`journal.remove_entries_from(op + 1)`). Asserted bounds: the new head is
    `>= commit_max`, and the old head is at most `pipeline_prepare_queue_max` above it.
    `commit_max` never decreases.
  - Canonical headers are installed with `replace_header` (trusted: overwrite whatever is there).
  - Headers from uncanonical JVs are only hints: those with `op <= jv.commit_min` are installed
    with `replace_header` (committed, therefore trusted); the rest go through `repair_header`,
    which accepts a header only if it connects to the hash chain.
- R-P3. Set `log_view = view` immediately, before the view starts. If another view change
  happens before this primary reaches normal, its JV must be distinguishable from JVs of replicas
  still at the old `log_view`, because its headers have already been replaced.
- R-P4 (repair before start). In `repair()`: wait until the header chain from `op_repair_min` to
  the head is complete (`primary_journal_headers_repaired`); then send `view` early so backups
  can repair in parallel; start `view_durable_update()`; wait until every prepare in that range is
  on disk (`primary_journal_prepares_repaired`: no dirty slots); commit everything up to
  `commit_max`; then rebuild the pipeline queue from the journal (`primary_repair_pipeline`) and
  call `primary_start_view_as_the_new_primary`, which goes to normal status and re-requests acks
  for the uncommitted suffix.
- R-P5. During this repair the new primary broadcasts `get_prepare` for uncommitted ops to all
  replicas. If nobody has an uncommitted prepare, the view cannot complete; the next view change
  will collect explicit nacks for it and truncate it.

### 3.6 Durability of view state

This is the part most implementations get wrong, and the code is explicit about it.

- R-D1. Never advertise a `view` that is not durable. `send_message_to_replica_base` drops any
  message this replica authored whose `header.view > view_durable()`, except `get_view`. Pings
  and pongs are stamped with the durable view so clock sync is not blocked.
- R-D2. `join_view`, `view` and `prepare_ok` are dropped while `log_view_durable() < log_view`.
  A JV states the `log_view`; a `view` or a `prepare_ok` implies it.
- R-D3. `view_durable_update()` writes `{view, log_view, view_headers, commit_max, sync info}` via
  `superblock.view_change`. One update at a time; if the in-memory values moved on, the callback
  starts another. The callback then does the sends that were deferred (primary: `view`; backup:
  prepare_oks; anyone with `log_view < view`: JV).
- R-D4 (why `view`). If a backup's view could go backward after a crash, it could ack a prepare
  from the old primary after having promised the new one, and the old primary could commit an op
  the new primary never hears about.
- R-D5 (why `log_view` and the headers). If the advertised `log_view` regressed, a replica could
  present stale headers as canonical in a later view change and resurrect a truncated op or drop
  a committed one. The doc comment above `view_durable_update` walks through a three-replica
  example. The headers are persisted together with `log_view` because after a crash the journal
  alone cannot say which of its entries belong to `log_view` (see section 4, "drop headers that
  cannot belong to log_view").
- R-D6. No client requests are accepted while `view_durable_updating()`.

### 3.7 The view message and backups

`create_view_message`: header `{view, checkpoint_op, op, commit_max, nonce}`; body is the
`vsr.CheckpointState` followed by headers: the suffix of up to `view_change_headers_suffix_max`
headers from the head down, plus up to two "hooks", the headers at
`op_prepare_max - vsr_checkpoint_ops` and `op_prepare_max - 2 * vsr_checkpoint_ops` (previous
checkpoint triggers, which let a lagging replica attach its log). The primary also sends a `view`
when a checkpoint becomes durable, and in reply to `get_view`.

`on_view`:

- R-V1. Ignore if `view < self.view`, or if `view == log_view` and `message.op < self.op`
  (stale). A `recovering_head` replica accepts a `view` only if it is fresh: `view > self.view`,
  or `op >= op_prepare_max`, or `nonce == self.random_nonce` (a reply to its own `get_view`).
  Without the nonce an old delayed `view` could install a head that is behind what this replica
  had acked before it lost its head.
- R-V2. Decide between state sync and log install (`on_view_set_checkpoint`): sync if the
  cluster's next checkpoint is durable and we are either far behind (more than one checkpoint) or
  `repair_stuck()`. See section 6.
- R-V3. Otherwise (`on_view_set_journal`): new head = the first header in the message with
  `op <= op_prepare_max`; `set_op_and_commit_max` (truncating our suffix above it); install the
  headers that fit with `replace_header`; store them as our `view_headers`; `view = log_view =
  message.view`; go to normal; start `view_durable_update()`; when durable, send prepare_oks for
  every uncommitted op we hold (`send_prepare_oks_after_view_change`); then commit and repair.
- R-V4. A backup sends prepare_oks from `commit_max + 1` to `op` for whatever headers it has,
  even across breaks. This is safe only because the ack carries `prepare_checksum` and the primary
  ignores acks that do not match its pipeline.

### 3.8 Restart (`open`)

- R-O1. Open the superblock, then recover the journal (section 4). All slots faulty is
  `error.WALInvalid`.
- R-O2. Choose the head. If `log_view == view`: the maximum op among journal headers. If
  `log_view < view` (crashed during a view change): the head comes from the superblock's
  `view_headers`, not the journal.
- R-O3. `commit_max = max(superblock commit_max, op - pipeline_prepare_queue_max)`.
- R-O4. Solo replica: any faulty slot is `error.WALCorrupt` (nobody to repair from). It bumps
  `view` and `log_view` on every restart and persists them, so a torn, truncated op and its
  replacement never share a view.
- R-O5. Multi-replica, `log_view == view`: if `op_head_certain()`, then go to view_change for
  `view + 1` if we were the primary (never resume as primary), else go to normal as a backup.
  If the head is not certain, go to `recovering_head`.
- R-O6. `log_view < view`: go to view_change for `view` and resend the stored JV.
- R-O7. `op_head_certain()`: certain if `op == op_prepare_max`. Uncertain if the head slot is
  faulty, or the `op_prepare_max` slot is faulty, or any slot in `(op, op_prepare_max]` is faulty:
  a faulty slot above the apparent head might have held a newer op that this replica acked.
- R-O8. `recovering_head` replicas ask for a `view`: any message from a view's primary triggers a
  `get_view` with the replica's nonce (`jump_view`).

### 3.9 Invariants to assert

- `commit_min <= commit_max <= op` once repaired; `op - commit_max <= pipeline_prepare_queue_max`
  on a primary.
- A committed op's header never changes: `repair_header` and `replace_header` assert that an op in
  `(op_checkpoint, commit_min]` is only ever "replaced" by the identical header.
- Repair never advances the head (`assert(header.op <= self.op)` in both functions).
- prepare_ok is only ever sent to `primary_index(self.view)` with `header.view == self.view`.
- JVs with equal `log_view` never conflict.
- The op after the checkpoint chains to the checkpoint header stored in the superblock.


## 4. Journal (WAL) layout and recovery

Source: TigerBeetle `main` at commit `a2a3c70` (2026-11-10, 0.18.0 era). All paths are relative to the repo root. Line numbers refer to that commit. Everything here was read, not executed; items marked "(inferred)" are my reasoning from the code rather than something the source states.

Primary file: `src/vsr/journal.zig` (2585 lines). Supporting: `src/vsr/replica_format.zig`, `src/vsr/message_header.zig`, `src/constants.zig`, `src/config.zig`, `src/vsr/replica.zig` (consumers), `src/testing/cluster/journal_checker.zig`.

### 4.1 On-disk layout

The data file zones are, in order: `superblock`, `wal_headers`, `wal_prepares`, `client_replies`, `grid_padding`, `grid` (`src/vsr.zig`, `Zone`, line 129). The WAL is the two zones `wal_headers` + `wal_prepares`: "two contiguous circular buffers on disk" (`journal.zig:16-45`, `Ring`).

| Ring | Entry | Entry size | Offset of slot `s` within the ring |
|---|---|---|---|
| `headers` ("redundant headers") | one `Header.Prepare` | 256 bytes (`@sizeOf(Header) == 256`, `message_header.zig:72`) | `sector_floor(s * 256)`; headers are always written a whole 4096-byte sector (16 headers) at a time |
| `prepares` | one full prepare message (header + body), padded to `message_size_max` | `message_size_max` | `message_size_max * s` |

- `slot = op % journal_slot_count` (`slot_for_op`, `journal.zig:396`). The same slot index addresses both rings and all in-memory arrays.
- Sizes (`constants.zig:189-198`): `journal_size_headers = journal_slot_count * 256`, `journal_size_prepares = journal_slot_count * message_size_max`.
- `sector_size == 4096` is asserted (`constants.zig:493-496`); `headers_per_sector = 16` (`journal.zig:47`).

| Constant | production (`config.zig` defaults) | `test_min` (simulator) |
|---|---|---|
| `journal_slot_count` | 1024 | 32 (`journal_slot_count_min = 2 * 16`) |
| `message_size_max` | 1 MiB | 4096 (`message_size_max_min(4)`) |
| headers ring | 256 KiB = 64 sectors | 8 KiB = 2 sectors |
| prepares ring | 1 GiB | 128 KiB |
| `journal_iops_read_max` | 8 (2 commit + 6 repair) | 3 (2 commit + 1 repair) |
| `journal_iops_write_max` | 32 | 2 |
| `pipeline_prepare_queue_max` | 8 | 4 |
| `lsm_compaction_ops` | 32 | 4 |
| `vsr_checkpoint_ops` (derived, `constants.zig:40-42`) | 960 | 20 |

Comptime invariants (`journal.zig:106-135`, `constants.zig:200-224`): `slot_count` even, a multiple of `headers_per_sector` and of `lsm_compaction_ops`; `slot_count > pipeline_prepare_queue_max` ("the length of the prepare pipeline is the upper bound on how many ops can be reordered during a view change"); `slot_count >= 2 * pipeline_prepare_queue_max`; in production, header sectors (64) must exceed `journal_iops_write_max` (32) so that a crash cannot tear every redundant-header sector at once. `journal_slot_count_min` is two header sectors for the same reason: "otherwise we could lose them all to a single torn write" (`config.zig:175-179`).

**Why headers are stored twice.** The prepare ring already contains each header. The redundant ring exists so recovery can "disentangle corruption from crashes" (`constants.zig:187-188`): comparing two independently written copies distinguishes a torn append (never acked, safe to forget) from a prepare that was durably acked and later lost (must not be forgotten or nacked). It also catches misdirected/lost writes that leave one stale-but-checksum-valid copy.

**Header format** (`message_header.zig:688-863`, `Header.Prepare`): `checksum` (u128, covers the rest of the header), `checksum_body`, `cluster`, `size`, `view`, `release`, `command`, `replica` (author), `parent` (checksum of the previous prepare: the hash chain), `request_checksum`, `checkpoint_id`, `client`, `op`, `commit`, `timestamp`, `request`, `operation`. Two special headers:
- `Header.Prepare.reserve(cluster, slot)`: `operation = .reserved`, `op = slot index`, everything else zero, valid checksum. "In each ring, the `op` for reserved headers is set to the corresponding slot index. This helps WAL recovery detect misdirected reads/writes" (`journal.zig:20-21`).
- `Header.Prepare.root(cluster)`: `operation = .root`, `op = 0`, `view = 0`, `parent = 0`, header-only.

**Format** (`replica_format.zig`, `queue_format_wal`, lines 87-196; `slot_header`, 313-323): slot 0 gets the root prepare, every other slot a reserved prepare, in both rings. One sector is written at the start of each prepare slot (`journal_slot_count` writes), then one write covering the whole headers ring (`writes_max = journal_slot_count + 1`). Prepares are issued first so that the invariant "never write the redundant header before the prepare" holds even at format time (comment, lines 98-100). Then the superblock is formatted and the file is flushed.

### 4.2 Durability assumptions and write ordering

- Production storage uses direct I/O (`direct_io = true`) and, per the comment in `src/storage.zig` (`flush_sectors`, ~line 474), `O_DSYNC`-style writes "which are always enabled and thus don't need an explicit fsync". So a write completion means durable. I did not read the IO layer's open flags directly.
- A read `EIO` (latent sector error) is bisected down to a sector, and the unreadable sector is zeroed (`src/storage.zig:255-300`), so the journal sees it as a checksum failure. A write `EIO` panics.

Order for one prepare (`journal.zig:1788-1956`):

1. Replica sets `replica.op` and calls `journal.set_header_as_dirty(header)`: the in-memory `headers[slot]` is updated first, `dirty` set (`replica.zig:2235-2238`).
2. `write_prepare`: write `message.buffer[0..sector_ceil(size)]` to the prepares ring. `prepare_inhabited[slot] = false`, `prepare_checksums[slot] = 0` while in flight. Trailing sector padding is asserted zero.
3. On prepare-write completion (`write_prepare_header`): set `prepare_inhabited/prepare_checksums`; set `headers_redundant[slot] = header`; snapshot the containing 16-header sector **from `headers_redundant`, not `headers`** ("we need to avoid writing (leaking) a redundant header before its corresponding prepare is on disk", `header_sector`, line 2143) into a per-IOP buffer (`write_headers_sectors[write_index]`, copy-on-write because in-memory headers can change mid-write); write that sector to the headers ring.
4. On header-write completion (`write_prepare_on_write_header`): if the slot still holds the same header, clear `dirty` and `faulty`, and invoke the callback. Only then does the replica `send_prepare_ok` (`replica.zig:11262-11270`; `send_prepare_ok` also requires `journal.has_prepare`, line 8695).

So: prepare first, redundant header second, ack third. A redundant header for op X on disk implies the prepare write for X completed. The field doc on `headers_redundant` (`journal.zig:221-236`) gives the motivating example: ops 6 and 7 arrive, 7's prepare finishes first and its header sector is written; header 6 is still written as reserved, so a crash leaves slot 6 in case `@L` (nil, locally fine) rather than `@K` (needs remote repair).

**Serialization of header-sector writes** (`write_sectors`, `lock_sectors`, `write_sectors_on_write`, lines 2014-2118). Each `Write` has a `Range {ring, offset, buffer, locked, next}`. `lock_sectors` scans in-flight writes; if a *locked* range overlaps (asserted possible only for the headers ring with identical offset and length), the new range is appended to that range's `next` list and waits. On completion the waiters are re-driven through `lock_sectors` in FIFO order. Because a later waiter snapshotted the sector later, it is a superset of earlier snapshots (inferred), so FIFO order never regresses a neighbor's header. Prepare-ring writes never overlap because the replica refuses to start a write when `journal.writing(header) != .none` (`replica.zig:11240`), giving the invariant "at most one Write to a given slot at any time" (`journal.zig:262-265`). Storage must be always-sync or always-async, never mixed (line 2078).

**Write IOPs.** `journal_iops_write_max` concurrent writes; if none is free, `write_prepare` calls back with `null` ("waiting for IOP") and repair retries later. A single-replica cluster writes strictly sequentially (`append`, `replica.zig:4243-4248`; `assert(journal.writes.executing() == 0)` at `journal.zig:1803`) so the WAL can never have gaps after a crash.

**Wrap-around overwrite.** The new op replaces `headers[slot]` in memory (`set_header_as_dirty` asserts `existing.op <= header.op`: "overwriting a new op with an old op would be a correctness bug"), `headers_redundant[slot]` is reset to reserved, then the prepare is overwritten in place, then the header sector. Safety of overwriting is a replica-level rule: the overwritten op must belong to a checkpoint that is durable on a commit quorum (`on_prepare`, `replica.zig:2210-2218`) and the new op must be `<= op_prepare_max()` (or the slot's old op already committed).

### 4.3 In-memory state

All arrays have `slot_count` entries (`journal.zig:210-298`).

| Field | Meaning |
|---|---|
| `headers: []Header.Prepare` | The authoritative in-memory log index. Each entry is `command=prepare`, either a real header or `reserved` with `op == slot`. Updated *before* writing. During recovery it temporarily holds the unvalidated headers read from the prepares ring. |
| `headers_redundant` | "Headers whose prepares are on disk"; the source for header-sector writes. Updated *after* the prepare write. During recovery holds the raw redundant ring. |
| `prepare_checksums: []u128`, `prepare_inhabited: []bool` | Checksum of whatever valid prepare physically sits in the slot, independent of `headers`. Lets the replica serve `get_prepare` even when the slot is faulty or was truncated in memory. Cleared while a write is in flight; deliberately *not* cleared by `remove_entry`. |
| `dirty: BitSet` | "We have not prepared the entry, or need to repair a faulty entry." In-memory header exists but the disk copy is missing or unverified. |
| `faulty: BitSet` | "We prepared and then lost the entry" (corruption, misdirected read/write, latent sector error), or cannot rule that out. Invariant: **faulty implies dirty**; "a faulty bit is used then only to qualify the severity of the dirty bit". |
| `status` | `init` → `recovering(callback)` → `recovered`. |
| `reads`, `writes` | Fixed IOP pools. Reads split into `reads_commit_count_max = 2` and `reads_repair_count_max = journal_iops_read_max - 2`, so `get_prepare` traffic from a partitioned peer cannot starve the commit path (lines 98-104). |

Helper types: `Slot { index }`, `SlotRange { head, tail }` (inclusive, wrapping; `contains` panics on `head == tail`, callers special-case it), `Writing = enum { none, slot, exact }`.

The key distinction for protocol-aware recovery: **dirty and not faulty = "I am certain I never durably held this"; faulty = "I might have".**

### 4.4 Recovery phases

`Journal.recover` (`journal.zig:941`) runs once from `Replica.open` after the superblock is opened (`replica.zig:760`).

1. **`recover_headers`** (960-1066): read the redundant ring in `message_size_max` chunks straight into `headers_redundant`, unvalidated.
2. **`recover_prepares` / `recover_prepare`** (1068-1168): read every prepare slot at full `message_size_max` (size is unknown), `journal_iops_read_max` at a time. A prepare's header is stored into `headers[slot]` only if header checksum, body checksum, and zeroed sector padding all verify; otherwise the entry stays invalid. (`dirty`/`faulty` are borrowed as progress bitmaps here and reset to full afterwards.)
3. **`recover_slots`** (1240-1341): for each slot compute `header = header_ok(headers_redundant[slot])`, `prepare = header_ok(headers[slot])`, classify with `recovery_case`, set `prepare_inhabited/prepare_checksums` for any valid non-reserved prepare; refine torn slots with `torn_prepares()`; apply `recover_slot` per slot; copy `headers` into `headers_redundant`; then drop headers outside the durable view range (4.7).
4. **`recover_fix`** (1608-1663): for `fix` slots (dirty, not faulty) rewrite the redundant-header sector, one sector per write, clearing `dirty`.
5. **`recover_done`** (1665-1701): asserts `dirty.count == faulty.count` (only `vsr` slots remain), every `headers[i]` valid and equal to `headers_redundant[i]`, every non-reserved header has `prepare_inhabited` with matching checksum, and a non-reserved slot 0 at op 0 is exactly the root header and not faulty.

`header_ok` (2478-2504) is the definition of "valid": checksum valid (checked first, before touching any field), `command == .prepare`, `cluster` matches, and the header is in the right slot (`reserved`: `op == slot`; otherwise `op % slot_count == slot`). There is no check on the `replica` field (it names the authoring primary).

### 4.5 The recovery decision table

Authoritative comptime table, verbatim (`journal.zig:2302-2344`):

```zig
const recovery_cases = table: {
    const __ = Matcher.any;
    const _0 = Matcher.is_false;
    const _1 = Matcher.is_true;
    // The replica will abort if any of these checks fail:
    const a0 = Matcher.assert_is_false;
    const a1 = Matcher.assert_is_true;

    break :table [_]Case{
        // Legend:
        //
        //    R>1   replica_count > 1  or  standby
        //    R=1   replica_count = 1 and !standby
        //     ok   valid checksum ∧ valid cluster ∧ valid slot ∧ valid command
        //    nil   operation == reserved
        //     ✓∑   header.checksum == prepare.checksum
        //    op⌈   prepare.op is maximum of all prepare.ops
        //    op>₁  prepare.op > op_prepare_max
        //    op>₂  header.op > op_prepare_max
        //    op=   header.op == prepare.op
        //    op<   header.op  < prepare.op
        //   view   header.view == prepare.view
        //
        //        Label  Decision      Header  Prepare Compare
        //               R>1   R=1     ok  nil ok  nil op⌈ op> op> ✓∑  op= op< view
        Case.init("@A", .vsr, .vsr, .{ _0, __, _0, __, __, a0, a0, __, __, __, __ }),
        Case.init("@B", .vsr, .vsr, .{ _1, _1, _0, __, __, a0, __, __, __, __, __ }),
        Case.init("@C", .vsr, .vsr, .{ _1, _0, _0, __, __, a0, __, __, __, __, __ }),
        Case.init("@D", .vsr, .fix, .{ _0, __, _1, _1, __, __, a0, __, __, __, __ }),
        Case.init("@E", .vsr, .fix, .{ _0, __, _1, _0, _0, _0, a0, __, __, __, __ }),
        Case.init("@F", .fix, .fix, .{ _0, __, _1, _0, _1, _0, a0, __, __, __, __ }),
        Case.init("@G", .fix, .fix, .{ _1, _1, _1, _0, __, _0, __, __, __, __, __ }),
        Case.init("@H", .cut, .unr, .{ __, __, _1, _0, __, _1, __, __, __, __, __ }), // prepare.op > op_prepare_max
        Case.init("@I", .cut, .unr, .{ _1, _0, _1, _0, __, _0, _1, __, __, a0, __ }), // header.op > op_prepare_max, prepare !reserved
        Case.init("@J", .cut, .unr, .{ _1, _0, _1, _1, __, __, _1, __, __, a0, __ }), // header.op > op_prepare_max, prepare reserved
        Case.init("@K", .vsr, .vsr, .{ _1, _0, _1, _1, __, __, _0, __, __, __, __ }),
        Case.init("@L", .nil, .nil, .{ _1, _1, _1, _1, __, __, __, a1, a1, a0, a1 }), // normal path: reserved
        Case.init("@M", .fix, .fix, .{ _1, _0, _1, _0, __, _0, _0, _0, _0, _1, __ }), // header.op < prepare.op
        Case.init("@N", .vsr, .vsr, .{ _1, _0, _1, _0, __, _0, _0, _0, _0, _0, __ }), // header.op > prepare.op
        Case.init("@O", .vsr, .vsr, .{ _1, _0, _1, _0, __, _0, _0, _0, _1, a0, a0 }), // header.view != prepare.view
        Case.init("@P", .eql, .eql, .{ _1, _0, _1, _0, __, _0, _0, _1, a1, a0, a1 }), // normal path: prepare
    };
};

const case_cut_torn = Case{
    .label = "@TruncateTorn",
    .decision_multiple = .cut_torn,
    .decision_single = .cut_torn,
    .pattern = undefined,
};
```

The eleven pattern columns, in order (`Case.pattern` doc, 2379-2389; computed in `recovery_case`, 2445-2457; any comparison involving a non-ok side evaluates to `false`):

0. `header_ok(header)` 1. `header.operation == reserved` 2. `header_ok(prepare)` and valid body checksum 3. `prepare.operation == reserved` 4. `prepare.op == op_max` (max valid op across *both* rings) 5. `prepare.op > op_prepare_max` 6. `header.op > op_prepare_max` 7. checksums equal 8. `header.op == prepare.op` 9. `header.op < prepare.op` 10. views equal.

The human-oriented version in the doc comment on `recover_slots` (`journal.zig:1206-1238`), also verbatim:

```
  label                   @A  @B  @C  @D  @E  @F  @G  @H  @I  @J  @K  @L  @M  @N  @O  @P
  header valid             0   1   1   0   0   0   1   _   1   1   1   1   1   1   1   1
  header reserved          _   1   0   _   _   _   1   _   0   0   0   1   0   0   0   0
  prepare valid            0   0   0   1   1   1   1   1   1   1   1   1   1   1   1   1
  prepare reserved         _   _   _   1   0   0   0   0   0   1   1   1   0   0   0   0
  prepare.op is maximum    _   _   _   _   0   1   _   _   _   _   _   _   _   _   _   _
  prepare.op > prep_max   !0  !0  !0   _   0   0   0   1   0   _   _   _   0   0   0   0
  header.op > prep_max    !0  !0  !0   _   0   0   0   1   1   1   0   _   0   0   0   0
  match checksum           _   _   _   _   _   _   _   _   _   _   _  !1   0   0   0   1
  match op                 _   _   _   _   _   _   _   _  !0  !0   _  !1   <   >   1  !1
  match view               _   _   _   _   _   _   _   _   _   _   _  !1   _   _  !0  !1
  decision (replicas>1)  vsr vsr vsr vsr vsr fix fix cut cut cut vsr nil fix vsr vsr eql
  decision (replicas=1)              fix fix

Legend:
   0  false          1  true         !0  assert false     !1  assert true      _  ignore
   <  header.op < prepare.op         >  header.op > prepare.op
 eql  The header and prepare are identical; no repair necessary.
 nil  Reserved; dirty/faulty are clear, no repair necessary.
 fix  Repair header using local intact prepare.
 vsr  Repair with VSR `get_prepare`.
```

The two tables differ in small ways (the doc comment is slightly stale): the code does not assert `header.op > prep_max` false for `@B`/`@C`, and the code gives `unr` (unreachable) for `@H`/`@I`/`@J` when `R=1`. Treat the comptime table as the spec. The test `"recovery_cases"` (2506-2540) enumerates all 2^11 inputs and checks that each matches exactly one case or trips an assertion; `recovery_case` asserts exactly one match at runtime.

Explained:

| Case | Redundant header | Prepare | Extra condition | R>1 | R=1 | Source's explanation (2204-2301) |
|---|---|---|---|---|---|---|
| @A | invalid | invalid | | vsr | vsr | Both copies lost. |
| @B | valid, reserved | invalid | | vsr | vsr | Prepare corrupt; "may be caused by crashing while writing the prepare (torn write)". |
| @C | valid, real op | invalid | | vsr (or `cut_torn`) | vsr (or `cut_torn`) | Prepare corrupt; have a header but need the full message. |
| @D | invalid | valid, reserved | | vsr | fix | Possibly a torn header-sector write; R=1 must repair locally (incorrect only with a triple coincidence during the first wrap). |
| @E | invalid | valid, real op | `prepare.op` is not the max, `<= op_prepare_max` | vsr | fix | Torn header write, or corrupt/misdirected header read, or multiple faults (e.g. header corrupt *and* latest prepare write misdirected), so the valid prepare may be stale. |
| @F | invalid | valid, real op | `prepare.op` is the max | fix | fix | Crashed after writing the prepare, before/while writing the header. |
| @G | valid, reserved | valid, real op | `prepare.op <= op_prepare_max` | fix | fix | Same as @F; or the prepare was truncated and the header rewritten as reserved; or a lost/misdirected header write. "Risk of data loss in the case of 2 lost writes." |
| @H | any | valid, real op | `prepare.op > op_prepare_max` | cut | unr | Op belongs past the replica's checkpoint window. |
| @I | valid, real op `> op_prepare_max` | valid, real op `<= max` | | cut | unr | Same, the header is the one past the window. |
| @J | valid, real op `> op_prepare_max` | valid, reserved | | cut | unr | Same. |
| @K | valid, real op | valid, reserved | `header.op <= op_prepare_max` | vsr | vsr | Header present, "the corresponding prepare was a lost or misdirected read or write". |
| @L | valid, reserved | valid, reserved | asserts equal | nil | nil | Legitimately empty slot (first fill of the log). |
| @M | valid, real op | valid, real op | different ops, `header.op < prepare.op` | fix | fix | Pick the higher op; prepare is newer: "the log wrapped, but the redundant header write was lost". |
| @N | valid, real op | valid, real op | different ops, `header.op > prepare.op` | vsr | vsr | Header is newer; the newer prepare was lost. |
| @O | valid, real op | valid, real op | same op, different checksum (asserted different view) | vsr | vsr | Rewritten in a view change, or a lost prepare write over a same-op older-view prepare. Recovery "can't distinguish which is actually *newer*", so cannot `fix` despite a valid prepare. |
| @P | valid | valid | checksums equal | eql | eql | Normal. |

For `@M`/`@N` the source justifies "always pick the higher op": the lower op "must be from a previous wrap; it is too far back (>pipeline) to have been replaced by a view change" (hence the comptime assert `slot_count > pipeline_prepare_queue_max`).

Effect of each decision (`recover_slot`, 1485-1606; `RecoveryDecision`, 2353-2369):

| Decision | `headers[slot]` after | dirty | faulty | Notes |
|---|---|---|---|---|
| `eql` | the header | 0 | 0 | |
| `nil` | reserved | 0 | 0 | |
| `fix` | the prepare's header | 1, then 0 after `recover_fix` rewrites the header sector | 0 | Local repair of the redundant ring only. |
| `vsr` | **reserved** (never the suspect header) | 1 | 1 | Repair via `get_prepare`. For R=1 the replica then fails to open. |
| `cut` | reserved | 0 | 0 | Truncate ops beyond `op_prepare_max`. |
| `cut_torn` | reserved | 0 | 0 | Truncate a torn append; asserts header valid, prepare invalid. |
| `unr` | | | | Unreachable. |

Rule stated in the source (1174-1197): "recovery will never load a header into a slot *and* mark that slot faulty". If it did, a later header-sector write for a neighboring slot would persist that header, and after another crash the slot would come back clean, allowing an incorrect nack.

### 4.6 Torn writes versus corruption

`torn_prepares()` (`journal.zig:1343-1483`) returns up to `journal_iops_write_max` slots that are safe to truncate (that many appends can be in flight at once). Conditions:

- `op_max` = the highest valid, non-reserved op across both rings. Require `op_checkpoint <= op_max < op_prepare_max` (below the checkpoint the head is uncertain; at or above `op_prepare_max` the `cut` cases already handle it).
- Candidate range: slots of ops `(op_max, op_prepare_max]`. Any valid header or prepare there must be from a previous wrap.
- For every slot in that range whose decision is `vsr`, all of the following must hold, otherwise **no slot at all** is truncated: (1) redundant header is `header_ok`; (2) it is not reserved; (3) the prepare's header checksum is invalid ("a valid prepare could be faulty due to a misdirected read/write"). The code then asserts the redundant header's op is `< op_max` and `<= op_checkpoint`, i.e. left over from a previous wrap.
- More than `journal_iops_write_max` such slots: truncate none ("err on the side of caution").
- `vsr` slots *outside* the range are fine: they lie in `(op_checkpoint, op_max]` and do not affect certainty about the head.

Why this distinguishes torn from corrupt: an acked prepare has its redundant header written (4.2), so a slot whose redundant header still shows a previous-wrap op, beyond the highest op seen anywhere, with an unreadable prepare, is an append that never completed and was never acked. Truncating it (reserved, clean, not faulty) is equivalent to never having received it, so the replica may nack it. If instead the head is uncertain (any other kind of fault beyond `op_max`), the broken slot might be an acked op, so it must stay faulty. This is "only safe for the head" in the generalized sense: only for slots strictly beyond a *certain* `op_max`.

Why a single replica differs: there is nobody to repair from, so `@D`/`@E` trust the local prepare (`fix`), any remaining `vsr` slot is fatal, appends are strictly sequential (at most one torn slot, no gaps), and after recovery a solo replica increments its view "otherwise, two different versions of an op could exist within a single view (the former version truncated as a torn write)" (`replica.zig:865-878`), which lets standbys discard the truncated op.

Two observations from reading the current code (both inferred, not executed):
- Condition (2) means a torn append during the **first** WAL wrap (redundant header still reserved, case `@B`) is not truncated; the slot stays faulty. The source's stated reason is that a reserved redundant header can also be the residue of a slot found corrupt on a previous startup and rewritten by a neighbor's header-sector write (1449-1455). Release 0.16.0's `recover_torn_prepare` accepted the reserved case and only considered slot `op_max + 1`. For R=1 this would mean `error.WALCorrupt` rather than truncation in that window.
- `torn_prepares` asserts `slot(op_prepare_max) == slot(op_checkpoint)` in the `op_max == op_checkpoint > 0` branch (line 1395). That equality holds for `test_min` (20 + 4 + 8 = 32 slots) but with production constants `op_prepare_max - op_checkpoint = 960 + 32 + 16 = 1008`, not 1024. Do not bake that equality into a reimplementation.

### 4.7 Truncation of out-of-window and out-of-view ops ("cut")

- **`cut` (`@H`/`@I`/`@J`)**: "We allow replicas to write to a slot past prepare_max when the replica has already committed the prepare in that slot. On startup, we must truncate all these prepares so we can replay all prepares in the checkpoint." It is safe because the replica never sent `prepare_ok` beyond `op_prepare_ok_max` (`replica.zig:9662-9664`, 7123-7146). `op_prepare_max = checkpoint_after(op_checkpoint) + lsm_compaction_ops + 2 * pipeline_prepare_queue_max` (`vsr.zig:1652-1697`).
- **View-range drop** (there is no decision literally named `cut_view_range` at this commit; this post-pass in `recover_slots`, 1299-1332, plays that role). After the case decisions, each valid non-reserved header is checked against `superblock.view_headers().view_for_op(header.op, log_view)` (`vsr.zig:1490-1527`), which bounds the views an op could legitimately have given the durable view headers: below the oldest header `[0, oldest.view]`; above the newest `[log_view, log_view]`; exact match `[h.view, h.view]`; between two headers `[prev.view, next.view]`. Headers outside the range are removed via `remove_entry` (reserved, clean). Origin: "we join a view, write some prepares from the new view, and then crash before the view_durable_update() finished". Purpose: do not pick a truncated-but-not-yet-overwritten message as the head, and keep generated JoinView headers complete. Ordering matters: it must run after `op_prepare_max`/`op_max` are computed, after case decisions and `torn_prepares`, and before the `fix` writes.

### 4.8 Misdirected reads and writes

- Slot check and cluster check in `header_ok`; reserved headers carry `op = slot` so even an empty slot is self-locating.
- Cross-ring comparison: a lost or misdirected write leaves one stale but valid copy, surfacing as `@G`, `@K`, `@M`, `@N` or `@O`.
- At read time (`read_prepare_with_op_and_checksum_callback`, 832-932) the result is checked against the caller's expected `op` and `checksum`, plus cluster, body checksum and zero padding. Failure reasons are logged ("wrong cluster", "op changed during read", "checksum changed during read", "corrupt body after read", "corrupt sector padding").
- A misdirection by an exact multiple of `slot_count` ops is only caught by the checksum expectation.

### 4.9 How recovery output feeds the replica

`Replica.open` (`replica.zig:759-900`):

- All slots faulty: `error.WALInvalid`.
- **Head op.** If `log_view == view` (crashed in normal status): `op_head` = max op over non-reserved `journal.headers` (all asserted `<= op_prepare_max` and `view <= log_view`). Then the first valid superblock view header with `op <= op_prepare_max` and `op >= op_head` overrides it and is installed with `set_header_as_dirty` if missing. If `log_view < view` (crashed mid view change) the head comes from the durable view headers only, because the WAL may contain repairs from a view that was never made durable. Headers older than the head are deliberately not loaded from the superblock, to avoid resurrecting a previous-wrap header into a slot whose real prepare was corrupted and then nacking it (comment, 810-819).
- `commit_max = max(commit_max, op - pipeline_prepare_queue_max)`.
- **Solo**: `faulty.count > 0` gives `error.WALCorrupt` (test "R=1 can never recover if a WAL-prepare is corrupt", `replica_test.zig:154`; header-only corruption is repaired locally, line 171). A corrupt prepare discovered later on the commit path panics ("cannot recover corrupt prepare", `replica.zig:4700`).
- **Multi-replica, `log_view == view`**: if `op_head_certain()` then go to normal (or, if this replica was the primary, straight to a view change for `view + 1`); otherwise `status = .recovering_head`.

`op_head_certain` (`replica.zig:6986-7068`): true if `op == op_prepare_max`; false if the head's own slot is faulty, the `op_prepare_max` slot is faulty, or any faulty slot lies in `(op, op_prepare_max]`. Faults at or below the head are harmless for this purpose (" ✓ ✗ o : view change is safe"; " ✓ o ✗ : unsafe"). In `recovering_head` the replica does not repair (`repairs_allowed`, 8470) and waits for a `view` message from a primary to give it a trustworthy head; "the replica must refrain from repairing any faulty slots until the highest op is known", otherwise it "may nack an op that we shouldn't" or "replace a prepared op that we were guaranteeing for the primary, potentially forking the log".

**Repair.** `repair_prepares_between` / `repair_prepare` (`replica.zig:8250-8465`) walk ops and, for any slot that is dirty or has no header, first try the in-memory pipeline cache, else send `get_prepare {op, checksum}` (one peer at a time in normal status, broadcast when primary in a view change), bounded by free write IOPs and a repair budget. Dirty slots outside `[op_repair_min, op]` are simply `remove_entry`'d (`repair_clean_out_of_bound_prepares`): they are at or before the checkpoint, or were truncated. The responder (`on_get_prepare`, 3053-3122) serves from the pipeline or by consulting `prepare_checksums` rather than `headers`, so a slot that recovery marked faulty (in-memory header reserved) can still serve its physically intact prepare. It never replies negatively: per the docs, "we do not nack corrupt entries, since they *might* be the prepare being requested" (`docs/internals/vsr.md`, Repair WAL).

**Nacks** (`send_join_view`, `replica.zig:8839-8889`). For each header in the JoinView the replica sets a nack bit iff it is certain it never durably held that exact prepare:

1. no header for that op **and the slot is not faulty**;
2. it has the same header in memory, `dirty and !faulty` (received, never finished writing);
3. it has a *different* header for that op (safe even if faulty).

A `present` bit is set if the prepare is on disk (`prepare_inhabited` with matching checksum), being written, or in the pipeline cache. A faulty slot therefore contributes neither nack nor presence; the new primary must wait for more JoinViews or repair. `set_header_as_dirty` (1749-1786) preserves this: installing a header over a reserved slot keeps an existing faulty bit ("the WAL might have written/prepared this exact header before crashing; leave the entry marked faulty because we cannot safely nack it"), whereas installing over a *different* real header clears faulty ("the WAL definitely did not hold this exact header").

### 4.10 Journal API summary

- `write_prepare(callback, message)`: requires `has_header` and `writing == .none`; no-op success if the slot is already clean; flow as in 4.2. Every completion stage re-checks that the slot still holds the same header ("entry changed while writing sectors/headers", "entry removed then added while writing headers", "entry changed twice") and reports `null` rather than marking clean. If a prepare write lands for a header that is no longer current, the slot is re-marked dirty.
- `read_prepare(callback, {op, checksum})`: needs an exact in-memory header and matching `prepare_checksums`, else `null`. Header-only messages are served from memory. Reads `sector_ceil(size)` bytes. On any validation failure, if the slot still holds that op+checksum: `faulty.set`, `dirty.set`, callback `null`; repair then re-fetches it. `read_prepare_with_op_and_checksum` is the variant that does not need an in-memory header (used for `get_prepare`).
- `set_header_as_dirty(header)`: install or keep a header, set dirty, faulty handling as above.
- `remove_entries_from(op_min)`: for every slot with `header.op >= op_min` (reserved included, since reserved headers carry `op = slot` and may be faulty from `@K`), `remove_entry`: both in-memory headers become reserved, dirty and faulty cleared. **Nothing is written to disk**; the old prepare and redundant header stay until overwritten ("TODO Explore scenarios where the data on disk may resurface after a crash"). Called from `set_op_and_commit_max` (`replica.zig:9694`) when a view change discards uncommitted ops. Truncation is safe there because the op is `>= commit_max` and within one pipeline of the old head (asserts at 9665-9668).
- Lookups: `header_for_op` (whatever is in the slot, possibly another wrap's op), `header_with_op`, `header_with_op_and_checksum`, `previous_entry`/`next_entry`, `has_header`, `has_prepare` (header matches and slot clean), `has_dirty`, `op_maximum`, `copy_latest_headers_between` (newest first), `writing`.
- **Hash chain.** The journal never verifies `parent` during recovery. It only offers `find_latest_headers_break_between(op_min, op_max)` (622-720), which reports the highest gap or chain break and encodes the rule "op numbers in the same view must be connected" as `unreachable`. Enforcement is in the replica: `panic_if_hash_chain_would_break_in_the_same_view` on append (`replica.zig:2204-2208`), `repair_header` refusing headers that would advance or change the head or not connect to it (7829-7961), and a `valid_hash_chain` check before committing across views (4651-4658).

### 4.11 JournalChecker (simulator)

`src/testing/cluster/journal_checker.zig`, called from `Cluster.tick` for each live replica with probability 1/100 per tick (`src/testing/cluster.zig:589-592`). Only when no journal write is executing:

- For every slot that is not dirty and whose in-memory header is not reserved: the on-disk redundant header's checksum is nonzero and equals both the on-disk prepare's header checksum and the in-memory header's checksum. (Reserved in-memory headers are skipped because truncation does not touch the disk.)
- For every slot: the prepare's trailing sector padding on disk is zero.

Related fault-model guards I noticed (the simulator fork owns the details): the test storage will not corrupt a prepare read when that slot's header write was misdirected, "to avoid a double-fault which the journal interprets as a torn prepare", and will not misdirect a prepare whose header mismatches or is corrupt (`src/testing/storage.zig:528-570`); on restart the VOPR clears faults on the header sectors because `test_min` has only two (`src/vopr.zig:1596-1612`).

### 4.12 Not verified / discrepancies

- **Zeroed redundant headers.** The doc comment (1176-1178), `header_sector` (2154-2159) and the VOPR (`vopr.zig:1617-1621`) all say faulty slots have their redundant header written with an invalid (zero) checksum so they stay faulty across a second crash. In 0.16.0 `header_sector` did this directly from the faulty bit. At this commit the only place a zero checksum is assigned is the end of `recover_slot` (1572-1575), and `recover_slots` then bulk-copies `headers` over `headers_redundant` (1292-1297) while `recover_done` asserts the two arrays are equal with valid checksums. By reading alone I could not find a path where a zeroed header is actually written at this commit; the comment in `torn_prepares` (1449-1455) instead describes a *reserved* header being written for such a slot. I did not run anything to confirm.
- `O_DSYNC` is taken from a comment in `src/storage.zig`, not from the IO layer's open flags.
- The format-time comment says the test storage checks "never write the redundant header before the prepare"; I did not locate that check in `src/testing/storage.zig`.
- `config.zig:177` still refers to `recover_torn_prepare()`; the function is now `torn_prepares()`.

### Lessons / traps for a Rust reimplementation (WAL)

1. Keep two independently written copies of each header and make recovery a pure function `(redundant header, prepare header, op_max, op_prepare_max) -> decision`, expressed as a table. Port the exhaustiveness test: every one of the 2^11 inputs matches exactly one row or an assertion.
2. Model three states per slot, not two: clean, dirty ("certainly never held"), faulty ("may have held"). Only the first two may nack. Never put a suspect header into the in-memory index while marking the slot faulty.
3. Fix the write order (prepare, then header sector, then ack) and make the header-sector image come from an "on disk" shadow array, never from the live index.
4. Header sectors are shared by 16 slots: serialize overlapping sector writes FIFO, snapshot the sector per write, and size the WAL so one crash cannot tear every header sector (at least two sectors; more sectors than concurrent writes).
5. Decide explicitly what a faulty slot's redundant header looks like on disk after a neighbor's sector write, and test a second crash at that point. This is the area where the current source and its comments disagree.
6. Truncate a torn append only when the head is certain; otherwise park the replica in a "recovering head" state that does not vote, nack, or repair until a primary supplies a head.
7. Keep a per-slot "what is physically on disk" checksum separate from the log index, so a replica can serve repairs for prepares it has logically truncated or marked faulty.
8. Validate the checksum before reading any other header field; give empty slots a self-locating header (`op = slot`); zero and verify sector padding.
9. Single-replica mode needs its own column in the table, strictly sequential appends, a view bump after every recovery, and a clear fatal error when a prepare is unrecoverable.
10. In-memory truncation does not erase the disk. Recovery must tolerate truncated ops resurfacing, which is what the view-range drop and the `op_prepare_max` cut are for.
11. Do not assume `slot(op_prepare_max) == slot(op_checkpoint)`; it depends on how the checkpoint interval rounds against the compaction bar.
12. The hash chain is a replica-level commit precondition, not a WAL recovery input. Recovery works slot by slot.


# Fork A notes: superblock, checkpoints, grid, free set, grid repair, state sync, clock

Source: TigerBeetle HEAD clone at commit `a2a3c7064519e08233abe26c54a43fca2f9ad9bc` (0.18.0 era). All paths are relative to the repo root. Read-only; nothing was built or run.

Naming at HEAD differs from the VRR paper: `exit_view` (start_view_change), `join_view` / "JV" (do_view_change), `get_view` (request_start_view), `view` (start_view), and `get_headers` / `get_prepare` / `get_reply` / `get_blocks` for repair requests (`src/vsr.zig` `Command`).

Anything marked "(inferred)" was derived from code I read rather than stated by it. Constants are given as production / `test_min` where they differ (`src/config.zig` `configs.default_production`, `configs.test_min`).

## 5. Durable state: superblock, checkpoints, grid, free set

### 5.1 Data file zones

`src/vsr.zig` `Zone`, in file order:

| Zone | Size | Notes |
|---|---|---|
| `superblock` | `superblock_copy_size * superblock_copies` | 4 copies |
| `wal_headers` | `journal_slot_count * 256` | redundant header ring |
| `wal_prepares` | `journal_slot_count * message_size_max` | prepare ring |
| `client_replies` | `clients_max * message_size_max` | one slot per client session |
| `grid_padding` | aligns grid start to `block_size` | |
| `grid` | elastic, `block_size` blocks | only zone written without dsync (`Zone.dsync`) |

Because the grid is not dsync, `Forest.compact_finish` calls `storage.flush_sectors` on the last beat of every bar, and the superblock (dsync) is only written after that.

### 5.2 Superblock layout and header

`src/vsr/superblock.zig`. `superblock_copies = 4` (allowed: 4, 6, 8). `superblock_copy_size = @sizeOf(SuperBlockHeader) + superblock_copy_padding`; copy `i` lives at offset `superblock_copy_size * i`. The padding is reserved for future growth of the pipeline.

`SuperBlockHeader` fields:

| Field | Purpose |
|---|---|
| `checksum`, `checksum_padding` | Covers everything except `checksum`, `checksum_padding`, `copy` (`calculate_checksum`). All copies therefore share one checksum. |
| `copy: u16` | Copy index, outside the checksum, used to detect misdirected writes. |
| `version: u16` | `SuperBlockVersion` (2 in releases, 0 in dev builds). |
| `release_format` | Release that formatted the file. |
| `sequence: u64` | Monotonic version of the superblock. |
| `cluster: u128` | |
| `parent: u128` | Checksum of the previous superblock (hash chain). |
| `vsr_state: VSRState` | 2048 bytes, see 5.3. |
| `flags`, `view_headers_count: u32`, `reserved` | |
| `view_headers_all: [view_headers_max]Header.Prepare` | At offset `sector_size`. Ordered high op to low op. |

`view_headers_max = view_change_headers_suffix_max + 2` = 11 / 7. Meaning of the view headers: when `log_view < view` they are the headers this replica will send in its `join_view` (JV); when `log_view == view` they are the `view` message headers. Persisting them is what lets a replica restart mid view change without regressing what it promised.

`@sizeOf(SuperBlockHeader)` is a sector multiple of at least two sectors; with production constants it is 8192 bytes (inferred from 4096 + 11 * 256 rounded up).

### 5.3 VSRState and CheckpointState

`VSRState` (`src/vsr/superblock.zig`):

| Field | Why it exists |
|---|---|
| `checkpoint: CheckpointState` | Root pointer of all checkpointed state. |
| `replica_id: u128`, `members: [12]u128`, `replica_count: u8` | Identity and membership; immutable across updates. |
| `commit_max: u64` | Highest op known committed; lets a restarted replica know how far behind it is. |
| `sync_op_min`, `sync_op_max: u64` | Nonzero `sync_op_max` means a state sync is incomplete: grid blocks and client replies written by commits in `[sync_op_min, sync_op_max]` may be missing and must be repaired. |
| `sync_view: u32` | Unused, always 0. |
| `log_view: u32` | Last view in which the replica's log was known canonical (status was normal). Must never regress or logs can fork (long comment above `log_view_durable` in `src/vsr/replica.zig`). |
| `view: u32` | Highest view the replica has participated in. |

`CheckpointState` (1024 bytes; comment: content is deterministic for the corresponding checkpoint; sent verbatim in the `view` message body):

| Field | Why |
|---|---|
| `header: Header.Prepare` | The last prepare included in the checkpoint; `header.op` is `op_checkpoint`. |
| `free_set_blocks_acquired_last_block_checksum/address`, `..._size`, `..._checksum` | Reference to the acquired-bitset trailer: last block of the chain, byte size, and whole-trailer checksum (defense in depth). |
| `free_set_blocks_released_*` | Same, for the released bitset. |
| `client_sessions_last_block_checksum/address`, `client_sessions_size`, `client_sessions_checksum` | Client sessions trailer. |
| `manifest_oldest_checksum/address`, `manifest_newest_checksum/address`, `manifest_block_count` | Ends of the manifest log block chain. |
| `snapshots_block_checksum/address` | Unused, 0. |
| `parent_checkpoint_id`, `grandparent_checkpoint_id` | Ids of the two preceding checkpoints. |
| `storage_size: u64` | `data_file_size_min + highest_address_acquired * block_size`. |
| `release` | Release that executes all prepares between this checkpoint and the next trigger. |

`checkpoint_id() = vsr.checksum(asBytes(&vsr_state.checkpoint))`.

### 5.4 Write, verify, open, torn writes, repair

Quorums (`src/vsr/superblock_quorums.zig` `Threshold.count`): `verify` = 3/4 (4/6, 5/8), `open` = 2/4 (3/6, 4/8). Stated rule: write quorum plus read quorum equals copies + 1. Open tolerates two bad copies because copies are overwritten in place.

Write protocol (`SuperBlock.checkpoint`, `SuperBlock.view_change`, shared write path):

1. `staging = working` with `sequence + 1` and `parent = working.checksum`, then apply the update.
2. Write copies 0..3 one at a time.
3. `read_working(.verify)`: read all copies back, sequentially (not in parallel, to avoid correlated faults), and require 3/4 whose checksum equals `staging`. Otherwise `@panic("superblock failed verification after writing")`.
4. `working = staging`, run the callback.

`format` writes sequence 1 (sequence 0 is never on disk). Only `format`, `open`, `checkpoint`, `view_change` touch the superblock. At most one update may queue behind the in-flight one, and only a checkpoint behind a view change or the reverse.

Open (`Quorums.working(copies, .open)`): keep copies with valid checksums, group by checksum, count distinct `copy` indexes per group (duplicates from misdirected writes count once), sort by valid-quorum, then sequence, then count, then checksum, all descending. Errors, each a panic at open: `NotFound`, `QuorumLost`, `Fork` (same sequence, different checksum), `ParentSkipped` (sequence gap larger than 1 between quorums), `ParentNotConnected` (`a.sequence + 1 == b.sequence` but `a.checksum != b.parent`), `VSRStateNotMonotonic`.

What a torn write looks like: a crash during step 2 leaves some copies at sequence N+1 and the rest at N. If at least 2 copies reached N+1, N+1 wins on open; if only 1 did, N wins (3 copies) and the lone N+1 copy is garbage to be overwritten (inferred from thresholds and sort order). Either outcome is safe because the update was never acknowledged upward before step 3.

Repair on open (`RepairIterator.next`): rewrite, in priority order, (a) slots whose copy index was not found anywhere, (b) slots that are empty but whose copy exists elsewhere, (c) slots holding a misdirected duplicate. It never writes a copy into a non-home slot. The comments walk through why naive repair of a misdirected copy can make the superblock backtrack.

### 5.5 Update kinds and invariants

| Kind | Changes | Asserts |
|---|---|---|
| `checkpoint(update)` | `checkpoint` (new header, trailer and manifest references, `storage_size`, `release`), `parent_checkpoint_id = old checkpoint_id()`, `grandparent = old parent`, `commit_max`, `sync_op_min/max`, optionally view, log_view and view headers | `update.header.op >` current checkpoint op; release non-decreasing |
| `view_change(update)` | `commit_max`, `log_view`, `view`, view headers, optional `sync_checkpoint { checkpoint, sync_op_min, sync_op_max }` | view, log_view, commit_max non-decreasing; at least one of view / log_view advances or `sync_checkpoint != null`; headers are `view` headers iff `log_view == view` |

State sync has no update kind of its own: it rides on `view_change` with `sync_checkpoint` set, which asserts the new checkpoint op is greater than the staged one and checks the parent/grandparent id chain when the target is the next or next-next checkpoint.

`VSRState.monotonic(old, new)`: same checkpoint op implies byte-identical `CheckpointState`; a different op implies a different header checksum and parent id; `replica_id`, `replica_count`, `members` unchanged; checkpoint op, `view`, `log_view`, `commit_max` never decrease. Header-comment invariants: `checkpoint.header.op <= commit_max`, `log_view <= view`, `sync_op_min <= sync_op_max`.

`VSRState.op_compacted(op)`: `checkpoint.header.op > 0 and op <= trigger_for_checkpoint(checkpoint.header.op)`. Compaction runs one bar ahead of the checkpoint, so these ops must not be compacted again on replay.

### 5.6 Checkpoint trailers

`src/vsr/checkpoint_trailer.zig`. A trailer is a byte buffer chunked into grid blocks of `chunk_size_max = block_size - @sizeOf(Header)` bytes. Blocks form a reverse-linked list: each block's header metadata (`schema.TrailerNode`) holds `previous_trailer_block_checksum/address`. They are written first to last and read back last to first, starting from the superblock's last-block reference. `TrailerType` is `free_set` or `client_sessions`.

- Free set: two trailers, each the EWAH-compressed (`vsr.ewah(u64)`) words of `blocks_acquired` and `blocks_released` (`FreeSet.encode_chunks`). Trailing zero runs are not encoded, so the encoding does not depend on `--limit-storage`.
- Client sessions: exactly one block. `ClientSessions.encode_size` = `clients_max` reply headers (256 bytes each) followed by `clients_max` `u64` session numbers (`src/vsr/client_sessions.zig`). Reply bodies are not here; they live in the `client_replies` zone.

Self-reference trap: the free-set trailer blocks are allocated after the bitset is encoded, so they are not in the encoded acquired set. On open, `FreeSet.open` patches them in with `mark_released` (set acquired, and queue for release). After each checkpoint the old trailer blocks are released (`Grid.mark_checkpoint_not_durable` for free-set trailers; `commit_checkpoint_superblock_callback` and `client_sessions_open_callback` for client sessions).

### 5.7 Grid

`src/vsr/grid.zig`.

- `block_size` = 512 KiB / 4096. Addresses are 1-based `u64`; `block_offset(address) = (address - 1) * block_size`. A block is always referenced as `BlockReference { checksum: u128, address: u64 }`; the checksum is stored in the parent, which catches misdirected and stale writes.
- Block header (`Header.Block`, `src/vsr/message_header.zig`): `checksum`, `checksum_body`, `cluster`, `size`, `release`, `command = .block`, `metadata_bytes[96]` (per `block_type` schema), `address`, `snapshot`, `block_type`. `view` and `replica` are always 0: nothing replica-specific may enter a block.
- Write path: `create_block` (new block; asserts the address is acquired and not being read) or `repair_block`. Writes `block[0..sector_ceil(header.size)]` with zeroed padding. A non-repair `Write` records the checkpoint id and asserts it is unchanged on completion.
- Read path: pending writes first, then join an in-flight read of the same address and checksum, then cache (resolved on the next tick), then storage. `read_block_validate` order: header checksum, `command == .block`, body checksum, expected checksum, zero padding.
- Two read flavors (`ReadBlockCallback`): `from_local_storage` returns a `ReadBlockResult` (used by the scrubber and when serving `get_blocks`); `from_local_or_global_storage` never fails: on a fault the read is parked on `read_global_queue` and completes only when the block arrives from a peer (`fulfill_block`). Compaction, prefetch, manifest and trailer reads use the second kind, so a local fault stalls the commit pipeline rather than crashing it.
- Cache: set-associative, 16 ways (`set_associative_cache_ways`), keyed by address, default 1 GiB; `CachePolicy` is `cache_bypass`, `cache_lookup_no_admit`, or `cache_lookup_and_admit`.
- `Grid.open`: open both free-set trailers, `free_set.open`, assert `storage_size` matches.
- `Grid.checkpoint`: requires zero reservations and an empty `read_global_queue`; encodes the free set and writes the two trailers.
- `Grid.cancel` (state sync only): drops queued reads and writes, resets `blocks_missing`, waits for in-flight IOPs.
- `Grid.reserve` failure is fatal: `vsr.fatal(.storage_size_would_exceed_limit)`.

"Checkpoint durable" is a pure function of ops (`src/vsr.zig`): `Checkpoint.durable(checkpoint, commit) = commit > trigger_for_checkpoint(checkpoint) + pipeline_prepare_queue_max` (true for checkpoint 0). It means a replication quorum holds the checkpoint, because replicas on the previous checkpoint refuse to `prepare_ok` ops beyond that bound (5.9). Locally, `Grid.checkpoint_durable` is invoked from `Replica.commit_checkpoint_durable` when `commit_min == trigger + pipeline_prepare_queue_max + 1`; it aborts repairs of blocks about to be freed (`blocks_missing.checkpoint_durable_commence`), then calls `free_set.mark_checkpoint_durable()`.

### 5.8 FreeSet

`src/vsr/free_set.zig`.

| Member | Meaning |
|---|---|
| `blocks_acquired` | Bit per block: allocated (or reserved-and-acquired). |
| `blocks_released` | Released, awaiting the next checkpoint durability event. Encoded into the checkpoint. |
| `blocks_released_prior_checkpoint_durability` | Hash set of releases made while the current checkpoint is not yet durable; in memory only. |
| `index` | Bit per shard of `shard_bits = 4096` blocks; set when the shard is full. Search accelerator only. |
| `checkpoint_durable: bool`, `opened: bool` | |
| `reservation_blocks`, `reservation_count`, `reservation_state`, `reservation_session` | Reservation cursor and lifecycle. |

Reservations, and why they give determinism:

- `reserve(n)` scans forward from `reservation_blocks` and returns a `Reservation { block_base, block_count, session }` spanning exactly `n` free blocks (plus any interleaved acquired ones). Reservations are handed out in call order and never overlap.
- `acquire(reservation)` returns the first free address inside that reservation only.
- `forfeit(reservation)` gives it back; when every outstanding reservation is forfeited, `reservation_blocks` resets to 0 and the session increments. No `reserve` is allowed between the first forfeit and the last.
- The rule this enforces: reserve calls happen in a deterministic order with an upper bound computed from deterministic inputs; after that, concurrent jobs can acquire in any interleaving and each still gets the same addresses on every replica, because each draws only from its own range.

Release lifecycle:

- `release(address)`: if `checkpoint_durable`, set the bit in `blocks_released`; otherwise insert into `blocks_released_prior_checkpoint_durability`. The block stays acquired.
- `mark_checkpoint_not_durable()`: called right after checkpoint N's superblock is written.
- `mark_checkpoint_durable()`: free every block in `blocks_released` (clear acquired and released bits), then move the prior-durability set into `blocks_released`.

Resulting timeline: a block released during the interval that ends in checkpoint N is recorded as released in checkpoint N's trailer and becomes allocatable only when checkpoint N is durable, that is when `commit_min` reaches `trigger(N) + pipeline_prepare_queue_max + 1`. A block released after checkpoint N is written but before N is durable waits one full checkpoint longer.

Why not earlier, two reasons:

1. Local crash safety: until checkpoint N's superblock is written, a crash recovers from N-1, whose manifest still references the block.
2. Cluster durability: until N is durable on a quorum, replicas still on N-1 may need that block from a peer. Overwriting it would reduce the copies of N-1's state below a commit quorum (comment on `blocks_released`).

Every replica frees at the same op, which the comment in `commit_checkpoint_durable` calls crucial for storage determinism. `encode` asserts `checkpoint_durable` and zero reservations. `free_set.open` leaves `checkpoint_durable = false`, so recovery passes through the same durability event during replay.

### 5.9 Checkpoint arithmetic

`src/constants.zig`:

```zig
pub const vsr_checkpoint_ops = journal_slot_count -
    lsm_compaction_ops -
    lsm_compaction_ops * stdx.div_ceil(pipeline_prepare_queue_max * 2, lsm_compaction_ops);
```

| Constant | Production | `test_min` |
|---|---|---|
| `journal_slot_count` | 1024 | 32 |
| `pipeline_prepare_queue_max` | 8 | 4 |
| `lsm_compaction_ops` (bar) | 32 | 4 |
| `vsr_checkpoint_ops` | 960 | 20 |
| `block_size` | 512 KiB | 4096 |
| `message_size_max` | 1 MiB | 4096 |
| `clients_max` | 64 | 7 |

Functions (`src/vsr.zig` `Checkpoint`), with C the checkpoint op and T its trigger:

| Function | Formula |
|---|---|
| `checkpoint_after(C)` | `vsr_checkpoint_ops - 1` if C = 0, else `C + vsr_checkpoint_ops` |
| `trigger_for_checkpoint(C)` | `C + lsm_compaction_ops` (null for 0) |
| `durable(C, commit)` | `commit > T + pipeline_prepare_queue_max` |
| `prepare_max_for_checkpoint(C)` | `T + 2 * pipeline_prepare_queue_max` |
| `valid(op)` | `op == 0 or (op + 1) % lsm_compaction_ops == 0` |

Replica-side (`src/vsr/replica.zig`): `op_checkpoint_next() = checkpoint_after(op_checkpoint())`; `op_checkpoint_next_trigger()`; `op_prepare_max() = prepare_max_for_checkpoint(op_checkpoint_next())` (highest op the replica may write to its WAL); `op_prepare_ok_max() = op_checkpoint_next_trigger() + pipeline_prepare_queue_max` (highest op it may acknowledge).

Why the checkpoint is a bar behind the trigger: when op T commits, the last bar `(C, T]` exists only in the in-memory immutable table. So the superblock records `op_checkpoint = T - lsm_compaction_ops`, and recovery replays `(C, T]` from the WAL without compacting.

Why the WAL cannot wrap over ops that are still needed. The margin in `vsr_checkpoint_ops` is one bar plus `2 * pipeline_prepare_queue_max`, rounded up to bars:

- A replica on the previous checkpoint acknowledges nothing above `T + pipeline_prepare_queue_max`. So committing `T + pipeline_prepare_queue_max + 1` proves a replication quorum is on checkpoint C. That is exactly `durable`.
- The primary can run at most `pipeline_prepare_queue_max` ops ahead of its commit, so before durability nobody prepares beyond `T + 2 * pipeline_prepare_queue_max`, and a lagging backup can store all of those without wrapping (`op_prepare_max`).
- The first op that wraps onto the previous interval's slots can only be prepared after durability (worked example below).

Worked example, production numbers:

| Event | Op |
|---|---|
| Checkpoints | 959, 1919, 2879, ... |
| Triggers | 991, 1951, 2911, ... |
| Replica on checkpoint 0: `op_prepare_ok_max` / `op_prepare_max` | 999 / 1007 |
| Checkpoint 959 durable when `commit_min` reaches | 1000 |
| Replica on checkpoint 959: `op_prepare_ok_max` / `op_prepare_max` | 1959 / 1967 |

Op 1024 is the first to reuse slot 0. Preparing it requires commit >= 1016 > 999, hence a quorum on checkpoint 959. At the far end, op 1967 lands in slot 943, overwriting op 943, which is below 959. `test_min`: checkpoints 19, 39, 59; triggers 23, 43, 63; durable at commit 28; a replica on checkpoint 0 acknowledges up to 27 and stores up to 31 (matches the example in the `op_prepare_ok_max` comment).

Sequence within one interval (production, replica holding checkpoint 959):

1. Each commit runs the stages of `commit_dispatch`: `start`, `check_prepare`, `prefetch`, `stall`, `reply_setup`, `execute`, `checkpoint_durable`, `compact`, `checkpoint_data`, `checkpoint_superblock`.
2. Ops 992..1951 commit; each is followed by `state_machine.compact(op)`.
3. At op 1000, `commit_checkpoint_durable` fires: checkpoint 959 is durable, blocks released before 959 are freed, the primary broadcasts a `view` so laggards can sync.
4. At op 1951 (last beat of bar 1920..1951): execute, compact (swap mutable into immutable, flush sectors), then `commit_checkpoint_data` runs in parallel the AOF, forest, client sessions and client replies checkpoints, with `grid.checkpoint` started last because it encodes the free set after everyone else has acquired their blocks.
5. `commit_checkpoint_superblock`: `superblock.checkpoint` with `header = prepare 1919`; `sync_op_min/max` become 0 if `sync_content_done()`.
6. Callback: `grid.mark_checkpoint_not_durable()`, release client-session trailer blocks, send the `prepare_ok`s that were withheld (`send_prepare_oks_after_checkpoint`, from `trigger + pipeline_prepare_queue_max + 1`).

Upgrade checkpoints (`release_for_next_checkpoint`, `execute_op_upgrade`, `ignore_request_message_upgrade`, `release_transition`): `CheckpointState.release` advances only if every one of the `lsm_compaction_ops` ops in the last bar before the trigger is `operation = upgrade`. While `upgrade_release` is set the primary ignores normal requests and keeps preparing upgrade no-ops. After the superblock is written with the higher release, the replica executes into the new binary, which replays that bar as no-ops. This guarantees no op is executed by two different releases. The upgrade quorum is all replicas (`quorums().upgrade = replica_count`).

### 5.10 Compaction determinism and the checkpoint id

`src/lsm/forest.zig`, `src/lsm/compaction.zig`, `src/lsm/manifest_log.zig`, `docs/internals/lsm.md`.

- `Forest.compact(callback, op)` runs after every commit. `beat = op % lsm_compaction_ops`; a half-bar is `lsm_compaction_ops / 2` beats (16 / 2).
- `level_active`: in the first half-bar, compactions into odd levels run (0 to 1, 2 to 3, 4 to 5); in the second, immutable table to level 0 and 1 to 2, 3 to 4, 5 to 6. Adjacent compactions never share a level.
- First beat of a half-bar: `Compaction.half_bar_commence(op)` per (level, tree) in fixed order picks the input table and overlapping range and sets the half-bar quota to the total input value count. A disjoint table is moved, not merged (`move_table`).
- Every beat: `trees_beat_input_size = div_ceil(remaining half-bar input bytes, beats_remaining)`; compactions run one at a time in fixed (level, tree) order until the beat quota is consumed. Work is metered in values, never in time or IO completions.
- Grid reservation is per beat: `compact_trees_reserve_grid_blocks` computes an upper bound on value and index blocks for the beat, makes one `grid.reserve` for the whole forest, and forfeits it when the beat's quota is consumed. Output addresses are acquired when the table builder fills a block (`flush_table_builder_blocks`), so acquisition order follows merge output order, not IO timing (inferred).
- Manifest log compaction runs only on the last beat of each half-bar, reserves before the trees (`compact_blocks + half_bar_append_blocks_max`), and forfeits in `compact_end`.
- `half_bar_complete()` applies all manifest changes at the last beat of the half-bar in fixed order. Comment: so that manifest log updates are ordered deterministically and are not visible until the blocks are on disk.
- Snapshots: `compaction_op_min(op) = op - op % half_bar`; output tables get `snapshot_min = op_min + half_bar`; inputs get `snapshot_max = snapshot_min - 1`.
- Last beat of the bar: `swap_mutable_and_immutable`.
- Never compact twice: `compact` is a no-op for `op < lsm_compaction_ops` and for `op_compacted(op)`.
- Recovery trap handled in `half_bar_commence`: the "skip flushing the immutable table and absorb the mutable table into it" optimization is disabled for the last half-bar before a trigger, because recovery replays only that bar and must rebuild the identical immutable table (`Forest.checkpoint` asserts `!absorbed`).

Why checkpoints are byte-identical across replicas: same ops in the same order, same compaction work per op, same addresses (reservations), same free times (durability is an op), no replica identity or wall-clock data in blocks. The checkpoint id hashes the `CheckpointState`, which transitively covers every reachable block through checksums. Ids travel on `prepare`, `prepare_ok`, `commit`, `ping`; a mismatch panics (`docs/internals/sync.md`).

`checkpoint_id_for_op(op)`: a prepare with op in `(C_k, C_k+1]` carries the id of checkpoint `C_k-1` (the parent of the checkpoint current at that op), since `C_k` is not written until its trigger. So divergence in a checkpoint is detected one interval later (derived from the case analysis in that function).

## 6b. Grid repair and state sync

### 6b.1 Grid repair

Messages: `get_blocks` (body: up to `grid_repair_request_max = 4` `vsr.BlockRequest { block_address, block_checksum }`) and `block` (the whole block, sent as a message whose header is the block header).

Requester (`on_grid_repair_timeout`, `send_get_blocks`, `on_block` in `src/vsr/replica.zig`):

- `grid_repair_timeout` fires every 100 ms with jitter. It reaps expired budget and, if `RepairBudgetGrid.next_destination` yields a replica, sends one `get_blocks`.
- Requests are filled first from `grid.read_global_queue` (stalled coherent reads; the queue is rotated to avoid starvation), limited so that at least half the message is left for `blocks_missing.faulty_blocks` when there are any, then from `faulty_blocks` starting at a random index. Only faults in state `waiting` are requested (`fault_at_index`).
- `on_block`: ignore if the block's release is newer than ours or the grid is canceling. `grid.fulfill_block` completes stalled reads; a fulfilled block is also enqueued into `blocks_missing` so it gets written. If `repair_block_waiting(address, checksum)`, write it via `grid.repair_block`. Credit the budget and immediately try to send another `get_blocks`.

Responder (`on_get_blocks`): standbys and a canceling grid ignore it. For each request not already being read for that destination, take one of `grid_repair_reads_max = 4` read slots (drop the rest if none), read with `from_local_storage` and `cache_lookup_no_admit`, and reply only if valid. There is no negative reply. Both sides work while the grid is still opening at startup (`docs/internals/vsr.md`).

`GridBlocksMissing` (`src/vsr/grid_blocks_missing.zig`): `FaultyBlock { checksum, cause, state }` with states `waiting`, `writing`, `aborting`; causes are single-block repair (optionally `scrub`) or table sync. Capacity is `grid_missing_blocks_max + grid_missing_tables_max * lsm_table_value_blocks_max`, with `grid_missing_tables_max` = 6 / 2. A table is synced as a unit (`sync_table`): the index block first, then every value block it names.

### 6b.2 Repair budget

`src/vsr/repair_budget.zig` `RepairBudgetGrid`: each remote replica has a budget of `grid_repair_request_max + 1 = 5` outstanding blocks. `next_destination` picks a random replica with at least 4 available. A requested block is not re-requested for `duration_retry = 100 ms`; unanswered requests return their budget after `duration_expiry = 250 ms`. When a block arrives, every replica it was requested from is credited, because the block cannot say who sent it (a replica id in the block would break storage determinism).

### 6b.3 Scrubber

`src/vsr/grid_scrubber.zig`, `on_grid_scrub_timeout`. Tour order: table index blocks, each table's value blocks, manifest log blocks, free-set acquired trailer, free-set released trailer, client sessions trailer, then wrap. Each replica starts at a random table so replicas scrub different regions first. Reads use `from_local_storage` and `cache_bypass`.

Cadence: the timeout interval is recomputed each firing as

`clamp(grid_scrubber_cycle_ticks / max(1, free_set.count_acquired()) * grid_scrubber_reads_max, interval_ticks_min, interval_ticks_max)`

with cycle 180 days / 1 hour, interval bounds 50 ms to 10 s, `grid_scrubber_reads_max` 1 / 2. Faults found are enqueued as `repair_block(..., .{ .scrub = true })`, at most `grid_scrubber_writes_max = 1` outstanding. The scrubber is idle while syncing or while table sync is pending. Reads in flight when a checkpoint becomes durable are marked `released` if their block is about to be freed, so no read barrier is needed.

### 6b.4 When to sync

Trigger point: `on_view_set_checkpoint` when a `view` message arrives (body = `CheckpointState` followed by headers; `view_message_checkpoint`). Rules, in order:

1. If `!Checkpoint.durable(op_checkpoint_next(), message.commit_max)`: do not sync. The cluster has not durably passed our next checkpoint; WAL repair suffices.
2. `far_behind = durable(op_checkpoint_next() + vsr_checkpoint_ops, commit_max)`: the cluster is at least two checkpoints ahead. Sync, because the WAL ops we need may be gone.
3. Otherwise sync only if `syncing == .idle and repair_stuck()`.
4. If a local checkpoint is in progress (`commit_stage` is `checkpoint_data` or `checkpoint_superblock`) or a sync is already updating, let that finish first.

`repair_stuck()`: `commit_min != commit_max`, not checkpointing, not `recovering_head`, no commit progress across the last `repair_sync_timeout` period (5 s; tracked in `sync_wal_repair_progress`), and at least one of: hash chain invalid, next prepare missing or dirty, `grid.read_global_queue` non-empty. When stuck, `on_repair_sync_timeout` sends `get_view` to the primary to solicit a fresh `view`. The primary also broadcasts `view` when a checkpoint becomes durable.

### 6b.5 Stages

`src/vsr/sync.zig` `Stage`: `idle`, `canceling_commit`, `canceling_grid`, `updating_checkpoint: CheckpointState`. Valid transitions: idle to canceling_commit or canceling_grid; canceling_commit to canceling_grid; canceling_grid to updating_checkpoint; updating_checkpoint to idle.

1. `sync_start_from_committing`: if the commit stage is uninterruptible (`start`, `reply_setup`, `stall`, `checkpoint_durable`, `checkpoint_data`, `checkpoint_superblock`) enter `canceling_commit` and wait; if `idle`, `prefetch` or `compact`, enter `canceling_grid` directly. The `view` message is held in `sync_view`.
2. `canceling_grid`: `grid.cancel()` and `blocks_missing.sync_jump_commence()` (drops single-block faults, parks in-progress table syncs).
3. `sync_cancel_grid_callback`: `commit_dispatch_cancel()`, release outstanding `get_blocks` reads and repair writes, cancel the scrubber, enter `updating_checkpoint`, then call `on_view_set_journal(message)`.
4. `sync_superblock_update_start`: `state_machine.reset()`, `free_set.reset()`, reset both free-set trailers, client sessions and its trailer, clear `client_replies.faulty`, set `sync_tables_op_range` if not already set.
5. `on_view_set_journal` installs the log suffix and calls `view_durable_update()`, which issues `superblock.view_change` with `sync_checkpoint`.
6. `view_durable_update_callback`: when the working checkpoint op equals the target, `sync_superblock_update_finish` returns to `idle`, refills both repair budgets, and calls `grid.open(grid_open_callback)`. If the new checkpoint's release is higher, it calls `release_transition` instead.
7. Open chain: free-set trailers, then client sessions trailer, then `state_machine.open` (manifest log). All these reads are `from_local_or_global_storage`, so missing blocks are fetched through ordinary grid repair. Then `state_machine_open_callback` calls `sync_content()` and resumes committing.

Sync is "done" for consensus purposes at step 6; everything after is background repair.

### 6b.6 What the superblock records

In `view_durable_update`:

- `sync_op_min = trigger_for_checkpoint(old op_checkpoint) + 1` (0 if the old checkpoint is 0); if a sync was already incomplete (`sync_op_max > 0`), take the min with the old value.
- `sync_op_max = trigger_for_checkpoint(target checkpoint op)`.

The range is in trigger terms because grid writes up to the trigger op belong to the checkpoint. It is cleared to 0/0 only at the next local checkpoint and only if `sync_content_done()` (`commit_checkpoint_superblock`); otherwise it carries over. Reason for waiting (`docs/internals/sync.md`): a table could be released before it was synced; after a crash, replay would then need a table that is absent.

On startup with `sync_op_max != 0`, the replica sets `sync_tables_op_range` to the superblock range and resumes table sync after opening.

### 6b.7 WAL handling

- `commit_min` is set to the target checkpoint op. It may move backward by up to `lsm_compaction_ops` (comment: state sync can "truncate" the first batch of committed ops); those ops are re-executed from the WAL.
- Headers from the `view` message replace local headers for ops `<= op_prepare_max_sync()`; the head becomes the first such header.
- Nothing is bulk-erased. Missing prepares are fetched by normal WAL repair between `op_repair_min()` and the head; dirty slots outside that range are dropped by `repair_clean_out_of_bound_prepares`.
- `op_repair_min()`: if the current checkpoint is durable, `op_checkpoint + 1` (or `(op + 1) -| journal_slot_count` once the head is past `op_prepare_max` and the next checkpoint is durable); if not durable, `(op_checkpoint + 1) -| vsr_checkpoint_ops`. A replica on a not-yet-durable checkpoint must keep the previous interval repairable, because every committed op must be in a quorum of WALs or a quorum of checkpoints.

### 6b.8 Background table sync

- `sync_tables_op_range = { min: trigger(old checkpoint) + 1 or 0, max: sync_op_max }`, with `sync_tables` a `ForestTableIterator`.
- `sync_enqueue_tables`: for each manifest table with `snapshot_min` in `[readable_at_commit(min), readable_at_commit(max)]` (`readable_at_commit(op) = op + 1`), call `blocks_missing.sync_table`. Concurrency is bounded by `grid_missing_tables_max`. Tables outside the range are checked present with `grid.verify_table`.
- `sync_reclaim_tables` runs after each repair write, frees finished tables and enqueues more.
- Ratchet: if a second sync happens while tables from the first are outstanding, the old range is finished first, then the range becomes `{ old max + 1, superblock sync_op_max }`. Tables no longer in the manifest are dropped (`sync_tables_cancel`).
- Completion: `sync_tables = null`, then `send_prepare_oks_after_syncing_tables()`.
- If a synced table's blocks are released and the checkpoint becomes durable, `checkpoint_durable_commence` drops or aborts the repair.

### 6b.9 Client sessions and replies

- The client sessions trailer is part of the checkpoint, so sessions and reply headers arrive with the new checkpoint.
- In `client_sessions_open_callback`, if `sync_op_max > 0`: for every session whose reply `header.op` is in `[sync_op_min, sync_op_max]` and whose reply has a body (`size > @sizeOf(Header)`), set `client_replies.faulty`.
- The repair loop sends `get_reply { reply_client, reply_op, reply_checksum }` for the first faulty slot to any other replica; peers answer with the `reply` or stay silent.
- `sync_client_replies_done()` requires all such slots to be `reply_durable`; `sync_content_done()` = replies done and `sync_grid_done()`.
- A lost reply body does not block the cluster (`src/vsr/client_replies.zig` header comment); at worst one client must re-register.

### 6b.10 Acknowledgment rule while syncing

`op_prepare_ok_max()` has three cases:

1. `sync_grid_done()`: `op_checkpoint_next_trigger() + pipeline_prepare_queue_max`.
2. Table sync outstanding but `Checkpoint.durable(op_checkpoint, commit_max)`: same bound.
3. Table sync outstanding and the checkpoint not known durable: `trigger_for_checkpoint(op_checkpoint) + pipeline_prepare_queue_max`. The replica must not count toward the durability of a checkpoint whose tables it does not fully hold.

A syncing replica otherwise participates normally: it stores prepares, votes in view changes, and serves blocks it has.

## 7. Clock

### 7.1 Sources and sampling

`src/vsr/clock.zig`, `src/vsr/marzullo.zig`, `on_ping_timeout` / `on_ping` / `on_pong` in `src/vsr/replica.zig`.

- Two clocks from an injected `Time` interface (`TimeSim` in simulation): `monotonic()` for measuring intervals and all timeouts; `realtime()` (wall clock) only as raw material for offsets.
- `ping_timeout` = 1 s. A `ping` carries `ping_timestamp_monotonic = m0` (plus `checkpoint_id`, `checkpoint_op`, and the bundled release list). The `pong` echoes `m0` and adds `pong_timestamp_wall = t1`. On receipt, `m2 = monotonic()` and `clock.learn(replica, m0, t1, m2)`. Pongs from standbys are ignored.
- `learn` rejects `m0 > m2`, `m0 < window.monotonic` (window was reset since the ping), and `m2 - window.monotonic > window_max`. It tolerates a round trip of 0.
- Sample: `one_way_delay = (m2 - m0) / 2`; `t2 = window.realtime + (m2 - window.monotonic)`; `clock_offset = t1 + one_way_delay - t2`. `estimate_asymmetric_delay` corrects one-off queuing delay against the previous epoch's sample (10 ms error margin). Per source, only the sample with the smallest one-way delay in the window is kept.
- The replica itself is always a source with offset 0 and delay 0.
- `round_trip_time_median_ns()` also feeds `prepare_timeout.set_rtt_ns`.

### 7.2 Windows and epochs

| Constant | Value |
|---|---|
| `clock_synchronization_window_min` | 2 s |
| `clock_synchronization_window_max` | 20 s |
| `clock_epoch_max` | 60 s |
| `clock_offset_tolerance_max` | 10 s |
| Clock quorum | `quorum_replication` (standby: one more) |

`Clock.tick()` calls `synchronize()`:

1. Nothing before `window_min`. At `window_max` without success, reset the window (log "partitioned" or "no agreement").
2. Only if a new sample was learned since the last attempt.
3. Starting at `tolerance = clock_offset_tolerance_max` and halving up to 64 rounds (ending with 0): build tuples `clock_offset -/+ (one_way_delay + tolerance)` per source, run `Marzullo.smallest_interval`, stop when `sources_true < quorum`. Keep the tightest interval that still had a quorum.
4. On success the window becomes the `epoch` and a fresh window starts.

If no window succeeds within `epoch_max`, the epoch is reset and the clock is unsynchronized. Synchronization is disabled for a single-replica cluster.

`Marzullo.smallest_interval`: insertion-sort tuples by offset, lower bound before upper at ties, then source index (fully specified order); sweep counting overlaps; return the interval with the most overlapping sources, preferring the narrower on ties, plus `sources_true` / `sources_false`.

### 7.3 Use by the primary

- `realtime_synchronized()` returns `clamp(realtime(), epoch.realtime + elapsed + lower_bound, epoch.realtime + elapsed + upper_bound)`, where `elapsed` is monotonic time since the epoch began, or null when no epoch is synchronized. The system clock is used as-is when it lies inside the cluster interval; otherwise it is clamped.
- `on_request`: for client requests, if `realtime_synchronized()` is null the request is dropped and `primary_abdicate_timeout` (10 s) starts. When it fires, `primary_abdicating = true`, which suppresses commit heartbeats so backups elect another primary. Receiving a fresh `prepare_ok` clears it.
- Timestamp assignment (`primary_pipeline_prepare`): `prepare_timestamp = max(max(prepare_timestamp, commit_timestamp) + 1, realtime)`. Timestamps are strictly monotonic across view changes and restarts regardless of wall-clock jumps.
- Backups never consult their clock for state: the timestamp is in the prepare. Time is an input chosen by consensus, not a local read.
- Requests the primary sends to itself (`client == 0`: pulse, upgrade) bypass the synchronized clock. `on_pulse_timeout` uses raw `realtime()` only to decide whether a pulse is needed.

## Lessons / traps for a Rust reimplementation

1. Define checkpoint, trigger, durable, prepare_max and prepare_ok_max as pure functions of op numbers and compile-time constants, and assert the wrap inequality at compile time. Durability is inferred from a commit number, never from an explicit message.
2. Checkpoint one bar behind the trigger and make "do not redo background work on replay" an explicit predicate (`op_compacted`). Replay must rebuild the exact in-memory state the checkpoint assumed.
3. Free blocks only at the durability op, on every replica, in the same commit stage. Freeing at checkpoint-write time loses both crash safety and quorum repairability.
4. Deterministic allocation under concurrency needs reservations made in a fixed order with deterministic upper bounds. Meter background work in logical units (values, blocks), never time or IO completions.
5. Keep replica identity, view numbers and wall-clock time out of checkpointed bytes. Hash the checkpoint root and attach it to protocol messages so divergence crashes early; note the id on a prepare lags one checkpoint.
6. Superblock: all copies share a checksum, the copy index is excluded from it, writes are sequential, then read back to a 3/4 quorum; open at 2/4 and repair only into home slots. Treat verification failure as fatal.
7. The allocator's own serialized form lives in blocks it allocates. Decide explicitly how those blocks are accounted for (TigerBeetle patches them in at open and releases them right after the checkpoint).
8. State sync should replace only the root pointer synchronously and repair everything else lazily through the ordinary block-repair path, with a persisted "incomplete sync" op range that is cleared at the next checkpoint, not when repair finishes.
9. A replica with an incomplete sync must not acknowledge ops that would attest to a checkpoint it does not fully hold.
10. Coherent reads that block on remote repair, instead of failing, let one code path serve both corruption and sync; they need an unbounded-safe wait queue and a cancel path.
11. Repair responders stay silent when they cannot help; requesters need per-peer budgets, expiry and retry suppression rather than acks.
12. Clock: sample offsets with monotonic round trips, keep the lowest-delay sample per peer, require a quorum interval, clamp rather than replace the local clock, refuse to timestamp when unsynchronized, and abdicate if that persists. Feed time into the state machine only through the log.

## Not verified

- `docs/internals/lsm.md` says free-set reservations are taken per half-bar; the code at HEAD reserves per beat (a comment in `manifest_log.zig` confirms the change). I followed the code.
- Table selection inside `Manifest.compaction_table` (least overlap) was taken from the docs, not read in `src/lsm/manifest.zig`.
- Exact byte sizes of `SuperBlockHeader` and the copy padding were computed, not read from an assertion.
- `src/vsr/grid.zig` lines 260-376 and 735-862, the tests at the end of `free_set.zig`, and `docs/ARCHITECTURE.md` were not read in this pass.
- Whether timeout jitter uses the replica PRNG everywhere was not checked beyond the repair timeouts.


## 8. Simulator (VOPR)

Source: TigerBeetle `main` at commit `a2a3c7064519e08233abe26c54a43fca2f9ad9bc` (2026-11-10, 0.18.0 era). All paths are relative to the repo root. Everything below was read from source unless marked "(inferred)". Line numbers are approximate anchors for that commit.

### 8.1 Entry points and how it is run

- The simulator binary is `src/vopr.zig` (there is no `src/simulator.zig` at this commit). `main()` parses flags, derives options from the seed, builds a `Simulator`, runs the safety phase, then the liveness phase.
- Build: `./zig/zig build vopr -- <seed>` (`build.zig`, `build_vopr`). Without a seed the build forces `ReleaseSafe` and the binary picks a random seed from the OS CSPRNG; with a seed it uses the requested mode (Debug by default). `ReleaseFast`/`ReleaseSmall` are rejected at comptime because they disable assertions (`comptime assert(builtin.mode == .Debug or builtin.mode == .ReleaseSafe)`), and `main` starts with `comptime assert(constants.verify)` (extra-verification code paths must be compiled in).
- Build options: `-Dvopr-state-machine=testing|accounting` (default `accounting`), `-Dvopr-log=short|full` (default `short`).
- Runtime flags (`CLIArgs` in `src/vopr.zig`): `--lite`, `--performance`, `--ticks-max-requests` (default 40,000,000), `--ticks-max-convergence` (default 10,000,000), `--packet-loss-ratio`, `--replica-missing`, `--replica-missing-until-request`, `--requests-max`, and a positional seed.
- The seed may be a decimal `u64` or a 40-character Git hash, which is parsed as hex `u160` and truncated to `u64` (`stdx.fuzz.parse_seed`, `src/stdx/testing/fuzz.zig`). CI runs `zig build vopr -Dvopr-state-machine={testing,accounting} -Drelease -- <git commit>` so every commit gets a different but reproducible seed (`build.zig`, `build_ci`).
- The VOPR always compiles against the minimal cluster config: `pub const tigerbeetle_config = @import("config.zig").configs.test_min` (`src/vopr.zig:74`). `test_min` (`src/config.zig`) shrinks everything so interesting boundaries are hit within ~100 requests:

| Constant (test_min) | Value | Why it matters |
|---|---|---|
| `journal_slot_count` | 32 (`journal_slot_count_min` = 2 header sectors x 16 headers) | WAL wraps after 32 ops |
| `lsm_compaction_ops` | 4 | a compaction "bar" is 4 ops |
| `pipeline_prepare_queue_max` | 4 | |
| `vsr_checkpoint_ops` | 20 (= 32 - 4 - 4*ceil(8/4), `src/constants.zig:40`) | checkpoint every 20 ops |
| `clients_max` | 7 (`4 + 3`) | sessions evict easily |
| `message_size_max` | 4096 (`message_size_max_min(4)`) | one sector per message |
| `block_size` | 4096 (`sector_size`) | one sector per grid block |
| `tick_ms` | 10 (default) | 1 tick = 10 ms simulated |
| `journal_iops_write_max` | 2 | |

`replicas_max = 6`, `standbys_max = 6`, `members_max = 12` (`src/constants.zig`).

### 8.2 Object model

`Simulator` (`src/vopr.zig`) owns:

- `prng: *stdx.PRNG` - the root PRNG created in `main` from the seed.
- `cluster: *Cluster` - `ClusterType(StateMachineType)` from `src/testing/cluster.zig`.
- `workload: StateMachine.Workload` - request generator plus (for accounting) the `Auditor` model.
- `reply_sequence: ReplySequence` - reorders committed replies into op order before the workload sees them.
- Per-replica bookkeeping: `replica_releases[]` (how many releases are in each replica's "binary"), `replica_releases_limit`, `replica_crash_stability[]`, `replica_reformats` (bitset of replicas that have ever been reformatted), `core` (bitset used in liveness mode), counters `requests_sent`, `requests_replied`, `requests_idle`.

`Cluster` (`src/testing/cluster.zig`, `ClusterType`) owns, per node (replicas followed by standbys):

- `replicas[]: Replica` where `Replica = vsr.ReplicaType(StateMachine, MessageBus, Storage, AOF)` - the production replica code, instantiated at comptime with simulated `MessageBus` (`src/testing/cluster/message_bus.zig`), simulated `Storage` (`src/testing/storage.zig`), and an AOF backed by an in-memory test IO (`stdx.IOTest`, 32 MiB buffer per replica).
- `storages[]: Storage`, `replica_pools[]: MessagePool`, `replica_times[]: TimeSim`, `replica_tracers[]`, `replica_health[]: ReplicaHealth` (`up{paused}`, `down`, `reformatting`), `replica_upgrades[]: ?Release`, `replica_reformats[]: ?ReplicaReformat`, `replica_releases_bundled[]`.
- One shared `network: *Network` (`src/testing/cluster/network.zig`), wrapping a `PacketSimulatorType(*Message)` (`src/testing/packet_simulator.zig`).
- One shared `storage_fault_atlas: *ClusterFaultAtlas` (`src/testing/storage.zig`).
- `clients[]: ?Client` - the production `vsr.ClientType`, with `client_count + reformats_max` entries (the extra clients are reserved for `ReplicaReformat`, i.e. `tigerbeetle recover`), each with its own `MessagePool` and `TimeSim`. Client ids are `client_id_permutation.encode(i + members_max)` so a client id never collides with a replica index.
- Checkers: `state_checker`, `storage_checker`, `grid_checker` (shared pointer handed to every `Storage`), `manifest_checker`. `JournalChecker` is stateless.
- Its own `prng` seeded from `options.cluster.seed`.

Two state machines can be plugged in (`vsr_vopr_options.state_machine`):

- `.testing` (`src/testing/state_machine.zig`): one operation, `echo`. `commit()` inserts a `Thing{ id = op, timestamp, value = checksum(client, op, timestamp, operation, input) }` into a one-groove LSM forest and echoes the input. It still drives the real forest (prefetch, compact, checkpoint), so it stresses VSR + LSM plumbing cheaply. Its `Workload` sends random-size random bodies and asserts `reply_body == request_body`. `pulse_needed` always returns false.
- `.accounting` (`src/state_machine.zig`): the real ledger, with `src/state_machine/workload.zig` + `src/state_machine/auditor.zig`.

The same `Cluster` is reused by hand-written scenario tests in `src/vsr/replica_test.zig` (section 8.16).

### 8.3 Seed and PRNG derivation

- PRNG: `stdx.PRNG` (`src/stdx/prng.zig`) is xoshiro256++ seeded through SplitMix64 (`from_seed`). The API deliberately has no floating point ("remove floating point from the API, to ensure determinism"; a unit test greps its own source for `f32`/`f64`), no exclusive ranges (`int_exclusive` is a `@compileError`), and unbiased bounded integers (Lemire's method in `int_inclusive`).
- Probabilities are exact rationals: `Ratio{numerator, denominator}`, `prng.chance(ratio)` = `int_inclusive(denominator - 1) < numerator`.
- Helpers used for swarm testing: `enum_uniform`, `enum_weighted(Enum, weights)`, `chances(.{...})`, `enum_weights` / `fuzz.random_enum_weights` (pick a random non-empty subset of variants via `Combination`, give each a weight in 1..100 and the rest 0 - "some variants are disabled completely, and the rest have wildly different probabilities"), `Combination` (uniform k-of-n selection), `Reservoir`, `shuffle`, `fuzz.random_int_exponential` (the one place floats are used, via `std.Random.floatExp`), `fuzz.range_inclusive_ms`.
- Derivation tree: `main` creates the root PRNG from the seed, then `options_swarm(&prng)` draws every option in a fixed order, including three child seeds: `cluster.seed = prng.int(u64)`, `network.seed = prng.int(u64)`, `storage.seed = prng.int(u64)`. Each subsystem then owns an independent PRNG:
  - Root PRNG (continues after options): request scheduling, crash/restart/pause/upgrade decisions, `random_core`, and the `Workload` (it holds the same `*PRNG`).
  - `Cluster.prng` (cluster seed): fault atlas generation, client id permutation, replica nonces, the 1% `JournalChecker` sampling, `previous_request_latency`.
  - `PacketSimulator.prng` (network seed): loss, replay, delays, partitions, clogging, capacity eviction.
  - `Storage.prng` (storage seed): latencies, fault decisions, uninitialized-sector fill. Note every replica's `Storage` is initialized from the same `options.storage.seed` (`Cluster.init` copies `options.storage` and only overrides `replica_index`, `fault_atlas`, `grid_checker`, `iops_write_max`); streams diverge only because each replica's IO pattern differs.
  - The AOF test IO is seeded with the cluster seed.

### 8.4 Swarm options (`options_swarm`, `src/vopr.zig`)

Every run draws a fresh configuration. Ranges are inclusive.

| Option | Distribution |
|---|---|
| `replica_count` | uniform 1..6 |
| `standby_count` | uniform 0..6 |
| `client_count` | uniform 1..(`clients_max*2 - 1`) = 1..13 (more clients than sessions, to force eviction; "-1 since otherwise it is possible that all clients will evict each other") |
| `batch_size_limit` | 50%: `message_body_size_max`; else uniform from largest single event size to `message_body_size_max` |
| `multi_batch_per_request_limit` | uniform 1..(events_max - 1) |
| `storage_size_limit` | `sector_floor(200 MiB - uniform(0..20 MiB))` |
| `reformats_max` | `replica_count + 2` |
| accounting state machine | `lsm_forest_compaction_block_count = min + uniform(0..256)`; each of the three caches is 256 entries or 0 (coin flip) |
| `one_way_delay_min` | uniform 0..30 ms |
| `one_way_delay_mean` | uniform 30..100 ms |
| `packet_loss_probability` | uniform 0..30% |
| `path_maximum_capacity` | uniform 2..20 messages |
| `path_clog_duration_mean` | uniform 0..5000 ms |
| `path_clog_probability` | uniform 0..2% per path per tick |
| `packet_replay_probability` | uniform 0..50% |
| `partition_mode` | uniform over `none, uniform_size, uniform_partition, isolate_single` |
| `partition_symmetry` | uniform over `symmetric, asymmetric` |
| `partition_probability` | uniform 0..3% per tick |
| `unpartition_probability` | uniform 1..10% per tick |
| `partition_stability` | 100 + uniform(0..100) ticks |
| `unpartition_stability` | uniform 0..20 ticks |
| `read_latency_min`, `write_latency_min` | uniform 0..30 ms each |
| `read_latency_mean` | uniform `read_latency_min`..100 ms |
| `write_latency_mean` | uniform `write_latency_min`..1000 ms |
| `read_fault_probability` | uniform 0..10% |
| `write_fault_probability` | uniform 0..10% |
| `write_misdirect_probability` | uniform 0..10% |
| `crash_fault_probability` | uniform 80..100% |
| fault atlas | `faulty_superblock = true`; `faulty_wal_headers = faulty_wal_prepares = faulty_client_replies = (replica_count > 1)`; `faulty_grid = (replica_count > 2)` |
| `replica_crash_probability` | fixed 2/10,000,000 per replica per tick (x10 while the replica has pending writes) |
| `replica_crash_stability`, `replica_restart_stability` | uniform 0..1000 ticks each |
| `replica_restart_probability` | fixed 2/1,000,000 per tick |
| `replica_reformat_probability` | fixed 30% per restart |
| `replica_pause_probability` / `replica_unpause_probability` | fixed 8/10,000,000 and 8/1,000,000 per tick |
| `replica_pause_stability`, `replica_unpause_stability` | uniform 0..1000 ticks each |
| `replica_release_advance_probability` | fixed 1/1,000,000 per replica per tick |
| `replica_release_catchup_probability` | fixed 1/100,000 |
| `requests_max` | fixed `journal_slot_count * 3` = 96 (not counting `register`) |
| `request_probability` | uniform 1..100% per tick |
| `request_idle_on_probability` | uniform 0..20% per tick |
| `request_idle_off_probability` | uniform 10..20% per tick |

The accounting workload adds its own swarm layer (`Options.generate`, `src/state_machine/workload.zig`): `accounts_max` 2..128; per-operation weights (`create_transfers` 1..100, lookups/queries 1..20, plus deprecated/unbatched variants); 1% invalid accounts/transfers; limit probability 0..100%; pending 1..100%; post/void 1..50% each; retry 1..10%; id permutation chosen from `identity | inversion | zigzag | random` (`src/testing/id.zig`); batch size spans up to the batch limit.

Other modes: `options_lite` = swarm options but forced to 3 replicas, 0 standbys ("only looks for crashes", skips the liveness phase). `options_performance` = 6 replicas, 4 clients, all faults zero, fixed workload seed 92, `requests_max = journal_slot_count * 8`, 500 warm-up ticks, optional `--replica-missing`.

There are no clock options: see 8.6.

### 8.5 Tick loop

`main` safety phase (`src/vopr.zig:271-294`):

```
tick = 0
while tick < ticks_max_requests:        # 40M; this is a no-progress budget
    replied_before = simulator.requests_replied
    simulator.tick(); tick += 1
    if simulator.requests_replied > replied_before: tick = 0
    requests_done = (requests_replied == requests_max)
    upgrades_done = any up replica runs releases[replica_releases_limit - 1]
    if requests_done and upgrades_done: break
```

Progress is "a client received a reply to a non-`register` request" (`on_client_reply`). The tick budget resets on progress, so the limit is 40M ticks (about 4.6 simulated days) without a reply, not total.

`Simulator.tick()` order:

1. `cluster.tick()`
2. `tick_requests()` - toggle idle with `request_idle_on/off_probability`; if not idle, under `requests_max`, and `chance(request_probability)`: pick a random non-evicted client; skip if it already has `request_inflight` or if `ReplySequence` might overflow; `workload.build_request()`; `cluster.request()`.
3. `tick_upgrade()` - per replica, `chance(replica_release_advance_probability)` adds the next release to that replica's bundle.
4. `tick_crash()` - per replica: decrement stability; if up and not paused maybe crash; if down maybe upgrade/restart/reformat.
5. `tick_pause()` - per replica maybe pause/unpause. (Quirk: `tick_pause` decrements the same `replica_crash_stability[]` array as `tick_crash`, so each stability window elapses at two decrements per tick.)
6. Performance mode only: restart the "missing" replica after the configured request.

`Cluster.tick()` order (`src/testing/cluster.zig:526`):

```
loop:                                   # "Interleave storage and network steps,
    advanced = network.step()           #  to allow for faster-than-a-tick IO."
    deinit clients that have been told they were evicted
    for each node not paused:
        advanced |= storage.step()
        if replica_upgrades[i] != null: replica_release_execute(i)
    if !advanced: break
network.tick()
for each client: client_time.tick(); client.tick()
for each node:
    replica_time.tick()                 # time advances even if down or paused
    storage.tick()
    reformatting -> tick_reformat()
    up and not paused ->
        replica.tick()
        aof_io.run()
        with probability 1/100: JournalChecker.check(replica)
        state_checker.check_state(replica)
    down -> nothing
```

Details:

- `Network.step()` first resumes any message bus that asked for `resume_receive`, then `PacketSimulator.step()` delivers at most one ready packet per (source, target) path. Delivery is a synchronous call into the target's `on_messages_callback`, so the replica handles the message inside the step.
- `Storage.step()` completes at most one IO (the earliest ready read or write; reads win ties) and then drains the `next_tick_queue` (callbacks registered through `Storage.on_next_tick`).
- IO latency: on submit, `ready_at = tick_instant + max(latency_min, exponential(latency_mean))` in nanoseconds; `tick_instant = ticks * tick_ms`. An IO completes when `ready_at <= tick_instant`. With zero latency, a write completes within the same `Cluster.tick`, hence the fixed-point loop.
- A `Cluster.tick` therefore maps to exactly one `replica.tick()`, one `storage.tick()`, one `network.tick()`, and one `time.tick()` per node, preceded by draining all work that is already due.

### 8.6 Simulated time

- `stdx.TimeSim` (`src/stdx/time.zig`) implements the `Time` interface (monotonic + realtime). `tick()` sets `monotonic = ticks * tick_duration` and `realtime = epoch + monotonic - offset(ticks)`. Offset models: `linear` (drift A per tick plus B), `periodic` (A * sin), `step` (jump A after B ticks), `non_ideal` (periodic with random phase and jitter).
- In the VOPR every replica and client clock is `fixtures.init_time(.{})` (`src/testing/fixtures.zig`): `tick_duration = tick_ms`, `offset_type = .linear`, A = B = C = 0. So at this commit the VOPR injects no clock skew or drift; all clocks are perfect and identical. Clock-fault modeling exists only in the `src/vsr/clock.zig` unit tests (which use `TimeSim` with non-zero offsets). The only time anomaly in the VOPR is that `time_sim.tick()` keeps running while a replica is paused or down, so an unpaused replica sees a monotonic jump.

### 8.7 Network fault model

`PacketSimulatorType(Packet)` (`src/testing/packet_simulator.zig`), wrapped by `Network`:

- Topology: `process_count = node_count + client_count`; one `Link` per ordered pair (`links[source * process_count + target]`). Nodes come first, then clients.
- Each `Link` has: a min-heap `queue` ordered by `ready_at` with capacity `path_maximum_capacity`; `filter: LinkFilter` (an `EnumSet(vsr.Command)`; commands in the set are delivered, others dropped); optional `drop_packet_fn`; `record: LinkFilter` (commands to capture for later replay); `clogged_till: Instant`.
- `submit_packet`: if the queue is full, evict a uniformly random queued packet; then enqueue with `ready_at = now + max(one_way_delay_min, exponential(one_way_delay_mean))`. Because each packet draws an independent delay, reordering is inherent. If the link is recording this command, a clone is appended to `recorded`.
- `step` (delivery): skip clogged paths; pop the head if ready; then in order: (1) drop if the link filter or `drop_packet_fn` rejects it (the filter is evaluated at delivery time, so a partition also kills packets already in flight); (2) drop with `packet_loss_probability`; (3) with `packet_replay_probability`, re-submit a clone on the same path (duplication with a fresh delay; clones can be replayed again); (4) deliver.
- `tick`: `ticks += 1`; partition state machine; then for every path, `chance(path_clog_probability)` sets `clogged_till = now + exponential(path_clog_duration_mean)`. A clogged path delivers nothing but keeps accepting packets (until capacity eviction).
- Partitions (`auto_partition_network`): only nodes (replicas and standbys) are partitioned; clients are always connected to everything. There are always exactly two sides. Modes: `none`; `uniform_size` (side size uniform in 1..n-1, random membership); `uniform_partition` (each node flips a coin, fixed up if all landed on one side); `isolate_single` (one random node alone). `symmetric`: links between sides are cut both ways. `asymmetric`: packets from one randomly chosen side to the other still flow; the reverse direction is cut. State machine: while `auto_partition_stability > 0` just count down; otherwise if partitioned, `chance(unpartition_probability)` heals all links and sets stability to `unpartition_stability`; if not partitioned and `node_count > 1`, `chance(partition_probability)` partitions and sets stability to `partition_stability`.
- Crashed targets: `Network.packet_deliver` drops the packet if `buses_enabled[target]` is false ("dropped; target is down"). `Cluster.replica_crash` calls `network.process_disable` but leaves in-flight packets to and from the replica in the queues, so stale messages from a previous incarnation can arrive after restart.
- Receive-buffer backpressure: if the target bus buffer cannot fit the message it is dropped; if the replica leaves a message unconsumed the bus is marked `suspended` until `resume_receive`.
- No bit-level message corruption is injected by the packet simulator at this commit. (Vortex injects network corruption against real sockets; `src/message_bus_fuzz.zig` fuzzes the real message bus.)
- `Network.send_message` copies the message into a network-owned `MessagePool` sized `1 + path_maximum_capacity * path_count + recorded_count_max`, and asserts the sender matches the header's claimed peer.
- Test-only controls used by `replica_test.zig`: `link_filter(path)`, `link_drop_packet_fn(path)`, `link_record(path)`, `replay_recorded()`, `link_clear(path)`. The VOPR itself never sets `drop_packet_fn` (asserted in `Network.transition_to_liveness_mode`).

### 8.8 Storage fault model

`Storage` (`src/testing/storage.zig`) is an in-memory data file:

- `memory` always holds pristine data as written. Faults are metadata on top: `faults` (bitset, one bit per 4096-byte sector) and up to two `overlays` (misdirected-write buffers). `memory_written` records which sectors were ever written.
- `faulty: bool` is a master switch: when false, reads return pristine data (fault bits may still accumulate but are invisible).
- Zones: `superblock`, `wal_headers`, `wal_prepares`, `client_replies`, `grid_padding`, `grid`.

| Fault | Trigger | Effect |
|---|---|---|
| Latency | every IO | `max(min, exponential(mean))`; reads and writes complete out of submission order |
| Read fault | on read completion, `chance(read_fault_probability)` | one random sector of the read that lies in an atlas-faulty chunk for this replica gets its fault bit set (sticky) |
| Write fault | on write completion, `chance(write_fault_probability)` | written sectors' fault bits are cleared first, then one atlas-faulty sector in range is marked faulty (the write "landed corrupt") |
| Misdirected write | on write completion, `chance(write_misdirect_probability)`, only if the write touches an atlas-faulty chunk and both overlay slots are free | the data also appears at another atlas-faulty chunk start in the same zone, and the intended location keeps showing the old data (two overlays). At most one misdirect per `Storage` at a time; cleared when either location is rewritten |
| Crash fault (torn write) | `Storage.reset()` at crash, per pending write, `chance(crash_fault_probability)` (80..100%) | the pending write is discarded entirely (old data remains) and one random sector of its target range is marked faulty. Not constrained by the atlas; applies to every zone including the superblock |
| Uninitialized read | read of a never-written sector (allowed only in `client_replies` and `grid`) | sector filled with random bytes |

- A faulty sector is surfaced on read as a single bit flip at a position derived deterministically from the sector's first 8 pristine bytes, "so that read-retries don't resolve the corruption". The flip may land in padding and be harmless.
- Fault bits are cleared when the sector is rewritten (repair works).
- Double-fault avoidance in `read_sectors_fault_eligible`: a WAL prepare read is not faulted if its header sector has a misdirect overlay (the journal would misread that as a torn prepare), and is not misdirected if the redundant header is already mismatched or faulty.
- Assertions in the storage itself: no two concurrent writes to overlapping ranges of a zone; reads of `superblock`/`wal_headers`/`wal_prepares` must target written sectors; `zone.verify_iop`; `grid_block()` asserts `header.address == address`.
- Not modeled: `flush_sectors` completes synchronously and only resets a counter, so there is no "acknowledged but unsynced write lost at crash" fault. A completed write is durable; only in-flight writes are lost. Torn writes are modeled as "old data plus one corrupt sector", not as a prefix of new sectors (there is a TODO to make this more varied).
- `Storage.synchronicity = .always_asynchronous`.

### 8.9 ClusterFaultAtlas: keeping faults recoverable

`ClusterFaultAtlas` (`src/testing/storage.zig:1018`) is generated once per cluster from `Cluster.prng` and decides where read/write/misdirect faults are allowed. It holds three families of per-replica bitsets: `faulty_wal_header_sectors`, `faulty_client_reply_slots`, `faulty_grid_blocks`.

Generation rule (`init`): for every chunk (a WAL header sector, a client-reply slot, or a grid block address), pick `faults_max = quorums(replica_count).replication - 1` distinct replicas at random and mark the chunk faulty for them, skipping a replica if that would make all of its chunks in that zone faulty ("Never corrupt all chunks of a particular replica. (For the WAL, this can cause error.WALInvalid)").

Consequences:

- With `quorum_replication` = 1,2,2,2,3,3 for R = 1..6 (`vsr.quorums`, `src/vsr.zig:1212`), any chunk can be damaged on at most 0,1,1,1,2,2 replicas. Since a committed op was persisted by at least `quorum_replication` replicas, at least one intact copy survives atlas-driven faults.
- WAL headers and prepares are coupled: `wal_prepares` uses the same bitset with `chunk_size = message_size_max * headers_per_sector`. A replica may fault header sector k and the 16 prepares whose headers live in sector k, but never a slot that another "allowed" replica set does not cover. In `test_min` there are only two header sectors, so each replica has at most one faulty WAL half.
- Standbys (indices >= `replica_count`) are never selected, so they receive no atlas-driven faults (they still receive crash faults).
- Superblock: `zone_chunks(.superblock)` returns null, so no read/write/misdirect faults are injected there at all, despite `faulty_superblock = true` and the file header comment claiming "one read/write fault is permitted per area". The stated reason: an extra fault plus a torn write could break the 4-copy quorum (`copy0: B ok, copy1: B torn, copy2: A corrupt, copy3: A ok`). The superblock is only hit by crash faults on a pending superblock write.
- Disabled combinations: `replica_count == 1` disables all WAL, client-reply, and grid faults (nothing to repair from; asserted in `init`). `faulty_grid` requires `replica_count > 2` because "in R=2, a lagging replica may sync to the leading replica, but then the leading replica may have the only copy of a block in the cluster".
- Startup protection: `Cluster.init` sets `storage.faulty = replica_index >= quorums.view_change`, so a view-change quorum starts with faults invisible ("so that the replicas don't get stuck recovering_head"). `Simulator.replica_restart` sets `faulty = true` after each restart.
- Crash faults are deliberately outside the atlas, so correlated damage is possible. The simulator compensates in two ways: (1) restart-time repairs (8.10); (2) the liveness phase does not assume recoverability - it checks it (8.13).

### 8.10 Replica lifecycle faults

Crash (`tick_crash_up` -> `Cluster.replica_crash`): only when up, not paused, and stability has expired. Probability is 2/10M per tick, multiplied by 10 while `storage.writes` is non-empty, to bias crashes toward mid-write. Sequence: `storage.reset()` first (drops pending reads and next-tick callbacks; tears pending writes), then `replica.deinit`, `network.process_disable`, health = `down`, and an assertion that the replica's message pool is fully returned (leak check). Stability is then set to `replica_crash_stability`.

Restart (`tick_crash_down` -> `Simulator.replica_restart(fault)`): when down and stability expired, with `chance(replica_restart_probability)` or when an upgrade catch-up triggers.

- Recoverability heuristic: count replicas that are up, never reformatted, not standby, not `recovering_head`, and not syncing. `fault = (count >= quorum_view_change) or replica.standby()`. If the cluster cannot spare this replica, it restarts with faults hidden "to improve VOPR utilization".
- Always: clear fault bits on all WAL header sectors, because with only two header sectors a crash can tear both, which in production is impossible (`journal_iops_write_max < header_sector_count`).
- If `fault == false`: additionally copy the prepare's header over any zeroed redundant header (the journal writes zeroed headers on purpose to keep a slot faulty across restarts), so the replica cannot be forced into `recovering_head`.
- Then `storage.faulty = fault`; `cluster.replica_restart()` (`Replica.open` with a new random nonce, running the newest bundled release); `storage.faulty = true`; stability = `replica_restart_stability`. Asserts that `recovering_head` after a fault-free restart is only possible with a header/prepare view mismatch.

Reformat (`Cluster.replica_reformat`): when restarting with `fault == true`, not a standby, `reformat_count < reformats_max`, and `chance(30%)`. The storage is re-initialized (data lost), health = `reformatting`, and a `ReplicaReformat` (the `tigerbeetle recover` logic) runs through one of the reserved clients. `tick_reformat` polls it, then formats, restarts the replica, and calls `state_checker.reformat()`. The simulator remembers the replica in `replica_reformats`.

Pause (`tick_pause`): a paused replica does not tick and its storage does not step ("catch more interesting interleaving of events" and "simulate real-world scenario of VM migration"). Network delivery to it is not disabled (inferred from `Cluster.tick`: only `storage.step` and `replica.tick` are skipped), and its clock keeps advancing.

Upgrades:

- `releases` = three versions 0.0.1, 0.0.2, 0.0.3 (`src/vopr.zig:43`). All replicas are formatted and started on `releases[0]`, with only that release bundled. Clients run `releases[0]`.
- `tick_upgrade` adds the next release to a replica's bundle (`Cluster.replica_set_releases`). The replica reads its bundle through a `Multiversion` vtable (`replica_multiversion`), mimicking a binary being replaced on disk.
- When the replica decides to switch release it calls `release_execute`; the cluster records it in `replica_upgrades` and tears the storage (`replica_release_execute_soon`). On the next `Cluster.tick` loop iteration `replica_release_execute` crashes the replica and reopens it on the target release with faults hidden. If the replica does not have that release bundled it stays down.
- A down replica behind `replica_releases_limit` gets the next release with `chance(replica_release_catchup_probability)`, or always if it is in the liveness core.
- `Replica.open` with a release that differs from what the data file needs triggers `release_execute` during open; `Cluster.replica_restart` handles that synchronously.
- Checks: `StateChecker` records the release under which each op first committed and asserts every other replica executes that op under the same release; the safety phase does not end until some up replica runs the newest available release; liveness requires all core replicas on the same release.

### 8.11 Checkers and invariants

| Checker | When it runs | What it checks |
|---|---|---|
| `StateChecker.check_state` (`src/testing/cluster/state_checker.zig`) | after every `replica.tick()`; on every `committed` event; on sync stage `updating_checkpoint` | Maintains one canonical list `commits[op] = {header, release, replicas}`. For each replica's `commit_min`: it may only advance by exactly one op or move backward; if the op is already in history its checksum must equal the canonical one; if new, `header.parent == checksum of previous commit`, `op == commits.len`, the op was requested by a client that currently has exactly that request in flight (`request_checksum`, `request`, `operation`, `size`), or is `upgrade`/`pulse` when `client == 0`. Invariant: all replicas commit the same hash-chained sequence, with no gaps, forks, or invented requests. A missing in-flight request returns `error.ReplicaTransitionedToInvalidState` (exit 129) |
| `StateChecker.on_message` | every sent message (`message_sent` event) | Tracks per-replica max `(view, op)` acked in `prepare_ok`, and the latest reply per client. `check_state` asserts `replica.view >=` the max acked view (a replica never forgets a view it acted in) |
| `StateChecker.reformat` | after a reformat completes | Asserts some other replica's `commit_min` exceeds the reformatted replica's last acked op, then forgets that replica's progress |
| `StateChecker.assert_cluster_convergence` | each `Simulator.pending()` call in liveness mode | Every op has at least one committing replica; ops are contiguous; parent chain holds; `view` is non-decreasing along the log |
| `StorageChecker.replica_compact` (`storage_checker.zig`) | `compaction_completed` event, at the end of each bar (`(commit_min + 1) % lsm_compaction_ops == 0`), skipped while replaying after a crash or syncing | Checksum of all acquired, non-released grid blocks (in-memory free set, excluding open manifest-log blocks, including blocks still in write queues) must equal what the first replica recorded for that op. Invariant: compaction is deterministic; grids are byte-identical at every bar boundary |
| `StorageChecker.replica_checkpoint` | `checkpoint_completed` event | `replica.assert_free_set_consistent()`, then per `op_checkpoint`: checksum of the superblock `vsr_state.checkpoint` struct, of client replies (if not syncing), and of the grid using the free set decoded from disk (if not syncing) must match across replicas. Also asserts grid block padding is zeroed. Invariant: checkpoints are byte-identical across replicas |
| `GridChecker.assert_coherent` (`grid_checker.zig`) | from `Grid.assert_coherent` in `src/vsr/grid.zig` on block read/write/repair paths (compiled only when `Storage` is the test storage) | Map `(checkpoint_id, address, checkpoint_durable) -> checksum` shared by all replicas. Invariant: for a given checkpoint, an address holds exactly one block version cluster-wide, and a block is not overwritten before the checkpoint that released it is durable |
| `ManifestChecker` (`manifest_checker.zig`) | `state_machine_opened` and `checkpoint_completed` | Checksum over every tree's and level's table list, keyed by checkpoint op. Invariant: the LSM manifest is identical across replicas and identical after a restart from that checkpoint |
| `JournalChecker.check` (`journal_checker.zig`) | 1% of ticks per up replica, only when no journal write is executing | For every clean, non-reserved slot: on-disk redundant header is non-zero and equals both the on-disk prepare header and the in-memory header; prepare sector padding is zero |
| `ReplySequence` + `Workload.on_reply` | `committed` event from whichever replica commits an op first (`on_cluster_reply`) | Replies are queued and released strictly in op order starting at op 1; each is checked against the canonical prepare (`checksum`, `request`, `operation`) and handed to the workload/auditor |
| `Cluster.client_on_reply` | client receives a reply | Reply is valid, for this cluster/client, and matches the in-flight request's `request` number and `operation` |
| AOF validation | end of a converged run | Core replicas' AOF must end at the last committed checksum; other replicas' AOFs must parse |
| Leak checks | crash and process exit | Replica message pool fully free after `deinit`; `DebugAllocator` reports leaks (`@panic("memory leaked")`) |

All of this sits on top of the replica's own assertions, which stay enabled (the VOPR refuses to build without them).

### 8.12 Workload and Auditor (accounting)

- `Workload` (`src/state_machine/workload.zig`) runs in fixed memory and never stores transfers. `Transfer.id` is a reversible permutation of an ascending index; `transfer_index_to_plan(index)` re-derives from `(transfer_plan_seed, index)` whether the transfer is valid, limited, pending/post/void, so the expected outcome (`success`, `failure`, `unknown`) is recomputable for any id seen in a later lookup. `Transfer.user_data` carries a checksum of the transfer's other fields to validate lookups and queries.
- `Auditor` (`src/state_machine/auditor.zig`) is the reference model: account balances, per-account transfer counts and timestamp ranges, pending transfers and an expiry priority queue, query-intersection counters, a change-event tracker, and per-client in-flight expectations. For each create it stores the set of acceptable result codes (`expect_create_transfers`), then on reply panics if the actual code is outside the set (`on_create_transfers: unexpected result`), checks result timestamps (`created` must equal the derived per-event timestamp, `exists` must be older), and applies the effect to the model. `on_lookup_accounts` compares returned accounts byte-for-byte with the model and asserts absent accounts are absent.
- It requires replies in commit order, which is why `ReplySequence` exists, and it consumes `pulse` commits (`on_pulse` -> `expire_pending_transfers(timestamp)`) so time-based expiry stays in lockstep with the cluster's deterministic timestamps.
- Retries: `transfers_retry_exists` and `transfers_retry_failed` re-send old transfers and expect `exists` / `id_already_failed`, exercising idempotency.

### 8.13 Liveness testing

After the safety phase (skipped in `--lite`):

1. Choose the core (`Network.Core` = bitset over members: "a strongly-connected component of replicas containing a view change quorum").
   - If requests and upgrades completed: `random_core` - `replica_core_count` uniform in `quorum_view_change..replica_count`, a uniform random subset of that size, plus a uniform random number of standbys.
   - If the safety phase ran out of ticks: `full_core` (everyone), to decide whether being stuck was legitimate.
2. `Simulator.transition_to_liveness_mode(core)`:
   - Core replicas that are down are restarted with faults hidden; paused ones are unpaused.
   - Core storages: latencies set to 1 ms, read/write/misdirect/crash fault probabilities set to zero (`Storage.transition_to_liveness_mode`). Existing fault bits are not cleared; they must be repaired through the protocol.
   - Network: delays 1 ms, loss/replay/partition/unpartition probabilities zero, and all links between core members forced open (`Network.transition_to_liveness_mode`). Path clogging is left enabled.
   - Simulator: crash, restart, reformat, pause, release-advance, and release-catchup probabilities set to zero.
   - Non-core replicas are left exactly as they are: down stays down, partitioned stays partitioned, faulty stays faulty. "For all failures involving non-core replicas, make those failures permanent."
3. Run up to `ticks_max_convergence` (10M) ticks until `Simulator.pending() == null`. `pending()` returns the first unmet condition:
   - `"pending request"`: some client still has `request_inflight`.
   - `"pending upgrade"`: a core replica is not on the core's max release.
   - `"pending replica convergence"`: a core replica's `commit_min` is not the last canonical op.
   - `"pending journal"`: a core replica lacks a prepare in `op_checkpoint+1..commit_max`.
   - `"pending sync content"`: `replica.sync_content_done()` is false.
   - Then it asserts all core replicas have the same `checkpoint_id`.
4. If still pending, call `cluster_recoverable()` to decide whether the stall is a bug. It returns false (run passes with a warning) when:
   - `core_missing_primary`: a non-core replica believes it is primary in a view shared by a core member (known limitation; exits 0 through `unimplemented("repair requires reachable primary")`).
   - `core_missing_quorum`: fewer than a view-change quorum of core replicas are outside `recovering_head`/reformatting.
   - `core_missing_prepare`: reconstructs the uncommitted suffix from the highest-`log_view` core replicas and finds an op whose prepare is missing from at least a nack quorum (uncommitted) or from every replica (committed range needed for repair).
   - `core_missing_blocks`: every core replica that wants a grid block lacks it; panics `"block found in core"` if any core replica actually holds an intact copy (that is a repair bug).
   - `core_missing_reply`: a client reply is intact on no core replica.
   - `core_reformat_evicted`: a reformat client was evicted.
   Otherwise: `fatal(.liveness, "no state convergence: {reason}")`.

The design choice: the fault generator does not try to guarantee the core is recoverable. Instead an independent oracle inspects the final state and proves the stall is explained by lost data; anything unexplained is a liveness bug.

### 8.14 Exit codes, reproduction, logs

- `Failure` enum (`src/testing/cluster.zig:48`): `crash = 127`, `liveness = 128`, `correctness = 129`.
  - 129: `StateChecker`/`StorageChecker` returned an error, or reformat failed (`Cluster.fatal(.correctness, ...)`).
  - 128: liveness phase did not converge and the cluster was judged recoverable.
  - 127: the comment says "Any assertion crash will be given an exit code of 127 by default". I did not find the code that maps a panic to 127 at this commit (a Zig panic normally aborts with SIGABRT), so treat the exact value as unverified.
  - 0: `PASSED (N ticks)`; also `unimplemented(...)` cases ("not crashing in VOPR"), and a stuck liveness phase that `cluster_recoverable` explained.
  - CLI misuse goes through `vsr.fatal(.cli, ...)` with that reason's non-zero status.
- Reproduction: the run prints `SEED=<n>` and the drawn options up front, and on liveness failure logs "you can reproduce this failure with seed=N". A run is a pure function of (git commit, build options, CLI flags, seed).
- Logging: `-Dvopr-log=short` prints only the `simulator` and `cluster` scopes, i.e. one line per replica event (`Cluster.log_replica`): replica index, event (`!` crash, `^` recover, `X` reformat, ` ` commit, `$` sync, `[`/`]` checkpoint start/end), role (`/` primary, `\` backup, `|` standby, `~` syncing, `#` crashed, `F` reformatting), a status column per replica (`.` normal, `v` view_change, `r` recovering, `h` recovering_head), then view, `checkpoint/commit_min/commit_max`, journal op range, faulty/dirty counts, WAL op range, sync range, release, grid counters, and pipeline occupancy (documented in `docs/internals/testing.md`). `full` enables debug logs for every scope. Pending IOs record a stack trace at submission (`Storage.Read.stack_trace`) for diagnostics.
- `fuzz.limit_ram()` caps address space at 20 GiB on Linux.

### 8.15 How non-determinism is excluded

- The replica is generic over `Storage`, `MessageBus`, `Time`, and `AOF`; the simulator supplies in-memory implementations, so the production logic never touches the OS. Everything is single-threaded and callback-driven; the simulator calls completions explicitly from `step()`.
- Time comes only from `TimeSim`, advanced by the simulator. Randomness comes only from seeded `stdx.PRNG` instances (replica nonces are passed in via `random_nonce`).
- All allocation happens at init; pools are fixed size, so allocation order cannot leak into behavior (inferred from the static-allocation design; the VOPR uses a `DebugAllocator` and checks for leaks).
- Checker state uses hash maps only for keyed lookup, not iteration-order-dependent decisions; `core_missing_blocks` uses an insertion-ordered `AutoArrayHashMap` (inferred as deliberate).
- Fault positions are derived from data, not from call counts (the bit flip uses the sector contents as seed), so retries see stable corruption.
- I found no explicit determinism self-check (for example running a seed twice and comparing a trace hash) in `src/vopr.zig` or `src/testing/` at this commit. Determinism is validated indirectly: the storage, grid, and manifest checkers would flag any divergence between replicas, and seeds from CI/fuzzing are expected to reproduce.

### 8.16 Scenario tests on the same harness

`src/vsr/replica_test.zig` builds the same `Cluster` with the `testing` state machine, a fixed seed (123), no automatic faults (atlas all false, zero loss), and drives it by hand:

- `TestContext.init(.{ .replica_count = 3 })`, `t.replica(.R0 | .R_ | .A0 | .B1 | .S0 ...)` selectors (`A0` = current primary, `B1` = next backup), `stop()`, `open()`, `open_upgrade()`, `open_reformat()`, `corrupt(.{ .wal_prepare = slot | .wal_header = slot | .client_reply = slot | .grid_block = address })`, `drop/pass/drop_all/pass_all(peer, direction, command)`, `drop_fn`, `record` + `replay_recorded`, and accessors (`status()`, `view()`, `commit()`, `op_checkpoint()`, `sync_status()`).
- `c.request(requests, expect_replies)` ticks until 3,000 ticks pass without a commit; `t.run()` uses 8,200 ticks. Tests are written against exact constants (`comptime assert(journal_slot_count == 32)`, `lsm_compaction_ops == 4`) with named checkpoint/trigger ops.
- All the checkers stay active, so each scenario also gets the safety oracle.
- `src/testing/marks.zig` provides coverage marks: production code logs through `log.mark.*`, and a test asserts that a specific line was hit (`marks.check("...")`, `expect_hit()`), tying a branch to the scenario that justifies it.
- `src/testing/exhaustigen.zig` is an exhaustive generator (`while (!g.done())` enumerates every sequence of bounded choices), used by small unit tests where full enumeration beats random sampling.

### 8.17 Vortex (non-deterministic system test)

`src/vortex.zig` and `src/testing/vortex/` run real `tigerbeetle` processes, a real client driver (Zig, Java, or Rust), and a workload inside a Linux pid/network namespace (`stdx.unshare`). The supervisor (`supervisor.zig`) picks weighted actions: `replica_terminate`, `replica_pause`/`replica_resume` (signals), `replica_upgrade`, `cluster_upgrade`, `network_delay`, `network_corrupt` (1..10%), `network_heal`; scenarios `default`, `upgrade`, `recover`. Faults pass through a userspace proxy (`faulty_network.zig`). It fails if a replica exits unexpectedly or no request completes for `liveness_requirement = 180 s`. It adds what the VOPR cannot cover: real IO and syscalls, real sockets and message framing, multiversion binaries, and language clients. It is explicitly not reproducible ("Vortex is non-deterministic, but providing a seed can still help constrain the scenario").

### Lessons / traps for a Rust reimplementation

1. Make the replica generic over storage, network, and time from day one, and keep completions callback- or poll-driven so the simulator decides when each IO finishes. One simulator tick = one replica tick, preceded by a fixed-point drain of due network and storage work.
2. Derive one child seed per subsystem from the root seed, in a fixed order. Print the seed and all drawn options at startup. Accept a commit hash as a seed for CI.
3. Use a PRNG with integer-only, unbiased APIs and rational probabilities. Keep floats out of anything that affects control flow.
4. Randomize the configuration, not just the events (swarm testing): cluster size, standbys, more clients than sessions, latencies, fault rates, partition mode, and even which operations exist in the workload.
5. Run against a deliberately tiny config (32-slot WAL, 4-op bars, 20-op checkpoints, 1-sector blocks) so wraparound, checkpoint, compaction, and sync happen in a 96-request run. Short runs with many seeds beat long runs.
6. Keep pristine data separate from fault metadata. It makes faults toggleable (`faulty` switch), repairable (clear on rewrite), and lets oracles read ground truth.
7. Constrain random storage faults with a per-cluster atlas so each datum stays intact on enough replicas (`quorum_replication - 1` faulty copies at most), and disable fault classes that the topology cannot recover from (R=1: none; R<=2: no grid faults).
8. Let crash faults be unconstrained, then use an independent recoverability oracle in the liveness phase rather than weakening the faults. Unexplained stalls are bugs; explained stalls pass.
9. Test liveness explicitly: pick a random quorum-sized core, heal only the core, freeze every other failure permanently, and require convergence within a tick budget.
10. Checkers should compare replicas against the first replica to reach a point (bar boundary, checkpoint) using checksums of exactly the bytes that must be deterministic, and should list what is excluded and why (WAL headers, superblock headers, free blocks).
11. Verify the workload against replica-side commits in op order, not client-side replies, so lost replies do not hide results. Design ids so expected outcomes can be recomputed without storing history.
12. Bias crashes toward in-flight writes, and add stability windows so the simulator does not spend its time flapping.
13. Traps observed in the VOPR itself: no clock skew is injected; no unsynced-write loss or realistic partial-write tearing; no network bit corruption; the superblock gets no read/write faults; all replica storages share one seed; `tick_pause` and `tick_crash` share one stability counter; several restart-time "repairs" exist only because the test config has two WAL header sectors. Decide deliberately which of these gaps to close.
14. Give failure classes distinct exit codes (crash, liveness, correctness) and make "known unimplemented" exit 0 loudly, so a fuzzing fleet can triage automatically.
15. Reuse the simulator cluster for hand-written scenario tests with link filters, record/replay, and targeted corruption, and tie rare branches to tests with coverage marks.


## 9. Continuous fuzzing: CFO and devhub

**Provenance.** The CFO and the devhub web UI are *not* in the public repo at HEAD of `main`
(commit `a2a3c70`, 0.18.0 era). Everything about `cfo.zig`, `cfo_supervisor.sh`, `src/devhub/*`,
`docs/internals/releases.md` and `.github/workflows/ci.yml` below is read from a shallow clone of
tag **0.17.10** (commit `33fa353`, 2026-10-11). Paths are relative to that clone unless prefixed
with "HEAD:". Section 9.13 lists what differs at HEAD. Claims I derived rather than read are
marked "(inferred)".

### 9.1 Big picture

- A fleet of Linux machines each runs a tiny POSIX shell loop (`src/scripts/cfo_supervisor.sh`)
  that re-clones/updates `main` and runs `./zig/zig build scripts -- cfo` for a fixed budget
  (default 60 minutes), forever. The orchestrator is therefore self-updating: its code *is* the
  `main` branch (`src/scripts/cfo.zig` header: "By modifying this script, we can make those
  machines do interesting things").
- Each CFO run keeps every core busy with randomly seeded fuzzer processes (VOPR variants, about
  15 component fuzzers, Vortex), drawn from `main`, the `release` branch, and open pull requests
  carrying a `fuzz <name>` label.
- Every 5 minutes it merges the finished seeds into a single JSON file,
  `fuzzing/data.json`, in a separate git repository (`tigerbeetle/devhubdb`) and pushes. Git is the
  database; `git push` failing is the conflict detector.
- A static page (`src/devhub/`, deployed with GitHub Pages) fetches that JSON straight from
  `raw.githubusercontent.com` and renders one table of failing seeds with copy-pasteable repro
  commands, plus benchmark graphs written by a second script (`src/scripts/devhub.zig`).
- There is no server, no queue, no coordination between machines. Machines are independent random
  samplers; the merge function is designed so that concurrent uploads converge.

### 9.2 Process model

**Supervisor** (`src/scripts/cfo_supervisor.sh`, 38 lines, `scp`-ed to the machine by hand):

```sh
trap 'trap - INT TERM EXIT; kill 0' INT TERM EXIT      # kill the whole process group on exit
while true; do
    if [ $((RANDOM % 24)) -eq 0 ]; then rm -rf ./tigerbeetle; fi   # "Drop the cache every ~24 hours"
    (
        [ -d ./tigerbeetle ] || git clone https://github.com/tigerbeetle/tigerbeetle tigerbeetle
        cd tigerbeetle
        git fetch
        git switch --discard-changes --detach origin/main
        ./zig/download.sh
        ./zig/zig build scripts -- cfo &
        wait "$!"
    ) || sleep 10        # "Be resilient to cfo bugs and network errors, but avoid busy-loop retries."
done
```

**CFO CLI** (`cfo.zig`, `CLIArgs`): `--budget` (default `.minutes(60)`: "How long to run the cfo
before exiting (so that cfo_supervisor can refresh our code)"), `--refresh` (default
`.minutes(5)`: upload interval), `--timeout` (default `.minutes(30)`: per-seed kill deadline),
`--concurrency` (default `std.Thread.getCpuCount()`). Not supported on Windows.

**Environment** (`cfo.zig`, `main`): `DEVHUBDB_PAT` (read-write token for the devhubdb repo; if
unset, results are not uploaded and each record is logged as JSON instead) and `GH_TOKEN` (read-only,
for `gh pr list`; if unset, PRs are not fuzzed and `main` is fuzzed in place from `.`, which is the
local-testing mode).

**Namespaces.** The header comment says "The CFO uses Linux's process namespaces to ensure that all
descendant processes are reaped", but at 0.17.10 `cfo.zig` never calls `stdx.unshare`; the only
caller of `stdx.unshare.maybe_unshare_and_relaunch` is `src/vortex.zig:76` (Vortex relaunches
itself as PID 1 of new user+pid+net namespaces so its replica/driver children die with it). For
the plain fuzzers, cleanup relies on `child.kill()` plus the supervisor's `kill 0`. Treat the header
sentence as stale or Vortex-specific (inferred).

### 9.3 What gets fuzzed

**Fuzzer list** (`cfo.zig`, `const Fuzzer = enum`), with scheduling multipliers
(`Fuzzer.weights`) and core reservations (`Fuzzer.concurrency`):

| Fuzzer | Weight | Cores | Build step (`args_build`) | Exec args (`args_exec`) |
|---|---|---|---|---|
| `vopr` | 8 | 1 | `vopr:build -Drelease` | (none) |
| `vopr_lite` | 8 | 1 | `vopr:build -Drelease` | `--lite` |
| `vopr_testing` | 8 | 1 | `vopr:build -Drelease -Dvopr-state-machine=testing` | (none) |
| `vopr_testing_lite` | 8 | 1 | same as `vopr_testing` | `--lite` |
| `vopr_debug` | 1 | 1 | `vopr:build` (Debug) | (none) |
| `lsm_forest` | 4 | 1 | `fuzz:build -Drelease` | `lsm_forest` |
| `lsm_cache_map`, `lsm_manifest_level`, `lsm_manifest_log`, `lsm_scan`, `lsm_tree`, `message_bus`, `state_machine` | 2 | 1 | `fuzz:build -Drelease` | the tag name |
| `canary`, `ewah`, `lsm_segmented_array`, `storage`, `vsr_free_set`, `vsr_superblock`, `vsr_multi_batch`, `signal` | 1 | 1 | `fuzz:build -Drelease` | the tag name |
| `vortex` | 1 | 3 | `vortex:build -Drelease` | `--log-debug --replica-count=3 --test-duration=10m` |
| `vortex_debug` | 1 | 1 | `vortex:build` | same as `vortex` |

`-Drelease` is ReleaseSafe (assertions stay on; `src/vopr.zig` asserts
`builtin.mode == .Debug or .ReleaseSafe`). "Lite" (`vopr.zig`, `options_lite`) is the swarm options
forced to 3 replicas, 0 standbys; the `testing` state machine swaps the accounting state machine for
a trivial one so the VOPR spends its time in consensus and storage.

**Targets** (`cfo.zig`, `run_fuzzers_prepare_tasks`, re-run every `--refresh`):

1. Branches `main` and `release`, each in an independent clone at `./working/main` and
   `./working/release` ("so that CFO and the fuzzer could be on different branches").
   `run_fuzzers_prepare_repository`: clone if no `.git`, `git fetch origin <branch>`,
   `git switch --detach FETCH_HEAD`, then `git rev-parse HEAD` and
   `git show -s --format=%ct <sha>` give `Commit{ sha: [40]u8, timestamp: u64 }`. Checkouts are
   reused so the Zig build cache survives.
2. Open PRs: `gh pr list --state open --json number,labels`; a PR qualifies if any label starts
   with `"fuzz "`. It is fetched as `git fetch origin refs/pull/<n>/head` into `./working/<n>`.
   A task is created for each fuzzer whose name matches a `fuzz <fuzzer>` label, **plus `canary`
   always** (`assert(pr_fuzzers_count >= 2)`).
3. Version skew: a fuzzer is scheduled on a branch only if that branch's own
   `src/scripts/cfo.zig` contains the text `"    <name>,"` (an enum field), read as a file, not
   compiled. VOPR variants are assumed always present. This lets `main`'s CFO drive an older
   `release` branch without failing on fuzzers that do not exist there.

A **task** is the triple `(fuzzer, commit sha, branch)` (`Tasks.Map` key). `branch` is carried in
records as a URL: `https://github.com/tigerbeetle/tigerbeetle` (main),
`.../tree/release`, or `.../pull/<n>` (`SeedRecord.Template.Branch`).

### 9.4 Scheduling across cores

All in `cfo.zig`, `run_fuzzers` and `Tasks`.

- **Slots.** `children: []?FuzzerChild` has `concurrency` slots; `concurrency_available: i32`
  starts at `concurrency`. Each loop iteration, for every empty slot while
  `concurrency_available > 0`: pick a task, subtract `fuzzer.concurrency()` (Vortex subtracts 3 and
  may drive the counter negative; the comment explains a multi-core fuzzer is allowed to start
  with one core free so the scheduler is not biased toward 1-core fuzzers), spawn. Invariant
  asserted every tick: `concurrency_max <= running_count <= concurrency_max + fuzzer_concurrency_max - 1`.
- **Tick.** `sleep_ns = 100 * ns_per_ms`. (The header says "every second"; the code is 100 ms. The
  CHANGELOG records the motivation: CPUs were "on average 40-45% idle" with the slower poll.)
- **Fair scheduler = weighted virtual runtime** (CFS/stride style). Each task has
  `runtime_virtual: u64`, "cumulative `runtime / weight`". `Tasks.sample` returns the active task
  with the **minimum `runtime_virtual`**, ties broken by **greater weight**. Every tick each
  running child adds `sleep_ns / task.weight` to its task; the same amount is also added at spawn
  time "to ensure that multiple fuzzers spawned in the same tick are spread out over tasks".
  Rationale (CHANGELOG, PR 2697): with naive random choice, long-running LSM fuzzers hogged the
  fleet and "only 1-10% of CFO time [was] spent on short running VOPR fuzzers".
- **Weights** (end of `run_fuzzers_prepare_tasks`): per-class budgets
  `weight_main = 1_000_000`, `weight_pull = 1_000_000`, `weight_release = 500_000`, the comment
  calls this "40:40:20 between main, pull requests, and release". Each task gets
  `(class_weight / active_task_count_in_class) * Fuzzer.weights[fuzzer]`. All PRs share the one
  pull budget.
- **Release boost.** If the release branch head is younger than 2 days
  (`now < commit_timestamp + 2 * s_per_day`), `weight_release` is tripled ("usually on Friday ...
  until Monday").
- **Generations.** Each refresh calls `Tasks.soft_remove_all` (`generation += 1`) and re-`put`s the
  current targets. Tasks not re-put stay in the list as inactive (their running children still
  finish and are recorded) but are never sampled. New tasks start at
  `runtime_virtual = tasks.runtime_virtual_init`, which ratchets up to the largest virtual runtime
  among re-activated tasks; a task that was inactive for more than one generation is reset to that
  value so it neither starves nor monopolizes (`Tasks.put`).
- **Seed choice.** `std.crypto.random.int(u64)` per spawn. No corpus, no coverage guidance, no
  seed partitioning between machines; duplicates across the fleet are statistically irrelevant.
- **Budget.** `budget_timer` and `refresh_timer` are `std.time.Timer`s that start after argument
  parsing; the header notes "the budget/refresh timers do not count time spent cloning or
  compiling". On the last iteration all still-running children are killed and **not recorded**.

### 9.5 Running one seed

`cfo.zig`, `run_fuzzers_start_fuzzer`.

- **Displayed command vs. executed command.** The record's `command` is the one-liner a human
  pastes: `./zig/zig build <args_run> <seed>`, for example
  - `./zig/zig build vopr -Drelease -- 17154947449604939200`
  - `./zig/zig build vopr -Drelease -- --lite <seed>`
  - `./zig/zig build vopr -Drelease -Dvopr-state-machine=testing -- [--lite] <seed>`
  - `./zig/zig build vopr -- <seed>` (`vopr_debug`)
  - `./zig/zig build fuzz -Drelease -- lsm_tree <seed>`
  - `./zig/zig build vortex -Drelease -- --log-debug --replica-count=3 --test-duration=10m <seed>`

  The CFO itself runs two steps: `zig build -Dprint-exe <args_build>` (a build option that prints
  the artifact path instead of installing it; `build.zig`, `print_or_install`), then spawns
  `<exe> <args_exec> [--log=<fuzzer>_<seed>.log] <seed>`. Reasons given in the comment: build time
  is excluded from runtime, and the fuzzer's own exit status is visible (not `zig build`'s). If the
  build fails the exe becomes the literal `false`, so a broken build shows up as a failing seed.
- **Completion detection without threads.** The child gets a stdin pipe set `O_NONBLOCK`; each
  tick the CFO writes one byte. `error.BrokenPipe` means the child is gone; `error.WouldBlock`
  (pipe full) is ignored. Then `child.wait()`.
- **Timeout.** If `now - seed_timestamp_start > --timeout` (30 min) the child is `kill()`ed and
  recorded as a **failure** (hangs are bugs). Zig's `Child.kill` sends SIGTERM on POSIX
  (inferred from the std library; consistent with the SIGTERM special-casing below).
- **Classification** (`term_adapted`):

| Termination | Meaning | Recorded? |
|---|---|---|
| `Exited 0` | pass | yes, `ok = true` |
| `Signal SIGKILL` (or Vortex exit code 128+9) | "likely OOM", someone else killed it | **no**, neither pass nor fail |
| `Signal SIGTERM` (or Vortex 128+15) **on the last iteration** | the CFO's own budget cancel | **no** |
| anything else: nonzero exit, SIGABRT (assertion), SIGSEGV, SIGTERM from timeout | failure | yes, `ok = false`, `debug = "{term}"` |

  The VOPR's own exit codes (`src/testing/cluster.zig`, `Failure`): `crash = 127`,
  `liveness = 128`, `correctness = 129`. The CFO does not interpret them beyond nonzero; they end
  up in the `debug` string. A sanity assert guarantees SIGABRT is never dropped.
- **Resource limits.** Not imposed by the CFO: every fuzz binary and the VOPR call
  `fuzz.limit_ram()` first thing in `main` (`src/testing/fuzz.zig`), which does
  `setrlimit(.AS, 20 GiB)` on Linux. An allocation failure then becomes an ordinary crash, while a
  kernel OOM kill (SIGKILL) is ignored as noise.
- **Logs.** Only Vortex captures logs (`Fuzzer.capture_logs`): it is passed
  `--log=<fuzzer>_<seed>.log`; on failure the **last 4 MiB** (`log_size_max`) are kept and uploaded.
  Deterministic fuzzers need no logs: the seed plus the commit is the whole repro.
- **Minimization.** There is no input shrinking. "Simple minimization" (CHANGELOG) is purely the
  merge preference for the failing seed with the **shortest wall-clock duration**
  (`order_by_seed_duration`: "prefer short duration, as coarse seed minimization").

### 9.6 Seed record data model

`cfo.zig`, `const SeedRecord`. Serialized by `std.json` with 2-space indentation and null optionals
omitted (`to_json`); the whole file is one JSON array.

| Field | Type | Notes |
|---|---|---|
| `commit_timestamp` | u64, Unix seconds | committer time of `commit_sha`; primary sort key (newest first) |
| `commit_sha` | 40 hex chars | |
| `fuzzer` | string | deliberately a string, not the enum, "to support deserializing unknown fuzzers" |
| `ok` | bool | `true` only for exit code 0 |
| `count` | u32, default 1 | "number of seeds merged into the current one"; total passing runs for the commit/fuzzer |
| `seed_timestamp_start` | u64 seconds | taken after spawn, so build time is excluded |
| `seed_timestamp_end` | u64 seconds | |
| `seed` | u64 | |
| `command` | string | repro command; excluded from ordering |
| `branch` | string URL | main / release / pull URL; drives UI grouping; excluded from ordering |
| `debug` | optional string | free-form, in practice the formatted termination status |
| `log` | optional string | path relative to the devhubdb root, `./fuzzing/logs/<32 hex digits>.vopr` (random u128 name) |

Example from the `"cfo: deserialization"` test:

```json
{ "commit_timestamp": 1721095881, "commit_sha": "c4bb1eaa658b77c37646d3854dd911adba71b764",
  "fuzzer": "canary", "ok": false, "count": 1,
  "seed_timestamp_start": 1721096948, "seed_timestamp_end": 1721096949,
  "seed": 17154947449604939200,
  "command": "./zig/zig build -Drelease fuzz -- canary 17154947449604939200",
  "branch": "https://github.com/tigerbeetle/tigerbeetle/pull/2104" }
```

**devhubdb layout:** `fuzzing/data.json` (the array above, read with a 1 MiB cap),
`fuzzing/logs/*.vopr` (Vortex log tails), `devhub/data.json` (benchmark metrics, section 9.9). No
separate totals file: totals live in `count`.

### 9.7 Merge algorithm

`SeedRecord.merge(arena, options, current, new)`; `MergeOptions{ commit_count_max = 32,
seed_count_max = 4 }`. Implemented as **concatenate, sort, one counting pass**, which makes it
deterministic and idempotent (`merge(x, []) == x` is what the upload loop uses to detect "nothing
to do").

Sort order (`SeedRecord.order`, wrapped by `less_than`):

1. Records of the **latest release commit** first (pinned "even if it is old").
2. `commit_timestamp` descending (fresh commits first; merged/inactive PRs sink).
3. `commit_sha`, then `fuzzer` (ascending, grouping keys).
4. `ok` ascending: **failures before passes**.
5. `count` descending.
6. Duration: failures shortest first; passes **longest first** ("near-timeouts might be
   interesting, and it gives us a p100 for the fuzzer's runtime"); skipped for canary.
7. `seed_timestamp_start` ascending, **older seeds preferred** so the displayed failing seed is
   stable while someone debugs it; reversed for canary ("to show that the canary is alive").
8. `seed_timestamp_end`, then `seed`.

Counting pass:

- Stop after `commit_count_max` distinct commits (older commits fall off entirely).
- Within a (commit, fuzzer) group, skip a record whose `seed` equals the previous one (dedup).
- A passing record that directly follows a passing record of the same commit and fuzzer is
  **folded into it** (`count += record.count`) instead of appended. Result: at most one passing
  record per (commit, fuzzer), carrying the run total.
- Otherwise append while the group has fewer than `seed_count_max` records.

Consequences worth knowing: up to 4 distinct failing seeds are kept per (commit, fuzzer); the
passing total is retained only when fewer than 4 failures exist (with 4 failures the pass record
has no slot and its count is dropped); branch is not part of the grouping, so the same commit on
`main` and `release` shares counts.

### 9.8 Upload protocol

`cfo.zig`, `upload_results`, run every `--refresh` and once at the end:

1. Delete `./devhubdb`; `git clone --single-branch --depth 1
   https://oauth2:<DEVHUBDB_PAT>@github.com/tigerbeetle/devhubdb.git devhubdb`.
2. Up to **32 attempts**: `git fetch origin main`; `git reset --hard origin/main`; parse
   `fuzzing/data.json`; merge with this run's seeds; if the result is byte-identical to the old
   records, log "no seeds uploaded" and return (happens when the CFO's commit is not among the 32
   newest).
3. Write log files only for new seeds that survived the merge; `git rm` any
   `fuzzing/logs/*.vopr` no longer referenced; write `data.json`; `git add`.
4. Commit as author = machine hostname, email `bot@tigerbeetle.com`
   (`Shell.git_env_setup(.{ .use_hostname = true })`); the message is a single seedling emoji.
5. `git push`. On failure log "conflict, retrying" and loop from step 2 (optimistic concurrency:
   re-read, re-merge, retry). After 32 failures: `error.CanNotPush`, which crashes the CFO; the
   supervisor sleeps 10 s and restarts.

After a successful upload the in-memory seed list is cleared. Because the merge is a pure function
of the union of records, concurrent uploaders converge regardless of push order.

### 9.9 devhub.zig: per-merge metrics

`src/scripts/devhub.zig` (still present at HEAD). Run by CI on every merge to `main`
(`.github/workflows/ci.yml` job `devhub`: `zig build ci -- devhub`, under `sudo -E` "to enable
memory locking for accurate RSS stats"; on PRs `devhub-dry-run` runs the same code with
`--skip-kcov` and no tokens so it is exercised but never uploads).

One `MetricBatch{ timestamp: u64 (commit time, seconds), metrics: []Metric{ name, unit, value: u64 },
attributes: { git_repo, branch, git_commit } }` per merge with these metrics:
`ci pipeline duration` (s), `executable size` (bytes), `TPS` (count), `batch p100` (ms),
`query p100` (ms), `RSS` (bytes), `datafile` (bytes), `datafile empty` (bytes),
`replica log lines` (count), `checksum(message_size_max)` (us), `build time debug` (ms),
`build time` (ms), `format time` (ms), `startup time - 8GiB grid cache` (ms), `stats count`
(count), `repl single command` (ms), `inspect integrity time` (ms).

Sources: a release binary is built through the real release script, then
`./tigerbeetle benchmark --validate --id-order=sequential --checksum-performance
--log-debug-replica --file=datafile-devhub`; values are scraped from stdout with
`get_measurement(stdout, "label", "unit")` (expects `label = <int> unit`); wall-clock timers
around `zig build install`, `tigerbeetle format`, `inspect integrity`, startup (time until a
hand-rolled ping gets an eviction reply), and one REPL command.

Storage: the batch is appended as **one JSON line** to `devhub/data.json` in devhubdb
(newline-delimited JSON: "This 'database' is just a newline-delimited JSON file in a git repo"),
using the same clone / fetch / reset / commit / push / retry-32 loop as the CFO (`upload_run`;
author "TigerBeetle Bot"). The same batch is POSTed to Nyrkio
(`https://nyrkio.com/api/v0/result/devhub`, bearer `NYRKIO_TOKEN`) for change-point detection.

Coverage (`devhub_coverage`): `kcov --include-path=./src ./src/devhub/coverage` over `test-unit`,
`fuzz --events-max=500000 lsm_tree 92`, `fuzz --events-max=500000 lsm_forest 92` and `vopr 92`;
the output directory is published with the page.

### 9.10 devhub web UI

`src/devhub/{index.html,devhub.js,style.css}`: no build step, no framework, one external script
(ApexCharts 3.45.2 from cdnjs). Deployed by uploading `./src/devhub` as a GitHub Pages artifact on
each `main` merge. `window.onload` runs three independent loaders:

- **`main_release_rotation`**: release manager for last/this/next week =
  `candidates[(iso_week + shift + {0,1,2}) % candidates.length]` over a hard-coded,
  alphabetically sorted (asserted) list of GitHub handles.
- **`main_seeds`**: fetches `fuzzing/data.json` from
  `raw.githubusercontent.com/tigerbeetle/devhubdb/main/`, the GitHub issues API (open PRs, and the
  count of issues lacking the `triaged` label), and the Actions API (failed `main` runs in the
  last 7 days = "Flakes" badge). Table columns: **Commit, Author, Fuzzer, Command, Duration,
  Freshness, Count**. Default filter, in record order (already sorted by the CFO):
  - skip records of PRs that are no longer open;
  - skip `canary` except on `main`;
  - always show `vopr` on the `release` branch (so the weekend's run count is visible);
  - otherwise show a record if it is failing, or belongs to a PR, **and** it is the first record
    for that `branch + fuzzer`: "one seed per fuzzer per commit; exclude successes for the main
    branch".
  - `?fuzzer=X&commit=Y` (the fuzzer cell is a link) shows every stored seed for that pair;
    `?all` disables filtering.

  Each row: short sha linking to the commit plus `(main)`, `(release)` or `#PR` link; PR author;
  the command in a `<code>` element that copies to the clipboard on click; duration with an
  optional `(log)` link; "N ago" freshness; count. Passing rows are styled `success`.
  **Canary inversion:** a canary record is "success" when `!record.ok` and turns into a failure
  with a warning if its newest failing seed is older than 3 hours. Since the canary fuzzer exits 1
  when `seed % 100 == 0` (`fuzz_tests.zig`, `main_single`), a fresh failing canary proves the
  whole pipeline (schedule, build, run, classify, merge, push, render) is alive.
  **VPM** ("VOPRs per minute"): from the first `vopr` record on main with `count > 100`,
  `count * 60 / (now - seed_timestamp_start)`. The page asserts that if any failure exists on main
  at least one failing row is rendered.
- **`main_metrics`**: fetches `devhub/data.json`, parses line by line, newest first, at most 200
  batches; `batches_to_series` transposes batches into one series per metric name; `plot_series`
  draws one ApexCharts line chart per metric (x = batch index, evenly spaced, not time; charts
  share a group so tooltips sync; clicking a point opens the commit). The three series with the
  largest week-over-week mean change (`outlier_score`: `|mean(last 7d) - mean(prior 7d)| / prior`)
  are moved to the top and drawn in red. `?metric=` narrows to one metric. A small "Performance
  tests" table shows the latest batches containing `Cluster TPS` ("CSO") and `CPO TPS` ("CPO");
  those metrics are not produced by `devhub.zig`, so some other job appends them (inferred).
- Static links: assigned code review, issue triage, coverage report, flakes, raw data, Nyrkio.

### 9.11 Fuzzing support library

**`src/fuzz_tests.zig`** is the single `fuzz` binary. CLI: `fuzz [--events-max=N] <fuzzer> [seed]`.
It compiles with the `test_min` config and `config_verify = true` (extra `constants.verify`
assertions), quiets noisy log scopes, and dispatches through a comptime tuple `Fuzzers`:
`ewah`, `lsm_scan`, `lsm_cache_map`, `lsm_forest`, `lsm_manifest_log`, `lsm_manifest_level`,
`lsm_segmented_array`, `lsm_tree`, `message_bus`, `storage`, `vsr_free_set`, `vsr_superblock`,
`vsr_superblock_quorums`, `vsr_multi_batch`, `signal`, `state_machine`, plus two pseudo-fuzzers:
`canary` ("intentionally fails, to test fuzzing infrastructure itself") and `smoke`. Each module
exports `pub fn main(gpa, fuzz.FuzzArgs) !void`. With no seed, one is drawn from
`std.crypto.random` and logged as `Fuzz seed = N`.

**Smoke mode** (`main_smoke`): `zig build fuzz -- smoke` (part of `zig build ci -- test`) runs
every fuzzer in-process with fixed seed 123 and a hand-tuned small `events_max` (for example
`lsm_forest` 10_000, `lsm_tree` 400, `message_bus` 10, `vsr_superblock` 3, null for cheap ones),
and logs an error if one takes more than 10 s. This is what keeps fuzzers compiling and roughly
working on every PR. CI additionally runs the VOPR once per state machine with **the commit hash
as the seed** (`build.zig`, `build_ci`).

**`src/testing/fuzz.zig`** (HEAD: `src/stdx/testing/fuzz.zig`):

- `FuzzArgs{ seed: u64, events_max: ?usize }`; each fuzzer does `args.events_max orelse <default>`
  (defaults range from 10 for `vsr_superblock` to 1e9 for `lsm_cache_map`).
- `parse_seed(bytes)`: decimal u64, or, if exactly 40 characters, a git hash parsed as `u160` hex
  and truncated to u64: "we run it with different, 'random' seeds, but the failures remain
  reproducible just from the commit hash".
- `random_int_exponential(prng, T, avg)`: exponentially distributed unsigned int (the one place
  floats are still used, acknowledged in a comment).
- `random_enum_weights(prng, Enum)`: swarm-testing weights (same as `PRNG.enum_weights`).
- `random_id(prng, Int, .{ .average_hot, .average_cold })`: coin flip between a small and a large
  id space, to get both collisions and cache overflow.
- `range_inclusive_ms`, `DeclEnumExcludingType(T, exclude)` (an enum of a type's public
  declarations, for API-level swarm fuzzing), `limit_ram()` (RLIMIT_AS = 20 GiB, Linux only).

**`src/stdx/prng.zig`** (`stdx.PRNG`):

- Algorithm: xoshiro256++ (`next`), state `[4]u64` seeded from a u64 via SplitMix64
  (`from_seed`). Same as Zig's `DefaultPrng`, but vendored to "remove floating point from the API,
  to ensure determinism", and to isolate tests from stdlib and algorithm churn.
- `Ratio{ numerator, denominator }` with `ratio(a, b)` (asserts `a <= b`, `b > 0`), parsed from
  CLI flags as `a/b` or `0`. `chance(ratio)` is `int_inclusive(u64, d - 1) < n`. All
  probabilities in the simulator are Ratios; a test named "no floating point please" greps the
  file for `f32`/`f64`.
- Closed ranges only: `int_inclusive(T, max)` (Lemire's unbiased method), `range_inclusive(T, min,
  max)`, `index(slice)`; `int_exclusive` is a `@compileError`. Also `int`, `boolean`, `bit`,
  `fill`, `shuffle`, `enum_uniform`, `error_uniform`.
- Weighted choice: `enum_weighted(Enum, weights)` (weights are a struct with one u64 per variant),
  `chances(.{ .a = 1, .b = 3 })`.
- Swarm testing: `enum_weights(Enum)` picks a subset size uniformly in `1..=n`, selects that many
  variants with `Combination`, gives each a weight uniform in `1..=100` and zero to the rest:
  "some variants are disabled completely, and the rest have wildly different probabilities".
- `Combination{ total, sample }.take(prng)` (sequential k-of-n sampling via
  `chance(ratio(k_left, n_left))`), `Reservoir.replace(prng, weight)` (weighted single-item
  reservoir sampling).
- `FuzzIterations.more()`: time-boxed loop for fuzz-style unit tests (at least 10 iterations, then
  stop after 100 ms).
- Reproducibility guard: snapshot tests pin output distributions for seed 92, so an accidental
  change to the stream fails CI.

### 9.12 Release qualification

`docs/internals/releases.md` (0.17.10) and the CHANGELOG entry introducing the process:

- **Friday:** the release manager checks devhub (no VOPR failures, "a bunch of successful runs for
  recent commits", sane graphs), merges the changelog, then
  `git push origin origin/main:release`. "From this point on, the CFO will be fuzzing the release
  branch over the weekend."
- The CFO's 2-day rule triples the release weight during exactly that window (section 9.4), the merge
  pins the latest release commit to the top of `data.json`, and the UI always shows the release
  `vopr` row with its run count.
- **Monday:** a different release manager checks "that there are no VOPR failures on the release
  branch", triages issues, and triggers the release workflow from the `release` branch. Skipping a
  release is explicitly cheap and the default answer when in doubt.

### 9.13 What changed at HEAD (0.18.0 era, `a2a3c70`)

- **Removed from the public tree:** `src/scripts/cfo.zig`, `src/scripts/cfo_supervisor.sh`,
  `src/devhub/`, `docs/internals/releases.md`, `.github/workflows/ci.yml` (only `release.yml` and
  `release_validate.yml` remain), the `cfo` and `changelog` subcommands in `src/scripts.zig`, and
  the "CFO" section of `docs/internals/HACKING.md`.
- **Explanation:** none specific. The 0.18.0 CHANGELOG entry only says the release "is larger than
  usual due to some major adjustments we have been making to our development infrastructure over
  the past few months". Where the CFO lives now is not stated in the repo; that it moved to
  non-public infrastructure is my inference.
- **Still present at HEAD and semantically unchanged:** `src/fuzz_tests.zig` (same fuzzer list,
  same smoke table and canary), `src/stdx/prng.zig` (API churn for Zig 0.16 only),
  `src/stdx/testing/fuzz.zig` (moved from `src/testing/fuzz.zig`), `src/scripts/devhub.zig` (same
  17 metrics and upload loop; it still writes coverage into `./src/devhub/coverage`, a directory
  that no longer exists in the tree), `build.zig` steps `fuzz`, `fuzz:build`, `vopr`, `vopr:build`,
  `-Dprint-exe`, and CI modes `devhub` / `devhub-dry-run`; VOPR exit codes 127/128/129.
- A leftover at both revisions: `src/vsr/superblock_quorums_fuzz.zig` has an empty `main` with
  "TODO: remove one CFO is updated"; the CFO enum and the `Fuzzers` tuple are kept in sync by
  hand, not by the compiler.

### 9.14 Lessons / traps for a Rust reimplementation

Patterns worth copying:

1. **A failure is (commit, fuzzer, seed) and nothing else.** Store the exact repro command in the
   record. This only works if every fuzzer is fully deterministic from a u64; logs are needed only
   for the non-deterministic end-to-end test.
2. **Orchestrator in the repo, driven by a dumb supervisor loop with a bounded budget.** Updating
   the fleet is `git push`; a crash costs a 10-second sleep.
3. **Build and exec separately.** Excludes compile time from seed duration, exposes the real exit
   status/signal, and turns a broken build into a visible failing seed.
4. **Three-way classification, not two.** Pass, fail, and "indeterminate" (SIGKILL/OOM, own
   cancellation). Timeouts are failures. Set an address-space rlimit inside the fuzzer so OOM is a
   crash you can see rather than a kernel kill you must ignore.
5. **Weighted virtual-runtime scheduling over (fuzzer, commit, branch) tasks.** Random selection
   starves short fuzzers of wall-clock share. Weight by importance and by branch class; boost the
   release candidate.
6. **Merge = concat + total order + single pass.** Deterministic, idempotent, commutative enough
   that uncoordinated machines converge; bounded size (32 commits, 4 seeds per group); failures
   beat passes; shortest failure first as free minimization; oldest failure wins so the displayed
   seed is stable; passes collapse into a count.
7. **Git repo as the database, static page as the dashboard.** Optimistic retry
   (fetch, reset, re-merge, push, up to 32 times). Zero servers to operate, full history for free.
8. **Canary fuzzer** that fails 1% of seeds, with a staleness alarm in the UI, to detect a
   silently dead pipeline.
9. **Smoke-run every fuzzer in CI** with a fixed seed and tiny event budget, and use the commit
   hash as the CI seed for the simulator: varied but reproducible.
10. **PRNG discipline:** own the generator, integer-only API, rational probabilities, closed
    ranges, snapshot-test the stream, and first-class swarm helpers (`enum_weights`).
11. **Label-driven PR fuzzing** and a string-typed `fuzzer` field so old data and other branches
    with different fuzzer sets keep deserializing.

Traps:

- The fuzzer list is duplicated (CFO enum vs. fuzz binary) and checked across branches by grepping
  source text; in Rust derive both from one enum and ask the target branch's binary for its list
  (for example `fuzz --list`).
- Passing counts are silently dropped when a group already holds 4 failing seeds, and `branch` is
  not part of the merge key; decide deliberately whether you want that.
- `data.json` is read with a 1 MiB cap and rewritten whole on every push; the design relies on the
  retention limits. The token is embedded in the clone URL (it can leak through process listings
  and error logs); prefer a credential helper.
- Wall-clock timeouts on an oversubscribed machine produce false failures (the Vortex liveness
  constant carries a note that the CFO oversaturates the CPU). Deterministic simulators should
  bound work in ticks/events and keep the wall-clock timeout only as a hang detector.
- stdin-pipe liveness polling requires that fuzzers never read stdin; in Rust use `try_wait()` or
  pidfd instead.
- The header comments in `cfo.zig` lag the code (1 s vs. 100 ms poll, namespace claim); trust the
  code.
- No corpus, coverage feedback or shrinking exists. That is a deliberate bet on swarm-randomized,
  assertion-dense deterministic simulation; if your simulator is weaker on assertions the bet pays
  less.
