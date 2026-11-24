# Deterministic simulation testing

The whole system runs in one process with no real disk, network or clock:
`wfe-sim` builds a **world** out of the production code and a seed.

A world is:

- one or two cells, each `shards` x 3 replicas of the real VSR + engine +
  LSM, on simulated disks;
- their gateways;
- a fleet of real SDK worker cores, versioned and unversioned, with payload
  codecs (compression and claim check) in the path;
- a driver that starts workflows, signals, cancels and terminates them;
- a **controller**: the same planners (`wfe-control`) the Kubernetes
  operator and the CLI run, rolling worker builds out, retiring old ones
  and moving the active role between cells;
- a simulated network joining all of it.

The seed decides the topology (cells, shards, concurrency), the workload,
every fault, and the **scenario**: which builds each deployment is asked to
run and when (rollbacks included), the rollout and sunset policies, and
when the active cell is asked to change. There is no fixed script. The same seed replays the same run, event for event; each run
ends with a digest of everything delivered, which is how determinism itself
is checked.

## Faults

- **Disks** lose or tear unsynced writes on a crash, and rot at rest.
- **The network** delays, reorders, drops and duplicates packets, and
  partitions nodes.
- **Processes** crash and restart: replicas, gateways and workers.
- **The controller** is interrupted halfway through a plan, acts on a plan
  it made from an old observation, and reads a clock that is off by up to
  two seconds.
- One seed in ten runs with no faults at all, to catch bugs that faults
  would mask.

Faults stop before the end of a run. The world then has to settle: every
workflow must finish, and everything must converge.

## What is checked

- Replicas of a shard commit the same log and hold byte-identical state.
  Every 256 ticks, replicas at the same op compare the LSM fingerprint
  (tables by checksum, plus the memtable), which reads no blocks; once the
  run settles, they compare a full scan of their contents.
- **Workflow outcomes**: every workflow's result is a function of its input,
  whatever it lived through. There are twelve kinds, covering activities in
  sequence and in parallel, timers, timer cancellation, signals, races,
  cancellation, termination, retries, failure, markers and large payloads.
- **History rules**: every history is well formed.
- **Signals exactly once**: a history holds exactly the signals that were
  acknowledged.
- **Versioning rules**: pinned workflows stay on their build, auto-upgrading
  ones only move forward, unversioned ones never run on a versioned worker.
- **Controller settled**: once faults stop, every deployment ends on the
  build that was last asked for, with no ramp left over, and old builds are
  retired only when nothing is pinned to them. A workflow stranded on a
  build with no workers fails the run.
- **Replicated deployments**: the standby cell ends with the same routing.
- **Replicated workflows**: a standby cell ends with the same workflows and
  the same histories as the active one; after a failover the new active
  cell finishes everything.

Two narrower suites sit underneath: `crates/sim/tests/vsr.rs` runs VSR alone
over a key-value state machine with heavier disk corruption, and
`crates/lsm/tests/model.rs` checks the storage engine against a model.

## Running it

```sh
wfe dst run                       # every core, random first seed, forever
wfe dst run --runs 200 --seed 1   # a fixed batch; exit code 1 on a failure
wfe dst replay --seed <seed> --ticks <ticks>
wfe dst findings --open
wfe dst status --dst-url http://127.0.0.1:7600
```

Build the fuzzer with `cargo build --profile sim -p wfe-cli`: optimized,
with debug assertions and overflow checks on. `--ticks` is the simulated
length of a run in 10 ms steps; `12000` is two simulated minutes. A replay
needs the same `--ticks` the run used, and the same source.

## The always-on box

One process uses every core and never stops:

```sh
wfe dst run --serve 0.0.0.0:7600 --log /var/lib/wfe/runs.jsonl
```

- `GET /status` is the live state as JSON: runs, failures, runs per minute,
  simulated time against wall time, totals of faults injected and work
  done, how often each check ran, in-flight seeds, recent runs, and every
  failing seed with the command that replays it.
- Point a UI at it from anywhere: `wfe ui --dst http://<box>:7600`. The
  **Simulation** page shows all of the above, and the findings log.
- In Kubernetes or under systemd, it is one container or unit running that
  command; restart it on a new revision and it carries on with new seeds.
  It stops cleanly on SIGTERM or Ctrl-C, after the runs in flight finish.

### History

`--history <file>` (or `WFE_DST_HISTORY`) appends one JSON line per minute:
the revision and session, the seeds that finished, runs, failures, wall and
simulated time with p50/p95/max run times and the slowest seed, and the
totals of faults injected and checks run. It is small enough to keep forever
(about 2 MB a day), and a restarted fuzzer reloads its runs-per-minute chart
from it.

```sh
wfe dst history -o table                  # every session: revision, seed range, runs, failures, speedup, faults
wfe dst history --revision 9c6c325        # one revision's sessions, as JSON
```

A session runs consecutive seeds, so `first_seed..=last_seed` is exactly what
passed on that revision (bar the few in flight when it stopped), and
`wfe dst run --seed <first_seed> --runs <n>` reruns it. Seeds only reproduce
on the same source.

`--log` keeps every run's full report; on the box it is rotated daily and
kept for a week.

`deploy/dst-box/provision.sh` builds one on Hetzner Cloud (default: a
8-core `cpx42` in Helsinki named `wfe-dst`) and joins it to Tailscale. It
needs `hcloud`, `gh` and `op` signed in, and is safe to rerun. On the box:

- `wfe-dst` runs the fuzzer on every core; findings go to
  `/home/wfe/findings/dst.jsonl`.
- `wfe-ui` serves the management UI on the box's Tailscale address, port
  7500. Hetzner's firewall only lets SSH in, so the UI is reachable only over
  Tailscale.
- `wfe-update.timer` checks `main` every 5 minutes; when it moved, it
  rebuilds and restarts both. A build that fails leaves the old one running.

`journalctl -u wfe-dst -u wfe-update` on the box shows what happened.

## Findings

Every failing run is appended to [`findings/dst.jsonl`](../findings/dst.jsonl)
(set `WFE_FINDINGS` to put it elsewhere): suite, seed, options, the
failure, the git revision, whether the tree was dirty, and a hash of the
source. Records are never deleted. When a bug is fixed, mark its seed:

```sh
findings/mark.py world 407509655152 "what was wrong and what changed"
```

so the file is both the history of what simulation caught and a corpus of
seeds worth replaying after any change to the code they touch.
