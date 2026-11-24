# WFE

A durable workflow engine in Rust, built so that the whole system can run
inside a deterministic simulator: its own storage engine, its own consensus,
and a fuzzer that runs new seeds around the clock.

- **Workflows** with activities, timers, signals, cancellation, retries and
  an event history; one input payload in, one result payload out.
- **Its own storage**: a log-structured merge tree over checksummed blocks,
  physically deterministic across replicas (`wfe-lsm`).
- **Replicated and sharded**: a *cell* is `shards` consensus groups, each
  replicated with Viewstamped Replication after TigerBeetle (`wfe-vsr`).
- **Cells replicate to cells**: a standby cell follows an active one and can
  take over, in order or by force.
- **Worker versioning from day 0**: deployments, pinned and auto-upgrading
  workflows, ramping, drainage.
- **Payload codecs** in the SDK: LZ4 compression and claim-check storage in
  any S3-compatible bucket.
- **One binary**, `wfe`: node, CLI, demo worker, fuzzer, management UI and
  Kubernetes operator. Output is JSON by default, so agents can drive it.

## Try it

```sh
cargo build --release -p wfe-cli
alias wfe=$PWD/target/release/wfe

wfe dev &                                 # a whole cell: 2 shards x 3 replicas
wfe worker &                              # the demo workflows
wfe workflow start --type greet --task-queue demo --id hello --input '{"name":"Ada"}'
wfe workflow result hello --wait-ms 5000
wfe workflow history hello -o table
wfe ui &                                  # http://127.0.0.1:7500
```

A second cell, replication and failover:

```sh
wfe dev --cell 2 --data-dir .wfe/west --http 127.0.0.1:7410 --listen 127.0.0.1:7411 --standby &
wfe cell replicate --to http://127.0.0.1:7410
wfe cell status -o table
wfe cell failover --to http://127.0.0.1:7410
wfe ui --cell east=http://127.0.0.1:7400 --cell west=http://127.0.0.1:7410
```

Worker versioning:

```sh
wfe worker --deployment shop --build-id v1 --behavior pinned &
wfe deployment set-current shop --build-id v1
wfe worker --deployment shop --build-id v2 --behavior pinned &
wfe deployment set-ramping shop --build-id v2 --percent 20
wfe deployment describe shop -o table      # v1 drains as its workflows finish
```

Every command takes `--help`; `-o table` is for people, the default JSON for
programs. Errors are `{"error":{"code","message"}}` on stderr; the exit code
is 1 when a request failed and 2 for a usage error.

## Simulation testing

```sh
wfe dst run --serve 0.0.0.0:7600          # every core, new seeds, forever
wfe dst replay --seed 407509655152 --ticks 6000
wfe dst findings --open
wfe ui --dst http://<fuzz-box>:7600       # watch it from anywhere
```

Each run builds one or two whole cells on simulated disks and a simulated
network, drives them with real SDK workers, injects faults, and checks the
outcome of every workflow. Every failure is appended to
[`findings/dst.jsonl`](findings/dst.jsonl) with its seed and the source
revision. See [docs/dst.md](docs/dst.md).

## Kubernetes

```sh
docker build -t wfe:dev .
wfe operator crds | kubectl apply -f -
kubectl apply -f deploy/operator.yaml
kubectl apply -f deploy/examples/cells.yaml -f deploy/examples/workers.yaml
kubectl get cells,cellreplications,workerdeployments
```

See [docs/kubernetes.md](docs/kubernetes.md).

## The SDK

```rust
use wfe_sdk::prelude::*;

async fn greet(ctx: WfContext, input: Payload) -> WorkflowResult {
    let name: String = input.to_json().map_err(|e| Failure::new(e.to_string()))?;
    let greeting = ctx.call("say_hello", Payload::json(&name).unwrap()).await?;
    ctx.timer(1_000).await;
    Ok(greeting)
}
```

A workflow takes one `Payload` and returns one; a payload is bytes plus
metadata, so what goes in it can grow without changing the API. The crate
docs (`cargo doc -p wfe-sdk --open`) show a worker and a client;
[`crates/cli/src/demo.rs`](crates/cli/src/demo.rs) is a worked example.

## Layout

| Crate | What it is |
|---|---|
| `wfe-core` | Ids, time, checksums, the API types, the block device trait. No clock, no threads, no entropy. |
| `wfe-lsm` | The storage engine. |
| `wfe-vsr` | Viewstamped Replication for one shard. |
| `wfe-engine` | The state machine each shard replicates: the CHASM-style execution framework, workflows, worker deployments, cell replication. |
| `wfe-control` | What an operator does next: rollout, sunset and failover planners shared by the operator, the CLI and the simulator. |
| `wfe-gateway` | A cell's stateless front door: routing to shards, long polls. |
| `wfe-sdk` | The Rust client and worker. |
| `wfe-codec` | Payload codecs: LZ4, claim check to memory, files or S3. |
| `wfe-sim` | The simulator, its workload and oracle, and the always-on fuzzer. |
| `wfe-server` | The production shell: files, sockets and a clock around the same core. |
| `wfe-ui` | The management UI (axum + maud, server-rendered). |
| `wfe-operator` | The Kubernetes operator. |
| `wfe-cli` | The `wfe` binary. |
| `wfe-rules` | Tests that hold the workspace to its own rules (what may depend on what). |

[docs/architecture.md](docs/architecture.md) explains how the pieces fit.

## Not there yet

Authentication (bind the HTTP ports and the UI to a trusted network),
namespaces, changing a cell's shard count, child workflows, schedules,
queries and updates.
