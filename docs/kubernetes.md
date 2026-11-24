# Kubernetes

`wfe operator run` manages three resources in the `wfe.dev/v1alpha1`
group. Install:

```sh
docker build -t wfe:dev .                      # one image for everything
wfe operator crds | kubectl apply -f -
kubectl apply -f deploy/operator.yaml
```

The operator runs cells from the image in `WFE_IMAGE` unless a `Cell` names
its own. `--namespace` restricts it to one namespace.

## Cell

```yaml
apiVersion: wfe.dev/v1alpha1
kind: Cell
metadata:
  name: east
spec:
  cellId: 1          # unique among cells that replicate to each other
  shards: 4          # fixed for the life of the cell
  replicas: 3        # nodes; each holds one replica of every shard
  role: Active       # or Standby
  storage: 10Gi      # per node; must hold `shards` data files
  dataFileMb: 1024   # capacity of each replica's data file
```

The operator creates:

- a StatefulSet `east` of `replicas` pods, started in parallel (a shard
  needs a majority of its replicas to start at all). Pod `i` is node `i`;
- a headless Service `east-nodes`, by which the nodes find each other;
- a Service `east`: clients, workers and the UI use
  `http://east.<namespace>.svc:7400`.

`kubectl get cells` shows the role, shards available, nodes ready and open
workflows as the cell itself reports them.

### Failing over

Roles are declared, and the operator moves the active role when two cells'
specs agree on it:

```sh
kubectl patch cell east --type merge -p '{"spec":{"role":"Standby"}}'
kubectl patch cell west --type merge -p '{"spec":{"role":"Active"}}'
```

It demotes `east`, waits for `west` to have everything, promotes `west`,
and turns replication around so `east` follows. The steps come from
`wfe-control`, the same planner the CLI uses and the simulator tests; an
interrupted failover continues from wherever the cells actually are.
It will not promote a cell while another is active and still declared
`Active`, or while a cell that might be active cannot be reached. If the
active cell is gone for good, annotate the one to promote:

```sh
kubectl annotate cell west wfe.dev/force-failover=true
```

`status.message` always says what the operator is waiting for.

## CellReplication

```yaml
apiVersion: wfe.dev/v1alpha1
kind: CellReplication
metadata:
  name: east-to-west
spec:
  source: east
  target: west
  paused: false
```

`status.state` is `Pending`, `CatchingUp`, `InSync` or `Paused`;
`status.pending` counts executions still to ship. Deleting the resource
removes the link. A failover does not need the reverse link declared: the
operator links the cell being promoted before it moves the role, and
afterwards leaves replication running from the new active cell only.

## WorkerDeployment

Runs versioned workers and drives worker versioning for them.

```yaml
apiVersion: wfe.dev/v1alpha1
kind: WorkerDeployment
metadata:
  name: shop
spec:
  cell: east
  replicas: 3
  rollout:
    strategy: Progressive        # AllAtOnce (default) | Progressive | Manual
    steps:
      - { rampPercent: 10, pauseSeconds: 300 }
      - { rampPercent: 50, pauseSeconds: 300 }
  sunset:
    scaledownAfterSeconds: 300
    deleteAfterSeconds: 3600
  template:                      # an ordinary pod template
    spec:
      containers:
        - name: worker
          image: registry.example/shop-worker:1.4.2
```

Every change to the pod template is a new **build** (its id is a hash of the
template unless `buildId` names it). For each build the operator runs a
Deployment `<name>-<build>` with `WFE_URL`, `WFE_DEPLOYMENT` and
`WFE_BUILD_ID` set in every container; in the SDK,
`WorkerOptions::versioned_from_env` picks them up.

A rollout then goes:

1. the new build's workers start beside the old ones and poll, which
   registers the version with the cell;
2. the operator moves new workflows to it: at once, through the ramp steps,
   or not at all (`Manual`: use `wfe deployment set-ramping/set-current` or
   the UI);
3. old builds keep running while the cell reports them `draining`, that is
   while open workflows are pinned to them;
4. once a build has been `drained` for `scaledownAfterSeconds` it is scaled
   to zero, and after `deleteAfterSeconds` its Deployment and its version
   are deleted.

Rollout state is not stored in the resource: each pass reads the routing
from the cell and decides the next step, so the operator can restart at any
point, and a ramp moved by hand is picked up from where it is.
`kubectl get workerdeployments` shows the target, current and ramping
builds; `status.builds` lists every build with its status and pinned
workflows.
