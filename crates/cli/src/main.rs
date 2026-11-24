//! `wfe`: everything needed to run and operate WFE, in one binary.
//!
//! Every command prints one JSON document on stdout (or a table with
//! `-o table`). Errors are one JSON document on stderr and a non-zero exit
//! code: 1 for a failed request, 2 for bad usage.

mod demo;
mod dst;
mod output;

use clap::{Args, Parser, Subcommand};
use output::{Format, Output, duration_ms, time};
use serde_json::json;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use wfe_core::api::*;
use wfe_core::{CellId, Error, Id128, Payload};
use wfe_sdk::{CellSummary, Client, Worker, WorkerOptions};
use wfe_server::{NodeConfig, NodeStatus};

#[derive(Parser)]
#[command(name = "wfe", version, about = "Run and operate WFE, a deterministic-simulation-tested workflow engine.")]
struct Cli {
    /// HTTP address of a node of the cell to talk to.
    #[arg(long, global = true, env = "WFE_URL", default_value = "http://127.0.0.1:7400")]
    url: String,
    /// Output format.
    #[arg(short, long, global = true, value_enum, default_value = "json")]
    output: Format,
    /// Payload codec: `none`, `lz4`, `fs:<dir>` (lz4, large payloads as
    /// files) or `s3` (lz4, large payloads in the bucket named by
    /// `WFE_S3_*`).
    #[arg(long, global = true, env = "WFE_CODEC", default_value = "none")]
    codec: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run one node of a cell.
    Server(ServerArgs),
    /// Run a whole cell in this process, for development.
    Dev(DevArgs),
    /// Start, inspect and manage workflows.
    #[command(subcommand, alias = "wf")]
    Workflow(WorkflowCommand),
    /// Worker deployments and their versions.
    #[command(subcommand)]
    Deployment(DeploymentCommand),
    /// Cells: status, replication, failover.
    #[command(subcommand)]
    Cell(CellCommand),
    /// Run the demo worker (workflows `greet`, `order`, `sleepy`, `flaky`).
    ///
    /// Inputs: `greet` {"name": "..."} runs one activity. `order` {"items": [...], "needs_approval": bool}
    /// reserves every item in parallel, optionally waits for an `approve` signal (giving up after
    /// `approval_timeout_ms`), then charges. `sleepy` {"ms": n} sleeps on one timer and stops early when
    /// canceled. `flaky` {"fail_times": n} runs an activity that fails `n` times first.
    Worker(WorkerArgs),
    /// Deterministic simulation testing.
    #[command(subcommand)]
    Dst(dst::DstCommand),
    /// Serve the management UI.
    Ui(UiArgs),
    /// The Kubernetes operator.
    #[command(subcommand)]
    Operator(OperatorCommand),
}

#[derive(Args)]
struct ServerArgs {
    #[arg(long, default_value_t = 1)]
    cell: CellId,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 3)]
    replicas: u8,
    /// This node's index (0..replicas). It hosts that replica of every shard.
    #[arg(long, env = "WFE_NODE")]
    node: u8,
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    /// Cluster (node to node) listen address.
    #[arg(long, default_value = "0.0.0.0:7401")]
    listen: SocketAddr,
    /// Client (HTTP) listen address.
    #[arg(long, default_value = "0.0.0.0:7400")]
    http: SocketAddr,
    /// Cluster addresses of all the cell's nodes, in node order.
    #[arg(long, value_delimiter = ',', required = true)]
    peers: Vec<String>,
    /// Another cell's cluster addresses, `<cell>=<addr>,<addr>`. Repeatable.
    /// Only needed when the cells cannot reach each other at the addresses
    /// they advertise.
    #[arg(long)]
    remote: Vec<String>,
    /// How other cells reach this cell's nodes, in node order. Defaults to
    /// `--peers`.
    #[arg(long, value_delimiter = ',')]
    advertise: Vec<String>,
    /// Capacity of each replica's data file, in mebibytes.
    #[arg(long, default_value_t = 1024)]
    data_file_mb: u64,
}

#[derive(Args)]
struct DevArgs {
    #[arg(long, default_value_t = 1)]
    cell: CellId,
    #[arg(long, default_value_t = 2)]
    shards: u32,
    #[arg(long, default_value_t = 3)]
    replicas: u8,
    #[arg(long, default_value = ".wfe/cell")]
    data_dir: PathBuf,
    #[arg(long, default_value = "127.0.0.1:7401")]
    listen: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:7400")]
    http: SocketAddr,
    /// Another cell's cluster addresses, `<cell>=<addr>`. Repeatable.
    #[arg(long)]
    remote: Vec<String>,
    /// Start as a standby (a fresh cell is active).
    #[arg(long)]
    standby: bool,
    #[arg(long, default_value_t = 64)]
    data_file_mb: u64,
}

#[derive(Args)]
struct RunRef {
    /// The workflow id.
    workflow_id: String,
    /// A specific run; the latest when omitted.
    #[arg(long)]
    run_id: Option<String>,
}

#[derive(Subcommand)]
enum WorkflowCommand {
    /// Start a workflow.
    Start {
        /// The workflow type the worker registered.
        #[arg(long = "type")]
        workflow_type: String,
        #[arg(long)]
        task_queue: String,
        /// Generated when omitted.
        #[arg(long)]
        id: Option<String>,
        /// The workflow's one input, as JSON.
        #[arg(long)]
        input: Option<String>,
        /// Read the input from a file (`-` for stdin).
        #[arg(long, conflicts_with = "input")]
        input_file: Option<PathBuf>,
        /// Return the open run instead of failing if the id is running.
        #[arg(long)]
        use_existing: bool,
        #[arg(long, default_value_t = 0)]
        execution_timeout_ms: u64,
        /// Wait this long for the result before returning.
        #[arg(long)]
        wait_ms: Option<u64>,
    },
    Describe(RunRef),
    /// The run's event history.
    History(RunRef),
    /// Wait for the workflow to close and print its result.
    Result {
        #[command(flatten)]
        run: RunRef,
        #[arg(long, default_value_t = 30_000)]
        wait_ms: u64,
    },
    Signal {
        workflow_id: String,
        #[arg(long)]
        name: String,
        #[arg(long)]
        input: Option<String>,
    },
    /// Ask the workflow to stop; it may clean up first.
    Cancel {
        workflow_id: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
    /// Stop the workflow now.
    Terminate {
        workflow_id: String,
        #[arg(long, default_value = "")]
        reason: String,
    },
    List {
        /// running, completed, failed, canceled, terminated, timed_out, continued_as_new
        #[arg(long)]
        status: Option<String>,
        #[arg(long = "type")]
        workflow_type: Option<String>,
        #[arg(long)]
        id_prefix: Option<String>,
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
    /// Pin a workflow to a version, make it auto-upgrade, or clear the override.
    SetOverride {
        workflow_id: String,
        /// `<deployment>:<build id>`
        #[arg(long, conflicts_with_all = ["auto_upgrade", "clear"])]
        pinned: Option<String>,
        #[arg(long, conflicts_with = "clear")]
        auto_upgrade: bool,
        #[arg(long)]
        clear: bool,
    },
}

#[derive(Subcommand)]
enum DeploymentCommand {
    List,
    Describe {
        deployment: String,
    },
    /// Send new and auto-upgrading workflows to a build.
    SetCurrent {
        deployment: String,
        /// Omit to route to unversioned workers.
        #[arg(long)]
        build_id: Option<String>,
    },
    /// Send a percentage of workflows to a build before making it current.
    SetRamping {
        deployment: String,
        /// Omit to stop ramping.
        #[arg(long)]
        build_id: Option<String>,
        #[arg(long, default_value_t = 0)]
        percent: u8,
    },
    /// Forget a drained version.
    DeleteVersion {
        deployment: String,
        #[arg(long)]
        build_id: String,
    },
    Delete {
        deployment: String,
    },
}

#[derive(Subcommand)]
enum CellCommand {
    /// Shards, role, workflow counts and replication links.
    Describe,
    /// This node's replicas (view, op, commit, primary).
    Status,
    /// Replicate this cell to another.
    Replicate {
        /// HTTP address of a node of the target cell.
        #[arg(long)]
        to: String,
    },
    /// Stop replicating to a cell.
    Unreplicate {
        #[arg(long)]
        cell: CellId,
    },
    Pause {
        #[arg(long)]
        cell: CellId,
    },
    Resume {
        #[arg(long)]
        cell: CellId,
    },
    /// Make this cell active (accepts writes) or standby.
    SetRole {
        #[arg(long, conflicts_with = "standby", required_unless_present = "standby")]
        active: bool,
        #[arg(long)]
        standby: bool,
        /// Defaults to one above the cell's current epoch.
        #[arg(long)]
        epoch: Option<u64>,
    },
    /// Move the active role from this cell to another.
    Failover {
        /// HTTP address of a node of the cell to promote.
        #[arg(long)]
        to: String,
        /// Promote without waiting for this cell (it is gone).
        #[arg(long)]
        force: bool,
        #[arg(long, default_value_t = 30_000)]
        wait_ms: u64,
    },
}

#[derive(Args)]
struct WorkerArgs {
    #[arg(long, env = "WFE_TASK_QUEUE", default_value = "demo")]
    task_queue: String,
    /// Defaults to the host name (the pod name, in Kubernetes).
    #[arg(long, env = "HOSTNAME")]
    identity: Option<String>,
    /// Run as a versioned worker of this deployment. The operator sets
    /// `WFE_DEPLOYMENT` and `WFE_BUILD_ID` in the workers it runs.
    #[arg(long, env = "WFE_DEPLOYMENT", requires = "build_id")]
    deployment: Option<String>,
    #[arg(long, env = "WFE_BUILD_ID", requires = "deployment")]
    build_id: Option<String>,
    /// What this build's workflows do when another version becomes current.
    #[arg(long, value_parser = ["pinned", "auto-upgrade"], default_value = "auto-upgrade")]
    behavior: String,
}

#[derive(Args)]
struct UiArgs {
    #[arg(long, default_value = "127.0.0.1:7500")]
    listen: SocketAddr,
    /// A cell to manage: `<name>=<url>[,<url>...]`. Repeatable. Defaults to
    /// `--url`.
    #[arg(long = "cell")]
    cells: Vec<String>,
    /// Address of a `wfe dst run --serve` to show.
    #[arg(long, env = "WFE_DST_URL")]
    dst: Option<String>,
}

#[derive(Subcommand)]
enum OperatorCommand {
    /// Watch Cell, CellReplication and WorkerDeployment resources.
    Run {
        /// Only this namespace; all namespaces when omitted.
        #[arg(long)]
        namespace: Option<String>,
        /// The image cells run when their spec names none.
        #[arg(long, env = "WFE_IMAGE", default_value = "wfe:latest")]
        image: String,
    },
    /// Print the custom resource definitions as YAML.
    Crds,
}

struct Failed(i32, Error);

impl From<Error> for Failed {
    fn from(e: Error) -> Failed {
        Failed(1, e)
    }
}

impl From<anyhow::Error> for Failed {
    fn from(e: anyhow::Error) -> Failed {
        Failed(1, Error::new(wfe_core::ErrorCode::Internal, format!("{e:#}")))
    }
}

fn usage(message: impl Into<String>) -> Failed {
    Failed(2, Error::invalid(message))
}

type Done = Result<Output, Failed>;

fn main() {
    let cli = Cli::parse();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_env("WFE_LOG").unwrap_or_else(|_| "info".into()))
        .with_writer(std::io::stderr)
        .init();
    let format = cli.output;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a multi-thread tokio runtime builds unless the OS is out of threads");
    let code = match runtime.block_on(run(cli)) {
        Ok(output) => {
            output.print(format);
            0
        }
        Err(Failed(code, error)) => {
            eprintln!("{}", json!({ "error": { "code": error.code.as_str(), "message": error.message } }));
            code
        }
    };
    runtime.shutdown_background();
    std::process::exit(code);
}

fn client(cli: &Cli, url: &str) -> Result<Client, Failed> {
    use wfe_sdk::codec::{Chain, Compression, FsStore, S3Config, S3Store};
    let client = Client::connect(url);
    Ok(match cli.codec.as_str() {
        "none" | "" => client,
        "lz4" => client.with_codec(Compression { threshold: 1024 }),
        "s3" => {
            let config = S3Config::from_env()
                .ok_or_else(|| usage("--codec s3 needs WFE_S3_ENDPOINT, WFE_S3_BUCKET and credentials"))?;
            client.with_codec(Chain::standard(S3Store::new(config), 1024, 64 * 1024))
        }
        other => match other.strip_prefix("fs:") {
            Some(dir) => client.with_codec(Chain::standard(FsStore { root: PathBuf::from(dir) }, 1024, 64 * 1024)),
            None => return Err(usage(format!("unknown codec {other:?}: use none, lz4, fs:<dir> or s3"))),
        },
    })
}

async fn run(cli: Cli) -> Done {
    let wfe = client(&cli, &cli.url)?;
    match &cli.command {
        Command::Server(args) => server(args).await,
        Command::Dev(args) => dev(args).await,
        Command::Workflow(command) => workflow(&wfe, command).await,
        Command::Deployment(command) => deployment(&wfe, command).await,
        Command::Cell(command) => cell(&cli, &wfe, command).await,
        Command::Worker(args) => worker(wfe, args).await,
        Command::Dst(command) => dst::run(command).await,
        Command::Ui(args) => {
            let mut cells = Vec::new();
            for spec in &args.cells {
                let (name, urls) = spec
                    .split_once('=')
                    .ok_or_else(|| usage(format!("--cell {spec:?}: expected <name>=<url>[,<url>]")))?;
                cells.push(wfe_ui::CellConfig {
                    name: name.to_string(),
                    urls: urls.split(',').map(str::to_string).collect(),
                });
            }
            if cells.is_empty() {
                cells.push(wfe_ui::CellConfig { name: "cell".into(), urls: vec![cli.url.clone()] });
            }
            wfe_ui::serve(wfe_ui::UiConfig { listen: args.listen, cells, dst: args.dst.clone() }).await?;
            Ok(Output::ok())
        }
        Command::Operator(OperatorCommand::Crds) => {
            print!("{}", wfe_operator::crds()?);
            std::process::exit(0);
        }
        Command::Operator(OperatorCommand::Run { namespace, image }) => {
            wfe_operator::run(wfe_operator::OperatorConfig { namespace: namespace.clone(), image: image.clone() })
                .await?;
            Ok(Output::ok())
        }
    }
}

fn remotes(specs: &[String]) -> Result<BTreeMap<CellId, Vec<String>>, Failed> {
    let mut cells = BTreeMap::new();
    for spec in specs {
        let parsed = spec.split_once('=').and_then(|(cell, addrs)| Some((cell.parse::<CellId>().ok()?, addrs)));
        let (cell, addrs) =
            parsed.ok_or_else(|| usage(format!("--remote {spec:?}: expected <cell>=<addr>[,<addr>]")))?;
        cells.insert(cell, addrs.split(',').map(str::to_string).collect());
    }
    Ok(cells)
}

async fn serve_forever(config: NodeConfig, standby: bool) -> Done {
    let node = wfe_server::start(config).await?;
    if standby {
        for _ in 0..100 {
            match node.call(ApiRequest::DescribeCell).await {
                Ok(ApiResponse::Cell(info)) if u32::try_from(info.shards.len()) == Ok(info.shard_count) => {
                    let summary = CellSummary::of(&info);
                    let still_the_fresh_active_cell = summary.role != "standby" && summary.epoch <= 1;
                    if still_the_fresh_active_cell {
                        node.call(ApiRequest::SetRole { active: false, epoch: summary.epoch + 1 }).await?;
                    }
                    break;
                }
                _ => tokio::time::sleep(std::time::Duration::from_millis(200)).await,
            }
        }
    }
    eprintln!(
        "{}",
        json!({ "listening": { "http": format!("http://{}", node.http), "cluster": node.cluster.to_string() } })
    );
    tokio::signal::ctrl_c().await.map_err(anyhow::Error::from)?;
    Ok(Output::json(json!({ "stopped": true })))
}

async fn server(args: &ServerArgs) -> Done {
    if args.peers.len() != args.replicas as usize {
        return Err(usage(format!(
            "--peers lists {} nodes but the cell has {} replicas",
            args.peers.len(),
            args.replicas
        )));
    }
    if args.node >= args.replicas {
        return Err(usage(format!("--node {} is out of range for {} replicas", args.node, args.replicas)));
    }
    let config = NodeConfig {
        cell: args.cell,
        shards: args.shards,
        replicas: args.replicas,
        node: args.node,
        all_in_one: false,
        data_dir: args.data_dir.clone(),
        listen: args.listen,
        http: args.http,
        peers: args.peers.clone(),
        cells: remotes(&args.remote)?,
        advertise: args.advertise.clone(),
        data_file_bytes: args.data_file_mb << 20,
    };
    serve_forever(config, false).await
}

async fn dev(args: &DevArgs) -> Done {
    let mut config =
        NodeConfig::dev(args.cell, args.shards, args.replicas, args.data_dir.clone(), args.listen, args.http);
    config.cells = remotes(&args.remote)?;
    config.data_file_bytes = args.data_file_mb << 20;
    serve_forever(config, args.standby).await
}

async fn worker(wfe: Client, args: &WorkerArgs) -> Done {
    let identity = args.identity.clone().unwrap_or_else(|| format!("wfe-worker-{}", std::process::id()));
    let mut options = WorkerOptions::new(&args.task_queue, &identity);
    let mut build = "unversioned".to_string();
    if let (Some(deployment), Some(build_id)) = (&args.deployment, &args.build_id) {
        let behavior =
            if args.behavior == "pinned" { VersioningBehavior::Pinned } else { VersioningBehavior::AutoUpgrade };
        options = options.versioned(DeploymentVersion::new(deployment, build_id), behavior);
        build = build_id.clone();
    }
    let mut worker = Worker::new(wfe, options);
    demo::register(&mut worker, &build);
    let handle = worker.handle();
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "could not listen for Ctrl-C; stopping the worker");
        }
        handle.stop();
    });
    eprintln!("{}", json!({ "polling": { "task_queue": args.task_queue, "identity": identity, "build": build } }));
    worker.run().await;
    Ok(Output::json(json!({ "stopped": true })))
}

fn payload(text: Option<&str>) -> Result<Payload, Failed> {
    match text {
        None => Ok(Payload::null()),
        Some(text) => {
            let value: serde_json::Value =
                serde_json::from_str(text).map_err(|e| usage(format!("the input is not JSON: {e}")))?;
            Ok(Payload::json(&value).expect("a JSON value serializes"))
        }
    }
}

fn run_id(text: &Option<String>) -> Result<Option<Id128>, Failed> {
    match text {
        None => Ok(None),
        Some(text) => {
            Id128::parse(text).map(Some).ok_or_else(|| usage(format!("{text:?} is not a run id (32 hex characters)")))
        }
    }
}

fn version_of(v: &WorkflowVersioning) -> String {
    let version = v.version.as_ref().map_or("-".to_string(), |v| v.to_string());
    match &v.override_ {
        Some(VersioningOverride::Pinned(p)) => format!("{p} (override)"),
        Some(VersioningOverride::AutoUpgrade) => format!("{version} (auto-upgrade override)"),
        None => version,
    }
}

fn workflow_rows(workflows: &[WorkflowInfo]) -> (Vec<&'static str>, Vec<Vec<String>>) {
    let rows = workflows
        .iter()
        .map(|w| {
            vec![
                w.workflow_id.clone(),
                w.workflow_type.clone(),
                w.status.as_str().to_string(),
                w.task_queue.clone(),
                version_of(&w.versioning),
                time(w.start_time),
                time(w.close_time),
                w.history_length.to_string(),
                w.run_id.to_string(),
            ]
        })
        .collect();
    (vec!["WORKFLOW ID", "TYPE", "STATUS", "TASK QUEUE", "VERSION", "STARTED", "CLOSED", "EVENTS", "RUN ID"], rows)
}

fn described(d: WorkflowDescription) -> Output {
    let (columns, rows) = workflow_rows(std::slice::from_ref(&d.info));
    Output::json(&d).with_table(columns, rows)
}

async fn workflow(wfe: &Client, command: &WorkflowCommand) -> Done {
    match command {
        WorkflowCommand::Start {
            workflow_type,
            task_queue,
            id,
            input,
            input_file,
            use_existing,
            execution_timeout_ms,
            wait_ms,
        } => {
            let text = match input_file {
                Some(path) if path.as_os_str() == "-" => {
                    Some(std::io::read_to_string(std::io::stdin()).map_err(anyhow::Error::from)?)
                }
                Some(path) => {
                    Some(std::fs::read_to_string(path).map_err(|e| usage(format!("{}: {e}", path.display())))?)
                }
                None => input.clone(),
            };
            let workflow_id = id.clone().unwrap_or_else(|| {
                let nanos =
                    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos());
                format!(
                    "{workflow_type}-{:010x}",
                    (u64::try_from(nanos % (1 << 64)).expect("a value below 2^64 fits in u64")
                        ^ (u64::from(std::process::id()) << 24))
                        & 0xff_ffff_ffff
                )
            });
            let run_id = wfe
                .start_with(StartWorkflow {
                    workflow_id: workflow_id.clone(),
                    workflow_type: workflow_type.clone(),
                    task_queue: task_queue.clone(),
                    input: payload(text.as_deref())?,
                    execution_timeout_ms: *execution_timeout_ms,
                    id_conflict: if *use_existing { IdConflictPolicy::UseExisting } else { IdConflictPolicy::Fail },
                    ..StartWorkflow::default()
                })
                .await?;
            match wait_ms {
                Some(wait_ms) => Ok(described(wfe.result(&workflow_id, Some(run_id), *wait_ms).await?)),
                None => Ok(Output::json(json!({ "workflow_id": workflow_id, "run_id": run_id }))),
            }
        }
        WorkflowCommand::Describe(run) => Ok(described(wfe.describe(&run.workflow_id, run_id(&run.run_id)?).await?)),
        WorkflowCommand::Result { run, wait_ms } => {
            let d = wfe.result(&run.workflow_id, run_id(&run.run_id)?, *wait_ms).await?;
            if !d.info.status.is_closed() {
                return Err(Error::deadline(format!("{} is still running after {wait_ms} ms", run.workflow_id)).into());
            }
            Ok(Output::json(json!({
                "workflow_id": d.info.workflow_id,
                "run_id": d.info.run_id,
                "status": d.info.status.as_str(),
                "result": d.result,
                "failure": d.failure,
            })))
        }
        WorkflowCommand::History(run) => {
            let events = wfe.history(&run.workflow_id, run_id(&run.run_id)?).await?;
            let rows = events
                .iter()
                .map(|e| {
                    let attrs = output::readable(serde_json::to_value(&e.attrs).unwrap_or_default());
                    let detail = match &attrs {
                        serde_json::Value::Object(map) => {
                            map.values().next().map(|v| v.to_string()).unwrap_or_default()
                        }
                        serde_json::Value::Null
                        | serde_json::Value::Bool(_)
                        | serde_json::Value::Number(_)
                        | serde_json::Value::String(_)
                        | serde_json::Value::Array(_) => String::new(),
                    };
                    let detail: String = if detail.chars().count() > 100 {
                        detail.chars().take(99).chain(['…']).collect()
                    } else {
                        detail
                    };
                    vec![e.id.to_string(), time(e.time), e.attrs.name().to_string(), detail]
                })
                .collect();
            Ok(Output::json(&events).with_table(vec!["ID", "TIME", "EVENT", "DETAIL"], rows))
        }
        WorkflowCommand::Signal { workflow_id, name, input } => {
            wfe.signal(workflow_id, name, payload(input.as_deref())?).await?;
            Ok(Output::ok())
        }
        WorkflowCommand::Cancel { workflow_id, reason } => {
            wfe.cancel(workflow_id, reason).await?;
            Ok(Output::ok())
        }
        WorkflowCommand::Terminate { workflow_id, reason } => {
            wfe.terminate(workflow_id, reason).await?;
            Ok(Output::ok())
        }
        WorkflowCommand::List { status, workflow_type, id_prefix, limit } => {
            let status = match status {
                Some(s) => Some(WorkflowStatus::parse(s).ok_or_else(|| usage(format!("unknown status {s:?}")))?),
                None => None,
            };
            let filter = ListFilter { status, workflow_type: workflow_type.clone(), id_prefix: id_prefix.clone() };
            let workflows = wfe.list(filter, *limit).await?;
            let (columns, rows) = workflow_rows(&workflows);
            Ok(Output::json(&workflows).with_table(columns, rows))
        }
        WorkflowCommand::SetOverride { workflow_id, pinned, auto_upgrade, clear } => {
            let value = match (pinned, auto_upgrade, clear) {
                (Some(version), _, _) => {
                    let (deployment, build_id) =
                        version.split_once(':').ok_or_else(|| usage("--pinned takes <deployment>:<build id>"))?;
                    Some(VersioningOverride::Pinned(DeploymentVersion::new(deployment, build_id)))
                }
                (None, true, _) => Some(VersioningOverride::AutoUpgrade),
                (None, false, true) => None,
                _ => return Err(usage("one of --pinned, --auto-upgrade or --clear is required")),
            };
            wfe.set_versioning_override(workflow_id, value).await?;
            Ok(Output::ok())
        }
    }
}

fn deployment_rows(deployments: &[DeploymentInfo]) -> Output {
    let mut rows = Vec::new();
    for d in deployments {
        for v in &d.versions {
            let share = match v.status {
                VersionStatus::Ramping => format!("{}%", d.routing.ramp_percent),
                VersionStatus::Current if d.routing.ramping.is_some() => {
                    format!("{}%", 100 - d.routing.ramp_percent.min(100))
                }
                VersionStatus::Current => "100%".into(),
                VersionStatus::Inactive | VersionStatus::Draining | VersionStatus::Drained => "-".into(),
            };
            rows.push(vec![
                d.name.clone(),
                v.build_id.clone(),
                format!("{:?}", v.status).to_lowercase(),
                share,
                v.pinned_open.to_string(),
                v.task_queues.join(","),
                time(v.last_poll_at),
                format!("{}/{}", d.propagated_shards, d.shard_count),
            ]);
        }
        if d.versions.is_empty() {
            rows.push(vec![
                d.name.clone(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
                "-".into(),
            ]);
        }
    }
    let columns =
        vec!["DEPLOYMENT", "BUILD", "STATUS", "SHARE", "PINNED OPEN", "TASK QUEUES", "LAST POLL", "PROPAGATED"];
    Output::json(deployments).with_table(columns, rows)
}

async fn deployment(wfe: &Client, command: &DeploymentCommand) -> Done {
    match command {
        DeploymentCommand::List => Ok(deployment_rows(&wfe.list_deployments().await?)),
        DeploymentCommand::Describe { deployment } => {
            let d = wfe.describe_deployment(deployment).await?;
            let mut out = deployment_rows(std::slice::from_ref(&d));
            out.json = output::readable(serde_json::to_value(&d).unwrap_or_default());
            Ok(out)
        }
        DeploymentCommand::SetCurrent { deployment, build_id } => {
            wfe.set_current_version(deployment, build_id.as_deref()).await?;
            Ok(Output::json(wfe.routing_propagated(deployment, 5_000).await?))
        }
        DeploymentCommand::SetRamping { deployment, build_id, percent } => {
            if *percent > 100 {
                return Err(usage("--percent is 0 to 100"));
            }
            wfe.set_ramping_version(deployment, build_id.as_deref(), *percent).await?;
            Ok(Output::json(wfe.routing_propagated(deployment, 5_000).await?))
        }
        DeploymentCommand::DeleteVersion { deployment, build_id } => {
            wfe.delete_version(deployment, build_id).await?;
            Ok(Output::ok())
        }
        DeploymentCommand::Delete { deployment } => {
            wfe.delete_deployment(deployment).await?;
            Ok(Output::ok())
        }
    }
}

async fn node_status(url: &str) -> Result<NodeStatus, Failed> {
    let url = format!("{}/status", url.trim_end_matches('/'));
    let response = reqwest::get(&url).await.map_err(|e| Error::unavailable(format!("{url}: {e}")))?;
    let status: Option<NodeStatus> = response.json().await.map_err(|e| Error::unavailable(format!("{url}: {e}")))?;
    status.ok_or_else(|| Error::unavailable("the node is shutting down").into())
}

async fn cell(cli: &Cli, wfe: &Client, command: &CellCommand) -> Done {
    match command {
        CellCommand::Describe => {
            let info = wfe.describe_cell().await?;
            let summary = CellSummary::of(&info);
            let rows = info
                .shards
                .iter()
                .map(|s| {
                    let links: Vec<String> = s
                        .replication
                        .iter()
                        .map(|r| {
                            format!(
                                "cell {}: {} pending{}",
                                r.target.cell,
                                r.pending,
                                if r.paused { " (paused)" } else { "" }
                            )
                        })
                        .collect();
                    vec![
                        s.shard.to_string(),
                        if s.active { "active" } else { "standby" }.to_string(),
                        s.epoch.to_string(),
                        s.executions.to_string(),
                        s.open_workflows.to_string(),
                        s.op.to_string(),
                        s.replicated_in.to_string(),
                        if links.is_empty() { "-".into() } else { links.join("; ") },
                    ]
                })
                .collect();
            let columns = vec!["SHARD", "ROLE", "EPOCH", "EXECUTIONS", "OPEN", "OP", "REPLICATED IN", "REPLICATING TO"];
            Ok(Output::json(json!({ "summary": summary, "shards": info.shards })).with_table(columns, rows))
        }
        CellCommand::Status => {
            let status = node_status(&cli.url).await?;
            let rows = status
                .hosted
                .iter()
                .map(|r| {
                    vec![
                        r.shard.to_string(),
                        r.replica.to_string(),
                        r.status.clone(),
                        if r.primary { "primary" } else { "backup" }.to_string(),
                        r.view.to_string(),
                        r.op.to_string(),
                        r.commit.to_string(),
                        r.checkpoint_op.to_string(),
                    ]
                })
                .collect();
            let _ = duration_ms;
            Ok(Output::json(&status)
                .with_table(vec!["SHARD", "REPLICA", "STATUS", "ROLE", "VIEW", "OP", "COMMIT", "CHECKPOINT"], rows))
        }
        CellCommand::Replicate { to } => {
            let target = CellSummary::of(&client(cli, to)?.describe_cell().await?);
            wfe.create_replication(target.cell_ref()).await?;
            Ok(Output::json(json!({ "ok": true, "replicating_to": target.cell })))
        }
        CellCommand::Unreplicate { cell } => {
            wfe.delete_replication(*cell).await?;
            Ok(Output::ok())
        }
        CellCommand::Pause { cell } => {
            wfe.pause_replication(*cell, true).await?;
            Ok(Output::ok())
        }
        CellCommand::Resume { cell } => {
            wfe.pause_replication(*cell, false).await?;
            Ok(Output::ok())
        }
        CellCommand::SetRole { active, standby: _, epoch } => {
            let epoch = match epoch {
                Some(epoch) => *epoch,
                None => CellSummary::of(&wfe.describe_cell().await?).epoch + 1,
            };
            wfe.set_role(*active, epoch).await?;
            Ok(Output::json(json!({ "ok": true, "role": if *active { "active" } else { "standby" }, "epoch": epoch })))
        }
        CellCommand::Failover { to, force, wait_ms } => {
            let target = client(cli, to)?;
            wfe_sdk::failover(wfe, &target, *force, *wait_ms).await?;
            let summary = CellSummary::of(&target.describe_cell().await?);
            Ok(Output::json(json!({ "ok": true, "active": summary })))
        }
    }
}
