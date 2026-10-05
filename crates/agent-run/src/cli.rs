//! Operator CLI. Starts and resumes always go through the resident broker.
use crate::{
    Result, capacity,
    config::{Adapter, Config},
    domain::{AgentId, OrchestratorRef},
    error::invalid,
    fs, hooks,
    service::{Query, Service},
    state::Store,
    transport,
};
use agent_run_domain::{
    CredentialRef,
    catalog::{AccountId, AccountRecord, AccountStatus, AuthFamily, SecretRef},
};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use serde_json::{Value, json};
use std::{
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    sync::Arc,
    time::Duration,
};

/// A boxed asynchronous CLI operation owned by an injected test seam.
pub type CliFuture<'a> = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>>;

/// Output callback used by [`CliDependencies`].
pub type CliOutput = Arc<dyn Fn(&Value) -> Result<()> + Send + Sync>;

/// Raw text chunk sink used by the transcript viewer's text format.
///
/// The sink writes exactly the bytes it is given and flushes; the transcript
/// renderer owns every intentional newline, so streamed fragments reach the
/// pipe as soon as they are rendered.
pub type CliTextOutput = Arc<dyn Fn(&str) -> Result<()> + Send + Sync>;

/// Output format of the `transcript` viewer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum TranscriptFormat {
    /// Human-readable activity stream.
    Text,
    /// Line-delimited transcript pages (the historical machine format).
    Json,
}

impl TranscriptFormat {
    /// Resolves an explicit `--format` or the automatic default.
    ///
    /// An explicit choice always wins; otherwise text is used when standard
    /// output is a terminal and JSON for any piped or captured consumer.
    pub fn effective(format: Option<Self>) -> Self {
        format.unwrap_or({
            use std::io::IsTerminal;
            if std::io::stdout().is_terminal() {
                Self::Text
            } else {
                Self::Json
            }
        })
    }
}

/// Structured doctor callback used by [`CliDependencies`].
pub type DoctorRunner = Arc<dyn Fn(&Path) -> Result<crate::doctor::Report> + Send + Sync>;

/// Exact-run operations plus public identity resolution used by CLI commands.
pub trait CliService: Send + Sync {
    /// Pin a public agent selection to an exact execution for this CLI command.
    ///
    /// Store-free test implementations retain literal ids; production verifies
    /// lineage membership and resolves an omitted run to the current execution.
    fn resolve_run_id(&self, id: &AgentId, run_id: Option<&AgentId>) -> Result<AgentId> {
        Ok(run_id.unwrap_or(id).clone())
    }
    /// Add stable identity to an exact-run result without altering its content.
    /// Store-free implementations may already supply a projected fixture.
    fn public_run_result(&self, _run_id: &AgentId, value: Value) -> Result<Value> {
        Ok(value)
    }
    /// Cancel one already pinned execution and return its unprojected view.
    fn cancel(&self, id: &AgentId) -> Result<Value>;
    /// Enqueue steering for one already pinned execution.
    fn steer(&self, id: &AgentId, text: &str) -> Result<Value>;
    /// List agents, optionally waiting for a durable revision.
    fn list<'a>(&'a self, query: Query) -> CliFuture<'a>;
    /// Read one pinned execution's verified answer envelope.
    fn answer(&self, id: &AgentId) -> Result<Value>;
    /// Read one pinned run for follow-up terminal checks without resolving again.
    fn agent(&self, id: &AgentId) -> Result<Value>;
    /// Read one bounded page from the already pinned execution.
    fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value>;
    /// Read retained lineage history; store-free seams may use a single run.
    fn transcript_public(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        self.transcript(id, cursor, limit)
    }
    /// Reads typed transcript options. Historical service seams retain raw
    /// behavior; block readers must implement this method explicitly.
    fn transcript_options(
        &self,
        id: &AgentId,
        query: &agent_run_domain::transcript::TranscriptQuery,
        lineage: bool,
    ) -> Result<Value> {
        query.validate()?;
        if query.view != agent_run_domain::transcript::TranscriptView::Raw {
            return Err(crate::Error::Unsupported(
                "block transcript reader is unavailable".into(),
            ));
        }
        if lineage {
            self.transcript_public(id, query.cursor, query.limit)
        } else {
            self.transcript(id, query.cursor, query.limit)
        }
    }
    /// Return the configured model roster or schema-2 provider catalog.
    fn models<'a>(&'a self, query: agent_run_domain::ModelsQuery) -> CliFuture<'a>;
    /// Return the configured capacity limits.
    fn limits(&self) -> Result<Value>;
    /// Return the ordered capacity view, optionally for one exact model.
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> Result<Value>;
    /// Return the plain-text delegation guide as a JSON string value.
    fn delegation_guide(&self) -> Result<Value>;
    /// Return delivery status for one exact execution.
    fn delivery_status(&self, id: &AgentId) -> Result<Value>;
    /// Cancel one completion-delivery attempt.
    fn delivery_cancel(&self, id: &str) -> Result<Value>;
}

/// Broker operation used by start, resume, wait, and MCP calls.
pub trait CliBroker: Send + Sync {
    /// Send one method and JSON object to the resident broker.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a>;
}

impl CliService for Service {
    /// Resolve latest or exact selection through the same lineage boundary as MCP.
    fn resolve_run_id(&self, id: &AgentId, run_id: Option<&AgentId>) -> Result<AgentId> {
        Ok(Service::resolve_run(self, id, run_id)?.id)
    }
    /// Preserve exact-run content while projecting its stable public identity.
    fn public_run_result(&self, run_id: &AgentId, value: Value) -> Result<Value> {
        Service::public_run_result(self, run_id, value)
    }
    fn cancel(&self, id: &AgentId) -> Result<Value> {
        Service::cancel(self, id)
    }
    fn steer(&self, id: &AgentId, text: &str) -> Result<Value> {
        Service::steer(self, id, text)
    }
    /// List execution rows with the common stable-id projection.
    fn list<'a>(&'a self, query: Query) -> CliFuture<'a> {
        Box::pin(Service::list_public(self, query))
    }
    fn answer(&self, id: &AgentId) -> Result<Value> {
        Service::answer(self, id)
    }
    fn agent(&self, id: &AgentId) -> Result<Value> {
        let store = Store::open(&self.home)?;
        let row = store.get(id)?;
        Service::view(self, &store, &row)
    }
    fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        Service::transcript(self, id, cursor, limit)
    }
    /// Reads raw or bounded blocks through the same public service validator.
    fn transcript_options(
        &self,
        id: &AgentId,
        query: &agent_run_domain::transcript::TranscriptQuery,
        lineage: bool,
    ) -> Result<Value> {
        Service::transcript_with_options(self, id, (!lineage).then_some(id), query)
    }
    /// Keep transcript cursors continuous across independent resume executions.
    fn transcript_public(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        Service::transcript_public(self, id, cursor, limit)
    }
    fn models<'a>(&'a self, query: agent_run_domain::ModelsQuery) -> CliFuture<'a> {
        Box::pin(Service::models(self, query))
    }
    fn limits(&self) -> Result<Value> {
        Service::limits(self)
    }
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> Result<Value> {
        Service::capacity_order(self, query)
    }
    fn delegation_guide(&self) -> Result<Value> {
        Service::delegation_guide(self)
    }
    fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        Service::delivery_status(self, id)
    }
    fn delivery_cancel(&self, id: &str) -> Result<Value> {
        Service::delivery_cancel(self, id)
    }
}

/// Production broker client preserving the existing Unix-socket call path.
pub(crate) struct SocketBroker {
    /// Agent-run home whose private socket receives each request.
    pub(crate) home: PathBuf,
}

impl CliBroker for SocketBroker {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
        Box::pin(transport::socket::client(&self.home, method, params))
    }
}

/// Dependencies for one CLI execution, with production implementations supplied by [`run`].
pub struct CliDependencies {
    /// Service used by local read/write commands.
    pub service: Arc<dyn CliService>,
    /// Resident broker used by admission and wait commands.
    pub broker: Arc<dyn CliBroker>,
    /// JSON sink; production writes one newline-delimited value to stdout.
    pub output: CliOutput,
    /// Raw text chunk sink used by the transcript viewer's human-readable
    /// format.
    pub text_output: CliTextOutput,
    /// Structured doctor report provider.
    pub doctor: DoctorRunner,
}

impl CliDependencies {
    /// Builds the default production dependencies for one resolved home.
    pub fn production(home: PathBuf) -> Self {
        Self {
            service: Arc::new(Service::new(home.clone())),
            broker: Arc::new(SocketBroker { home }),
            output: Arc::new(emit),
            text_output: Arc::new(write_chunk),
            doctor: Arc::new(crate::doctor::run),
        }
    }
}
/// The operator command line shared with the resident broker transports.
///
/// This parser is intentionally the single source of command names and flag
/// spelling for the binary. Service calls keep their request schemas in the
/// shared domain tool registry; this layer only adapts shell values to those
/// schemas and never launches a one-shot start locally. Clap renders the
/// package version through `--version` and `-V` before command dispatch.
#[derive(Parser, Debug)]
#[command(
    name = "agent-run",
    version,
    about = "Durable local coding-agent supervisor"
)]
pub struct Cli {
    #[arg(long, global = true, env = "AGENT_RUN_HOME")]
    pub home: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}
/// Top-level commands, including two hidden runtime implementation commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    Init,
    Doctor,
    Start(Start),
    Resume(Resume),
    /// Start a cooperative pool from one JSON request (`-` reads stdin).
    StartPool {
        /// JSON file or `-`: request_id, goal, optional acceptance, members.
        #[arg(long)]
        spec: String,
        #[command(flatten)]
        session: SessionArgs,
    },
    /// Post a message, stamped as from the operator, to every pool member.
    PoolPost {
        #[arg(long)]
        pool_id: String,
        #[arg(long)]
        request_id: String,
        /// Message text; `-` reads standard input.
        #[arg(long)]
        text: String,
    },
    /// Replace one terminal, fully cleaned pool member.
    PoolReplace {
        #[arg(long)]
        pool_id: String,
        /// Stable agent id of the current member.
        #[arg(long)]
        agent_id: String,
        #[arg(long)]
        request_id: String,
        /// Optional JSON file or `-` with an explicit replacement start.
        #[arg(long)]
        spec: Option<String>,
    },
    /// Read a pool's status and one cursor page of its log.
    Pool {
        #[arg(long)]
        pool_id: String,
        #[arg(long)]
        after_seq: Option<u64>,
        #[arg(long)]
        before_seq: Option<u64>,
        #[arg(long)]
        limit: Option<u32>,
    },
    Bind(Bind),
    Cancel {
        /// Stable agent id, or a historical run alias for that agent.
        agent_id: AgentId,
        /// Pin cancellation to this exact run within the agent's history.
        #[arg(long, hide = true)]
        run_id: Option<AgentId>,
    },
    Steer {
        /// Stable agent id, or a historical run alias for that agent.
        agent_id: AgentId,
        /// Pin steering to this exact run within the agent's history.
        #[arg(long, hide = true)]
        run_id: Option<AgentId>,
        #[arg(long)]
        text: String,
    },
    Agents(Agents),
    /// Discover pools through the resident broker, newest-created first.
    #[command(alias = "list-pools")]
    Pools {
        /// Optional exact state filter.
        #[arg(long, value_parser = ["open", "completed"])]
        state: Option<String>,
        /// Matching pools to skip.
        #[arg(long, default_value_t = 0)]
        offset: usize,
        /// Page size, 1..=200.
        #[arg(long, default_value_t = 50)]
        limit: usize,
        /// Print a compact human page instead of structured JSON.
        #[arg(long)]
        text: bool,
    },
    Answer {
        /// Stable agent id; defaults to its latest execution's answer.
        agent_id: AgentId,
        /// Retrieve this exact historical run's verified answer.
        #[arg(long, hide = true)]
        run_id: Option<AgentId>,
    },
    Transcript {
        /// Stable agent id; pages include retained history across resumes.
        agent_id: AgentId,
        /// Retrieve this exact historical run's transcript.
        #[arg(long, hide = true)]
        run_id: Option<AgentId>,
        #[arg(long, default_value_t = 0)]
        cursor: i64,
        #[arg(long, default_value_t = agent_run_domain::transcript::default_limit())]
        limit: usize,
        /// Raw rows by default, or consecutive native-identity blocks.
        #[arg(long, default_value = "raw")]
        view: agent_run_domain::transcript::TranscriptView,
        /// Last 1..=200 blocks; requires --view blocks and cursor zero.
        #[arg(long, conflicts_with_all = ["follow", "full"])]
        tail_blocks: Option<usize>,
        /// Exclusive upper sequence for an older block page.
        #[arg(long, conflicts_with_all = ["follow", "full"])]
        before_cursor: Option<i64>,
        #[arg(long)]
        follow: bool,
        #[arg(long, conflicts_with = "follow")]
        full: bool,
        /// Output format; defaults to text on a terminal, JSON otherwise.
        #[arg(long, value_enum)]
        format: Option<TranscriptFormat>,
    },
    /// Model roster, or the schema-2 provider catalog with exact filters.
    Models {
        /// Exact configured provider id.
        #[arg(long)]
        provider: Option<String>,
        /// Exact canonical role/profile name.
        #[arg(long)]
        profile: Option<String>,
        /// Exact provider-visible model id.
        #[arg(long)]
        model: Option<String>,
    },
    Limits,
    /// Print the compact plain-text routing guide for orchestrators.
    DelegationGuide,
    Doc {
        topic: Option<String>,
    },
    Mcp,
    /// Internal run-bound worker channel; context is inherited from the supervisor.
    #[command(name = "_worker-mcp", hide = true)]
    WorkerMcp,
    Api {
        #[command(subcommand)]
        command: Api,
    },
    Capacity {
        #[command(subcommand)]
        command: Capacity,
    },
    Delivery {
        #[command(subcommand)]
        command: Delivery,
    },
    Login {
        runtime: String,
        #[arg(long)]
        account: Option<String>,
    },
    Auth {
        label: String,
        runtime: String,
    },
    /// One-time schema-1 → schema-2 configuration migration and rollback.
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
    /// Operator storage administration for the shared managed-asset store.
    Storage {
        #[command(subcommand)]
        command: StorageCommand,
    },
    /// Manage global account references without invoking native login.
    Accounts {
        #[command(subcommand)]
        command: AccountCommand,
    },
    Context(Context),
    Hook {
        #[command(subcommand)]
        command: Hook,
    },
    #[command(name = "_supervisor", hide = true)]
    Supervisor {
        agent_id: AgentId,
        #[arg(long)]
        ready_fd: i32,
        #[arg(long)]
        identity_fd: i32,
        #[arg(long)]
        error_fd: i32,
    },
    #[command(name = "_deny-command", hide = true)]
    DenyCommand {
        name: String,
    },
    /// Commits a broker-authorized service identity before replacing this process with its foreground command.
    #[command(name = "_service-exec", hide = true)]
    ServiceExec {
        /// Durable generation authorized by this process's parent broker.
        generation: String,
    },
    /// Runs the doctor bootstrap canary using its three inherited descriptors.
    #[command(name = "_doctor_canary", hide = true)]
    DoctorCanary {
        #[arg(long)]
        ready_fd: i32,
        #[arg(long)]
        identity_fd: i32,
        #[arg(long)]
        error_fd: i32,
    },
    /// Privately evaluates one generated Codex trusted-MCP permission request.
    #[command(name = "_permission-request", hide = true)]
    PermissionRequest {
        /// Repeated configured MCP namespaces eligible for the narrow allow decision.
        #[arg(long = "allow-mcp", required = true)]
        allow_mcp: Vec<String>,
    },
}

/// Administrative account registry operations; references name existing
/// protected stores and never accept credential bytes as an argument.
#[derive(Subcommand, Debug)]
pub enum AccountCommand {
    /// Register one global account and existing credential reference.
    Register {
        /// Opaque global account identity shared by provider aliases.
        #[arg(long)]
        id: AccountId,
        /// Protocol credential family matching provider bindings.
        #[arg(long)]
        auth_family: AuthFamily,
        /// Nonsecret native/named/env/file/Keychain storage reference.
        ///
        /// Taken as text and parsed by the handler, so a rejected value (for
        /// example a pasted raw token) is never echoed in the parse error.
        #[arg(long)]
        reference: String,
    },
    /// List registered account metadata without full storage references.
    List,
    /// Disable future account selection while preserving history.
    Disable {
        /// Existing global account identity.
        id: AccountId,
    },
}
/// Optional orchestrator identity shared by public tool commands.
#[derive(Args, Debug, Default)]
pub struct SessionArgs {
    #[arg(long, requires = "session_id")]
    pub session_transport: Option<String>,
    #[arg(long, requires = "session_transport")]
    pub session_id: Option<String>,
    #[arg(long, requires = "session_id")]
    pub session_turn_id: Option<String>,
}
impl SessionArgs {
    /// Validates and converts explicitly supplied session fields, if any.
    fn resolve(&self) -> Result<Option<OrchestratorRef>> {
        let value = match (&self.session_transport, &self.session_id) {
            (Some(transport), Some(session)) => Some(OrchestratorRef {
                transport: transport.clone(),
                external_session_id: session.clone(),
                external_turn_id: self.session_turn_id.clone(),
            }),
            (None, None) => None,
            _ => return Err(invalid("session transport and ID are required together")),
        };
        if let Some(o) = &value {
            o.validate()?;
        }
        Ok(value)
    }
}
/// The resident-broker start command's shell request fields.
#[derive(Args, Debug)]
pub struct Start {
    /// Configured provider id (schema 2); pair it with an explicit `--model`.
    #[arg(long)]
    pub provider: String,
    #[arg(long)]
    pub model: String,
    #[arg(long)]
    pub profile: String,
    #[arg(long)]
    pub task: String,
    #[arg(long)]
    pub workdir: Option<PathBuf>,
    #[arg(long)]
    pub write: bool,
    #[arg(long)]
    pub fast: bool,
    #[arg(long)]
    pub effort: Option<String>,
    #[arg(
        long = "timeout",
        id = "timeout",
        help = "Whole-run deadline in seconds, at most 2592000; defaults to core.default_timeout_seconds; either base is scaled once by core.timeout_multiplier"
    )]
    pub timeout_seconds: Option<f64>,
    #[arg(long = "read-root", id = "read_root")]
    pub read_roots: Vec<PathBuf>,
    #[arg(long)]
    pub output_schema: Option<String>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub request_id: Option<String>,
    /// Optional human display label shown in list views; at most 64 Unicode
    /// characters, no control or bidi formatting. `--display-name` is an alias.
    #[arg(long = "name", alias = "display-name")]
    pub display_name: Option<String>,
    #[arg(long)]
    pub wait: bool,
    #[command(flatten)]
    pub session: SessionArgs,
}
/// The resident-broker continuation command's shell request fields.
#[derive(Args, Debug)]
#[command(group = ArgGroup::new("resume_task").required(true).args(["task", "task_file"]))]
pub struct Resume {
    /// Stable agent id or an old run alias; omission of run_id continues its tip.
    pub agent_id: AgentId,
    /// Pin the terminal parent rather than resolving the latest run.
    #[arg(long, hide = true)]
    pub run_id: Option<AgentId>,
    #[arg(long)]
    pub task: Option<String>,
    #[arg(long)]
    pub task_file: Option<PathBuf>,
    #[arg(
        long = "timeout",
        id = "timeout",
        help = "Whole-run deadline in seconds, at most 2592000; a new value is scaled once by core.timeout_multiplier, omission inherits the previous run's effective deadline"
    )]
    pub timeout_seconds: Option<f64>,
    #[arg(long)]
    pub request_id: Option<String>,
    /// Optional replacement display label; omission inherits the previous
    /// run's label. `--display-name` is an alias.
    #[arg(long = "name", alias = "display-name")]
    pub display_name: Option<String>,
    #[command(flatten)]
    pub session: SessionArgs,
}
/// A durable session binding request accepted by the Python command surface.
#[derive(Args, Debug)]
pub struct Bind {
    /// Stable agent whose selected execution should be bound.
    #[arg(required_unless_present = "pool", conflicts_with = "pool")]
    pub agent_id: Option<AgentId>,
    /// Bind a whole pool and every current member together instead of one agent.
    #[arg(long)]
    pub pool: Option<String>,
    /// Legacy execution selector, retained for older callers.
    #[arg(long, hide = true)]
    pub run_id: Option<AgentId>,
    /// Orchestrator transport name.
    #[arg(long)]
    pub session_transport: String,
    /// External session identity.
    #[arg(long)]
    pub session_id: String,
    /// Optional external turn identity.
    #[arg(long)]
    pub session_turn_id: Option<String>,
}

/// A durable session context lookup request accepted by the Python surface.
#[derive(Args, Debug)]
pub struct Context {
    /// Orchestrator transport name.
    #[arg(long)]
    pub session_transport: String,
    /// External session identity.
    #[arg(long)]
    pub session_id: String,
    /// Optional external turn identity.
    #[arg(long)]
    pub session_turn_id: Option<String>,
}

/// An input hook subcommand.
#[derive(Subcommand, Debug)]
pub enum Hook {
    /// Render context for a user prompt hook.
    Context(HookTransport),
    /// Bind a completed tool invocation from a post-tool hook.
    Bind(HookTransport),
}

/// Hook transport selection constrained to the two Python-compatible values.
#[derive(Args, Debug)]
pub struct HookTransport {
    /// The delivery transport encoded by the hook payload.
    #[arg(long, default_value = "codex_queue", value_parser = ["claude_uds", "codex_queue"])]
    pub transport: String,
}
/// Paging filters for the Python-compatible agent listing command.
#[derive(Args, Debug)]
pub struct Agents {
    #[arg(long)]
    pub active: bool,
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    /// Keep one process watching and print one NDJSON snapshot per meaningful
    /// change; observation-time-only movement never reprints a page.
    #[arg(long)]
    pub follow: bool,
    #[command(flatten)]
    pub session: SessionArgs,
}
/// Long-poll window, in seconds, of one follow watch round.
const FOLLOW_WAIT_SECONDS: f64 = 25.0;

/// Reduces one agent view to the stable facts a follow snapshot reports.
///
/// Only fields whose change is meaningful re-emit a page: observation-time
/// drift (`observed_at`, `elapsed_seconds`, `silence_seconds`) moves every
/// rebuild and must never redraw unchanged data. Unlisted fields are absent
/// rather than defaulted so a view gaining evidence still changes the digest.
fn follow_signature(agent: &Value) -> Option<String> {
    let object = agent.as_object()?;
    let mut stable = serde_json::Map::new();
    for key in [
        "agent_id",
        "name",
        "runtime",
        "model",
        "profile",
        "task_summary",
        "status",
        "phase",
        "failure_kind",
        "failure_text",
        "answer_available",
        "answer_bytes",
        "answer_sha256",
        "effort",
        "last_progress_at",
        "warned",
        "usage",
        "usage_cumulative",
        "tool_counts",
        "mcp",
    ] {
        if let Some(value) = object.get(key) {
            stable.insert(key.into(), value.clone());
        }
    }
    if let Some(delivery) = object.get("delivery").filter(|d| d.is_object()) {
        let mut filtered = serde_json::Map::new();
        for key in [
            "state",
            "notification_id",
            "attempts",
            "ambiguous",
            "last_error",
        ] {
            if let Some(value) = delivery.get(key) {
                filtered.insert(key.into(), value.clone());
            }
        }
        stable.insert("delivery".into(), Value::Object(filtered));
    }
    serde_json::to_string(&Value::Object(stable)).ok()
}

/// Runs the persistent `agents --follow` watcher for one command invocation.
///
/// One process emits the first bounded page immediately and then one NDJSON
/// snapshot per meaningful change: each round long-polls the broker with the
/// last event revision and transcript watermark, so status, usage, tool-count,
/// name, failure, delivery and answer changes all wake it, while pages whose
/// only movement is observation time are counted and dropped. The waiter is
/// abortable at every await: Ctrl-C ends the viewer (never the supervised
/// agents) and a closed output pipe terminates the process through the
/// writer's own error.
async fn follow_agents(dependencies: CliDependencies, a: &Agents) -> Result<()> {
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut after_revision: Option<i64> = None;
    let mut after_message_revision: Option<i64> = None;
    let mut signatures: std::collections::BTreeMap<String, String> = Default::default();
    let mut total: Option<i64> = None;
    loop {
        let request = Query {
            active: a.active,
            offset: a.offset,
            limit: a.limit,
            after_revision,
            after_message_revision,
            wait_seconds: if after_revision.is_none() {
                0.0
            } else {
                FOLLOW_WAIT_SECONDS
            },
            orchestrator: a.session.resolve()?,
        };
        // A pending wait owns no SQLite transaction and no engine handle, so
        // dropping it at the interrupt boundary leaves nothing behind.
        let page = tokio::select! {
            biased;
            _ = interrupt.recv() => return Ok(()),
            page = dependencies.service.list(request) => page?,
        };
        after_revision = Some(page["revision"].as_i64().unwrap_or_default());
        after_message_revision = Some(page["message_revision"].as_i64().unwrap_or_default());
        // Rebuild the page digest from stable per-agent facts; identical
        // digests mean nothing meaningful moved since the last snapshot.
        let mut next = std::collections::BTreeMap::new();
        for agent in page["items"].as_array().into_iter().flatten() {
            if let (Some(id), Some(signature)) =
                (agent["agent_id"].as_str(), follow_signature(agent))
            {
                next.insert(id.to_owned(), signature);
            }
        }
        let page_total = page["total"].as_i64();
        let unchanged = next == signatures && total == page_total;
        (signatures, total) = (next, page_total);
        if unchanged {
            continue;
        }
        (dependencies.output)(&page)?;
    }
}

/// API daemon commands retained from the Python operator surface.
#[derive(Subcommand, Debug)]
pub enum Api {
    Serve {
        #[arg(long)]
        socket: Option<PathBuf>,
    },
    Launchd {
        #[arg(long)]
        binary: PathBuf,
        #[arg(long, default_value = "com.agent-run.api")]
        label: String,
        #[arg(long)]
        stdout_log: Option<PathBuf>,
        #[arg(long)]
        stderr_log: Option<PathBuf>,
    },
}
/// Capacity worker commands retained from the Python operator surface.
/// Configuration migration commands (see `crate::migrate`).
#[derive(Subcommand, Debug)]
pub enum ConfigCommand {
    /// Plan or apply a paired configuration/database migration. Use a v1 mapping
    /// or an explicit replacement v2 configuration; apply snapshots both sides first.
    #[command(group(ArgGroup::new("mode").required(true).args(["dry_run", "apply"])))]
    #[command(group(ArgGroup::new("migration_input").required(true).args(["mapping", "target_config"])))]
    Migrate {
        /// TOML mapping: `[harnesses.*]`, `[accounts.<id>]` and one
        /// `[runtimes.<v1 name>]` each.
        #[arg(long)]
        mapping: Option<PathBuf>,
        /// Complete target v2 configuration for an already-v2 home; existing accounts are preserved.
        #[arg(long)]
        target_config: Option<PathBuf>,
        /// Print the plan and rendered config; write nothing.
        #[arg(long)]
        dry_run: bool,
        /// Snapshot, then migrate the database, register the declared accounts
        /// and atomically publish the v2 config under the broker lock.
        #[arg(long)]
        apply: bool,
        /// Acknowledge one dry-run manual_review marker (repeat for each).
        #[arg(long = "ack")]
        ack: Vec<String>,
        /// The installed pre-migration sealed release directory (required
        /// with `--apply`); its seal and schema are verified, and rollback
        /// returns to it.
        #[arg(long)]
        from_release: Option<PathBuf>,
    },
    /// Restore the original config and database from one verified
    /// snapshot while nothing changed since the migration; also recovers an
    /// interrupted migration or rollback of that snapshot.
    Rollback {
        /// The snapshot directory printed by `config migrate --apply`.
        #[arg(long)]
        snapshot: PathBuf,
    },
}

/// Operator storage administration commands (see `crate::storage_admin`).
#[derive(Subcommand, Debug)]
pub enum StorageCommand {
    /// Report the shared store and every retained runtime home read-only.
    Status,
    /// Plan offline compaction; add `--apply` to relocate and collect.
    #[command(group(ArgGroup::new("storage_mode").required(true).args(["dry_run", "apply"])))]
    Compact {
        /// Print the survey and what collection would reclaim; write nothing.
        #[arg(long)]
        dry_run: bool,
        /// Hold the broker and service startup locks, relocate eligible
        /// retained homes behind the guard preflight, and collect.
        #[arg(long)]
        apply: bool,
    },
    /// Roll interrupted shared-storage relocations forward, offline.
    Recover,
}

#[derive(Subcommand, Debug)]
pub enum Capacity {
    Collect {
        #[arg(long, required = true)]
        once: bool,
    },
    /// Capacity order; schema 2 lists providers, optionally for one model.
    Order {
        /// Exact provider-visible model id.
        #[arg(long)]
        model: Option<String>,
    },
    Launchd {
        #[arg(long)]
        binary: PathBuf,
        #[arg(long, default_value = "com.pluto.agent-run.capacity")]
        label: String,
        #[arg(long, default_value = "/dev/null")]
        stdout_log: PathBuf,
        #[arg(long)]
        stderr_log: Option<PathBuf>,
    },
}
/// Completion-delivery commands retained from the Python operator surface.
#[derive(Subcommand, Debug)]
pub enum Delivery {
    /// Read delivery for the latest run, or an explicitly pinned execution.
    Status {
        /// Stable agent id or historical alias.
        agent_id: AgentId,
        /// Exact execution whose notification should be inspected.
        #[arg(long, hide = true)]
        run_id: Option<AgentId>,
    },
    Cancel {
        delivery_id: String,
    },
    Dispatch,
    Launchd {
        #[arg(long)]
        binary: PathBuf,
        #[arg(long, default_value = "com.pluto.agent-run.delivery")]
        label: String,
        #[arg(long, default_value = "/dev/null")]
        stdout_log: PathBuf,
        #[arg(long)]
        stderr_log: Option<PathBuf>,
    },
}
fn absolute(p: &Path) -> Result<PathBuf> {
    if p.is_absolute() || p.to_string_lossy().starts_with('~') {
        fs::expand(p)
    } else {
        Ok(std::env::current_dir()?.join(p))
    }
}
fn read_input(p: &Path, max: usize) -> Result<String> {
    let p = absolute(p)?;
    let parent = p.parent().ok_or_else(|| invalid("invalid input path"))?;
    let name = p.file_name().ok_or_else(|| invalid("invalid input file"))?;
    let bytes = fs::Dir::open(parent)?.read(Path::new(name), max)?;
    String::from_utf8(bytes).map_err(|_| invalid("input must be UTF-8"))
}
/// Reads bounded UTF-8 standard input for Python-compatible `-` task values.
fn read_stdin(max: usize) -> Result<String> {
    use std::io::Read;
    let mut bytes = Vec::with_capacity(max.min(8192));
    std::io::stdin()
        .lock()
        .take(max.saturating_add(1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(invalid("stdin exceeds the maximum input size"));
    }
    String::from_utf8(bytes).map_err(|_| invalid("input must be UTF-8"))
}

/// Reads one bounded JSON object from a file, or from standard input for `-`.
fn spec_json(source: &str) -> Result<Value> {
    const MAX: usize = 1024 * 1024;
    let text = if source == "-" {
        read_stdin(MAX)?
    } else {
        let bytes = std::fs::read(source).map_err(|_| invalid("spec file is unreadable"))?;
        if bytes.len() > MAX {
            return Err(invalid("spec file exceeds the maximum input size"));
        }
        String::from_utf8(bytes).map_err(|_| invalid("spec must be UTF-8"))?
    };
    let value: Value =
        serde_json::from_str(&text).map_err(|_| invalid("spec must be a JSON object"))?;
    if !value.is_object() {
        return Err(invalid("spec must be a JSON object"));
    }
    Ok(value)
}

/// Reads a task argument, interpreting exactly `-` as bounded standard input.
fn task_text(value: &str, max: usize) -> Result<String> {
    let text = if value == "-" {
        read_stdin(max)?
    } else {
        value.to_owned()
    };
    if text.trim().is_empty() {
        return Err(invalid("task must be nonblank"));
    }
    Ok(text)
}
/// Writes one raw viewer text chunk to stdout and flushes immediately.
///
/// The chunk is written exactly as supplied: the transcript renderer owns
/// every intentional newline, so a streamed fragment becomes visible on the
/// pipe the moment it is rendered instead of at a line boundary.
pub fn write_chunk(chunk: &str) -> Result<()> {
    let mut out = std::io::stdout().lock();
    out.write_all(chunk.as_bytes())?;
    out.flush()?;
    Ok(())
}
pub fn emit(value: &Value) -> Result<()> {
    let encoded = serde_json::to_vec(value)?;
    if encoded.len() + 1 > transport::socket::MAX_FRAME {
        return Err(invalid(
            "CLI response exceeds maximum size; request a smaller page",
        ));
    }
    let mut out = std::io::stdout().lock();
    out.write_all(&encoded)?;
    out.write_all(b"\n")?;
    out.flush()?;
    Ok(())
}

/// Reduces a broker admission DTO to the Python CLI's public acknowledgement.
///
/// Socket/MCP calls deliberately retain the full durable agent snapshot, but
/// the shell command has historically emitted only an agent id and the replay
/// indicator. Internal execution identities are omitted.
/// A malformed broker result is treated as a typed validation
/// failure rather than silently producing a partial acknowledgement.
fn admission_output(result: &Value) -> Result<Value> {
    let agent_id = result
        .get("agent_id")
        .cloned()
        .ok_or_else(|| invalid("broker admission result has no agent_id"))?;
    let created = result
        .get("created")
        .cloned()
        .ok_or_else(|| invalid("broker admission result has no created flag"))?;
    if !agent_id.is_string() || !created.is_boolean() {
        return Err(invalid("broker admission result has invalid fields"));
    }
    Ok(json!({"agent_id":agent_id,"created":created}))
}
fn result_code(value: &Value) -> i32 {
    match value.get("status").and_then(Value::as_str) {
        Some("failed" | "lost" | "timed_out" | "cancelled") => 2,
        _ => 0,
    }
}
pub fn init(home: &Path) -> Result<Value> {
    fs::private_dir(home)?;
    let dir = fs::Dir::open(home)?;
    for name in [
        "agents", "accounts", "runtimes", "profiles", "skills", "logs", "probes",
    ] {
        dir.directory(Path::new(name))?;
    }
    if dir
        .optional(Path::new("config.toml"), 1024 * 1024)?
        .is_none()
    {
        dir.write(
            Path::new("config.toml"),
            include_bytes!("../../../assets/config.example.toml"),
            0o600,
        )?;
    }
    for (name, body) in [
        (
            "review",
            "+++\nwrite = false\nnetwork = false\n+++\nReview the repository read-only. Separate observed facts, risks, and recommendations. Do not modify files.\n",
        ),
        (
            "architect",
            "+++\nwrite = false\nnetwork = false\n+++\nStudy the repository read-only and propose an implementation plan. Do not change files.\n",
        ),
        (
            "code",
            "+++\nwrite = true\nnetwork = false\n+++\nImplement the assigned change within the granted workspace. Preserve existing behaviour and report the exact checks performed.\n",
        ),
        (
            "research",
            "+++\nwrite = false\nnetwork = true\n+++\nResearch the task. Distinguish sourced facts from assumptions. Do not change local files.\n",
        ),
    ] {
        let file = format!("profiles/{name}.md");
        if dir.optional(Path::new(&file), 1024 * 1024)?.is_none() {
            dir.write(Path::new(&file), body.as_bytes(), 0o600)?;
        }
    }
    for (name, write, body) in [
        (
            "role-review",
            false,
            "Perform a read-only review. Report evidence and recommendations; do not modify files.",
        ),
        (
            "role-architect",
            false,
            "Analyze architecture read-only and produce a plan with explicit acceptance tests.",
        ),
        (
            "role-code",
            true,
            "Implement the assigned task within the granted workspace. Test changes and report remaining uncertainty.",
        ),
    ] {
        let file = format!("profiles/{name}.md");
        let text = format!(
            "+++\nrevision = \"rust-role-v1\"\nwrite = {write}\nnetwork = false\nallow_external_read_roots = true\nskills = []\nmcp = []\nrequired_constraints = []\n+++\n{body}\n"
        );
        if dir.optional(Path::new(&file), 1024 * 1024)?.is_none() {
            dir.write(Path::new(&file), text.as_bytes(), 0o600)?;
        }
    }
    let _ = operator_config(home)?;
    let store = Store::initialize(home)?;
    let _ = store.health()?;
    Ok(json!({"home":home,"config":home.join("config.toml"),"state":home.join("state.db")}))
}
/// The operator view of either schema: a valid schema-2 config's shared
/// controls with the config itself, or a schema-1 config.
fn operator_config(
    home: &Path,
) -> Result<(
    Config,
    Option<agent_run_config::provider_config::ProviderConfig>,
)> {
    match agent_run_config::provider_config::ProviderConfig::load(home) {
        Ok((v2, _)) => Ok((v2.shared(), Some(v2))),
        Err(_) => Ok((Config::load(home)?, None)),
    }
}
pub fn doc(topic: &str) -> Result<&'static str> {
    crate::dispatch::doc(topic)
}
pub async fn doctor(home: &Path) -> Result<Value> {
    let (cfg, v2) = operator_config(home)?;
    let store = Store::open(home)?;
    let mut checks = vec![json!({"name":"state","result":store.health()?})];
    drop(store);
    // A schema-2 home reports its harness executables and each provider's
    // bound accounts; an empty catalog simply has none.
    if let Some(v2) = &v2 {
        use std::os::unix::fs::PermissionsExt;
        for (id, harness) in &v2.harnesses {
            let executable = std::fs::metadata(&harness.binary)
                .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
                .unwrap_or(false);
            checks.push(json!({"name":format!("harness:{}", id.as_str()),"executable":executable}));
        }
        for (id, provider) in &v2.providers {
            let accounts: Vec<&str> = provider
                .bindings
                .iter()
                .map(|b| b.account.as_str())
                .collect();
            checks.push(json!({"name":id.as_str(),"harness":provider.harness.as_str(),"accounts":accounts,"limits_source":provider.limits_source}));
        }
    }
    for (name, runtime) in cfg.runtimes.iter().filter(|(_, r)| r.enabled) {
        use std::os::unix::fs::PermissionsExt;
        let executable = std::fs::metadata(&runtime.binary)
            .map(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
            .unwrap_or(false);
        let mut missing = Vec::new();
        if let Some(crate::config::Auth::Environment { names }) = &runtime.auth {
            for n in names {
                if std::env::var(n).map(|s| s.is_empty()).unwrap_or(true) {
                    missing.push(n.clone());
                }
            }
        }
        let auth = match &runtime.auth {
            Some(crate::config::Auth::FileLink { source, .. }) => {
                json!({"kind":"file_link","available":source.is_file()})
            }
            Some(crate::config::Auth::Environment { .. }) => {
                json!({"kind":"environment","available":missing.is_empty(),"missing_names":missing})
            }
            None => json!({"kind":"native","available":null}),
        };
        checks.push(json!({"name":name,"executable":executable,"auth":auth,"accounts":runtime.accounts,"limits_source":runtime.limits_source}));
    }
    let broker = transport::socket::client(home, "ping", json!({}))
        .await
        .is_ok();
    let ok = broker
        && checks.iter().all(|c| {
            c.get("executable") != Some(&Value::Bool(false))
                && c.pointer("/auth/available") != Some(&Value::Bool(false))
                && c.pointer("/result/ok") != Some(&Value::Bool(false))
        });
    Ok(
        json!({"ok":ok,"home":home,"broker_available":broker,"checks":checks,"validation_level":"filesystem-and-configuration; provider authentication is checked at launch"}),
    )
}
/// Escapes a scalar value for insertion into a launchd plist XML text node.
fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
/// Renders the Python-compatible launchd document and its machine-readable metadata.
pub fn launchd(
    home: &Path,
    binary: PathBuf,
    kind: &str,
    interval: u64,
    label: &str,
    stdout_log: PathBuf,
    stderr_log: PathBuf,
) -> Result<Value> {
    let binary = absolute(&binary)?;
    let args = match kind {
        "api" => vec!["api", "serve"],
        "capacity" => vec!["capacity", "collect", "--once"],
        "delivery" => vec!["delivery", "dispatch"],
        _ => return Err(invalid("unknown launchd job")),
    };
    let mut argv = vec![
        binary.to_string_lossy().into_owned(),
        "--home".into(),
        home.to_string_lossy().into_owned(),
    ];
    argv.extend(args.into_iter().map(str::to_owned));
    let args = argv
        .iter()
        .map(|a| format!("      <string>{}</string>\n", xml(a)))
        .collect::<String>();
    let home_env = std::env::var("HOME").map_err(|_| invalid("HOME is missing"))?;
    let path = std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".into());
    let schedule = if kind == "api" {
        "  <key>KeepAlive</key><true/>\n".into()
    } else {
        format!(
            "  <key>StartInterval</key><integer>{}</integer>\n",
            interval.max(1)
        )
    };
    let plist = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n  <key>Label</key><string>{}</string>\n  <key>ProgramArguments</key><array>\n{args}  </array>\n  <key>EnvironmentVariables</key><dict><key>HOME</key><string>{}</string><key>PATH</key><string>{}</string></dict>\n  <key>RunAtLoad</key><true/>\n{schedule}  <key>StandardOutPath</key><string>{}</string>\n  <key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",
        xml(label),
        xml(&home_env),
        xml(&path),
        xml(&stdout_log.to_string_lossy()),
        xml(&stderr_log.to_string_lossy())
    );
    Ok(if kind == "api" {
        json!({"label":label,"argv":argv,"plist":plist})
    } else {
        json!({"label":label,"interval_seconds":interval,"argv":argv,"plist":plist})
    })
}
/// Builds one credential-isolated native login or post-login status command.
///
/// Codex scopes labelled accounts below the agent-run home; a labelled Claude
/// login uses the directory chosen by
/// [`crate::adapters::materialize::claude_account_config`], the same one runs
/// and quota collection read, so an existing legacy login is refreshed in
/// place and an ambiguous pair fails. `status` selects the provider's
/// noninteractive verification invocation; both command forms retain no
/// provider output in agent-run's JSON response.
fn native_login_command(
    home: &Path,
    binary: &Path,
    runtime_home: &Path,
    kind: Adapter,
    account: Option<&str>,
    status: bool,
) -> Result<tokio::process::Command> {
    let mut command = tokio::process::Command::new(binary);
    // Only PATH and HOME are needed to locate the provider binary and run its
    // browser flow; every other inherited variable, including explicit
    // credential variables, is withheld from the interactive child.
    command.env_clear();
    for name in ["PATH", "HOME"] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    match kind {
        Adapter::Codex => {
            command.arg("login");
            if status {
                command.arg("status");
            }
            if let Some(label) = account {
                let path = crate::adapters::materialize::account_home(home, kind, label);
                fs::private_dir(&path)?;
                command.env("CODEX_HOME", path);
            }
        }
        Adapter::Claude => {
            // Claude verifies an existing session with `auth status --json`,
            // a sibling of `auth login` rather than a suffix appended to it.
            if status {
                command.args(["auth", "status", "--json"]);
            } else {
                command.args(["auth", "login"]);
            }
            match account {
                Some(label) => {
                    let path = crate::adapters::materialize::claude_account_config(
                        home,
                        runtime_home,
                        label,
                    )?;
                    fs::private_dir(&path)?;
                    command.env("CLAUDE_CONFIG_DIR", path);
                }
                // An omitted account authenticates the host CLI's native
                // global state rather than any agent-run private directory.
                None => {
                    let native = std::env::var_os("CLAUDE_CONFIG_DIR")
                        .filter(|value| !value.is_empty())
                        .map(PathBuf::from)
                        .or_else(|| {
                            std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".claude"))
                        })
                        .ok_or_else(|| invalid("HOME is missing"))?;
                    command.env("CLAUDE_CONFIG_DIR", native);
                }
            }
        }
        Adapter::Glm => {
            return Err(invalid("auth login is not supported for this runtime yet"));
        }
    }
    Ok(command)
}

/// Runs Python-compatible native authentication and verifies its resulting state.
///
/// `claude_only` distinguishes the convenience `login` syntax from explicit
/// `auth <label> <runtime>`: the former accepts only Claude, while the latter
/// supports configured Codex accounts. Provider failures preserve their exit
/// status, emit only a fixed diagnostic, and never expose native status output.
async fn login(
    home: &Path,
    name: &str,
    account: Option<&str>,
    claude_only: bool,
) -> Result<(i32, Value)> {
    let target = match agent_run_config::provider_config::ProviderConfig::load(home) {
        Ok((cfg, _)) => provider_login_target(home, &cfg, name, account, claude_only)?,
        Err(_) => {
            let cfg = Config::load(home)?;
            let runtime = cfg.runtime(name)?;
            let kind = runtime.kind()?;
            let account = runtime.selected_account(account)?;
            LoginTarget {
                binary: runtime.binary.clone(),
                runtime_home: runtime.home.clone(),
                kind,
                label: account.clone(),
                reply: json!({"account":account,"runtime":if kind == Adapter::Claude {"claude"} else {name},"status":"ok"}),
            }
        }
    };
    let kind = target.kind;
    if claude_only && kind != Adapter::Claude {
        return Err(invalid(format!(
            "login supports Claude only; use agent-run auth <label> {name}"
        )));
    }
    let (binary, runtime_home, account) = (&target.binary, &target.runtime_home, &target.label);
    let account_name = account.as_deref().unwrap_or("default");
    // Interactive native authentication owns its prompts and credential storage.
    let status = native_login_command(home, binary, runtime_home, kind, account.as_deref(), false)?
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;
    let code = status.code().unwrap_or(1);
    if code != 0 {
        eprintln!("auth login failed for {account_name} {name} (exit {code})");
        return Ok((code, Value::Null));
    }
    let status = native_login_command(home, binary, runtime_home, kind, account.as_deref(), true)?
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await?;
    let code = status.code().unwrap_or(1);
    if code != 0 {
        eprintln!("auth login status failed for {account_name} {name} (exit {code})");
        return Ok((code, Value::Null));
    }
    Ok((0, target.reply))
}

/// One resolved native login: the harness executable and state root, the
/// adapter family, the protected account label (`None` = the harness's
/// native global login) and the success reply.
struct LoginTarget {
    binary: PathBuf,
    runtime_home: PathBuf,
    kind: Adapter,
    label: Option<String>,
    reply: Value,
}

/// Resolves a schema-2 login: `name` is a native-connection provider, the
/// account is one of its bindings (by provider-local label or global account
/// id; optional only when it has exactly one), and the credential storage is
/// the registered account's own `native:<harness>` or
/// `named:<harness>:<label>` reference on that provider's harness. A label
/// colliding with another binding's global id is refused before login.
fn provider_login_target(
    home: &Path,
    cfg: &agent_run_config::provider_config::ProviderConfig,
    name: &str,
    account: Option<&str>,
    claude_only: bool,
) -> Result<LoginTarget> {
    use agent_run_domain::{
        catalog::{HarnessId, ProviderConnection},
        credential_ref::CredentialRef,
    };
    let provider = name
        .parse()
        .ok()
        .and_then(|id| cfg.providers.get(&id))
        .ok_or_else(|| invalid("unknown provider"))?;
    if claude_only && provider.harness != HarnessId::ClaudeCode {
        return Err(invalid(format!(
            "login supports Claude only; use agent-run auth <label> {name}"
        )));
    }
    if provider.connection != ProviderConnection::Native {
        return Err(invalid(
            "auth login is supported only for native-login providers",
        ));
    }
    let binding = match account {
        Some(wanted) => {
            let label = provider
                .bindings
                .iter()
                .find(|binding| binding.label.as_str() == wanted);
            let id = provider
                .bindings
                .iter()
                .find(|binding| binding.account.as_str() == wanted);
            if label
                .zip(id)
                .is_some_and(|(label, id)| label.account != id.account)
            {
                return Err(invalid("ambiguous account selector: label and id differ"));
            }
            label.or(id)
        }
        None if provider.bindings.len() == 1 => provider.bindings.first(),
        None => {
            return Err(invalid(
                "provider binds several accounts; name one with --account",
            ));
        }
    }
    .ok_or_else(|| invalid("account is not bound to this provider"))?;
    let record = Store::open(home)?
        .account(&binding.account)?
        .ok_or_else(|| invalid("bound account is not registered"))?;
    let label = match CredentialRef::from_secret(&record.secret_ref)? {
        CredentialRef::Native(harness) if harness == provider.harness => None,
        CredentialRef::Named { harness, label } if harness == provider.harness => {
            Some(label.as_str().to_owned())
        }
        _ => {
            return Err(invalid(
                "account is not a native login of this provider's harness",
            ));
        }
    };
    let harness = cfg
        .harnesses
        .get(&provider.harness)
        .ok_or_else(|| invalid("provider harness is not configured"))?;
    let kind = match provider.harness {
        HarnessId::Codex => Adapter::Codex,
        HarnessId::ClaudeCode => Adapter::Claude,
    };
    Ok(LoginTarget {
        binary: harness.binary.clone(),
        runtime_home: harness.home.clone(),
        kind,
        label,
        reply: json!({"account": binding.account, "provider": name, "status": "ok"}),
    })
}

/// Executes one parsed command and returns its public process exit status.
///
/// Success writes JSON except for the text guide, text transcript viewer and
/// stdio server. Expected failures
/// propagate as typed errors for `main` to render as the Python-compatible
/// JSON error envelope. `start` and `resume` exclusively call the resident
/// socket broker. Long-lived API and supervisor processes select their own
/// component log before the process-wide logger is initialized.
pub async fn run(cli: Cli) -> Result<i32> {
    // A worker frontend is a thin socket client. It must never migrate/open
    // the store or inherit Desktop host capabilities through the normal MCP.
    if matches!(cli.command, Command::WorkerMcp) {
        transport::worker_mcp::serve_from_env().await?;
        return Ok(0);
    }
    let home = fs::home(cli.home.clone())?;
    // An older state database must be migrated together with its config;
    // no other command may open (and so auto-upgrade) it first.
    // The permission hook helper never opens the database.
    if !matches!(
        cli.command,
        Command::Config { .. } | Command::Doc { .. } | Command::PermissionRequest { .. }
    ) {
        crate::migrate::require_current_store(&home)?;
    }
    if matches!(&cli.command, Command::Mcp) {
        transport::mcp::exec_desktop_frontend(&home)?;
    }
    let component = match &cli.command {
        Command::Mcp => "mcp",
        Command::Api { .. } => "api",
        Command::Supervisor { .. } => "supervisor",
        Command::ServiceExec { .. } => "services",
        _ => "cli",
    };
    agent_run_core::logging::configure(&home, component);
    run_with(cli, CliDependencies::production(home)).await
}

/// Executes one parsed command against explicitly supplied service, broker, and output seams.
///
/// The parser and command surface are unchanged. Production callers use [`run`], which supplies
/// the real service, Unix-socket broker, and stdout sink; tests can provide fakes without opening
/// a broker socket or constructing a local runtime. The supplied dependencies remain borrowed by
/// asynchronous calls only for the duration of this execution.
pub async fn run_with(cli: Cli, dependencies: CliDependencies) -> Result<i32> {
    let home = fs::home(cli.home)?;
    match cli.command {
        Command::Init => (dependencies.output)(&crate::init::initialize(&home)?)?,
        Command::Doctor => {
            let report = (dependencies.doctor)(&home)?;
            let ok = report.ok();
            (dependencies.output)(&serde_json::to_value(report)?)?;
            return Ok(if ok { 0 } else { 2 });
        }
        Command::Start(a) => {
            let task = task_text(&a.task, 1024 * 1024)?;
            let schema = if let Some(p) = a.output_schema {
                Some(
                    serde_json::from_str::<serde_json::Map<String, Value>>(&p)
                        .map_err(|_| invalid("output schema must be a JSON object"))?,
                )
            } else {
                None
            };
            // The strict provider request; the broker admits it (never this
            // one-shot process) and chooses the account unless one is named.
            let read_roots = a
                .read_roots
                .iter()
                .map(|p| absolute(p))
                .collect::<Result<Vec<_>>>()?;
            let mut request: agent_run_domain::ProviderStartRequest =
                serde_json::from_value(json!({
                    "provider": a.provider,
                    "model": a.model,
                    "profile": a.profile,
                    "task": task,
                    "workdir": absolute(&a.workdir.unwrap_or(std::env::current_dir()?))?,
                    "write": a.write,
                    "fast": a.fast,
                    "effort": a.effort,
                    "timeout_seconds": a.timeout_seconds,
                    "read_roots": read_roots,
                    "output_schema": schema,
                    "orchestrator": a.session.resolve()?,
                    "request_id": a.request_id,
                    "account": a.account,
                    "display_name": a.display_name,
                }))
                .map_err(|_| invalid("invalid provider start arguments"))?;
            request.validate()?;
            let result = dependencies
                .broker
                .call("start", serde_json::to_value(request)?)
                .await?;
            if a.wait {
                let id: AgentId = serde_json::from_value(result["agent_id"].clone())?;
                let mut wait = json!({"agent_id": id});
                if let Some(sequence) = result
                    .get("sequence")
                    .or_else(|| result["agent"].get("sequence"))
                {
                    wait["sequence"] = sequence.clone();
                } else if let Some(run_id) = result.get("run_id") {
                    wait["run_id"] = run_id.clone();
                }
                let result = dependencies.broker.call("wait", wait).await?;
                (dependencies.output)(&result)?;
                return Ok(result_code(&result));
            }
            (dependencies.output)(&admission_output(&result)?)?;
        }
        Command::Resume(a) => {
            let task = match (a.task, a.task_file) {
                (Some(task), None) => task_text(&task, 1024 * 1024)?,
                (None, Some(path)) if path == Path::new("-") => read_stdin(1024 * 1024)?,
                (None, Some(path)) => read_input(&path, 1024 * 1024)?,
                _ => return Err(invalid("provide exactly one resume task source")),
            };
            let mut arguments = json!({"agent_id":a.agent_id,"task":task,"timeout_seconds":a.timeout_seconds,"request_id":a.request_id,"orchestrator":a.session.resolve()?});
            if let Some(run_id) = a.run_id {
                arguments["run_id"] = json!(run_id);
            }
            if let Some(display_name) = a.display_name {
                arguments["display_name"] = json!(display_name);
            }
            let result = dependencies.broker.call("resume", arguments).await?;
            (dependencies.output)(&admission_output(&result)?)?;
        }
        Command::Cancel { agent_id, run_id } => {
            let id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let value = dependencies.service.cancel(&id)?;
            (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?
        }
        Command::Steer {
            agent_id,
            run_id,
            text,
        } => {
            let id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let value = dependencies.service.steer(&id, &text)?;
            (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?
        }
        Command::Bind(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            let mut store = Store::open(&home)?;
            if let Some(pool) = &a.pool {
                let pool: agent_run_domain::pool::PoolId = pool.parse()?;
                store.bind_pool(&pool, &reference, crate::domain::now())?;
                (dependencies.output)(&json!({"pool_id": pool, "bound": true}))?;
                return Ok(0);
            }
            let agent_id = a
                .agent_id
                .ok_or_else(|| invalid("agent id or --pool is required"))?;
            let run =
                agent_run_core::agent_identity::resolve(&store, &agent_id, a.run_id.as_ref())?;
            hooks::bind::bind(&mut store, run.id.clone(), reference, crate::domain::now())?;
            (dependencies.output)(&agent_run_core::agent_identity::result(
                &run,
                store.delivery_status(&run.id)?,
            )?)?;
        }
        Command::StartPool { spec, session } => {
            let mut request = spec_json(&spec)?;
            if let Some(reference) = session.resolve()? {
                request["orchestrator"] = serde_json::to_value(reference)?;
            }
            let result = dependencies.broker.call("start_pool", request).await?;
            (dependencies.output)(&result)?;
        }
        Command::PoolPost {
            pool_id,
            request_id,
            text,
        } => {
            let message = task_text(&text, agent_run_domain::pool::MAX_BODY_BYTES)?;
            let result = dependencies
                .broker
                .call(
                    "pool_post",
                    json!({"pool_id": pool_id, "request_id": request_id, "message": message}),
                )
                .await?;
            (dependencies.output)(&result)?;
        }
        Command::PoolReplace {
            pool_id,
            agent_id,
            request_id,
            spec,
        } => {
            let start = spec.as_deref().map(spec_json).transpose()?;
            let result = dependencies
                .broker
                .call(
                    "pool_replace",
                    json!({"pool_id": pool_id, "agent_id": agent_id, "request_id": request_id, "start": start}),
                )
                .await?;
            (dependencies.output)(&result)?;
        }
        Command::Pool {
            pool_id,
            after_seq,
            before_seq,
            limit,
        } => {
            let result = dependencies
                .broker
                .call(
                    "pool",
                    json!({"pool_id": pool_id, "after_seq": after_seq, "before_seq": before_seq, "limit": limit}),
                )
                .await?;
            (dependencies.output)(&result)?;
        }
        Command::Pools {
            state,
            offset,
            limit,
            text,
        } => {
            let query: agent_run_domain::pool::ListPoolsQuery =
                serde_json::from_value(json!({"state": state, "offset": offset, "limit": limit}))
                    .map_err(|_| invalid("invalid pool list arguments"))?;
            query.validate()?;
            let result = dependencies
                .broker
                .call("list_pools", serde_json::to_value(query)?)
                .await?;
            if text {
                (dependencies.text_output)(&format!(
                    "{}\n",
                    crate::transport::mcp_text::list_pools_text(&result)?
                ))?;
            } else {
                (dependencies.output)(&result)?;
            }
        }
        Command::Context(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            (dependencies.output)(&serde_json::to_value(hooks::context::build(
                &home, &reference, None,
            )?)?)?;
        }
        Command::Hook { command } => {
            let input = read_stdin(1024 * 1024)?;
            let payload: Value = serde_json::from_str(&input)
                .map_err(|_| invalid("hook payload must be a JSON object"))?;
            match command {
                Hook::Context(transport) => {
                    let reference =
                        hooks::bind::normalize(&payload, false, &transport.transport)?.reference;
                    let context = hooks::context::build(&home, &reference, None)?;
                    let output = if context.injected && !context.text.trim().is_empty() {
                        json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":context.text}})
                    } else {
                        json!({})
                    };
                    (dependencies.output)(&output)?;
                }
                Hook::Bind(transport) => {
                    let mut store = Store::open(&home)?;
                    let result = hooks::bind::run_hook_bound(
                        &mut store,
                        &payload,
                        &transport.transport,
                        None,
                    )?;
                    (dependencies.output)(
                        &json!({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":result.message()}}),
                    )?;
                }
            }
        }
        Command::Agents(a) => {
            if !a.follow {
                (dependencies.output)(
                    &dependencies
                        .service
                        .list(Query {
                            active: a.active,
                            offset: a.offset,
                            limit: a.limit,
                            after_revision: None,
                            after_message_revision: None,
                            wait_seconds: 0.0,
                            orchestrator: a.session.resolve()?,
                        })
                        .await?,
                )?;
            } else {
                follow_agents(dependencies, &a).await?;
            }
        }
        Command::Answer { agent_id, run_id } => {
            let id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let value = dependencies.service.answer(&id)?;
            (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?;
        }
        Command::Transcript {
            agent_id,
            run_id,
            mut cursor,
            limit,
            view,
            tail_blocks,
            before_cursor,
            follow,
            full,
            format,
        } => {
            let agent_id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let query = agent_run_domain::transcript::TranscriptQuery {
                cursor,
                limit,
                view,
                tail_blocks,
                before_cursor,
            };
            query.validate()?;
            if view == agent_run_domain::transcript::TranscriptView::Blocks && (follow || full) {
                return Err(invalid(
                    "blocks are bounded pages; --follow and --full require raw view",
                ));
            }
            let page_at = |cursor| {
                let mut page = query.clone();
                page.cursor = cursor;
                dependencies
                    .service
                    .transcript_options(&agent_id, &page, run_id.is_none())
            };
            // An explicit --format always wins; otherwise text is interactive
            // and JSON keeps piped consumers on the historical machine shape.
            let text = TranscriptFormat::effective(format) == TranscriptFormat::Text;
            // Streaming state persists across pages and polls so journal
            // fragments of one model message render continuously; the sink
            // writes each rendered chunk immediately.
            let mut renderer = crate::transcript::Renderer::default();
            if full {
                let page = page_at(cursor)?;
                let mut messages = page["messages"].as_array().cloned().unwrap_or_default();
                let mut page_cursor = cursor;
                let mut pages = 1usize;
                let mut current = page;
                while current["complete"] != true {
                    let next = current["next_cursor"]
                        .as_i64()
                        .ok_or_else(|| invalid("transcript pagination did not advance"))?;
                    if next <= page_cursor {
                        return Err(invalid("transcript pagination did not advance"));
                    }
                    page_cursor = next;
                    current = page_at(page_cursor)?;
                    messages.extend(current["messages"].as_array().cloned().unwrap_or_default());
                    pages += 1;
                }
                if text {
                    renderer.page(&messages, &mut |line| (dependencies.text_output)(line))?;
                    renderer.finish(&mut |line| (dependencies.text_output)(line))?;
                } else {
                    let value = json!({"agent_id":agent_id,"messages":messages,"cursor":cursor,"next_cursor":null,"complete":true,"pages":pages});
                    (dependencies.output)(
                        &dependencies.service.public_run_result(&agent_id, value)?,
                    )?;
                }
            } else {
                // The interrupt listener is installed before the first page,
                // so a Ctrl-C at any point ends only the viewer, gracefully.
                let mut interrupt = follow
                    .then(|| {
                        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    })
                    .transpose()?;
                loop {
                    let page = page_at(cursor)?;
                    if text {
                        renderer.page(
                            page["messages"]
                                .as_array()
                                .map(Vec::as_slice)
                                .unwrap_or(&[]),
                            &mut |line| (dependencies.text_output)(line),
                        )?;
                    } else {
                        (dependencies.output)(
                            &dependencies
                                .service
                                .public_run_result(&agent_id, page.clone())?,
                        )?;
                    }
                    if let Some(seq) = page["messages"]
                        .as_array()
                        .and_then(|a| a.last())
                        .and_then(|v| v["seq"].as_i64())
                    {
                        cursor = seq;
                    }
                    if !follow {
                        break;
                    }
                    if page["complete"] == true
                        && dependencies.service.agent(&agent_id)?["status"]
                            .as_str()
                            .is_some_and(|status| {
                                matches!(
                                    status,
                                    "succeeded" | "failed" | "lost" | "timed_out" | "cancelled"
                                )
                            })
                    {
                        break;
                    }
                    // Interrupting the viewer never cancels the supervised
                    // agent; the resident supervisor keeps running it.
                    if let Some(interrupt) = interrupt.as_mut() {
                        tokio::select! {_=interrupt.recv()=>break,_=tokio::time::sleep(Duration::from_millis(250))=>{}}
                    }
                }
                // Flush the tail of the streamed item on any viewer exit.
                renderer.finish(&mut |line| (dependencies.text_output)(line))?;
            }
        }
        Command::Models {
            provider,
            profile,
            model,
        } => (dependencies.output)(
            &dependencies
                .service
                .models(agent_run_domain::ModelsQuery {
                    provider,
                    profile,
                    model,
                })
                .await?,
        )?,
        Command::Limits => (dependencies.output)(&dependencies.service.limits()?)?,
        Command::DelegationGuide => {
            // The guide is text, not JSON: print it with a normal newline.
            let value = dependencies.service.delegation_guide()?;
            let text = value
                .as_str()
                .ok_or_else(|| invalid("delegation guide must be text"))?;
            (dependencies.text_output)(&format!("{text}\n"))?;
        }
        Command::Doc { topic } => {
            let topic = topic.as_deref().unwrap_or("index");
            (dependencies.output)(&json!({"topic":topic,"text":crate::dispatch::doc(topic)?}))?;
        }
        Command::Mcp => transport::mcp::serve_with(home, None, dependencies.broker.clone()).await?,
        Command::WorkerMcp => transport::worker_mcp::serve_from_env().await?,
        Command::Api { command } => match command {
            Api::Serve { socket } => {
                if let Some(socket) = socket {
                    transport::socket::serve_at(&home, &socket).await?
                } else {
                    transport::socket::serve(&home).await?
                }
            }
            Api::Launchd {
                binary,
                label,
                stdout_log,
                stderr_log,
            } => emit(&crate::launchd::render(
                &home,
                binary,
                "api",
                0,
                &label,
                stdout_log.unwrap_or_else(|| home.join("logs/api.log")),
                stderr_log.unwrap_or_else(|| home.join("logs/api.err.log")),
            )?)?,
        },
        Command::Capacity { command } => match command {
            Capacity::Order { model } => (dependencies.output)(
                &dependencies
                    .service
                    .capacity_order(agent_run_domain::CapacityOrderQuery { model })?,
            )?,
            Capacity::Launchd {
                binary,
                label,
                stdout_log,
                stderr_log,
            } => emit(&crate::launchd::render(
                &home,
                binary,
                "capacity",
                operator_config(&home)?.0.capacity.collect_interval_seconds,
                &label,
                stdout_log,
                stderr_log.unwrap_or_else(|| home.join("capacity-worker.err.log")),
            )?)?,
            Capacity::Collect { once: _ } => {
                let result = capacity::collect(&home).await?;
                (dependencies.output)(&result)?;
                return Ok(if result["ok"] == true { 0 } else { 2 });
            }
        },
        Command::Delivery { command } => match command {
            Delivery::Status { agent_id, run_id } => {
                let id = dependencies
                    .service
                    .resolve_run_id(&agent_id, run_id.as_ref())?;
                let value = dependencies.service.delivery_status(&id)?;
                (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?
            }
            Delivery::Cancel { delivery_id } => {
                (dependencies.output)(&dependencies.service.delivery_cancel(&delivery_id)?)?
            }
            Delivery::Launchd {
                binary,
                label,
                stdout_log,
                stderr_log,
            } => emit(&crate::launchd::render(
                &home,
                binary,
                "delivery",
                operator_config(&home)?.0.delivery.retry_base_seconds.ceil() as u64,
                &label,
                stdout_log,
                stderr_log.unwrap_or_else(|| home.join("delivery-worker.err.log")),
            )?)?,
            Delivery::Dispatch => {
                (dependencies.output)(
                    &json!({"processed":crate::delivery::dispatch_once(&home).await?}),
                )?;
            }
        },
        Command::Login { runtime, account } => {
            let (code, value) = login(&home, &runtime, account.as_deref(), true).await?;
            if code == 0 {
                emit(&value)?;
            }
            return Ok(code);
        }
        Command::Auth { label, runtime } => {
            let (code, value) = login(&home, &runtime, Some(&label), false).await?;
            if code == 0 {
                emit(&value)?;
            }
            return Ok(code);
        }
        Command::Config { command } => match command {
            ConfigCommand::Migrate {
                mapping,
                target_config,
                dry_run: _,
                apply,
                ack,
                from_release,
            } => {
                let result = match (mapping.as_deref(), target_config.as_deref()) {
                    (Some(mapping), None) => crate::migrate::migrate(
                        &home,
                        mapping,
                        apply,
                        &ack,
                        from_release.as_deref(),
                    )?,
                    (None, Some(target)) if ack.is_empty() => {
                        crate::migrate::migrate_v2(&home, target, apply, from_release.as_deref())?
                    }
                    _ => {
                        return Err(invalid(
                            "choose one migration input; --ack applies only to a legacy mapping",
                        ));
                    }
                };
                (dependencies.output)(&result)?;
            }
            ConfigCommand::Rollback { snapshot } => {
                (dependencies.output)(&crate::migrate::rollback(&home, &snapshot)?)?
            }
        },
        Command::Storage { command } => match command {
            StorageCommand::Status => (dependencies.output)(&crate::storage_admin::status(&home)?)?,
            StorageCommand::Compact { dry_run: _, apply } => {
                (dependencies.output)(&crate::storage_admin::compact(&home, apply)?)?
            }
            StorageCommand::Recover => {
                (dependencies.output)(&crate::storage_admin::recover(&home)?)?
            }
        },
        Command::Accounts { command } => match command {
            AccountCommand::Register {
                id,
                auth_family,
                reference,
            } => {
                let reference: SecretRef = reference.parse()?;
                let source = CredentialRef::from_secret(&reference)?;
                let record = AccountRecord {
                    account_id: id.clone(),
                    auth_family: auth_family.clone(),
                    secret_ref: reference,
                    status: AccountStatus::Enabled,
                };
                Store::open(&home)?.register_account(&record)?;
                (dependencies.output)(&json!({
                    "account_id": id, "auth_family": auth_family.as_str(),
                    "status": "enabled", "source": source.kind()
                }))?;
            }
            AccountCommand::List => {
                let records = Store::open(&home)?.list_accounts()?;
                let views: Vec<_> = records
                    .iter()
                    .map(|record| {
                        json!({
                            "account_id": record.account_id,
                            "auth_family": record.auth_family.as_str(),
                            "status": record.status.as_str(),
                            "source": CredentialRef::from_secret(&record.secret_ref)
                                .map(|reference| reference.kind()).unwrap_or("unknown"),
                        })
                    })
                    .collect();
                (dependencies.output)(&json!({"accounts":views}))?;
            }
            AccountCommand::Disable { id } => {
                Store::open(&home)?.disable_account(&id)?;
                (dependencies.output)(&json!({"account_id":id,"status":"disabled"}))?;
            }
        },
        // The supervisor is spawned by posix_spawn with three fixed bootstrap
        // descriptors; keep that signature from the launch work.
        Command::Supervisor {
            agent_id,
            ready_fd,
            identity_fd,
            error_fd,
        } => crate::supervisor::run(&home, &agent_id, [ready_fd, identity_fd, error_fd]).await?,
        Command::ServiceExec { generation } => {
            agent_run_core::managed_services::bootstrap(&home, &generation)?
        }
        Command::DenyCommand { name: _ } => {
            eprintln!("agent-run: command denied by the configured developer environment");
            return Ok(126);
        }
        Command::DoctorCanary {
            ready_fd,
            identity_fd,
            error_fd,
        } => crate::doctor::run_canary([ready_fd, identity_fd, error_fd])?,
        Command::PermissionRequest { allow_mcp } => {
            if allow_mcp.iter().any(|server| {
                server.is_empty()
                    || !server.bytes().enumerate().all(|(index, byte)| {
                        (index == 0 && (byte.is_ascii_lowercase() || byte.is_ascii_digit()))
                            || byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'-' | b'_')
                    })
            }) {
                return Err(invalid(
                    "--allow-mcp values must be lowercase MCP server identifiers",
                ));
            }
            let payload: Value = serde_json::from_str(&read_stdin(1024 * 1024)?)?;
            if let Some(decision) = crate::adapters::codex::permission_request_decision(
                &payload,
                &allow_mcp.into_iter().collect(),
            ) {
                emit(&decision)?;
            }
        }
    }
    Ok(0)
}

#[cfg(test)]
mod login_selection_tests {
    //! Credential selection must not depend on the order of provider bindings.

    use super::provider_login_target;
    use agent_run_config::provider_config::ProviderConfig;
    use agent_run_domain::catalog::{AccountRecord, AccountStatus};
    use agent_run_store::Store;

    /// A provider label that is another binding's global ID is ambiguous;
    /// unambiguous labels and IDs still select their registered account.
    #[test]
    fn login_rejects_cross_namespace_account_collision() {
        let home = tempfile::tempdir().unwrap();
        Store::initialize(home.path()).unwrap();
        let config = format!(
            "schema_version = 2\n[harnesses.codex]\nbinary = \"/bin/true\"\nhome = \"{0}/codex\"\n[harnesses.claude-code]\nbinary = \"/bin/true\"\nhome = \"{0}/claude\"\n[providers.codex]\nharness = \"codex\"\nconnection = {{ kind = \"native\" }}\nauth_family = \"openai\"\nlimits_source = \"none\"\n[[providers.codex.models]]\nid = \"gpt\"\n[[providers.codex.bindings]]\nlabel = \"acct-b\"\naccount = \"acct-a\"\n[[providers.codex.bindings]]\nlabel = \"b\"\naccount = \"acct-b\"\n",
            home.path().display()
        );
        let config = ProviderConfig::parse(&config, home.path()).unwrap();
        let mut store = Store::open(home.path()).unwrap();
        for (id, reference) in [("acct-a", "native:codex"), ("acct-b", "named:codex:b")] {
            store
                .register_account(&AccountRecord {
                    account_id: id.parse().unwrap(),
                    auth_family: "openai".parse().unwrap(),
                    secret_ref: reference.parse().unwrap(),
                    status: AccountStatus::Enabled,
                })
                .unwrap();
        }
        let error = provider_login_target(home.path(), &config, "codex", Some("acct-b"), false)
            .err()
            .expect("ambiguous selector is refused");
        assert!(error.to_string().contains("ambiguous"), "{error}");
        let by_label =
            provider_login_target(home.path(), &config, "codex", Some("b"), false).unwrap();
        assert_eq!(by_label.reply["account"], "acct-b");
        let by_id =
            provider_login_target(home.path(), &config, "codex", Some("acct-a"), false).unwrap();
        assert_eq!(by_id.reply["account"], "acct-a");
    }
}
