//! Operator CLI. Starts and resumes always go through the resident broker.
use crate::{
    Error, Result,
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
    /// Read exact-execution rows with proven attempt boundaries. A service seam
    /// lacking this evidence refuses historical reconstruction instead of guessing.
    fn transcript_recovery(&self, _id: &AgentId, _cursor: i64, _limit: usize) -> Result<Value> {
        Err(crate::Error::Unsupported(
            "scoped transcript reader is unavailable".into(),
        ))
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
    fn limits(&self) -> CliFuture<'_>;
    /// Return the ordered capacity view, optionally for one exact model.
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> CliFuture<'_>;
    /// Returns account-free guide text for the exact catalog filters; omitted
    /// filters retain default guidance, and invalid names preserve typed errors.
    fn delegation_guide(&self, query: agent_run_domain::ModelsQuery) -> CliFuture<'_>;
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
        {
            let _ = id;
            Err(crate::Error::BrokerUnavailable)
        }
    }
    fn steer(&self, id: &AgentId, text: &str) -> Result<Value> {
        {
            let _ = (id, text);
            Err(crate::Error::BrokerUnavailable)
        }
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
    /// Read bounded recovery rows directly through the existing read-only store.
    fn transcript_recovery(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        Store::open(&self.home)?.transcript_recovery(id, cursor, limit)
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
    fn limits(&self) -> CliFuture<'_> {
        Box::pin(Service::limits(self))
    }
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> CliFuture<'_> {
        Box::pin(Service::capacity_order(self, query))
    }
    /// Forwards the CLI's exact optional filters to the shared guide service.
    fn delegation_guide(&self, query: agent_run_domain::ModelsQuery) -> CliFuture<'_> {
        Box::pin(Service::delegation_guide_filtered(self, query))
    }
    fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        Service::delivery_status(self, id)
    }
    fn delivery_cancel(&self, id: &str) -> Result<Value> {
        {
            let _ = id;
            Err(crate::Error::BrokerUnavailable)
        }
    }
}

/// Production broker client preserving the existing Unix-socket call path.
pub(crate) struct SocketBroker {
    /// Agent-run home whose private socket receives each request.
    pub(crate) home: PathBuf,
}

/// Authenticated private writer client for operator-only CLI mutations.
/// The broker derives authority from its kernel peer; unavailable broker
/// returns an error and never falls back to a local writable Store.
fn operator(home: &Path) -> agent_run_core::persist::Producer {
    agent_run_core::persist::Producer::connect(
        &home.join("writer.sock"),
        agent_run_core::persist::cli_hello(),
        agent_run_core::persist::cli_producer_id(),
    )
}

impl CliBroker for SocketBroker {
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
        Box::pin(transport::socket::client(&self.home, method, params))
    }
}

/// One-shot broker calls through an explicitly selected operator socket.
struct OperatorBroker {
    socket: PathBuf,
    expected_uid: Option<u32>,
}

impl CliBroker for OperatorBroker {
    /// Retains the ordinary CLI's deadlines and retry rules while verifying the server UID.
    fn call<'a>(&'a self, method: &'a str, params: Value) -> CliFuture<'a> {
        let uid = self.expected_uid.unwrap_or_else(|| {
            // SAFETY: geteuid only reads the current process identity.
            unsafe { libc::geteuid() }
        });
        Box::pin(transport::socket::client_at(
            &self.socket,
            uid,
            method,
            params,
        ))
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
    /// Explicit operator endpoint; local state is not opened in this mode.
    #[arg(long, global = true)]
    pub broker_socket: Option<PathBuf>,
    /// Trusted broker UID; defaults to the caller's effective UID.
    #[arg(long, global = true, requires = "broker_socket")]
    pub broker_uid: Option<u32>,
    #[arg(long, global = true, env = "AGENT_RUN_HOME")]
    pub home: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}
/// Top-level commands, including two hidden runtime implementation commands.
#[derive(Subcommand, Debug)]
pub enum Command {
    Init,
    /// Gracefully restart the service and continue its active agents.
    Restart(Restart),
    /// Read-only local diagnostics; emits one JSON report and exits 0/2/3.
    Doctor {
        /// Explicit JSON selection; JSON remains the default output.
        #[arg(long)]
        json: bool,
    },
    Start(Start),
    /// Atomically admit independent jobs from one JSON request (`-` reads stdin).
    StartBatch {
        /// JSON file or `-`: request_id and ordered jobs.
        #[arg(long)]
        spec: String,
    },
    /// Look up the permanent receipt for an exact scoped batch intent.
    BatchReceipt {
        /// JSON file or `-`: the original batch intent with expected_store_id.
        #[arg(long)]
        spec: String,
    },
    /// Read exact status selectors from one JSON request (`-` reads stdin).
    Status {
        /// JSON file or `-`: version, targets and optional view/fence.
        #[arg(long)]
        spec: String,
    },
    /// Export one exact pinned transcript fragment from JSON (`-` reads stdin).
    TranscriptExact {
        /// JSON file or `-`: store, selector and bounded continuation.
        #[arg(long)]
        spec: String,
    },
    /// Read one page of a root execution's exact initial task from JSON (`-` reads stdin).
    InitialTask {
        /// JSON file or `-`: mode, store, root agent, fences and page.
        #[arg(long)]
        spec: String,
    },
    /// List or release durable lineage history pins.
    HistoryPins {
        #[command(subcommand)]
        command: HistoryPinsCommand,
    },
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
    /// Report the resident broker's versioned read schemas and transport limits.
    Capabilities {
        /// Explicitly read the current database identity.
        #[arg(long)]
        include_store_id: bool,
        /// Detect the `initial_task` read method, its modes and bounds.
        #[arg(long)]
        include_initial_task: bool,
    },
    /// Read an exact verified artifact chunk fenced to the current execution.
    AnswerChunk {
        /// Stable logical agent identity.
        agent_id: AgentId,
        /// Required positive current execution receipt.
        #[arg(long)]
        sequence: std::num::NonZeroU32,
        /// Required lowercase SHA-256 of the complete artifact.
        #[arg(long)]
        expected_sha256: agent_run_domain::Sha256Digest,
        /// Byte offset on a UTF-8 boundary, including EOF.
        #[arg(long)]
        offset_bytes: u64,
        /// Payload bytes in 1..=131072; defaults to 65536.
        #[arg(long, default_value_t = agent_run_domain::observation::DEFAULT_CHUNK_BYTES)]
        max_bytes: usize,
    },
    /// Read one verified answer with the historical CLI inline cutoff.
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
        /// Read-only reconstruction of legacy Claude fragments from an explicit
        /// native JSONL file. Requires --full; raw journal remains unchanged.
        #[arg(long, requires="full", conflicts_with_all=["follow","tail_blocks","before_cursor"])]
        native_history: Option<PathBuf>,
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
    /// Prints compact routing guidance, with the same exact filters as models.
    DelegationGuide {
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
    Doc {
        topic: Option<String>,
    },
    Mcp,
    /// Internal run-bound worker channel; context is inherited from the supervisor.
    #[command(name = "_worker-mcp", hide = true)]
    WorkerMcp,
    /// Account-owned launcher used by a shared broker.
    #[command(name = "_launch-worker", hide = true)]
    LaunchWorker {
        /// Unix socket whose access is controlled by the host.
        #[arg(long)]
        socket: PathBuf,
    },
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
        /// Private pipe carrying the launch capability (see `launch::spawn_launch`).
        #[arg(long)]
        capability_fd: i32,
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

/// Operator actions for durable history pins.
#[derive(Subcommand, Debug)]
pub enum HistoryPinsCommand {
    /// List pins in exclusive key order.
    List {
        /// Current pin state to list.
        #[arg(long, default_value = "all", value_parser = ["all", "active", "released"])]
        status: String,
        /// Maximum page size, at most 1000.
        #[arg(long, default_value_t = 100)]
        limit: usize,
        /// Exclude this key and all earlier keys.
        #[arg(long)]
        after_pin_id: Option<String>,
        /// Refuse a database different from the recorded store identity.
        #[arg(long)]
        expected_store_id: Option<String>,
    },
    /// Release one exact pin idempotently.
    Release {
        /// Canonical pin identifier from a batch receipt.
        #[arg(long)]
        pin_id: String,
        /// Refuse a database different from the recorded store identity.
        #[arg(long)]
        expected_store_id: Option<String>,
    },
}

/// Select the native service-manager target for one planned restart.
#[derive(Args, Debug)]
pub struct Restart {
    /// systemd unit or launchd service target; defaults to the standard service.
    #[arg(long)]
    pub target: Option<String>,
    /// Use the caller's systemd user manager rather than the system manager.
    #[arg(long)]
    pub systemd_user: bool,
    /// Stable request identity for retrying the same operation.
    #[arg(long)]
    pub request_id: Option<String>,
    /// Resume a prepared operation after its manager definitively failed.
    #[arg(long, conflicts_with = "request_id")]
    pub recover: Option<i64>,
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
    /// Callback completion is the default; false selects legacy compatibility.
    #[arg(long, action=clap::ArgAction::Set, num_args=0..=1, default_value="true", default_missing_value="true")]
    pub explicit_finish: bool,
    /// Observe without inference, opt into bounded model recovery, or pause probes.
    #[arg(long, value_parser = ["off", "observe", "recover"], default_value = "observe")]
    pub idle_ping: String,
    /// Compatibility only: complete on the native result instead of finish.
    #[arg(long, conflicts_with = "explicit_finish")]
    pub legacy_completion: bool,
    #[arg(long)]
    pub effort: Option<String>,
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

/// Native host transport selection for normalized bind/context hook payloads.
#[derive(Args, Debug)]
pub struct HookTransport {
    /// The delivery transport encoded by the hook payload.
    #[arg(long, default_value = "codex_queue", value_parser = ["claude_uds", "codex_queue", "opencode"])]
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

/// Builds the same list filters and revision window for local and operator viewers.
fn agents_query(
    agents: &Agents,
    after_revision: Option<i64>,
    after_message_revision: Option<i64>,
) -> Result<Query> {
    Ok(Query {
        active: agents.active,
        offset: agents.offset,
        limit: agents.limit,
        after_revision,
        after_message_revision,
        wait_seconds: if after_revision.is_none() {
            0.0
        } else {
            FOLLOW_WAIT_SECONDS
        },
        orchestrator: agents.session.resolve()?,
    })
}

/// Sends a validated list query through the broker without resolving a local home.
async fn broker_agent_list(broker: &dyn CliBroker, query: Query) -> Result<Value> {
    query.validate()?;
    broker
        .call(
            "list_agents",
            json!({
                "active": query.active,
                "offset": query.offset,
                "limit": query.limit,
                "after_revision": query.after_revision,
                "after_message_revision": query.after_message_revision,
                "wait_seconds": query.wait_seconds,
                "orchestrator": query.orchestrator,
            }),
        )
        .await
}

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
/// snapshot per meaningful change: each round long-polls the list source with the
/// last event revision and transcript watermark, so status, usage, tool-count,
/// name, failure, delivery and answer changes all wake it, while pages whose
/// only movement is observation time are counted and dropped. The waiter is
/// abortable at every await: Ctrl-C ends the viewer (never the supervised
/// agents) and a closed output pipe terminates the process through the
/// writer's own error.
async fn follow_agents<F, Fut>(a: &Agents, list: F, output: &CliOutput) -> Result<()>
where
    F: Fn(Query) -> Fut,
    Fut: Future<Output = Result<Value>>,
{
    let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    let mut after_revision: Option<i64> = None;
    let mut after_message_revision: Option<i64> = None;
    let mut signatures: std::collections::BTreeMap<String, String> = Default::default();
    let mut total: Option<i64> = None;
    loop {
        let request = agents_query(a, after_revision, after_message_revision)?;
        // A local wait retains no transaction; dropping a remote wait closes
        // its socket without affecting agents or retrying the observation.
        let page = tokio::select! {
            biased;
            _ = interrupt.recv() => return Ok(()),
            page = list(request) => page?,
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
        (output)(&page)?;
    }
}

/// API daemon commands retained from the Python operator surface.
#[derive(Subcommand, Debug)]
pub enum Api {
    Serve {
        #[arg(long)]
        socket: Option<PathBuf>,
        /// Group-accessible operator socket in a protected directory.
        #[arg(long, requires = "operator_gid", conflicts_with = "socket")]
        operator_socket: Option<PathBuf>,
        /// Unix group granted access to the operator socket.
        #[arg(long, requires = "operator_socket", conflicts_with = "socket")]
        operator_gid: Option<u32>,
        /// Shared broker routes; OS users and groups control the sockets.
        #[arg(long, conflicts_with_all = ["socket", "operator_socket", "operator_gid"])]
        shared_config: Option<PathBuf>,
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
    /// Capacity order; schema 2 lists providers, optionally for one model.
    Order {
        /// Exact provider-visible model id.
        #[arg(long)]
        model: Option<String>,
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
/// Initialize the legacy-compatible operator home under exclusive offline authority.
/// Existing valid files remain; schema creation/migration completes before health
/// reads. Active or uncertain ownership refuses before configuration changes.
pub fn init(home: &Path) -> Result<Value> {
    fs::private_dir(home)?;
    let mut authority = crate::offline::acquire(home)?;
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
    authority.guard.initialize()?;
    let store = authority.guard.store()?;
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
/// Returns the canonical read-only local diagnostic report as JSON.
/// Configuration/state failures are report findings; no store bootstrap, migration,
/// broker mutation or model turn is performed. Serialization/report errors stay typed.
pub async fn doctor(home: &Path) -> Result<Value> {
    Ok(serde_json::to_value(crate::doctor::run(home)?)?)
}
/// Escapes a scalar value for insertion into a launchd plist XML text node.
fn xml(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
/// Renders a launchd document and its machine-readable metadata for supported jobs.
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
        Adapter::Glm | Adapter::OpenCode => {
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
        HarnessId::OpenCode => Adapter::OpenCode,
    };
    Ok(LoginTarget {
        binary: harness.binary.clone(),
        runtime_home: harness.home.clone(),
        kind,
        label,
        reply: json!({"account": binding.account, "provider": name, "status": "ok"}),
    })
}

/// Recognizes the doctor command in argv before Clap reports invalid flags.
/// Only the leading command after global option values is considered, so other
/// commands whose task text contains "doctor" retain their exit policy.
pub fn doctor_invocation(arguments: &[std::ffi::OsString]) -> bool {
    let mut arguments = arguments.iter().skip(1);
    while let Some(argument) = arguments.next() {
        if ["--home", "--broker-socket", "--broker-uid"].contains(&argument.to_str().unwrap_or(""))
        {
            arguments.next();
            continue;
        }
        if argument.to_str().is_some_and(|value| {
            ["--home=", "--broker-socket=", "--broker-uid="]
                .iter()
                .any(|prefix| value.starts_with(prefix))
        }) {
            continue;
        }
        return argument == "doctor";
    }
    false
}

/// Identifies only the leading `hook context` command before Clap validation,
/// ignoring global options. Extend this recognizer alongside any future global
/// options. Task text, ordinary `context`, and `hook bind`
/// never gain advisory handling. Parse failures of recognized context hooks
/// remain validation failures in diagnostics, with a nonblocking process exit.
pub fn context_hook_invocation(arguments: &[std::ffi::OsString]) -> bool {
    let mut arguments = arguments.iter().skip(1);
    let mut commands = Vec::new();
    while let Some(argument) = arguments.next() {
        if ["--home", "--broker-socket", "--broker-uid"].contains(&argument.to_str().unwrap_or(""))
        {
            arguments.next();
        } else if argument.to_str().is_some_and(|value| {
            ["--home=", "--broker-socket=", "--broker-uid="]
                .iter()
                .any(|prefix| value.starts_with(prefix))
        }) {
            continue;
        } else {
            commands.push(argument);
            if commands.len() == 2 {
                return commands[0] == "hook" && commands[1] == "context";
            }
        }
    }
    false
}

/// Logs a context hook's final failure using closed metadata, including the
/// read-only schema observation when available. Missing/invalid homes and
/// unreadable metadata remain unknown; they never trigger initialization.
pub fn log_context_failure(home: Option<&Path>, error: &Error) {
    let version = home.and_then(|home| crate::migrate::stored_version(home).ok().flatten());
    hooks::context::log_failure(error, version);
}

/// Executes one parsed command and returns its public process exit status.
///
/// Success writes JSON except for the text guide, text transcript viewer and
/// stdio server. Expected failures
/// propagate as typed errors for `main` to render as the Python-compatible
/// JSON error envelope. `start` and `resume` exclusively call the resident
/// socket broker. An explicit operator socket bypasses all local state and
/// logging, and refuses commands that require local facilities. Long-lived API
/// and supervisor processes select their own component log before initialization.
pub async fn run(cli: Cli) -> Result<i32> {
    if cli.broker_uid.is_some() && cli.broker_socket.is_none() {
        return Err(invalid("--broker-uid requires --broker-socket"));
    }
    if let Some(socket) = cli.broker_socket.clone() {
        if socket.as_os_str().is_empty() {
            return Err(invalid("--broker-socket must not be empty"));
        }
        if !operator_client_command(&cli.command) {
            return Err(unsupported_operator_command());
        }
        let broker = Arc::new(OperatorBroker {
            socket,
            expected_uid: cli.broker_uid,
        });
        return run_client_command(
            cli.command,
            broker,
            Arc::new(emit),
            Arc::new(write_chunk),
            PathBuf::new(),
        )
        .await;
    }
    // A worker frontend is a thin socket client. It must never migrate/open
    // the store or inherit Desktop host capabilities through the normal MCP.
    if matches!(cli.command, Command::WorkerMcp) {
        transport::worker_mcp::serve_from_env().await?;
        return Ok(0);
    }
    let home = fs::home(cli.home.clone())?;
    if let Command::LaunchWorker { socket } = &cli.command {
        crate::system::serve_worker(&home, socket).await?;
        return Ok(0);
    }
    if matches!(
        &cli.command,
        Command::Doctor { .. } | Command::DoctorCanary { .. }
    ) {
        return run_with(cli, CliDependencies::production(home)).await;
    }
    // An older state database must be migrated together with its config;
    // no other command may open (and so auto-upgrade) it first.
    // The permission hook helper never opens the database.
    if !matches!(
        cli.command,
        Command::Config { .. } | Command::Doc { .. } | Command::PermissionRequest { .. }
    ) {
        crate::migrate::require_current_store(&home)?;
    }
    let component = match &cli.command {
        Command::Mcp => "mcp",
        Command::Api { .. } => "api",
        Command::Supervisor { .. } => "supervisor",
        Command::ServiceExec { .. } => "services",
        _ => "cli",
    };
    // A system supervisor selects its account-private logging home only
    // after authenticating and validating its frozen launch identity.
    if !matches!(&cli.command, Command::Supervisor { .. })
        || std::env::var_os("AGENT_RUN_SYSTEM_WRITER_SOCKET").is_none()
    {
        agent_run_core::logging::configure(&home, component);
    }
    run_with(cli, CliDependencies::production(home)).await
}

/// Commands whose complete CLI path already uses only the resident broker (or embedded docs).
fn client_command(command: &Command) -> bool {
    matches!(
        command,
        Command::Start(_)
            | Command::StartBatch { .. }
            | Command::BatchReceipt { .. }
            | Command::Status { .. }
            | Command::TranscriptExact { .. }
            | Command::InitialTask { .. }
            | Command::HistoryPins { .. }
            | Command::Restart(_)
            | Command::Resume(_)
            | Command::StartPool { .. }
            | Command::PoolPost { .. }
            | Command::PoolReplace { .. }
            | Command::Pool { .. }
            | Command::Pools { .. }
            | Command::Capabilities { .. }
            | Command::AnswerChunk { .. }
            | Command::Doc { .. }
            | Command::Mcp
    )
}

/// Operator-only adapters for commands whose local implementation reads private state.
/// The broker resolves a selected run once and returns its public projection.
fn operator_client_command(command: &Command) -> bool {
    client_command(command)
        || matches!(
            command,
            Command::Agents(_)
                | Command::Answer { .. }
                | Command::Transcript { .. }
                | Command::Cancel { .. }
                | Command::Steer { .. }
                | Command::Models { .. }
                | Command::Limits
                | Command::DelegationGuide { .. }
                | Command::Hook { .. }
                | Command::Capacity {
                    command: Capacity::Order { .. }
                }
        )
}

/// Refuses commands that still need a local service, store, or private writer.
fn unsupported_operator_command() -> Error {
    Error::Unsupported(
        "command is unavailable in operator mode; run it locally as the owning user".into(),
    )
}

/// Socket-only metadata for one verified and execution-pinned CLI answer.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorAnswer {
    /// Original public answer metadata, with inline bytes temporarily omitted.
    answer: Value,
    /// Exact retained execution receipt selected by the broker.
    sequence: std::num::NonZeroU32,
    /// Whether the original CLI answer included inline content.
    inline: bool,
    /// Opaque fingerprint of the verified execution and answer proof.
    proof_token: Option<agent_run_domain::Sha256Digest>,
}

/// Restores the original CLI answer after verifying every bounded page and the
/// full artifact digest. No local home or artifact path is opened by the client.
async fn operator_answer(
    broker: &dyn CliBroker,
    agent_id: AgentId,
    run_id: Option<AgentId>,
) -> Result<Value> {
    let invalid_answer =
        || Error::AnswerIntegrity("broker returned invalid answer evidence".into());
    let mut selected: OperatorAnswer = serde_json::from_value(
        broker
            .call("_cli_answer", json!({"agent_id":agent_id,"run_id":run_id}))
            .await?,
    )
    .map_err(|_| invalid_answer())?;
    let metadata: agent_run_domain::AnswerView =
        serde_json::from_value(selected.answer.clone()).map_err(|_| invalid_answer())?;
    if metadata.content.is_some() || metadata.inline_complete {
        return Err(invalid_answer());
    }
    if !metadata.available {
        if selected.inline
            || selected.proof_token.is_some()
            || metadata.size_bytes.is_some()
            || metadata.sha256.is_some()
        {
            return Err(invalid_answer());
        }
        return Ok(selected.answer);
    }
    let total = metadata.size_bytes.ok_or_else(invalid_answer)?;
    let digest: agent_run_domain::Sha256Digest = metadata
        .sha256
        .as_deref()
        .ok_or_else(invalid_answer)?
        .parse()
        .map_err(|_| invalid_answer())?;
    let version = metadata
        .proof_version
        .filter(|version| [1, 2].contains(version))
        .ok_or_else(invalid_answer)?;
    let token = selected.proof_token.as_ref().ok_or_else(invalid_answer)?;
    if metadata.path.is_none()
        || total > crate::verify::MAX_ANSWER as u64
        || selected.inline != (total <= crate::verify::INLINE_ANSWER as u64)
    {
        return Err(invalid_answer());
    }
    if !selected.inline {
        return Ok(selected.answer);
    }

    let mut content = String::with_capacity(usize::try_from(total).map_err(|_| invalid_answer())?);
    let mut offset = 0_u64;
    loop {
        let page: agent_run_domain::observation::AnswerChunkView = serde_json::from_value(
            broker
                .call(
                    "_cli_answer_chunk",
                    json!({
                        "query": {"agent_id":&metadata.agent_id,"sequence":selected.sequence,
                            "expected_sha256":&digest,"offset_bytes":offset,
                            "max_bytes":agent_run_domain::observation::MAX_CHUNK_BYTES},
                        "proof_token":token,
                    }),
                )
                .await?,
        )
        .map_err(|_| invalid_answer())?;
        let next = offset
            .checked_add(page.content.len() as u64)
            .ok_or_else(invalid_answer)?;
        if page.agent_id != metadata.agent_id
            || page.sequence != selected.sequence.get()
            || page.sha256 != digest.as_str()
            || page.total_bytes != total
            || page.offset_bytes != offset
            || page.next_offset != next
            || page.content.len() > agent_run_domain::observation::MAX_CHUNK_BYTES
            || next > total
            || page.complete != (next == total)
            || (!page.complete && next == offset)
        {
            return Err(invalid_answer());
        }
        content.push_str(&page.content);
        offset = next;
        if page.complete {
            break;
        }
    }
    if crate::fs::sha256(content.as_bytes()) != digest.as_str() {
        return Err(Error::AnswerHashMismatch);
    }
    if version == 1 {
        if !content.as_bytes().ends_with(crate::verify::LEGACY_FRAME) {
            return Err(invalid_answer());
        }
        content.truncate(content.len() - crate::verify::LEGACY_FRAME.len());
    }
    selected.answer["content"] = Value::String(content);
    selected.answer["inline_complete"] = Value::Bool(true);
    Ok(selected.answer)
}

/// The exact execution selected once for an operator transcript viewer.
#[derive(Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct OperatorTranscriptSelection {
    /// Stable lineage identity used in public results.
    agent_id: AgentId,
    /// Exact execution retained for the entire viewer.
    run_id: AgentId,
    /// Immutable receipt checked by the broker on every read.
    sequence: std::num::NonZeroU32,
}

/// One fragment of a digest-fenced serialized transcript page.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OperatorTranscriptPiece {
    selection: OperatorTranscriptSelection,
    sha256: agent_run_domain::Sha256Digest,
    total_bytes: u64,
    offset_bytes: u64,
    next_offset: u64,
    complete: bool,
    content: String,
}

/// Selects one exact run once and checks an explicitly requested historical run.
async fn operator_transcript_selection(
    broker: &dyn CliBroker,
    agent_id: &AgentId,
    run_id: Option<&AgentId>,
) -> Result<OperatorTranscriptSelection> {
    let selection: OperatorTranscriptSelection = serde_json::from_value(
        broker
            .call(
                "_cli_transcript_select",
                json!({"agent_id":agent_id,"run_id":run_id}),
            )
            .await?,
    )
    .map_err(|_| invalid("broker returned invalid transcript selection"))?;
    if run_id.is_some_and(|run| *run != selection.run_id) {
        return Err(Error::ExecutionChanged);
    }
    Ok(selection)
}

/// Reassembles one exact broker page only while its digest and execution remain stable.
async fn operator_transcript_page(
    broker: &dyn CliBroker,
    selection: &OperatorTranscriptSelection,
    query: &agent_run_domain::transcript::TranscriptQuery,
    lineage: bool,
    recovery: bool,
) -> Result<Value> {
    // A busy journal may change the page mid-transfer; retry only this read,
    // never a mutation, and stop after a bounded number of fresh snapshots.
    const PAGE_RETRIES: usize = 3;
    let invalid_page = || invalid("broker returned invalid transcript page");
    let mut offset = 0_u64;
    let mut digest: Option<String> = None;
    let mut total = None;
    let mut encoded = String::new();
    let mut retries = 0;
    loop {
        let result = broker
            .call(
                "_cli_transcript_page",
                json!({"selection":selection,"query":query,"lineage":lineage,"recovery":recovery,
                "offset_bytes":offset,"expected_sha256":digest}),
            )
            .await;
        let value = match result {
            Ok(value) => value,
            Err(error) => {
                let changed = matches!(&error, Error::Broker { broker_error_code: Some(code), .. }
                    if code == agent_run_core::dispatch::CLI_TRANSCRIPT_PAGE_CHANGED_CODE);
                if !changed || retries == PAGE_RETRIES {
                    return Err(error);
                }
                retries += 1;
                offset = 0;
                digest = None;
                total = None;
                encoded.clear();
                continue;
            }
        };
        let piece: OperatorTranscriptPiece =
            serde_json::from_value(value).map_err(|_| invalid_page())?;
        if piece.total_bytes > agent_run_core::dispatch::CLI_TRANSCRIPT_MAX_PAGE_BYTES as u64 {
            return Err(Error::Runtime(
                "transcript page exceeds 64 MiB limit".into(),
            ));
        }
        let next = offset
            .checked_add(piece.content.len() as u64)
            .ok_or_else(invalid_page)?;
        if piece.selection != *selection
            || digest
                .as_ref()
                .is_some_and(|hash| hash != piece.sha256.as_str())
            || total.is_some_and(|size| size != piece.total_bytes)
            || piece.offset_bytes != offset
            || piece.next_offset != next
            || piece.content.len() > agent_run_domain::observation::MAX_CHUNK_BYTES
            || next > piece.total_bytes
            || piece.complete != (next == piece.total_bytes)
            || (!piece.complete && next == offset)
        {
            return Err(invalid_page());
        }
        total = Some(piece.total_bytes);
        digest = Some(piece.sha256.as_str().to_owned());
        encoded.push_str(&piece.content);
        offset = next;
        if piece.complete {
            break;
        }
    }
    if digest.as_deref() != Some(crate::fs::sha256(encoded.as_bytes()).as_str()) {
        return Err(invalid_page());
    }
    let page: Value = serde_json::from_str(&encoded).map_err(|_| invalid_page())?;
    if !page.is_object() || page["agent_id"] != json!(selection.agent_id) {
        return Err(invalid_page());
    }
    Ok(page)
}

/// Local or socket-backed access to the existing transcript viewer contract.
enum TranscriptSource<'a> {
    Local {
        service: &'a dyn CliService,
        id: AgentId,
        lineage: bool,
        recovery: bool,
    },
    Operator {
        broker: &'a dyn CliBroker,
        selection: OperatorTranscriptSelection,
        lineage: bool,
        recovery: bool,
    },
}

impl TranscriptSource<'_> {
    /// Returns the exact run fixed at viewer startup.
    fn id(&self) -> &AgentId {
        match self {
            Self::Local { id, .. } => id,
            Self::Operator { selection, .. } => &selection.run_id,
        }
    }

    /// Reads one page using the original local or broker-backed selection.
    async fn page(&self, query: agent_run_domain::transcript::TranscriptQuery) -> Result<Value> {
        match self {
            Self::Local {
                service,
                id,
                lineage,
                recovery,
            } => {
                if *recovery {
                    service.transcript_recovery(id, query.cursor, query.limit)
                } else {
                    service.transcript_options(id, &query, *lineage)
                }
            }
            Self::Operator {
                broker,
                selection,
                lineage,
                recovery,
            } => operator_transcript_page(*broker, selection, &query, *lineage, *recovery).await,
        }
    }

    /// Returns the selected execution's status without following a later resume.
    async fn status(&self) -> Result<Value> {
        match self {
            Self::Local { service, id, .. } => Ok(service.agent(id)?["status"].clone()),
            Self::Operator {
                broker, selection, ..
            } => {
                let result = broker
                    .call("_cli_transcript_status", serde_json::to_value(selection)?)
                    .await?;
                let actual: OperatorTranscriptSelection =
                    serde_json::from_value(result["selection"].clone())
                        .map_err(|_| invalid("broker returned invalid transcript selection"))?;
                if actual != *selection {
                    return Err(Error::ExecutionChanged);
                }
                Ok(result["status"].clone())
            }
        }
    }

    /// Projects one page or completed full view to the original stable public id.
    fn project(&self, mut value: Value) -> Result<Value> {
        match self {
            Self::Local { service, id, .. } => service.public_run_result(id, value),
            Self::Operator { selection, .. } => {
                let object = value
                    .as_object_mut()
                    .ok_or_else(|| invalid("agent result must be an object"))?;
                object.insert("agent_id".into(), json!(selection.agent_id));
                for key in [
                    "run_id",
                    "parent_run_id",
                    "parent_agent_id",
                    "root_agent_id",
                    "attempt_id",
                ] {
                    object.remove(key);
                }
                Ok(value)
            }
        }
    }
}

/// Display options retained for one local or remote transcript invocation.
struct TranscriptViewer {
    cursor: i64,
    limit: usize,
    view: agent_run_domain::transcript::TranscriptView,
    tail_blocks: Option<usize>,
    before_cursor: Option<i64>,
    follow: bool,
    full: bool,
    native_history: Option<PathBuf>,
    format: Option<TranscriptFormat>,
}

/// Renders the shared transcript view, retaining local pagination and Ctrl-C behavior.
async fn view_transcript(
    source: TranscriptSource<'_>,
    options: TranscriptViewer,
    output: &CliOutput,
    text_output: &CliTextOutput,
) -> Result<()> {
    let TranscriptViewer {
        mut cursor,
        limit,
        view,
        tail_blocks,
        before_cursor,
        follow,
        full,
        native_history,
        format,
    } = options;
    let query = agent_run_domain::transcript::TranscriptQuery {
        cursor,
        limit,
        view,
        tail_blocks,
        before_cursor,
    };
    query.validate()?;
    if native_history.is_some()
        && (!full
            || follow
            || cursor != 0
            || view != agent_run_domain::transcript::TranscriptView::Raw)
    {
        return Err(invalid(
            "native history reconstruction requires --full, raw view and cursor zero",
        ));
    }
    if view == agent_run_domain::transcript::TranscriptView::Blocks && (follow || full) {
        return Err(invalid(
            "blocks are bounded pages; --follow and --full require raw view",
        ));
    }
    let page_at = |cursor| {
        let mut page = query.clone();
        page.cursor = cursor;
        source.page(page)
    };
    let text = TranscriptFormat::effective(format) == TranscriptFormat::Text;
    let mut renderer = crate::transcript::Renderer::default();
    if full {
        let page = page_at(cursor).await?;
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
            current = page_at(page_cursor).await?;
            messages.extend(current["messages"].as_array().cloned().unwrap_or_default());
            pages += 1;
        }
        let reconstruction = if let Some(path) = native_history.as_ref() {
            let parent = path
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let name = path
                .file_name()
                .ok_or_else(|| invalid("native history needs a file name"))?;
            let bytes = agent_run_platform::fs::Dir::open(parent)?
                .read(Path::new(name), 64 * 1024 * 1024)?;
            let repaired = crate::transcript::restore_claude_history(&mut messages, &bytes)?;
            Some(
                json!({"source_sha256":agent_run_platform::fs::sha256(&bytes),"reconstructed_messages":repaired,"raw_journal_unchanged":true}),
            )
        } else {
            None
        };
        if text {
            renderer.page(&messages, &mut |line| (text_output)(line))?;
            renderer.finish(&mut |line| (text_output)(line))?;
        } else {
            let mut value = json!({"agent_id":source.id(),"messages":messages,"cursor":cursor,"next_cursor":null,"complete":true,"pages":pages});
            if let Some(proof) = reconstruction {
                value["reconstruction"] = proof;
            }
            (output)(&source.project(value)?)?;
        }
        return Ok(());
    }
    // Install the listener before the first page; dropping a remote read closes
    // that observation only and cannot cancel a supervised agent.
    let mut interrupt = follow
        .then(|| tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt()))
        .transpose()?;
    // A terminal status may be committed after the page was read; drain one
    // fresh snapshot so a final journal fragment cannot be skipped.
    let mut draining_terminal = false;
    loop {
        let page = if let Some(interrupt) = interrupt.as_mut() {
            tokio::select! { _ = interrupt.recv() => break, result = page_at(cursor) => result? }
        } else {
            page_at(cursor).await?
        };
        if draining_terminal
            && page["complete"] == true
            && page["messages"].as_array().is_some_and(Vec::is_empty)
        {
            break;
        }
        if text {
            renderer.page(
                page["messages"]
                    .as_array()
                    .map(Vec::as_slice)
                    .unwrap_or(&[]),
                &mut |line| (text_output)(line),
            )?;
        } else {
            (output)(&source.project(page.clone())?)?;
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
        if draining_terminal {
            if page["complete"] == true {
                break;
            }
            continue;
        }
        if page["complete"] == true {
            let status = if let Some(interrupt) = interrupt.as_mut() {
                tokio::select! { _ = interrupt.recv() => break, result = source.status() => result? }
            } else {
                source.status().await?
            };
            if status.as_str().is_some_and(|status| {
                matches!(
                    status,
                    "succeeded" | "failed" | "lost" | "timed_out" | "cancelled"
                )
            }) {
                draining_terminal = true;
                continue;
            }
        }
        if let Some(interrupt) = interrupt.as_mut() {
            tokio::select! { _=interrupt.recv()=>break, _=tokio::time::sleep(Duration::from_millis(250))=>{} }
        }
    }
    renderer.finish(&mut |line| (text_output)(line))?;
    Ok(())
}

/// Keep only the host session and blocked-restart receipt when forwarding a hook.
fn restart_hook_payload(payload: &Value, transport: &str) -> Result<Option<Value>> {
    Ok(
        hooks::bind::restart_waiter(payload, transport)?.map(|(restart_id, reference)| {
            json!({"hook_event_name":"PostToolUse","session_id":reference.external_session_id,
            "tool_response":{"accepted":false,"agent_run_restart_id":restart_id}})
        }),
    )
}

/// Executes existing broker-only CLI operations with their original payloads and output sinks.
/// The MCP home is metadata only: operator mode passes an empty path and never resolves a home.
async fn run_client_command(
    command: Command,
    broker: Arc<dyn CliBroker>,
    output: CliOutput,
    text_output: CliTextOutput,
    home: PathBuf,
) -> Result<i32> {
    match command {
        Command::Restart(args) => return crate::restart::run(broker.as_ref(), &output, args).await,
        Command::Hook { command } => {
            let (bind, transport) = match command {
                Hook::Context(transport) => (false, transport.transport),
                Hook::Bind(transport) => (true, transport.transport),
            };
            let result: Result<Value> = async {
                let input = read_stdin(1024 * 1024)?;
                let payload: Value = serde_json::from_str(&input)
                    .map_err(|_| invalid("hook payload must be a JSON object"))?;
                if bind && let Some(safe) = restart_hook_payload(&payload, &transport)? {
                    return broker
                        .call("_hook_bind", json!({"payload":safe,"transport":transport}))
                        .await;
                }
                let normalized = hooks::bind::normalize(&payload, bind, &transport)?;
                let mut safe = json!({
                    "transport": normalized.reference.transport,
                    "external_session_id": normalized.reference.external_session_id,
                });
                if let Some(turn) = normalized.reference.external_turn_id {
                    safe["external_turn_id"] = json!(turn);
                }
                if let Some(pool) = normalized.pool_id {
                    safe["pool_id"] = json!(pool);
                }
                if let Some(agent) = normalized.agent_id {
                    safe["agent_id"] = json!(agent);
                }
                if let Some(sequence) = normalized.sequence {
                    safe["sequence"] = json!(sequence);
                }
                broker
                    .call(
                        if bind { "_hook_bind" } else { "_hook_context" },
                        json!({"payload":safe,"transport":transport}),
                    )
                    .await
            }
            .await;
            match result {
                Ok(value) => (output)(&value)?,
                Err(error) if !bind => {
                    hooks::context::log_failure(&error, None);
                    (output)(&json!({}))?;
                }
                Err(error) => return Err(error),
            }
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
                    "explicit_finish": a.explicit_finish && !a.legacy_completion,
                    "idle_ping": a.idle_ping,
                    "effort": a.effort,
                    "read_roots": read_roots,
                    "output_schema": schema,
                    "orchestrator": a.session.resolve()?,
                    "request_id": a.request_id,
                    "account": a.account,
                    "display_name": a.display_name,
                }))
                .map_err(|_| invalid("invalid provider start arguments"))?;
            request.validate()?;
            let result = broker.call("start", serde_json::to_value(request)?).await?;
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
                let result = broker.call("wait", wait).await?;
                (output)(&result)?;
                return Ok(result_code(&result));
            }
            (output)(&admission_output(&result)?)?;
        }
        Command::Resume(a) => {
            let task = match (a.task, a.task_file) {
                (Some(task), None) => task_text(&task, 1024 * 1024)?,
                (None, Some(path)) if path == Path::new("-") => read_stdin(1024 * 1024)?,
                (None, Some(path)) => read_input(&path, 1024 * 1024)?,
                _ => return Err(invalid("provide exactly one resume task source")),
            };
            let mut arguments = json!({"agent_id":a.agent_id,"task":task,"request_id":a.request_id,"orchestrator":a.session.resolve()?});
            if let Some(run_id) = a.run_id {
                arguments["run_id"] = json!(run_id);
            }
            if let Some(display_name) = a.display_name {
                arguments["display_name"] = json!(display_name);
            }
            let result = broker.call("resume", arguments).await?;
            (output)(&admission_output(&result)?)?;
        }
        Command::StartBatch { spec } => {
            let text = if spec == "-" {
                read_stdin(1024 * 1024)?
            } else {
                read_input(Path::new(&spec), 1024 * 1024)?
            };
            let mut request: agent_run_domain::batch::StartBatchRequest =
                serde_json::from_str(&text).map_err(|_| invalid("invalid batch spec"))?;
            request.validate_intent()?;
            let params = serde_json::to_value(request)?;
            let result = broker.call("start_batch", params.clone()).await?;
            if !transport::socket::valid_batch_response(&result, &params) {
                return Err(Error::BatchOutcomeUnknown {
                    source: Box::new(invalid("invalid batch receipt")),
                });
            }
            (output)(&result)?;
        }
        Command::BatchReceipt { spec } => {
            let text = if spec == "-" {
                read_stdin(1024 * 1024)?
            } else {
                read_input(Path::new(&spec), 1024 * 1024)?
            };
            let mut request: agent_run_domain::batch::StartBatchRequest =
                serde_json::from_str(&text).map_err(|_| invalid("invalid batch receipt spec"))?;
            request.validate_receipt_lookup()?;
            let params = serde_json::to_value(request)?;
            let result = broker.call("batch_receipt", params.clone()).await?;
            if !transport::socket::valid_batch_receipt_response(&result, &params) {
                return Err(Error::Runtime(
                    "invalid batch receipt lookup response".into(),
                ));
            }
            (output)(&result)?;
        }
        Command::Status { spec } => {
            let text = if spec == "-" {
                read_stdin(1024 * 1024)?
            } else {
                read_input(Path::new(&spec), 1024 * 1024)?
            };
            let request: agent_run_domain::status::StatusRequest =
                serde_json::from_str(&text).map_err(|_| invalid("invalid status spec"))?;
            request.validate()?;
            let params = serde_json::to_value(request)?;
            let result = broker.call("status", params.clone()).await?;
            if !transport::socket::valid_status_response(&result, &params) {
                return Err(Error::Runtime("invalid status response".into()));
            }
            (output)(&result)?;
        }
        Command::TranscriptExact { spec } => {
            let text = if spec == "-" {
                read_stdin(1024 * 1024)?
            } else {
                read_input(Path::new(&spec), 1024 * 1024)?
            };
            let request: agent_run_domain::transcript_exact::TranscriptExactRequest =
                serde_json::from_str(&text)
                    .map_err(|_| invalid("invalid exact transcript spec"))?;
            request.validate()?;
            let params = serde_json::to_value(request)?;
            let result = broker.call("transcript_exact", params.clone()).await?;
            if !transport::socket::valid_transcript_exact_response(&result, &params) {
                return Err(Error::Runtime("invalid exact transcript response".into()));
            }
            (output)(&result)?;
        }
        Command::InitialTask { spec } => {
            let text = if spec == "-" {
                read_stdin(1024 * 1024)?
            } else {
                read_input(Path::new(&spec), 1024 * 1024)?
            };
            let request: agent_run_domain::initial_task::InitialTaskRequest =
                serde_json::from_str(&text).map_err(|_| invalid("invalid initial task spec"))?;
            request.validate()?;
            let params = serde_json::to_value(request)?;
            let result = broker.call("initial_task", params.clone()).await?;
            if !transport::socket::valid_initial_task_response(&result, &params) {
                return Err(Error::Runtime("invalid initial task response".into()));
            }
            (output)(&result)?;
        }
        Command::HistoryPins { command } => {
            let (mut params, expected_store_id) = match command {
                HistoryPinsCommand::List {
                    status,
                    limit,
                    after_pin_id,
                    expected_store_id,
                } => (
                    json!({
                        "action":"list", "status":status, "limit":limit, "after_pin_id":after_pin_id
                    }),
                    expected_store_id,
                ),
                HistoryPinsCommand::Release {
                    pin_id,
                    expected_store_id,
                } => (
                    json!({"action":"release", "pin_id":pin_id}),
                    expected_store_id,
                ),
            };
            if let Some(id) = expected_store_id {
                params["expected_store_id"] = json!(id);
            }
            let request: agent_run_domain::batch::HistoryPinsRequest =
                serde_json::from_value(params.clone())
                    .map_err(|_| invalid("invalid history pin request"))?;
            if matches!(request, agent_run_domain::batch::HistoryPinsRequest::List { limit, .. } if !(1..=1000).contains(&limit))
            {
                return Err(invalid("history pin limit must be 1..=1000"));
            }
            let result = broker.call("history_pins", params.clone()).await?;
            if !transport::socket::valid_history_pin_response(&result, &params) {
                return Err(Error::Runtime(
                    "history pin result is invalid; reconcile release by pin_id".into(),
                ));
            }
            (output)(&result)?;
        }
        Command::StartPool { spec, session } => {
            let mut request = spec_json(&spec)?;
            if let Some(reference) = session.resolve()? {
                request["orchestrator"] = serde_json::to_value(reference)?;
            }
            let result = broker.call("start_pool", request).await?;
            (output)(&result)?;
        }
        Command::PoolPost {
            pool_id,
            request_id,
            text,
        } => {
            let message = task_text(&text, agent_run_domain::pool::MAX_BODY_BYTES)?;
            let result = broker
                .call(
                    "pool_post",
                    json!({"pool_id": pool_id, "request_id": request_id, "message": message}),
                )
                .await?;
            (output)(&result)?;
        }
        Command::PoolReplace {
            pool_id,
            agent_id,
            request_id,
            spec,
        } => {
            let start = spec.as_deref().map(spec_json).transpose()?;
            let result = broker
                .call(
                    "pool_replace",
                    json!({"pool_id": pool_id, "agent_id": agent_id, "request_id": request_id, "start": start}),
                )
                .await?;
            (output)(&result)?;
        }
        Command::Pool {
            pool_id,
            after_seq,
            before_seq,
            limit,
        } => {
            let result = broker
                .call(
                    "pool",
                    json!({"pool_id": pool_id, "after_seq": after_seq, "before_seq": before_seq, "limit": limit}),
                )
                .await?;
            (output)(&result)?;
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
            let result = broker
                .call("list_pools", serde_json::to_value(query)?)
                .await?;
            if text {
                (text_output)(&format!(
                    "{}\n",
                    crate::transport::mcp_text::list_pools_text(&result)?
                ))?;
            } else {
                (output)(&result)?;
            }
        }
        Command::Capabilities {
            include_store_id,
            include_initial_task,
        } => {
            let mut params = json!({});
            if include_store_id {
                params["include_store_id"] = json!(true);
            }
            if include_initial_task {
                params["include_initial_task"] = json!(true);
            }
            let result = broker.call("capabilities", params).await?;
            if include_store_id
                && !result
                    .get("store_id")
                    .and_then(Value::as_str)
                    .is_some_and(|id| id.parse::<agent_run_domain::StoreId>().is_ok())
            {
                return Err(Error::Runtime("invalid capabilities store identity".into()));
            }
            if include_initial_task
                && serde_json::from_value::<agent_run_domain::initial_task::InitialTaskCapability>(
                    result.get("initial_task").cloned().unwrap_or(Value::Null),
                )
                .is_err()
            {
                return Err(Error::Runtime(
                    "invalid initial task capability response".into(),
                ));
            }
            (output)(&result)?;
        }
        Command::AnswerChunk {
            agent_id,
            sequence,
            expected_sha256,
            offset_bytes,
            max_bytes,
        } => {
            let query = agent_run_domain::observation::AnswerChunkQuery {
                agent_id,
                sequence,
                expected_sha256,
                offset_bytes,
                max_bytes,
            };
            query.validate()?;
            (output)(
                &broker
                    .call("answer_chunk", serde_json::to_value(query)?)
                    .await?,
            )?;
        }
        Command::Agents(agents) => {
            let broker = broker.as_ref();
            if agents.follow {
                follow_agents(&agents, |query| broker_agent_list(broker, query), &output).await?;
            } else {
                (output)(&broker_agent_list(broker, agents_query(&agents, None, None)?).await?)?;
            }
        }
        Command::Answer { agent_id, run_id } => {
            (output)(&operator_answer(broker.as_ref(), agent_id, run_id).await?)?;
        }
        Command::Transcript {
            agent_id,
            run_id,
            cursor,
            limit,
            view,
            tail_blocks,
            before_cursor,
            follow,
            full,
            native_history,
            format,
        } => {
            let selection =
                operator_transcript_selection(broker.as_ref(), &agent_id, run_id.as_ref()).await?;
            let source = TranscriptSource::Operator {
                broker: broker.as_ref(),
                selection,
                lineage: run_id.is_none() && native_history.is_none(),
                recovery: native_history.is_some(),
            };
            view_transcript(
                source,
                TranscriptViewer {
                    cursor,
                    limit,
                    view,
                    tail_blocks,
                    before_cursor,
                    follow,
                    full,
                    native_history,
                    format,
                },
                &output,
                &text_output,
            )
            .await?;
        }
        Command::Cancel { agent_id, run_id } => {
            (output)(
                &broker
                    .call("cancel", json!({"agent_id":agent_id,"run_id":run_id}))
                    .await?,
            )?;
        }
        Command::Steer {
            agent_id,
            run_id,
            text,
        } => {
            (output)(
                &broker
                    .call(
                        "steer",
                        json!({"agent_id":agent_id,"run_id":run_id,"text":text}),
                    )
                    .await?,
            )?;
        }
        Command::Models {
            provider,
            profile,
            model,
        } => {
            (output)(
                &broker
                    .call(
                        "models",
                        json!({"provider":provider,"profile":profile,"model":model}),
                    )
                    .await?,
            )?;
        }
        Command::Limits => (output)(&broker.call("limits", json!({})).await?)?,
        Command::DelegationGuide {
            provider,
            profile,
            model,
        } => {
            let value = broker
                .call(
                    "delegation_guide",
                    json!({"provider":provider,"profile":profile,"model":model}),
                )
                .await?;
            let text = value
                .as_str()
                .ok_or_else(|| invalid("delegation guide must be text"))?;
            (text_output)(&format!("{text}\n"))?;
        }
        Command::Capacity {
            command: Capacity::Order { model },
        } => {
            (output)(
                &broker
                    .call("capacity_order", json!({"model":model}))
                    .await?,
            )?;
        }
        Command::Doc { topic } => {
            let topic = topic.as_deref().unwrap_or("index");
            (output)(&json!({"topic":topic,"text":crate::dispatch::doc(topic)?}))?;
        }
        Command::Mcp => transport::mcp::serve_with(home, None, broker.clone()).await?,
        _ => return Err(unsupported_operator_command()),
    }
    Ok(0)
}

/// Executes one parsed command against supplied service, broker, and output seams.
/// Explicit operator mode never resolves a local home and refuses local commands.
/// Production callers use [`run`]; injected dependencies remain borrowed only
/// for the duration of the execution.
pub async fn run_with(cli: Cli, dependencies: CliDependencies) -> Result<i32> {
    if cli.broker_uid.is_some() && cli.broker_socket.is_none() {
        return Err(invalid("--broker-uid requires --broker-socket"));
    }
    let operator_mode = cli.broker_socket.is_some();
    if cli
        .broker_socket
        .as_ref()
        .is_some_and(|socket| socket.as_os_str().is_empty())
    {
        return Err(invalid("--broker-socket must not be empty"));
    }
    if operator_mode && !operator_client_command(&cli.command) {
        return Err(unsupported_operator_command());
    }
    let home = if operator_mode {
        PathBuf::new()
    } else {
        fs::home(cli.home)?
    };
    if operator_mode || client_command(&cli.command) {
        return run_client_command(
            cli.command,
            dependencies.broker,
            dependencies.output,
            dependencies.text_output,
            home,
        )
        .await;
    }
    match cli.command {
        Command::Init => (dependencies.output)(&crate::init::initialize(&home)?)?,
        Command::Doctor { .. } => {
            let report = (dependencies.doctor)(&home)?;
            let ok = report.ok();
            (dependencies.output)(&serde_json::to_value(report)?)?;
            return Ok(if ok { 0 } else { 2 });
        }
        Command::Cancel { agent_id, run_id } => {
            let id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let value = dependencies
                .broker
                .call("cancel", json!({"agent_id":agent_id,"run_id":id}))
                .await?;
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
            let value = dependencies
                .broker
                .call(
                    "steer",
                    json!({"agent_id":agent_id,"run_id":id,"text":text}),
                )
                .await?;
            (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?
        }
        Command::Bind(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            let store = Store::open(&home)?;
            if let Some(pool) = &a.pool {
                let pool: agent_run_domain::pool::PoolId = pool.parse()?;
                operator(&home)
                    .call(
                        agent_run_store::ops::BindPool {
                            pool_id: pool.clone(),
                            reference: reference.clone(),
                            at: crate::domain::now(),
                        },
                        agent_run_core::broker_writer::BROKER_BUDGET,
                    )
                    .await?;
                (dependencies.output)(&json!({"pool_id": pool, "bound": true}))?;
                return Ok(0);
            }
            let agent_id = a
                .agent_id
                .ok_or_else(|| invalid("agent id or --pool is required"))?;
            let run =
                agent_run_core::agent_identity::resolve(&store, &agent_id, a.run_id.as_ref())?;
            hooks::bind::bind(&home, run.id.clone(), reference, crate::domain::now()).await?;
            (dependencies.output)(&agent_run_core::agent_identity::result(
                &run,
                store.delivery_status(&run.id)?,
            )?)?;
        }
        Command::Context(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            (dependencies.output)(&serde_json::to_value(
                hooks::context::build(&home, &reference, None).await?,
            )?)?;
        }
        Command::Hook { command } => {
            let input = read_stdin(1024 * 1024)?;
            let payload: Value = serde_json::from_str(&input)
                .map_err(|_| invalid("hook payload must be a JSON object"))?;
            match command {
                Hook::Context(transport) => {
                    let reference =
                        hooks::bind::normalize(&payload, false, &transport.transport)?.reference;
                    let context = hooks::context::build_for_hook(&home, &reference, None).await?;
                    let output = if context.injected && !context.text.trim().is_empty() {
                        json!({"hookSpecificOutput":{"hookEventName":"UserPromptSubmit","additionalContext":context.text}})
                    } else {
                        json!({})
                    };
                    (dependencies.output)(&output)?;
                }
                Hook::Bind(transport) => {
                    if let Some(safe) = restart_hook_payload(&payload, &transport.transport)? {
                        let output = dependencies
                            .broker
                            .call(
                                "_hook_bind",
                                json!({"payload":safe,"transport":transport.transport}),
                            )
                            .await?;
                        (dependencies.output)(&output)?;
                        return Ok(0);
                    }
                    let result =
                        hooks::bind::run_hook_bound(&home, &payload, &transport.transport, None)
                            .await?;
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
                        .list(agents_query(&a, None, None)?)
                        .await?,
                )?;
            } else {
                let service = dependencies.service.as_ref();
                follow_agents(&a, |query| service.list(query), &dependencies.output).await?;
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
            cursor,
            limit,
            view,
            tail_blocks,
            before_cursor,
            follow,
            full,
            native_history,
            format,
        } => {
            let id = dependencies
                .service
                .resolve_run_id(&agent_id, run_id.as_ref())?;
            let source = TranscriptSource::Local {
                service: dependencies.service.as_ref(),
                id,
                lineage: run_id.is_none() && native_history.is_none(),
                recovery: native_history.is_some(),
            };
            view_transcript(
                source,
                TranscriptViewer {
                    cursor,
                    limit,
                    view,
                    tail_blocks,
                    before_cursor,
                    follow,
                    full,
                    native_history,
                    format,
                },
                &dependencies.output,
                &dependencies.text_output,
            )
            .await?;
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
        Command::Limits => (dependencies.output)(&dependencies.service.limits().await?)?,
        Command::DelegationGuide {
            provider,
            profile,
            model,
        } => {
            // The guide is text, not JSON: print it with a normal newline.
            let value = dependencies
                .service
                .delegation_guide(agent_run_domain::ModelsQuery {
                    provider,
                    profile,
                    model,
                })
                .await?;
            let text = value
                .as_str()
                .ok_or_else(|| invalid("delegation guide must be text"))?;
            (dependencies.text_output)(&format!("{text}\n"))?;
        }
        Command::WorkerMcp => transport::worker_mcp::serve_from_env().await?,
        Command::LaunchWorker { socket } => crate::system::serve_worker(&home, &socket).await?,
        Command::Api { command } => match command {
            Api::Serve {
                socket,
                operator_socket,
                operator_gid,
                shared_config,
            } => match (socket, operator_socket, operator_gid, shared_config) {
                (None, None, None, Some(path)) => {
                    let config = crate::system::Config::load(&path)?;
                    let operator = config.operator_socket.clone();
                    let settings = transport::socket::SystemServeOptions {
                        writer_socket: config.writer_socket.clone(),
                        operator_sockets: config.operator_sockets.clone(),
                        launcher: Arc::new(crate::system::BrokerLauncher::new(
                            home.clone(),
                            config,
                        )?),
                    };
                    transport::socket::serve_system(
                        &home,
                        &operator,
                        settings,
                        transport::socket::ServeOptions::default(),
                    )
                    .await?
                }
                (None, Some(path), Some(gid), None) => {
                    transport::socket::serve_with_operator(
                        &home,
                        &path,
                        gid,
                        transport::socket::ServeOptions::default(),
                    )
                    .await?
                }
                (Some(socket), None, None, None) => {
                    transport::socket::serve_at(&home, &socket).await?
                }
                (None, None, None, None) => transport::socket::serve(&home).await?,
                _ => {
                    return Err(invalid(
                        "operator socket and group must be paired and cannot be combined with --socket",
                    ));
                }
            },
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
                    .capacity_order(agent_run_domain::CapacityOrderQuery { model })
                    .await?,
            )?,
        },
        Command::Delivery { command } => match command {
            Delivery::Status { agent_id, run_id } => {
                let id = dependencies
                    .service
                    .resolve_run_id(&agent_id, run_id.as_ref())?;
                let value = dependencies.service.delivery_status(&id)?;
                (dependencies.output)(&dependencies.service.public_run_result(&id, value)?)?
            }
            Delivery::Cancel { delivery_id } => (dependencies.output)(
                &json!({"delivery_id":delivery_id,"cancelled":operator(&home).call(agent_run_store::ops::CancelDelivery { delivery_id: delivery_id.clone() }, agent_run_core::broker_writer::BROKER_BUDGET).await?}),
            )?,
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
                match crate::offline::route(&home).await? {
                    crate::offline::Route::Online(producer) => {
                        producer
                            .call(
                                agent_run_store::ops::RegisterAccount {
                                    record: record.clone(),
                                },
                                Duration::from_secs(30),
                            )
                            .await?;
                    }
                    crate::offline::Route::Offline(mut session) => {
                        session.guard.store()?.register_account(&record)?;
                    }
                }
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
                match crate::offline::route(&home).await? {
                    crate::offline::Route::Online(producer) => {
                        producer
                            .call(
                                agent_run_store::ops::DisableAccount {
                                    account: id.clone(),
                                },
                                Duration::from_secs(30),
                            )
                            .await?;
                    }
                    crate::offline::Route::Offline(mut session) => {
                        session.guard.store()?.disable_account(&id)?;
                    }
                }
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
            capability_fd,
        } => {
            crate::supervisor::run(
                &home,
                &agent_id,
                [ready_fd, identity_fd, error_fd],
                capability_fd,
            )
            .await?
        }
        Command::ServiceExec { generation } => {
            agent_run_core::managed_services::bootstrap(&home, &generation).await?
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
        _ => return Err(unsupported_operator_command()),
    }
    Ok(0)
}

#[cfg(test)]
mod login_selection_tests {
    //! Credential selection must not depend on the order of provider bindings.

    use super::provider_login_target;
    use agent_run_config::provider_config::ProviderConfig;
    use agent_run_domain::catalog::{AccountRecord, AccountStatus};

    /// Forward only the host identity and restart receipt from a blocked start.
    #[test]
    fn restart_hook_drops_unrelated_tool_data() {
        let payload = serde_json::json!({"hook_event_name":"PostToolUse","session_id":"chat",
            "tool_input":{"task":"private task"},
            "tool_response":{"accepted":false,"agent_run_restart_id":7}});
        let safe = super::restart_hook_payload(&payload, "codex_queue")
            .unwrap()
            .unwrap();
        assert_eq!(safe["session_id"], "chat");
        assert_eq!(safe["tool_response"]["agent_run_restart_id"], 7);
        assert!(!safe.to_string().contains("private task"));
    }

    /// A provider label that is another binding's global ID is ambiguous;
    /// unambiguous labels and IDs still select their registered account.
    #[test]
    fn login_rejects_cross_namespace_account_collision() {
        let home = tempfile::tempdir().unwrap();
        agent_run_store::fixture::initialize(home.path()).unwrap();
        let config = format!(
            "schema_version = 2\n[harnesses.codex]\nbinary = \"/bin/true\"\nhome = \"{0}/codex\"\n[harnesses.claude-code]\nbinary = \"/bin/true\"\nhome = \"{0}/claude\"\n[providers.codex]\nharness = \"codex\"\nconnection = {{ kind = \"native\" }}\nauth_family = \"openai\"\nlimits_source = \"none\"\n[[providers.codex.models]]\nid = \"gpt\"\n[[providers.codex.bindings]]\nlabel = \"acct-b\"\naccount = \"acct-a\"\n[[providers.codex.bindings]]\nlabel = \"b\"\naccount = \"acct-b\"\n",
            home.path().display()
        );
        let config = ProviderConfig::parse(&config, home.path()).unwrap();
        let mut store = agent_run_store::fixture::open(home.path()).unwrap();
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
