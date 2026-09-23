//! Operator CLI. Starts and resumes always go through the resident broker.
use crate::{
    capacity,
    config::{Adapter, Config},
    domain::{AgentId, OrchestratorRef},
    error::invalid,
    fs, hooks,
    service::{Query, Service},
    state::Store,
    transport, Result,
};
use agent_run_domain::{
    catalog::{AccountId, AccountRecord, AccountStatus, AuthFamily, SecretRef},
    CredentialRef,
};
use clap::{ArgGroup, Args, Parser, Subcommand, ValueEnum};
use serde_json::{json, Value};
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

/// Service operations used by public CLI commands.
pub trait CliService: Send + Sync {
    /// Cancel one durable agent and return its public view.
    fn cancel(&self, id: &AgentId) -> Result<Value>;
    /// Send one steering command and return its durable acknowledgement.
    fn steer(&self, id: &AgentId, text: &str) -> Result<Value>;
    /// List agents, optionally waiting for a durable revision.
    fn list<'a>(&'a self, query: Query) -> CliFuture<'a>;
    /// Read one verified answer envelope.
    fn answer(&self, id: &AgentId) -> Result<Value>;
    /// Read one durable agent view for follow-up terminal checks.
    fn agent(&self, id: &AgentId) -> Result<Value>;
    /// Read one bounded transcript page.
    fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value>;
    /// Return the configured model roster or schema-2 provider catalog.
    fn models<'a>(&'a self, query: agent_run_domain::ModelsQuery) -> CliFuture<'a>;
    /// Return the configured capacity limits.
    fn limits(&self) -> Result<Value>;
    /// Return the ordered capacity view, optionally for one exact model.
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> Result<Value>;
    /// Return one completion-delivery status view.
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
    fn cancel(&self, id: &AgentId) -> Result<Value> {
        Service::cancel(self, id)
    }
    fn steer(&self, id: &AgentId, text: &str) -> Result<Value> {
        Service::steer(self, id, text)
    }
    fn list<'a>(&'a self, query: Query) -> CliFuture<'a> {
        Box::pin(Service::list(self, query))
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
    fn models<'a>(&'a self, query: agent_run_domain::ModelsQuery) -> CliFuture<'a> {
        Box::pin(Service::models(self, query))
    }
    fn limits(&self) -> Result<Value> {
        Service::limits(self)
    }
    fn capacity_order(&self, query: agent_run_domain::CapacityOrderQuery) -> Result<Value> {
        Service::capacity_order(self, query)
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
    Bind(Bind),
    Cancel {
        agent_id: AgentId,
    },
    Steer {
        agent_id: AgentId,
        #[arg(long)]
        text: String,
    },
    Agents(Agents),
    Answer {
        agent_id: AgentId,
    },
    Transcript {
        agent_id: AgentId,
        #[arg(long, default_value_t = 0)]
        cursor: i64,
        #[arg(long, default_value_t = 200)]
        limit: usize,
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
    Doc {
        topic: Option<String>,
    },
    Mcp,
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
        #[arg(long)]
        reference: SecretRef,
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
        help = "Legacy metadata only; does not stop execution"
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
    #[arg(long)]
    pub wait: bool,
    #[command(flatten)]
    pub session: SessionArgs,
}
/// The resident-broker continuation command's shell request fields.
#[derive(Args, Debug)]
#[command(group = ArgGroup::new("resume_task").required(true).args(["task", "task_file"]))]
pub struct Resume {
    pub agent_id: AgentId,
    #[arg(long)]
    pub task: Option<String>,
    #[arg(long)]
    pub task_file: Option<PathBuf>,
    #[arg(long = "timeout", id = "timeout")]
    pub timeout_seconds: Option<f64>,
    #[arg(long)]
    pub request_id: Option<String>,
    #[command(flatten)]
    pub session: SessionArgs,
}
/// A durable session binding request accepted by the Python command surface.
#[derive(Args, Debug)]
pub struct Bind {
    /// Existing durable agent to bind.
    pub agent_id: AgentId,
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
    #[command(flatten)]
    pub session: SessionArgs,
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
    /// Plan (`--dry-run`) or apply (`--apply`) the paired migration (v1 config
    /// and older state database → v2 config and current database) from an
    /// explicit operator mapping file; apply snapshots config and state first.
    #[command(group(ArgGroup::new("mode").required(true).args(["dry_run", "apply"])))]
    Migrate {
        /// TOML mapping: `[harnesses.*]`, `[accounts.<id>]` and one
        /// `[runtimes.<v1 name>]` each.
        #[arg(long)]
        mapping: PathBuf,
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
    /// Restore the v1 config and original database from one verified
    /// snapshot while nothing changed since the migration; also recovers an
    /// interrupted migration or rollback of that snapshot.
    Rollback {
        /// The snapshot directory printed by `config migrate --apply`.
        #[arg(long)]
        snapshot: PathBuf,
    },
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
    Status {
        agent_id: AgentId,
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
/// indicator. A malformed broker result is treated as a typed validation
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
    for (name,body) in [
        ("review","+++\nwrite = false\nnetwork = false\n+++\nReview the repository read-only. Separate observed facts, risks, and recommendations. Do not modify files.\n"),
        ("architect","+++\nwrite = false\nnetwork = false\n+++\nStudy the repository read-only and propose an implementation plan. Do not change files.\n"),
        ("code","+++\nwrite = true\nnetwork = false\n+++\nImplement the assigned change within the granted workspace. Preserve existing behaviour and report the exact checks performed.\n"),
        ("research","+++\nwrite = false\nnetwork = true\n+++\nResearch the task. Distinguish sourced facts from assumptions. Do not change local files.\n"),
    ]{let file=format!("profiles/{name}.md");if dir.optional(Path::new(&file),1024*1024)?.is_none(){dir.write(Path::new(&file),body.as_bytes(),0o600)?;}}
    for (name,write,body) in [("role-review",false,"Perform a read-only review. Report evidence and recommendations; do not modify files."),("role-architect",false,"Analyze architecture read-only and produce a plan with explicit acceptance tests."),("role-code",true,"Implement the assigned task within the granted workspace. Test changes and report remaining uncertainty.")]{
        let file=format!("profiles/{name}.md");let text=format!("+++\nrevision = \"rust-role-v1\"\nwrite = {write}\nnetwork = false\nallow_external_read_roots = true\nskills = []\nmcp = []\nrequired_constraints = []\n+++\n{body}\n");
        if dir.optional(Path::new(&file),1024*1024)?.is_none(){dir.write(Path::new(&file),text.as_bytes(),0o600)?;}
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
    let plist = format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n  <key>Label</key><string>{}</string>\n  <key>ProgramArguments</key><array>\n{args}  </array>\n  <key>EnvironmentVariables</key><dict><key>HOME</key><string>{}</string><key>PATH</key><string>{}</string></dict>\n  <key>RunAtLoad</key><true/>\n{schedule}  <key>StandardOutPath</key><string>{}</string>\n  <key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",xml(label),xml(&home_env),xml(&path),xml(&stdout_log.to_string_lossy()),xml(&stderr_log.to_string_lossy()));
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
/// `named:<harness>:<label>` reference on that provider's harness.
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
        Some(wanted) => provider
            .bindings
            .iter()
            .find(|binding| binding.label.as_str() == wanted || binding.account.as_str() == wanted),
        None if provider.bindings.len() == 1 => provider.bindings.first(),
        None => {
            return Err(invalid(
                "provider binds several accounts; name one with --account",
            ))
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
            ))
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
/// Success writes JSON (except the stdio server), while expected failures
/// propagate as typed errors for `main` to render as the Python-compatible
/// JSON error envelope. `start` and `resume` exclusively call the resident
/// socket broker.
pub async fn run(cli: Cli) -> Result<i32> {
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
    agent_run_core::logging::configure(
        &home,
        if matches!(&cli.command, Command::Mcp) {
            "mcp"
        } else {
            "cli"
        },
    );
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
                }))
                .map_err(|_| invalid("invalid provider start arguments"))?;
            request.validate()?;
            let result = dependencies
                .broker
                .call("start", serde_json::to_value(request)?)
                .await?;
            if a.wait {
                let id: AgentId = serde_json::from_value(result["agent_id"].clone())?;
                let result = dependencies
                    .broker
                    .call("wait", json!({"agent_id": id}))
                    .await?;
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
            let result = dependencies
                .broker
                .call(
                    "resume",
                    json!({"agent_id":a.agent_id,"task":task,"timeout_seconds":a.timeout_seconds,"request_id":a.request_id,"orchestrator":a.session.resolve()?}),
                )
                .await?;
            (dependencies.output)(&admission_output(&result)?)?;
        }
        Command::Cancel { agent_id } => {
            (dependencies.output)(&dependencies.service.cancel(&agent_id)?)?
        }
        Command::Steer { agent_id, text } => {
            (dependencies.output)(&dependencies.service.steer(&agent_id, &text)?)?
        }
        Command::Bind(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            let mut store = Store::open(&home)?;
            hooks::bind::bind(
                &mut store,
                a.agent_id.clone(),
                reference,
                crate::domain::now(),
            )?;
            (dependencies.output)(&store.delivery_status(&a.agent_id)?)?;
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
                    let result =
                        hooks::bind::run_hook(&mut store, &payload, &transport.transport, None)?;
                    (dependencies.output)(
                        &json!({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":result.message()}}),
                    )?;
                }
            }
        }
        Command::Agents(a) => (dependencies.output)(
            &dependencies
                .service
                .list(Query {
                    active: a.active,
                    offset: a.offset,
                    limit: a.limit,
                    after_revision: None,
                    wait_seconds: 0.0,
                    orchestrator: a.session.resolve()?,
                })
                .await?,
        )?,
        Command::Answer { agent_id } => {
            let value = dependencies.service.answer(&agent_id)?;
            (dependencies.output)(&value)?;
        }
        Command::Transcript {
            agent_id,
            mut cursor,
            limit,
            follow,
            full,
            format,
        } => {
            // An explicit --format always wins; otherwise text is interactive
            // and JSON keeps piped consumers on the historical machine shape.
            let text = TranscriptFormat::effective(format) == TranscriptFormat::Text;
            // Streaming state persists across pages and polls so journal
            // fragments of one model message render continuously; the sink
            // writes each rendered chunk immediately.
            let mut renderer = crate::transcript::Renderer::default();
            if full {
                let page = dependencies.service.transcript(&agent_id, cursor, limit)?;
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
                    current = dependencies
                        .service
                        .transcript(&agent_id, page_cursor, limit)?;
                    messages.extend(current["messages"].as_array().cloned().unwrap_or_default());
                    pages += 1;
                }
                if text {
                    renderer.page(&messages, &mut |line| (dependencies.text_output)(line))?;
                    renderer.finish(&mut |line| (dependencies.text_output)(line))?;
                } else {
                    (dependencies.output)(
                        &json!({"agent_id":agent_id,"messages":messages,"cursor":cursor,"next_cursor":null,"complete":true,"pages":pages}),
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
                    let page = dependencies.service.transcript(&agent_id, cursor, limit)?;
                    if text {
                        renderer.page(
                            page["messages"]
                                .as_array()
                                .map(Vec::as_slice)
                                .unwrap_or(&[]),
                            &mut |line| (dependencies.text_output)(line),
                        )?;
                    } else {
                        (dependencies.output)(&page)?;
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
        Command::Doc { topic } => {
            let topic = topic.as_deref().unwrap_or("index");
            (dependencies.output)(&json!({"topic":topic,"text":crate::dispatch::doc(topic)?}))?;
        }
        Command::Mcp => transport::mcp::serve_with(home, None, dependencies.broker.clone()).await?,
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
            Delivery::Status { agent_id } => {
                (dependencies.output)(&dependencies.service.delivery_status(&agent_id)?)?
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
                dry_run: _,
                apply,
                ack,
                from_release,
            } => (dependencies.output)(&crate::migrate::migrate(
                &home,
                &mapping,
                apply,
                &ack,
                from_release.as_deref(),
            )?)?,
            ConfigCommand::Rollback { snapshot } => {
                (dependencies.output)(&crate::migrate::rollback(&home, &snapshot)?)?
            }
        },
        Command::Accounts { command } => match command {
            AccountCommand::Register {
                id,
                auth_family,
                reference,
            } => {
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
