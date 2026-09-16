//! Operator CLI. Starts and resumes always go through the resident broker.
use crate::{
    capacity,
    config::{Adapter, Config},
    domain::{AgentId, OrchestratorRef, StartRequest},
    error::invalid,
    fs, hooks,
    policy::Constraint,
    service::{Query, Service},
    state::Store,
    transport, Result,
};
use clap::{ArgGroup, Args, Parser, Subcommand};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
/// The Python-compatible operator command line.
///
/// This parser is intentionally the single source of command names and flag
/// spelling for the binary. Service calls keep their request schemas in the
/// shared domain tool registry; this layer only adapts shell values to those
/// schemas and never launches a one-shot start locally.
#[derive(Parser, Debug)]
#[command(
    name = "agent-run",
    about = "Durable local coding-agent supervisor (Rust migration)"
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
    },
    Models,
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
    /// Privately evaluates one generated Codex trusted-MCP permission request.
    #[command(name = "_permission-request", hide = true)]
    PermissionRequest {
        /// Repeated configured MCP namespaces eligible for the narrow allow decision.
        #[arg(long = "allow-mcp", required = true)]
        allow_mcp: Vec<String>,
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
    #[arg(long)]
    pub runtime: String,
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
#[derive(Subcommand, Debug)]
pub enum Capacity {
    Collect {
        #[arg(long, required = true)]
        once: bool,
    },
    Order,
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
    let _ = Config::load(home)?;
    let store = Store::initialize(home)?;
    let _ = store.health()?;
    Ok(json!({"home":home,"config":home.join("config.toml"),"state":home.join("state.db")}))
}
pub fn doc(topic: &str) -> Result<&'static str> {
    crate::dispatch::doc(topic)
}
pub async fn doctor(home: &Path) -> Result<Value> {
    let cfg = Config::load(home)?;
    let store = Store::open(home)?;
    let mut checks = vec![json!({"name":"state","result":store.health()?})];
    drop(store);
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
/// Codex scopes labelled accounts below the agent-run home and Claude scopes
/// them below the configured runtime home. `status` selects the provider's
/// noninteractive verification invocation; both command forms retain no
/// provider output in agent-run's JSON response.
fn native_login_command(
    home: &Path,
    runtime: &crate::config::Runtime,
    kind: Adapter,
    account: Option<&str>,
    status: bool,
) -> Result<tokio::process::Command> {
    let mut command = tokio::process::Command::new(&runtime.binary);
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
            command.args(["auth", "login"]);
            if status {
                command.args(["status", "--json"]);
            }
            if let Some(label) = account {
                let path = crate::adapters::materialize::account_home(home, kind, label)
                    .join("claude-config");
                fs::private_dir(&path)?;
                command.env("CLAUDE_CONFIG_DIR", path);
            } else {
                command.env_remove("CLAUDE_CONFIG_DIR");
            }
        }
        Adapter::Glm | Adapter::Qwen => {
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
    let cfg = Config::load(home)?;
    let runtime = cfg.runtime(name)?;
    let kind = runtime.kind()?;
    if claude_only && kind != Adapter::Claude {
        return Err(invalid(format!(
            "login supports Claude only; use agent-run auth <label> {name}"
        )));
    }
    let account = runtime.selected_account(account)?;
    let account_name = account.as_deref().unwrap_or("default");
    // Interactive native authentication owns its prompts and credential storage.
    let status = native_login_command(home, runtime, kind, account.as_deref(), false)?
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
    let status = native_login_command(home, runtime, kind, account.as_deref(), true)?
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
    Ok((
        0,
        json!({"account":account,"runtime":if kind == Adapter::Claude {"claude"} else {name},"status":"ok"}),
    ))
}
/// Executes one parsed command and returns its public process exit status.
///
/// Success writes JSON (except the stdio server), while expected failures
/// propagate as typed errors for `main` to render as the Python-compatible
/// JSON error envelope. `start` and `resume` exclusively call the resident
/// socket broker.
pub async fn run(cli: Cli) -> Result<i32> {
    let home = fs::home(cli.home)?;
    let service = Service::new(home.clone());
    match cli.command {
        Command::Init => emit(&crate::init::initialize(&home)?)?,
        Command::Doctor => {
            let report = crate::doctor::run(&home)?;
            let ok = report.ok();
            emit(&serde_json::to_value(report)?)?;
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
            let mut request = StartRequest {
                runtime: a.runtime,
                model: a.model,
                profile: a.profile,
                task,
                workdir: absolute(&a.workdir.unwrap_or(std::env::current_dir()?))?,
                write: a.write,
                fast: a.fast,
                effort: a.effort,
                timeout_seconds: a.timeout_seconds,
                read_roots: a
                    .read_roots
                    .iter()
                    .map(|p| absolute(p))
                    .collect::<Result<_>>()?,
                output_schema: schema,
                orchestrator: a.session.resolve()?,
                request_id: a.request_id,
                account: a.account,
                required_constraints: BTreeSet::<Constraint>::new(),
            };
            request.validate()?;
            let result =
                transport::socket::client(&home, "start", serde_json::to_value(request)?).await?;
            emit(&admission_output(&result)?)?;
            if a.wait {
                let id: AgentId = serde_json::from_value(result["agent_id"].clone())?;
                let result =
                    transport::socket::client(&home, "wait", json!({"agent_id":id})).await?;
                emit(&result)?;
                return Ok(result_code(&result));
            }
        }
        Command::Resume(a) => {
            let task = match (a.task, a.task_file) {
                (Some(task), None) => task_text(&task, 1024 * 1024)?,
                (None, Some(path)) if path == Path::new("-") => read_stdin(1024 * 1024)?,
                (None, Some(path)) => read_input(&path, 1024 * 1024)?,
                _ => return Err(invalid("provide exactly one resume task source")),
            };
            let result=transport::socket::client(&home,"resume",json!({"agent_id":a.agent_id,"task":task,"timeout_seconds":a.timeout_seconds,"request_id":a.request_id,"orchestrator":a.session.resolve()?})).await?;
            emit(&admission_output(&result)?)?;
        }
        Command::Cancel { agent_id } => emit(&service.cancel(&agent_id)?)?,
        Command::Steer { agent_id, text } => emit(&service.steer(&agent_id, &text)?)?,
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
            emit(&store.delivery_status(&a.agent_id)?)?;
        }
        Command::Context(a) => {
            let reference = OrchestratorRef {
                transport: a.session_transport,
                external_session_id: a.session_id,
                external_turn_id: a.session_turn_id,
            };
            emit(&serde_json::to_value(hooks::context::build(
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
                    emit(&output)?;
                }
                Hook::Bind(transport) => {
                    let mut store = Store::open(&home)?;
                    let result =
                        hooks::bind::run_hook(&mut store, &payload, &transport.transport, None)?;
                    emit(
                        &json!({"hookSpecificOutput":{"hookEventName":"PostToolUse","additionalContext":result.message()}}),
                    )?;
                }
            }
        }
        Command::Agents(a) => emit(
            &service
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
            let value = service.answer(&agent_id)?;
            emit(&value)?;
        }
        Command::Transcript {
            agent_id,
            mut cursor,
            limit,
            follow,
            full,
        } => loop {
            let page = service.transcript(&agent_id, cursor, limit)?;
            if full {
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
                    current = service.transcript(&agent_id, page_cursor, limit)?;
                    messages.extend(current["messages"].as_array().cloned().unwrap_or_default());
                    pages += 1;
                }
                emit(
                    &json!({"agent_id":agent_id,"messages":messages,"cursor":cursor,"next_cursor":null,"complete":true,"pages":pages}),
                )?;
                break;
            }
            emit(&page)?;
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
            if page["complete"] == true && Store::open(&home)?.get(&agent_id)?.status.terminal() {
                break;
            }
            tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(Duration::from_millis(250))=>{}}
        },
        Command::Models => emit(&service.models().await?)?,
        Command::Limits => emit(&service.limits()?)?,
        Command::Doc { topic } => {
            let topic = topic.as_deref().unwrap_or("index");
            emit(&json!({"topic":topic,"text":crate::dispatch::doc(topic)?}))?;
        }
        Command::Mcp => transport::mcp::serve(home, None).await?,
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
            } => emit(&launchd(
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
            Capacity::Order => emit(&service.capacity_order()?)?,
            Capacity::Launchd {
                binary,
                label,
                stdout_log,
                stderr_log,
            } => emit(&launchd(
                &home,
                binary,
                "capacity",
                Config::load(&home)?.capacity.collect_interval_seconds,
                &label,
                stdout_log,
                stderr_log.unwrap_or_else(|| home.join("capacity-worker.err.log")),
            )?)?,
            Capacity::Collect { once: _ } => {
                let result = capacity::collect(&home).await?;
                emit(&result)?;
                return Ok(if result["ok"] == true { 0 } else { 2 });
            }
        },
        Command::Delivery { command } => match command {
            Delivery::Status { agent_id } => emit(&service.delivery_status(&agent_id)?)?,
            Delivery::Cancel { delivery_id } => emit(&service.delivery_cancel(&delivery_id)?)?,
            Delivery::Launchd {
                binary,
                label,
                stdout_log,
                stderr_log,
            } => emit(&launchd(
                &home,
                binary,
                "delivery",
                2,
                &label,
                stdout_log,
                stderr_log.unwrap_or_else(|| home.join("delivery-worker.err.log")),
            )?)?,
            Delivery::Dispatch => {
                emit(&json!({"processed":crate::delivery::dispatch_once(&home).await?}))?;
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
