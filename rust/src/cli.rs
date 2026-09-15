//! Operator CLI. Starts and resumes always go through the resident broker.
use crate::{
    capacity,
    config::{Adapter, Config},
    domain::{AgentId, OrchestratorRef, StartRequest, Status},
    error::invalid,
    fs,
    policy::Constraint,
    service::{Query, Service},
    state::Store,
    transport, Error, Result,
};
use clap::{Args, Parser, Subcommand};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
#[derive(Parser, Debug)]
#[command(
    name = "agent-run",
    version,
    about = "Durable local coding-agent supervisor (Rust migration)"
)]
pub struct Cli {
    #[arg(long, global = true, env = "AGENT_RUN_HOME")]
    pub home: Option<PathBuf>,
    #[command(subcommand)]
    pub command: Command,
}
#[derive(Subcommand, Debug)]
pub enum Command {
    Init,
    Doctor,
    Start(Start),
    Resume(Resume),
    Cancel {
        agent_id: AgentId,
    },
    Steer {
        agent_id: AgentId,
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
    },
    Models,
    Limits,
    Doc {
        topic: Option<String>,
    },
    Mcp {
        #[command(flatten)]
        session: SessionArgs,
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
        account: String,
        runtime: String,
    },
    State {
        #[command(subcommand)]
        command: State,
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
}
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
    fn resolve(&self, infer: bool) -> Result<Option<OrchestratorRef>> {
        let value = match (&self.session_transport, &self.session_id) {
            (Some(transport), Some(session)) => Some(OrchestratorRef {
                transport: transport.clone(),
                external_session_id: session.clone(),
                external_turn_id: self.session_turn_id.clone(),
            }),
            (None, None) if infer => {
                if let Ok(id) = std::env::var("CODEX_THREAD_ID") {
                    Some(OrchestratorRef {
                        transport: "codex_queue".into(),
                        external_session_id: id,
                        external_turn_id: None,
                    })
                } else if let Ok(id) = std::env::var("CLAUDE_SESSION_ID") {
                    Some(OrchestratorRef {
                        transport: "claude_uds".into(),
                        external_session_id: id,
                        external_turn_id: None,
                    })
                } else {
                    None
                }
            }
            (None, None) => None,
            _ => return Err(invalid("session transport and ID are required together")),
        };
        if let Some(o) = &value {
            o.validate()?;
        }
        Ok(value)
    }
}
#[derive(Args, Debug)]
pub struct Start {
    #[arg(long)]
    pub runtime: String,
    #[arg(long)]
    pub model: String,
    #[arg(long)]
    pub profile: String,
    #[arg(
        long,
        required_unless_present = "task_file",
        conflicts_with = "task_file"
    )]
    pub task: Option<String>,
    #[arg(long)]
    pub task_file: Option<PathBuf>,
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
        alias = "timeout-seconds",
        help = "Legacy metadata only; does not stop execution"
    )]
    pub timeout_seconds: Option<f64>,
    #[arg(long = "read-root")]
    pub read_roots: Vec<PathBuf>,
    #[arg(long)]
    pub output_schema: Option<PathBuf>,
    #[arg(long)]
    pub account: Option<String>,
    #[arg(long)]
    pub request_id: Option<String>,
    #[arg(long = "required-constraint")]
    pub required_constraints: Vec<String>,
    #[arg(long)]
    pub wait: bool,
    #[command(flatten)]
    pub session: SessionArgs,
}
#[derive(Args, Debug)]
pub struct Resume {
    pub agent_id: AgentId,
    #[arg(long)]
    pub task: String,
    #[arg(long = "timeout", alias = "timeout-seconds")]
    pub timeout_seconds: Option<f64>,
    #[arg(long)]
    pub request_id: Option<String>,
    #[arg(long)]
    pub wait: bool,
    #[command(flatten)]
    pub session: SessionArgs,
}
#[derive(Args, Debug)]
pub struct Agents {
    #[arg(long)]
    pub active: bool,
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
    #[arg(long)]
    pub after_revision: Option<i64>,
    #[arg(long, default_value_t = 0.0)]
    pub wait_seconds: f64,
    #[command(flatten)]
    pub session: SessionArgs,
}
#[derive(Subcommand, Debug)]
pub enum Api {
    Serve,
    Launchd {
        #[arg(long)]
        binary: Option<PathBuf>,
    },
    Ping,
    Tools,
}
#[derive(Subcommand, Debug)]
pub enum Capacity {
    Collect {
        #[arg(long)]
        once: bool,
    },
    Order,
    Launchd {
        #[arg(long)]
        binary: Option<PathBuf>,
    },
}
#[derive(Subcommand, Debug)]
pub enum Delivery {
    Status {
        agent_id: AgentId,
    },
    Dispatch {
        #[arg(long)]
        once: bool,
    },
    Launchd {
        #[arg(long)]
        binary: Option<PathBuf>,
    },
}
#[derive(Subcommand, Debug)]
pub enum State {
    Check,
    Backup {
        #[arg(long)]
        to: PathBuf,
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
            include_bytes!("../resources/config.example.toml"),
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
    Ok(json!({"home":home,"initialized":true,"state":store.health()?}))
}
pub fn doc(topic: &str) -> Result<&'static str> {
    match topic{
    "index"=>Ok(include_str!("../docs/OPERATOR_GUIDE.md")),
    "completion"=>Ok("agent-run/completion is a lifecycle notification, never a new task or user approval. Read answer and transcript using the durable agent ID. A succeeded runtime is not proof that the requested software change is correct; acceptance tests remain a separate decision. Unbound starts have no completion delivery. A disconnected client never implicitly cancels an admitted run."),
    "migration"=>Ok(include_str!("../docs/MIGRATION_STATUS.md")),
    "config"=>Ok(include_str!("../resources/config.example.toml")),
    "architecture"=>Ok(include_str!("../docs/PROJECT_MAP.md")),
    _=>Err(invalid("unknown guide topic; use index, completion, migration, config, architecture")),
}
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
pub fn launchd(home: &Path, binary: Option<PathBuf>, kind: &str, interval: u64) -> Result<String> {
    let binary = absolute(&binary.unwrap_or(std::env::current_exe()?))?;
    let args = match kind {
        "api" => vec!["api", "serve"],
        "capacity" => vec!["capacity", "collect", "--once"],
        "delivery" => vec!["delivery", "dispatch", "--once"],
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
    Ok(format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" \"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n<plist version=\"1.0\"><dict>\n  <key>Label</key><string>com.agent-run.{kind}</string>\n  <key>ProgramArguments</key><array>\n{args}  </array>\n  <key>EnvironmentVariables</key><dict><key>HOME</key><string>{}</string><key>PATH</key><string>{}</string></dict>\n  <key>RunAtLoad</key><true/>\n{schedule}  <key>StandardOutPath</key><string>{}</string>\n  <key>StandardErrorPath</key><string>{}</string>\n</dict></plist>\n",xml(&home_env),xml(&path),xml(&home.join(format!("logs/{kind}.out.log")).to_string_lossy()),xml(&home.join(format!("logs/{kind}.err.log")).to_string_lossy())))
}
async fn login(home: &Path, name: &str, account: Option<&str>) -> Result<i32> {
    let cfg = Config::load(home)?;
    let runtime = cfg.runtime(name)?;
    let kind = runtime.kind()?;
    if let Some(label) = account {
        runtime.selected_account(Some(label))?;
    }
    let mut command = tokio::process::Command::new(&runtime.binary);
    match kind {
        Adapter::Codex => {
            command.arg("login");
            if let Some(label) = account {
                let path = crate::adapters::materialize::account_home(home, kind, label);
                fs::private_dir(&path)?;
                command.env("CODEX_HOME", path);
            }
        }
        Adapter::Claude => {
            command.args(["auth", "login"]);
            if let Some(label) = account {
                let path = crate::adapters::materialize::account_home(home, kind, label)
                    .join("claude-config");
                fs::private_dir(&path)?;
                command.env("CLAUDE_CONFIG_DIR", path);
            } else {
                command.env_remove("CLAUDE_CONFIG_DIR");
            }
        }
        _ => {
            return Err(Error::Unsupported(
                "GLM/Qwen use explicitly declared environment authentication in this port".into(),
            ))
        }
    }
    // Interactive native authentication owns its prompts and credential storage.
    let status = command
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .await?;
    Ok(status.code().unwrap_or(1))
}
pub async fn run(cli: Cli) -> Result<i32> {
    let home = fs::home(cli.home)?;
    let service = Service::new(home.clone());
    match cli.command {
        Command::Init => emit(&init(&home)?)?,
        Command::Doctor => {
            let value = doctor(&home).await?;
            emit(&value)?;
            return Ok(if value["ok"] == true { 0 } else { 2 });
        }
        Command::Start(a) => {
            let task = match (a.task, a.task_file) {
                (Some(t), None) => t,
                (None, Some(p)) => read_input(&p, 512 * 1024)?,
                _ => return Err(invalid("provide exactly one task source")),
            };
            let schema = if let Some(p) = a.output_schema {
                Some(
                    serde_json::from_str::<serde_json::Map<String, Value>>(&read_input(
                        &p,
                        256 * 1024,
                    )?)
                    .map_err(|_| invalid("output schema must be a JSON object"))?,
                )
            } else {
                None
            };
            let constraints: BTreeSet<Constraint> = a
                .required_constraints
                .iter()
                .map(|s| {
                    serde_json::from_value(Value::String(s.clone()))
                        .map_err(|_| invalid("unknown required constraint"))
                })
                .collect::<Result<_>>()?;
            if constraints.len() != a.required_constraints.len() {
                return Err(invalid("duplicate required constraint"));
            }
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
                orchestrator: a.session.resolve(false)?,
                request_id: a.request_id,
                account: a.account,
                required_constraints: constraints,
            };
            request.validate()?;
            let result =
                transport::socket::client(&home, "start", serde_json::to_value(request)?).await?;
            emit(&result)?;
            if a.wait {
                let id: AgentId = serde_json::from_value(result["agent_id"].clone())?;
                let result =
                    transport::socket::client(&home, "wait", json!({"agent_id":id})).await?;
                emit(&result)?;
                return Ok(result_code(&result));
            }
        }
        Command::Resume(a) => {
            let result=transport::socket::client(&home,"resume",json!({"agent_id":a.agent_id,"task":a.task,"timeout_seconds":a.timeout_seconds,"request_id":a.request_id,"orchestrator":a.session.resolve(false)?})).await?;
            emit(&result)?;
            if a.wait {
                let result = transport::socket::client(
                    &home,
                    "wait",
                    json!({"agent_id":result["agent_id"]}),
                )
                .await?;
                emit(&result)?;
                return Ok(result_code(&result));
            }
        }
        Command::Cancel { agent_id } => emit(&service.cancel(&agent_id)?)?,
        Command::Steer { agent_id, text } => emit(&service.steer(&agent_id, &text)?)?,
        Command::Agents(a) => emit(
            &service
                .list(Query {
                    active: a.active,
                    offset: a.offset,
                    limit: a.limit,
                    after_revision: a.after_revision,
                    wait_seconds: a.wait_seconds,
                    orchestrator: a.session.resolve(false)?,
                })
                .await?,
        )?,
        Command::Answer { agent_id } => {
            let value = service.answer(&agent_id)?;
            emit(&value)?;
            return Ok(result_code(&value));
        }
        Command::Transcript {
            agent_id,
            mut cursor,
            limit,
            follow,
        } => loop {
            let page = service.transcript(&agent_id, cursor, limit)?;
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
            emit(&json!({"topic":topic,"text":doc(topic)?}))?;
        }
        Command::Mcp { session } => transport::mcp::serve(home, session.resolve(true)?).await?,
        Command::Api { command } => match command {
            Api::Serve => transport::socket::serve(&home).await?,
            Api::Launchd { binary } => print!("{}", launchd(&home, binary, "api", 0)?),
            Api::Ping => emit(&transport::socket::client(&home, "ping", json!({})).await?)?,
            Api::Tools => emit(&json!({"tools":crate::dispatch::tools()}))?,
        },
        Command::Capacity { command } => match command {
            Capacity::Order => emit(&service.capacity_order()?)?,
            Capacity::Launchd { binary } => print!(
                "{}",
                launchd(
                    &home,
                    binary,
                    "capacity",
                    Config::load(&home)?.capacity.collect_interval_seconds
                )?
            ),
            Capacity::Collect { once } => loop {
                let result = capacity::collect(&home).await?;
                emit(&result)?;
                if once {
                    return Ok(if result["ok"] == true { 0 } else { 2 });
                }
                let interval = Config::load(&home)?.capacity.collect_interval_seconds;
                tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(Duration::from_secs(interval))=>{}}
            },
        },
        Command::Delivery { command } => match command {
            Delivery::Status { agent_id } => emit(&service.delivery_status(&agent_id)?)?,
            Delivery::Launchd { binary } => print!("{}", launchd(&home, binary, "delivery", 2)?),
            Delivery::Dispatch { once } => loop {
                emit(&json!({"processed":crate::delivery::dispatch_once(&home).await?}))?;
                if once {
                    break;
                }
                tokio::select! {_=tokio::signal::ctrl_c()=>break,_=tokio::time::sleep(Duration::from_secs(1))=>{}}
            },
        },
        Command::Login { runtime, account } => {
            return login(&home, &runtime, account.as_deref()).await
        }
        Command::Auth { account, runtime } => return login(&home, &runtime, Some(&account)).await,
        Command::State { command } => match command {
            State::Check => emit(&Store::open(&home)?.health()?)?,
            State::Backup { to } => {
                let to = absolute(&to)?;
                Store::open(&home)?.backup(&to)?;
                emit(&json!({"backup":to,"complete":true}))?;
            }
        },
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
    }
    Ok(0)
}
