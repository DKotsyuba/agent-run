//! Opt-in live qualification of the owned OpenCode v2 adapter against a real
//! OpenCode binary and a free public model.
//!
//! Set `AGENT_RUN_LIVE_OPENCODE` to an OpenCode binary (qualified: 2.3.0). The
//! test builds a disposable Run home with its own resident broker, never the
//! owner's, and drives start → finish → explicit resume → cancel through the
//! real CLI/socket boundaries. `AGENT_RUN_LIVE_OPENCODE_MODEL` overrides the
//! native model (default `opencode/deepseek-v4.1-flash-free`). Without the
//! variable the test returns immediately.

use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

/// One disposable Run home plus its resident broker.
struct Live {
    /// Keeps the short Unix-socket base directory alive.
    _temp: tempfile::TempDir,
    /// Run home passed to every CLI call.
    home: PathBuf,
    /// The home's own broker; killed on drop after cancelling live agents.
    broker: Option<Child>,
}

impl Live {
    /// Initializes a schema-2 home with one OpenCode provider on `binary`.
    fn new(binary: &Path, model: &str) -> Self {
        let temp = tempfile::Builder::new()
            .prefix("ar-oc-")
            .tempdir_in(
                std::env::var_os("AGENT_RUN_TEST_TMP")
                    .unwrap_or_else(|| std::env::temp_dir().into_os_string()),
            )
            .unwrap();
        let home = temp.path().canonicalize().unwrap();
        let mut live = Self {
            _temp: temp,
            home,
            broker: None,
        };
        live.cli(&["init"]);
        let config = format!(
            "schema_version=2\n[harnesses.codex]\nbinary='/bin/false'\nhome={codex}\n\
             [harnesses.claude-code]\nbinary='/bin/false'\nhome={claude}\n\
             [harnesses.opencode]\nbinary={binary}\nhome={runtime}\n\
             [providers.opencode-zen]\nharness='opencode'\nconnection={{kind='native'}}\n\
             auth_family='opencode'\nlimits_source='none'\n\
             [[providers.opencode-zen.models]]\nid='live-free'\nnative_model={model}\n\
             [[providers.opencode-zen.bindings]]\nlabel='public'\naccount='acct-opencode-public'\n",
            binary = toml::Value::String(binary.to_string_lossy().into_owned()),
            runtime =
                toml::Value::String(live.home.join("opencode").to_string_lossy().into_owned()),
            model = toml::Value::String(model.into()),
            codex = toml::Value::String(live.home.join("codex").to_string_lossy().into_owned()),
            claude = toml::Value::String(live.home.join("claude").to_string_lossy().into_owned()),
        );
        std::fs::write(live.home.join("config.toml"), config).unwrap();
        std::fs::create_dir_all(live.home.join("profiles")).unwrap();
        std::fs::write(
            live.home.join("profiles/review.md"),
            "+++\nrevision = \"1\"\nwrite = false\nnetwork = false\nallow_external_read_roots = false\nskills = []\nmcp = []\nrequired_constraints = []\n+++\nYou are a qualification worker. Follow the task literally and keep replies short.\n",
        )
        .unwrap();
        std::fs::create_dir_all(live.home.join("work")).unwrap();
        live.cli(&[
            "accounts",
            "register",
            "--id",
            "acct-opencode-public",
            "--auth-family",
            "opencode",
            "--reference",
            "native:opencode",
        ]);
        live.start_broker();
        live
    }

    /// Runs one CLI command against this home only and returns its JSON output.
    fn cli(&self, args: &[&str]) -> Value {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
        command.arg("--home").arg(&self.home).args(args);
        scrub(&mut command);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{args:?}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice(&output.stdout).unwrap_or(Value::Null)
    }

    /// Starts this home's own broker and waits for its socket.
    fn start_broker(&mut self) {
        let error = std::fs::File::create(self.home.join("broker-live.stderr")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
        command
            .arg("--home")
            .arg(&self.home)
            .args(["api", "serve"])
            .env("HOME", &self.home)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(error);
        scrub(&mut command);
        self.broker = Some(command.spawn().unwrap());
        let deadline = Instant::now() + Duration::from_secs(20);
        while !self.home.join("api.sock").exists() {
            assert!(
                Instant::now() < deadline,
                "live broker did not start: {}",
                std::fs::read_to_string(self.home.join("broker-live.stderr")).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        std::thread::sleep(Duration::from_millis(300));
    }

    /// Reads the run's current answer view (status, availability, content).
    fn answer(&self, id: &str) -> Value {
        self.cli(&["answer", id])
    }

    /// Waits for a terminal status within `limit`.
    fn terminal(&self, id: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            let answer = self.answer(id);
            if !matches!(
                answer["status"].as_str(),
                Some("created" | "starting" | "running" | "cancelling")
            ) {
                return answer;
            }
            assert!(Instant::now() < deadline, "{id} did not settle: {answer}");
            std::thread::sleep(Duration::from_millis(500));
        }
    }

    /// Opens the home's store read-only for evidence queries.
    fn store(&self) -> rusqlite::Connection {
        rusqlite::Connection::open_with_flags(
            self.home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap()
    }

    /// The execution an explicit resume of `parent` created, once admitted.
    fn child_of(&self, parent: &str) -> String {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if let Ok(child) = self.store().query_row(
                "SELECT id FROM agents WHERE parent_agent_id=?",
                [parent],
                |row| row.get(0),
            ) {
                return child;
            }
            assert!(
                Instant::now() < deadline,
                "no resumed execution of {parent}"
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// Event kinds recorded for one run, oldest first.
    fn events(&self, id: &str) -> Vec<String> {
        let store = self.store();
        let mut statement = store
            .prepare("SELECT kind FROM events WHERE agent_id=? ORDER BY seq")
            .unwrap();
        statement
            .query_map([id], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
}

impl Drop for Live {
    /// Cancels anything still active through this home's broker, then stops it.
    fn drop(&mut self) {
        if let Ok(store) = rusqlite::Connection::open_with_flags(
            self.home.join("state.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        ) && let Ok(mut statement) =
            store.prepare("SELECT id FROM agents WHERE status IN ('created','starting','running')")
            && let Ok(rows) = statement.query_map([], |row| row.get::<_, String>(0))
        {
            for id in rows.flatten() {
                let mut cancel = Command::new(env!("CARGO_BIN_EXE_agent-run"));
                cancel
                    .arg("--home")
                    .arg(&self.home)
                    .args(["cancel", &id])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null());
                scrub(&mut cancel);
                let _ = cancel.status();
            }
            std::thread::sleep(Duration::from_secs(3));
        }
        if let Some(mut broker) = self.broker.take() {
            let _ = broker.kill();
            let _ = broker.wait();
        }
        // `AGENT_RUN_LIVE_KEEP` retains the disposable home for diagnosis.
        if std::env::var_os("AGENT_RUN_LIVE_KEEP").is_some() {
            eprintln!("live opencode home kept: {}", self.home.display());
            self._temp.disable_cleanup(true);
        }
    }
}

/// Removes every inherited Agent Run variable so nothing reaches a live broker.
fn scrub(command: &mut Command) {
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("AGENT_RUN_") {
            command.env_remove(name);
        }
    }
}

/// Every live process whose command line names `needle`, with its environment.
#[cfg(target_os = "linux")]
fn processes_naming(needle: &str) -> Vec<(u32, String, Vec<String>)> {
    let mut found = Vec::new();
    for entry in std::fs::read_dir("/proc").unwrap().flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(cmdline) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let cmdline = String::from_utf8_lossy(&cmdline).replace('\0', " ");
        if !cmdline.contains(needle) {
            continue;
        }
        let environment = std::fs::read(entry.path().join("environ"))
            .map(|bytes| {
                bytes
                    .split(|byte| *byte == 0)
                    .filter_map(|name| {
                        let text = String::from_utf8_lossy(name);
                        text.split_once('=').map(|(name, _)| name.to_owned())
                    })
                    .collect()
            })
            .unwrap_or_default();
        found.push((pid, cmdline, environment));
    }
    found
}

/// Live 2.3.0 qualification: start → finish "OK" → resume → finish "OK2" →
/// resume → cancel, with the seal's version recorded, the resume accepted on
/// the same native session, and no worker capability in any OpenCode process
/// environment while the worker MCP still authenticates.
#[test]
fn opencode_live_start_finish_resume_cancel() {
    let Some(binary) = std::env::var_os("AGENT_RUN_LIVE_OPENCODE") else {
        return;
    };
    let model = std::env::var("AGENT_RUN_LIVE_OPENCODE_MODEL")
        .unwrap_or_else(|_| "opencode/deepseek-v4.1-flash-free".into());
    let live = Live::new(Path::new(&binary), &model);
    let workdir = live.home.join("work");
    let started = live.cli(&[
        "start",
        "--provider",
        "opencode-zen",
        "--model",
        "live-free",
        "--profile",
        "review",
        "--workdir",
        workdir.to_str().unwrap(),
        "--task",
        "Call the agent_run_worker finish tool now with summary exactly: OK",
    ]);
    let first = started["agent_id"].as_str().unwrap().to_owned();
    eprintln!("live opencode start: {first}");
    let answer = live.terminal(&first, Duration::from_secs(300));
    assert_eq!(answer["status"], "succeeded", "{answer}");
    assert_eq!(
        answer["content"].as_str().map(str::trim),
        Some("OK"),
        "{answer}"
    );
    let state: String = live
        .store()
        .query_row(
            "SELECT adapter_state_json FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [&first],
            |row| row.get(0),
        )
        .unwrap();
    let state: Value = serde_json::from_str(&state).unwrap();
    let seal = &state["native_history"]["seal"];
    assert_eq!(seal["harness"], "opencode", "{state}");
    assert_eq!(seal["sqlite"]["version"], "2.3.0", "{state}");

    live.cli(&[
        "resume",
        &first,
        "--task",
        "Call the agent_run_worker finish tool now with summary exactly: OK2",
    ]);
    let second = live.child_of(&first);
    eprintln!("live opencode resume: {second}");
    let answer = live.terminal(&second, Duration::from_secs(300));
    assert_eq!(answer["status"], "succeeded", "{answer}");
    assert_eq!(
        answer["content"].as_str().map(str::trim),
        Some("OK2"),
        "{answer}"
    );
    assert!(
        live.events(&second)
            .iter()
            .any(|kind| kind == "native_resume_accepted"),
        "resume did not continue the native session"
    );

    live.cli(&[
        "resume",
        &first,
        "--task",
        "Do not call finish. Reply with the single word WAITING and then stop and wait for further instructions.",
    ]);
    let third = live.child_of(&second);
    eprintln!("live opencode cancel target: {third}");
    let deadline = Instant::now() + Duration::from_secs(120);
    while !live
        .events(&third)
        .iter()
        .any(|kind| kind == "native_resume_accepted")
    {
        assert!(
            Instant::now() < deadline,
            "third run never resumed natively"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    // While the owned server runs, neither it nor any MCP/tool child carries a
    // private Agent Run name; the worker MCP reads only its context file.
    #[cfg(target_os = "linux")]
    {
        let runtime = live.home.join("opencode");
        let serve = processes_naming(&binary.to_string_lossy())
            .into_iter()
            .filter(|(_, cmdline, _)| cmdline.contains("serve"))
            .collect::<Vec<_>>();
        assert!(!serve.is_empty(), "no live OpenCode serve process");
        let worker = processes_naming("_worker-mcp")
            .into_iter()
            .filter(|(_, cmdline, _)| cmdline.contains(&runtime.to_string_lossy().into_owned()))
            .collect::<Vec<_>>();
        assert!(!worker.is_empty(), "no live worker MCP for this home");
        for (pid, cmdline, environment) in serve.iter().chain(&worker) {
            assert!(
                environment
                    .iter()
                    .all(|name| !name.starts_with("AGENT_RUN_WORKER_")
                        && !name.starts_with("AGENT_RUN_SYSTEM_")),
                "{pid} {cmdline} inherited {environment:?}"
            );
        }
        assert!(
            worker
                .iter()
                .all(|(_, cmdline, _)| cmdline.contains("--context-file"))
        );
        let rendered = std::fs::read_to_string(
            std::fs::read_dir(runtime.join("runs"))
                .unwrap()
                .flatten()
                .map(|entry| entry.path().join("opencode.json"))
                .find(|path| path.exists())
                .unwrap(),
        )
        .unwrap();
        assert!(!rendered.contains("AGENT_RUN_WORKER_TOKEN"));
    }
    std::thread::sleep(Duration::from_secs(5));
    live.cli(&["cancel", &third]);
    let answer = live.terminal(&third, Duration::from_secs(60));
    assert_eq!(answer["status"], "cancelled", "{answer}");
    eprintln!("live opencode ids: {first} {second} {third}");
}
