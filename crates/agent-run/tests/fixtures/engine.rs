//! Offline fake engine. It neither contacts a provider nor executes task text.
//! Only explicit fixture-mode keywords change deterministic test behavior.
use serde_json::{json, Value};
use std::io::{self, BufRead, Write};
use std::time::Duration;
fn emit(value: Value) {
    println!("{value}");
    io::stdout().flush().expect("fixture stdout");
}
fn argument(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|s| s == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    // A detached helper mode used only by `fixture:descendant`: sleep past the
    // leader's own lifetime so the supervisor must reap it separately.
    if let Some(seconds) = argument(&args, "--child-sleep") {
        std::thread::sleep(Duration::from_secs(
            seconds.parse().expect("fixture child sleep"),
        ));
        return;
    }
    if args.first().map(String::as_str) == Some("app-server") {
        app_server();
        return;
    }
    if args.iter().any(|s| s == "--version") {
        println!("agent-run offline fixture 1.0");
        return;
    }
    let session = argument(&args, "--resume")
        .or_else(|| argument(&args, "--session-id"))
        .unwrap_or_else(|| "fixture-session".into());
    let task = if let Some(task) = argument(&args, "-p") {
        task
    } else {
        let mut line = String::new();
        io::stdin()
            .lock()
            .read_line(&mut line)
            .expect("fixture input");
        let value: Value = serde_json::from_str(&line).expect("fixture JSON");
        value
            .pointer("/message/content/0/text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned()
    };
    if task == "fixture:slow-start" {
        std::thread::sleep(Duration::from_secs(2));
    }
    if task == "fixture:ignore-sigterm" {
        // SAFETY: SIGTERM/SIG_IGN are valid disposition constants; this only
        // changes this process's own signal mask, no pointers are touched.
        unsafe {
            libc::signal(libc::SIGTERM, libc::SIG_IGN);
        }
    }
    emit(json!({"type":"system","subtype":"init","session_id":session}));
    emit(
        json!({"type":"assistant","session_id":session,"message":{"content":[{"type":"text","text":"fixture partial\n"}]}}),
    );
    if task == "fixture:truncated" {
        // No terminating newline: the reader must reject this as a truncated
        // frame instead of blocking forever or dropping it silently.
        print!("{{\"type\":\"result\",\"partial\":true");
        io::stdout().flush().expect("fixture stdout");
        return;
    }
    if task == "fixture:invalid-utf8" {
        io::stdout()
            .write_all(&[b'{', 0xff, 0xfe, b'}', b'\n'])
            .expect("fixture stdout");
        return;
    }
    if task == "fixture:oversized" {
        io::stdout()
            .write_all(&vec![b'a'; 9 * 1024 * 1024])
            .expect("fixture stdout");
        return;
    }
    if task == "fixture:command-flood" {
        // Reading one control frame lets the test observe the page boundary
        // without synchronizing on a wall-clock sleep.
        let marker_session = session.clone();
        std::thread::spawn(move || {
            let stdin = io::stdin();
            let mut line = String::new();
            let _ = stdin.lock().read_line(&mut line);
            emit(
                json!({"type":"assistant","session_id":marker_session,"message":{"content":[{"type":"text","text":"fixture poll marker\n"}]}}),
            );
        });
    }
    if task == "fixture:hang" || task == "fixture:command-flood" || task == "fixture:ignore-sigterm"
    {
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }
    if task == "fixture:descendant" {
        let exe = std::env::current_exe().expect("fixture exe path");
        // Deliberately not waited on: the whole point of this fixture mode
        // is a descendant that outlives its leader, so the supervisor must
        // reap it independently.
        #[allow(clippy::zombie_processes)]
        std::process::Command::new(exe)
            .arg("--child-sleep")
            .arg("2")
            .spawn()
            .expect("fixture descendant spawn");
        // Give the supervisor's periodic refresh a chance to observe the
        // descendant while this leader is still alive.
        std::thread::sleep(Duration::from_millis(500));
    }
    if task == "fixture:escaped-descendant" {
        use std::os::unix::process::CommandExt;

        let exe = std::env::current_exe().expect("fixture exe path");
        // The escaped child must not inherit this engine's stdout: holding that
        // pipe open would delay the supervisor's EOF until the descendant itself
        // exits, so cleanup would always observe an already-dead descendant and
        // the escaped-descendant scenario would never be exercised at all.
        #[allow(clippy::zombie_processes)]
        let child = std::process::Command::new(exe)
            .arg("--child-sleep")
            .arg("10")
            .process_group(0)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("fixture escaped descendant spawn");
        let marker = std::env::current_dir()
            .expect("fixture workdir")
            .join("escaped.pid");
        std::fs::write(marker, child.id().to_string()).expect("fixture escaped pid");
        std::thread::sleep(Duration::from_millis(500));
    }
    if task == "fixture:missing-result" {
        return;
    }
    if task == "fixture:slow" {
        std::thread::sleep(Duration::from_secs(3));
    }
    native_history(&session, &task);
    // A Claude Code 2.1.280 protocol frame rejecting a usage window, and the
    // same JSON merely quoted in assistant text (which must not count).
    let quota_frame = json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","resetsAt":1790200000},"uuid":"fixture-uuid","session_id":session});
    if task == "fixture:quota" || task.starts_with("fixture:quota-then-") {
        emit(quota_frame.clone());
    }
    // A model-scoped weekly window (no exact physical pool mapping).
    if task == "fixture:quota-opus" {
        emit(
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"seven_day_opus"},"uuid":"fixture-uuid-3","session_id":session}),
        );
    }
    // A later protocol state supersedes the rejection: the window is allowed
    // again, or the turn ends on an assistant error of another class.
    if task == "fixture:quota-then-allowed" {
        emit(
            json!({"type":"rate_limit_event","rate_limit_info":{"status":"allowed","rateLimitType":"five_hour"},"uuid":"fixture-uuid-2","session_id":session}),
        );
    }
    if task == "fixture:quota-then-auth" {
        emit(
            json!({"type":"assistant","session_id":session,"parent_tool_use_id":null,"error":"authentication_failed","message":{"content":[{"type":"text","text":"login required"}]}}),
        );
    }
    if task == "fixture:quota-text" {
        emit(
            json!({"type":"assistant","session_id":session,"message":{"content":[{"type":"text","text":quota_frame.to_string()}]}}),
        );
    }
    let failed = task == "fixture:error"
        || task == "fixture:quota"
        || task == "fixture:quota-opus"
        || task.starts_with("fixture:quota-then-");
    emit(
        json!({"type":"result","subtype":if failed{"error_during_execution"}else{"success"},"is_error":failed,"session_id":session,"result":if failed{"fixture failure"}else{"fixture final answer\n"},"usage":{"input_tokens":2,"output_tokens":3},"num_turns":1}),
    );
    if task == "fixture:nonzero-after-result" {
        io::stdout().flush().expect("fixture stdout");
        std::process::exit(3);
    }
}

/// Appends this turn to a Claude-shaped native transcript, as the real
/// harness does before its result: `$CLAUDE_CONFIG_DIR/projects/
/// -fixture-workdir/<session>.jsonl` with the user task, one completed tool
/// call and the assistant text. Nothing is written without a config dir.
fn native_history(session: &str, task: &str) {
    let Some(root) = std::env::var_os("CLAUDE_CONFIG_DIR") else {
        return;
    };
    let dir = std::path::Path::new(&root).join("projects/-fixture-workdir");
    std::fs::create_dir_all(&dir).expect("fixture history dir");
    let path = dir.join(format!("{session}.jsonl"));
    let turn = std::fs::read_to_string(&path)
        .map(|text| text.lines().count())
        .unwrap_or(0);
    let tool = format!("toolu-fixture-{turn}");
    let lines = [
        json!({"sessionId":session,"type":"user","message":{"role":"user","content":[{"type":"text","text":task}]}}),
        json!({"sessionId":session,"type":"assistant","message":{"content":[{"type":"tool_use","id":tool}]}}),
        json!({"sessionId":session,"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":tool}]}}),
        json!({"sessionId":session,"type":"assistant","message":{"content":[{"type":"text","text":"fixture final answer"}]}}),
    ];
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .expect("fixture history");
    for line in lines {
        writeln!(file, "{line}").expect("fixture history write");
    }
}

/// A minimal Codex app-server (JSON lines over stdio) for provider tests.
///
/// Answers `initialize`, `model/list` (model `fixture`), `thread/start` or
/// `thread/resume` (echoing the requested grant and keeping the resumed
/// thread id) and `turn/start`. It keeps a Codex-shaped rollout in
/// `$CODEX_HOME/sessions/.../rollout-fixture-<thread>.jsonl` with the turn
/// input. A turn fails with the authoritative `usageLimitExceeded` code when
/// the linked `$CODEX_HOME/auth.json` contains `exhausted`; otherwise it
/// completes with an agent message naming the thread.
fn app_server() {
    let home = std::path::PathBuf::from(std::env::var_os("CODEX_HOME").expect("CODEX_HOME"));
    let exhausted = std::fs::read_to_string(home.join("auth.json"))
        .map(|text| text.contains("exhausted"))
        .unwrap_or(false);
    let mut thread = String::new();
    let mut turns = 0;
    for line in io::stdin().lock().lines() {
        let Ok(line) = line else { break };
        let Ok(request) = serde_json::from_str::<Value>(&line) else {
            continue;
        };
        let (Some(id), Some(method)) = (request.get("id").cloned(), request["method"].as_str())
        else {
            continue;
        };
        let params = &request["params"];
        match method {
            "model/list" => emit(json!({"id":id,"result":{"data":[{"id":"fixture"}]}})),
            "thread/start" | "thread/resume" => {
                thread = params["threadId"]
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("019a-fixture-{}", std::process::id()));
                let rollout = rollout_path(&home, &thread);
                if method == "thread/start" {
                    std::fs::create_dir_all(rollout.parent().unwrap()).expect("rollout dir");
                    append(
                        &rollout,
                        &json!({"type":"session_meta","payload":{"id":thread}}),
                    );
                }
                let mut echo = json!({
                    "model":params["model"],"cwd":params["cwd"],"approvalPolicy":params["approvalPolicy"],
                    "sandbox":{"type":"readOnly","networkAccess":false},
                    "runtimeWorkspaceRoots":params.get("runtimeWorkspaceRoots").cloned().unwrap_or_else(|| json!([params["cwd"]])),
                    "thread":{"id":thread,"status":{"type":"idle"}},"threadId":thread,
                });
                if let Some(profile) = params["permissions"].as_str() {
                    echo["activePermissionProfile"] = json!({"id":profile});
                }
                if let Some(reviewer) = params.get("approvalsReviewer") {
                    echo["approvalsReviewer"] = reviewer.clone();
                }
                emit(json!({"id":id,"result":echo}));
            }
            "turn/start" => {
                // `slow` in the auth file makes each turn take 1.5 s.
                if std::fs::read_to_string(home.join("auth.json"))
                    .map(|text| text.contains("slow"))
                    .unwrap_or(false)
                {
                    std::thread::sleep(Duration::from_millis(1500));
                }
                turns += 1;
                let turn = format!("turn-{turns}");
                let input = params["input"][0]["text"].as_str().unwrap_or("").to_owned();
                let rollout = rollout_path(&home, &thread);
                append(
                    &rollout,
                    &json!({"type":"response_item","payload":{"type":"message","role":"user","content":input}}),
                );
                emit(json!({"id":id,"result":{"turn":{"id":turn}}}));
                // `hold` in the auth file pauses before the terminal frame
                // until the test releases it (a deterministic barrier).
                let hold = std::fs::read_to_string(home.join("auth.json"))
                    .map(|text| text.contains("hold"))
                    .unwrap_or(false);
                if hold {
                    std::fs::write(home.join("fixture-held"), "").expect("fixture hold marker");
                    for _ in 0..400 {
                        if home.join("fixture-release").exists() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                }
                if exhausted {
                    emit(
                        json!({"method":"turn/completed","params":{"threadId":thread,"turn":{"id":turn,"status":"failed","items":[],"error":{"message":"usage limit reached","codexErrorInfo":"usageLimitExceeded"}}}}),
                    );
                } else {
                    // Streamed as two deltas (leading whitespace and a
                    // terminal escape included) before the completed item.
                    let head = "fixture  \u{1b}[31mcodex ".to_owned();
                    let tail = format!("answer on {thread}");
                    let text = format!("{head}{tail}");
                    for delta in [&head, &tail] {
                        emit(
                            json!({"method":"item/agentMessage/delta","params":{"threadId":thread,"turnId":turn,"itemId":format!("msg-{turns}"),"delta":delta}}),
                        );
                    }
                    append(
                        &rollout,
                        &json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":text}}),
                    );
                    emit(
                        json!({"method":"turn/completed","params":{"threadId":thread,"turn":{"id":turn,"status":"completed","items":[{"type":"agentMessage","id":format!("msg-{turns}"),"text":text}]}}}),
                    );
                }
            }
            _ => emit(json!({"id":id,"result":{}})),
        }
    }
}

/// The fixture rollout file of `thread` below `home`.
fn rollout_path(home: &std::path::Path, thread: &str) -> std::path::PathBuf {
    home.join(format!(
        "sessions/2026/09/23/rollout-fixture-{thread}.jsonl"
    ))
}

/// Appends one JSON line to a fixture rollout.
fn append(path: &std::path::Path, value: &Value) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("fixture rollout");
    writeln!(file, "{value}").expect("fixture rollout write");
}
