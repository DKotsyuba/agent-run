//! Offline fake engine. It neither contacts a provider nor executes task text.
//! Only explicit fixture-mode keywords change deterministic test behavior.
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
use std::time::{Duration, Instant};
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
/// Exercise the sealed worker MCP using only inherited capability names. The
/// child has a six-second deadline and kill-on-drop; no provider or Desktop
/// is contacted. Persist only the nonsecret tool response for the owning test.
fn worker_report(args: &[String]) {
    let config: Value = serde_json::from_str(
        &std::fs::read_to_string(argument(args, "--mcp-config").expect("worker config path"))
            .unwrap(),
    )
    .unwrap();
    let server = &config["mcpServers"]["agent_run_worker"];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use agent_run::transport::{frame, socket};
        use tokio::io::BufReader;
        use std::process::Stdio;
        let mut child = tokio::process::Command::new(server["command"].as_str().unwrap())
            .args(server["args"].as_array().unwrap().iter().map(|arg|arg.as_str().unwrap()))
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        tokio::time::timeout(Duration::from_secs(6), async {
            frame::write(&mut input,&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"worker-fixture","version":"1"}}}),socket::MAX_FRAME).await.unwrap();
            frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap();
            frame::write(&mut input,&json!({"jsonrpc":"2.0","method":"notifications/initialized"}),socket::MAX_FRAME).await.unwrap();
            frame::write(&mut input,&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),socket::MAX_FRAME).await.unwrap();
            let roster: Value=serde_json::from_slice(&frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
            let mut names: Vec<&str>=roster["result"]["tools"].as_array().unwrap().iter().map(|tool|tool["name"].as_str().unwrap()).collect();
            names.sort_unstable();
            assert_eq!(names,["notify_orchestrator","pool_post","pool_propose","pool_read","pool_vote"]);
            frame::write(&mut input,&json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"notify_orchestrator","arguments":{"request_id":"fixture-report","kind":"risk","message":"Fixture material finding"}}}),socket::MAX_FRAME).await.unwrap();
            let reply: Value=serde_json::from_slice(&frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
            assert_ne!(reply["result"]["isError"],true,"{reply}");
            assert!(reply.get("error").is_none(),"{reply}");
            std::fs::write("worker-receipt.json",reply.to_string()).unwrap();
            drop(input);
            assert!(child.wait().await.unwrap().success());
        }).await.expect("bounded worker MCP fixture");
    });
}
/// Scripted pool member over the real private worker MCP boundary: confirms the
/// five-tool catalog, reads the pool log, the lead proposes, every member votes
/// ready on the current proposal, and the child then exits. Bounded to twenty
/// seconds so a lost peer still ends this child finitely.
fn pool_script(args: &[String], lead: bool) {
    let config: Value = serde_json::from_str(
        &std::fs::read_to_string(argument(args, "--mcp-config").expect("worker config path"))
            .unwrap(),
    )
    .unwrap();
    let server = &config["mcpServers"]["agent_run_worker"];
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        use agent_run::transport::{frame, socket};
        use std::process::Stdio;
        use tokio::io::BufReader;
        let mut child = tokio::process::Command::new(server["command"].as_str().unwrap())
            .args(server["args"].as_array().unwrap().iter().map(|arg| arg.as_str().unwrap()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        tokio::time::timeout(Duration::from_secs(20), async {
            let mut next_id = 0u64;
            let mut request = |method: &str, params: Value| {
                next_id += 1;
                json!({"jsonrpc":"2.0","id":next_id,"method":method,"params":params})
            };
            frame::write(&mut input, &request("initialize", json!({"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"pool-fixture","version":"1"}})), socket::MAX_FRAME).await.unwrap();
            frame::read(&mut output, socket::MAX_FRAME).await.unwrap().unwrap();
            frame::write(&mut input, &json!({"jsonrpc":"2.0","method":"notifications/initialized"}), socket::MAX_FRAME).await.unwrap();
            frame::write(&mut input, &request("tools/list", json!({})), socket::MAX_FRAME).await.unwrap();
            let roster: Value = serde_json::from_slice(&frame::read(&mut output, socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
            let mut names: Vec<_> = roster["result"]["tools"].as_array().unwrap().iter().map(|t| t["name"].as_str().unwrap().to_owned()).collect();
            names.sort();
            assert_eq!(names, ["notify_orchestrator", "pool_post", "pool_propose", "pool_read", "pool_vote"]);
            let mut call = |name: &str, arguments: Value| {
                request("tools/call", json!({"name":name,"arguments":arguments}))
            };
            let (mut proposed, mut voted) = (false, false);
            let mut transcript = String::new();
            while !voted {
                frame::write(&mut input, &call("pool_read", json!({"after_seq":0,"limit":50,"wait_seconds":1})), socket::MAX_FRAME).await.unwrap();
                let reply: Value = serde_json::from_slice(&frame::read(&mut output, socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
                let text = reply["result"]["content"][0]["text"].as_str().unwrap_or_default().to_owned();
                transcript.push_str(&text);
                let proposal = text.split("Current proposal #").nth(1).and_then(|rest| {
                    rest.chars().take_while(char::is_ascii_digit).collect::<String>().parse::<u64>().ok()
                });
                match proposal {
                    None if lead && !proposed => {
                        frame::write(&mut input, &call("pool_propose", json!({"request_id":"fixture-proposal","message":"fixture result is ready","snapshot":"commit abc123"})), socket::MAX_FRAME).await.unwrap();
                        let reply: Value = serde_json::from_slice(&frame::read(&mut output, socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
                        assert_ne!(reply["result"]["isError"], true, "{reply}");
                        proposed = true;
                    }
                    Some(seq) => {
                        frame::write(&mut input, &call("pool_vote", json!({"request_id":"fixture-vote","proposal_seq":seq,"decision":"ready","checks":[{"criterion_id":"goal","status":"met","evidence":"fixture verified"}]})), socket::MAX_FRAME).await.unwrap();
                        let reply: Value = serde_json::from_slice(&frame::read(&mut output, socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
                        assert_ne!(reply["result"]["isError"], true, "{reply}");
                        voted = true;
                    }
                    None => {}
                }
            }
            std::fs::write(if lead { "pool-script-lead.txt" } else { "pool-script-peer.txt" }, transcript).unwrap();
            drop(input);
            assert!(child.wait().await.unwrap().success());
        })
        .await
        .expect("bounded pool member fixture");
    });
}

/// Keeps an independent fixture active across mixed admission, reads its new
/// live membership through the already running private MCP, and posts the exact
/// broker challenge with a useful summary. The native work/session is unchanged.
/// All I/O and the owned MCP child lifetime are finite; only test markers persist.
fn pool_enroll(args: &[String]) {
    let config: Value = serde_json::from_str(
        &std::fs::read_to_string(argument(args, "--mcp-config").expect("worker config path"))
            .unwrap(),
    )
    .unwrap();
    let server = &config["mcpServers"]["agent_run_worker"];
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        use agent_run::transport::{frame,socket};
        use std::process::Stdio;
        let mut child=tokio::process::Command::new(server["command"].as_str().unwrap())
            .args(server["args"].as_array().unwrap().iter().map(|a|a.as_str().unwrap()))
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).spawn().unwrap();
        let mut input=child.stdin.take().unwrap();
        let mut output=tokio::io::BufReader::new(child.stdout.take().unwrap());
        let result=tokio::time::timeout(Duration::from_secs(20),async {
            frame::write(&mut input,&json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"enrollment-fixture","version":"1"}}}),socket::MAX_FRAME).await.unwrap();
            frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap();
            frame::write(&mut input,&json!({"jsonrpc":"2.0","method":"notifications/initialized"}),socket::MAX_FRAME).await.unwrap();
            frame::write(&mut input,&json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),socket::MAX_FRAME).await.unwrap();
            let listed:Value=serde_json::from_slice(&frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
            assert_eq!(listed["result"]["tools"].as_array().unwrap().len(),5);
            std::fs::write("enrollment-worker-ready","five-tool MCP initialized").unwrap();
            let mut id=2;
            loop {
                id+=1;
                frame::write(&mut input,&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"pool_read","arguments":{"after_seq":0,"limit":50}}}),socket::MAX_FRAME).await.unwrap();
                let reply:Value=serde_json::from_slice(&frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
                let text=reply["result"]["content"][0]["text"].as_str().unwrap_or_default();
                // This is an offline model emulator interpreting its private
                // tool result, never a production broker control parser.
                let key=text.split("request_id=").nth(1).map(|s|s.split_whitespace().next().unwrap());
                if let Some(key)=key {
                    id+=1;
                    frame::write(&mut input,&json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":"pool_post","arguments":{"request_id":key,"message":"I read the pool context and continue the original independent review."}}}),socket::MAX_FRAME).await.unwrap();
                    let ack:Value=serde_json::from_slice(&frame::read(&mut output,socket::MAX_FRAME).await.unwrap().unwrap()).unwrap();
                    assert_ne!(ack["result"]["isError"],true,"{ack}");
                    assert!(ack.get("error").is_none(),"{ack}");
                    std::fs::write("enrollment-acked","current attempt acknowledged").unwrap();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
            while !std::path::Path::new("enrollment-release").exists() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await;
        drop(input);
        if result.is_err() { let _=child.kill().await; }
        if tokio::time::timeout(Duration::from_secs(2),child.wait()).await.is_err() {
            child.kill().await.unwrap();child.wait().await.unwrap();
        }
        result.expect("bounded active enrollment fixture");
    });
}

/// Blocks until the marker file exists in the current workdir, bounded to
/// twenty seconds so a lost test driver still ends this child finitely.
fn wait_marker(name: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !std::path::Path::new(name).exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Runs one offline native-protocol scenario selected only by fixture task text.
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
    // Pool fixtures record the exact first-turn text this child received, as
    // proof that every peer identity was already committed when it started.
    if task.contains("fixture:pool-observe") {
        std::fs::write("pool-observed.txt", &task).expect("fixture pool record");
    }
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
        // Capture handshake. The supervisor records a descendant only from a
        // refresh made while this leader lives, and refreshes at the top of each
        // stdout read once 200 ms have passed since the last one. After this
        // pause any earlier refresh is stale, so the read that returns frame
        // `ready` (which begins only after frame `warmup` was processed) must
        // refresh with the helper present. The leader then stays alive until the
        // owning test has seen `ready` journaled and releases it, bounded to
        // twenty seconds so a lost driver still ends this child finitely.
        std::thread::sleep(Duration::from_millis(250));
        for text in [
            "fixture escaped warmup\n",
            "fixture escaped capture ready\n",
        ] {
            emit(
                json!({"type":"assistant","session_id":session,"message":{"content":[{"type":"text","text":text}]}}),
            );
        }
        wait_marker("escaped-release");
    }
    if task == "fixture:missing-result" {
        return;
    }
    if task == "fixture:slow" {
        std::thread::sleep(Duration::from_secs(3));
    }
    if task == "fixture:pool-enroll" {
        pool_enroll(&args);
    }
    if task.contains("fixture:pool-script") {
        pool_script(&args, task.contains("fixture:pool-script lead"));
    }
    if task == "fixture:worker-notify" {
        worker_report(&args);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !std::path::Path::new("worker-continue").exists()
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(20));
        }
    }
    if task == "fixture:follow-tools" {
        // Phase one waits for the driver's marker, then one native tool
        // invocation is streamed so a follow viewer observes a tool-count
        // change; phase two waits for the release marker before the turn's
        // normal completion path runs.
        wait_marker("follow-tools");
        emit(
            json!({"type":"stream_event","event":{"type":"content_block_start","content_block":{"type":"tool_use","id":"toolu_1","name":"shell"}}}),
        );
        emit(
            json!({"type":"assistant","session_id":session,"message":{"id":"msg_one","role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"shell","input":{"cmd":"ls"}}]}}),
        );
        emit(
            json!({"type":"user","session_id":session,"message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","is_error":false,"content":"file.txt"}]}}),
        );
        wait_marker("follow-release");
    }
    native_history(&session, &task);
    // A Claude Code 2.1.280 protocol frame rejecting a usage window, and the
    // same JSON merely quoted in assistant text (which must not count).
    let quota_frame = json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","rateLimitType":"five_hour","resetsAt":agent_run::domain::now() + 3600.0},"uuid":"fixture-uuid","session_id":session});
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
    if task == "fixture:result-then-hang" {
        // A complete, valid success result followed by a root process that keeps
        // its stdout open and never exits on its own accord: only the run
        // deadline's cleanup should end it. The twenty second ceiling is a
        // safety net so a failed cleanup still ends this child finitely.
        let ceiling = Instant::now() + Duration::from_secs(20);
        while Instant::now() < ceiling {
            std::thread::sleep(Duration::from_millis(50));
        }
    }
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
/// thread id) and `turn/start`. Built-in workspace profiles and legacy write
/// modes echo the matching filesystem mode without widening read-only grants.
/// It keeps a Codex-shaped rollout in
/// `$CODEX_HOME/sessions/.../rollout-fixture-<thread>.jsonl` with the turn
/// input as the installed Codex records it: a user `message` whose
/// `input_text` is the exact wire input, tagged with the turn id in
/// `internal_chat_message_metadata_passthrough`. Turn ids are unique per
/// process and turn, like real ones. A turn fails with the authoritative
/// `usageLimitExceeded` code when the linked `$CODEX_HOME/auth.json`
/// contains `exhausted`; with `early` as well, it is rejected before the
/// user input is recorded (meta-only history), and with `rewrite` the whole
/// rollout is replaced by a structurally valid meta-only file before that
/// failure (earlier turns vanish). Otherwise it completes with an agent
/// message naming the thread. The task marker `fixture:usage` also emits native
/// cumulative token counters scaled by the number of recorded user turns;
/// resumed processes therefore preserve the thread total without claiming a
/// native turn counter.
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
                if params["permissions"].as_str() == Some(":workspace")
                    || params["sandbox"].as_str() == Some("workspace-write")
                {
                    echo["sandbox"] = json!({
                        "type": "workspaceWrite",
                        "writableRoots": [],
                        "networkAccess": params.pointer("/config/sandbox_workspace_write/network_access")
                            .and_then(Value::as_bool).unwrap_or(false)
                    });
                }
                if let Some(profile) = params["permissions"].as_str() {
                    echo["activePermissionProfile"] = json!({"id":profile});
                }
                if let Some(reviewer) = params.get("approvalsReviewer") {
                    echo["approvalsReviewer"] = reviewer.clone();
                }
                emit(json!({"id":id,"result":echo}));
            }
            "turn/start" => {
                // `slow` spends 3 s of the deadline before account switching,
                // leaving a clear gap between the remainder and a fresh budget.
                if std::fs::read_to_string(home.join("auth.json"))
                    .map(|text| text.contains("slow"))
                    .unwrap_or(false)
                {
                    std::thread::sleep(Duration::from_millis(3000));
                }
                turns += 1;
                let turn = format!("turn-{}-{turns}", std::process::id());
                let input = params["input"][0]["text"].as_str().unwrap_or("").to_owned();
                let rollout = rollout_path(&home, &thread);
                let early = exhausted
                    && std::fs::read_to_string(home.join("auth.json"))
                        .map(|text| text.contains("early"))
                        .unwrap_or(false);
                if !early {
                    append(
                        &rollout,
                        &json!({"type":"response_item","payload":{"type":"message","role":"user",
                            "content":[{"type":"input_text","text":input}],
                            "internal_chat_message_metadata_passthrough":{"turn_id":turn}}}),
                    );
                }
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
                    if std::fs::read_to_string(home.join("auth.json"))
                        .map(|text| text.contains("rewrite"))
                        .unwrap_or(false)
                    {
                        std::fs::write(
                            &rollout,
                            format!(
                                "{}\n",
                                json!({"type":"session_meta","payload":{"id":thread}})
                            ),
                        )
                        .expect("rewrite rollout");
                    }
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
                    if input.contains("fixture:usage") {
                        let history = std::fs::read_to_string(&rollout).expect("usage rollout");
                        let observed = history
                            .lines()
                            .filter(|line| {
                                serde_json::from_str::<Value>(line)
                                    .ok()
                                    .is_some_and(|entry| entry["payload"]["role"] == "user")
                            })
                            .count() as u64;
                        emit(json!({"method":"thread/tokenUsage/updated","params":{
                            "threadId":thread,"tokenUsage":{"total":{
                                "inputTokens":100 * observed,"outputTokens":20 * observed,
                                "cachedInputTokens":10 * observed,"reasoningOutputTokens":5 * observed,
                                "totalTokens":120 * observed
                            }}
                        }}));
                    }
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
