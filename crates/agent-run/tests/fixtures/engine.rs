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
        #[allow(clippy::zombie_processes)]
        let child = std::process::Command::new(exe)
            .arg("--child-sleep")
            .arg("10")
            .process_group(0)
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
    let failed = task == "fixture:error";
    emit(
        json!({"type":"result","subtype":if failed{"error_during_execution"}else{"success"},"is_error":failed,"session_id":session,"result":if failed{"fixture failure"}else{"fixture final answer\n"},"usage":{"input_tokens":2,"output_tokens":3},"num_turns":1}),
    );
    if task == "fixture:nonzero-after-result" {
        io::stdout().flush().expect("fixture stdout");
        std::process::exit(3);
    }
}
