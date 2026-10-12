//! Real-descriptor and environment-scrub scenarios for the launch capability.
//!
//! This target has no libtest harness (`harness = false`). Run bare (as
//! `cargo test` does) it is only a launcher: it re-executes itself as the
//! single-threaded `inner` driver with `Command::env_clear()`, a minimal `PATH`
//! and ONLY synthetic sentinel values, so no real token, flag, home or
//! credential of the executing environment can reach any scenario, probe or
//! control child, and nothing in this process or its parent is mutated. The
//! `inner` role runs the scenarios in order and exits non-zero with a closed
//! message on the first failure; the `probe` role is the detached child it
//! launches by re-executing itself. Fixtures only ever test the PRESENCE of the
//! synthetic names and print counts or booleans, never an environment dump.
use agent_run_platform::launch::{self, Timeouts};
use std::{
    ffi::OsStr,
    io::{ErrorKind, Write},
    os::fd::{AsRawFd, IntoRawFd},
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

/// Synthetic environment given to the inner driver (and therefore to its
/// unscrubbed control child). None of these values is real; the last is an
/// unknown worker flag no allow-list could have anticipated.
const SENTINELS: [(&str, &str); 4] = [
    ("AGENT_RUN_HOME", "/synthetic/not-a-home"),
    ("AGENT_RUN_WORKER_BINDING", "synthetic-binding"),
    ("AGENT_RUN_WORKER_TOKEN", "synthetic-token-not-real"),
    ("AGENT_RUN_WORKER_UNKNOWN_SENTINEL_9", "synthetic-unknown"),
];

/// Entry point: the launcher (no arguments), the `inner` driver or the launched
/// `probe` role.
fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        None => launcher(),
        Some("inner") => driver(),
        Some("probe") => probe(&args),
        _ => fail("usage: launch_fd6"),
    }
}

/// Re-execute this binary as the inner driver in a controlled environment: the
/// whole environment is cleared, then only a minimal `PATH` and the synthetic
/// sentinels are set on the child `Command`. This process is never modified.
fn launcher() {
    let me = std::env::current_exe().unwrap_or_else(|_| fail("current_exe"));
    let mut command = Command::new(me);
    command
        .arg("inner")
        .env_clear()
        .env("PATH", "/usr/bin:/bin");
    for (name, value) in SENTINELS {
        command.env(name, value);
    }
    let output = command.output().unwrap_or_else(|_| fail("inner spawn"));
    print!("{}", String::from_utf8_lossy(&output.stdout));
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    if !output.status.success() {
        fail("inner driver failed");
    }
}

/// Print `message` and exit non-zero.
fn fail(message: &str) -> ! {
    eprintln!("launch_fd6: {message}");
    std::process::exit(1);
}

/// Value following `flag` in `args`, parsed as a descriptor number.
fn flag(args: &[String], name: &str) -> i32 {
    let at = args
        .iter()
        .position(|a| a == name)
        .unwrap_or_else(|| fail("missing descriptor flag"));
    args[at + 1]
        .parse()
        .unwrap_or_else(|_| fail("bad descriptor flag"))
}

/// Launched child: block reading the capability while a concurrent exec checks
/// whether descriptor 6 leaks, then report the outcome and READY.
fn probe(args: &[String]) {
    let result = Path::new(&args[2]).to_owned();
    let (ready, identity, error, capability) = (
        flag(args, "--ready-fd"),
        flag(args, "--identity-fd"),
        flag(args, "--error-fd"),
        flag(args, "--capability-fd"),
    );
    launch::report_identity(identity, error).unwrap_or_else(|_| fail("identity"));
    let reader =
        std::thread::spawn(move || launch::read_capability(capability, Duration::from_secs(10)));
    // Give the reader time to be blocked on the still-open parent pipe end.
    std::thread::sleep(Duration::from_millis(300));
    let leaked = Command::new("/bin/sh")
        .args([
            "-c",
            "if [ -e /dev/fd/6 ]; then printf inherited; else printf closed; fi",
        ])
        .output()
        .unwrap_or_else(|_| fail("probe exec"));
    let secret = reader.join().unwrap_or_else(|_| fail("reader"));
    let text = format!(
        "{}|{}",
        String::from_utf8_lossy(&leaked.stdout),
        secret.map_or(0, |s| s.len())
    );
    std::fs::write(&result, text).unwrap_or_else(|_| fail("result"));
    launch::report_ready(ready, Ok(())).unwrap_or_else(|_| fail("ready"));
}

/// Short deadlines so a failure is observed quickly.
fn timeouts() -> Timeouts {
    Timeouts {
        ready: Duration::from_secs(20),
        dispatch: Duration::from_millis(500),
        grace: Duration::from_millis(200),
        kill: Duration::from_millis(200),
    }
}

/// Whether descriptor `fd` is closed in this process.
fn closed(fd: i32) -> bool {
    // SAFETY: F_GETFD only probes whether the descriptor is open.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    flags == -1
}

/// Run every scenario in the controlled inner environment.
fn driver() {
    for (name, value) in SENTINELS {
        if std::env::var(name).as_deref() != Ok(value) {
            fail("the inner environment must hold exactly the synthetic sentinels");
        }
    }
    let ambient = std::env::vars_os()
        .filter(|(key, _)| key.to_string_lossy().starts_with("AGENT_RUN"))
        .count();
    if ambient != SENTINELS.len() {
        fail("the inner environment holds a non-synthetic AGENT_RUN value");
    }
    env_scrub_scenario();
    exec_does_not_inherit_capability();
    receiver_closure_scenarios();
    println!("launch_fd6: all scenarios passed");
}

/// Shell body that records, for each name, only whether it is set (never a
/// value), then the count of `AGENT_RUN*` variables, and reports READY.
const PRESENCE_SCRIPT: &str = r#"for n in AGENT_RUN_HOME AGENT_RUN_WORKER_BINDING AGENT_RUN_WORKER_TOKEN AGENT_RUN_WORKER_UNKNOWN_SENTINEL_9; do
  eval "if [ -n \"\${$n+x}\" ]; then echo $n=present; else echo $n=absent; fi"
done > "$1"
echo "count=$(env | grep -c '^AGENT_RUN')" >> "$1"
echo $$ >&4; exec 4>&-; echo ready >&3"#;

/// L3: a scrubbed child sees none of the synthetic AGENT_RUN* names while an
/// unscrubbed control child (inheriting only the synthetic inner environment)
/// sees all of them; only presence is recorded and this process is unchanged.
fn env_scrub_scenario() {
    let temp = tempfile::tempdir().unwrap_or_else(|_| fail("tempdir"));
    let scrubbed = temp.path().join("scrubbed");
    let launched = launch::launch_detached(
        Path::new("/usr/bin/env"),
        &[
            OsStr::new("-i"),
            OsStr::new("PATH=/usr/bin:/bin"),
            OsStr::new("/bin/sh"),
            OsStr::new("-c"),
            OsStr::new(PRESENCE_SCRIPT),
            OsStr::new("sh"),
            scrubbed.as_os_str(),
        ],
        timeouts(),
        |_, _| Ok(()),
    )
    .unwrap_or_else(|_| fail("scrubbed launch"));
    let _ = launch::spawn_reaper(launched.pid);
    wait_file(&scrubbed);
    wait_lines(&scrubbed, 5);
    let report = std::fs::read_to_string(&scrubbed).unwrap_or_default();
    for (name, _) in SENTINELS {
        if !report.contains(&format!("{name}=absent")) {
            fail("a scrubbed child must not see a synthetic AGENT_RUN name");
        }
    }
    if !report.contains("count=0") {
        fail("a scrubbed child must see no AGENT_RUN variable at all");
    }
    let control = temp.path().join("control");
    let launched = launch::launch_detached(
        Path::new("/bin/sh"),
        &[
            OsStr::new("-c"),
            OsStr::new(PRESENCE_SCRIPT),
            OsStr::new("sh"),
            control.as_os_str(),
        ],
        timeouts(),
        |_, _| Ok(()),
    )
    .unwrap_or_else(|_| fail("control launch"));
    let _ = launch::spawn_reaper(launched.pid);
    wait_file(&control);
    wait_lines(&control, 5);
    let report = std::fs::read_to_string(&control).unwrap_or_default();
    for (name, _) in SENTINELS {
        if !report.contains(&format!("{name}=present")) {
            fail(
                "the unscrubbed control must inherit the synthetic names (production propagation)",
            );
        }
    }
    if !report.contains(&format!("count={}", SENTINELS.len())) {
        fail("the control must hold exactly the synthetic AGENT_RUN variables");
    }
    for (name, value) in SENTINELS {
        if std::env::var(name).as_deref() != Ok(value) {
            fail("the driver environment changed");
        }
    }
}

/// Wait up to ten seconds until `path` holds at least `lines` lines.
fn wait_lines(path: &Path, lines: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::read_to_string(path).map_or(0, |t| t.lines().count()) < lines {
        if Instant::now() >= deadline {
            fail("timed out waiting for fixture lines");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Wait up to ten seconds for a non-empty file.
fn wait_file(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while std::fs::metadata(path).map_or(true, |m| m.len() == 0) {
        if Instant::now() >= deadline {
            fail("timed out waiting for fixture output");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// L2: a real inherited descriptor 6 is close-on-exec while the read is blocked.
fn exec_does_not_inherit_capability() {
    let temp = tempfile::tempdir().unwrap_or_else(|_| fail("tempdir"));
    let result = temp.path().join("result");
    let me = std::env::current_exe().unwrap_or_else(|_| fail("current_exe"));
    let mut started = launch::spawn_launch(
        &me,
        &[OsStr::new("probe"), result.as_os_str()],
        timeouts(),
        true,
    )
    .unwrap_or_else(|_| fail("spawn"));
    std::thread::sleep(Duration::from_millis(900));
    started
        .release_capability(b"0123456789ab")
        .unwrap_or_else(|_| fail("release"));
    let launched = started
        .complete(timeouts())
        .unwrap_or_else(|_| fail("complete"));
    let _ = launch::spawn_reaper(launched.pid);
    wait_file(&result);
    let text = std::fs::read_to_string(&result).unwrap_or_default();
    if text != "closed|12" {
        fail(&format!(
            "concurrent exec saw the capability descriptor: {text}"
        ));
    }
}

/// Receiver-side bounds: oversize, empty EOF, timeout and success all close fd 6.
fn receiver_closure_scenarios() {
    // Oversize secret.
    let (read, write) = launch::cloexec_pipe().unwrap_or_else(|_| fail("pipe"));
    let mut file = std::fs::File::from(write);
    file.write_all(&[b'a'; launch::CAPABILITY_MAX_BYTES + 1])
        .unwrap_or_else(|_| fail("write"));
    drop(file);
    let fd = read.into_raw_fd();
    match launch::read_capability(fd, Duration::from_secs(2)) {
        Err(e) if e.kind() == ErrorKind::InvalidData && closed(fd) => {}
        _ => fail("oversize secret must be refused and closed"),
    }
    // Closed without a secret.
    let (read, write) = launch::cloexec_pipe().unwrap_or_else(|_| fail("pipe"));
    drop(write);
    let fd = read.into_raw_fd();
    match launch::read_capability(fd, Duration::from_secs(2)) {
        Err(e) if e.kind() == ErrorKind::UnexpectedEof && closed(fd) => {}
        _ => fail("empty EOF must be refused and closed"),
    }
    // Never released.
    let (read, write) = launch::cloexec_pipe().unwrap_or_else(|_| fail("pipe"));
    let fd = read.into_raw_fd();
    match launch::read_capability(fd, Duration::from_millis(150)) {
        Err(e) if e.kind() == ErrorKind::TimedOut && closed(fd) => {}
        _ => fail("an unreleased capability must time out and close"),
    }
    drop(write);
    // Success.
    let (read, write) = launch::cloexec_pipe().unwrap_or_else(|_| fail("pipe"));
    let mut file = std::fs::File::from(write);
    file.write_all(b"abc").unwrap_or_else(|_| fail("write"));
    drop(file);
    let fd = read.into_raw_fd();
    match launch::read_capability(fd, Duration::from_secs(2)) {
        Ok(secret) if secret == b"abc" && closed(fd) => {}
        _ => fail("a valid secret must be returned and the descriptor closed"),
    }
    // Stdio descriptors are never adopted.
    match launch::read_capability(2, Duration::from_millis(10)) {
        Err(e) if e.kind() == ErrorKind::InvalidInput && !closed(2) => {}
        _ => fail("stdio must be refused without closing it"),
    }
    let _ = std::io::stdout().as_raw_fd();
}
