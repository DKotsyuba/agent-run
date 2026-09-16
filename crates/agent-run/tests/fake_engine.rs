#![cfg(feature = "test-fixtures")]
//! Fault-controlled fake-engine acceptance tests (task M27).
//!
//! None of these tests need the resident broker or its Unix socket: they
//! admit a row into the durable store directly (the same preparation
//! `Service::start` performs) and then drive `agent_run::supervisor::run`
//! by spawning the real `agent-run _supervisor <id>` subcommand as a plain
//! child process, exactly as `supervisor::launch` would. That keeps them
//! runnable in a sandbox that cannot open Unix sockets. Broker/CLI-restart
//! lifecycle behavior is already covered by `tests/end_to_end.rs`, which
//! does need the broker.
//!
use agent_run::{
    adapters,
    config::Config,
    domain::{AgentId, StartRequest, Status},
    policy,
    process::{OwnedProcess, ProcessState},
    profiles,
    service::{LaunchIdentity, Service},
    state::{Record, Store},
    verify,
};
use serde_json::json;
use std::{
    collections::BTreeSet,
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, time::Instant};

/// Open one close-on-exec descriptor above the bootstrap descriptor range.
fn bootstrap_fd() -> OwnedFd {
    let file = std::fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/null")
        .expect("open bootstrap descriptor");
    // SAFETY: F_DUPFD_CLOEXEC duplicates the test-owned descriptor at a number
    // above the three bootstrap targets; the returned descriptor has one owner.
    let duplicated = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 10) };
    assert!(duplicated >= 0, "duplicate bootstrap descriptor");
    // SAFETY: fcntl returned a fresh descriptor that is not owned elsewhere.
    unsafe { OwnedFd::from_raw_fd(duplicated) }
}

/// Builds a fixture home with the explicitly provisioned profile Python init omits.
fn home() -> (tempfile::TempDir, PathBuf) {
    let temp = tempfile::Builder::new()
        .prefix("ar-fe-")
        .tempdir_in(std::env::var_os("AGENT_RUN_TEST_TMP").unwrap_or_else(|| "/tmp".into()))
        .unwrap();
    let home = temp.path().canonicalize().unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_agent-run"))
        .args(["--home", home.to_str().unwrap(), "init"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let config = format!(
        "schema_version=1\n[runtimes.mock]\nenabled=true\nadapter='claude'\nbinary={}\nhome={}\nmodels=['fixture']\nlimits_source='none'\n",
        toml::Value::String(env!("CARGO_BIN_EXE_agent-run-fixture").into()),
        toml::Value::String(home.join("runtime").to_string_lossy().into_owned()),
    );
    std::fs::write(home.join("config.toml"), config).unwrap();
    std::fs::create_dir(home.join("profiles")).unwrap();
    std::fs::write(home.join("profiles/review.md"), "Review.\n").unwrap();
    (temp, home)
}

/// Mirrors `Service::start`'s preparation, stopping short of
/// `supervisor::launch` (which respawns `current_exe()` — the test binary,
/// not `agent-run`, inside `cargo test`). Tests spawn the real `_supervisor`
/// subcommand themselves instead; see `spawn_supervisor`.
fn admit(home: &Path, task: &str) -> AgentId {
    let mut request = StartRequest {
        runtime: "mock".into(),
        model: "fixture".into(),
        profile: "review".into(),
        task: task.into(),
        workdir: home.to_path_buf(),
        write: false,
        fast: false,
        effort: None,
        timeout_seconds: None,
        read_roots: vec![],
        output_schema: None,
        orchestrator: None,
        request_id: None,
        account: None,
        required_constraints: BTreeSet::new(),
    };
    request.validate().unwrap();
    let config = Config::load(home).unwrap();
    let runtime = config.runtime(&request.runtime).unwrap();
    request.account = runtime
        .selected_account(request.account.as_deref())
        .unwrap();
    request.timeout_seconds = Some(
        request
            .timeout_seconds
            .unwrap_or(config.core.default_timeout_seconds),
    );
    let profile = profiles::load(&config, runtime, &request).unwrap();
    request.write = profile.write;
    request.read_roots = profile.read_roots.clone();
    request.required_constraints = profile.required_constraints.clone();
    adapters::validate(&request, runtime, &profile).unwrap();
    let policy = policy::evaluate(&request.runtime, runtime, &profile);
    policy.admit().unwrap();
    let identity = LaunchIdentity {
        rust_identity_version: 1,
        replay_request_sha256: None,
        config: config.clone(),
        profile,
        effective_policy: policy,
        runtime_home: None,
        snapshot_sha256: None,
    };
    let mut store = Store::open(home).unwrap();
    let (id, created) = store
        .admit(
            &request,
            &config,
            &serde_json::to_value(identity).unwrap(),
            None,
        )
        .unwrap();
    assert!(created, "expected a freshly admitted row");
    id
}

/// Spawns the real `agent-run` binary's hidden `_supervisor` subcommand
/// directly, the same process `supervisor::launch` spawns in production.
/// No socket of any kind is involved.
fn spawn_supervisor(home: &Path, id: &AgentId) -> tokio::process::Child {
    use std::os::unix::{io::AsRawFd, process::CommandExt};

    let ready = bootstrap_fd();
    let identity = bootstrap_fd();
    let error = bootstrap_fd();
    let mut command = Command::new(env!("CARGO_BIN_EXE_agent-run"));
    command
        .arg("--home")
        .arg(home)
        .args([
            "_supervisor",
            "--ready-fd",
            "3",
            "--identity-fd",
            "4",
            "--error-fd",
            "5",
            id.as_str(),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    let sources = [ready.as_raw_fd(), identity.as_raw_fd(), error.as_raw_fd()];
    // SAFETY: this test-only pre-exec closure captures only raw descriptors and
    // invokes the async-signal-safe `setsid` and `dup2` syscalls before exec.
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if libc::setsid() < 0 {
                return Err(std::io::Error::last_os_error());
            }
            for (source, target) in sources.into_iter().zip([3, 4, 5]) {
                if libc::dup2(source, target) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
            }
            Ok(())
        });
    }
    command
        .kill_on_drop(true)
        .spawn()
        .expect("spawn supervisor")
}

async fn wait_status(home: &Path, id: &AgentId, status: Status, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let row = Store::open(home).unwrap().get(id).unwrap();
        if row.status == status {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "agent {id} never reached {status:?} (last seen {:?})",
            row.status
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Runs one task to completion, spawning and awaiting the real supervisor
/// subprocess directly.
async fn run_task(home: &Path, task: &str) -> Record {
    let id = admit(home, task);
    let mut child = spawn_supervisor(home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor subprocess timed out")
        .expect("wait on supervisor subprocess");
    assert!(status.success(), "supervisor subprocess failed: {status:?}");
    Store::open(home).unwrap().get(&id).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn normal_run_produces_verifiable_terminal_evidence() {
    let (_tmp, home) = home();
    let row = run_task(&home, "hello").await;
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(row.exit_code, Some(0));
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&row.id, "process_cleanup")
        .unwrap()
        .expect("process_cleanup event recorded");
    assert_eq!(cleanup["confirmed"], json!(true));
    let answer = Service::new(home.clone()).answer(&row.id).unwrap();
    assert_eq!(answer["available"], json!(true));
    assert_eq!(answer["content"], json!("fixture final answer\n"));
}

/// Mirrors Python `adapters/claude/stream.py:335-349` (`StreamDecoder.finalize`)
/// and `adapters/claude/session.py:344-349`: the fixture engine emits one
/// "assistant" text line before exiting 0 with no terminal "result" line
/// (`crates/agent-run/tests/fixtures/engine.rs:58-61,101-103`), so
/// `_saw_assistant_text` is true and `finalize()` reports subtype `"cut_off"`
/// — a mid-turn cutoff, not the unrelated invented label `"missing_result"`
/// (never present in Python; verified live via `StreamDecoder(...).finalize()`
/// under python3.14, which returns `subtype="cut_off"` for this exact input).
/// A run with no streamed content at all instead classifies as `"no_answer"`
/// (`stream.py:344`), which Rust's own EOF branch also distinguishes
/// (`crates/agent-run-core/src/stream.rs:310-321`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_zero_without_terminal_result_is_not_success() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:missing-result").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("cut_off"));
    let answer = Service::new(home.clone()).answer(&row.id).unwrap();
    assert_eq!(answer["available"], json!(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nonzero_exit_after_valid_result_is_marked_failed() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:nonzero-after-result").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("nonzero_exit"));
    assert_eq!(row.exit_code, Some(3));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truncated_frame_fails_the_run() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:truncated").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("engine_transport_failure")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_utf8_frame_fails_the_run() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:invalid-utf8").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("malformed_engine_json"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_frame_fails_the_run() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:oversized").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("engine_transport_failure")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_start_delays_admission_then_succeeds_with_a_late_answer() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:slow-start");
    let mut child = spawn_supervisor(&home, &id);
    // The engine sleeps 2s before writing anything at all. Well before that,
    // the answer must not exist yet: the artifact is sealed only on
    // completion, never speculatively.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let early = Store::open(&home).unwrap().get(&id).unwrap();
    assert!(!early.status.terminal(), "finished suspiciously fast");
    assert!(early.answer_path.is_none());
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::Succeeded);
    assert!(
        row.answer_path.is_some(),
        "answer sealed only after the delay"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descendant_process_is_reaped_before_finish() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:descendant").await;
    assert_eq!(row.status, Status::Succeeded);
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&row.id, "process_cleanup")
        .unwrap()
        .expect("process_cleanup event recorded");
    // A surviving descendant must be positively confirmed gone, not assumed
    // gone because the leader itself returned.
    assert_eq!(cleanup["confirmed"], json!(true));
    assert_eq!(cleanup["descendants_gone"], json!(true));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_stops_a_hanging_engine_and_records_terminal_state() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:hang");
    let mut child = spawn_supervisor(&home, &id);
    wait_status(&home, &id, Status::Running, Duration::from_secs(10)).await;
    Service::new(home.clone()).cancel(&id).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::Cancelled);
    let answer = Service::new(home.clone()).answer(&id).unwrap();
    assert_eq!(answer["available"], json!(false));
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&id, "process_cleanup")
        .unwrap()
        .expect("process_cleanup event recorded");
    // Plain SIGTERM is enough for a well-behaved engine; SIGKILL should not
    // have been necessary.
    assert_eq!(cleanup["signals"], json!(["SIGTERM"]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_ignoring_sigterm_requires_sigkill() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:ignore-sigterm");
    let mut child = spawn_supervisor(&home, &id);
    wait_status(&home, &id, Status::Running, Duration::from_secs(10)).await;
    Service::new(home.clone()).cancel(&id).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    let row = Store::open(&home).unwrap().get(&id).unwrap();
    assert_eq!(row.status, Status::Cancelled);
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&id, "process_cleanup")
        .unwrap()
        .expect("process_cleanup event recorded");
    assert_eq!(cleanup["signals"], json!(["SIGTERM", "SIGKILL"]));
    assert_eq!(cleanup["confirmed"], json!(true));
}

/// Mirrors Python `verify.py:505` (`read_answer_payload`), which raises
/// `AnswerTamperedError` — an `AnswerError` subclass, `verify.py:70` — for
/// exactly this message, `"answer artifact hash does not match its recorded
/// proof"`. The answer-proof work landed on this branch tonight
/// (`b7553f2`, "verify and seal answer proofs against the Python corpus")
/// and moved `verify::read`'s hash-mismatch branch from the old generic
/// `Error::Integrity` to `Error::AnswerIntegrity`
/// (`crates/agent-run-platform/src/verify/mod.rs:242-245`), which is also
/// the variant every other answer/proof check in that module now uses.
/// `crates/agent-run-domain/src/error.rs:92-97` documents the direction
/// explicitly: `AnswerIntegrity` is the live variant, `Integrity` is
/// "retained temporarily for existing migration callers" — both still map
/// to the same public `AnswerIntegrityError` machine code
/// (`error.rs:134`), so no caller-visible behavior changed, only the
/// internal variant this test must name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_answer_proof_is_rejected_even_after_a_real_seal() {
    let (_tmp, home) = home();
    let row = run_task(&home, "hello").await;
    assert_eq!(row.status, Status::Succeeded);
    let path = row.answer_path.clone().expect("answer path recorded");
    let proof = verify::Proof {
        path: path.clone(),
        bytes: row.answer_bytes.unwrap(),
        sha256: row.answer_sha256.clone().unwrap(),
        proof_version: 2,
    };
    // A valid, unmodified proof reads back cleanly first.
    let root = home.join("agents").join(row.id.as_str());
    verify::read(&root, &proof, verify::INLINE_ANSWER).expect("valid proof reads back");
    // Corrupt the sealed bytes on disk (e.g. partial disk write, tampering)
    // without updating the recorded proof: the digest mismatch must be
    // caught, never silently accepted as the recorded answer.
    std::fs::write(&path, b"corrupted answer bytes").unwrap();
    let error = verify::read(&root, &proof, verify::INLINE_ANSWER)
        .expect_err("corrupted bytes must not verify");
    assert!(matches!(error, agent_run::Error::AnswerIntegrity(_)));
}

#[test]
fn a_surviving_process_group_is_never_reported_as_finished() {
    let outcome = agent_run::domain::Outcome::success(None);
    let proof = verify::AnswerProof {
        path: PathBuf::from("/tmp/does-not-matter.md"),
        exists: true,
        size_bytes: 4,
        sha256: Some("deadbeef".into()),
        sentinel_found: true,
        proof_version: 2,
        proof_error: None,
    };
    // Even a clean success outcome with a present proof must be downgraded
    // to failed the moment the process group did not actually go away.
    let completed = verify::verify_completion(
        Some(outcome),
        None,
        Some(&proof),
        false,
        None,
        agent_run::domain::now(),
        verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
    )
    .expect("completion policy decides without I/O");
    assert_eq!(completed.status, Status::Failed);
    assert_eq!(
        completed.failure_kind.as_deref(),
        Some("engine_group_survived")
    );
}

#[test]
fn signal_is_never_sent_to_an_already_dead_process() {
    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_agent-run-fixture"))
        .arg("--child-sleep")
        .arg("1")
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let mut owned = OwnedProcess::capture(pid);
    child.wait().unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while agent_run::process::observe(
        Some(pid),
        owned.leader.as_ref().map(|p| p.token.as_str()),
        owned.leader.as_ref().map(|p| p.birth),
    ) != ProcessState::Dead
    {
        assert!(std::time::Instant::now() < deadline, "child never reaped");
        std::thread::sleep(Duration::from_millis(10));
    }
    // The identity/token check must refuse to signal a PID that is no
    // longer the process it captured, even though the PID number itself
    // could already have been recycled by the OS by this point.
    let signalled = owned.signal(libc::SIGTERM).unwrap();
    assert!(!signalled, "must not signal a dead/possibly-reused PID");
}
