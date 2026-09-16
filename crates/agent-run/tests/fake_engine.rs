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
    admit_runtime(home, "mock", task)
}

/// Admit one fixture task against a named configured runtime.
fn admit_runtime(home: &Path, runtime_name: &str, task: &str) -> AgentId {
    let mut request = StartRequest {
        runtime: runtime_name.into(),
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

/// Removes the admitted mock runtime from one frozen launch identity.
///
/// This models a row accepted by an older process before its runtime was
/// removed. The profile remains intact so the real supervisor, after recording
/// ownership, reaches the runtime lookup as its first preparation failure.
fn remove_frozen_runtime(home: &Path, id: &AgentId) {
    let store = Store::open(home).unwrap();
    let mut identity = store.get(id).unwrap().identity.expect("launch identity");
    identity["config"]["runtimes"]
        .as_object_mut()
        .expect("runtime mapping")
        .remove("mock");
    store
        .update_identity(id, &identity, "pending:materialization")
        .unwrap();
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

/// Waits until the supervisor has persisted the engine's own runtime session id.
///
/// `Status::Running` is not engine readiness: `supervisor.rs:234` records it
/// immediately after `Process::spawn` returns, before the fixture engine has
/// execed, read its task from stdin, or changed any signal disposition. A test
/// that cancels on `Running` therefore races the engine's own startup.
///
/// The runtime session id is different: it is persisted only from the first
/// engine frame that carries `session_id` (`stream.rs:404-420`), which the
/// fixture emits at `tests/fixtures/engine.rs:58`. Observing it is a
/// happens-after proof that the fixture executed everything preceding that
/// emit — including the `SIGTERM`/`SIG_IGN` disposition change at
/// `tests/fixtures/engine.rs:51-57`. This mirrors the readiness-marker poll the
/// Python reference performs before cancelling a signal-ignoring child
/// (`tests/test_claude_session.py:455-471`).
///
/// `home` is the fixture home directory, `id` the admitted agent, and `timeout`
/// the budget allowed before readiness is treated as a failure. Polls the store
/// every 20ms and returns once the id is present; panics with the last observed
/// status when the deadline passes.
async fn wait_engine_ready(home: &Path, id: &AgentId, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let row = Store::open(home).unwrap().get(id).unwrap();
        if row.runtime_session_id.is_some() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "agent {id} never reported a runtime session (last status {:?}, failure {:?}: {:?})",
            row.status,
            row.failure_kind,
            row.failure_text
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Wait until the fixture records that the supervisor returned to engine polling.
async fn wait_engine_poll_marker(home: &Path, id: &AgentId) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let markers: i64 = Store::open(home)
            .unwrap()
            .conn
            .query_row(
                "SELECT COUNT(*) FROM messages WHERE agent_id=? AND content LIKE '%fixture poll marker%'",
                [id.as_str()],
                |row| row.get(0),
            )
            .unwrap();
        if markers > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "fixture never observed an engine poll"
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

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_early_exited_engine_succeeds_only_with_complete_answer_evidence`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_identity_is_durable_before_ready_and_the_group_refines_once`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_ready_follows_handlers_and_durable_starting_and_precedes_launch`.
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
    let store = Store::open(&home).unwrap();
    let (ready_seq, preparing_seq): (i64, i64) = store
        .conn
        .query_row(
            "SELECT \
                (SELECT seq FROM events WHERE agent_id=? AND kind='supervisor_ready'), \
                (SELECT seq FROM events WHERE agent_id=? AND kind='phase' AND json_extract(data_json,'$.phase')='preparing')",
            rusqlite::params![row.id.as_str(), row.id.as_str()],
            |value| Ok((value.get(0)?, value.get(1)?)),
        )
        .unwrap();
    assert!(ready_seq < preparing_seq, "READY must precede preparation");
    assert!(row.supervisor_pid.is_some());
    assert!(row.supervisor_birth_time.is_some());
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
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_early_exited_engine_success_without_an_answer_stays_failed`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_early_exited_engine_success_without_sentinel_stays_failed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exit_zero_without_terminal_result_is_not_success() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:missing-result").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("cut_off"));
    let answer = Service::new(home.clone()).answer(&row.id).unwrap();
    assert_eq!(answer["available"], json!(false));
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_early_exited_engine_keeps_nonzero_failure`.
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
    assert!(Store::open(&home)
        .unwrap()
        .last_event(&row.id, "process_cleanup")
        .unwrap()
        .is_some());
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_session_wait_exception_reaches_cleanup_and_durable_failure`
/// Rust's bounded process reader reports the wait/transport fault as an engine
/// result, then the supervisor performs the same cleanup and terminal commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_wait_failure_reaches_cleanup_and_durable_failure() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:truncated").await;
    assert_eq!(row.status, Status::Failed);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("engine_transport_failure")
    );
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&row.id, "process_cleanup")
        .unwrap()
        .expect("cleanup event");
    assert_eq!(cleanup["confirmed"], json!(true));
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_running_transition_exception_reaches_cleanup_and_failure`
/// The SQLite trigger is a test-only durable commit seam: production still uses
/// the concrete Store, while the real supervisor must clean up after `running`
/// is rejected and persist the resulting failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_transition_failure_reaches_cleanup_and_durable_failure() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:hang");
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "CREATE TRIGGER reject_running BEFORE UPDATE OF status ON agents WHEN NEW.status='running' BEGIN SELECT RAISE(ABORT,'running rejected'); END",
            [],
        )
        .unwrap();
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    let store = Store::open(&home).unwrap();
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Failed);
    assert_eq!(
        row.failure_kind.as_deref(),
        Some("runtime_transport_failed")
    );
    assert!(store.last_event(&id, "process_cleanup").unwrap().is_some());
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_commit_rejection_is_not_swallowed_while_agent_is_active`
/// A rejected terminal update must reach the supervisor entrypoint rather than
/// being converted into a successful child exit or an inactive row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_commit_rejection_is_not_swallowed_while_active() {
    let (_tmp, home) = home();
    let id = admit(&home, "hello");
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "CREATE TRIGGER reject_terminal BEFORE UPDATE OF status ON agents WHEN NEW.status IN ('succeeded','failed','timed_out','cancelled','lost') BEGIN SELECT RAISE(ABORT,'terminal rejected'); END",
            [],
        )
        .unwrap();
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(!status.success());
    let store = Store::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Running);
    assert!(store.last_event(&id, "process_cleanup").unwrap().is_some());
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

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_elapsed_clock_never_stops_a_runtime`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_natural_quiesce_allows_answer_flush_without_term`.
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

/// Mirrors `tests/test_supervisor.py::RunStatsSupervisorTests::test_a_terminal_commit_writes_the_run_stats_row`.
///
/// The terminal row and normalized runtime measurements must commit together;
/// querying the durable statistics table after the real child exits proves the
/// supervisor never leaves a completed run without its accounting row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_supervisor_commit_persists_runtime_statistics() {
    let (_tmp, home) = home();
    let row = run_task(&home, "hello").await;
    let store = Store::open(&home).unwrap();
    let stats: (String, i64, i64, i64, String) = store
        .conn
        .query_row(
            "SELECT status,input_tokens,output_tokens,num_turns,usage_source FROM run_stats WHERE agent_id=?",
            [row.id.as_str()],
            |value| Ok((value.get(0)?, value.get(1)?, value.get(2)?, value.get(3)?, value.get(4)?)),
        )
        .expect("terminal run-stat row");
    assert_eq!(
        stats,
        ("succeeded".into(), 2, 3, 1, "runtime_result".into())
    );
}

/// Mirrors `tests/test_supervisor.py::RunStatsSupervisorTests::test_a_stats_failure_still_returns_the_committed_outcome`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_supervisor_outcome_survives_a_stats_failure() {
    let (_tmp, home) = home();
    let id = admit(&home, "hello");
    let store = Store::open(&home).unwrap();
    store
        .conn
        .execute("ALTER TABLE run_stats RENAME TO run_stats_unavailable", [])
        .unwrap();
    drop(store);
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    assert_eq!(
        Store::open(&home).unwrap().get(&id).unwrap().status,
        Status::Succeeded
    );
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_a_failed_launch_is_durable_not_a_crash`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_startup_failure_reports_ready_failure_without_launch`.
/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_unknown_runtime_fails_durably_after_ready`.
///
/// The spawned entrypoint records its own identity before a frozen runtime
/// becomes unavailable. Its nonzero exit must therefore leave one durable
/// failed row, a READY ownership event, and no engine-spawn phase.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_frozen_runtime_fails_durably_after_supervisor_ownership() {
    let (_tmp, home) = home();
    let id = admit(&home, "hello");
    remove_frozen_runtime(&home, &id);
    let mut child = spawn_supervisor(&home, &id);
    let exit = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor subprocess timed out")
        .expect("wait on supervisor subprocess");
    assert!(
        !exit.success(),
        "preparation failure must reach the entrypoint"
    );
    let store = Store::open(&home).unwrap();
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Failed);
    assert_eq!(row.failure_kind.as_deref(), Some("prepare_runtime_failed"));
    assert!(
        row.supervisor_pid.is_some(),
        "ownership must be durable first"
    );
    assert!(store.last_event(&id, "supervisor_ready").unwrap().is_some());
    assert!(store.last_event(&id, "phase").unwrap().is_none());
}

/// Mirrors `tests/test_supervisor_main.py::SupervisorMainTests::test_ten_consecutive_exec_launches_all_land_durably`.
///
/// Keep a parent SQLite connection open while ten real `_supervisor` entrypoint
/// processes start and terminate. Every row must carry exactly one terminal
/// status event, proving repeated exec-based supervision does not lose durable
/// lifecycle writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ten_consecutive_supervisor_entrypoints_land_durably() {
    let (_tmp, home) = home();
    let parent = Store::open(&home).unwrap();
    for round in 0..10 {
        let row = run_task(&home, "hello").await;
        assert_eq!(row.status, Status::Succeeded, "round {round}");
        let terminal_events: i64 = parent
            .conn
            .query_row(
                "SELECT COUNT(*) FROM events WHERE agent_id=? AND to_status='succeeded'",
                [row.id.as_str()],
                |value| value.get(0),
            )
            .unwrap();
        assert_eq!(terminal_events, 1, "round {round}");
    }
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_a_grandchild_is_killed_and_reaped_after_a_clean_exit`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_missing_leader_does_not_hide_a_surviving_descendant`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_escaped_descendant_is_reported_and_cleaned_by_fixture_owner`.
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

/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_escaped_descendant_is_reported_and_cleaned_by_fixture_owner`.
///
/// Group cleanup must not signal an escaped descendant individually, and its
/// unconfirmed cleanup evidence must remain separate from the runtime result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn escaped_descendant_is_not_signalled_and_does_not_change_runtime_outcome() {
    let (_tmp, home) = home();
    let row = run_task(&home, "fixture:escaped-descendant").await;
    assert_eq!(row.status, Status::Succeeded);
    let cleanup = Store::open(&home)
        .unwrap()
        .last_event(&row.id, "process_cleanup")
        .unwrap()
        .expect("process_cleanup event recorded");
    assert_eq!(cleanup["scope"], json!("verified_descendants"));
    assert_eq!(cleanup["group_gone"], json!(true));
    assert_eq!(cleanup["descendants_gone"], json!(false));
    assert_eq!(cleanup["confirmed"], json!(false));
    let escaped: i32 = std::fs::read_to_string(home.join("escaped.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    // The supervisor sent only its verified group signal; the escaped child
    // remains alive until this test-owned fixture cleanup.
    // SAFETY: `escaped` is a positive PID this test's own fixture spawned and
    // recorded, so signal 0 addresses one process and never a group or wildcard.
    assert_eq!(unsafe { libc::kill(escaped, 0) }, 0);
    // SAFETY: same single owned PID, proven alive by the probe above; this test
    // owns the escaped child and is responsible for reaping it.
    assert_eq!(unsafe { libc::kill(escaped, libc::SIGKILL) }, 0);
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_cancel_queued_before_launch_cannot_orphan_the_engine`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_cancel_accepted_at_terminal_barrier_cannot_be_lost`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_final_drain_completes_late_cancel_steer_and_unknown`.
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

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_command_flood_yields_to_engine_poll_after_one_bounded_page`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_flood_yields_after_one_bounded_page() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:command-flood");
    let mut store = Store::open(&home).unwrap();
    for index in 0..40 {
        store
            .enqueue(&id, "steer", &json!({"text": format!("steer {index}")}))
            .unwrap();
    }
    drop(store);
    let mut child = spawn_supervisor(&home, &id);
    wait_engine_ready(&home, &id, Duration::from_secs(10)).await;
    wait_engine_poll_marker(&home, &id).await;
    let accepted: i64 = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND state='completed' AND json_extract(result_json,'$.accepted')=1",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(accepted, 16);
    Service::new(home.clone()).cancel(&id).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    let store = Store::open(&home).unwrap();
    let steer_accepted: i64 = store
        .conn
        .query_row(
            "SELECT COUNT(*) FROM commands WHERE agent_id=? AND kind='steer' AND json_extract(result_json,'$.accepted')=1",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(steer_accepted, 16);
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_command_time_budget_yields_before_the_count_limit`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_page_deadline_bounds_blocked_engine_writes() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:command-flood");
    let mut store = Store::open(&home).unwrap();
    let text = "x".repeat(512 * 1024);
    for _ in 0..16 {
        store.enqueue(&id, "steer", &json!({"text": text})).unwrap();
    }
    drop(store);
    let mut child = spawn_supervisor(&home, &id);
    wait_engine_ready(&home, &id, Duration::from_secs(10)).await;
    let all_completed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let completed: i64 = Store::open(&home)
                .unwrap()
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM commands WHERE agent_id=? AND state='completed'",
                    [id.as_str()],
                    |row| row.get(0),
                )
                .unwrap();
            if completed == 16 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    assert!(!all_completed, "one command page ignored its deadline");
    Service::new(home.clone()).cancel(&id).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_steer_commands_are_durably_answered_by_capability`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn steer_commands_are_answered_by_the_runtime_capability() {
    let (_tmp, home) = home();
    let config = std::fs::read_to_string(home.join("config.toml")).unwrap();
    std::fs::write(
        home.join("config.toml"),
        format!(
            "{config}\n[runtimes.qwen]\nenabled=true\nadapter='qwen'\nbinary='{}'\nhome='{}'\nmodels=['fixture']\nlimits_source='none'\n",
            env!("CARGO_BIN_EXE_agent-run-fixture"),
            home.join("qwen-runtime").display()
        ),
    )
    .unwrap();
    // The Qwen adapter validates its configured environment credential before
    // it reaches the fixture process; this value is never persisted.
    // SAFETY: this test-only credential is scoped to the fixture process
    // configuration and is not read by another test in this process.
    unsafe { std::env::set_var("OPENAI_API_KEY", "fixture-key") };
    let id = admit_runtime(&home, "qwen", "fixture:command-flood");
    let mut store = Store::open(&home).unwrap();
    store
        .enqueue(&id, "steer", &json!({"text":"focus"}))
        .unwrap();
    drop(store);
    let mut child = spawn_supervisor(&home, &id);
    wait_engine_ready(&home, &id, Duration::from_secs(10)).await;
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let state: Option<String> = Store::open(&home)
            .unwrap()
            .conn
            .query_row(
                "SELECT result_json FROM commands WHERE agent_id=?",
                [id.as_str()],
                |row| row.get(0),
            )
            .ok();
        if state.is_some() {
            break;
        }
        assert!(Instant::now() < deadline, "steer was not answered");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let result: String = Store::open(&home)
        .unwrap()
        .conn
        .query_row(
            "SELECT result_json FROM commands WHERE agent_id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .unwrap();
    assert!(result.contains("capability"));
    Service::new(home.clone()).cancel(&id).unwrap();
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_cancel_queued_before_launch_cannot_orphan_the_engine`.
///
/// The cancellation is persisted before the real supervisor process starts;
/// its deterministic terminal row and missing spawn phase prove no fixture
/// engine was launched while the child still reached a reaped terminal state.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pre_spawn_cancellation_never_launches_the_fixture_engine() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:hang");
    Service::new(home.clone()).cancel(&id).unwrap();
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor subprocess timed out")
        .expect("wait on supervisor subprocess");
    assert!(status.success());
    let store = Store::open(&home).unwrap();
    assert_eq!(store.get(&id).unwrap().status, Status::Cancelled);
    assert!(store.last_event(&id, "phase").unwrap().is_none());
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_ready_accepts_prestarted_and_cancelling_rows`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_accepts_a_cancelling_admission_before_engine_spawn() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:hang");
    let mut store = Store::open(&home).unwrap();
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='cancelling' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    drop(store);
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());
    assert_eq!(
        Store::open(&home).unwrap().get(&id).unwrap().status,
        Status::Cancelled
    );
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_cancel_accepted_at_terminal_barrier_cannot_be_lost`.
///
/// The trigger injects a cancel during the terminal update itself, exercising
/// the terminal barrier without depending on a wall-clock race.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_race_cancel_leaves_fsm_and_cleanup_evidence_valid() {
    let (_tmp, home) = home();
    let id = admit(&home, "hello");
    Store::open(&home)
        .unwrap()
        .conn
        .execute(
            "CREATE TRIGGER inject_terminal_cancel BEFORE UPDATE OF status ON agents \
             WHEN NEW.status='succeeded' BEGIN \
               INSERT INTO commands(agent_id,kind,payload_json,state,created_at) \
               VALUES(NEW.id,'cancel','{}','pending',0); \
             END",
            [],
        )
        .unwrap();
    let mut child = spawn_supervisor(&home, &id);
    let status = tokio::time::timeout(Duration::from_secs(20), child.wait())
        .await
        .expect("supervisor timed out")
        .expect("wait on supervisor");
    assert!(status.success());

    let store = Store::open(&home).unwrap();
    let row = store.get(&id).unwrap();
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT state FROM commands WHERE agent_id=?",
                [id.as_str()],
                |value| value.get::<_, String>(0),
            )
            .unwrap(),
        "completed"
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT result_json FROM commands WHERE agent_id=?",
                [id.as_str()],
                |value| value.get::<_, String>(0),
            )
            .unwrap(),
        r#"{"accepted":true,"reason":"already_stopping"}"#
    );
    let cleanup = store
        .last_event(&id, "process_cleanup")
        .unwrap()
        .expect("cleanup evidence");
    assert_eq!(cleanup["confirmed"], json!(true));
}

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_unkillable_cancel_is_failed_not_coerced_to_cancelled`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_stuck_native_interrupt_cannot_block_group_enforcement`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_ignoring_sigterm_requires_sigkill() {
    let (_tmp, home) = home();
    let id = admit(&home, "fixture:ignore-sigterm");
    let mut child = spawn_supervisor(&home, &id);
    wait_engine_ready(&home, &id, Duration::from_secs(10)).await;
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
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_answer_inspection_failure_after_cleanup_is_durable`.
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

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_a_surviving_group_is_stopped_before_lost_is_committed`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_surviving_nonleader_never_claims_group_gone`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_a_surviving_group_is_reported_not_gone`.
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

/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_early_exited_engine_never_cancels_or_signals_an_unverified_group`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_non_group_leader_is_never_native_cancelled_or_group_signalled`.
/// Mirrors `tests/test_supervisor.py::SupervisorTests::test_reused_group_id_never_receives_native_cancel_or_signal`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_an_already_dead_group_is_not_signalled`.
/// Mirrors Python `tests/test_lifecycle.py::TerminateProcessGroupTests::test_natural_quiesce_reaps_before_signalling`.
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
