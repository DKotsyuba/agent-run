//! Separate executable process per admitted run. READY precedes authentication/materialization.
use crate::{
    Error, Result,
    adapters::{self, io::Process, materialize},
    broker_writer::{BROKER_BUDGET, BrokerLane},
    commands,
    config::Adapter,
    domain::{self, AgentId, Outcome, Status},
    error::invalid,
    fs, launch,
    persist::{self, Persist},
    process,
    service::{LaunchIdentity, ProviderLaunchIdentity},
    state::Store,
    verify,
};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::catalog::{AccountId, HarnessId};
use agent_run_store::{ops, writer::StoreReader};
use rusqlite::OptionalExtension;
use serde_json::json;
use std::collections::BTreeMap;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    time::Duration,
};

/// Longest the supervisor waits for the launcher to release its capability
/// after the READY gate opened.
const CAPABILITY_WAIT: Duration = Duration::from_secs(10);

/// A fresh launch capability: 32 random bytes as 64 lowercase hex characters.
/// Only its SHA-256 is ever stored; the secret travels over a private pipe.
fn new_capability() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Start one detached `_supervisor` session leader and return after its READY.
///
/// The launch is split so no step blocks the async runtime on storage: the
/// capability hash is committed through the broker's writer first
/// (`RecordSupervisorLaunch`), the child is spawned holding its first gate
/// (it cannot authenticate to the writer without the secret), the spawned
/// process's identity evidence is committed and AWAITED
/// (`RecordStartupOwner`), and only then is the secret written to the child's
/// private pipe. Any failure after the spawn abandons the child with verified
/// cleanup. A failed READY read does not cancel the already admitted durable job.
///
/// The launch runs as an owned task: cancelling or timing out the caller never
/// drops it between steps, so a spawned child is always either completed,
/// reaped or abandoned by the task itself. An internal `LaunchGuard` covers the one
/// remaining edge, the runtime shutting the task down mid-launch.
///
/// Spawning is `posix_spawn(POSIX_SPAWN_SETSID)`-first (`launch.rs`).
pub async fn launch(home: &Path, id: &AgentId, lane: &BrokerLane) -> Result<()> {
    launch_program(&std::env::current_exe()?, home, id, lane).await
}

/// [`launch()`] of an explicit supervisor executable (a test drives the real
/// `agent-run` binary instead of its own test harness).
pub async fn launch_program(
    program: &Path,
    home: &Path,
    id: &AgentId,
    lane: &BrokerLane,
) -> Result<()> {
    // Test-only: `<home>/fixture-supervisor-spawn-error` makes the OS-level
    // spawn refusal observable without a real exhausted process table.
    #[cfg(feature = "test-fixtures")]
    if home.join("fixture-supervisor-spawn-error").exists() {
        return Err(Error::Io(std::io::Error::other(
            "injected supervisor spawn refusal",
        )));
    }
    let (program, home, id, lane) = (
        program.to_path_buf(),
        home.to_path_buf(),
        id.clone(),
        lane.clone(),
    );
    tokio::spawn(async move { launch_owned(&program, &home, &id, &lane).await })
        .await
        .map_err(|e| Error::Runtime(format!("supervisor launch task failed: {e}")))?
}

/// Owns a spawned, not yet authenticated supervisor: unless it is taken back
/// by [`Self::take`], dropping it abandons the child with verified cleanup on
/// a helper thread (the child holds no capability, so it can do nothing
/// durable meanwhile).
struct LaunchGuard {
    /// The launch to clean up; `None` once taken.
    started: Option<launch::StartedLaunch>,
    /// Deadlines of the cleanup.
    timeouts: launch::Timeouts,
}

impl LaunchGuard {
    /// Takes the launch back from the guard.
    fn take(&mut self) -> launch::StartedLaunch {
        self.started.take().expect("launch is held until taken")
    }

    /// The held launch.
    fn held(&mut self) -> &mut launch::StartedLaunch {
        self.started.as_mut().expect("launch is held until taken")
    }
}

impl Drop for LaunchGuard {
    /// Abandons a launch dropped before it completed.
    fn drop(&mut self) {
        if let Some(started) = self.started.take() {
            let timeouts = self.timeouts;
            std::thread::spawn(move || {
                let _ = started.abandon(timeouts, "supervisor launch was cancelled".into());
            });
        }
    }
}

/// The body of [`launch_program`], run as an owned task.
async fn launch_owned(program: &Path, home: &Path, id: &AgentId, lane: &BrokerLane) -> Result<()> {
    let program = program.to_path_buf();
    let secret = new_capability();
    lane.local(
        ops::RecordSupervisorLaunch {
            execution: id.clone(),
            capability_sha256: fs::sha256(secret.as_bytes()),
        },
        BROKER_BUDGET,
    )
    .await?;
    let timeouts = launch::Timeouts::default();
    let (spawn_home, spawn_id) = (home.to_path_buf(), id.clone());
    // The blocking spawn hands its result over a channel; when the receiver is
    // already gone the thread abandons the child itself.
    let (handoff, spawned) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let args = [
            OsStr::new("--home"),
            spawn_home.as_os_str(),
            OsStr::new("_supervisor"),
            OsStr::new(spawn_id.as_str()),
        ];
        let result = launch::spawn_launch(&program, &args, timeouts, true);
        if let Err(Ok(started)) = handoff.send(result) {
            let _ = started.abandon(timeouts, "supervisor launch was cancelled".into());
        }
    });
    let started = spawned
        .await
        .map_err(|e| Error::Runtime(format!("supervisor launch task failed: {e}")))?
        .map_err(launch_error)?;
    let mut guard = LaunchGuard {
        started: Some(started),
        timeouts,
    };
    let (pid, backend) = (guard.held().pid, guard.held().backend);
    // Durable before any proof or capability, so reconciliation can observe this child.
    let mut evidence = process::inspect(pid)
        .ok()
        .and_then(|p| serde_json::to_value(p).ok())
        .unwrap_or_else(|| json!({"pid": pid}));
    evidence["spawn_backend"] = json!(backend);
    let recorded = lane
        .local(
            ops::RecordStartupOwner {
                execution: id.clone(),
                identity: evidence,
            },
            BROKER_BUDGET,
        )
        .await;
    if let Err(error) = recorded {
        let message = format!("supervisor startup evidence was not recorded: {error}");
        return Err(abandon(guard.take(), timeouts, message).await);
    }
    if let Err(error) = guard.held().release_capability(secret.as_bytes()) {
        let message = format!("supervisor capability was not released: {error}");
        return Err(abandon(guard.take(), timeouts, message).await);
    }
    let started = guard.take();
    // The exact-PID reaper starts inside the blocking task, before its result
    // can be dropped; the committed owner record stands even if it cannot start.
    let (handoff, launched) = tokio::sync::oneshot::channel();
    tokio::task::spawn_blocking(move || {
        let result = started.complete(timeouts);
        if let Ok(done) = &result {
            let _ = launch::spawn_reaper(done.pid);
        }
        let _ = handoff.send(result);
    });
    launched
        .await
        .map_err(|e| Error::Runtime(format!("supervisor launch task failed: {e}")))?
        .map(drop)
        .map_err(launch_error)
}

/// Cleans up a spawned but unauthenticated supervisor off the async runtime.
async fn abandon(
    started: launch::StartedLaunch,
    timeouts: launch::Timeouts,
    message: String,
) -> Error {
    match tokio::task::spawn_blocking(move || started.abandon(timeouts, message)).await {
        Ok(error) => launch_error(error),
        Err(error) => Error::Runtime(format!("supervisor cleanup task failed: {error}")),
    }
}

/// Maps a platform launch failure to the product error classes.
fn launch_error(error: launch::LaunchError) -> Error {
    match error {
        launch::LaunchError::Spawn(error) => Error::Io(error),
        // A child that died before its PID proof owns nothing: fail like a failed spawn.
        error @ launch::LaunchError::Bootstrap(_) => {
            Error::Io(std::io::Error::other(error.to_string()))
        }
        error => Error::Runtime(error.to_string()),
    }
}
/// Child side: prove session identity, authenticate to the writer with the
/// private launch capability, commit ownership and report READY.
///
/// `fds` are the inherited ready, identity and error descriptors and
/// `capability_fd` the private pipe holding the launch capability. The secret
/// is read once, kept only in memory and in the writer hello, and its pipe is
/// closed (and was never inherited by any harness). Ownership is one awaited
/// `RecordSupervisorOwner` operation; the first heartbeat makes the complete
/// ownership record eligible for later recovery. Native journal rows are
/// coalesced into bounded batches. Execution errors are logged by fixed public
/// class without persisting task text or credentials, including errors whose
/// owned process prevents finish.
pub async fn run(home: &Path, id: &AgentId, fds: [i32; 3], capability_fd: i32) -> Result<()> {
    crate::logging::configure(home, "supervisor");
    let [ready_fd, identity_fd, error_fd] = fds;
    if let Err(error) = launch::report_identity(identity_fd, error_fd) {
        let message = if error.to_string().trim().is_empty() {
            "OSError".to_owned()
        } else {
            error.to_string()
        };
        let _ = launch::report_ready(ready_fd, Err(&message));
        return Err(error.into());
    }
    // A disconnected READY reader cannot undo an already committed owner.
    let mut persist = match bootstrap(home, id, capability_fd).await {
        Ok(persist) => {
            let _ = launch::report_ready(ready_fd, Ok(()));
            persist
        }
        Err(error) => {
            let message = error_text(&error);
            let _ = launch::report_ready(ready_fd, Err(&message));
            return Err(error);
        }
    };
    match execute(home, id, &mut persist).await {
        Ok(()) => persist.flush().await,
        Err(error) => finish_failed_run(home, id, &mut persist, error).await,
    }
}

/// Reads the capability, authenticates to the writer and commits ownership.
///
/// # Errors
/// An unreadable or malformed capability, an unreachable or refusing writer, or
/// a rejected owner record (the execution is no longer an unowned `starting`
/// or `cancelling` one).
async fn bootstrap(home: &Path, id: &AgentId, capability_fd: i32) -> Result<Persist> {
    let secret = tokio::task::spawn_blocking(move || {
        launch::read_capability(capability_fd, CAPABILITY_WAIT)
    })
    .await
    .map_err(|e| Error::Runtime(format!("capability task failed: {e}")))?
    .map_err(Error::Io)?;
    let capability = String::from_utf8(secret)
        .ok()
        .filter(|text| {
            text.len() == 64
                && text
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        .ok_or_else(|| invalid("launch capability is malformed"))?;
    let logical = StoreReader::open_with(home, Duration::from_millis(250))?
        .get(id)?
        .root_agent_id;
    let producer = persist::Producer::connect(
        &home.join("writer.sock"),
        persist::supervisor_hello(&logical, id, capability),
        persist::supervisor_producer_id(id),
    );
    let mut persist = Persist::open(home, id, producer)?;
    let owner = process::inspect(std::process::id() as i32)?;
    persist.record_owner(&owner, None).await?;
    Ok(persist)
}

/// Records and logs a failed run and, when nothing owned still needs recovery,
/// finishes it with a preparation-failure terminal outcome.
async fn finish_failed_run(
    home: &Path,
    id: &AgentId,
    store: &mut Persist,
    error: Error,
) -> Result<()> {
    // Final checkpoint failures can return before an engine result is
    // committed; their closed diagnostic must survive that path too.
    let metadata = execution_error_metadata(&error);
    // A database outage must not erase the operation's safe failure facts.
    let directory = home.join("agents").join(id.as_str());
    let _ = fs::Dir::open(&directory).and_then(|dir| {
        dir.write(
            Path::new("execution-failure.json"),
            &fs::canonical_json(&metadata)?,
            0o600,
        )
    });
    let _ = store
        .event_durable(id, "execution_failure_v1", &metadata)
        .await;
    if let Some(logger) = crate::logging::configured() {
        logger.log(
            crate::logging::Level::Error,
            &format!(
                "supervisor execution failed agent_id={} class={} cause={}",
                id,
                error.public().kind,
                metadata
            ),
        );
    }
    let row = store.get(id)?;
    if !row.status.terminal() {
        if let Ok((attempt, _)) = store.provider_attempt(id)
            && !crate::lifecycle::reconcile::cleanup_mcp_discovery(store, &attempt)?
        {
            return Err(error);
        }
        if row
            .identity
            .as_ref()
            .is_some_and(|value| value["provider_identity_version"] == 2)
            && !persist::attempt_never_spawned(&store.conn, id)?
        {
            return Err(error);
        }
        let mut outcome = Outcome::failure(preparation_failure_kind(&error));
        outcome.failure_text = Some(error.public().message);
        if store.cancel_pending(id)? {
            outcome.status = Status::Cancelled;
        }
        store.finish(id, &outcome, None, None).await?;
        store.complete_terminal(id).await?;
    }
    Err(error)
}

/// The ownership sink of one attempt: every call claims its submission position
/// in the supervisor's ordered lane at once and returns the owned future of
/// that one `RememberProcesses` operation. The write is awaited by the process
/// observer while the primary and its descendants keep being observed, is never
/// restarted by cancellation, and never limits an execution's lifetime.
fn ownership_sink(producer: &persist::Producer, attempt: String) -> adapters::io::OwnershipSink {
    let producer = producer.clone();
    Box::new(move |snapshot| {
        persist::ownership_future(
            &producer,
            attempt.clone(),
            snapshot,
            persist::CRITICAL_BUDGET,
        )
    })
}

/// The owned future persisting one discovery-child ownership snapshot as an
/// `mcp_discovery_ownership` journal event of `attempt`. Like
/// [`ownership_sink`], the submission position is claimed when it is built and
/// dropping the future never withdraws the operation.
fn discovery_ownership(
    producer: &persist::Producer,
    agent: &AgentId,
    attempt: &str,
    snapshot: process::OwnershipSnapshot,
) -> adapters::io::PersistFuture {
    let data = match serde_json::to_value(&snapshot) {
        Ok(data) => data,
        Err(error) => return Box::pin(async move { Err(Error::from(error)) }),
    };
    let pending = producer.submit(
        ops::AppendJournal {
            execution: agent.clone(),
            fragments: vec![ops::JournalFragment::Event {
                kind: "mcp_discovery_ownership".to_owned(),
                data,
                attempt: Some(attempt.to_owned()),
            }],
        },
        persist::CRITICAL_BUDGET,
    );
    Box::pin(async move { persist::into_result::<Vec<i64>>(pending.await).map(drop) })
}

/// Preserve exact owned identities, observed cleanup and an optional sealed
/// candidate before final database operations. The private per-attempt receipt
/// is NOT a terminal verdict, a signalling capability or permission to replay
/// SQL/native work: recovery must recheck the database and native identities.
/// It contains no environment, credentials, SQL or original error messages.
/// The candidate is already redacted; its hash alone cannot establish success.
/// Atomic no-follow writes use the existing agent-directory durability guard.
/// Candidate sealing failure still writes cleanup evidence, then returns the
/// original error; optional answer handling cannot discard required proof.
fn retain_cleanup_evidence(
    agent_dir: &Path,
    attempt: &str,
    process: &Process,
    cleanup: &Result<agent_run_platform::process::Cleanup>,
    execution: &Result<adapters::EngineResult>,
    exit: Option<i32>,
) -> Result<()> {
    let candidate = match execution {
        Ok(result) => result
            .answer
            .as_deref()
            .filter(|text| !text.trim().is_empty()),
        Err(_) => None,
    };
    let candidate = candidate
        .map(|text| {
            verify::seal(
                agent_dir,
                Path::new(&format!("recovery.{attempt}.answer.md")),
                text,
            )
        })
        .transpose();
    let candidate_diagnostic = candidate.as_ref().err().map(execution_error_metadata);
    let cleanup = match cleanup {
        Ok(proof) => serde_json::to_value(proof)?,
        Err(error) => json!({"confirmed":false,"diagnostic":execution_error_metadata(error)}),
    };
    let diagnostic = execution.as_ref().err().map(execution_error_metadata);
    let data = json!({"version":1,"attempt":attempt,"recorded_at":domain::now(),
        "terminal_commit":"not_proven_by_receipt","ownership":process.owner.snapshot(),
        "cleanup":cleanup,"engine_result_available":execution.is_ok(),
        "native_status":execution.as_ref().ok().map(|result|result.outcome.status),
        "candidate":candidate.as_ref().ok().and_then(|proof|proof.as_ref()),
        "candidate_diagnostic":candidate_diagnostic,"exit_code":exit,"diagnostic":diagnostic});
    fs::Dir::open(agent_dir)?.write(
        Path::new(&format!("recovery.{attempt}.json")),
        &fs::canonical_json(&data)?,
        0o600,
    )?;
    candidate.map(|_| ())
}

/// Returns closed, content-free categories for an execution error. Original
/// messages, SQL text, paths, JSON contents and credential/environment values
/// never enter diagnostics. Native numeric errno/SQLite codes remain nullable.
pub(crate) fn execution_error_metadata(error: &Error) -> serde_json::Value {
    let mut data = json!({"version":1,"stage":"engine_execution","class":error.public().kind});
    match error {
        Error::OwnershipCheckpoint { stage, source } => {
            data = execution_error_metadata(source);
            data["stage"] = json!(stage);
            data["ownership_category"] = json!(match source.as_ref() {
                Error::Integrity(_) | Error::AnswerIntegrity(_) => "integrity_failure",
                Error::Sql(_) | Error::Persistence { .. } => "persistence_sqlite",
                Error::Io(_) => "io_failure",
                _ => "checkpoint_failure",
            });
        }
        Error::Persistence {
            operation,
            elapsed_ms,
            attempts,
            phase,
            source,
            writer_identity,
        } => {
            data = execution_error_metadata(source);
            // The closed writer phase (acquire, statement, commit, ...) is a
            // separate axis from the ownership stage and survives unchanged.
            data["phase"] = json!(phase);
            data["stage"] = json!(match operation {
                agent_run_domain::error::PersistenceOperation::TerminalCommit
                | agent_run_domain::error::PersistenceOperation::CleanupWrite => "final",
                agent_run_domain::error::PersistenceOperation::StoreOpen => "store_open",
                _ => "streaming",
            });
            data["operation"] = json!(operation);
            data["wait_ms"] = json!(elapsed_ms);
            data["attempts"] = json!(attempts);
            if let Some((id, kind)) = writer_identity {
                data["writer_operation_id"] = json!(id);
                data["writer_op_kind"] =
                    json!(agent_run_store::ops::OpKindRef::from_wire_id(*kind).map(|k| k.name()));
            }
        }
        Error::Io(source) => {
            data["io_kind"] = json!(format!("{:?}", source.kind()));
            data["os_code"] = json!(source.raw_os_error());
        }
        Error::WriterFailure(failure) => {
            use agent_run_domain::storage::WriterFailure;
            match failure {
                WriterFailure::NotEnqueued(reason) => {
                    data["outcome"] = json!("not_enqueued");
                    data["admission_reason"] = json!(reason);
                }
                WriterFailure::Unknown(cause) => {
                    data["outcome"] = json!("unknown");
                    data["unknown_cause"] = json!(cause);
                }
            }
        }
        Error::Sql(rusqlite::Error::SqliteFailure(source, _)) => {
            data["sqlite_code"] = json!(format!("{:?}", source.code));
            data["sqlite_extended_code"] = json!(source.extended_code);
        }
        Error::Json(source) => {
            data["json_category"] = json!(format!("{:?}", source.classify()));
        }
        _ => {}
    }
    data
}

/// Preserves a failed execution's safe cause across both supervisor routes.
/// Logging precedes best-effort event persistence; either sink may be unavailable.
/// Diagnostic failure never replaces the original failure. The returned failure
/// has no answer; existing identity/cleanup verification still applies.
async fn failed_execution(
    store: &mut Persist,
    id: &AgentId,
    error: &Error,
) -> adapters::EngineResult {
    let metadata = execution_error_metadata(error);
    if let Some(logger) = crate::logging::configured() {
        logger.log(
            crate::logging::Level::Error,
            &format!("execution_failure {metadata}"),
        );
    }
    if store
        .event_durable(id, "execution_failure_v1", &metadata)
        .await
        .is_err()
        && let Some(logger) = crate::logging::configured()
    {
        logger.log(
            crate::logging::Level::Error,
            "execution_failure_diagnostic_not_persisted",
        );
    }
    failed_result(error)
}

/// The failed engine result for `error`: a closed failure kind, the closed
/// diagnostic as text and no answer. Pure; persists nothing.
fn failed_result(error: &Error) -> adapters::EngineResult {
    let metadata = execution_error_metadata(error);
    let mut outcome = Outcome::failure(match error {
        Error::Integrity(_) | Error::AnswerIntegrity(_) => "runtime_integrity_failed",
        Error::Validation(_) => "runtime_contract_rejected",
        Error::Sql(_) | Error::Persistence { .. } => "runtime_storage_failed",
        Error::OwnershipCheckpoint {
            stage: agent_run_domain::OwnershipStage::Observation,
            ..
        } => "runtime_ownership_observation_failed",
        Error::OwnershipCheckpoint { .. } => "runtime_ownership_checkpoint_failed",
        Error::Json(_) => "runtime_protocol_failed",
        _ => "runtime_transport_failed",
    });
    outcome.failure_text = Some(format!("execution failed; diagnostic={metadata}"));
    adapters::EngineResult {
        native_failure: None,
        outcome,
        answer: None,
        usage: None,
    }
}

/// Returns a nonblank startup diagnostic even when an error carries no text.
fn error_text(error: &Error) -> String {
    let message = error.to_string();
    if message.trim().is_empty() {
        match error {
            Error::Validation(_) => "ValidationError",
            Error::Runtime(_) => "RuntimeError",
            Error::Io(_) => "OSError",
            Error::Sql(_) => "DatabaseError",
            _ => "SupervisorError",
        }
        .into()
    } else {
        message
    }
}

/// Classifies a supervisor-side preparation error for the durable terminal row.
///
/// A frozen launch identity whose runtime was removed or disabled is a runtime
/// preparation failure, rather than a caller-input rejection: ownership has
/// already been committed and the supervisor must leave a diagnosable terminal
/// row. Other validation failures remain preparation rejections.
fn preparation_failure_kind(error: &Error) -> &'static str {
    match error {
        Error::Integrity(_) => "snapshot_integrity_failed",
        Error::Validation(message) if message == "runtime is not enabled" => {
            "prepare_runtime_failed"
        }
        Error::Validation(_) => "preparation_rejected",
        _ => "supervisor_failed",
    }
}

/// Run one admitted agent through preparation, supervision, cleanup, and terminal storage.
async fn execute(home: &Path, id: &AgentId, store: &mut Persist) -> Result<()> {
    let first = store.get(id)?;
    if first
        .identity
        .as_ref()
        .is_some_and(|value| value["provider_identity_version"] == 2)
    {
        return execute_provider(home, id, store).await;
    }
    if store.cancel_pending(id)? {
        return cancelled_before_spawn(id, store).await;
    }
    let mut row = store.get(id)?;
    let mut identity = LaunchIdentity::read(&row)?;
    let runtime = identity.config.runtime(&row.request.runtime)?.clone();
    adapters::validate(&row.request, &runtime, &identity.profile)?;
    let agent_dir = home.join("agents").join(id.as_str());
    fs::private_dir(&agent_dir)?;
    store
        .event(id, "phase", &json!({"phase":"preparing"}))
        .await?;
    let runtime_home = if let Some(path) = &identity.runtime_home {
        path.clone()
    } else {
        match runtime.kind()? {
            Adapter::Codex => {
                crate::codex::runtime_home(&runtime.home, row.request.account.as_deref(), id)?
            }
            _ => runtime.home.join("runs").join(id.as_str()),
        }
    };
    let snapshot = if row.parent_agent_id.is_some() {
        materialize::verify(
            &runtime_home,
            identity
                .snapshot_sha256
                .as_deref()
                .ok_or_else(|| invalid("resume snapshot proof missing"))?,
        )?
    } else {
        let (snapshot, digest) = materialize::materialize(
            &identity.config,
            &runtime,
            &row.request,
            &identity.profile,
            &runtime_home,
            home,
        )?;
        identity.runtime_home = Some(runtime_home.clone());
        identity.snapshot_sha256 = Some(digest);
        snapshot
    };
    let revision = identity
        .snapshot_sha256
        .as_deref()
        .ok_or_else(|| invalid("materialization proof missing"))?;
    seal_identity(store, id, &runtime_home, revision, None, revision).await?;
    if store.cancel_pending(id)? {
        return cancelled_before_spawn(id, store).await;
    }
    row = store.get(id)?;
    let plan = match runtime.kind()? {
        Adapter::Codex => crate::codex::plan(
            &identity.config,
            &runtime,
            &row,
            &identity.profile,
            &runtime_home,
            home,
        )?,
        _ => crate::stream::plan(
            &identity.config,
            &runtime,
            &row,
            &identity.profile,
            &runtime_home,
            home,
            &snapshot,
        )?,
    };
    store
        .event(id, "phase", &json!({"phase":"spawning"}))
        .await?;
    let mut process = Process::spawn(&plan)?;
    // From this point EVERY path must clean up before returning, including a
    // database failure immediately after spawning the engine.
    let execution = async {
        // The spawned engine is observed (exit, descendants, escalation) while
        // every durable write below is pending, from the first one onward.
        let pid = process.owner.pid;
        process
            .observing(async {
                fixture_hold(home, "legacy-process").await;
                store.running(id, pid).await
            })
            .await?;
        let attempt = format!("{id}:1");
        let leader = process
            .owner
            .leader
            .clone()
            .ok_or_else(|| Error::Runtime("legacy leader identity unavailable".into()))?;
        process
            .observing(async {
                fixture_hold(home, "legacy-anchor").await;
                store.anchor_legacy_attempt(&attempt, &leader).await
            })
            .await?;
        process
            .observe_ownership(ownership_sink(store.producer(), attempt))
            .await?;
        process
            .observing(store.journal(id, "user", &row.request.task, None, None))
            .await?;
        match runtime.kind()? {
            Adapter::Codex => {
                crate::codex::run(
                    &mut process,
                    store,
                    &row,
                    &row,
                    &runtime,
                    &identity.profile,
                    &runtime_home,
                )
                .await
            }
            _ => {
                crate::stream::run(
                    &mut process,
                    store,
                    &row,
                    &row,
                    plan.initial_input.as_deref(),
                )
                .await
            }
        }
    };
    let execution = execution.await;
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    let exit = process.reap().await;
    retain_cleanup_evidence(
        &agent_dir,
        &format!("{id}:1"),
        &process,
        &cleanup,
        &execution,
        exit,
    )?;
    if let Err(error) = process
        .checkpoint_ownership_ready(agent_run_domain::OwnershipStage::Final)
        .await
    {
        let _ = store
            .event(
                id,
                "ownership_checkpoint_failure_v1",
                &execution_error_metadata(&error),
            )
            .await;
        return execution.and(Err(error));
    }
    let cleanup = cleanup?;
    store
        .event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)
        .await?;
    if cleanup.confirmed {
        let attempt = format!("{id}:1");
        // Runs that failed before recording ownership have nothing to release.
        let owned: bool = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND ownership_active=1)",
            [&attempt],
            |row| row.get(0),
        )?;
        if owned {
            store.provider_cleanup(id, &attempt, &cleanup).await?;
        }
    }
    let cancelled = store.cancel_pending(id)?;
    let mut result = match execution {
        Ok(result) => result,
        Err(error) => failed_execution(store, id, &error).await,
    };
    // Codex reports its semantic turn outcome before app-server shutdown. Its
    // deliberate post-result SIGTERM does not turn a verified completed turn
    // into a native failure. Stream adapters already verified native exit zero.
    if result.outcome.exit_code.is_none() {
        result.outcome.exit_code = exit;
    }
    let proof = match result.answer.as_deref() {
        Some(text) if !text.trim().is_empty() => {
            Some(verify::seal(&agent_dir, Path::new("answer.md"), text)?)
        }
        _ => None,
    };
    let evidence = match &proof {
        Some(p) => verify::AnswerProof::sealed(p),
        None => verify::AnswerProof::absent(agent_dir.join("answer.md")),
    };
    let last_progress_at = store.last_progress(id)?;
    let outcome = verify::verify_completion(
        Some(result.outcome),
        cancelled.then_some(verify::StopReason::Cancel),
        Some(&evidence),
        cleanup.confirmed,
        last_progress_at,
        domain::now(),
        verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
    )?;
    #[cfg(feature = "test-fixtures")]
    {
        store.flush().await?;
        fixture_hold(home, "legacy-terminal").await;
    }
    store
        .finish(id, &outcome, proof.as_ref(), result.usage.as_ref())
        .await?;
    commands::complete_terminal_persisted(store, id).await?;
    Ok(())
}

/// The one compatibility scope every explicitly managed immutable shared tree
/// is imported under: a digest of the versioned managed-assets domain and the
/// effective UID, so payloads never alias across scopes or users while
/// unrelated compatibility domains can later choose their own.
/// The per-user scope every managed tree of this installation imports under.
pub fn managed_scope() -> String {
    // SAFETY: geteuid only reads this process's effective identity.
    let uid = unsafe { libc::geteuid() };
    fs::sha256(format!("agent-run/managed-assets/v1:{uid}").as_bytes())
}

/// Installs the shared layout for one freshly sealed provider home.
///
/// Planning is read-only; a home whose index maps no managed roots, or one
/// already committed to the same digest, needs no work. Before anything is
/// pinned or moved, the canonical shared store is validated and the Codex
/// grant, when present, must exclude it from every writable and temporary
/// root. A refusal never falls back to a silent private copy. The install itself is the registry-backed coordinator, which
/// pins its references before importing, switches roots behind a token-bound
/// in-home backup, proves the unchanged original index, commits, and removes
/// only backups proven to still hold the replaced assets.
async fn install_shared_assets<B: crate::runtime_storage::LayoutBackend>(
    store: &mut B,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
    owner: &AgentId,
    codex_grant: Option<&crate::codex::Grant>,
) -> Result<()> {
    let scope = managed_scope();
    let Some(layout) =
        crate::runtime_storage::plan(store.reader(), app_home, runtime_home, expected, &scope)?
    else {
        return Ok(());
    };
    shared_publication_root(app_home, codex_grant)?;
    crate::runtime_storage::install_on(store, app_home, &layout, Some(owner)).await
}

/// Validates the shared store and returns its canonical root as the witness
/// every publication path must hold.
///
/// The store directory is created owner-only when absent and must be an
/// existing absolute real directory whose canonical form equals its path, so no
/// symlinked component can alias it. For Codex the grant must also keep the
/// store outside every admitted writable and temporary root. There is no
/// OS-level write denial for the launched harness: the store's immutability
/// rests on owner-only modes plus digest verification before each launch, and a
/// same-UID process can still alter it between verifications. The returned path
/// is the only root a caller may publish into: `consolidate` refuses any
/// other, so an unvalidated publication cannot masquerade as a validated one.
pub fn shared_publication_root(
    app_home: &Path,
    codex_grant: Option<&crate::codex::Grant>,
) -> Result<PathBuf> {
    let root = crate::runtime_storage::store_root(app_home)?;
    fs::private_dir(&root)?;
    let root = agent_run_platform::shared_assets::validated_root(&root)?;
    if let Some(grant) = codex_grant {
        grant.admits_shared_root(&root)?;
    }
    Ok(root)
}

/// Validates the shared store for one frozen execution's native publication.
///
/// Builds the exact launch plan the sealed home would launch with — frozen
/// configuration and authority, recorded account, host environment — so a
/// home whose sealed record, configuration, role or credentials no longer
/// plan is refused, and derives the Codex grant from it. The harness binary and
/// workdir are not stat'ed or executed here; their absence surfaces at launch.
/// [`shared_publication_root`] then validates the store against that grant. A
/// failure means the home stays private: no anchor, no shared link, nothing
/// published.
pub fn native_publication_root(
    store: &Store,
    home: &Path,
    identity: &ProviderLaunchIdentity,
    account: &AccountId,
    runtime_home: &Path,
    request: &domain::StartRequest,
) -> Result<PathBuf> {
    // Native preparation precedes planning and publication: a frozen remote
    // plugin parent must be private again before any child could write to it.
    crate::runtime_cache::prepare_native(home, runtime_home, true)?;
    let config = &identity.provider_config;
    let catalog = config.resolve_catalog(store.list_accounts()?)?;
    if !config.harnesses.contains_key(&identity.authority.harness) {
        return Err(invalid("recorded harness is unavailable"));
    }
    // An already-shared managed home is planned through its verified
    // committed registry mapping, exactly as it launches: the strict private
    // verifier would reject its managed links and silently skip every
    // managed-role home's native consolidation.
    let shared = shared_launch_assets(store, home, runtime_home)?;
    let host: BTreeMap<String, String> = std::env::vars().collect();
    let preliminary = adapters::provider::plan_selected_with(
        config,
        &catalog,
        &identity.authority,
        account,
        runtime_home,
        home,
        &host,
        &adapters::authorized_request::SystemCredentialReader,
        identity.provider_request.task.as_str(),
        None,
        adapters::provider::LaunchOptions {
            fast: request.fast,
            output_schema: identity.provider_request.output_schema.as_ref(),
        },
        shared.as_ref(),
    )?;
    let codex_grant = if identity.authority.harness == HarnessId::Codex {
        Some(crate::codex::Grant::new(
            &preliminary.runtime,
            request,
            &preliminary.profile,
            home,
        )?)
    } else {
        None
    };
    shared_publication_root(home, codex_grant.as_ref())
}

/// The result of one operator-driven relocation of a retained sealed home.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Relocation {
    /// The home has no managed roots or already carries its layout.
    AlreadyShared,
    /// The home was relocated into the shared store and committed.
    Installed,
    /// The home was preserved for this explicit reason, with the private
    /// managed-root bytes the operator still holds.
    Skipped { reason: String, private_bytes: u64 },
}

/// Relocates one already-sealed retained home after the launch preflight.
///
/// This is the narrow trusted seam for operator compaction: it replays the
/// supervisor's own preflight exactly — the recorded launch identity (and
/// the frozen configuration and authority it carries), the account the
/// agent's attempt was actually admitted with, and the Codex grant derived
/// from them — and only then hands the home to the registry coordinator. It
/// never rewrites the frozen identity or configuration to make a check pass,
/// never guesses the current provider, and never launches a model. It neither
/// spawns nor stats the harness binary or workdir, so their absence is not
/// detected here; it surfaces when the home next launches. An unreadable
/// identity, a launch that no longer plans, an invalid store root, or an
/// unverifiable Codex grant surfaces as [`Relocation::Skipped`] with
/// the reason and the bytes still held privately; the home itself is untouched.
pub async fn relocate_retained_home_on<B: crate::runtime_storage::LayoutBackend>(
    store: &mut B,
    app_home: &Path,
    agent: &AgentId,
) -> Result<Relocation> {
    let skip = |home: Option<&Path>, reason: String| Relocation::Skipped {
        reason,
        private_bytes: home.map(private_managed_bytes).unwrap_or(0),
    };
    // A row this build cannot even read — a legacy or damaged record — is a
    // reported skip, never a forced switch or a hard failure of the whole
    // compaction pass.
    let row = match store.reader().get(agent) {
        Ok(row) => row,
        Err(error) => {
            let home = store
                .reader()
                .retained_runtime_homes(None, 1)
                .ok()
                .and_then(|(page, _)| {
                    page.into_iter()
                        .find(|reference| reference.agent_id == agent.as_str())
                        .map(|reference| PathBuf::from(reference.runtime_home))
                });
            return Ok(skip(home.as_deref(), error.to_string()));
        }
    };
    let recorded_home = row.identity.as_ref().and_then(|identity| {
        identity
            .get("runtime_home")
            .and_then(serde_json::Value::as_str)
            .map(std::path::PathBuf::from)
    });
    let identity = match ProviderLaunchIdentity::read(&row) {
        Ok(identity) => identity,
        Err(error) => return Ok(skip(recorded_home.as_deref(), error.to_string())),
    };
    let Some(runtime_home) = identity.runtime_home.clone() else {
        return Ok(Relocation::AlreadyShared);
    };
    let config = identity.provider_config.clone();
    let catalog = match config.resolve_catalog(store.reader().list_accounts()?) {
        Ok(catalog) => catalog,
        Err(error) => return Ok(skip(Some(&runtime_home), error.to_string())),
    };
    let account = match store.reader().recorded_account(agent) {
        Ok(account) => account,
        Err(error) => return Ok(skip(Some(&runtime_home), error.to_string())),
    };
    if !config.harnesses.contains_key(&identity.authority.harness) {
        return Ok(skip(
            Some(&runtime_home),
            "recorded harness is unavailable".into(),
        ));
    }
    let host: BTreeMap<String, String> = std::env::vars().collect();
    let preliminary = match adapters::provider::plan_selected_with(
        &config,
        &catalog,
        &identity.authority,
        &account,
        &runtime_home,
        app_home,
        &host,
        &adapters::authorized_request::SystemCredentialReader,
        identity.provider_request.task.as_str(),
        row.resume_of_runtime_session_id.as_deref(),
        adapters::provider::LaunchOptions {
            fast: row.request.fast,
            output_schema: identity.provider_request.output_schema.as_ref(),
        },
        None,
    ) {
        Ok(plan) => plan,
        Err(error) => return Ok(skip(Some(&runtime_home), error.to_string())),
    };
    let codex_grant = if identity.authority.harness == HarnessId::Codex {
        match crate::codex::Grant::new(
            &preliminary.runtime,
            &row.request,
            &preliminary.profile,
            app_home,
        ) {
            Ok(grant) => Some(grant),
            Err(error) => return Ok(skip(Some(&runtime_home), error.to_string())),
        }
    } else {
        None
    };
    match install_shared_assets(
        store,
        app_home,
        &runtime_home,
        identity.authority.assets_sha256.as_str(),
        agent,
        codex_grant.as_ref(),
    )
    .await
    {
        Ok(()) => Ok(Relocation::Installed),
        Err(error) => Ok(skip(Some(&runtime_home), error.to_string())),
    }
}

/// Consolidates one retained home's native caches from its recorded
/// authority, for the operator's offline compaction.
///
/// The same narrow seam as [`relocate_retained_home`]: the recorded identity
/// and frozen configuration supply the compatibility domain, the recorded
/// attempt the account, and the caller holds the offline locks with every
/// holder terminal. An unverifiable home is a reported skip with its reason
/// and preserved byte count, never a forced consolidation.
pub async fn consolidate_retained_native_on<B: crate::runtime_storage::LayoutBackend>(
    store: &mut B,
    app_home: &Path,
    agent: &AgentId,
) -> std::result::Result<serde_json::Value, String> {
    let row = store
        .reader()
        .get(agent)
        .map_err(|error| error.to_string())?;
    let identity = ProviderLaunchIdentity::read(&row).map_err(|error| error.to_string())?;
    let Some(runtime_home) = identity.runtime_home.clone() else {
        return Err("recorded identity has no sealed runtime home".into());
    };
    let account = store
        .reader()
        .recorded_account(agent)
        .map_err(|error| error.to_string())?;
    // Native caches publish only into the root validated exactly as a managed
    // relocation validates it, replayed from the recorded frozen authority.
    let witness = native_publication_root(
        store.reader(),
        app_home,
        &identity,
        &account,
        &runtime_home,
        &row.request,
    )
    .map_err(|error| error.to_string())?;
    let report = crate::runtime_cache::consolidate_on(
        store,
        agent,
        &identity,
        &account,
        app_home,
        &runtime_home,
        &witness,
    )
    .await
    .map_err(|error| error.to_string())?;
    serde_json::to_value(&report).map_err(|error| error.to_string())
}

/// Sums the file bytes of one home's still-private managed trees, counting
/// each inode once and never following a symlink. Bounded to the store's own
/// entry bound, so a pathological home reports a partial total rather than
/// walking without end; an unreadable home reports zero.
fn private_managed_bytes(home: &Path) -> u64 {
    fn walk(dir: &fs::Dir, seen: &mut BTreeMap<u64, u64>, budget: &mut usize) -> u64 {
        let mut total = 0;
        let names = match dir.list(None) {
            Ok(names) => names,
            Err(_) => return 0,
        };
        for name in names {
            if *budget == 0 {
                return total;
            }
            let relative = std::path::PathBuf::from(&name);
            let Ok(entry) = dir.entry(Some(&relative)) else {
                continue;
            };
            *budget -= 1;
            match entry.kind {
                fs::EntryType::Directory => match dir.subdir(&relative) {
                    Ok(child) => total += walk(&child, seen, budget),
                    Err(_) => continue,
                },
                fs::EntryType::File => {
                    if seen.contains_key(&entry.inode) {
                        continue;
                    }
                    let Ok(file) = dir.open_file(&relative) else {
                        continue;
                    };
                    let Ok(metadata) = file.metadata() else {
                        continue;
                    };
                    seen.insert(entry.inode, metadata.len());
                    total += metadata.len();
                }
                _ => continue,
            }
        }
        total
    }
    match fs::Dir::open(home) {
        Ok(dir) => walk(&dir, &mut BTreeMap::new(), &mut 4096),
        Err(_) => 0,
    }
}

/// Returns the shared placement every harness launch is bound to.
///
/// The trusted store namespace is created empty when absent, so every launch
/// names the same canonical root whatever the creation order. The roots are
/// the home's verified committed registry mapping when one exists — a
/// still-prepared row is an explicit refusal — and an empty map otherwise:
/// the launch still verifies through the shared bridge and a Codex grant must
/// keep the store outside every writable root.
pub fn launch_shared_assets(
    store: &Store,
    app_home: &Path,
    runtime_home: &Path,
) -> Result<adapters::provider::SharedLaunchAssets> {
    let root = crate::runtime_storage::store_root(app_home)?;
    fs::private_dir(&root)?;
    let roots = shared_launch_assets(store, app_home, runtime_home)?
        .map(|assets| assets.roots)
        .unwrap_or_default();
    Ok(adapters::provider::SharedLaunchAssets {
        store_root: root,
        roots,
    })
}

/// Returns the committed shared placement of one sealed home for its launch
/// plan, or `None` while the home is entirely private.
///
/// A still-prepared row is an explicit refusal, not a private fallback: the
/// caller must recover or commit the installation first.
fn shared_launch_assets(
    store: &Store,
    app_home: &Path,
    runtime_home: &Path,
) -> Result<Option<adapters::provider::SharedLaunchAssets>> {
    let key = runtime_home
        .canonicalize()
        .map_err(|_| invalid("sealed runtime home is not resolvable"))?
        .to_string_lossy()
        .into_owned();
    let Some(record) = store.runtime_storage_layout(&key)? else {
        return Ok(None);
    };
    if record.state != agent_run_store::runtime_storage::LayoutState::Committed {
        return Err(invalid(
            "runtime home has a prepared storage layout; recover it before launching",
        ));
    }
    let store_root = crate::runtime_storage::store_root(app_home)?;
    let roots = record
        .layout()
        .roots
        .iter()
        .map(|(root, mapping)| {
            (
                root.clone(),
                agent_run_platform::shared_assets::SharedTreeRef {
                    scope: mapping.scope.clone(),
                    manifest_sha256: mapping.manifest_sha256.clone(),
                },
            )
        })
        .collect();
    Ok(Some(adapters::provider::SharedLaunchAssets {
        store_root,
        roots,
    }))
}

/// Verifies one sealed provider home through the shared-asset registry,
/// rolling forward an interrupted installation first.
///
/// A prepared row means a previous attempt of this same agent died mid-switch;
/// recovery is provably-owner-checked and idempotent, and the verification
/// after it fails closed on anything still wrong. A private home keeps the
/// original strict verifier.
async fn verify_sealed_home<B: crate::runtime_storage::LayoutBackend>(
    store: &mut B,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
) -> Result<()> {
    let key = runtime_home
        .canonicalize()
        .map_err(|_| invalid("sealed runtime home is not resolvable"))?
        .to_string_lossy()
        .into_owned();
    if matches!(
        store.reader().runtime_storage_layout(&key),
        Ok(Some(record)) if record.state == agent_run_store::runtime_storage::LayoutState::Prepared
    ) {
        crate::runtime_storage::recover_on(store, app_home, runtime_home).await?;
    }
    crate::runtime_storage::verify(store.reader(), app_home, runtime_home, expected)?;
    Ok(())
}

/// Runs an admitted provider attempt through fenced process ownership and the terminal outbox.
/// After the durable outcome, logs only public execution identity and numeric
/// per-stage completion durations; logging never changes that outcome.
/// Checkpoints root and descendant identities through cleanup, and injects an attempt-bound
/// worker capability. Preparation, execution, cleanup and durable-store failures propagate;
/// an unconfirmed cleanup keeps ownership reserved for recovery. Native-owner settings
/// are filtered against the frozen role and actual v2 adapter before process preparation.
async fn execute_provider(home: &Path, id: &AgentId, store: &mut Persist) -> Result<()> {
    if store.cancel_pending(id)? {
        if !persist::attempt_never_spawned(&store.conn, id)? {
            return Err(invalid("provider attempt was already spawning"));
        }
        return cancelled_before_spawn(id, store).await;
    }
    let mut row = store.get(id)?;
    let mut identity = ProviderLaunchIdentity::read(&row)?;
    let config = identity.provider_config.clone();
    let catalog = config.resolve_catalog(store.list_accounts()?)?;
    // `continuing` is set only after an in-flight account switch: the native
    // session of this agent's PREVIOUS attempt and that attempt's recorded
    // adapter state (its history seal). Explicit-resume lineage stays on
    // `row.resume_of_runtime_session_id` and is used only by the first attempt.
    let mut continuing: Option<(String, String)> = None;
    loop {
        let (attempt_id, account) = store.provider_attempt(id)?;
        // Everything this supervisor journals originates from this attempt,
        // including final and cleanup records written after ownership ends.
        store.repin(&attempt_id).await?;
        let harness = config
            .harnesses
            .get(&identity.authority.harness)
            .ok_or_else(|| invalid("admitted provider harness is unavailable"))?;
        let runtime_home = identity
            .runtime_home
            .clone()
            .unwrap_or_else(|| harness.home.join("runs").join(id.as_str()));
        let agent_dir = home.join("agents").join(id.as_str());
        fs::private_dir(&agent_dir)?;
        store
            .event(id, "phase", &json!({"phase":"preparing"}))
            .await?;
        // Native caches precede every child of this attempt: recover this home's own interrupted native operations, then thaw
        // every frozen remote plugin parent, because the native store
        // rewrites those in place and a frozen parent must be private again
        // before anything can write to it.
        crate::runtime_cache::prepare_native(home, &runtime_home, identity.runtime_home.is_some())?;
        if identity.runtime_home.is_none() {
            let role = ResolvedRolePlan::from_payload(&identity.authority.role_payload)?;
            let (_, digest) = adapters::provider::materialize_selected(
                &config,
                &catalog,
                &identity.authority.provider,
                &identity.authority.model,
                &account,
                &role,
                &identity.authority.workdir,
                &runtime_home,
                home,
            )?;
            identity.authority.assets_sha256 = digest.clone();
            identity.snapshot_sha256 = Some(digest.as_str().into());
            identity.runtime_home = Some(runtime_home.clone());
            seal_identity(
                store,
                id,
                &runtime_home,
                digest.as_str(),
                Some(digest.as_str()),
                &format!("snapshot:v2:{}", digest.as_str()),
            )
            .await?;
            // The sealed home, identity and index digest are durable; only
            // now may its managed trees move into the shared store, after the
            // store root and Codex grant are validated, so the next attempt and any resume verify the
            // same original index through the registry.
            let host: BTreeMap<String, String> = std::env::vars().collect();
            let preliminary = adapters::provider::plan_selected_with(
                &config,
                &catalog,
                &identity.authority,
                &account,
                &runtime_home,
                home,
                &host,
                &adapters::authorized_request::SystemCredentialReader,
                identity.provider_request.task.as_str(),
                row.resume_of_runtime_session_id.as_deref(),
                adapters::provider::LaunchOptions {
                    fast: row.request.fast,
                    output_schema: identity.provider_request.output_schema.as_ref(),
                },
                None,
            )?;
            let codex_grant = if identity.authority.harness == HarnessId::Codex {
                Some(crate::codex::Grant::new(
                    &preliminary.runtime,
                    &row.request,
                    &preliminary.profile,
                    home,
                )?)
            } else {
                None
            };
            install_shared_assets(
                store,
                home,
                &runtime_home,
                digest.as_str(),
                id,
                codex_grant.as_ref(),
            )
            .await?;
        } else {
            verify_sealed_home(
                store,
                home,
                &runtime_home,
                identity.authority.assets_sha256.as_str(),
            )
            .await?;
        }
        // The trusted store namespace is ensured before every harness launch
        // and a Codex grant must keep it outside its writable roots. There is
        // no launch wrapper: a Claude or GLM child runs with its own native
        // permissions. A committed shared layout joins the plan through its
        // verified registry mapping; anything still pending was recovered or
        // refused above.
        let shared_assets = launch_shared_assets(store, home, &runtime_home)?;
        if store.cancel_pending(id)? {
            if !persist::attempt_never_spawned(&store.conn, id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            return cancelled_before_spawn(id, store).await;
        }
        row = store.get(id)?;
        let catalog = config.resolve_catalog(store.list_accounts()?)?;
        let host: BTreeMap<String, String> = std::env::vars().collect();
        let resume_session = continuing
            .as_ref()
            .map(|(session, _)| session.as_str())
            .or(row.resume_of_runtime_session_id.as_deref());
        // A switched attempt never resends the original task: it continues
        // the native thread with one explicit internal control turn.
        let task = if continuing.is_some() {
            CONTINUATION_CONTROL
        } else {
            identity.provider_request.task.as_str()
        };
        // The supervisor, not task text, declares the verified new execution.
        // Retained history contains earlier finish receipts which must not fence
        // this separately admitted continuation or overwrite its parent's answer.
        let resumed_task =
            (row.request.explicit_finish && row.parent_agent_id.is_some() && continuing.is_none())
                .then(|| {
                    format!(
                        "{}\n\n{task}",
                        agent_run_domain::worker::EXPLICIT_RESUME_BOUNDARY
                    )
                });
        let task = resumed_task.as_deref().unwrap_or(task);
        let mut planned = adapters::provider::plan_selected_with(
            &config,
            &catalog,
            &identity.authority,
            &account,
            &runtime_home,
            home,
            &host,
            &adapters::authorized_request::SystemCredentialReader,
            task,
            resume_session,
            adapters::provider::LaunchOptions {
                fast: row.request.fast,
                output_schema: identity.provider_request.output_schema.as_ref(),
            },
            Some(&shared_assets),
        )?;
        // Codex keeps its original managed profile and its own native
        // sandbox: the shared store must stay provably outside every root that
        // profile can write and outside the temporary directories the native
        // sandbox grants. The live app-server grant echo is still verified in
        // the runner.
        if identity.authority.harness == HarnessId::Codex {
            crate::codex::Grant::new(&planned.runtime, &row.request, &planned.profile, home)?
                .admits_shared_root(&shared_assets.store_root)?;
        }
        if planned.role.worker_mcp {
            // This attempt-only capability is never part of the sealed home or
            // argv. New attempts receive a new token; the broker checks durable
            // ownership and run liveness for every report.
            let token = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            store
                .issue_worker_capability(id, &attempt_id, &token, domain::now())
                .await?;
            store
                .begin_worker_lifecycle(id, &attempt_id, domain::now())
                .await?;
            planned.launch.environment.insert(
                agent_run_domain::worker::FINISH_ENV.into(),
                if row.request.explicit_finish {
                    "1"
                } else {
                    "0"
                }
                .into(),
            );
            let values = [
                home.to_string_lossy().into_owned(),
                id.as_str().to_owned(),
                attempt_id.clone(),
                token,
            ];
            planned.launch.environment.insert(
                agent_run_domain::worker::RESEARCH_ENV.into(),
                if planned.role.research_tools_only() {
                    "1"
                } else {
                    "0"
                }
                .into(),
            );
            for (name, value) in agent_run_domain::worker::ENV_NAMES.into_iter().zip(values) {
                planned.launch.environment.insert(name.into(), value);
            }
        }
        // Any continuation re-verifies its recorded history seal at the
        // handoff itself, bound to the history root this exact launch plan
        // selects: a seal for one directory never authorizes a launch whose
        // environment points the harness at another. In-flight switches use
        // this agent's previous attempt; explicit resume uses the parent.
        if let Some(session) = resume_session {
            let planned_root = history_root(
                identity.authority.harness,
                &planned.launch.environment,
            )
            .ok_or_else(|| {
                Error::Unsupported(
                    "continuation_unavailable: the launch plan names no native storage".into(),
                )
            })?;
            let recorded = match (&continuing, &row.parent_agent_id) {
                (Some((_, state)), _) => state.clone(),
                (None, Some(parent)) => store.latest_attempt_state(parent)?,
                (None, None) => {
                    return Err(invalid("a resumed attempt has no recorded history source"));
                }
            };
            crate::service::verify_recorded_history(
                &recorded,
                &identity,
                Some(&planned_root),
                session,
            )?;
        }
        // A resumed execution records its thread's usage baseline once, before
        // the first launch, so its terminal statistics report this execution's
        // delta instead of the whole native thread's cumulative total. The
        // store guards applicability (parent session, runtime and model) and
        // keeps every measurement null when no comparable baseline exists.
        if identity.authority.harness == HarnessId::Codex {
            store.record_resume_usage_baseline(id).await?;
        }
        #[cfg(feature = "test-fixtures")]
        fixture_barrier(home, &attempt_id, store, "spawn")?;
        // A switched attempt re-checks, at the handoff itself, that its
        // selected account is still enabled and the current configuration
        // still permits the frozen execution; otherwise it never spawns.
        if continuing.is_some()
            && let Some(blocker) = handoff_blocker(home, store, &account, &identity)?
        {
            if !persist::attempt_never_spawned(&store.conn, id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            store
                .event(id, "failover_blocked", &json!({"reason":blocker}))
                .await?;
            let mut outcome = Outcome::failure("quota_exhausted");
            outcome.failure_text = Some(format!("failover_blocked: {blocker}"));
            store.finish(id, &outcome, None, None).await?;
            return commands::complete_terminal_persisted(store, id).await;
        }
        if !config.services.is_empty() {
            store
                .event(id, "phase", &json!({"phase":"warming_services"}))
                .await?;
        }
        let service_gate = crate::managed_services::wait_for_gate(home, id).await;
        service_gate?;
        if identity.authority.harness == HarnessId::ClaudeCode && !store.cancel_pending(id)? {
            // Discovery children are owned by this attempt: each snapshot is one
            // journal event submitted through the same ordered lane, so the
            // rows buffered so far are written first.
            store.flush().await?;
            let producer = store.producer().clone();
            let ownership_agent = id.clone();
            let ownership_attempt = attempt_id.clone();
            let discovery =
                adapters::mcp_catalog::apply_claude_tool_filters(&mut planned, move |snapshot| {
                    discovery_ownership(&producer, &ownership_agent, &ownership_attempt, snapshot)
                });
            discovery.await?;
        }
        let owner_supported = matches!(
            planned.runtime.kind()?,
            crate::config::Adapter::Codex
                | crate::config::Adapter::Claude
                | crate::config::Adapter::OpenCode
        );
        crate::native_owner::configure(
            &mut planned.launch.environment,
            &row,
            &planned.profile,
            owner_supported,
        );
        if row.request.explicit_finish && planned.role.worker_mcp {
            // Before the spawn claim, give the broker this attempt's final
            // launch secrets: it refuses every first finish intent that
            // carries one, or that arrives while the context is unavailable.
            // A refusal here (an oversized context or a writer error) still
            // leaves a never-spawned attempt that failure handling closes.
            store
                .install_finish_guard(&planned.launch.environment)
                .await?;
        }
        // The spawn claim itself refuses while a cancel is pending, so an
        // accepted cancel can never race past this boundary.
        if !store.provider_spawning(id, &attempt_id).await? {
            if !persist::attempt_never_spawned(&store.conn, id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            return cancelled_before_spawn(id, store).await;
        }
        store
            .event(id, "phase", &json!({"phase":"spawning"}))
            .await?;
        // Process::spawn yields Io only when the OS refused Command::spawn,
        // before a child exists; other failures retain ownership for recovery.
        let spawned = if identity.authority.harness == HarnessId::OpenCode {
            Process::spawn_lease(&planned.launch)
        } else {
            Process::spawn(&planned.launch)
        };
        let mut process = match spawned {
            Ok(process) => process,
            Err(error @ Error::Io(_)) => {
                store.provider_spawn_failed(id, &attempt_id).await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let switched = continuing.is_some();
        let execution = async {
            // The spawned engine is observed (exit, descendants, escalation)
            // while every durable write below is pending, from the first one
            // onward; its ownership is persisted as soon as the attempt anchor
            // is durable.
            let leader = process
                .owner
                .leader
                .clone()
                .ok_or_else(|| Error::Runtime("provider leader identity unavailable".into()))?;
            process
                .observing(async {
                    fixture_hold(home, "provider-process").await;
                    store.provider_process(id, &attempt_id, &leader).await
                })
                .await?;
            process
                .observe_ownership(ownership_sink(store.producer(), attempt_id.clone()))
                .await?;
            if switched {
                // The logical run is already running; its original start
                // time is kept. No user entry is journaled.
                process
                    .observing(store.provider_rerunning(id, &attempt_id, process.owner.pid))
                    .await?;
                process
                    .observing(store.event(
                        id,
                        "continuation_control",
                        &json!({"session":resume_session,"account":account}),
                    ))
                    .await?;
            } else {
                process
                    .observing(store.running(id, process.owner.pid))
                    .await?;
                process
                    .observing(store.journal(
                        id,
                        "user",
                        &identity.provider_request.task,
                        None,
                        None,
                    ))
                    .await?;
            }
            let mut native_row = row.clone();
            native_row.request.task = task.to_owned();
            native_row.resume_of_runtime_session_id = resume_session.map(str::to_owned);
            if identity.authority.harness == HarnessId::OpenCode {
                crate::opencode::run(
                    &mut process,
                    store,
                    &native_row,
                    &row,
                    &planned,
                    &runtime_home,
                )
                .await
            } else if identity.authority.harness == HarnessId::Codex {
                native_row.request.model = planned.native_model.clone();
                crate::codex::run(
                    &mut process,
                    store,
                    &native_row,
                    &row,
                    &planned.runtime,
                    &planned.profile,
                    &runtime_home,
                )
                .await
            } else {
                crate::stream::run(
                    &mut process,
                    store,
                    &native_row,
                    &row,
                    planned.launch.initial_input.as_deref(),
                )
                .await
            }
        };
        let execution = execution.await;
        let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
        let cleanup_ended = std::time::Instant::now();
        let exit = process.reap().await;
        retain_cleanup_evidence(
            &agent_dir,
            &attempt_id,
            &process,
            &cleanup,
            &execution,
            exit,
        )?;
        #[cfg(feature = "test-fixtures")]
        fixture_barrier(home, &attempt_id, store, "terminal")?;
        let checkpoint = process
            .checkpoint_ownership_ready(agent_run_domain::OwnershipStage::Final)
            .await;
        if let Err(error) = checkpoint {
            let _ = store
                .event(
                    id,
                    "ownership_checkpoint_failure_v1",
                    &execution_error_metadata(&error),
                )
                .await;
            // Cleanup diagnostics cannot replace an earlier engine failure.
            // A valid result still fails when its final checkpoint is missing.
            return execution.and(Err(error));
        }
        #[cfg(feature = "test-fixtures")]
        let cleanup = injected_cleanup_error(home, &attempt_id, store, cleanup)?;
        match &cleanup {
            Ok(proof) if !proof.confirmed => {
                store
                    .event(
                        id,
                        "process_cleanup_unverified",
                        &serde_json::to_value(proof)?,
                    )
                    .await?;
            }
            Err(error) => {
                store
                    .event(
                        id,
                        "process_cleanup_unavailable",
                        &json!({"class":error.public().kind}),
                    )
                    .await?;
            }
            _ => {}
        }
        let cleanup = cleanup?;
        store.provider_cleanup(id, &attempt_id, &cleanup).await?;
        store
            .event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)
            .await?;
        let cleanup_recorded = std::time::Instant::now();
        let cancelled = store.cancel_pending(id)?;
        let mut result = match execution {
            Ok(result) => result,
            Err(error) => failed_execution(store, id, &error).await,
        };
        #[cfg(feature = "test-fixtures")]
        inject_native_failure(&mut result, &attempt_id, store, id)?;
        if result.outcome.exit_code.is_none() {
            result.outcome.exit_code = exit;
        }
        // Continuation evidence and the typed native failure are recorded on
        // the attempt; the failure carries trusted supervisor context (never
        // the payload's): this attempt, its account, provider and model.
        let history_started = std::time::Instant::now();
        let mut state = history_evidence(
            store,
            id,
            &attempt_id,
            &identity,
            &planned.launch.environment,
        );
        let history_captured = std::time::Instant::now();
        let quota = matches!(
            result.native_failure,
            Some(adapters::native_failure::NativeFailure::QuotaExhausted { .. })
        );
        if let Some(failure) = &result.native_failure {
            let mut data = serde_json::to_value(failure)?;
            data["attempt"] = json!(attempt_id);
            data["account"] = json!(account);
            data["provider"] = json!(identity.authority.provider);
            data["model"] = json!(identity.authority.model);
            if let Some((lane, window)) = latch_mapping(&identity, failure) {
                let reset = data["resets_at"].as_f64();
                store
                    .latch_native_exhaustion(ops::LatchNativeExhaustion {
                        execution: id.clone(),
                        account: account.clone(),
                        runtime: identity.authority.provider.as_str().to_owned(),
                        lane: lane.clone(),
                        window: window.clone(),
                        source: "native-quota-exhaustion".to_owned(),
                        models: governed_lanes(&identity, &account),
                        observed_at: domain::now(),
                        reset_at: reset,
                    })
                    .await?;
                data["latched"] = json!({"lane":lane,"window":window});
            }
            store.event(id, "native_failure", &data).await?;
            state["native_failure"] = data;
        }
        store
            .provider_history(id, &attempt_id, history_metadata(&state))
            .await?;
        let history_recorded = std::time::Instant::now();
        if quota
            && !cancelled
            && result.outcome.status == Status::Failed
            && store.worker_finish_intent(id, false)?.is_none()
        {
            match failover(home, id, store, &identity).await {
                Ok(next) => {
                    store.event(id, "account_switched", &next).await?;
                    let session = store
                        .get(id)?
                        .runtime_session_id
                        .ok_or_else(|| invalid("switched run lost its native session"))?;
                    continuing = Some((session, state.to_string()));
                    continue;
                }
                Err(blocker) => {
                    store
                        .event(id, "failover_blocked", &json!({"reason":blocker}))
                        .await?;
                    result.outcome.failure_kind = Some("quota_exhausted".into());
                    result.outcome.failure_text = Some(format!("failover_blocked: {blocker}"));
                }
            }
        }
        if row.request.explicit_finish {
            // Only an observed native receipt publishes the exact summary,
            // for failed runs too; a lost reply leaves the run answerless.
            result.answer = crate::worker_finish::answer(&process, store, id)?;
        }
        let answer_started = std::time::Instant::now();
        let proof = match result.answer.as_deref() {
            Some(text) if !text.trim().is_empty() => {
                Some(verify::seal(&agent_dir, Path::new("answer.md"), text)?)
            }
            _ => None,
        };
        let answer_sealed = std::time::Instant::now();
        let evidence = match &proof {
            Some(proof) => verify::AnswerProof::sealed(proof),
            None => verify::AnswerProof::absent(agent_dir.join("answer.md")),
        };
        let outcome = verify::verify_completion(
            Some(result.outcome),
            cancelled.then_some(verify::StopReason::Cancel),
            Some(&evidence),
            cleanup.group_gone,
            store.last_progress(id)?,
            domain::now(),
            verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
        )?;
        let verdict_recorded = std::time::Instant::now();
        // Optional native tree hashing/copying never holds a verified answer
        // behind filesystem work. The existing offline compactor replays the
        // frozen authority under exclusive home/broker ownership; no work is
        // scheduled after terminal state and the private cache remains intact.
        if cleanup.confirmed && identity.authority.harness == HarnessId::Codex {
            let _ = store
                .event(
                    id,
                    "native_cache_consolidation_deferred",
                    &json!({"reason": "offline_compaction"}),
                )
                .await;
        }
        let terminal_started = std::time::Instant::now();
        store
            .finish(id, &outcome, proof.as_ref(), result.usage.as_ref())
            .await?;
        if let Some(logger) = crate::logging::configured() {
            logger.log(crate::logging::Level::Info, &format!("terminal_stage_ms agent_id={} cleanup_record={} history_capture={} history_record={} answer_seal={} verdict={} terminal_write={} total={}", id, cleanup_recorded.duration_since(cleanup_ended).as_millis(), history_captured.duration_since(history_started).as_millis(), history_recorded.duration_since(history_captured).as_millis(), answer_sealed.duration_since(answer_started).as_millis(), verdict_recorded.duration_since(answer_sealed).as_millis(), terminal_started.elapsed().as_millis(), cleanup_ended.elapsed().as_millis()));
        }
        commands::complete_terminal_persisted(store, id).await?;
        return Ok(());
    }
}

/// The explicit internal control turn a switched attempt sends to continue
/// the native conversation. Codex app-server 0.155.1 requires `input` on
/// `turn/start` and `thread/resume` takes none, so a continuation needs one
/// turn; this fixed text is that control — never the original task, a
/// summary or synthesized history.
pub const CONTINUATION_CONTROL: &str = "The previous turn stopped because the account's usage limit was reached. Continue the same task from where it stopped, without repeating completed steps.";

/// Moves an exhausted run to its next account, or names why it cannot.
///
/// Only Codex continues across accounts (its conversation lives in the
/// run's own `CODEX_HOME`); Claude Code keeps history per login, so cross-
/// account continuation is refused. The current configuration must still
/// permit the frozen execution, the run must be automatic and not
/// cancelled, and the next account comes from trusted ranker candidates
/// (frozen scope ∩ current bindings, never an account tried before) through
/// the atomic store allocation, with bounded stale-revision recomputes.
/// Returns the `account_switched` event data, or the typed blocker.
async fn failover(
    home: &Path,
    id: &AgentId,
    store: &mut Persist,
    identity: &ProviderLaunchIdentity,
) -> std::result::Result<serde_json::Value, String> {
    let authority = &identity.authority;
    if authority.harness != HarnessId::Codex {
        return Err("cross_account_continuation_unverified".into());
    }
    if store
        .cancel_pending(id)
        .map_err(|error| error.to_string())?
    {
        return Err("cancelled".into());
    }
    let intent: String = store
        .conn
        .query_row(
            "SELECT selection_intent FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if intent != "auto" {
        return Err("pinned_account".into());
    }
    let (current, _) = agent_run_config::provider_config::ProviderConfig::load(home)
        .map_err(|_| "current_config_unavailable".to_owned())?;
    crate::service::current_policy_permits(&current, identity, &identity.provider_request)
        .map_err(|_| "current_policy_refused".to_owned())?;
    let accounts = store.list_accounts().map_err(|error| error.to_string())?;
    let bound = current_bindings(&current, authority);
    let frozen = identity
        .provider_config
        .resolve_catalog(accounts)
        .map_err(|error| error.to_string())?;
    let hard: std::collections::BTreeSet<_> = frozen
        .provider(&authority.provider)
        .map(|definition| {
            definition
                .bindings
                .iter()
                .map(|binding| binding.account.clone())
                .filter(|account| {
                    !bound.contains(account) || !authority.eligible_accounts.contains(account)
                })
                .collect()
        })
        .unwrap_or_default();
    for _ in 0..=crate::service::PROVIDER_STALE_RETRIES {
        let candidates = crate::capacity::provider_ranking::provider_candidates(
            store,
            &frozen,
            &authority.provider,
            &authority.model,
            None,
            &hard,
        )
        .map_err(|_| "no_eligible_account".to_owned())?;
        let catalog = serde_json::to_value(&frozen).map_err(|error| error.to_string())?;
        let allocation = store
            .allocate_next_attempt(ops::AllocateNextAttempt {
                execution: id.clone(),
                catalog,
                candidates,
            })
            .await;
        match allocation {
            Ok(next) => {
                return Ok(json!({"previous":next.released,"attempt":next.attempt_id,
                    "number":next.number,"account":next.account_id}));
            }
            Err(Error::QuotaAdmission(
                agent_run_domain::catalog::QuotaAdmissionError::SelectionStale { .. },
            )) => continue,
            Err(Error::QuotaAdmission(
                agent_run_domain::catalog::QuotaAdmissionError::NoEligibleAccount { .. },
            )) => return Err("no_eligible_account".into()),
            Err(error) => return Err(error.to_string()),
        }
    }
    Err("selection_busy".into())
}

/// Test-only controlled failure injection: `AGENT_RUN_INJECT_QUOTA=<n>` makes
/// attempt number `n` of every provider run report an authoritative Codex
/// usage-limit failure, so the automatic switch can be exercised against a
/// real harness without draining any quota. Absent in production builds.
#[cfg(feature = "test-fixtures")]
fn inject_native_failure(
    result: &mut adapters::EngineResult,
    attempt: &str,
    store: &Store,
    id: &AgentId,
) -> Result<()> {
    let Some(target) = std::env::var("AGENT_RUN_INJECT_QUOTA")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
    else {
        return Ok(());
    };
    let number: u32 = store.conn.query_row(
        "SELECT number FROM attempts WHERE id=? AND agent_id=?",
        rusqlite::params![attempt, id.as_str()],
        |row| row.get(0),
    )?;
    if number == target {
        result.outcome.status = Status::Failed;
        result.outcome.failure_kind = Some("codex_usageLimitExceeded".into());
        result.answer = None;
        result.native_failure = Some(adapters::native_failure::codex_turn_error(
            &json!({"message":"injected","codexErrorInfo":"usageLimitExceeded"}),
        ));
    }
    Ok(())
}
/// The typed continuation metadata of a history state value: the sealed history,
/// or its bounded unavailable reason (1..512 bytes), and the typed native failure.
fn history_metadata(state: &serde_json::Value) -> ops::ProviderHistoryMetadata {
    let unavailable = state
        .get("native_history_unavailable")
        .and_then(serde_json::Value::as_str)
        .map(|reason| {
            let mut end = reason.len().min(512);
            while !reason.is_char_boundary(end) {
                end -= 1;
            }
            match &reason[..end] {
                "" => "native history is unavailable".to_owned(),
                bounded => bounded.to_owned(),
            }
        });
    ops::ProviderHistoryMetadata {
        native_history: state.get("native_history").cloned(),
        native_history_unavailable: unavailable,
        native_failure: state.get("native_failure").cloned(),
    }
}

/// Seals the finished attempt's native history at its cleanup boundary, in
/// the storage root the attempt's own environment named (`CODEX_HOME`, or
/// `CLAUDE_CONFIG_DIR` falling back to `$HOME/.claude`). Returns the adapter
/// state to record: `native_history` with the seal bound to provider, assets
/// and attempt, or `native_history_unavailable` with the reason. Never fails
/// the finished task itself.
fn history_evidence(
    store: &Store,
    id: &AgentId,
    attempt: &str,
    identity: &ProviderLaunchIdentity,
    environment: &BTreeMap<String, String>,
) -> serde_json::Value {
    let session = match store.get(id).map(|row| row.runtime_session_id) {
        Ok(Some(session)) => session,
        _ => return json!({"native_history_unavailable":"no native session was recorded"}),
    };
    let Some(root) = history_root(identity.authority.harness, environment) else {
        return json!({"native_history_unavailable":"the attempt named no native storage"});
    };
    match crate::continuity::seal(identity.authority.harness, &root, &session) {
        Ok(seal) => {
            // Any failure to prove it (including changed history) is simply
            // unproven: automatic continuation then fails closed.
            let task_proof = task_proof(store, id, attempt, &seal).unwrap_or(None);
            json!({"native_history":{
                "seal": seal,
                "provider": identity.authority.provider,
                "assets_sha256": identity.authority.assets_sha256,
                "attempt": attempt,
                "task_proven": task_proof.is_some(),
                "task_proof": task_proof,
            }})
        }
        Err(error) => json!({"native_history_unavailable": error.to_string()}),
    }
}

/// The proof, as `{"turn","input_sha256"}`, that THIS logical run's
/// original admitted task is present in the history sealed at the end of
/// `attempt`; `None` when it is not provable.
///
/// The first attempt of a logical agent proves its own recorded
/// `native_turn_started` turn (its id and input digest) against `seal`.
/// Every later automatic attempt carries the previous attempt's proof only
/// after re-finding that same original turn in its OWN new seal: its own
/// turn is a continuation-control turn and never counts, and a previous
/// attempt without a proof yields none. A rewritten or truncated current
/// history therefore loses the proof even when an earlier seal had it.
/// Attempts of a different agent (an explicit-resume parent) never count,
/// and matching text is never used.
fn task_proof(
    store: &Store,
    id: &AgentId,
    attempt: &str,
    seal: &crate::continuity::HistorySeal,
) -> Result<Option<serde_json::Value>> {
    let prior: Option<Option<String>> = store
        .conn
        .query_row(
            "SELECT adapter_state_json FROM attempts WHERE agent_id=?1 \
             AND number < (SELECT number FROM attempts WHERE id=?2 AND agent_id=?1) \
             ORDER BY number DESC LIMIT 1",
            rusqlite::params![id.as_str(), attempt],
            |row| row.get(0),
        )
        .optional()?;
    let candidate = match prior {
        // A continuing attempt: only the carried original proof may qualify.
        Some(state) => state
            .and_then(|state| serde_json::from_str::<serde_json::Value>(&state).ok())
            .map(|state| state["native_history"]["task_proof"].clone()),
        // The first attempt: its own admitted turn.
        None => store
            .conn
            .query_row(
                "SELECT data_json FROM events WHERE agent_id=? AND attempt_id=? \
                 AND kind='native_turn_started' ORDER BY seq DESC LIMIT 1",
                rusqlite::params![id.as_str(), attempt],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok()),
    };
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let (Some(turn), Some(digest)) = (
        candidate["turn"].as_str(),
        candidate["input_sha256"].as_str(),
    ) else {
        return Ok(None);
    };
    Ok(
        crate::continuity::admitted_turn_recorded(seal, turn, digest)?
            .then(|| json!({"turn": turn, "input_sha256": digest})),
    )
}
/// The native history storage root a launch environment selects: `CODEX_HOME`
/// for Codex; `CLAUDE_CONFIG_DIR`, else `$HOME/.claude`, for Claude Code.
/// Shared by sealing and the resume handoff so both use one derivation.
fn history_root(
    harness: HarnessId,
    environment: &BTreeMap<String, String>,
) -> Option<std::path::PathBuf> {
    match harness {
        HarnessId::OpenCode => environment
            .get("OPENCODE_DATA_ROOT")
            .map(std::path::PathBuf::from),
        HarnessId::Codex => environment.get("CODEX_HOME").map(std::path::PathBuf::from),
        HarnessId::ClaudeCode => environment
            .get("CLAUDE_CONFIG_DIR")
            .map(std::path::PathBuf::from)
            .or_else(|| {
                environment
                    .get("HOME")
                    .map(|home| std::path::PathBuf::from(home).join(".claude"))
            }),
    }
}
/// The exact physical pool of an authoritative exhaustion, when the
/// provider's own collector mapping establishes it: a Claude Code provider
/// whose limits come from the first-party `anthropic_usage` collector maps
/// `five_hour` to `primary`/`five_hour` and `seven_day` to
/// `secondary`/`seven_day` (both over every bound model, as that collector
/// records them). Model-scoped weekly windows, overage, Codex's windowless
/// `usageLimitExceeded` and every other provider stay unmapped: they only
/// exclude the account for this run.
fn latch_mapping(
    identity: &ProviderLaunchIdentity,
    failure: &adapters::native_failure::NativeFailure,
) -> Option<(String, String)> {
    let adapters::native_failure::NativeFailure::QuotaExhausted {
        window: Some(window),
        ..
    } = failure
    else {
        return None;
    };
    let provider = identity
        .provider_config
        .providers
        .get(&identity.authority.provider)?;
    if provider.limits_source != agent_run_domain::LimitsSource::Exec {
        return None;
    }
    let pool = provider
        .collector
        .as_ref()?
        .exhaustion_windows
        .get(window)?;
    Some((pool.clone(), window.clone()))
}

/// The model lanes (native alias, else id) an explicitly configured native exhaustion pool
/// of `account` governs for this provider: every provider model the
/// account's bindings admit, exactly as that collector builds its unit.
fn governed_lanes(
    identity: &ProviderLaunchIdentity,
    account: &agent_run_domain::catalog::AccountId,
) -> std::collections::BTreeSet<String> {
    let Some(provider) = identity
        .provider_config
        .providers
        .get(&identity.authority.provider)
    else {
        return Default::default();
    };
    provider
        .bindings
        .iter()
        .filter(|binding| &binding.account == account)
        .flat_map(|binding| {
            provider.models.iter().filter(move |model| {
                binding
                    .models
                    .as_ref()
                    .is_none_or(|ids| ids.contains(&model.id))
            })
        })
        .map(|model| {
            model
                .native_model
                .clone()
                .unwrap_or_else(|| model.id.clone())
        })
        .collect()
}

/// Why a switched attempt must not spawn after all: its account is no longer
/// enabled (`account_revoked_at_handoff`), the current configuration no
/// longer permits the frozen execution including its harness and connection
/// (`current_policy_refused`), or the account is no longer bound to the
/// provider for the model (`account_unbound_at_handoff`).
fn handoff_blocker(
    home: &Path,
    store: &Store,
    account: &agent_run_domain::catalog::AccountId,
    identity: &ProviderLaunchIdentity,
) -> Result<Option<&'static str>> {
    let enabled = store
        .account(account)?
        .is_some_and(|record| record.status == agent_run_domain::catalog::AccountStatus::Enabled);
    if !enabled {
        return Ok(Some("account_revoked_at_handoff"));
    }
    let Ok((current, _)) = agent_run_config::provider_config::ProviderConfig::load(home) else {
        return Ok(Some("current_policy_refused"));
    };
    if crate::service::current_policy_permits(&current, identity, &identity.provider_request)
        .is_err()
    {
        return Ok(Some("current_policy_refused"));
    }
    if !current_bindings(&current, &identity.authority).contains(account) {
        return Ok(Some("account_unbound_at_handoff"));
    }
    Ok(None)
}

/// The global accounts the current configuration still binds to the frozen
/// provider for the frozen model (a binding without a model subset binds
/// every model). Empty when the provider is no longer configured. Current
/// bindings only narrow the frozen authority; they never add an account.
fn current_bindings(
    current: &agent_run_config::provider_config::ProviderConfig,
    authority: &agent_run_domain::catalog::ResolvedLaunchAuthority,
) -> std::collections::BTreeSet<agent_run_domain::catalog::AccountId> {
    current
        .providers
        .get(&authority.provider)
        .map(|provider| {
            provider
                .bindings
                .iter()
                .filter(|binding| {
                    binding
                        .models
                        .as_ref()
                        .is_none_or(|models| models.contains(&authority.model))
                })
                .map(|binding| binding.account.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Test-only: while `<home>/fixture-hold-<phase>` exists, the first durable
/// write after the engine was spawned stays pending (it announces
/// `fixture-held-<phase>` and waits, without blocking the executor, up to
/// twenty seconds for `fixture-release-<phase>`). It models a held writer so a
/// test can prove the engine keeps being observed meanwhile. Absent in
/// production.
#[cfg(feature = "test-fixtures")]
async fn fixture_hold(home: &Path, phase: &str) {
    if !home.join(format!("fixture-hold-{phase}")).exists() {
        return;
    }
    let _ = std::fs::write(home.join(format!("fixture-held-{phase}")), "");
    for _ in 0..400 {
        if home.join(format!("fixture-release-{phase}")).exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Production build: no write is ever held.
#[cfg(not(feature = "test-fixtures"))]
async fn fixture_hold(_home: &Path, _phase: &str) {}

/// Test-only finite barrier for the internal spawn or terminal phase. For
/// attempt number n, an existing fixture-pause-PHASE-n marker makes the driver
/// observe fixture-paused-PHASE-n then release fixture-resume-PHASE-n. No SQLite
/// transaction is held while waiting up to twenty seconds. Absent in production.
#[cfg(feature = "test-fixtures")]
fn fixture_barrier(home: &Path, attempt: &str, store: &Store, phase: &str) -> Result<()> {
    let number: u32 =
        store
            .conn
            .query_row("SELECT number FROM attempts WHERE id=?", [attempt], |row| {
                row.get(0)
            })?;
    if !home
        .join(format!("fixture-pause-{phase}-{number}"))
        .exists()
    {
        return Ok(());
    }
    std::fs::write(home.join(format!("fixture-paused-{phase}-{number}")), "")?;
    for _ in 0..400 {
        if home
            .join(format!("fixture-resume-{phase}-{number}"))
            .exists()
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Ok(())
}

/// Test-only: when `<home>/fixture-cleanup-error-<n>` exists for this
/// attempt's number `n`, report the cleanup observation as unavailable, as
/// the platform does when it cannot observe the group. Absent in production.
#[cfg(feature = "test-fixtures")]
fn injected_cleanup_error(
    home: &Path,
    attempt: &str,
    store: &Store,
    cleanup: Result<process::Cleanup>,
) -> Result<Result<process::Cleanup>> {
    let number: u32 =
        store
            .conn
            .query_row("SELECT number FROM attempts WHERE id=?", [attempt], |row| {
                row.get(0)
            })?;
    if home
        .join(format!("fixture-cleanup-error-{number}"))
        .exists()
    {
        return Ok(Err(Error::Runtime(
            "process cleanup observation unavailable".into(),
        )));
    }
    Ok(cleanup)
}
/// Terminal outcome for a cancel observed before any native spawn.
async fn cancelled_before_spawn(id: &AgentId, store: &mut Persist) -> Result<()> {
    let mut outcome = Outcome::failure("cancelled_before_spawn");
    outcome.status = Status::Cancelled;
    store.finish(id, &outcome, None, None).await?;
    commands::complete_terminal_persisted(store, id).await
}

/// Seals the frozen identity of a `starting` execution with its runtime home and
/// snapshot digests (a revision compare-and-swap patch; every other frozen
/// identity byte, including legacy identities of any size, stays untouched).
async fn seal_identity(
    store: &mut Persist,
    id: &AgentId,
    runtime_home: &Path,
    snapshot_sha256: &str,
    assets_sha256: Option<&str>,
    new_revision: &str,
) -> Result<()> {
    let expected: Option<String> = store.conn.query_row(
        "SELECT config_revision FROM agents WHERE id=?",
        [id.as_str()],
        |row| row.get(0),
    )?;
    let expected = expected.ok_or_else(|| invalid("execution has no recorded config revision"))?;
    store
        .update_identity(
            id,
            &expected,
            new_revision,
            &runtime_home.to_string_lossy(),
            snapshot_sha256,
            assets_sha256,
        )
        .await
}

/// [`relocate_retained_home_on`] over a raw store held under the caller's
/// offline authority.
pub fn relocate_retained_home(
    store: &mut Store,
    app_home: &Path,
    agent: &AgentId,
) -> Result<Relocation> {
    crate::runtime_storage::ready(relocate_retained_home_on(store, app_home, agent))
}

/// [`consolidate_retained_native_on`] over a raw store held under the caller's
/// offline authority.
pub fn consolidate_retained_native(
    store: &mut Store,
    app_home: &Path,
    agent: &AgentId,
) -> std::result::Result<serde_json::Value, String> {
    crate::runtime_storage::ready(consolidate_retained_native_on(store, app_home, agent))
}

#[cfg(test)]
/// Closed-diagnostic and failed-result tests (private constructors only).
mod tests {
    use super::{Status, execution_error_metadata, failed_result};

    /// Safe execution causes contain only closed labels and numeric codes.
    /// A failed diagnostic insertion cannot erase the original failed outcome.
    #[test]
    fn execution_failure_metadata_keeps_categories_without_source_contents() {
        let errors = [
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "SOURCE_SECRET",
            )),
            Error::Sql(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(5),
                Some("SOURCE_SECRET".into()),
            )),
            Error::Validation("SOURCE_SECRET".into()),
            Error::Runtime("SOURCE_SECRET".into()),
            Error::Json(serde_json::from_str::<serde_json::Value>("{SOURCE_SECRET").unwrap_err()),
        ];
        for error in &errors {
            let data = execution_error_metadata(error);
            assert!(data.to_string().len() < 4096);
            assert!(!data.to_string().contains("SOURCE_SECRET"));
            let result = failed_result(error);
            assert_eq!(result.outcome.status, Status::Failed);
            assert!(result.answer.is_none());
            assert!(
                !result
                    .outcome
                    .failure_text
                    .unwrap()
                    .contains("SOURCE_SECRET")
            );
        }
        let data = execution_error_metadata(&errors[1]);
        assert_eq!(data["sqlite_code"], "DatabaseBusy");
        assert_eq!(data["sqlite_extended_code"], 5);
        assert_eq!(
            execution_error_metadata(&errors[0])["io_kind"],
            "PermissionDenied"
        );
    }
    use super::error_text;
    use crate::Error;

    /// Checkpoint causes retain their closed phase and SQLite numeric class;
    /// neither public errors nor durable outcomes expose source diagnostics or
    /// turn persistence failures into proof that an owned process died.
    #[test]
    fn ownership_checkpoint_diagnostics_keep_phase_and_hide_source() {
        for stage in [
            agent_run_domain::OwnershipStage::Initial,
            agent_run_domain::OwnershipStage::Streaming,
            agent_run_domain::OwnershipStage::Final,
        ] {
            let error = Error::ownership_checkpoint(
                stage,
                Error::Sql(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),
                    Some("SOURCE_SECRET".into()),
                )),
            );
            let data = execution_error_metadata(&error);
            assert_eq!(data["stage"], serde_json::json!(stage));
            assert_eq!(data["class"], "StorageError");
            assert_eq!(data["sqlite_extended_code"], 5);
            assert!(!data.to_string().contains("SOURCE_SECRET"));
            assert!(!error.public().message.contains("SOURCE_SECRET"));
            let result = failed_result(&error);
            assert_eq!(
                result.outcome.failure_kind.as_deref(),
                Some("runtime_ownership_checkpoint_failed")
            );
            assert_eq!(result.outcome.status, Status::Failed);
            assert!(
                !result
                    .outcome
                    .failure_text
                    .unwrap()
                    .contains("SOURCE_SECRET")
            );
        }
    }

    /// The closed writer phase and the additive `writer_apply` operation label
    /// survive into the closed diagnostic next to the ownership stage, together
    /// with the SQLite numeric codes, wait and attempt counts and no source text.
    #[test]
    fn writer_phase_and_operation_survive_into_the_diagnostic() {
        use agent_run_domain::{error::PersistenceOperation, storage::RejectStage};
        for phase in [
            RejectStage::Acquire,
            RejectStage::Statement,
            RejectStage::Commit,
        ] {
            let error = Error::Persistence {
                writer_identity: None,
                operation: PersistenceOperation::WriterApply,
                elapsed_ms: 17,
                attempts: 3,
                phase: Some(phase),
                source: Box::new(Error::Sql(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(5),
                    Some("SOURCE_SECRET".into()),
                ))),
            };
            let data = execution_error_metadata(&error);
            assert_eq!(data["operation"], "writer_apply");
            assert_eq!(data["phase"], serde_json::json!(phase));
            assert_eq!(data["wait_ms"], 17);
            assert_eq!(data["attempts"], 3);
            assert_eq!(data["sqlite_extended_code"], 5);
            assert!(!data.to_string().contains("SOURCE_SECRET"));
            // Never a retry-eligible ownership acquire, whatever the phase.
            assert!(!error.retryable_ownership_acquire());
        }
        let legacy = Error::persistence(
            PersistenceOperation::TerminalCommit,
            std::time::Duration::from_millis(2),
            Error::Runtime("x".into()),
        );
        assert!(execution_error_metadata(&legacy)["phase"].is_null());
    }

    /// Closed non-enqueue/unknown diagnostics remain distinct, and domain validation
    /// still reaches failed_result as a contract failure rather than a storage failure.
    #[test]
    fn writer_outcomes_are_closed_and_validation_stays_typed() {
        use agent_run_domain::storage::{
            NotEnqueuedReason, RejectCode, RejectStage, StorageRejection, SubmitResult,
            UnknownCause,
        };
        for result in [
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineExpired),
            SubmitResult::NotEnqueued(NotEnqueuedReason::DeadlineBeforeStart),
            SubmitResult::Unknown(UnknownCause::CommitUncertain),
        ] {
            let error = crate::persist::into_result::<()>(result.clone()).unwrap_err();
            let metadata = execution_error_metadata(&error);
            assert_eq!(
                metadata["outcome"],
                if matches!(result, SubmitResult::Unknown(_)) {
                    "unknown"
                } else {
                    "not_enqueued"
                }
            );
            assert_eq!(metadata["operation"], "writer_apply");
        }
        let error = crate::persist::into_result::<()>(SubmitResult::Rejected(
            StorageRejection::new(RejectCode::Validation, RejectStage::Gate, "invalid fixture"),
        ))
        .unwrap_err();
        assert!(matches!(error, Error::Validation(_)));
        assert_eq!(
            failed_result(&error).outcome.failure_kind.as_deref(),
            Some("runtime_contract_rejected")
        );
    }

    /// Mirrors `tests/test_supervisor.py::SupervisorTests::test_blank_startup_error_uses_exception_type_in_ready_failure`.
    #[test]
    fn blank_startup_errors_report_their_error_type() {
        assert_eq!(error_text(&Error::Runtime(String::new())), "RuntimeError");
    }
}

#[cfg(test)]
mod writer_identity_diagnostic_tests {
    use super::*;

    /// A failed write keeps its original unknown outcome, phase and totals,
    /// while supplying the exact immutable receipt key without source text.
    #[test]
    fn writer_identity_survives_failure_diagnostics() {
        let identity = agent_run_domain::storage::OperationId::new("fixture", 1);
        let totals = crate::storage_client::Totals {
            identity: Some((
                identity.clone(),
                agent_run_store::ops::OpKindRef::AppendJournal.wire_id(),
            )),
            attempts: 2,
            elapsed: std::time::Duration::from_secs(30),
            ..Default::default()
        };
        let error = crate::persist::into_result_totals::<()>(
            agent_run_domain::storage::SubmitResult::Unknown(
                agent_run_domain::storage::UnknownCause::DeadlineDuringExecution,
            ),
            &totals,
        )
        .unwrap_err();
        let metadata = execution_error_metadata(&error);
        assert_eq!(metadata["writer_operation_id"], identity.as_str());
        assert_eq!(metadata["writer_op_kind"], "append_journal");
        assert_eq!(metadata["outcome"], "unknown");
        assert_eq!(metadata["wait_ms"], 30000);
        assert_eq!(metadata["attempts"], 2);
        assert_eq!(error.public().message, "state persistence operation failed");
    }
}
