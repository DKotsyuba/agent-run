//! Separate executable process per admitted run. READY precedes authentication/materialization.
use crate::{
    adapters::{self, io::Process, materialize},
    commands,
    config::Adapter,
    domain::{self, AgentId, Outcome, Status},
    error::invalid,
    fs, launch, process,
    service::{LaunchIdentity, ProviderLaunchIdentity},
    state::Store,
    verify, Error, Result,
};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::catalog::HarnessId;
use rusqlite::{params, TransactionBehavior};
use serde_json::json;
use std::collections::BTreeMap;
use std::{ffi::OsStr, path::Path, time::Duration};

/// Start one detached `_supervisor` session leader and return after its READY.
///
/// Spawning is `posix_spawn(POSIX_SPAWN_SETSID)`-first (`launch.rs`). A failed
/// READY read does not cancel the already admitted durable job.
pub async fn launch(home: &Path, id: &AgentId) -> Result<()> {
    let program = std::env::current_exe()?;
    let (home, id) = (home.to_path_buf(), id.clone());
    let launched = tokio::task::spawn_blocking(move || {
        let args = [
            OsStr::new("--home"),
            home.as_os_str(),
            OsStr::new("_supervisor"),
            OsStr::new(id.as_str()),
        ];
        launch::launch_detached(&program, &args, launch::Timeouts::default(), |pid, backend| {
            // Durable before any proof, so reconciliation can observe this child.
            let mut evidence = process::inspect(pid)
                .ok()
                .and_then(|p| serde_json::to_value(p).ok())
                .unwrap_or_else(|| json!({"pid": pid}));
            evidence["spawn_backend"] = json!(backend);
            let store = Store::open(&home).map_err(|e| e.to_string())?;
            store
                .conn
                .execute(
                    "UPDATE agents SET startup_owner_pid_identity=? WHERE id=? AND supervisor_pid IS NULL",
                    rusqlite::params![evidence.to_string(), id.as_str()],
                )
                .map(drop)
                .map_err(|e| e.to_string())
        })
    })
    .await
    .map_err(|e| Error::Runtime(format!("supervisor launch task failed: {e}")))?;
    match launched {
        Ok(launched) => {
            // Exact-PID reaper; the committed owner record stands even if it cannot start.
            let _ = launch::spawn_reaper(launched.pid);
            Ok(())
        }
        Err(launch::LaunchError::Spawn(error)) => Err(Error::Io(error)),
        // A child that died before its PID proof owns nothing: fail like a failed spawn.
        Err(error @ launch::LaunchError::Bootstrap(_)) => {
            Err(Error::Io(std::io::Error::other(error.to_string())))
        }
        Err(error) => Err(Error::Runtime(error.to_string())),
    }
}
/// Child side: prove session identity, commit ownership and heartbeat, then report READY.
///
/// `fds` are the inherited ready, identity and error descriptors.  The first
/// heartbeat makes the complete ownership record eligible for later recovery.
pub async fn run(home: &Path, id: &AgentId, fds: [i32; 3]) -> Result<()> {
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
    let owned = (|| {
        let mut store = Store::open(home)?;
        let owner = process::inspect(std::process::id() as i32)?;
        record_owner(&mut store, id, &owner)?;
        Ok::<_, Error>(store)
    })();
    // A disconnected READY reader cannot undo an already committed owner.
    let mut store = match owned {
        Ok(store) => {
            let _ = launch::report_ready(ready_fd, Ok(()));
            store
        }
        Err(error) => {
            let message = error_text(&error);
            let _ = launch::report_ready(ready_fd, Err(&message));
            return Err(error);
        }
    };
    match execute(home, id, &mut store).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let row = store.get(id)?;
            if !row.status.terminal() {
                if row
                    .identity
                    .as_ref()
                    .is_some_and(|value| value["provider_identity_version"] == 2)
                    && !store.provider_never_spawned(id)?
                {
                    return Err(error);
                }
                let mut outcome = Outcome::failure(preparation_failure_kind(&error));
                outcome.failure_text = Some(error.public().message);
                if store.cancel_pending(id)? {
                    outcome.status = Status::Cancelled;
                }
                store.finish(id, &outcome, None, None)?;
                commands::complete_terminal(&mut store, id)?;
            }
            Err(error)
        }
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

/// Atomically bind the supervising process and its first recovery heartbeat.
///
/// `owner` is the inspected current supervisor, whose PID/token/birth values
/// become immutable ownership evidence.  The row must still be an unowned
/// `starting` admission or this returns `Conflict`; no partial owner row is
/// committed if the accompanying event cannot be appended.
fn record_owner(store: &mut Store, id: &AgentId, owner: &process::Identity) -> Result<()> {
    let tx = store
        .conn
        .transaction_with_behavior(TransactionBehavior::Immediate)?;
    let heartbeat = domain::now();
    let changed = tx.execute(
        "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,heartbeat_at=? \
         WHERE id=? AND status IN ('starting','cancelling') AND supervisor_pid IS NULL",
        params![owner.pid, owner.token, owner.birth, heartbeat, id.as_str()],
    )?;
    if changed != 1 {
        return Err(Error::Conflict);
    }
    tx.execute(
        "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) \
         VALUES(?,(SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1),?,?,?)",
        params![
            id.as_str(),
            id.as_str(),
            heartbeat,
            "supervisor_ready",
            json!({"pid":owner.pid}).to_string()
        ],
    )?;
    tx.commit()?;
    Ok(())
}
/// Run one admitted agent through preparation, supervision, cleanup, and terminal storage.
async fn execute(home: &Path, id: &AgentId, store: &mut Store) -> Result<()> {
    let first = store.get(id)?;
    if first
        .identity
        .as_ref()
        .is_some_and(|value| value["provider_identity_version"] == 2)
    {
        return execute_provider(home, id, store).await;
    }
    if store.cancel_pending(id)? {
        return cancelled_before_spawn(id, store);
    }
    let mut row = store.get(id)?;
    let mut identity = LaunchIdentity::read(&row)?;
    let runtime = identity.config.runtime(&row.request.runtime)?.clone();
    adapters::validate(&row.request, &runtime, &identity.profile)?;
    let agent_dir = home.join("agents").join(id.as_str());
    fs::private_dir(&agent_dir)?;
    store.event(id, "phase", &json!({"phase":"preparing"}))?;
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
    store.update_identity(id, &serde_json::to_value(&identity)?, revision)?;
    if store.cancel_pending(id)? {
        return cancelled_before_spawn(id, store);
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
    store.event(id, "phase", &json!({"phase":"spawning"}))?;
    let mut process = Process::spawn(&plan)?;
    // From this point EVERY path must clean up before returning, including a
    // database failure immediately after spawning the engine.
    let execution = async {
        store.running(id, process.owner.pid)?;
        crate::journal(store, id, "user", &row.request.task, None, None)?;
        match runtime.kind()? {
            Adapter::Codex => {
                crate::codex::run(
                    &mut process,
                    store,
                    &row,
                    &runtime,
                    &identity.profile,
                    &runtime_home,
                )
                .await
            }
            _ => crate::stream::run(&mut process, store, &row, plan.initial_input.as_deref()).await,
        }
    }
    .await;
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    let exit = process.reap().await;
    let cleanup = cleanup?;
    store.event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)?;
    let cancelled = store.cancel_pending(id)?;
    let mut result = match execution {
        Ok(result) => result,
        Err(error) => adapters::EngineResult {
            native_failure: None,
            outcome: Outcome::failure(match error {
                Error::Integrity(_) => "runtime_integrity_failed",
                Error::Validation(_) => "runtime_contract_rejected",
                _ => "runtime_transport_failed",
            }),
            answer: None,
            usage: None,
        },
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
        cleanup.group_gone,
        last_progress_at,
        domain::now(),
        verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
    )?;
    store.finish(id, &outcome, proof.as_ref(), result.usage.as_ref())?;
    commands::complete_terminal(store, id)?;
    Ok(())
}

/// Runs one admitted v2 attempt through the existing detached supervisor,
/// process-identity fence, transcript, cleanup verifier, and terminal outbox.
async fn execute_provider(home: &Path, id: &AgentId, store: &mut Store) -> Result<()> {
    if store.cancel_pending(id)? {
        if !store.provider_never_spawned(id)? {
            return Err(invalid("provider attempt was already spawning"));
        }
        return cancelled_before_spawn(id, store);
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
        store.bind_attempt(&attempt_id);
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
        store.event(id, "phase", &json!({"phase":"preparing"}))?;
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
            store.update_identity(
                id,
                &serde_json::to_value(&identity)?,
                &format!("snapshot:v2:{}", digest.as_str()),
            )?;
        } else {
            materialize::verify(&runtime_home, identity.authority.assets_sha256.as_str())?;
        }
        if store.cancel_pending(id)? {
            if !store.provider_never_spawned(id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            return cancelled_before_spawn(id, store);
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
        let planned = adapters::provider::plan_selected_with(
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
                fast: identity.provider_request.fast,
                output_schema: identity.provider_request.output_schema.as_ref(),
            },
        )?;
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
                    return Err(invalid("a resumed attempt has no recorded history source"))
                }
            };
            crate::service::verify_recorded_history(
                &recorded,
                &identity,
                Some(&planned_root),
                session,
            )?;
        }
        #[cfg(feature = "test-fixtures")]
        spawn_barrier(home, &attempt_id, store)?;
        // A switched attempt re-checks, at the handoff itself, that its
        // selected account is still enabled and the current configuration
        // still permits the frozen execution; otherwise it never spawns.
        if continuing.is_some() {
            if let Some(blocker) = handoff_blocker(home, store, &account, &identity)? {
                if !store.provider_never_spawned(id)? {
                    return Err(invalid("provider attempt was already spawning"));
                }
                store.event(id, "failover_blocked", &json!({"reason":blocker}))?;
                let mut outcome = Outcome::failure("quota_exhausted");
                outcome.failure_text = Some(format!("failover_blocked: {blocker}"));
                store.finish(id, &outcome, None, None)?;
                return commands::complete_terminal(store, id);
            }
        }
        // The run's one overall deadline (admission time + its timeout) is
        // re-read from durable state before every spawn; an expired run never
        // spawns another attempt.
        let deadline = run_deadline(store, id)?;
        if domain::now() >= deadline && !store.cancel_pending(id)? {
            if !store.provider_never_spawned(id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            return timed_out_before_spawn(id, store);
        }
        // The spawn claim itself refuses while a cancel is pending, so an
        // accepted cancel can never race past this boundary.
        if !store.provider_spawning(id, &attempt_id)? {
            if !store.provider_never_spawned(id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            return cancelled_before_spawn(id, store);
        }
        store.event(id, "phase", &json!({"phase":"spawning"}))?;
        // Process::spawn yields Io only when the OS refused Command::spawn,
        // before a child exists; other failures retain ownership for recovery.
        let mut process = match Process::spawn(&planned.launch) {
            Ok(process) => process,
            Err(error @ Error::Io(_)) => {
                store.provider_spawn_failed(id, &attempt_id)?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let switched = continuing.is_some();
        let remaining = std::time::Duration::from_secs_f64((deadline - domain::now()).max(0.0));
        let execution = async {
            let leader = process
                .owner
                .leader
                .as_ref()
                .ok_or_else(|| Error::Runtime("provider leader identity unavailable".into()))?;
            store.provider_process(id, &attempt_id, leader)?;
            if switched {
                // The logical run is already running; its start time (and
                // so any deadline) is kept. No user entry is journaled.
                store.provider_rerunning(id, &attempt_id, process.owner.pid)?;
                store.event(
                    id,
                    "continuation_control",
                    &json!({"session":resume_session,"account":account}),
                )?;
            } else {
                store.running(id, process.owner.pid)?;
                crate::journal(
                    store,
                    id,
                    "user",
                    &identity.provider_request.task,
                    None,
                    None,
                )?;
            }
            let mut native_row = row.clone();
            native_row.request.task = task.to_owned();
            native_row.resume_of_runtime_session_id = resume_session.map(str::to_owned);
            if identity.authority.harness == HarnessId::Codex {
                native_row.request.model = planned.native_model.clone();
                crate::codex::run(
                    &mut process,
                    store,
                    &native_row,
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
                    planned.launch.initial_input.as_deref(),
                )
                .await
            }
        };
        // Only the remainder of the run's deadline is available to this
        // attempt; on expiry the runner is dropped and the group cleaned.
        let (execution, expired) = match tokio::time::timeout(remaining, execution).await {
            Ok(execution) => (execution, false),
            Err(_) => (Err(Error::Runtime("run deadline expired".into())), true),
        };
        let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
        let exit = process.reap().await;
        #[cfg(feature = "test-fixtures")]
        let cleanup = injected_cleanup_error(home, &attempt_id, store, cleanup)?;
        let cleanup = cleanup?;
        store.provider_cleanup(id, &attempt_id, &cleanup)?;
        store.event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)?;
        let cancelled = store.cancel_pending(id)?;
        let mut result = match execution {
            Ok(result) => result,
            Err(error) => adapters::EngineResult {
                native_failure: None,
                outcome: Outcome::failure(match error {
                    Error::Integrity(_) => "runtime_integrity_failed",
                    Error::Validation(_) => "runtime_contract_rejected",
                    _ => "runtime_transport_failed",
                }),
                answer: None,
                usage: None,
            },
        };
        #[cfg(feature = "test-fixtures")]
        inject_native_failure(&mut result, &attempt_id, store, id)?;
        if result.outcome.exit_code.is_none() {
            result.outcome.exit_code = exit;
        }
        // Continuation evidence and the typed native failure are recorded on
        // the attempt; the failure carries trusted supervisor context (never
        // the payload's): this attempt, its account, provider and model.
        let mut state = history_evidence(
            store,
            id,
            &attempt_id,
            &identity,
            &planned.launch.environment,
        );
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
                store.latch_native_exhaustion(
                    &account,
                    lane,
                    window,
                    "claude-rate-limit-event",
                    domain::now(),
                    reset,
                )?;
                data["latched"] = json!({"lane":lane,"window":window});
            }
            store.event(id, "native_failure", &data)?;
            state["native_failure"] = data;
        }
        store.provider_history(id, &attempt_id, &state)?;
        // Cancellation wins over expiry; an expired run takes no next attempt.
        let expired = expired || domain::now() >= run_deadline(store, id)?;
        if expired && !cancelled {
            store.event(id, "run_deadline_expired", &json!({"deadline":deadline}))?;
        }
        if quota && !cancelled && !expired && result.outcome.status == Status::Failed {
            match failover(home, id, store, &identity) {
                Ok(next) => {
                    store.event(id, "account_switched", &next)?;
                    let session = store
                        .get(id)?
                        .runtime_session_id
                        .ok_or_else(|| invalid("switched run lost its native session"))?;
                    continuing = Some((session, state.to_string()));
                    continue;
                }
                Err(blocker) => {
                    store.event(id, "failover_blocked", &json!({"reason":blocker}))?;
                    result.outcome.failure_kind = Some("quota_exhausted".into());
                    result.outcome.failure_text = Some(format!("failover_blocked: {blocker}"));
                }
            }
        }
        let proof = match result.answer.as_deref() {
            Some(text) if !text.trim().is_empty() => {
                Some(verify::seal(&agent_dir, Path::new("answer.md"), text)?)
            }
            _ => None,
        };
        let evidence = match &proof {
            Some(proof) => verify::AnswerProof::sealed(proof),
            None => verify::AnswerProof::absent(agent_dir.join("answer.md")),
        };
        let outcome = verify::verify_completion(
            Some(result.outcome),
            if cancelled {
                Some(verify::StopReason::Cancel)
            } else {
                expired.then_some(verify::StopReason::Timeout)
            },
            Some(&evidence),
            cleanup.group_gone,
            store.last_progress(id)?,
            domain::now(),
            verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
        )?;
        store.finish(id, &outcome, proof.as_ref(), result.usage.as_ref())?;
        commands::complete_terminal(store, id)?;
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
fn failover(
    home: &Path,
    id: &AgentId,
    store: &mut Store,
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
    let bound: std::collections::BTreeSet<_> = current
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
        .unwrap_or_default();
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
        match store.allocate_next_attempt(id, &frozen, &candidates) {
            Ok(next) => {
                return Ok(json!({"previous":next.released,"attempt":next.attempt_id,
                    "number":next.number,"account":next.account_id}))
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
        Ok(seal) => json!({"native_history":{
            "seal": seal,
            "provider": identity.authority.provider,
            "assets_sha256": identity.authority.assets_sha256,
            "attempt": attempt,
        }}),
        Err(error) => json!({"native_history_unavailable": error.to_string()}),
    }
}
/// The native history storage root a launch environment selects: `CODEX_HOME`
/// for Codex; `CLAUDE_CONFIG_DIR`, else `$HOME/.claude`, for Claude Code.
/// Shared by sealing and the resume handoff so both use one derivation.
fn history_root(
    harness: HarnessId,
    environment: &BTreeMap<String, String>,
) -> Option<std::path::PathBuf> {
    match harness {
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
) -> Option<(&'static str, &'static str)> {
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
    if identity.authority.harness != HarnessId::ClaudeCode
        || provider.limits_source != agent_run_domain::LimitsSource::Lua
        || provider.collector.as_ref()?.script != "anthropic_usage"
    {
        return None;
    }
    match window.as_str() {
        "five_hour" => Some(("primary", "five_hour")),
        "seven_day" => Some(("secondary", "seven_day")),
        _ => None,
    }
}

/// Why a switched attempt must not spawn after all: its account is no longer
/// enabled (`account_revoked_at_handoff`) or the current configuration no
/// longer permits the frozen execution (`current_policy_refused`).
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
    let permitted = agent_run_config::provider_config::ProviderConfig::load(home)
        .ok()
        .is_some_and(|(current, _)| {
            crate::service::current_policy_permits(&current, identity, &identity.provider_request)
                .is_ok()
        });
    Ok((!permitted).then_some("current_policy_refused"))
}

/// Test-only handoff barrier: when `<home>/fixture-pause-spawn-<n>` exists
/// for this attempt's number `n`, write `fixture-paused-spawn-<n>` and wait
/// (at most 20 s) for `fixture-resume-spawn-<n>`. Absent in production.
#[cfg(feature = "test-fixtures")]
fn spawn_barrier(home: &Path, attempt: &str, store: &Store) -> Result<()> {
    let number: u32 =
        store
            .conn
            .query_row("SELECT number FROM attempts WHERE id=?", [attempt], |row| {
                row.get(0)
            })?;
    if !home.join(format!("fixture-pause-spawn-{number}")).exists() {
        return Ok(());
    }
    std::fs::write(home.join(format!("fixture-paused-spawn-{number}")), "")?;
    for _ in 0..400 {
        if home.join(format!("fixture-resume-spawn-{number}")).exists() {
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

/// The provider run's one overall deadline: its durable admission time plus
/// its stored timeout. It spans every attempt and preparation; a switch never
/// restarts it.
fn run_deadline(store: &Store, id: &AgentId) -> Result<f64> {
    Ok(store.conn.query_row(
        "SELECT created_at + timeout_seconds FROM agents WHERE id=?",
        [id.as_str()],
        |row| row.get(0),
    )?)
}
/// Ends a run whose deadline expired before its next attempt spawned; the
/// caller has already closed that attempt with never-spawned evidence.
fn timed_out_before_spawn(id: &AgentId, store: &mut Store) -> Result<()> {
    let mut outcome = Outcome::failure("timed_out");
    outcome.status = Status::TimedOut;
    store.finish(id, &outcome, None, None)?;
    commands::complete_terminal(store, id)
}
fn cancelled_before_spawn(id: &AgentId, store: &mut Store) -> Result<()> {
    let mut outcome = Outcome::failure("cancelled_before_spawn");
    outcome.status = Status::Cancelled;
    store.finish(id, &outcome, None, None)?;
    commands::complete_terminal(store, id)
}

#[cfg(test)]
mod tests {
    use super::error_text;
    use crate::Error;

    /// Mirrors `tests/test_supervisor.py::SupervisorTests::test_blank_startup_error_uses_exception_type_in_ready_failure`.
    #[test]
    fn blank_startup_errors_report_their_error_type() {
        assert_eq!(error_text(&Error::Runtime(String::new())), "RuntimeError");
    }
}
