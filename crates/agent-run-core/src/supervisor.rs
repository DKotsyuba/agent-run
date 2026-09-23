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
    let host: BTreeMap<String, String> = std::env::vars().collect();
    let planned = adapters::provider::plan_selected_with(
        &config,
        &catalog,
        &identity.authority,
        &account,
        &runtime_home,
        home,
        &host,
        &adapters::authorized_request::SystemCredentialReader,
        &identity.provider_request.task,
        row.resume_of_runtime_session_id.as_deref(),
        adapters::provider::LaunchOptions {
            fast: identity.provider_request.fast,
            output_schema: identity.provider_request.output_schema.as_ref(),
        },
    )?;
    // A resumed attempt re-verifies the parent's recorded history seal at the
    // handoff itself, bound to the history root this exact launch plan
    // selects: a seal for one directory never authorizes a launch whose
    // environment points the harness at another.
    if let (Some(session), Some(parent)) = (&row.resume_of_runtime_session_id, &row.parent_agent_id)
    {
        let planned_root = history_root(identity.authority.harness, &planned.launch.environment)
            .ok_or_else(|| {
                Error::Unsupported(
                    "continuation_unavailable: the launch plan names no native storage".into(),
                )
            })?;
        crate::service::verify_recorded_history(
            &store.latest_attempt_state(parent)?,
            &identity,
            Some(&planned_root),
            session,
        )?;
    }
    store.provider_spawning(id, &attempt_id)?;
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
    let execution = async {
        let leader = process
            .owner
            .leader
            .as_ref()
            .ok_or_else(|| Error::Runtime("provider leader identity unavailable".into()))?;
        store.provider_process(id, &attempt_id, leader)?;
        store.running(id, process.owner.pid)?;
        crate::journal(
            store,
            id,
            "user",
            &identity.provider_request.task,
            None,
            None,
        )?;
        if identity.authority.harness == HarnessId::Codex {
            let mut native_row = row.clone();
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
                &row,
                planned.launch.initial_input.as_deref(),
            )
            .await
        }
    }
    .await;
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    let exit = process.reap().await;
    let cleanup = cleanup?;
    store.provider_cleanup(id, &attempt_id, &cleanup)?;
    store.event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)?;
    let history = history_evidence(
        store,
        id,
        &attempt_id,
        &identity,
        &planned.launch.environment,
    );
    store.provider_history(id, &attempt_id, &history)?;
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
    if result.outcome.exit_code.is_none() {
        result.outcome.exit_code = exit;
    }
    // A typed native failure is journaled with trusted supervisor context
    // (never the payload's): this attempt, its account, provider and model.
    if let Some(failure) = &result.native_failure {
        let mut data = serde_json::to_value(failure)?;
        data["attempt"] = json!(attempt_id);
        data["account"] = json!(account);
        data["provider"] = json!(identity.authority.provider);
        data["model"] = json!(identity.authority.model);
        store.event(id, "native_failure", &data)?;
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
        cancelled.then_some(verify::StopReason::Cancel),
        Some(&evidence),
        cleanup.group_gone,
        store.last_progress(id)?,
        domain::now(),
        verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
    )?;
    store.finish(id, &outcome, proof.as_ref(), result.usage.as_ref())?;
    commands::complete_terminal(store, id)?;
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
