//! Separate executable process per admitted run. READY precedes authentication/materialization.
use crate::{
    adapters::{self, io::Process, materialize},
    config::Adapter,
    domain::{self, AgentId, Outcome, Status},
    error::invalid,
    fs, launch, process,
    service::LaunchIdentity,
    state::Store,
    verify, Error, Result,
};
use serde_json::json;
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
/// Child side: prove session identity, commit ownership, then report READY.
///
/// `fds` are the inherited ready, identity and error descriptors.
pub async fn run(home: &Path, id: &AgentId, fds: [i32; 3]) -> Result<()> {
    let [ready_fd, identity_fd, error_fd] = fds;
    if let Err(error) = launch::report_identity(identity_fd, error_fd) {
        let _ = launch::report_ready(ready_fd, Err(&error.to_string()));
        return Err(error.into());
    }
    let owned = (|| {
        let store = Store::open(home)?;
        let owner = process::inspect(std::process::id() as i32)?;
        store.set_owner(id, owner.pid, &owner.token, Some(owner.birth))?;
        Ok::<_, Error>(store)
    })();
    // A disconnected READY reader cannot undo an already committed owner.
    let mut store = match owned {
        Ok(store) => {
            let _ = launch::report_ready(ready_fd, Ok(()));
            store
        }
        Err(error) => {
            let _ = launch::report_ready(ready_fd, Err(&error.to_string()));
            return Err(error);
        }
    };
    match execute(home, id, &mut store).await {
        Ok(()) => Ok(()),
        Err(error) => {
            let row = store.get(id)?;
            if !row.status.terminal() {
                let mut outcome = Outcome::failure(match &error {
                    Error::Integrity(_) => "snapshot_integrity_failed",
                    Error::Validation(_) => "preparation_rejected",
                    _ => "supervisor_failed",
                });
                outcome.failure_text = Some(error.public().message);
                if store.cancel_pending(id)? {
                    outcome.status = Status::Cancelled;
                }
                store.finish(id, &outcome, None, None)?;
            }
            Err(error)
        }
    }
}
/// Run one admitted agent through preparation, supervision, cleanup, and terminal storage.
async fn execute(home: &Path, id: &AgentId, store: &mut Store) -> Result<()> {
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
        runtime.home.join("runs").join(id.as_str())
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
            _ => {
                crate::stream::run(
                    &mut process,
                    store,
                    &row,
                    runtime.kind()?,
                    plan.initial_input.as_deref(),
                )
                .await
            }
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
    Ok(())
}
fn cancelled_before_spawn(id: &AgentId, store: &mut Store) -> Result<()> {
    let mut outcome = Outcome::failure("cancelled_before_spawn");
    outcome.status = Status::Cancelled;
    store.finish(id, &outcome, None, None)
}
