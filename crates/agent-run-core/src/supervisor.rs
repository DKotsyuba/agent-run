//! Separate executable process per admitted run. READY precedes authentication/materialization.
use crate::{
    adapters::{self, io::Process, materialize},
    config::Adapter,
    domain::{AgentId, Outcome, Status},
    error::invalid,
    frame, fs, process,
    service::LaunchIdentity,
    state::Store,
    verify, Error, Result,
};
use serde_json::json;
use std::{io::Write, path::Path, process::Stdio, time::Duration};
use tokio::{io::BufReader, process::Command};

pub async fn launch(home: &Path, id: &AgentId) -> Result<()> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--home")
        .arg(home)
        .arg("_supervisor")
        .arg(id.as_str())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(false);
    use std::os::unix::process::CommandExt;
    // SAFETY: the pre-exec hook calls only async-signal-safe setsid. No allocation,
    // locks, logging, or Rust runtime operation takes place in the forked child.
    unsafe {
        command.as_std_mut().pre_exec(|| {
            if libc::setsid() < 0 {
                Err(std::io::Error::last_os_error())
            } else {
                Ok(())
            }
        });
    }
    let mut child = command.spawn()?;
    if let Some(pid) = child.id() {
        let evidence = process::inspect(pid as i32)
            .ok()
            .and_then(|p| serde_json::to_value(p).ok())
            .unwrap_or_else(|| json!({"pid":pid}));
        let store = Store::open(home)?;
        store.conn.execute(
            "UPDATE agents SET startup_owner_pid_identity=? WHERE id=? AND supervisor_pid IS NULL",
            rusqlite::params![serde_json::to_string(&evidence)?, id.as_str()],
        )?;
    }
    let output = child
        .stdout
        .take()
        .ok_or_else(|| Error::Runtime("supervisor READY pipe missing".into()))?;
    let ready = tokio::time::timeout(
        Duration::from_secs(15),
        frame::read(&mut BufReader::new(output), 4096),
    )
    .await;
    // Reap only the supervisor process when it eventually ends; dropping a
    // client's connection cannot abort the detached run or its ownership.
    tokio::spawn(async move {
        let _ = child.wait().await;
    });
    match ready {
        Ok(Ok(Some(bytes))) => {
            let v: serde_json::Value = serde_json::from_slice(&bytes)?;
            if v.get("ready").and_then(|v| v.as_bool()) != Some(true)
                || v.get("agent_id").and_then(|v| v.as_str()) != Some(id.as_str())
            {
                return Err(Error::Runtime("invalid supervisor READY".into()));
            }
            Ok(())
        }
        _ => Err(Error::Runtime(
            "supervisor ownership handoff was not observed".into(),
        )),
    }
}
pub async fn run(home: &Path, id: &AgentId) -> Result<()> {
    let mut store = Store::open(home)?;
    let owner = process::inspect(std::process::id() as i32)?;
    store.set_owner(id, owner.pid, &owner.token, Some(owner.birth))?;
    // A disconnected READY reader cannot undo an already committed owner.
    let _ = writeln!(std::io::stdout(), "{}", json!({"ready":true,"agent_id":id}));
    let _ = std::io::stdout().flush();
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
    let outcome = verify::completion(result.outcome, proof.as_ref(), cleanup.confirmed, cancelled);
    store.finish(id, &outcome, proof.as_ref(), result.usage.as_ref())?;
    Ok(())
}
fn cancelled_before_spawn(id: &AgentId, store: &mut Store) -> Result<()> {
    let mut outcome = Outcome::failure("cancelled_before_spawn");
    outcome.status = Status::Cancelled;
    store.finish(id, &outcome, None, None)
}
