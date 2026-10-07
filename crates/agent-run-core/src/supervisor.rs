//! Separate executable process per admitted run. READY precedes authentication/materialization.
use crate::{
    Error, Result,
    adapters::{self, io::Process, materialize},
    commands,
    config::Adapter,
    domain::{self, AgentId, Outcome, Status},
    error::invalid,
    fs, launch, process,
    service::{LaunchIdentity, ProviderLaunchIdentity},
    state::Store,
    verify,
};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::catalog::{AccountId, HarnessId};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use serde_json::json;
use std::collections::BTreeMap;
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
    time::Duration,
};

/// Start one detached `_supervisor` session leader and return after its READY.
///
/// Spawning is `posix_spawn(POSIX_SPAWN_SETSID)`-first (`launch.rs`). A failed
/// READY read does not cancel the already admitted durable job.
pub async fn launch(home: &Path, id: &AgentId) -> Result<()> {
    // Test-only: `<home>/fixture-supervisor-spawn-error` makes the OS-level
    // spawn refusal observable without a real exhausted process table.
    #[cfg(feature = "test-fixtures")]
    if home.join("fixture-supervisor-spawn-error").exists() {
        return Err(Error::Io(std::io::Error::other(
            "injected supervisor spawn refusal",
        )));
    }
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
/// Execution errors are logged by fixed public class without persisting task
/// text or credentials, including errors whose owned process prevents finish.
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
            if let Some(logger) = crate::logging::configured() {
                logger.log(
                    crate::logging::Level::Error,
                    &format!(
                        "supervisor execution failed agent_id={} class={}",
                        id,
                        error.public().kind
                    ),
                );
            }
            let row = store.get(id)?;
            if !row.status.terminal() {
                if let Ok((attempt, _)) = store.provider_attempt(id)
                    && !crate::lifecycle::reconcile::cleanup_mcp_discovery(&store, &attempt)?
                {
                    return Err(error);
                }
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
    // The same one run deadline as a provider run: admission time plus the
    // stored (already multiplier-scaled) timeout, which preparation consumes.
    // An expired run never spawns an engine.
    let deadline = run_deadline(store, id)?;
    if domain::now() >= deadline {
        return timed_out_before_spawn(id, store);
    }
    store.event(id, "phase", &json!({"phase":"spawning"}))?;
    let mut process = Process::spawn(&plan)?;
    // From this point EVERY path must clean up before returning, including a
    // database failure immediately after spawning the engine.
    let remaining =
        Duration::try_from_secs_f64((deadline - domain::now()).max(0.0)).unwrap_or(Duration::MAX);
    let execution = async {
        store.running(id, process.owner.pid)?;
        let attempt = format!("{id}:1");
        let leader = process
            .owner
            .leader
            .as_ref()
            .ok_or_else(|| Error::Runtime("legacy leader identity unavailable".into()))?;
        store.conn.execute(
            "UPDATE attempts SET ownership_active=1,phase='spawning',process_identity=?,process_birth_time=? WHERE id=?",
            rusqlite::params![leader.token, leader.birth, attempt],
        )?;
        let ownership_home = home.to_owned();
        process.observe_ownership(move |snapshot| {
            Store::open(&ownership_home)?.remember_processes("attempt", &attempt, snapshot)
        })?;
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
    };
    // On expiry the pending runner, and with it any result already received
    // but not yet concluded by EOF and exit, is dropped; the group is then
    // cleaned exactly as for any other end.
    let (execution, expired) = match tokio::time::timeout(remaining, execution).await {
        Ok(execution) => (execution, false),
        Err(_) => (Err(Error::Runtime("run deadline expired".into())), true),
    };
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    let exit = process.reap().await;
    process.checkpoint_ownership()?;
    let cleanup = cleanup?;
    store.event(id, "process_cleanup", &serde_json::to_value(&cleanup)?)?;
    if cleanup.confirmed {
        let attempt = format!("{id}:1");
        // Runs that failed before recording ownership have nothing to release.
        let owned: bool = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=? AND ownership_active=1)",
            [&attempt],
            |row| row.get(0),
        )?;
        if owned {
            store.provider_cleanup(id, &attempt, &cleanup)?;
        }
    }
    let cancelled = store.cancel_pending(id)?;
    // Same predicate as a provider run: the timer firing, or the stored
    // deadline having passed by the time cleanup finished. Cancellation wins.
    let expired = expired || domain::now() >= run_deadline(store, id)?;
    if expired && !cancelled {
        store.event(id, "run_deadline_expired", &json!({"deadline":deadline}))?;
    }
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
        if cancelled {
            Some(verify::StopReason::Cancel)
        } else {
            expired.then_some(verify::StopReason::Timeout)
        },
        Some(&evidence),
        cleanup.confirmed,
        last_progress_at,
        domain::now(),
        verify::DEFAULT_SILENCE_THRESHOLD_SECONDS,
    )?;
    store.finish(id, &outcome, proof.as_ref(), result.usage.as_ref())?;
    commands::complete_terminal(store, id)?;
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
fn install_shared_assets(
    store: &mut Store,
    app_home: &Path,
    runtime_home: &Path,
    expected: &str,
    owner: &AgentId,
    codex_grant: Option<&crate::codex::Grant>,
) -> Result<()> {
    let scope = managed_scope();
    let Some(layout) =
        crate::runtime_storage::plan(store, app_home, runtime_home, expected, &scope)?
    else {
        return Ok(());
    };
    shared_publication_root(app_home, codex_grant)?;
    crate::runtime_storage::install(store, app_home, &layout, Some(owner))
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
/// home that cannot launch is refused, and derives the Codex grant from it.
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
/// never guesses the current provider, and never launches a model. An
/// invalid store root, a vanished workdir or binary, or an unverifiable
/// grant surfaces as [`Relocation::Skipped`] with the reason and the bytes
/// still held privately; the home itself is untouched.
pub fn relocate_retained_home(
    store: &mut Store,
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
    let row = match store.get(agent) {
        Ok(row) => row,
        Err(error) => {
            let home = store
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
    let catalog = match config.resolve_catalog(store.list_accounts()?) {
        Ok(catalog) => catalog,
        Err(error) => return Ok(skip(Some(&runtime_home), error.to_string())),
    };
    let account = match store.recorded_account(agent) {
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
    ) {
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
pub fn consolidate_retained_native(
    store: &mut Store,
    app_home: &Path,
    agent: &AgentId,
) -> std::result::Result<serde_json::Value, String> {
    let row = store.get(agent).map_err(|error| error.to_string())?;
    let identity = ProviderLaunchIdentity::read(&row).map_err(|error| error.to_string())?;
    let Some(runtime_home) = identity.runtime_home.clone() else {
        return Err("recorded identity has no sealed runtime home".into());
    };
    let account = store
        .recorded_account(agent)
        .map_err(|error| error.to_string())?;
    // Native caches publish only into the root validated exactly as a managed
    // relocation validates it, replayed from the recorded frozen authority.
    let witness = native_publication_root(
        store,
        app_home,
        &identity,
        &account,
        &runtime_home,
        &row.request,
    )
    .map_err(|error| error.to_string())?;
    let report = crate::runtime_cache::consolidate(
        store,
        agent,
        &identity,
        &account,
        app_home,
        &runtime_home,
        &witness,
    )
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
fn verify_sealed_home(
    store: &mut Store,
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
        store.runtime_storage_layout(&key),
        Ok(Some(record)) if record.state == agent_run_store::runtime_storage::LayoutState::Prepared
    ) {
        crate::runtime_storage::recover(store, app_home, runtime_home)?;
    }
    crate::runtime_storage::verify(store, app_home, runtime_home, expected)?;
    Ok(())
}

/// Runs an admitted provider attempt through fenced process ownership and the terminal outbox.
/// Checkpoints root and descendant identities through cleanup, and injects an attempt-bound
/// worker capability. Preparation, execution, cleanup and durable-store failures propagate;
/// an unconfirmed cleanup keeps ownership reserved for recovery.
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
            store.update_identity(
                id,
                &serde_json::to_value(&identity)?,
                &format!("snapshot:v2:{}", digest.as_str()),
            )?;
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
            )?;
        } else {
            verify_sealed_home(
                store,
                home,
                &runtime_home,
                identity.authority.assets_sha256.as_str(),
            )?;
        }
        // The trusted store namespace is ensured before every harness launch
        // and a Codex grant must keep it outside its writable roots. There is
        // no launch wrapper: a Claude or GLM child runs with its own native
        // permissions. A committed shared layout joins the plan through its
        // verified registry mapping; anything still pending was recovered or
        // refused above.
        let shared_assets = launch_shared_assets(store, home, &runtime_home)?;
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
            store.issue_worker_capability(id, &attempt_id, &token, domain::now())?;
            let values = [
                home.to_string_lossy().into_owned(),
                id.as_str().to_owned(),
                attempt_id.clone(),
                token,
            ];
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
            store.record_resume_usage_baseline(id)?;
        }
        #[cfg(feature = "test-fixtures")]
        spawn_barrier(home, &attempt_id, store)?;
        // A switched attempt re-checks, at the handoff itself, that its
        // selected account is still enabled and the current configuration
        // still permits the frozen execution; otherwise it never spawns.
        if continuing.is_some()
            && let Some(blocker) = handoff_blocker(home, store, &account, &identity)?
        {
            if !store.provider_never_spawned(id)? {
                return Err(invalid("provider attempt was already spawning"));
            }
            store.event(id, "failover_blocked", &json!({"reason":blocker}))?;
            let mut outcome = Outcome::failure("quota_exhausted");
            outcome.failure_text = Some(format!("failover_blocked: {blocker}"));
            store.finish(id, &outcome, None, None)?;
            return commands::complete_terminal(store, id);
        }
        if !config.services.is_empty() {
            store.event(id, "phase", &json!({"phase":"warming_services"}))?;
        }
        let service_gate = crate::managed_services::wait_for_gate(home, id).await;
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
        service_gate?;
        if identity.authority.harness == HarnessId::ClaudeCode && !store.cancel_pending(id)? {
            let ownership_home = home.to_owned();
            let ownership_agent = id.clone();
            let ownership_attempt = attempt_id.clone();
            let remaining = Duration::try_from_secs_f64((deadline - domain::now()).max(0.0))
                .unwrap_or(Duration::MAX);
            let discovery =
                adapters::mcp_catalog::apply_claude_tool_filters(&mut planned, move |snapshot| {
                    let mut store = Store::open(&ownership_home)?;
                    store.bind_attempt(&ownership_attempt);
                    store.event(
                        &ownership_agent,
                        "mcp_discovery_ownership",
                        &serde_json::to_value(snapshot)?,
                    )
                });
            let discovered = tokio::time::timeout(remaining, discovery).await;
            if discovered.is_err() {
                if !crate::lifecycle::reconcile::cleanup_mcp_discovery(store, &attempt_id)?
                    || !store.provider_never_spawned(id)?
                {
                    return Err(invalid("MCP discovery cleanup is unconfirmed"));
                }
                return timed_out_before_spawn(id, store);
            }
            discovered.expect("checked discovery deadline")?;
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
        // Admission bounds new timeouts, but an older stored row may not be:
        // the conversion is checked and saturates (tokio clamps an
        // unrepresentable timeout) instead of panicking after the spawn.
        let remaining = std::time::Duration::try_from_secs_f64((deadline - domain::now()).max(0.0))
            .unwrap_or(Duration::MAX);
        let execution = async {
            let leader = process
                .owner
                .leader
                .as_ref()
                .ok_or_else(|| Error::Runtime("provider leader identity unavailable".into()))?;
            store.provider_process(id, &attempt_id, leader)?;
            let ownership_home = home.to_owned();
            let ownership_attempt = attempt_id.clone();
            process.observe_ownership(move |snapshot| {
                Store::open(&ownership_home)?.remember_processes(
                    "attempt",
                    &ownership_attempt,
                    snapshot,
                )
            })?;
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
        let checkpoint = process.checkpoint_ownership();
        let exit = process.reap().await;
        checkpoint?;
        #[cfg(feature = "test-fixtures")]
        let cleanup = injected_cleanup_error(home, &attempt_id, store, cleanup)?;
        match &cleanup {
            Ok(proof) if !proof.confirmed => {
                store.event(
                    id,
                    "process_cleanup_unverified",
                    &serde_json::to_value(proof)?,
                )?;
            }
            Err(error) => {
                store.event(
                    id,
                    "process_cleanup_unavailable",
                    &json!({"class":error.public().kind}),
                )?;
            }
            _ => {}
        }
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
                    identity.authority.provider.as_str(),
                    &lane,
                    &window,
                    "native-quota-exhaustion",
                    &governed_lanes(&identity, &account),
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
        // Only the Codex harness has native caches; validating any other
        // harness would only prepare a no-op publication.
        if cleanup.confirmed && identity.authority.harness == HarnessId::Codex {
            // The destination is still exclusively owned: resume admission
            // cannot observe it as terminal until the following store.finish.
            // Consolidation covers every terminal outcome and every resumed
            // run, and never changes the model's completion verdict: a
            // failed optional cache step is reported and skipped, never
            // allowed to fail a valid answer or erase valid source data.
            // Publication happens only into the root validated against the
            // frozen plan and account: a refused root leaves the private home
            // exactly as it was.
            let witness = native_publication_root(
                store,
                home,
                &identity,
                &account,
                &runtime_home,
                &row.request,
            );
            match witness.and_then(|root| {
                crate::runtime_cache::consolidate(
                    store,
                    id,
                    &identity,
                    &account,
                    home,
                    &runtime_home,
                    &root,
                )
            }) {
                Ok(report)
                    if report.frozen + report.packed + report.already_frozen + report.skipped
                        > 0 =>
                {
                    let _ = store.event(id, "native_cache_consolidated", &json!(report));
                }
                Err(error) => {
                    let _ = store.event(
                        id,
                        "native_cache_consolidation_skipped",
                        &json!({"class": error.public().kind}),
                    );
                }
                _ => {}
            }
        }
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
        match store.allocate_next_attempt(id, &frozen, &candidates) {
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
