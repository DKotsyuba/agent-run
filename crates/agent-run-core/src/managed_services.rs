//! Resident-broker ownership, warmup and expiry of explicitly configured foreground services.

use crate::{
    domain::{now, AgentId},
    process::{self, Identity, OwnedProcess, ProcessState},
    service::ProviderLaunchIdentity,
    state::Store,
    Error, Result,
};
use agent_run_config::services::ManagedService;
use agent_run_domain::error::invalid;
use fs2::FileExt;
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{File, OpenOptions},
    os::unix::{
        fs::{MetadataExt, OpenOptionsExt, PermissionsExt},
        process::CommandExt,
    },
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

/// One immutable service generation and its current lifecycle observations; never stores environment values.
#[derive(Deserialize)]
struct Generation {
    /// Unique launch generation, independent of the human-readable service name.
    id: String,
    /// Stable configuration key shared across agents.
    service_id: String,
    /// Hash of the frozen nonsecret service definition.
    revision: String,
    /// Frozen foreground command and lifecycle bounds.
    definition: ManagedService,
    /// Current durable lifecycle state.
    state: String,
    /// Broker which originally authorized this launch.
    broker: Identity,
    /// Foreground identity committed before exec, when bootstrap reached that boundary.
    root: Option<Identity>,
    /// Admission time of this generation, in Unix seconds.
    created_at: f64,
    /// Most recent completed readiness observation, in Unix seconds.
    checked_at: Option<f64>,
    /// Time the last agent lease ended; absent while a lease is active.
    idle_since: Option<f64>,
}

/// Explicit projection avoids accidentally returning arguments or environment through diagnostics.
const GENERATIONS: &str = "SELECT json_object('id',id,'service_id',service_id,'revision',revision,'definition',json(definition_json),'state',state,'broker',json(broker_identity_json),'root',json(process_identity_json),'created_at',created_at,'checked_at',checked_at,'idle_since',idle_since) FROM managed_service_generations";

/// Reads frozen generations without holding a database connection across process or probe waits.
fn generations(home: &Path) -> Result<Vec<Generation>> {
    let store = Store::open(home)?;
    let mut query = store.conn.prepare(&format!(
        "{GENERATIONS} WHERE state != 'stopped' ORDER BY created_at,id"
    ))?;
    let result = query
        .query_map([], |row| row.get::<_, String>(0))?
        .map(|row| -> Result<Generation> {
            let generation: Generation = serde_json::from_str(&row?)?;
            if generation.definition.revision()? != generation.revision {
                return Err(Error::Integrity(
                    "managed service definition changed".into(),
                ));
            }
            Ok(generation)
        })
        .collect();
    result
}

/// Observes an exact recorded identity; missing permissions never mean a stopped service.
fn observe(identity: &Identity) -> ProcessState {
    process::observe(
        Some(identity.pid),
        Some(&identity.token),
        Some(identity.birth),
    )
}

/// Resolves only declared host variables and ordinary executable environment defaults.
fn environment(definition: &ManagedService) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    for name in [
        "HOME", "PATH", "TMPDIR", "LANG", "LC_ALL", "LC_CTYPE", "USER", "LOGNAME",
    ] {
        if let Ok(value) = std::env::var(name) {
            values.insert(name.to_owned(), value);
        }
    }
    for name in &definition.env_from {
        values.insert(
            name.clone(),
            std::env::var(name)
                .map_err(|_| invalid("declared service environment variable is unavailable"))?,
        );
    }
    Ok(values)
}

/// Single resident manager per home; drops child handles without killing services held by running agents.
pub struct Manager {
    /// Private store and process-control home.
    home: PathBuf,
    /// Current native binary used only for its internal service bootstrap command.
    executable: PathBuf,
    /// Native identity of this broker, recorded on every new generation.
    owner: Identity,
    /// Direct children retained for wait/reaping while this broker lives.
    children: BTreeMap<String, Child>,
    /// Generations health-checked by this manager; restart does not trust the prior broker's ready bit alone.
    qualified: BTreeSet<String>,
    /// Home-wide writer lease, including brokers bound to a non-default socket.
    _lock: File,
}

impl Manager {
    /// Acquires the home-wide manager lock without starting any configured service.
    pub fn new(home: &Path, executable: PathBuf) -> Result<Self> {
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(home.join(".services.lock"))?;
        let metadata = lock.metadata()?;
        // SAFETY: geteuid has no pointer arguments or mutation side effects.
        if !metadata.is_file() || metadata.uid() != unsafe { libc::geteuid() } {
            return Err(invalid("managed service lock is not an owned regular file"));
        }
        lock.set_permissions(std::fs::Permissions::from_mode(0o600))?;
        lock.try_lock_exclusive()
            .map_err(|_| invalid("another broker owns managed services for this home"))?;
        Ok(Self {
            home: home.to_owned(),
            executable,
            owner: process::inspect(std::process::id() as i32)?,
            children: BTreeMap::new(),
            qualified: BTreeSet::new(),
            _lock: lock,
        })
    }

    /// Reaps direct children, releases finished leases, prepares durable admissions and checks warm services.
    /// All process waits happen outside SQLite transactions. Calling this with no admissions never starts a daemon.
    pub async fn tick(&mut self) -> Result<()> {
        self.children
            .retain(|_, child| !matches!(child.try_wait(), Ok(Some(_))));
        self.recover_probes().await?;
        self.release_finished()?;
        self.prepare_pending()?;
        for generation in generations(&self.home)? {
            self.observe_generation(generation).await?;
        }
        Ok(())
    }

    /// Cleans probes interrupted by broker death before any new probe or service admission can run.
    async fn recover_probes(&self) -> Result<()> {
        let ids: Vec<String> = {
            let store = Store::open(&self.home)?;
            let mut query = store
                .conn
                .prepare("SELECT id FROM managed_service_probes")?;
            let ids = query
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            ids
        };
        for id in ids {
            let captured = Store::open(&self.home)?.remembered_processes("probe", &id)?;
            let confirmed = match captured {
                Some(mut owned) => owned
                    .cleanup(Duration::from_millis(100))
                    .await
                    .is_ok_and(|proof| proof.confirmed),
                // The exec gate is opened only after ownership is committed.
                // Broker death closes its pipe, so an unrecorded wrapper exits without exec.
                None => true,
            };
            if confirmed {
                forget_probe(&self.home, &id)?;
            }
        }
        Ok(())
    }

    /// Ends leases only after logical completion and resolution of all attempt ownership.
    fn release_finished(&self) -> Result<()> {
        let store = Store::open(&self.home)?;
        let at = now();
        store.conn.execute("UPDATE managed_service_leases SET released_at=COALESCE((SELECT finished_at FROM agents WHERE id=managed_service_leases.agent_id),?1) WHERE released_at IS NULL AND EXISTS(SELECT 1 FROM agents a WHERE a.id=managed_service_leases.agent_id AND a.status IN ('succeeded','failed','cancelled','lost','timed_out')) AND NOT EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=managed_service_leases.agent_id AND t.ownership_active=1)", [at])?;
        store.conn.execute("UPDATE managed_service_generations SET idle_since=COALESCE((SELECT MAX(released_at) FROM managed_service_leases WHERE generation_id=managed_service_generations.id),?1) WHERE state!='stopped' AND idle_since IS NULL AND NOT EXISTS(SELECT 1 FROM managed_service_leases l WHERE l.generation_id=managed_service_generations.id AND l.released_at IS NULL)", [at])?;
        Ok(())
    }

    /// Resolves each pending agent's frozen definitions and acquires leases before publishing its ready gate.
    fn prepare_pending(&mut self) -> Result<()> {
        let ids: Vec<String> = {
            let store = Store::open(&self.home)?;
            let mut query = store.conn.prepare("SELECT g.agent_id FROM agent_service_gates g JOIN agents a ON a.id=g.agent_id WHERE g.state='pending' AND a.status IN ('created','starting','running','cancelling') ORDER BY a.created_at,a.id")?;
            let ids = query
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?;
            ids
        };
        for id in ids {
            let agent: AgentId = id.parse()?;
            let row = Store::open(&self.home)?.get(&agent)?;
            let identity = match ProviderLaunchIdentity::read(&row) {
                Ok(identity) => identity,
                Err(_) => {
                    self.fail_gate(&id, "service_authority_invalid")?;
                    continue;
                }
            };
            let mut complete = true;
            for (name, definition) in &identity.provider_config.services {
                match self.ensure_generation(name, definition) {
                    Ok(Some(generation)) => {
                        let store = Store::open(&self.home)?;
                        store.conn.execute("INSERT INTO managed_service_leases(generation_id,agent_id,acquired_at) VALUES (?,?,?) ON CONFLICT DO NOTHING", params![generation,id,now()])?;
                        store.conn.execute(
                            "UPDATE managed_service_generations SET idle_since=NULL WHERE id=? AND idle_since IS NOT NULL",
                            [&generation],
                        )?;
                        complete &= self.qualified.contains(&generation);
                    }
                    Ok(None) => {
                        complete = false;
                        break;
                    }
                    Err(_) => {
                        complete = false;
                        self.fail_gate(&id, "service_unavailable")?;
                        break;
                    }
                }
            }
            if complete {
                Store::open(&self.home)?.conn.execute("UPDATE agent_service_gates SET state='ready' WHERE agent_id=?1 AND state='pending' AND (SELECT COUNT(*) FROM managed_service_leases WHERE agent_id=?1 AND released_at IS NULL)=?2 AND NOT EXISTS(SELECT 1 FROM managed_service_leases l JOIN managed_service_generations g ON g.id=l.generation_id WHERE l.agent_id=?1 AND l.released_at IS NULL AND g.state!='ready')", params![id, identity.provider_config.services.len() as i64])?;
            }
        }
        Ok(())
    }

    /// Reuses an identical generation or starts one internal bootstrap; never replaces an occupied revision.
    fn ensure_generation(
        &mut self,
        name: &str,
        definition: &ManagedService,
    ) -> Result<Option<String>> {
        let revision = definition.revision()?;
        if let Some(current) = generations(&self.home)?
            .into_iter()
            .find(|g| g.service_id == name)
        {
            if current.revision != revision {
                if self.held(&current.id)? {
                    return Err(invalid("service revision is held by active agents"));
                }
                Store::open(&self.home)?.conn.execute(
                    "UPDATE managed_service_generations SET state='stopping' WHERE id=?",
                    [&current.id],
                )?;
                return Ok(None);
            }
            if !matches!(current.state.as_str(), "starting" | "ready") {
                return Err(invalid("service generation is unavailable"));
            }
            return Ok(Some(current.id));
        }
        let environment = environment(definition)?;
        let id = uuid::Uuid::new_v4().to_string();
        Store::open(&self.home)?.conn.execute("INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at) VALUES (?,?,?,?,'starting',?,?)", params![id,name,revision,serde_json::to_string(definition)?,serde_json::to_string(&self.owner)?,now()])?;
        let child = Command::new(&self.executable)
            .arg("--home")
            .arg(&self.home)
            .arg("_service-exec")
            .arg(&id)
            .current_dir(&definition.cwd)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn();
        match child {
            Ok(child) => {
                let owned = OwnedProcess::capture(child.id() as i32);
                self.children.insert(id.clone(), child);
                if let Some(snapshot) = owned.snapshot() {
                    Store::open(&self.home)?.remember_processes("service", &id, &snapshot)?;
                }
            }
            Err(_) => {
                Store::open(&self.home)?.conn.execute("UPDATE managed_service_generations SET state='stopped',failure_kind='service_spawn_failed',cleanup_json='{\"never_spawned\":true,\"confirmed\":true}' WHERE id=?", [&id])?;
                return Err(invalid("service bootstrap could not be spawned"));
            }
        }
        Ok(Some(id))
    }

    /// Tests whether any agent still holds this exact generation, including unresolved lost attempts.
    fn held(&self, id: &str) -> Result<bool> {
        Ok(Store::open(&self.home)?.conn.query_row("SELECT EXISTS(SELECT 1 FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)", [id], |row| row.get(0))?)
    }

    /// Records a bounded static failure code without command arguments, probe output or credentials.
    fn fail_gate(&self, agent: &str, reason: &str) -> Result<()> {
        Store::open(&self.home)?.conn.execute("UPDATE agent_service_gates SET state='failed',failure_kind=? WHERE agent_id=? AND state='pending'", params![reason,agent])?;
        Ok(())
    }

    /// Marks a generation unhealthy and refuses pending admissions; active agents retain their leases.
    fn unhealthy(&mut self, generation: &str, reason: &str) -> Result<()> {
        self.qualified.remove(generation);
        let store = Store::open(&self.home)?;
        store.conn.execute("UPDATE managed_service_generations SET state='unhealthy',failure_kind=?,checked_at=? WHERE id=?", params![reason,now(),generation])?;
        store.conn.execute("UPDATE agent_service_gates SET state='failed',failure_kind=? WHERE state='pending' AND agent_id IN (SELECT agent_id FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)", params![reason,generation])?;
        Ok(())
    }

    /// Checks root identity, startup/health and idle expiry, restoring only previously captured members.
    async fn observe_generation(&mut self, generation: Generation) -> Result<()> {
        let mut store = Store::open(&self.home)?;
        let held = self.held(&generation.id)?;
        let mut owned = store.remembered_processes("service", &generation.id)?;
        if let Some(root) = &generation.root {
            if owned
                .as_ref()
                .and_then(|owned| owned.leader.as_ref())
                .is_none_or(|saved| {
                    root.pid != saved.pid
                        || root.token != saved.token
                        || root.birth != saved.birth
                        || root.group != saved.group
                })
            {
                self.unhealthy(&generation.id, "service_identity_mismatch")?;
                return Ok(());
            }
        }
        if let Some(owned) = owned.as_mut() {
            owned.refresh();
            if let Some(snapshot) = owned.snapshot() {
                store.remember_processes("service", &generation.id, &snapshot)?;
            }
        }
        drop(store);
        let expired = !held
            && generation
                .idle_since
                .is_some_and(|at| now() - at >= generation.definition.idle_timeout_seconds as f64);
        if generation.state == "stopping"
            || expired
            || (!held && matches!(generation.state.as_str(), "unhealthy" | "unknown"))
        {
            return self.stop(&generation, owned).await;
        }
        let Some(root) = &generation.root else {
            if matches!(
                observe(&generation.broker),
                ProcessState::Dead | ProcessState::Reused
            ) {
                return self.stop(&generation, owned).await;
            }
            if now() - generation.created_at >= generation.definition.startup_timeout_seconds as f64
            {
                self.unhealthy(&generation.id, "service_bootstrap_timeout")?;
            }
            return Ok(());
        };
        let root_state = observe(root);
        if root_state != ProcessState::Alive {
            if generation.state != "unhealthy"
                && matches!(root_state, ProcessState::Dead | ProcessState::Reused)
            {
                if let Some(mut owned) = owned {
                    let cleanup = owned
                        .cleanup(Duration::from_secs(
                            generation.definition.stop_grace_seconds,
                        ))
                        .await;
                    if let Some(snapshot) = owned.snapshot() {
                        Store::open(&self.home)?.remember_processes(
                            "service",
                            &generation.id,
                            &snapshot,
                        )?;
                    }
                    if let Ok(proof) = cleanup {
                        Store::open(&self.home)?.conn.execute(
                            "UPDATE managed_service_generations SET cleanup_json=? WHERE id=?",
                            params![serde_json::to_string(&proof)?, generation.id],
                        )?;
                    }
                }
            }
            self.unhealthy(&generation.id, "service_process_unavailable")?;
            return Ok(());
        }
        if matches!(generation.state.as_str(), "unhealthy" | "unknown") {
            return Ok(());
        }
        if generation.state == "starting"
            && now() - generation.created_at >= generation.definition.startup_timeout_seconds as f64
        {
            self.unhealthy(&generation.id, "service_readiness_timeout")?;
            return Ok(());
        }
        if self.qualified.contains(&generation.id)
            && generation.checked_at.is_some_and(|at| {
                now() - at < generation.definition.monitor_interval_seconds as f64
            })
        {
            return Ok(());
        }
        let healthy = match probe(&self.home, &generation).await {
            Ok(healthy) => healthy,
            Err(_) => {
                self.unhealthy(&generation.id, "service_probe_unavailable")?;
                return Ok(());
            }
        };
        let at = now();
        Store::open(&self.home)?.conn.execute(
            "UPDATE managed_service_generations SET checked_at=? WHERE id=?",
            params![at, generation.id],
        )?;
        if healthy && observe(root) == ProcessState::Alive {
            Store::open(&self.home)?.conn.execute("UPDATE managed_service_generations SET state='ready',ready_at=COALESCE(ready_at,?),failure_kind=NULL WHERE id=? AND state IN ('starting','ready')", params![at,generation.id])?;
            self.qualified.insert(generation.id.clone());
        } else if generation.state == "ready"
            || at - generation.created_at >= generation.definition.startup_timeout_seconds as f64
        {
            self.unhealthy(&generation.id, "service_readiness_failed")?;
        }
        Ok(())
    }

    /// Stops verified owned members, then retires the generation only with complete cleanup evidence.
    async fn stop(&mut self, generation: &Generation, owned: Option<OwnedProcess>) -> Result<()> {
        self.qualified.remove(&generation.id);
        let store = Store::open(&self.home)?;
        store.conn.execute(
            "UPDATE managed_service_generations SET state='stopping' WHERE id=?",
            [&generation.id],
        )?;
        drop(store);
        let mut proof = if let Some(mut owned) = owned {
            let cleanup = owned
                .cleanup(Duration::from_secs(
                    generation.definition.stop_grace_seconds,
                ))
                .await;
            if let Some(snapshot) = owned.snapshot() {
                Store::open(&self.home)?.remember_processes(
                    "service",
                    &generation.id,
                    &snapshot,
                )?;
            }
            serde_json::to_value(cleanup?)?
        } else if generation.root.is_none()
            && matches!(
                observe(&generation.broker),
                ProcessState::Dead | ProcessState::Reused
            )
        {
            serde_json::json!({"never_spawned":true,"confirmed":true})
        } else {
            serde_json::json!({"confirmed":false})
        };
        let store = Store::open(&self.home)?;
        let probes: bool = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM managed_service_probes WHERE generation_id=?)",
            [&generation.id],
            |row| row.get(0),
        )?;
        if probes {
            proof["confirmed"] = serde_json::json!(false);
            proof["probes_gone"] = serde_json::json!(false);
        }
        let confirmed = proof["confirmed"] == true;
        store.conn.execute(
            "UPDATE managed_service_generations SET state=?,cleanup_json=? WHERE id=?",
            params![
                if confirmed { "stopped" } else { "unknown" },
                proof.to_string(),
                generation.id
            ],
        )?;
        if confirmed {
            store.conn.execute("UPDATE managed_service_leases SET released_at=? WHERE generation_id=? AND released_at IS NULL", params![now(),generation.id])?;
        }
        Ok(())
    }
}

/// Executes one bounded probe in its own owned group; cancellation also cleans its descendants.
async fn probe(home: &Path, generation: &Generation) -> Result<bool> {
    use tokio::io::AsyncWriteExt;
    /// Makes interrupted broker maintenance release this probe's captured child tree.
    struct Probe(OwnedProcess);
    impl Drop for Probe {
        /// Keeps cancellation and assertion failures from leaving probe children running.
        fn drop(&mut self) {
            let _ = self.0.cleanup_blocking(Duration::from_millis(100));
        }
    }
    let mut environment = environment(&generation.definition)?;
    if let Some(root) = &generation.root {
        environment.insert("AGENT_RUN_SERVICE_PID".into(), root.pid.to_string());
    }
    environment.insert("AGENT_RUN_SERVICE_ID".into(), generation.service_id.clone());
    environment.insert("AGENT_RUN_SERVICE_GENERATION".into(), generation.id.clone());
    // The shell only gates exec on our pipe. Arguments remain literal; the
    // probe cannot finish or fork before its root identity has been captured.
    let mut command = tokio::process::Command::new("/bin/sh");
    command
        .args([
            "-c",
            "IFS= read -r ready || exit 125; exec \"$@\"",
            "agent-run-readiness",
        ])
        .arg(&generation.definition.readiness.command)
        .args(&generation.definition.readiness.args)
        .current_dir(&generation.definition.cwd)
        .env_clear()
        .envs(environment)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    command.as_std_mut().process_group(0);
    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(_) => return Ok(false),
    };
    let mut owned = Probe(OwnedProcess::capture(
        child.id().ok_or_else(|| invalid("probe pid unavailable"))? as i32,
    ));
    let id = uuid::Uuid::new_v4().to_string();
    {
        let mut store = Store::open(home)?;
        store.conn.execute(
            "INSERT INTO managed_service_probes(id,generation_id,created_at) VALUES (?,?,?)",
            params![id, generation.id, now()],
        )?;
        store.remember_processes(
            "probe",
            &id,
            &owned
                .0
                .snapshot()
                .ok_or_else(|| invalid("probe root is unavailable"))?,
        )?;
    }
    let mut input = child
        .stdin
        .take()
        .ok_or_else(|| invalid("probe bootstrap pipe is missing"))?;
    input.write_all(b"ready\n").await?;
    drop(input);
    let budget = if generation.state == "starting" {
        (generation.created_at + generation.definition.startup_timeout_seconds as f64 - now())
            .max(0.0)
            .min(generation.definition.readiness.timeout_seconds as f64)
    } else {
        generation.definition.readiness.timeout_seconds as f64
    };
    let deadline = tokio::time::Instant::now() + Duration::from_secs_f64(budget);
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    let success = loop {
        tokio::select! {
            status = child.wait() => break status.is_ok_and(|status| status.success()),
            _ = tokio::time::sleep_until(deadline) => break false,
            _ = poll.tick() => {
                let previous = owned.0.capture_revision();
                owned.0.refresh();
                if previous != owned.0.capture_revision() {
                    Store::open(home)?.remember_processes("probe", &id, &owned.0.snapshot().ok_or_else(|| invalid("probe root is unavailable"))?)?;
                }
            },
        }
    };
    let cleanup = owned.0.cleanup(Duration::from_millis(100)).await;
    Store::open(home)?.remember_processes(
        "probe",
        &id,
        &owned
            .0
            .snapshot()
            .ok_or_else(|| invalid("probe root is unavailable"))?,
    )?;
    let cleanup = cleanup?;
    let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    if cleanup.confirmed {
        forget_probe(home, &id)?;
    }
    Ok(success && cleanup.confirmed)
}

/// Removes a confirmed finished transient probe without accumulating per-health-check history.
fn forget_probe(home: &Path, id: &str) -> Result<()> {
    let mut store = Store::open(home)?;
    let tx = store.conn.transaction()?;
    tx.execute(
        "DELETE FROM process_members WHERE owner_kind='probe' AND owner_id=?",
        [id],
    )?;
    tx.execute(
        "DELETE FROM process_ownership WHERE owner_kind='probe' AND owner_id=?",
        [id],
    )?;
    tx.execute("DELETE FROM managed_service_probes WHERE id=?", [id])?;
    tx.commit()?;
    Ok(())
}

/// Internal child-side bootstrap: persist the exact foreground identity before exec can create descendants.
/// A late bootstrap cannot execute after its originating broker disappeared or the generation was retired.
pub fn bootstrap(home: &Path, id: &str) -> Result<()> {
    let generation = generations(home)?
        .into_iter()
        .find(|g| g.id == id)
        .ok_or_else(|| invalid("service generation is unavailable"))?;
    generation.definition.validate()?;
    let me = process::inspect(std::process::id() as i32)?;
    if generation.state != "starting"
        || generation.root.is_some()
        || me.ppid != generation.broker.pid
        || me.group != me.pid
        || observe(&generation.broker) != ProcessState::Alive
    {
        return Err(invalid("service bootstrap ownership rejected"));
    }
    let snapshot = OwnedProcess::capture(me.pid)
        .snapshot()
        .ok_or_else(|| invalid("service bootstrap identity unavailable"))?;
    let mut store = Store::open(home)?;
    store.remember_processes("service", id, &snapshot)?;
    let claimed = store.conn.execute("UPDATE managed_service_generations SET process_identity_json=? WHERE id=? AND state='starting' AND process_identity_json IS NULL", params![serde_json::to_string(&me)?,id])?;
    if claimed != 1 {
        return Err(invalid("service bootstrap generation changed"));
    }
    drop(store);
    let error = Command::new(&generation.definition.command)
        .args(&generation.definition.args)
        .current_dir(&generation.definition.cwd)
        .env_clear()
        .envs(environment(&generation.definition)?)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .exec();
    Err(error.into())
}

/// Waits for the broker's durable warmup gate, respecting cancellation and the agent's original deadline.
pub async fn wait_for_gate(home: &Path, id: &AgentId) -> Result<()> {
    loop {
        let store = Store::open(home)?;
        let gate: Option<(String, Option<String>)> = store
            .conn
            .query_row(
                "SELECT state,failure_kind FROM agent_service_gates WHERE agent_id=?",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((state, failure)) = gate else {
            let row = store.get(id)?;
            if row
                .identity
                .as_ref()
                .and_then(|identity| identity.pointer("/provider_config/services"))
                .and_then(serde_json::Value::as_object)
                .is_some_and(|services| !services.is_empty())
            {
                return Err(Error::Integrity("managed service gate is missing".into()));
            }
            return Ok(());
        };
        if store.cancel_pending(id)? {
            return Err(invalid("service warmup cancelled"));
        }
        if state == "failed" {
            return Err(Error::Runtime(
                failure.unwrap_or_else(|| "service_unavailable".into()),
            ));
        }
        if state == "ready" {
            let roots: Vec<Option<String>> = store.conn.prepare("SELECT g.process_identity_json FROM managed_service_leases l JOIN managed_service_generations g ON g.id=l.generation_id WHERE l.agent_id=? AND l.released_at IS NULL AND g.state='ready'")?.query_map([id.as_str()], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
            let expected: i64 = store.conn.query_row("SELECT COUNT(*) FROM managed_service_leases WHERE agent_id=? AND released_at IS NULL", [id.as_str()], |row| row.get(0))?;
            if roots.len() as i64 != expected || roots.is_empty() {
                return Err(invalid("service lease is unavailable"));
            }
            for root in roots {
                let root: Identity =
                    serde_json::from_str(&root.ok_or_else(|| invalid("service root is missing"))?)?;
                if observe(&root) != ProcessState::Alive {
                    return Err(invalid("service process is unavailable"));
                }
            }
            return Ok(());
        }
        let row = store.get(id)?;
        if now() >= row.created_at + row.request.timeout_seconds.unwrap_or(480.0) {
            return Err(invalid("service warmup exceeded agent deadline"));
        }
        drop(store);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
