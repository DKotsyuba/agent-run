//! Resident-broker ownership, warmup and expiry of explicitly configured foreground services.

use crate::broker_writer::{BROKER_BUDGET, BrokerLane};
use crate::{
    Error, Result,
    domain::{AgentId, now},
    process::{self, Identity, OwnedProcess, ProcessState},
    service::ProviderLaunchIdentity,
    state::Store,
};
use agent_run_config::services::ManagedService;
use agent_run_domain::error::invalid;
use agent_run_store::ops;
use fs2::FileExt;
use rusqlite::OptionalExtension;
#[cfg(test)]
use rusqlite::params;
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
    /// Whether the broker owns a process or only observes an external application endpoint.
    ownership: Ownership,
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

/// Durable authority: external readiness never authorizes signalling the external daemon.
#[derive(Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Ownership {
    /// The generation may own its captured bootstrap process and descendants.
    Managed,
    /// Only the probe is owned; any original managed bootstrap has already been cleaned.
    External,
}

/// Explicit projection avoids accidentally returning arguments or environment through diagnostics.
const GENERATIONS: &str = "SELECT json_object('id',id,'service_id',service_id,'revision',revision,'definition',json(definition_json),'state',state,'ownership',ownership,'broker',json(broker_identity_json),'root',json(process_identity_json),'created_at',created_at,'checked_at',checked_at,'idle_since',idle_since) FROM managed_service_generations";

/// Reads frozen generations without holding a database connection across process or probe waits.
fn generations(home: &Path) -> Result<Vec<Generation>> {
    let store = Store::open(home)?;
    let mut query = store.conn.prepare(&format!(
        "{GENERATIONS} WHERE state != 'stopped' ORDER BY created_at,id"
    ))?;

    query
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
        .collect()
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
        "HOME", "PATH", "TMPDIR", "TMP", "TEMP", "LANG", "LC_ALL", "LC_CTYPE", "USER", "LOGNAME",
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

/// Generate an executor capability from two OS-random UUIDs; never log it.
fn fresh_capability() -> Vec<u8> {
    [
        uuid::Uuid::new_v4().as_bytes().as_slice(),
        uuid::Uuid::new_v4().as_bytes().as_slice(),
    ]
    .concat()
}

/// SHA-256 of raw capability bytes, matching the private writer handshake.
fn capability_digest(secret: &[u8]) -> String {
    let hex: String = secret.iter().map(|byte| format!("{byte:02x}")).collect();
    crate::fs::sha256(hex.as_bytes())
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
    /// Explicit broker-local producer for all service mutations.
    writer: Option<BrokerLane>,
    /// Per-generation executor secrets held in memory and sent only by private pipe.
    capabilities: BTreeMap<String, Vec<u8>>,
}

impl Drop for Manager {
    /// Releases the writer lease even if a concurrent fork still holds a duplicate
    /// descriptor before exec. Closing our descriptor alone cannot release that
    /// shared lock. Service processes remain alive for the replacement broker.
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self._lock);
    }
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
            writer: None,
            capabilities: BTreeMap::new(),
        })
    }

    /// Attach the sole resident writer; there is no local SQL fallback.
    pub fn with_writer(mut self, writer: BrokerLane) -> Self {
        self.writer = Some(writer);
        self
    }

    /// Obtain the already-attached broker lane, refusing an unbound manager.
    fn lane(&self) -> Result<BrokerLane> {
        self.writer.clone().ok_or(Error::BrokerUnavailable)
    }

    /// Persist an already captured ownership snapshot through the sole writer.
    async fn remember(
        &self,
        kind: &str,
        id: &str,
        snapshot: &process::OwnershipSnapshot,
    ) -> Result<()> {
        self.lane()?
            .remote(
                ops::RememberProcesses {
                    kind: kind.to_owned(),
                    owner: id.to_owned(),
                    snapshot: snapshot.clone(),
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Reaps direct children, releases finished leases, prepares durable admissions and checks warm services.
    /// All process waits happen outside SQLite transactions. Calling this with no admissions never starts a daemon.
    pub async fn tick(&mut self) -> Result<()> {
        self.children
            .retain(|_, child| !matches!(child.try_wait(), Ok(Some(_))));
        self.recover_probes().await?;
        self.release_finished().await?;
        self.prepare_pending().await?;
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

            query
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
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
                forget_probe(&self.lane()?, &id).await?;
            }
        }
        Ok(())
    }

    /// Ends leases only after logical completion and resolution of all attempt ownership.
    /// Records actual release time so delayed cleanup cannot consume the service's idle grace.
    async fn release_finished(&self) -> Result<()> {
        self.lane()?
            .local(
                ops::ReleaseFinishedServiceLeases { at: now() },
                BROKER_BUDGET,
            )
            .await
            .map(drop)
    }

    /// Resolves each pending agent's frozen definitions and acquires leases before publishing its ready gate.
    async fn prepare_pending(&mut self) -> Result<()> {
        let ids: Vec<String> = {
            let store = Store::open(&self.home)?;
            let mut query = store.conn.prepare("SELECT g.agent_id FROM agent_service_gates g JOIN agents a ON a.id=g.agent_id WHERE g.state='pending' AND a.status IN ('created','starting','running','cancelling') ORDER BY a.created_at,a.id")?;

            query
                .query_map([], |row| row.get(0))?
                .collect::<rusqlite::Result<_>>()?
        };
        for id in ids {
            let agent: AgentId = id.parse()?;
            let row = Store::open(&self.home)?.get(&agent)?;
            let identity = match ProviderLaunchIdentity::read(&row) {
                Ok(identity) => identity,
                Err(_) => {
                    self.fail_gate(&id, "service_authority_invalid").await?;
                    continue;
                }
            };
            let mut complete = true;
            for (name, definition) in &identity.provider_config.services {
                match self.ensure_generation(name, definition).await {
                    Ok(Some(generation)) => {
                        self.lane()?
                            .local(
                                ops::PrepareServiceGate {
                                    execution: agent.clone(),
                                    step: ops::GateStep::Lease {
                                        generation: generation.clone(),
                                    },
                                    at: now(),
                                },
                                BROKER_BUDGET,
                            )
                            .await?;
                        complete &= self.qualified.contains(&generation);
                    }
                    Ok(None) => {
                        complete = false;
                        break;
                    }
                    Err(_) => {
                        complete = false;
                        self.fail_gate(&id, "service_unavailable").await?;
                        break;
                    }
                }
            }
            if complete {
                self.lane()?
                    .local(
                        ops::PrepareServiceGate {
                            execution: agent.clone(),
                            step: ops::GateStep::Ready {
                                expected_leases: identity.provider_config.services.len() as u32,
                            },
                            at: now(),
                        },
                        BROKER_BUDGET,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    /// Reuses an identical generation or reserves one for preflight/bootstrap; never replaces an occupied revision.
    async fn ensure_generation(
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
                self.lane()?
                    .local(
                        ops::EnsureServiceGeneration {
                            action: ops::GenerationAction::Retire {
                                generation: current.id.clone(),
                            },
                        },
                        BROKER_BUDGET,
                    )
                    .await?;
                return Ok(None);
            }
            if !matches!(current.state.as_str(), "starting" | "ready") {
                return Err(invalid("service generation is unavailable"));
            }
            return Ok(Some(current.id));
        }
        environment(definition)?;
        let id = uuid::Uuid::new_v4().to_string();
        let ownership = if definition.reuse_existing {
            "external"
        } else {
            "managed"
        };
        let secret = fresh_capability();
        let digest = capability_digest(&secret);
        self.lane()?
            .local(
                ops::EnsureServiceGeneration {
                    action: ops::GenerationAction::Create {
                        generation: id.clone(),
                        service: name.to_owned(),
                        revision,
                        definition_json: serde_json::to_string(definition)?,
                        broker_identity_json: serde_json::to_string(&self.owner)?,
                        ownership: ownership.to_owned(),
                        exec_capability_sha256: Some(digest),
                        at: now(),
                    },
                },
                BROKER_BUDGET,
            )
            .await?;
        self.capabilities.insert(id.clone(), secret);
        if !definition.reuse_existing {
            self.spawn_generation(&id, definition).await?;
        }
        Ok(Some(id))
    }

    /// Starts the owned bootstrap only after durable admission; spawn failures retire this generation.
    async fn spawn_generation(&mut self, id: &str, definition: &ManagedService) -> Result<()> {
        use std::io::Write;
        use std::os::fd::AsRawFd;
        let environment = environment(definition)?;
        let secret = self
            .capabilities
            .get(id)
            .cloned()
            .ok_or_else(|| invalid("service executor capability unavailable"))?;
        let (cap_r, cap_w) = crate::launch::cloexec_pipe()?;
        let read_fd = cap_r.as_raw_fd();
        let mut command = Command::new(&self.executable);
        command
            .arg("--home")
            .arg(&self.home)
            .arg("_service-exec")
            .arg(id)
            .current_dir(&definition.cwd)
            .env_clear()
            .envs(environment)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0);
        // SAFETY: child hook only duplicates/marks/closes preopened descriptors;
        // no allocation, environment access, locks or Rust callbacks run after fork.
        unsafe {
            command.pre_exec(move || {
                if read_fd != crate::launch::CAPABILITY_FD
                    && libc::dup2(read_fd, crate::launch::CAPABILITY_FD) < 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::fcntl(crate::launch::CAPABILITY_FD, libc::F_SETFD, 0) < 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if read_fd != crate::launch::CAPABILITY_FD {
                    libc::close(read_fd);
                }
                Ok(())
            });
        }
        let child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                self.lane()?
                    .local(
                        ops::FailServiceSpawn {
                            generation: id.to_owned(),
                        },
                        BROKER_BUDGET,
                    )
                    .await?;
                return Err(invalid("service bootstrap could not be spawned"));
            }
        };
        drop(cap_r);
        let mut owned = OwnedProcess::capture(child.id() as i32);
        self.children.insert(id.to_owned(), child);
        let snapshot = owned
            .snapshot()
            .ok_or_else(|| invalid("service bootstrap root unavailable"))?;
        if let Err(error) =
            observe_storage(&mut owned, self.remember("service", id, &snapshot)).await
        {
            drop(cap_w);
            let _ = owned.cleanup(Duration::from_millis(100)).await;
            return Err(error);
        }
        // No native service command may execute before durable ownership ACK.
        File::from(cap_w).write_all(&secret)?;
        Ok(())
    }

    /// Tests whether any agent still holds this exact generation, including unresolved lost attempts.
    fn held(&self, id: &str) -> Result<bool> {
        Ok(Store::open(&self.home)?.conn.query_row("SELECT EXISTS(SELECT 1 FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)", [id], |row| row.get(0))?)
    }

    /// Records a bounded static failure code without command arguments, probe output or credentials.
    async fn fail_gate(&self, agent: &str, reason: &str) -> Result<()> {
        self.lane()?
            .local(
                ops::FailServiceGate {
                    execution: agent.parse()?,
                    reason: reason.to_owned(),
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Marks a generation unhealthy and refuses pending admissions; active agents retain their leases.
    async fn unhealthy(&mut self, generation: &str, reason: &str) -> Result<()> {
        self.qualified.remove(generation);
        self.lane()?
            .local(
                ops::MarkServiceUnhealthy {
                    generation: generation.to_owned(),
                    reason: reason.to_owned(),
                    at: now(),
                },
                BROKER_BUDGET,
            )
            .await
    }

    /// Checks root identity, startup/health and idle expiry, restoring only previously captured members.
    async fn observe_generation(&mut self, generation: Generation) -> Result<()> {
        if generation.ownership == Ownership::External {
            return self.observe_external(generation).await;
        }
        let store = Store::open(&self.home)?;
        let held = self.held(&generation.id)?;
        let mut owned = store.remembered_processes("service", &generation.id)?;
        if let Some(root) = &generation.root
            && owned
                .as_ref()
                .and_then(|owned| owned.leader.as_ref())
                .is_none_or(|saved| {
                    root.pid != saved.pid
                        || root.token != saved.token
                        || root.birth != saved.birth
                        || root.group != saved.group
                })
        {
            self.unhealthy(&generation.id, "service_identity_mismatch")
                .await?;
            return Ok(());
        }
        if let Some(owned) = owned.as_mut() {
            owned.refresh();
            if let Some(snapshot) = owned.snapshot() {
                self.remember("service", &generation.id, &snapshot).await?;
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
                self.unhealthy(&generation.id, "service_bootstrap_timeout")
                    .await?;
            }
            return Ok(());
        };
        let root_state = observe(root);
        if root_state != ProcessState::Alive {
            let mut cleaned = false;
            if generation.state != "unhealthy"
                && matches!(root_state, ProcessState::Dead | ProcessState::Reused)
                && let Some(mut owned) = owned
            {
                let cleanup = owned
                    .cleanup(Duration::from_secs(
                        generation.definition.stop_grace_seconds,
                    ))
                    .await;
                if let Some(snapshot) = owned.snapshot() {
                    self.remember("service", &generation.id, &snapshot).await?;
                }
                if let Ok(proof) = cleanup {
                    cleaned = proof.confirmed;
                    self.lane()?
                        .local(
                            ops::ObserveServiceGeneration {
                                generation: generation.id.clone(),
                                observation: ops::GenerationObservation::Cleanup {
                                    proof: serde_json::to_value(&proof)?,
                                },
                            },
                            BROKER_BUDGET,
                        )
                        .await?;
                }
            }
            // Another launcher may have won between preflight and exec. Only a
            // fully cleaned, never-ready bootstrap may fall back to external use.
            if cleaned && generation.state == "starting" && generation.definition.reuse_existing {
                match probe(&self.home, &generation, true, &self.lane()?).await {
                    Ok(true) => {
                        self.lane()?
                            .local(
                                ops::ObserveServiceGeneration {
                                    generation: generation.id.clone(),
                                    observation: ops::GenerationObservation::AdoptExternal,
                                },
                                BROKER_BUDGET,
                            )
                            .await?;
                        return self.ready(&generation.id, now()).await;
                    }
                    Ok(false) => {}
                    Err(_) => {
                        self.unhealthy(&generation.id, "service_probe_unavailable")
                            .await?;
                        return Ok(());
                    }
                }
            }
            self.unhealthy(&generation.id, "service_process_unavailable")
                .await?;
            return Ok(());
        }
        if matches!(generation.state.as_str(), "unhealthy" | "unknown") {
            return Ok(());
        }
        if generation.state == "starting"
            && now() - generation.created_at >= generation.definition.startup_timeout_seconds as f64
        {
            self.unhealthy(&generation.id, "service_readiness_timeout")
                .await?;
            return Ok(());
        }
        if self.qualified.contains(&generation.id)
            && generation.checked_at.is_some_and(|at| {
                now() - at < generation.definition.monitor_interval_seconds as f64
            })
        {
            return Ok(());
        }
        let healthy = match probe(&self.home, &generation, false, &self.lane()?).await {
            Ok(healthy) => healthy,
            Err(_) => {
                self.unhealthy(&generation.id, "service_probe_unavailable")
                    .await?;
                return Ok(());
            }
        };
        let at = now();
        self.lane()?
            .local(
                ops::ObserveServiceGeneration {
                    generation: generation.id.clone(),
                    observation: ops::GenerationObservation::Checked { at },
                },
                BROKER_BUDGET,
            )
            .await?;
        if healthy && observe(root) == ProcessState::Alive {
            self.ready(&generation.id, at).await?;
        } else if generation.state == "ready"
            || at - generation.created_at >= generation.definition.startup_timeout_seconds as f64
        {
            self.unhealthy(&generation.id, "service_readiness_failed")
                .await?;
        }
        Ok(())
    }

    /// Marks an application-qualified generation ready; process authority is unchanged.
    async fn ready(&mut self, id: &str, at: f64) -> Result<()> {
        self.lane()?
            .local(
                ops::MarkServiceReady {
                    generation: id.to_owned(),
                    at,
                },
                BROKER_BUDGET,
            )
            .await?;
        self.qualified.insert(id.to_owned());
        Ok(())
    }

    /// Checks an explicitly permitted external endpoint, never capturing or signalling its PID.
    /// A negative initial preflight starts the owned bootstrap; later health loss blocks admissions.
    async fn observe_external(&mut self, generation: Generation) -> Result<()> {
        if !generation.definition.reuse_existing {
            return Err(Error::Integrity(
                "external service reuse was not authorized".into(),
            ));
        }
        let held = self.held(&generation.id)?;
        let expired = !held
            && generation
                .idle_since
                .is_some_and(|at| now() - at >= generation.definition.idle_timeout_seconds as f64);
        if generation.state == "stopping"
            || expired
            || (!held && matches!(generation.state.as_str(), "unhealthy" | "unknown"))
        {
            return self.stop(&generation, None).await;
        }
        if matches!(generation.state.as_str(), "unhealthy" | "unknown") {
            return Ok(());
        }
        if self.qualified.contains(&generation.id)
            && generation.checked_at.is_some_and(|at| {
                now() - at < generation.definition.monitor_interval_seconds as f64
            })
        {
            return Ok(());
        }
        let healthy = match probe(&self.home, &generation, true, &self.lane()?).await {
            Ok(healthy) => healthy,
            Err(_) => {
                self.unhealthy(&generation.id, "service_probe_unavailable")
                    .await?;
                return Ok(());
            }
        };
        if healthy {
            self.ready(&generation.id, now()).await?;
        } else if generation.state == "starting" && generation.root.is_none() {
            if now() - generation.created_at >= generation.definition.startup_timeout_seconds as f64
            {
                return self
                    .unhealthy(&generation.id, "service_readiness_timeout")
                    .await;
            }
            // Persist owned mode before allowing the child to exec. A broker
            // crash here retains the ordinary bootstrap recovery contract.
            let secret = fresh_capability();
            let digest = capability_digest(&secret);
            self.lane()?
                .local(
                    ops::ObserveExternalService {
                        generation: generation.id.clone(),
                        broker_identity_json: serde_json::to_string(&self.owner)?,
                        exec_capability_sha256: Some(digest),
                    },
                    BROKER_BUDGET,
                )
                .await?;
            self.capabilities.insert(generation.id.clone(), secret);
            self.spawn_generation(&generation.id, &generation.definition)
                .await?;
        } else {
            self.unhealthy(&generation.id, "service_readiness_failed")
                .await?;
        }
        Ok(())
    }

    /// Stops verified owned members, then retires the generation only with complete cleanup evidence.
    /// External generations only release leases; prior bootstrap cleanup evidence is retained.
    async fn stop(&mut self, generation: &Generation, owned: Option<OwnedProcess>) -> Result<()> {
        self.qualified.remove(&generation.id);
        if generation.state != "unknown" {
            self.lane()?
                .local(
                    ops::StopServiceGeneration {
                        generation: generation.id.clone(),
                        finalize: None,
                        at: now(),
                    },
                    BROKER_BUDGET,
                )
                .await?;
        }
        let mut proof = if generation.ownership == Ownership::External {
            let prior: Option<String> = Store::open(&self.home)?.conn.query_row(
                "SELECT cleanup_json FROM managed_service_generations WHERE id=?",
                [&generation.id],
                |row| row.get(0),
            )?;
            let mut proof = prior
                .map(|value| serde_json::from_str(&value))
                .transpose()?
                .unwrap_or_else(|| serde_json::json!({"confirmed":true}));
            // External mode was entered only with no owned process or after
            // confirmed bootstrap cleanup. Re-evaluate transient probes below;
            // their previously unresolved cleanup must not strand this record.
            proof["confirmed"] = serde_json::json!(true);
            proof["external"] = serde_json::json!(true);
            proof
        } else if let Some(mut owned) = owned {
            let cleanup = owned
                .cleanup(Duration::from_secs(
                    generation.definition.stop_grace_seconds,
                ))
                .await;
            if let Some(snapshot) = owned.snapshot() {
                self.remember("service", &generation.id, &snapshot).await?;
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
        }
        proof["probes_gone"] = serde_json::json!(!probes);
        drop(store);
        self.lane()?
            .local(
                ops::StopServiceGeneration {
                    generation: generation.id.clone(),
                    finalize: Some(proof),
                    at: now(),
                },
                BROKER_BUDGET,
            )
            .await?;
        Ok(())
    }
}

/// Executes one bounded probe in its own owned group; cancellation also cleans its descendants.
/// External mode omits the owned PID and cannot confer ownership of the checked service.
async fn probe(
    _home: &Path,
    generation: &Generation,
    external: bool,
    lane: &BrokerLane,
) -> Result<bool> {
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
    if let Some(root) = generation.root.as_ref().filter(|_| !external) {
        environment.insert("AGENT_RUN_SERVICE_PID".into(), root.pid.to_string());
    }
    environment.insert(
        "AGENT_RUN_SERVICE_OWNERSHIP".into(),
        if external { "external" } else { "managed" }.into(),
    );
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
    let snapshot = owned
        .0
        .snapshot()
        .ok_or_else(|| invalid("probe root is unavailable"))?;
    observe_storage(
        &mut owned.0,
        lane.local(
            ops::RecoverServiceProbes {
                action: ops::ServiceProbeAction::Record {
                    probe: id.clone(),
                    generation: generation.id.clone(),
                    snapshot,
                    at: now(),
                },
            },
            BROKER_BUDGET,
        ),
    )
    .await?;

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
                    remember_observed(lane, "probe", &id, &mut owned.0).await?;
                }
            },
        }
    };
    let cleanup = owned.0.cleanup(Duration::from_millis(100)).await;
    remember_observed(lane, "probe", &id, &mut owned.0).await?;
    let cleanup = cleanup?;
    let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
    if cleanup.confirmed {
        forget_probe(lane, &id).await?;
    }
    Ok(success && cleanup.confirmed)
}

/// Refresh captured process identities while an asynchronous storage ACK is pending.
/// No PID is signalled here; mandatory write completion still gates progress.
async fn observe_storage<F: std::future::Future>(owned: &mut OwnedProcess, future: F) -> F::Output {
    tokio::pin!(future);
    let mut poll = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            output = &mut future => { owned.refresh(); return output; },
            _ = poll.tick() => { owned.refresh(); },
        }
    }
}

/// Persist all observed ownership changes while process observation stays live.
async fn remember_observed(
    lane: &BrokerLane,
    kind: &str,
    id: &str,
    owned: &mut OwnedProcess,
) -> Result<()> {
    loop {
        let revision = owned.capture_revision();
        let snapshot = owned
            .snapshot()
            .ok_or_else(|| invalid("owned root unavailable"))?;
        observe_storage(
            owned,
            lane.remote(
                ops::RememberProcesses {
                    kind: kind.to_owned(),
                    owner: id.to_owned(),
                    snapshot,
                },
                BROKER_BUDGET,
            ),
        )
        .await?;
        owned.refresh();
        if revision == owned.capture_revision() {
            return Ok(());
        }
    }
}

/// Removes a confirmed finished transient probe without accumulating per-health-check history.
async fn forget_probe(lane: &BrokerLane, id: &str) -> Result<()> {
    lane.local(
        ops::RecoverServiceProbes {
            action: ops::ServiceProbeAction::Forget {
                probe: id.to_owned(),
            },
        },
        BROKER_BUDGET,
    )
    .await
}

/// Internal child-side bootstrap: persist the exact foreground identity before exec can create descendants.
/// A late bootstrap cannot execute after its originating broker disappeared or the generation was retired.
pub async fn bootstrap(home: &Path, id: &str) -> Result<()> {
    let secret = tokio::task::spawn_blocking(|| {
        crate::launch::read_capability(crate::launch::CAPABILITY_FD, Duration::from_secs(30))
    })
    .await
    .map_err(|_| invalid("service capability reader stopped"))??;
    let generation = generations(home)?
        .into_iter()
        .find(|g| g.id == id)
        .ok_or_else(|| invalid("service generation is unavailable"))?;
    generation.definition.validate()?;
    let me = process::inspect(std::process::id() as i32)?;
    if generation.state != "starting"
        || generation.ownership != Ownership::Managed
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
    let producer = crate::persist::Producer::connect(
        &home.join("writer.sock"),
        crate::storage_client::Hello {
            protocol: agent_run_domain::storage::WRITE_PROTOCOL,
            role: "service_exec".into(),
            logical: None,
            execution: None,
            attempt: None,
            generation: Some(id.to_owned()),
            capability_hex: Some(secret.iter().map(|byte| format!("{byte:02x}")).collect()),
        },
        agent_run_domain::storage::ProducerId {
            kind: agent_run_domain::storage::ProducerKind::ServiceExec,
            scope: Some(id.to_owned()),
            instance: uuid::Uuid::new_v4().simple().to_string(),
        },
    );
    producer
        .call(
            ops::BootstrapServiceProcess {
                generation: id.to_owned(),
                snapshot,
            },
            BROKER_BUDGET,
        )
        .await?;
    let claimed = producer
        .call(
            ops::ClaimServiceProcessIdentity {
                generation: id.to_owned(),
                identity: me.clone(),
            },
            BROKER_BUDGET,
        )
        .await?;
    if !claimed {
        return Err(invalid("service bootstrap generation changed"));
    }
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
            let ids: Vec<String> = store.conn.prepare("SELECT g.id FROM managed_service_leases l JOIN managed_service_generations g ON g.id=l.generation_id WHERE l.agent_id=? AND l.released_at IS NULL AND g.state='ready'")?.query_map([id.as_str()], |row| row.get(0))?.collect::<rusqlite::Result<_>>()?;
            let expected: i64 = store.conn.query_row("SELECT COUNT(*) FROM managed_service_leases WHERE agent_id=? AND released_at IS NULL", [id.as_str()], |row| row.get(0))?;
            if ids.len() as i64 != expected || ids.is_empty() {
                return Err(invalid("service lease is unavailable"));
            }
            let current = generations(home)?;
            for id in ids {
                let generation = current
                    .iter()
                    .find(|g| g.id == id && g.state == "ready")
                    .ok_or_else(|| invalid("service lease is unavailable"))?;
                if generation.ownership == Ownership::External {
                    if !generation.definition.reuse_existing
                        || generation.checked_at.is_none_or(|at| {
                            at > now()
                                || now() - at
                                    > generation.definition.monitor_interval_seconds as f64 * 3.0
                        })
                    {
                        return Err(invalid("external service health is stale or unauthorized"));
                    }
                    continue;
                }
                let root = generation
                    .root
                    .as_ref()
                    .ok_or_else(|| invalid("service root is missing"))?;
                if observe(root) != ProcessState::Alive {
                    return Err(invalid("service process is unavailable"));
                }
            }
            return Ok(());
        }
        drop(store);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A duplicate descriptor may outlive the manager during a concurrent fork before exec.
    #[test]
    fn dropping_manager_releases_lock_with_a_live_duplicate_descriptor() {
        let home = tempfile::tempdir().unwrap();
        let executable = std::env::current_exe().unwrap();
        let manager = Manager::new(home.path(), executable.clone()).unwrap();
        let inherited = manager._lock.try_clone().unwrap();
        assert!(Manager::new(home.path(), executable.clone()).is_err());
        drop(manager);
        let replacement = Manager::new(home.path(), executable.clone())
            .expect("dropping the manager must release its writer lease");
        drop(inherited);
        assert!(Manager::new(home.path(), executable).is_err());
        drop(replacement);
    }

    /// An old logical finish cannot consume the idle grace while process ownership remains unresolved.
    #[tokio::test]
    async fn idle_grace_starts_when_final_ownership_is_released() {
        let home = tempfile::tempdir().unwrap();
        let store = agent_run_store::fixture::initialize(home.path()).unwrap();
        let old = now() - 7200.0;
        store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,finished_at,config_revision) VALUES ('agent','fixture','fixture','review','fixture','fixture','/tmp','{}','lost',?1,?1,'fixture')", [old]).unwrap();
        store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES ('attempt','agent',1,'lost','{}',?,1)", [old]).unwrap();
        store.conn.execute("INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at) VALUES ('generation','fixture',?,'{}','ready','{}',?)",params!["a".repeat(64),old]).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO managed_service_leases VALUES ('generation','agent',?,NULL)",
                [old],
            )
            .unwrap();
        let writer = crate::writer_test_harness::Harness::start(home.path());
        let manager = Manager::new(home.path(), std::env::current_exe().unwrap())
            .unwrap()
            .with_writer(writer.broker().lane("services"));
        manager.release_finished().await.unwrap();
        let held: bool = store
            .conn
            .query_row(
                "SELECT released_at IS NULL FROM managed_service_leases",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(held);
        store
            .conn
            .execute("UPDATE attempts SET ownership_active=0", [])
            .unwrap();
        let before_release = now();
        manager.release_finished().await.unwrap();
        let released: f64 = store
            .conn
            .query_row(
                "SELECT released_at FROM managed_service_leases",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let idle: f64 = store
            .conn
            .query_row(
                "SELECT idle_since FROM managed_service_generations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            released >= before_release,
            "idle grace was backdated to logical completion"
        );
        assert_eq!(idle, released);
        manager.release_finished().await.unwrap();
        let unchanged: f64 = store
            .conn
            .query_row(
                "SELECT idle_since FROM managed_service_generations",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            unchanged, idle,
            "later maintenance must not postpone idle expiry"
        );
    }
}
