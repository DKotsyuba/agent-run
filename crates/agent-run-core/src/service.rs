//! Application facade: transport code has no direct access to adapters or SQL.
use crate::{
    adapters,
    config::{Adapter, Config},
    domain::{now, AgentId, OrchestratorRef, Outcome, StartRequest, Status},
    error::invalid,
    lifecycle::reconcile,
    logging,
    policy::{self, EffectivePolicy},
    process,
    profiles::{self, Profile},
    state::{Record, Store},
    supervisor,
    verify::{self, Proof},
    Error, Result,
};
use agent_run_config::role_plan;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchIdentity {
    pub rust_identity_version: u32,
    #[serde(default)]
    pub replay_request_sha256: Option<String>,
    pub config: Config,
    pub profile: Profile,
    pub effective_policy: EffectivePolicy,
    pub runtime_home: Option<PathBuf>,
    pub snapshot_sha256: Option<String>,
}
impl LaunchIdentity {
    pub fn read(row: &Record) -> Result<Self> {
        let value = row
            .identity
            .clone()
            .ok_or_else(|| invalid("native resume requires a recorded launch identity"))?;
        let result:Self=serde_json::from_value(value).map_err(|_|Error::Unsupported("resuming Python-created runs is not yet supported; their history and answers remain readable".into()))?;
        if result.rust_identity_version != 1 {
            return Err(invalid("unsupported launch identity version"));
        }
        if result.profile.name != row.request.profile
            || result.profile.write != row.request.write
            || result.profile.read_roots != row.request.read_roots
        {
            return Err(Error::Integrity(
                "stored role grants contradict the launch request".into(),
            ));
        }
        Ok(result)
    }
}
#[derive(Clone)]
pub struct Service {
    pub home: PathBuf,
}
#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Query {
    pub active: bool,
    pub orchestrator: Option<OrchestratorRef>,
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
    pub after_revision: Option<i64>,
    pub wait_seconds: f64,
}
fn default_limit() -> usize {
    100
}
impl Default for Query {
    fn default() -> Self {
        Self {
            active: false,
            orchestrator: None,
            offset: 0,
            limit: 100,
            after_revision: None,
            wait_seconds: 0.0,
        }
    }
}
impl Query {
    pub fn validate(&self) -> Result<()> {
        if self.limit == 0
            || self.limit > 1000
            || self.offset > i64::MAX as usize
            || !self.wait_seconds.is_finite()
            || !(0.0..=60.0).contains(&self.wait_seconds)
            || self.after_revision.is_some_and(|n| n < 0)
        {
            return Err(invalid("invalid agent page or wait arguments"));
        }
        if let Some(o) = &self.orchestrator {
            o.validate()?;
        }
        Ok(())
    }
}
impl Service {
    pub fn new(home: PathBuf) -> Self {
        Self { home }
    }
    pub async fn start(&self, mut request: StartRequest) -> Result<Value> {
        request.validate()?;
        if request.runtime == "opencode" {
            return Err(invalid(
                "runtime 'opencode' is no longer supported; remove [runtimes.opencode] from config.toml",
            ));
        }
        let fingerprint =
            agent_run_domain::canonical::sha256_hex(&serde_json::to_value(&request)?, true);
        {
            let store = Store::open(&self.home)?;
            if let Some(row) = store.replay_request(&request)? {
                if let Some(previous) = row
                    .identity
                    .as_ref()
                    .and_then(|v| v.get("replay_request_sha256"))
                    .and_then(Value::as_str)
                {
                    if previous != fingerprint || row.parent_agent_id.is_some() {
                        return Err(Error::Conflict);
                    }
                    logging::start(&request.runtime, &request.model, &row.id.to_string(), false);
                    return Ok(
                        json!({"agent_id":row.id,"created":false,"agent":self.view(&store,&row)?}),
                    );
                }
            }
        }
        let config = Config::load(&self.home)?;
        let runtime = config.runtime(&request.runtime)?;
        request.account = runtime.selected_account(request.account.as_deref())?;
        request.timeout_seconds = Some(
            request
                .timeout_seconds
                .unwrap_or(config.core.default_timeout_seconds),
        );
        let profile = profiles::load(&config, runtime, &request)?;
        request.write = profile.write;
        request.read_roots = profile.read_roots.clone();
        request.required_constraints = profile.required_constraints.clone();
        let role_plan = profile
            .canonical
            .then(|| {
                role_plan::resolve_role_plan(
                    &profile,
                    config.skills_dir(),
                    &config.mcp,
                    if request.account.is_some() {
                        "account"
                    } else {
                        "global"
                    },
                    request.account.as_deref(),
                )
            })
            .transpose()?;
        let config_revision = role_plan
            .as_ref()
            .map(|plan| plan.config_revision.as_str())
            .unwrap_or("pending:materialization");
        adapters::validate(&request, runtime, &profile)?;
        let policy = policy::evaluate(&request.runtime, runtime, &profile);
        policy.admit()?;
        let identity = LaunchIdentity {
            rust_identity_version: 1,
            replay_request_sha256: Some(fingerprint),
            config: config.clone(),
            profile,
            effective_policy: policy,
            runtime_home: None,
            snapshot_sha256: None,
        };
        let runtime = request.runtime.clone();
        let model = request.model.clone();
        let result = self
            .admit(request, &config, identity, None, config_revision)
            .await?;
        if let (Some(agent_id), Some(created)) = (
            result.get("agent_id").and_then(Value::as_str),
            result.get("created").and_then(Value::as_bool),
        ) {
            logging::start(&runtime, &model, agent_id, created);
        }
        Ok(result)
    }
    async fn admit(
        &self,
        request: StartRequest,
        config: &Config,
        identity: LaunchIdentity,
        parent: Option<&Record>,
        config_revision: &str,
    ) -> Result<Value> {
        let (id, created) = {
            let mut store = Store::open(&self.home)?;
            store.admit_with_config_revision(
                &request,
                config,
                config_revision,
                &serde_json::to_value(identity)?,
                parent,
            )?
        };
        if created {
            // A failed READY read does not cancel an already admitted durable job.
            if let Err(error) = supervisor::launch(&self.home, &id).await {
                let mut store = Store::open(&self.home)?;
                let row = store.get(&id)?;
                store.event(
                    &id,
                    "supervisor_handoff_error",
                    &json!({"kind":error.public().kind}),
                )?;
                if row.supervisor_pid.is_none() && matches!(error, Error::Io(_)) {
                    // No owned supervisor was established. Explicit failure is better
                    // than an immortal starting row after a failed OS spawn.
                    let out = Outcome::failure("supervisor_bootstrap_failed");
                    store.finish(&id, &out, None, None)?;
                }
            }
        }
        let store = Store::open(&self.home)?;
        let row = store.get(&id)?;
        Ok(json!({"agent_id":id,"created":created,"agent":self.view(&store,&row)?}))
    }
    /// Admits one native continuation after proving terminal lineage and snapshot identity.
    ///
    /// The parent must be terminal with a native session and a sealed Rust launch
    /// identity. Snapshot continuations retain the parent's immutable revision;
    /// legacy revisions remain pending until their supervisor rematerializes them.
    pub async fn resume(
        &self,
        id: &AgentId,
        task: String,
        timeout: Option<f64>,
        request_id: Option<String>,
        orchestrator: Option<OrchestratorRef>,
    ) -> Result<Value> {
        let parent = Store::open(&self.home)?.get(id)?;
        if !parent.status.terminal() || parent.runtime_session_id.is_none() {
            return Err(invalid(
                "resume requires a terminal run with a native session ID",
            ));
        }
        let mut identity = LaunchIdentity::read(&parent)?;
        identity.replay_request_sha256 = None;
        let runtime_home = identity
            .runtime_home
            .as_deref()
            .ok_or_else(|| invalid("parent has no sealed runtime home"))?;
        let digest = identity
            .snapshot_sha256
            .as_deref()
            .ok_or_else(|| invalid("parent has no runtime snapshot proof"))?;
        adapters::materialize::verify(runtime_home, digest)?;
        let current = Config::load(&self.home)?;
        let active = current.runtime(&parent.request.runtime)?;
        if !active.models.contains(&parent.request.model) {
            return Err(invalid("parent model is no longer enabled"));
        }
        let recorded = identity.config.runtime(&parent.request.runtime)?;
        if active.home != recorded.home
            || serde_json::to_value(&active.auth)? != serde_json::to_value(&recorded.auth)?
        {
            return Err(invalid("runtime identity changed since the parent ran"));
        }
        if let Some(account) = parent.request.account.as_deref() {
            active.selected_account(Some(account))?;
        }
        let mut request = parent.request.clone();
        request.task = task;
        request.request_id = request_id;
        request.timeout_seconds = timeout.or(parent.request.timeout_seconds);
        if orchestrator.is_some() {
            request.orchestrator = orchestrator;
        }
        request.validate()?;
        let config_revision: String = Store::open(&self.home)?.conn.query_row(
            "SELECT config_revision FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let child_revision = if config_revision.starts_with("snapshot:v1:") {
            config_revision.as_str()
        } else {
            "pending:materialization"
        };
        match self
            .admit(request, &current, identity, Some(&parent), child_revision)
            .await
        {
            Ok(value) => Ok(value),
            Err(Error::Conflict) => {
                let winner: Option<String> = Store::open(&self.home)?.conn.query_row(
                    "SELECT id FROM agents WHERE parent_agent_id=? ORDER BY sequence DESC LIMIT 1",
                    [id.as_str()],
                    |row| row.get(0),
                ).optional()?;
                match winner {
                    Some(winner) => Err(invalid(format!(
                        "agent {id} has already been resumed by {winner}"
                    ))),
                    None => Err(Error::Conflict),
                }
            }
            Err(error) => Err(error),
        }
    }
    pub fn cancel(&self, id: &AgentId) -> Result<Value> {
        Store::open(&self.home)?.enqueue(id, "cancel", &json!({}))
    }
    pub fn steer(&self, id: &AgentId, text: &str) -> Result<Value> {
        crate::domain::nonblank("steer text", text)?;
        if text.len() > 256 * 1024 {
            return Err(invalid("steer text exceeds 256 KiB"));
        }
        let mut store = Store::open(&self.home)?;
        let row = store.get(id)?;
        let identity = LaunchIdentity::read(&row)?;
        if identity.config.runtime(&row.request.runtime)?.kind()? == Adapter::Qwen {
            return Err(Error::Unsupported(
                "Qwen does not support live steering".into(),
            ));
        }
        store.enqueue(id, "steer", &json!({"text":text}))
    }
    pub fn answer(&self, id: &AgentId) -> Result<Value> {
        let row = Store::open(&self.home)?.get(id)?;
        let Some(path) = &row.answer_path else {
            return Ok(
                json!({"agent_id":id,"status":row.status,"available":false,"path":null,"size_bytes":null,"sha256":null,"content":null,"inline_complete":false,"relative_path":null,"kind":null,"media_type":null,"proof_version":null}),
            );
        };
        let proof = Proof {
            path: path.clone(),
            bytes: row
                .answer_bytes
                .ok_or_else(|| Error::Integrity("answer size evidence is missing".into()))?,
            sha256: row
                .answer_sha256
                .ok_or_else(|| Error::Integrity("answer digest is missing".into()))?,
            proof_version: 2,
        };
        let root = self.home.join("agents").join(id.as_str());
        let (version, content) = verify::read(&root, &proof, verify::INLINE_ANSWER)?;
        Ok(
            json!({"agent_id":id,"status":row.status,"available":true,"path":path,"size_bytes":proof.bytes,"sha256":proof.sha256,"inline_complete":content.is_some(),"content":content,"relative_path":path.strip_prefix(&root).ok(),"kind":"agent_answer","media_type":verify::MEDIA_TYPE,"proof_version":version}),
        )
    }
    pub fn transcript(&self, id: &AgentId, cursor: i64, limit: usize) -> Result<Value> {
        Store::open(&self.home)?.transcript(id, cursor, limit)
    }
    pub fn delivery_status(&self, id: &AgentId) -> Result<Value> {
        Store::open(&self.home)?.delivery_status(id)
    }
    /// Cancels one pending completion-delivery notification by durable identifier.
    ///
    /// A missing, delivered, or already cancelled notification returns `false` in
    /// the Python-compatible acknowledgement; invalid identifiers are rejected.
    pub fn delivery_cancel(&self, delivery_id: &str) -> Result<Value> {
        let cancelled = Store::open(&self.home)?.cancel_delivery(delivery_id)?;
        Ok(json!({"delivery_id":delivery_id,"cancelled":cancelled}))
    }
    pub async fn list(&self, query: Query) -> Result<Value> {
        query.validate()?;
        let until = tokio::time::Instant::now() + Duration::from_secs_f64(query.wait_seconds);
        loop {
            let value = {
                let store = Store::open(&self.home)?;
                let revision = store.revision()?;
                if query.after_revision.is_some_and(|r| revision <= r)
                    && tokio::time::Instant::now() < until
                {
                    None
                } else {
                    let (rows, total) = store.list(
                        query.active,
                        query.offset,
                        query.limit,
                        query.orchestrator.as_ref(),
                    )?;
                    let items = rows
                        .iter()
                        .map(|r| self.view(&store, r))
                        .collect::<Result<Vec<_>>>()?;
                    let next = query.offset.saturating_add(items.len());
                    Some(
                        json!({"items":items,"total":total,"offset":query.offset,"limit":query.limit,"next_offset":if next<total as usize{Some(next)}else{None},"complete":next>=total as usize,"revision":revision,"observed_at":now()}),
                    )
                }
            };
            if let Some(value) = value {
                return Ok(value);
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }
    /// Wait for a terminal durable revision or this observer's optional deadline.
    ///
    /// The run itself is never given an execution deadline here: `seconds`
    /// bounds only this observer.  Each read opens and drops its own
    /// thread-affine store connection before the Tokio sleep, allowing another
    /// process to commit the terminal status that supplies the answer envelope.
    pub async fn wait(&self, id: &AgentId, seconds: Option<f64>) -> Result<Value> {
        if seconds.is_some_and(|n| !n.is_finite() || n < 0.0) {
            return Err(invalid("wait_seconds must be finite and nonnegative"));
        }
        let until = seconds
            .map(|s| {
                let duration = Duration::try_from_secs_f64(s)
                    .map_err(|_| invalid("wait_seconds is too large"))?;
                tokio::time::Instant::now()
                    .checked_add(duration)
                    .ok_or_else(|| invalid("wait deadline is out of range"))
            })
            .transpose()?;
        loop {
            // This scope drops the thread-affine connection before the await.
            // A waiter therefore owns neither a SQLite transaction nor a socket
            // lane while another process commits the terminal revision.
            let row = { Store::open(&self.home)?.get(id)? };
            if row.status.terminal() {
                return self.answer(id);
            }
            if until.is_some_and(|d| tokio::time::Instant::now() >= d) {
                return Ok(
                    json!({"agent_id":id,"status":row.status,"terminal":false,"available":false}),
                );
            }
            let poll = Duration::from_millis(200);
            let sleep = until
                .map(|deadline| {
                    deadline
                        .saturating_duration_since(tokio::time::Instant::now())
                        .min(poll)
                })
                .unwrap_or(poll);
            tokio::time::sleep(sleep).await;
        }
    }
    pub fn view(&self, store: &Store, row: &Record) -> Result<Value> {
        let observed = now();
        let progress = store.last_progress(&row.id)?;
        let phase = if row.status.terminal() {
            "terminal"
        } else if row.status == Status::Running {
            "running"
        } else if row.status == Status::Cancelling {
            "stopping"
        } else {
            "accepted"
        };
        let phase_event = store.last_event(&row.id, "phase")?;
        let phase = if phase == "accepted" {
            phase_event
                .as_ref()
                .and_then(|v| v.get("phase"))
                .and_then(Value::as_str)
                .filter(|s| ["preparing", "spawning"].contains(s))
                .unwrap_or(phase)
        } else {
            phase
        };
        let policy = row
            .identity
            .as_ref()
            .and_then(|i| i.get("effective_policy"));
        Ok(
            json!({"agent_id":row.id,"runtime":row.request.runtime,"model":row.request.model,"profile":row.request.profile,"task_summary":row.request.task.split_whitespace().collect::<Vec<_>>().join(" ").chars().take(160).collect::<String>(),"status":row.status,
            "created_at":row.created_at,"started_at":row.started_at,"finished_at":row.finished_at,"elapsed_seconds":(row.finished_at.unwrap_or(observed)-row.started_at.unwrap_or(row.created_at)).max(0.0),"last_progress_at":progress,"silence_seconds":if row.status.terminal(){None}else{Some((observed-progress.or(row.started_at).unwrap_or(row.created_at)).max(0.0))},"warned":false,"failure_kind":row.failure_kind,"failure_text":row.failure_text,"answer_available":row.answer_path.is_some(),"answer_bytes":row.answer_bytes,"answer_sha256":row.answer_sha256,"effort":row.request.effort,
            "delivery":store.delivery_status(&row.id)?,"parent_agent_id":row.parent_agent_id,"root_agent_id":row.root_agent_id,"sequence":row.sequence,"cleanup":store.last_event(&row.id,"process_cleanup")?,"policy":policy,"phase":phase,"phase_started_at":row.finished_at.or(row.started_at).unwrap_or(row.created_at),"process_state":process::observe(row.supervisor_pid,row.supervisor_identity.as_deref(),row.supervisor_birth_time),"observed_at":observed,"runtime_outcome":if row.status.terminal(){Some(row.status.as_str())}else{None},"acceptance":"pending"}),
        )
    }
    /// Reconcile a bounded fair page of rows whose recorded ownership is proven gone.
    ///
    /// Native process observation is performed by the lifecycle module and
    /// never treats unavailable OS evidence as a terminal outcome.
    pub fn reconcile(&self) -> Result<usize> {
        let mut store = Store::open(&self.home)?;
        Ok(reconcile::reconcile(&mut store, 100)?.len())
    }
    pub async fn models(&self) -> Result<Value> {
        crate::capacity::models(&self.home).await
    }
    pub fn limits(&self) -> Result<Value> {
        crate::capacity::limits(&self.home)
    }
    pub fn capacity_order(&self) -> Result<Value> {
        crate::capacity::order(&self.home)
    }
}
