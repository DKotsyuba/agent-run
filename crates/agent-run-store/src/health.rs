//! Attempt-fenced health probes using existing events, commands and delivery receipts.
//! No lifetime timeout, schema migration, model loop or automatic cancellation.

use crate::Store;
use agent_run_domain::{
    Error, Result,
    domain::AgentId,
    error::invalid,
    health::{PingPolicy, PingReply, ReplyState},
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Fixed non-model control cadence in seconds.
pub const CONTROL_INTERVAL: f64 = 60.0;
/// Unexplained idle grace before a single optional model probe, in seconds.
pub const IDLE_GRACE: f64 = 300.0;
/// A probe's bounded observation allowance; never an execution lifetime limit.
pub const REPLY_GRACE: f64 = 120.0;
/// Indexed event kind carrying the latest closed, typed state of one attempt.
const STATE: &str = "worker_ping_state_v1";

/// One durable control or model request, never replayed with a different identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Probe {
    /// Command queue identity, scoped by the containing attempt.
    command: i64,
    /// Original enqueue time; retries never extend the probe allowance.
    at: f64,
    /// Optional authenticated model response, not task-success evidence.
    reply: Option<PingReply>,
    /// Persisted closure prevents a clock rollback from reviving an expired probe.
    #[serde(default)]
    closed: bool,
    /// Expected model turn counter; inspection inside this recovery turn cannot
    /// rearm another paid probe. Zero denotes a non-model control command.
    #[serde(default)]
    turn: u64,
}

/// One append-only state fact per control cadence; repeated early sweeps are no-ops.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    /// Frozen new-admission policy; old attempts have no snapshot.
    policy: PingPolicy,
    /// Last broker sweep, used for fair bounded scheduling.
    checked_at: f64,
    /// Last observed non-health native tool row, independent of heartbeat noise.
    progress: i64,
    /// Most recent non-model supervisor challenge.
    control: Option<Probe>,
    /// At most one model challenge until a later native turn brings task activity;
    /// inspections within the recovery turn itself cannot rearm paid polling.
    model: Option<Probe>,
    /// Closed broker diagnosis; no model prose or credentials are retained.
    reason: Option<String>,
    /// Bounded last consistent read facts; public status never rescans transcripts.
    #[serde(default)]
    activity: Option<Activity>,
}

/// Opaque read-prepared facts accepted only by the broker-local operation.
/// Message/turn fences are rechecked in the writer; this type is not wire-decodable.
#[derive(Debug, Clone, Serialize)]
pub struct HealthFacts {
    /// Attempt selected by the reader, never an implicit latest alias.
    attempt: String,
    /// Native turn counter owning the read snapshot.
    turn_count: u64,
    /// Phase observed before potentially expensive activity preparation.
    phase: String,
    /// Absent while running or when concurrent journal progress made the read unstable.
    activity: Option<Activity>,
}

/// Bounded, consistent activity evidence, prepared outside the writer/executor.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Activity {
    /// Time at which the reader completed the observation.
    observed_at: f64,
    /// Indexed per-execution journal fence, including health/user traffic.
    watermark: i64,
    /// Latest non-health tool row of this attempt.
    progress: i64,
    /// Outstanding distinct native tool identities, not application pending prose.
    pending: u64,
    /// Latest task-tool row strictly after this native turn's running boundary.
    turn_progress: i64,
    /// Actual catalog and native coverage proof from the read snapshot.
    ready: bool,
    /// Prepared nonsecret challenge digest; hashing never runs in HealthTick.
    ping_id: String,
    /// At most eight display-safe tool/cursor references.
    sources: Vec<Value>,
    /// Read-time bounded pool wait evidence; never a promise peers will act.
    #[serde(default)]
    pool: Option<Value>,
}

impl Store<'_> {
    /// Initialize only a newly admitted explicit attempt under its frozen policy.
    /// Caller owns the surrounding lifecycle transaction; existing state is retained.
    pub(crate) fn begin_worker_health(&self, run: &AgentId, attempt: &str, at: f64) -> Result<()> {
        let policy = self.get(run)?.request.idle_ping;
        if policy != PingPolicy::Off && self.health_snapshot(run, attempt)?.is_none() {
            self.save_health(
                run,
                attempt,
                at,
                &Snapshot {
                    policy,
                    checked_at: at,
                    progress: 0,
                    control: None,
                    model: None,
                    reason: None,
                    activity: None,
                },
            )?;
        }
        Ok(())
    }

    /// Select at most `limit` due attempts in oldest-check order. Historical,
    /// terminal, closing and cancellation-pending attempts are excluded. This
    /// bounded indexed event lookup never reads task text or tool arguments.
    pub fn health_due(&self, at: f64, limit: usize) -> Result<Vec<AgentId>> {
        if !at.is_finite() || at < 0.0 || !(1..=100).contains(&limit) {
            return Err(invalid("invalid health sweep"));
        }
        let mut stmt=self.conn.prepare("SELECT a.id FROM agents a JOIN attempts t ON t.agent_id=a.id JOIN worker_lifecycle l ON l.attempt_id=t.id \
            JOIN events e ON e.seq=(SELECT MAX(x.seq) FROM events x WHERE x.agent_id=a.id AND x.attempt_id=t.id AND x.kind='worker_ping_state_v1') \
            WHERE a.status='running' AND a.finished_at IS NULL AND t.ownership_active=1 AND t.finished_at IS NULL AND l.finish_json IS NULL \
            AND (json_extract(e.data_json,'$.checked_at')<=? OR json_extract(e.data_json,'$.checked_at')>?) \
            AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed')) \
            ORDER BY json_extract(e.data_json,'$.checked_at'),a.id LIMIT ?")?;
        let ids = stmt
            .query_map(
                params![at - CONTROL_INTERVAL, at + CONTROL_INTERVAL, limit],
                |r| r.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ids.into_iter().map(|id| id.parse()).collect()
    }

    /// Reconcile one attempt's probes atomically. Challenges use existing command
    /// ownership and receipts; source progress, pending tools, closing and cancel
    /// are rechecked inside the sole writer. A timeout records attention, never
    /// kills a task. `recover` alone authorizes one model turn per progress episode.
    /// Returns true for a due committed observation, false for an ineligible/not-due attempt.
    #[cfg(feature = "test-fixtures")]
    pub fn health_tick(&mut self, run: &AgentId, at: f64) -> Result<bool> {
        let Some(facts) = self.prepare_health(run)? else {
            return Ok(false);
        };
        self.health_tick_prepared(run, at, &facts)
    }

    /// Prepare read-heavy activity and hashes outside the sole writer. Running
    /// or concurrently changing work still permits a cheap service probe, but
    /// cannot authorize inference from inconsistent evidence.
    pub fn prepare_health(&self, run: &AgentId) -> Result<Option<HealthFacts>> {
        let Some(mut facts) = self.prepare_health_control(run)? else {
            return Ok(None);
        };
        if facts.phase == "idle" {
            let watermark = self.health_message_watermark(run)?;
            // ponytail: reader-bounded attempt scan; add a projection if read timeouts become common.
            let (progress, pending, sources) = self.health_activity(run, &facts.attempt)?;
            let turn_progress = self.health_progress_in_current_turn(run, &facts.attempt)?;
            let ready = self.health_observation_ready(run, &facts.attempt)?;
            if self.health_message_watermark(run)? == watermark {
                facts.activity = Some(Activity {
                    observed_at: agent_run_domain::domain::now(),
                    watermark,
                    progress,
                    pending,
                    turn_progress,
                    ready,
                    ping_id: agent_run_platform::fs::sha256(
                        format!("{}:{progress}", facts.attempt).as_bytes(),
                    ),
                    sources,
                    pool: self.health_pool_wait(run)?,
                });
            }
        }
        Ok(Some(facts))
    }

    /// Cheap fallback when a bounded activity read cannot complete. A responsive
    /// control channel never turns missing activity evidence into paid inference.
    pub fn prepare_health_control(&self, run: &AgentId) -> Result<Option<HealthFacts>> {
        let Some((attempt, phase, _, turn_count)) = self.health_attempt(run)? else {
            return Ok(None);
        };
        if self.health_snapshot(run, &attempt)?.is_none() {
            return Ok(None);
        }
        Ok(Some(HealthFacts {
            attempt,
            turn_count,
            phase,
            activity: None,
        }))
    }

    /// Commit only bounded, prepared facts after indexed scope/turn/message
    /// rechecks. No transcript scan or nonce hashing runs inside this transaction.
    pub fn health_tick_prepared(
        &mut self,
        run: &AgentId,
        at: f64,
        facts: &HealthFacts,
    ) -> Result<bool> {
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid health observation"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let Some((attempt, phase, transition, turn_count)) = self.health_attempt(run)? else {
            return Ok(false);
        };
        if facts.attempt != attempt {
            return Ok(false);
        }
        let Some(mut state) = self.health_snapshot(run, &attempt)? else {
            return Ok(false);
        };
        if at < state.checked_at {
            state.checked_at = at;
            state.reason = Some("clock_regression".into());
            if let Some(probe) = &mut state.model {
                probe.closed = true;
            }
            self.health_notice(run, &attempt, state.progress, "clock_regression", at)?;
            self.save_health(run, &attempt, at, &state)?;
            tx.commit()?;
            return Ok(true);
        }
        if at < state.checked_at + CONTROL_INTERVAL {
            return Ok(false);
        }
        state.checked_at = at;
        let activity = match &facts.activity {
            Some(activity)
                if facts.attempt == attempt
                    && facts.turn_count == turn_count
                    && activity.watermark == self.health_message_watermark(run)? =>
            {
                Some(activity)
            }
            _ => None,
        };
        let progress = activity.map_or(state.progress, |a| a.progress);
        let pending = activity.map_or(u64::MAX, |a| a.pending);
        if let Some(activity) = activity {
            state.activity = Some(activity.clone());
            if state.reason.as_deref() == Some("activity_observation_unavailable") {
                state.reason = None;
            }
        } else if facts.attempt == attempt
            && facts.turn_count == turn_count
            && facts.phase == "idle"
            && phase == "idle"
            && at - transition >= IDLE_GRACE
        {
            state.reason = Some("activity_observation_unavailable".into());
        }

        let fresh_turn = match &state.model {
            None => true,
            Some(probe) => {
                (turn_count > probe.turn
                    && activity.is_some_and(|a| a.turn_progress > state.progress))
                    || self.health_known_unsent(probe.command)?
            }
        };
        if progress > state.progress && fresh_turn {
            state.progress = progress;
            state.model = None;
            state.reason = None;
        }
        let mut control_alive = false;
        let mut control_pending = false;
        if let Some(probe) = &state.control {
            let (status, result, observed) = self.health_command(probe.command)?;
            control_pending = status != "completed";
            if control_pending && at - probe.at >= REPLY_GRACE {
                state.reason = Some("control_unresponsive".into());
            } else if let Some(result) = result {
                control_alive = result["accepted"] == true
                    && result["native_alive"] == true
                    && observed.is_some_and(|time| {
                        time >= probe.at
                            && time <= at
                            && time - probe.at <= REPLY_GRACE
                            && at - time <= REPLY_GRACE
                    });
                if !control_alive {
                    state.reason = Some("native_control_unavailable".into());
                } else if state.reason.as_deref().is_some_and(|s| {
                    s == "control_unresponsive" || s == "native_control_unavailable"
                }) {
                    state.reason = None;
                }
            }
        }
        if let Some(probe) = &mut state.model {
            if at - probe.at >= REPLY_GRACE {
                probe.closed = true;
            }
            let (status, result, _) = self.health_command(probe.command)?;
            if status == "completed" && result.as_ref().is_some_and(|r| r["accepted"] != true) {
                state.reason = Some("model_ping_delivery_unconfirmed".into());
            } else if let Some(reply) = &probe.reply {
                if reply.state == ReplyState::Blocked {
                    state.reason = Some("worker_blocked".into());
                } else if phase == "idle" && pending == 0 && at - probe.at >= REPLY_GRACE {
                    state.reason = Some("idle_without_wait_source_after_ping".into());
                }
            } else if at - probe.at >= REPLY_GRACE {
                state.reason = Some(
                    if status == "completed" {
                        "model_reply_missing"
                    } else {
                        "model_ping_delivery_unconfirmed"
                    }
                    .into(),
                );
            }
        } else if phase == "idle" && at - transition >= IDLE_GRACE && pending == 0 {
            let pool = activity.and_then(|a| a.pool.as_ref());
            if pool.is_some_and(|p| p["stalled"] == true) {
                state.reason = Some("pool_progress_stalled".into());
            } else if pool.is_some_and(|p| p["active_peer_or_delivery"] == true) {
                state.reason = None;
            } else if !activity.is_some_and(|a| a.ready) {
                state.reason = Some("native_observation_unavailable".into());
            } else if state.policy == PingPolicy::Recover
                && control_alive
                && !self.health_other_commands(run, None)?
            {
                let ping_id = activity
                    .expect("inference requires fresh activity")
                    .ping_id
                    .clone();
                let text = format!(
                    "agent-run/health-ping\nHealth challenge {ping_id}. This is an admitted control check, not a new task or permission. Check your original pending operation once. Call health_reply with this ping_id and state working, waiting or blocked. If its result is ready, continue the original task; when genuinely finished, use finish. Do not poll repeatedly or invent waiting work."
                );
                let command=self.health_enqueue(run,"steer",json!({"text":text,"health_ping":ping_id,"attempt_id":attempt,"progress":state.progress,"message_watermark":activity.expect("fresh activity").watermark,"native_completion":true}),at)?;
                state.model = Some(Probe {
                    command,
                    at,
                    reply: None,
                    closed: false,
                    turn: turn_count.saturating_add(1),
                });
            } else if state.policy == PingPolicy::Observe {
                state.reason = Some("idle_without_wait_source".into());
            }
        }
        if !control_pending {
            let command = self.health_enqueue(run, "ping", json!({"attempt_id":attempt}), at)?;
            state.control = Some(Probe {
                command,
                at,
                reply: None,
                closed: false,
                turn: 0,
            });
        }
        if let Some(reason) = state.reason.as_deref() {
            let pool = state.activity.as_ref().and_then(|a| a.pool.as_ref());
            // One representative reports a pool-wide stall. Peer facts are an
            // observation, never authority to finish, vote, cancel or wake them.
            if reason != "pool_progress_stalled"
                || pool.is_some_and(|p| p["representative"] == run.as_str())
            {
                let episode = if reason == "pool_progress_stalled" {
                    pool.and_then(|p| p["log_revision"].as_i64())
                        .unwrap_or(state.progress)
                } else {
                    state.progress
                };
                self.health_notice(run, &attempt, episode, reason, at)?;
            }
        }
        self.save_health(run, &attempt, at, &state)?;
        tx.commit()?;
        Ok(true)
    }

    /// Authenticate and accept only the exact outstanding challenge. Identical
    /// retries of the current challenge preserve their receipt; changed replies
    /// conflict and retired challenges cannot affect a new probe. No arbitrary
    /// model prose, process authority, finish outcome or task progress is trusted.
    /// Returns true for an identical replay and false for first acceptance.
    pub fn accept_health_reply(
        &mut self,
        run: &AgentId,
        attempt: &str,
        token: &str,
        input: &PingReply,
        at: f64,
    ) -> Result<bool> {
        input.validate()?;
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid health reply time"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let auth = crate::worker::authenticate_attempt(&tx, run, attempt, token, at)?
            .filter(|a| a.running);
        if auth.is_none()
            || self
                .health_attempt(run)?
                .as_ref()
                .is_none_or(|a| a.0 != attempt)
        {
            return Err(invalid("worker attempt is not running"));
        }
        let mut state = self
            .health_snapshot(run, attempt)?
            .ok_or_else(|| invalid("health checks were not admitted"))?;
        let probe = state
            .model
            .as_mut()
            .ok_or_else(|| invalid("no model ping is pending"))?;
        if !self.health_catalog_ready(attempt)? {
            return Err(invalid("health reply catalog was not observed"));
        }
        let payload: String = tx.query_row(
            "SELECT payload_json FROM commands WHERE id=?",
            [probe.command],
            |r| r.get(0),
        )?;
        let payload: Value = serde_json::from_str(&payload)?;
        if payload["health_ping"] != input.ping_id {
            return Err(invalid("stale or foreign ping_id"));
        }
        if let Some(prior) = &probe.reply {
            if prior != input {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(true);
        }
        let command_state: String = tx.query_row(
            "SELECT state FROM commands WHERE id=?",
            [probe.command],
            |r| r.get(0),
        )?;
        if probe.closed
            || command_state == "pending"
            || at < probe.at
            || at - probe.at >= REPLY_GRACE
        {
            return Err(invalid("model ping was not delivered or its reply is late"));
        }
        probe.reply = Some(input.clone());
        self.save_health(run, attempt, at, &state)?;
        tx.commit()?;
        Ok(false)
    }

    /// Revalidate a control/model command against its current exact owned attempt.
    /// A stale queued model request cannot interrupt new native work, cancellation,
    /// finish or changed task progress. Called immediately before native input.
    pub fn health_command_current(
        &self,
        run: &AgentId,
        payload: &Value,
        model: bool,
        at: f64,
    ) -> Result<bool> {
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid probe observation time"));
        }
        let Some((attempt, phase, _, _)) = self.health_attempt(run)? else {
            return Ok(false);
        };
        if payload["attempt_id"] != attempt {
            return Ok(false);
        }
        if !model {
            return Ok(true);
        }
        let Some(state) = self.health_snapshot(run, &attempt)? else {
            return Ok(false);
        };
        let watermark = self.health_message_watermark(run)?;
        let Some(probe) = state.model else {
            return Ok(false);
        };
        if probe.closed || at < probe.at || at - probe.at >= REPLY_GRACE {
            return Ok(false);
        }
        let expected: String = self.conn.query_row(
            "SELECT payload_json FROM commands WHERE id=?",
            [probe.command],
            |r| r.get(0),
        )?;
        let expected: Value = serde_json::from_str(&expected)?;
        Ok(phase == "idle"
            && payload["message_watermark"] == watermark
            && state.policy == PingPolicy::Recover
            && self.health_small_observation_ready(run, &attempt)?
            && payload["health_ping"] == expected["health_ping"]
            && payload["progress"] == state.progress
            && !self.health_other_commands(run, Some(probe.command))?
            && self.health_command(probe.command)?.0 == "claimed")
    }

    /// Return public content-free diagnostics of the latest attempt. This does
    /// not mutate state, certify a model's claim or extend a probe's deadline.
    pub fn health_view(&self, run: &AgentId) -> Result<Option<Value>> {
        let row = self.get(run)?;
        let attempt: Option<String> = self
            .conn
            .query_row(
                "SELECT id FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
                [run.as_str()],
                |r| r.get(0),
            )
            .optional()?;
        let Some(attempt) = attempt else {
            return Ok(None);
        };
        let Some(state) = self.health_snapshot(run, &attempt)? else {
            return Ok(None);
        };
        let idle = self
            .worker_lifecycle_view(run)?
            .is_some_and(|v| v["phase"] == "idle");
        let pending = state.activity.as_ref().map_or(0, |a| a.pending);
        let sources = state
            .activity
            .as_ref()
            .map_or_else(Vec::new, |a| a.sources.clone());
        let observed_at = state.activity.as_ref().map(|a| a.observed_at);
        let more = pending.saturating_sub(sources.len() as u64);
        let control=state.control.as_ref()
.map(|probe|self.health_command(probe.command).map(|(status,result,at)|json!({"state":status,"checked_at":at,"native_alive":result.and_then(|r|r["native_alive"].as_bool())}))).transpose()?;
        let model=state.model.as_ref().map(|probe|self.conn.query_row("SELECT json_extract(payload_json,'$.health_ping') FROM commands WHERE id=?",[probe.command],|r|r.get::<_,String>(0)).map(|id|json!({"ping_id":id,"requested_at":probe.at,"observation_closed":probe.closed,"reply":probe.reply}))).transpose()?;
        let pool = state.activity.as_ref().and_then(|a| a.pool.clone());
        Ok(Some(
            json!({"pool":pool,"policy":state.policy,"state":if row.status.terminal(){"terminal"}else if state.reason.is_some(){"needs_attention"}else if model.is_some(){"checking"}else if idle&&pending>0{"waiting"}else{"observing"},"reason":state.reason,"waiting_for":if idle&&pending>0{Some("native_tool")}else if idle&&pool.is_some(){Some("pool_entry_or_proposal")}else{None},"activity_observed_at":observed_at,"pending_native_tools":pending,"waiting_sources":sources,"waiting_sources_more":more,"control":control,"model":model}),
        ))
    }

    /// Prepare concrete pool wait evidence off the writer: at most five current
    /// members, their exact lineage tips, native phases, queued message count,
    /// and the indexed durable-log watermark. No task/message bodies are read.
    /// A membership alone never proves a useful event will be produced.
    fn health_pool_wait(&self, run: &AgentId) -> Result<Option<Value>> {
        let pool:Option<String>=self.conn.query_row("SELECT m.pool_id FROM pool_members m JOIN pools p ON p.id=m.pool_id WHERE m.replaced_by IS NULL AND p.state='open' AND (m.agent_id=?1 OR m.agent_id=(SELECT root_agent_id FROM agents WHERE id=?1)) LIMIT 1",[run.as_str()],|r|r.get(0)).optional()?;
        let Some(pool) = pool else { return Ok(None) };
        let (revision, last_at): (i64, Option<f64>) = self.conn.query_row(
            "SELECT COALESCE(MAX(seq),0),MAX(created_at) FROM pool_entries WHERE pool_id=?",
            [&pool],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let mut statement=self.conn.prepare("SELECT agent_id FROM pool_members WHERE pool_id=? AND replaced_by IS NULL ORDER BY agent_id LIMIT 6")?;
        let roots = statement
            .query_map([&pool], |r| r.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if roots.len() > 5 {
            return Err(Error::Integrity("pool wait roster exceeds bound".into()));
        }
        let mut peers = Vec::new();
        let mut all_idle = !roots.is_empty();
        let mut active = false;
        let mut representative = None;
        for root in roots {
            let tip = crate::pool_log::tip_of(&self.conn, &root.parse()?)?;
            let monitored:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM events e JOIN attempts t ON t.id=e.attempt_id WHERE e.agent_id=? AND e.kind='worker_ping_state_v1' AND t.ownership_active=1 AND t.finished_at IS NULL AND json_extract(e.data_json,'$.policy') IN ('observe','recover'))",[tip.as_str()],|r|r.get(0))?;
            if representative.is_none() && monitored {
                representative = Some(tip.to_string());
            }
            let (status,phase):(String,Option<String>)=self.conn.query_row("SELECT a.status,(SELECT l.phase FROM attempts t JOIN worker_lifecycle l ON l.attempt_id=t.id WHERE t.agent_id=a.id AND t.ownership_active=1 AND t.finished_at IS NULL ORDER BY t.number DESC LIMIT 1) FROM agents a WHERE a.id=?",[tip.as_str()],|r|Ok((r.get(0)?,r.get(1)?)))?;
            let queued:i64=self.conn.query_row("SELECT COUNT(*) FROM commands WHERE agent_id=? AND state IN ('pending','claimed') AND kind IN ('pool','steer') AND json_extract(payload_json,'$.health_ping') IS NULL",[tip.as_str()],|r|r.get(0))?;
            all_idle &= status == "running" && phase.as_deref() == Some("idle") && queued == 0;
            active |= (tip != *run && status == "running" && phase.as_deref() == Some("running"))
                || queued > 0;
            peers.push(json!({"agent_id":root,"execution_id":tip,"status":status,"phase":phase,"pending_messages":queued}));
        }
        Ok(Some(
            json!({"pool_id":pool,"log_revision":revision,"last_entry_at":last_at,"members":peers,"representative":representative,"stalled":all_idle,"active_peer_or_delivery":active}),
        ))
    }

    /// True only for the new explicit catalog which declares read-loop guard
    /// side effects. Older frozen catalogs retain their original read contract.
    /// The subsequent read/report still authenticates the actual bearer token.
    pub fn pool_read_guard_enabled(&self, run: &AgentId, attempt: &str) -> Result<bool> {
        let proof:Option<(u32,String)>=self.conn.query_row("SELECT c.pool_catalog_version,c.pool_catalog_digest FROM worker_capabilities c JOIN attempts t ON t.id=c.attempt_id WHERE c.attempt_id=?1 AND t.agent_id=?2 AND c.pool_catalog_version IS NOT NULL AND c.pool_catalog_digest IS NOT NULL",params![attempt,run.as_str()],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        Ok(self.get(run)?.request.explicit_finish
            && proof.is_some_and(|(version, digest)| {
                version == agent_run_domain::worker::FINISH_CATALOG_VERSION
                    && agent_run_domain::worker::known_pool_catalog(version, &digest)
            }))
    }

    /// Persist one fixed attention notice for an authenticated empty-read
    /// episode, including unbound runs. The indexed pool revision is rechecked;
    /// stale observations and duplicates return false without another notice.
    /// Notice, guarded control intent and health reason commit atomically. This
    /// performs no native I/O and cannot close, finish, vote or cancel a run.
    pub fn report_pool_read_loop(
        &self,
        run: &AgentId,
        attempt: &str,
        token: &str,
        revision: u64,
        at: f64,
    ) -> Result<bool> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let auth = crate::worker::authenticate_attempt(&tx, run, attempt, token, at)?;
        if !auth.is_some_and(|a| a.running)
            || !self.pool_read_guard_enabled(run, attempt)?
            || revision > i64::MAX as u64
        {
            return Err(invalid("invalid pool read-loop observation"));
        }
        let current:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM pool_members m JOIN pools p ON p.id=m.pool_id WHERE m.replaced_by IS NULL AND p.state='open' AND (m.agent_id=?1 OR m.agent_id=(SELECT root_agent_id FROM agents WHERE id=?1)) AND (SELECT COALESCE(MAX(seq),0) FROM pool_entries WHERE pool_id=p.id)=?2)",params![run.as_str(),revision as i64],|r|r.get(0))?;
        if !current {
            tx.commit()?;
            return Ok(false);
        }
        let number: i64 =
            tx.query_row("SELECT number FROM attempts WHERE id=?", [attempt], |r| {
                r.get(0)
            })?;
        let key = format!("health-{number}-{revision}-pool_empty_read_loop");
        let exists: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_notifications WHERE agent_id=? AND request_id=?)",
            params![run.as_str(), key],
            |r| r.get(0),
        )?;
        if !exists {
            self.health_notice(run, attempt, revision as i64, "pool_empty_read_loop", at)?;
            self.health_enqueue(run,"steer",json!({"text":"Agent Run detected repeated empty reads of an unchanged pool log. End this inference turn now and await a new pushed pool entry on this same session. Do not call pool_read again without a new event. Do not finish or cancel merely to suspend. Resume the original task on the next owned event; use finish only when it is truly done, blocked or failed. Report any concrete external blocker once to the orchestrator.","attempt_id":attempt,"pool_read_guard":revision}),at)?;
            if let Some(mut state) = self.health_snapshot(run, attempt)? {
                state.reason = Some("pool_empty_read_loop".into());
                self.save_health(run, attempt, at, &state)?;
            }
        }
        tx.commit()?;
        Ok(!exists)
    }

    /// A polling correction may steer only its exact still-running native turn
    /// while the same pool log revision remains current. It must never wake an
    /// idle model, a replacement attempt, a closing run or a newly advanced pool.
    pub fn pool_poll_guard_current(&self, run: &AgentId, payload: &Value) -> Result<bool> {
        let (Some(attempt), Some(revision)) = (
            payload["attempt_id"].as_str(),
            payload["pool_read_guard"].as_i64(),
        ) else {
            return Ok(false);
        };
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM attempts t JOIN agents a ON a.id=t.agent_id JOIN worker_lifecycle l ON l.attempt_id=t.id JOIN pool_members m ON (m.agent_id=a.id OR m.agent_id=a.root_agent_id) JOIN pools p ON p.id=m.pool_id WHERE a.id=? AND t.id=? AND a.status='running' AND t.ownership_active=1 AND t.finished_at IS NULL AND l.phase='running' AND l.finish_json IS NULL AND m.replaced_by IS NULL AND p.state='open' AND (SELECT COALESCE(MAX(seq),0) FROM pool_entries WHERE pool_id=p.id)=?)",params![run.as_str(),attempt,revision],|r|r.get::<_,bool>(0))?)
    }

    /// Decode the current typed snapshot without silently replacing corrupt state.
    fn health_snapshot(&self, run: &AgentId, attempt: &str) -> Result<Option<Snapshot>> {
        let raw:Option<String>=self.conn.query_row("SELECT data_json FROM events WHERE agent_id=? AND attempt_id=? AND kind=? ORDER BY seq DESC LIMIT 1",params![run.as_str(),attempt,STATE],|r|r.get(0)).optional()?;
        let state: Option<Snapshot> = raw
            .map(|s| serde_json::from_str(&s).map_err(Error::from))
            .transpose()?;
        let policy = self.get(run)?.request.idle_ping;
        if state.as_ref().is_some_and(|state| state.policy != policy) {
            return Err(Error::Integrity(
                "health policy differs from frozen admission".into(),
            ));
        }
        Ok(state)
    }

    /// Append one transaction-owned state fact, retaining historical evidence.
    fn save_health(&self, run: &AgentId, attempt: &str, at: f64, state: &Snapshot) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,?,?)",
            params![
                run.as_str(),
                attempt,
                at,
                STATE,
                serde_json::to_string(state)?
            ],
        )?;
        Ok(())
    }

    /// Resolve the usable current attempt with finish and cancellation fences.
    fn health_attempt(&self, run: &AgentId) -> Result<Option<(String, String, f64, u64)>> {
        Ok(self.conn.query_row("SELECT t.id,l.phase,l.transition_at,l.turn_count FROM attempts t JOIN agents a ON a.id=t.agent_id JOIN worker_lifecycle l ON l.attempt_id=t.id \
            WHERE t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL AND a.status='running' AND a.finished_at IS NULL AND l.finish_json IS NULL \
            AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed'))",[run.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?)
    }

    /// Read actual native task activity; service/model health traffic cannot reset
    /// the recovery budget. Unmatched owned calls protect long native jobs.
    fn health_activity(&self, run: &AgentId, attempt: &str) -> Result<(i64, u64, Vec<Value>)> {
        let progress=self.conn.query_row("SELECT COALESCE(MAX(seq),0) FROM messages WHERE agent_id=? AND attempt_id=? AND role IN ('tool_call','tool_result') AND name NOT GLOB '*health_reply'",params![run.as_str(),attempt],|r|r.get(0))?;
        let mut statement=self.conn.prepare("SELECT MIN(seq),MIN(name) FROM messages WHERE agent_id=? AND attempt_id=? AND role IN ('tool_call','tool_result') AND (name IS NULL OR name NOT GLOB '*health_reply') AND raw_ref IS NOT NULL GROUP BY raw_ref HAVING SUM(role='tool_call')>0 AND SUM(role='tool_result')=0 ORDER BY MIN(seq)")?;
        let mut rows = statement.query(params![run.as_str(), attempt])?;
        let mut pending = 0_u64;
        let mut sources = Vec::new();
        while let Some(row) = rows.next()? {
            pending += 1;
            if sources.len() < 8 {
                let name = row
                    .get::<_, Option<String>>(1)?
                    .map(|n| n.chars().take(128).collect::<String>());
                sources.push(json!({"kind":"native_tool","name":name,"transcript_cursor":row.get::<_,i64>(0)?.saturating_sub(1)}));
            }
        }
        Ok((progress, pending, sources))
    }

    /// Indexed latest message id fences all prepared work without rescanning its content.
    fn health_message_watermark(&self, run: &AgentId) -> Result<i64> {
        Ok(self.conn.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM messages WHERE agent_id=?",
            [run.as_str()],
            |r| r.get(0),
        )?)
    }

    /// Constant-size observer/catalog checks for the supervisor's input boundary.
    /// A new unowned tool row also changes the separately checked message fence.
    fn health_small_observation_ready(&self, run: &AgentId, attempt: &str) -> Result<bool> {
        let covered:bool=self.conn.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND attempt_id=?2 AND kind='native_tool_observer_v1') AND NOT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND attempt_id=?2 AND kind='native_tool_coverage_gap_v1')",params![run.as_str(),attempt],|r|r.get(0))?;
        Ok(covered && self.health_catalog_ready(attempt)?)
    }

    /// Recovery requires complete native ownership observation and the actual
    /// connected worker catalog, not just a configured tool name or process pid.
    fn health_observation_ready(&self, run: &AgentId, attempt: &str) -> Result<bool> {
        let covered: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND attempt_id=?2 AND kind='native_tool_observer_v1') AND NOT EXISTS(SELECT 1 FROM events WHERE agent_id=?1 AND attempt_id=?2 AND kind='native_tool_coverage_gap_v1') AND NOT EXISTS(SELECT 1 FROM messages WHERE agent_id=?1 AND attempt_id=?2 AND role='tool_call' AND raw_ref IS NULL)", params![run.as_str(),attempt], |r|r.get(0))?;
        Ok(covered && self.health_catalog_ready(attempt)?)
    }

    /// Only the current explicit health-capable catalog can answer challenges;
    /// recognizing a historical pool catalog is not proof it has health_reply.
    fn health_catalog_ready(&self, attempt: &str) -> Result<bool> {
        Ok(self.conn.query_row("SELECT pool_catalog_version=? AND length(pool_catalog_digest)=64 FROM worker_capabilities WHERE attempt_id=?",params![agent_run_domain::worker::FINISH_CATALOG_VERSION,attempt],|r|r.get::<_,Option<bool>>(0))?.unwrap_or(false))
    }

    /// The latest real running boundary precedes these tool facts. Critical
    /// turn writes flush prior journal rows first, so work from the recovery
    /// turn cannot be retroactively credited to a later empty peer/user wake.
    fn health_progress_in_current_turn(&self, run: &AgentId, attempt: &str) -> Result<i64> {
        Ok(self.conn.query_row("SELECT COALESCE(MAX(seq),0) FROM messages m WHERE m.agent_id=?1 AND m.attempt_id=?2 AND m.role IN ('tool_call','tool_result') AND m.name NOT GLOB '*health_reply' AND m.at>(SELECT MAX(at) FROM events WHERE agent_id=?1 AND attempt_id=?2 AND kind='worker_turn_phase_v1' AND json_extract(data_json,'$.phase')='running')",params![run.as_str(),attempt],|r|r.get(0))?)
    }

    /// Only our explicit pre-input refusals establish that no model turn was
    /// started. Generic transport errors/unknown native outcomes never rearm.
    fn health_known_unsent(&self, command: i64) -> Result<bool> {
        let (state, result, _) = self.health_command(command)?;
        Ok(state == "completed"
            && result.is_some_and(|result| {
                result["accepted"] == false
                    && matches!(
                        result["reason"].as_str(),
                        Some("stale_health_probe" | "native_not_idle")
                    )
            }))
    }

    /// Read one exact durable command receipt, including uncertain native outcomes.
    fn health_command(&self, id: i64) -> Result<(String, Option<Value>, Option<f64>)> {
        let (state, raw, at): (String, Option<String>, Option<f64>) = self.conn.query_row(
            "SELECT state,result_json,completed_at FROM commands WHERE id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        Ok((
            state,
            raw.map(|s| serde_json::from_str(&s)).transpose()?,
            at,
        ))
    }

    /// Avoid competing with an already admitted user/native/peer wake.
    fn health_other_commands(&self, run: &AgentId, except: Option<i64>) -> Result<bool> {
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind<>'ping' AND state IN ('pending','claimed') AND id<>?)",params![run.as_str(),except.unwrap_or(-1)],|r|r.get(0))?)
    }

    /// Queue prepared bounded facts under the caller's existing writer transaction.
    fn health_enqueue(&self, run: &AgentId, kind: &str, payload: Value, at: f64) -> Result<i64> {
        self.conn.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,?,?,'pending',?)",params![run.as_str(),kind,payload.to_string(),at])?;
        Ok(self.conn.last_insert_rowid())
    }

    /// Queue one fixed broker risk notice per cause/progress episode through the
    /// existing durable worker-report delivery route. Unbound notices stay held;
    /// no callback token, model prose, process arguments or new authority is used.
    fn health_notice(
        &self,
        run: &AgentId,
        attempt: &str,
        progress: i64,
        reason: &str,
        at: f64,
    ) -> Result<()> {
        let number: i64 = self.conn.query_row(
            "SELECT number FROM attempts WHERE id=? AND agent_id=?",
            params![attempt, run.as_str()],
            |r| r.get(0),
        )?;
        let key = format!("health-{number}-{progress}-{reason}");
        let exists: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_notifications WHERE agent_id=? AND request_id=?)",
            params![run.as_str(), key],
            |r| r.get(0),
        )?;
        if exists {
            return Ok(());
        }
        let session: Option<String> = self.conn.query_row(
            "SELECT orchestrator_session_id FROM agents WHERE id=?",
            [run.as_str()],
            |r| r.get(0),
        )?;
        self.conn.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'worker_notification',?)",params![run.as_str(),attempt,at,json!({"request_id":key,"kind":"risk","source":"broker_health"}).to_string()])?;
        let event = self.conn.last_insert_rowid();
        let id = format!("ntf_{}", uuid::Uuid::new_v4().simple());
        self.conn.execute("INSERT INTO deliveries(id,agent_id,orchestrator_session_id,terminal_event_seq,state,next_attempt_at) VALUES(?,?,?,?,?,?)",params![id,run.as_str(),session,event,if session.is_some(){"pending"}else{"waiting_binding"},at])?;
        self.conn.execute("INSERT INTO worker_notifications(delivery_id,agent_id,attempt_id,request_id,kind,message,created_at) VALUES(?,?,?,?,'risk',?,?)",params![id,run.as_str(),attempt,key,format!("Agent Run health check: {reason}. Execution remains active; inspect its health state before continuing or cancelling. No work was discarded."),at])?;
        Ok(())
    }
}
