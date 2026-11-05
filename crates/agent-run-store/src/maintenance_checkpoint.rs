//! Staged attempt-bound maintenance checkpoints, separate from terminal finish.
//!
//! Broker callbacks provide a verified secret guard and exact worker authority;
//! accepting a checkpoint never closes a turn or releases process ownership.

use crate::{Store, writer::WriteTx};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::{
    Error, Result,
    domain::AgentId,
    error::invalid,
    worker::{
        FinishGuard, MAINTENANCE_PAUSE_CATALOG_VERSION, MaintenancePauseReceipt,
        MaintenancePauseRequest, maintenance_pause_catalog_digest,
    },
};
use agent_run_platform::fs::sha256;
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Immutable request for one exact attempt in one maintenance epoch.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceChallenge {
    /// Durable maintenance epoch that captured this execution.
    pub epoch: i64,
    /// Exact logical execution selected by the broker.
    pub execution: AgentId,
    /// Exact live attempt bound to the worker capability.
    pub attempt_id: String,
    /// Opaque identifier of this request, echoed by the worker.
    pub challenge_id: String,
    /// Time the challenge was first recorded, in Unix seconds.
    pub requested_at: f64,
}

impl std::fmt::Debug for MaintenanceChallenge {
    /// Do not reveal the challenge through diagnostic formatting.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MaintenanceChallenge")
            .field("epoch", &self.epoch)
            .field("execution", &self.execution)
            .field("attempt_id", &self.attempt_id)
            .field("requested_at", &self.requested_at)
            .finish_non_exhaustive()
    }
}

/// A recorded challenge and the single queued instruction that carries it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceSteer {
    /// Exact epoch, execution, attempt and challenge selected by the broker.
    pub challenge: MaintenanceChallenge,
    /// Durable command id; retries return this same id.
    pub command_id: i64,
    /// Whether this challenge already had a command.
    pub duplicate: bool,
}

/// Reject malformed challenge/digest spellings before a lookup or mutation.
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// Time is observation metadata; no lifetime or automatic cancellation is added.
fn valid_time(at: f64) -> Result<()> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("invalid maintenance observation time"));
    }
    Ok(())
}

/// New pause requests stop when preparation is deferred or ready; already
/// claimed requests can still deliver their callback and settle safely.
fn require_preparing(conn: &rusqlite::Connection, epoch: i64) -> Result<()> {
    let active: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM maintenance_fences WHERE epoch=? AND ended_at IS NULL AND phase='preparing')",
        [epoch], |row| row.get(0),
    )?;
    if !active {
        return Err(invalid("maintenance is not preparing"));
    }
    Ok(())
}

/// The fixed broker-authored pause instruction for one validated challenge.
pub(crate) fn maintenance_steer_text(challenge_id: &str) -> String {
    format!(
        "Agent Run is preparing a planned restart. Finish or safely stop your current operation and do not start new long-running work. Wait for your current tools and background tasks to settle, then call maintenance_pause with challenge_id {challenge_id} and a concise checkpoint of completed and remaining work. This is a temporary pause; call finish only if the original task is actually complete. Never include credentials."
    )
}

impl Store<'_> {
    /// Observe whether the exact attempt currently supports a pause request.
    /// This is advisory; the writer rechecks the same facts atomically.
    pub fn maintenance_pause_eligible(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
    ) -> Result<bool> {
        self.maintenance_attempt_eligible(epoch, run, attempt)
    }

    /// Prove the open cohort, current live attempt, absence of cancellation and
    /// actual terminal finish, and the exact v5 role-bound catalog. Old
    /// catalogs remain ineligible even when they support ordinary finish.
    fn maintenance_attempt_eligible(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
    ) -> Result<bool> {
        let live: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_cohort m JOIN maintenance_fences f ON f.epoch=m.epoch \
             JOIN agents a ON a.id=m.agent_id JOIN attempts t ON t.agent_id=a.id \
             JOIN worker_lifecycle l ON l.attempt_id=t.id \
             WHERE m.epoch=? AND a.id=? AND a.sequence=m.sequence AND t.id=? AND f.ended_at IS NULL \
             AND a.status='running' AND a.finished_at IS NULL AND t.state='running' \
             AND t.finished_at IS NULL AND t.ownership_active=1 AND l.finish_json IS NULL \
             AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed')))",
            params![epoch,run.as_str(),attempt], |row| row.get(0),
        )?;
        if !live {
            return Ok(false);
        }
        let record = self.get(run)?;
        if !record.request.explicit_finish {
            return Ok(false);
        }
        let Some(payload) = record
            .identity
            .as_ref()
            .and_then(|identity| identity.get("authority")?.get("role_payload"))
        else {
            return Ok(false);
        };
        let role = ResolvedRolePlan::from_payload(payload)?;
        if !role.worker_mcp {
            return Ok(false);
        }
        let expected = maintenance_pause_catalog_digest(role.research_tools_only());
        Ok(self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM worker_capabilities WHERE attempt_id=? AND pool_catalog_version=? \
             AND pool_catalog_digest=? AND pool_catalog_observed_at IS NOT NULL)",
            params![attempt,MAINTENANCE_PAUSE_CATALOG_VERSION,expected], |row| row.get(0),
        )?)
    }

    /// Record a broker-issued challenge without sending a message or changing
    /// lifecycle state. Exact retries retain the challenge; a replacement for
    /// the same epoch/attempt conflicts. Catalog v5 requires an explicit
    /// transport marker that existing supervisors do not set.
    pub fn request_maintenance_checkpoint(
        &mut self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        challenge_id: &str,
        at: f64,
    ) -> Result<MaintenanceChallenge> {
        let tx = WriteTx::begin(&self.conn)?;
        require_preparing(&tx, epoch)?;
        let challenge = self.ensure_maintenance_challenge(epoch, run, attempt, challenge_id, at)?;
        tx.commit()?;
        Ok(challenge)
    }

    /// Create a challenge inside the caller's write transaction, or identify
    /// an identical existing challenge without changing its original time.
    fn ensure_maintenance_challenge(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        challenge_id: &str,
        at: f64,
    ) -> Result<MaintenanceChallenge> {
        valid_time(at)?;
        if epoch <= 0 || !valid_digest(challenge_id) {
            return Err(invalid("invalid maintenance challenge"));
        }
        if !self.maintenance_attempt_eligible(epoch, run, attempt)? {
            return Err(invalid(
                "worker attempt is not eligible for maintenance pause",
            ));
        }
        let prior: Option<(String,f64)> = self.conn.query_row(
            "SELECT challenge_id,requested_at FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
            params![epoch,run.as_str(),attempt], |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional()?;
        let requested_at = if let Some((prior, requested_at)) = prior {
            if prior != challenge_id {
                return Err(Error::Conflict);
            }
            requested_at
        } else {
            if self.conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM maintenance_checkpoints WHERE challenge_id=?)",
                [challenge_id],
                |row| row.get::<_, bool>(0),
            )? {
                return Err(Error::Conflict);
            }
            self.conn.execute("INSERT INTO maintenance_checkpoints(epoch,agent_id,attempt_id,challenge_id,requested_at) VALUES(?,?,?,?,?)", params![epoch,run.as_str(),attempt,challenge_id,at])?;
            at
        };
        Ok(MaintenanceChallenge {
            epoch,
            execution: run.clone(),
            attempt_id: attempt.into(),
            challenge_id: challenge_id.into(),
            requested_at,
        })
    }

    /// Atomically bind one maintenance request to exactly one durable steer
    /// command. The command's attempt identity must be rechecked by the
    /// supervisor before any native send; a claimed or uncertain send is never
    /// repeated. Only the broker-owned maintenance controller issues it.
    pub fn request_maintenance_steer(
        &mut self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        challenge_id: &str,
        at: f64,
    ) -> Result<MaintenanceSteer> {
        let tx = WriteTx::begin(&self.conn)?;
        require_preparing(&tx, epoch)?;
        let challenge = self.ensure_maintenance_challenge(epoch, run, attempt, challenge_id, at)?;
        let recipient: Option<String> = tx.query_row(
            "SELECT orchestrator_session_id FROM maintenance_cohort WHERE epoch=? AND agent_id=?",
            params![epoch, run.as_str()],
            |row| row.get(0),
        )?;
        if let Some(session) = recipient {
            let accepted: bool = tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM maintenance_recipients r JOIN bridge_delivery_intents b ON b.delivery_id=r.before_delivery_id \
                 WHERE r.epoch=? AND r.orchestrator_session_id=? AND b.custody_state='custody_accepted' AND b.intent_json IS NOT NULL)",
                params![epoch, session],
                |row| row.get(0),
            )?;
            if !accepted {
                return Err(Error::Transition(
                    "maintenance pre-notice has not reached Bridge custody".into(),
                ));
            }
        }
        let session: Option<String> = tx.query_row(
            "SELECT runtime_session_id FROM agents WHERE id=?",
            [run.as_str()],
            |row| row.get(0),
        )?;
        if !session.as_deref().is_some_and(|session| {
            agent_run_domain::domain::external_id("runtime_session_id", session).is_ok()
        }) {
            return Err(invalid("maintenance native session is not available"));
        }
        let (command_id, accepted): (Option<i64>, bool) = tx.query_row(
            "SELECT command_id,checkpoint_json IS NOT NULL FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
            params![epoch, run.as_str(), attempt],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if let Some(command_id) = command_id {
            tx.commit()?;
            return Ok(MaintenanceSteer {
                challenge,
                command_id,
                duplicate: true,
            });
        }
        if accepted {
            return Err(invalid("maintenance checkpoint was already accepted"));
        }
        let text = maintenance_steer_text(challenge_id);
        let payload = serde_json::json!({"text":text,"maintenance_epoch":epoch,"maintenance_attempt":attempt,"maintenance_challenge":challenge_id});
        tx.execute(
            "INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'steer',?,'pending',?)",
            params![run.as_str(), payload.to_string(), at],
        )?;
        let command_id = tx.last_insert_rowid();
        let changed = tx.execute(
            "UPDATE maintenance_checkpoints SET command_id=? WHERE epoch=? AND agent_id=? AND attempt_id=? AND command_id IS NULL",
            params![command_id, epoch, run.as_str(), attempt],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(MaintenanceSteer {
            challenge,
            command_id,
            duplicate: false,
        })
    }

    /// Authenticate and persist a bounded checkpoint for the exact challenge.
    /// The trusted caller supplies a broker-computed launch-secret verdict.
    /// Identical accepted retries return their original digest even without the
    /// guard context; a first acceptance requires Clean. Task output_schema is
    /// intentionally irrelevant. Pending cancel or a real finish always refuses.
    #[expect(
        clippy::too_many_arguments,
        reason = "The exact epoch, attempt capability and trusted secret verdict are independent authorization inputs."
    )]
    pub fn accept_maintenance_checkpoint(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        token: &str,
        input: &MaintenancePauseRequest,
        at: f64,
        guard: FinishGuard,
    ) -> Result<MaintenancePauseReceipt> {
        input.validate()?;
        valid_time(at)?;
        let tx = WriteTx::begin(&self.conn)?;
        if crate::worker::authenticate_attempt(&tx, run, attempt, token, at)?.is_none() {
            return Err(invalid("invalid worker capability"));
        }
        if !self.maintenance_attempt_eligible(epoch, run, attempt)? {
            return Err(invalid(
                "worker attempt is not eligible for maintenance pause",
            ));
        }
        let prior: Option<(String,Option<String>,Option<String>)> = tx.query_row(
            "SELECT challenge_id,checkpoint_json,checkpoint_sha256 FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
            params![epoch,run.as_str(),attempt], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional()?;
        let Some((challenge, prior, prior_digest)) = prior else {
            return Err(invalid("no maintenance challenge was requested"));
        };
        if challenge != input.challenge_id {
            return Err(invalid("stale or foreign maintenance challenge"));
        }
        let payload = String::from_utf8(agent_run_domain::canonical::dumps(
            &serde_json::to_value(input)?,
            true,
        ))
        .map_err(|_| invalid("maintenance checkpoint serialization failed"))?;
        let digest = sha256(payload.as_bytes());
        if let Some(prior) = prior {
            if prior != payload || prior_digest.as_deref() != Some(&digest) {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(MaintenancePauseReceipt {
                sha256: digest,
                duplicate: true,
            });
        }
        if input.checkpoint.contains(token) || guard == FinishGuard::Secret {
            return Err(invalid(
                "maintenance checkpoint contains launch secret material; nothing was recorded",
            ));
        }
        if guard == FinishGuard::Unavailable {
            return Err(Error::Unsupported("maintenance launch-secret context is unavailable; retry the same checkpoint shortly".into()));
        }
        tx.execute("UPDATE maintenance_checkpoints SET checkpoint_json=?,checkpoint_sha256=?,accepted_at=? WHERE epoch=? AND attempt_id=? AND checkpoint_json IS NULL", params![payload,digest,at,epoch,attempt])?;
        tx.commit()?;
        Ok(MaintenancePauseReceipt {
            sha256: digest,
            duplicate: false,
        })
    }

    /// Resolve the exact challenge's epoch inside the same transaction as the
    /// authenticated acceptance. Worker-supplied input never selects an epoch.
    /// The nested acceptance scope is a savepoint in this one transaction.
    pub fn accept_maintenance_checkpoint_for_challenge(
        &self,
        run: &AgentId,
        attempt: &str,
        token: &str,
        input: &MaintenancePauseRequest,
        at: f64,
        guard: FinishGuard,
    ) -> Result<MaintenancePauseReceipt> {
        input.validate()?;
        valid_time(at)?;
        let tx = WriteTx::begin(&self.conn)?;
        if crate::worker::authenticate_attempt(&tx, run, attempt, token, at)?.is_none() {
            return Err(invalid("invalid worker capability"));
        }
        let epoch: Option<i64> = tx.query_row(
            "SELECT epoch FROM maintenance_checkpoints WHERE challenge_id=? AND agent_id=? AND attempt_id=?",
            params![input.challenge_id, run.as_str(), attempt],
            |row| row.get(0),
        ).optional()?;
        let epoch = epoch.ok_or_else(|| invalid("stale or foreign maintenance challenge"))?;
        let receipt =
            self.accept_maintenance_checkpoint(epoch, run, attempt, token, input, at, guard)?;
        tx.commit()?;
        Ok(receipt)
    }

    /// Mark native receipt observation for an exact challenge and accepted
    /// digest. The future caller must verify native tool identity and possess
    /// pinned supervisor authority; worker acceptance alone cannot call this.
    /// This records no cleanup proof and changes no execution/lifecycle state.
    pub fn observe_maintenance_checkpoint_receipt(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        challenge_id: &str,
        digest: &str,
    ) -> Result<bool> {
        if !valid_digest(challenge_id) || !valid_digest(digest) {
            return Err(invalid("invalid maintenance receipt"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        if !self.maintenance_attempt_eligible(epoch, run, attempt)? {
            return Err(invalid(
                "worker attempt is not eligible for maintenance pause",
            ));
        }
        let prior: Option<(String,Option<String>,bool)> = tx.query_row(
            "SELECT challenge_id,checkpoint_sha256,receipt_observed FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
            params![epoch,run.as_str(),attempt], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional()?;
        let Some((challenge, accepted, observed)) = prior else {
            return Err(invalid("no maintenance checkpoint was accepted"));
        };
        if challenge != challenge_id || accepted.as_deref() != Some(digest) {
            return Err(Error::Conflict);
        }
        if !observed {
            tx.execute("UPDATE maintenance_checkpoints SET receipt_observed=1 WHERE epoch=? AND attempt_id=?",params![epoch,attempt])?;
        }
        tx.commit()?;
        Ok(!observed)
    }

    /// Read only a still-eligible checkpoint for the exact epoch/attempt.
    /// Real finish, cancellation, released ownership or a closed fence hides it
    /// from pause/resume consumers without rewriting its immutable evidence.
    pub fn maintenance_checkpoint_intent(
        &self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        require_receipt: bool,
    ) -> Result<Option<MaintenancePauseRequest>> {
        // One read snapshot prevents eligibility and payload from describing
        // opposite sides of a concurrently committed cancel or terminal finish.
        let snapshot = if self.conn.is_autocommit() {
            Some(self.conn.unchecked_transaction()?)
        } else {
            None
        };
        if !self.maintenance_attempt_eligible(epoch, run, attempt)? {
            return Ok(None);
        }
        let stored: Option<(String,String)> = self.conn.query_row(
            "SELECT checkpoint_json,checkpoint_sha256 FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=? AND checkpoint_json IS NOT NULL AND (?=0 OR receipt_observed=1)",
            params![epoch,run.as_str(),attempt,require_receipt], |row| Ok((row.get(0)?,row.get(1)?)),
        ).optional()?;
        let result = stored
            .map(|(payload, digest)| {
                if sha256(payload.as_bytes()) != digest {
                    return Err(Error::Integrity(
                        "maintenance checkpoint digest mismatch".into(),
                    ));
                }
                let input: MaintenancePauseRequest = serde_json::from_str(&payload)?;
                input.validate()?;
                Ok(input)
            })
            .transpose()?;
        if let Some(snapshot) = snapshot {
            snapshot.commit()?;
        }
        Ok(result)
    }
}
