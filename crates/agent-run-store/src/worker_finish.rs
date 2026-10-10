//! Private attempt-bound completion intent and turn state, without a lifetime limit.

use crate::Store;
use agent_run_domain::{
    Error, Result,
    domain::AgentId,
    error::invalid,
    worker::{FinishReceipt, FinishRequest},
};
use agent_run_platform::fs::sha256;
use rusqlite::{OptionalExtension, TransactionBehavior, params};

impl Store {
    /// Queue one owned native completion through existing steering. The caller
    /// has already verified the native thread, turn, started item and redacted
    /// result. A 64-byte digest deduplicates retries within the current attempt;
    /// neither a late completion nor closing finish can admit another turn.
    /// Returns true for a durable new/replayed enqueue, false after closing.
    pub fn enqueue_native_wake(
        &mut self,
        run: &AgentId,
        digest: &str,
        text: &str,
        at: f64,
    ) -> Result<bool> {
        if digest.len() != 64
            || !digest
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            || text.is_empty()
            || text.len() > 8192
            || !at.is_finite()
            || at < 0.0
        {
            return Err(invalid("invalid native wake"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let attempt: Option<String> = tx.query_row("SELECT t.id FROM attempts t JOIN worker_lifecycle l ON l.attempt_id=t.id \
            JOIN agents a ON a.id=t.agent_id WHERE t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL \
            AND a.status='running' AND a.finished_at IS NULL AND l.finish_json IS NULL",[run.as_str()],|r|r.get(0)).optional()?;
        let Some(attempt) = attempt else {
            return Ok(false);
        };
        if tx.query_row("SELECT EXISTS(SELECT 1 FROM worker_native_wakes WHERE attempt_id=? AND receipt_sha256=?)",params![attempt,digest],|r|r.get::<_,bool>(0))? {
            tx.commit()?;return Ok(true);
        }
        tx.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'steer',?,'pending',?)",
            params![run.as_str(),serde_json::json!({"text":text,"native_completion":true}).to_string(),at])?;
        let command = tx.last_insert_rowid();
        tx.execute("INSERT INTO worker_native_wakes(attempt_id,receipt_sha256,command_id,created_at) VALUES(?,?,?,?)",params![attempt,digest,command,at])?;
        tx.commit()?;
        Ok(true)
    }
    /// Persist one authenticated callback atomically. The immutable admission
    /// must enable explicit completion and the exact attempt must remain owned.
    /// Identical retries return the original digest; changed payloads conflict.
    /// Cancellation wins if already pending. No outcome or answer is published
    /// until the supervisor observes the native receipt and verifies cleanup.
    pub fn accept_worker_finish(
        &mut self,
        run: &AgentId,
        attempt: &str,
        token: &str,
        input: &FinishRequest,
        at: f64,
    ) -> Result<FinishReceipt> {
        input.validate()?;
        if input.summary.contains(token) || !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid finish payload"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let auth = crate::worker::authenticate_attempt(&tx, run, attempt, token, at)?;
        if auth.is_none() {
            return Err(invalid("invalid worker capability"));
        }
        let admissible: bool = tx.query_row(
            "SELECT a.status='running' AND a.finished_at IS NULL \
             AND t.state='running' AND t.finished_at IS NULL \
             AND NOT EXISTS(SELECT 1 FROM commands c WHERE c.agent_id=a.id AND c.kind='cancel' AND c.state IN ('pending','claimed')) \
             FROM agents a JOIN attempts t ON t.agent_id=a.id \
             WHERE a.id=? AND t.id=?", params![run.as_str(), attempt], |r| r.get(0))?;
        if !admissible {
            return Err(invalid("worker attempt is not running"));
        }
        let request: String = tx.query_row(
            "SELECT request_json FROM agents WHERE id=?",
            [run.as_str()],
            |r| r.get(0),
        )?;
        let request =
            agent_run_domain::domain::StartRequest::from_history(serde_json::from_str(&request)?)?;
        if !request.explicit_finish {
            return Err(invalid("explicit finish was not admitted"));
        }
        input.validate_schema(request.output_schema.as_ref())?;
        let payload = String::from_utf8(agent_run_domain::canonical::dumps(
            &serde_json::to_value(input)?,
            true,
        ))
        .map_err(|_| invalid("finish serialization failed"))?;
        let digest = sha256(payload.as_bytes());
        let existing: Option<(Option<String>, Option<String>)> = tx
            .query_row(
                "SELECT finish_json,finish_sha256 FROM worker_lifecycle WHERE attempt_id=?",
                [attempt],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        let Some((prior, prior_digest)) = existing else {
            return Err(invalid("explicit finish was not admitted"));
        };
        if let Some(prior) = prior {
            if prior != payload || prior_digest.as_deref() != Some(&digest) {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(FinishReceipt {
                sha256: digest,
                duplicate: true,
            });
        }
        tx.execute("UPDATE worker_lifecycle SET idle_seconds=idle_seconds+CASE WHEN phase='idle' THEN MAX(0,?-transition_at) ELSE 0 END, \
            phase='closing',transition_at=?,finish_json=?,finish_sha256=?,accepted_at=? \
            WHERE attempt_id=? AND finish_json IS NULL",params![at,at,payload,digest,at,attempt])?;
        tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) VALUES(?,?,?,'worker_finish_accepted_v1',?)",
            params![run.as_str(),attempt,at,serde_json::json!({"sha256":digest,"bytes":input.summary.len(),"status":input.status}).to_string()])?;
        tx.commit()?;
        Ok(FinishReceipt {
            sha256: digest,
            duplicate: false,
        })
    }

    /// Enable lifecycle for a frozen explicit attempt only, after capability
    /// issuance and before task release. Repeated initialization preserves intent.
    pub fn begin_worker_lifecycle(&self, run: &AgentId, attempt: &str, at: f64) -> Result<()> {
        let row = self.get(run)?;
        if !row.request.explicit_finish {
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO worker_lifecycle(attempt_id,phase,transition_at) \
            SELECT id,'running',? FROM attempts WHERE id=? AND agent_id=? AND ownership_active=1 \
            ON CONFLICT(attempt_id) DO NOTHING",
            params![at, attempt, run.as_str()],
        )?;
        Ok(())
    }

    /// Return the exact current attempt's immutable intent. Never select an old
    /// attempt after failover/resume. Receipt observation must precede shutdown.
    pub fn worker_finish_intent(
        &self,
        run: &AgentId,
        require_receipt: bool,
    ) -> Result<Option<FinishRequest>> {
        let payload: Option<(String,String)> = self
            .conn
            .query_row(
                "SELECT l.finish_json,l.finish_sha256 FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id \
             WHERE t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND l.finish_json IS NOT NULL AND (?=0 OR l.receipt_observed=1)",
                params![run.as_str(), require_receipt],
                |r| Ok((r.get(0)?,r.get(1)?)),
            )
            .optional()?;
        payload
            .map(|(p, digest)| {
                if sha256(p.as_bytes()) != digest {
                    return Err(Error::Integrity("finish payload digest mismatch".into()));
                }
                let input: FinishRequest = serde_json::from_str(&p)?;
                input.validate()?;
                Ok(input)
            })
            .transpose()
    }

    /// Mark the authenticated receipt visible only after the native adapter has
    /// observed its finish tool result. This is not model approval.
    pub fn observe_worker_finish_receipt(&self, run: &AgentId) -> Result<()> {
        self.conn.execute("UPDATE worker_lifecycle SET receipt_observed=1 \
            WHERE finish_json IS NOT NULL AND attempt_id IN \
            (SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL)", [run.as_str()])?;
        Ok(())
    }

    /// Commit a turn boundary only on the current owned explicit attempt; never
    /// overwrite accepted finish. Repeated idle observations are no-ops. A wake
    /// increments the turn count once; phases and times never expire authority.
    pub fn worker_turn(&self, run: &AgentId, idle: bool, at: f64) -> Result<()> {
        let phase = if idle { "idle" } else { "running" };
        if !at.is_finite() || at < 0.0 {
            return Err(invalid("invalid turn observation"));
        }
        let tx = self.conn.unchecked_transaction()?;
        let changed=tx.execute("UPDATE worker_lifecycle SET idle_seconds=idle_seconds+CASE WHEN phase='idle' THEN MAX(0,?-transition_at) ELSE 0 END, \
            phase=?,transition_at=?,turn_count=turn_count+? \
            WHERE phase<>? AND finish_json IS NULL AND attempt_id IN \
            (SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1 AND finished_at IS NULL)",
            params![at,phase,at,if idle {0}else{1},phase,run.as_str()])?;
        if changed != 0 {
            tx.execute("INSERT INTO events(agent_id,attempt_id,at,kind,data_json) \
                SELECT ?,attempt_id,?,'worker_turn_phase_v1',? FROM worker_lifecycle l \
                JOIN attempts t ON t.id=l.attempt_id WHERE t.agent_id=? AND t.ownership_active=1 AND t.finished_at IS NULL",
                params![run.as_str(),at,serde_json::json!({"phase":phase}).to_string(),run.as_str()])?;
        }
        tx.commit()?;
        Ok(())
    }

    /// Read current turn phase/counter for public observations; legacy rows have
    /// no lifecycle and return None. Does not turn idle into a terminal status.
    pub fn worker_lifecycle_view(&self, run: &AgentId) -> Result<Option<serde_json::Value>> {
        self.worker_lifecycle_view_at(run, agent_run_domain::domain::now())
    }

    /// Read lifecycle at a finite nonnegative observation time. Terminal rows
    /// clamp accumulated idle time at finished_at, even after native exit without
    /// a callback. Observing later never extends a finished execution's metrics.
    pub fn worker_lifecycle_view_at(
        &self,
        run: &AgentId,
        observed_at: f64,
    ) -> Result<Option<serde_json::Value>> {
        if !observed_at.is_finite() || observed_at < 0.0 {
            return Err(invalid("invalid lifecycle observation"));
        }
        Ok(self.conn.query_row("SELECT l.phase,l.turn_count,l.transition_at,l.finish_sha256, \
            l.idle_seconds+CASE WHEN l.phase='idle' THEN MAX(0,COALESCE(a.finished_at,?)-l.transition_at) ELSE 0 END \
            FROM worker_lifecycle l JOIN attempts t ON t.id=l.attempt_id JOIN agents a ON a.id=t.agent_id \
            WHERE t.agent_id=? ORDER BY t.number DESC LIMIT 1",params![observed_at,run.as_str()],|r|Ok(serde_json::json!({
                "phase":r.get::<_,String>(0)?,"turn_count":r.get::<_,u64>(1)?,"transition_at":r.get::<_,f64>(2)?,"finish_sha256":r.get::<_,Option<String>>(3)?,"idle_seconds":r.get::<_,f64>(4)?
            }))).optional()?)
    }
}
