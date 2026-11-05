//! Atomic pre-cleanup maintenance closing with exact native evidence.
//!
//! Committing this boundary does not stop a process, prove cleanup, release
//! ownership, park an attempt, or authorize a service restart.

use crate::{
    Store, maintenance_checkpoint::maintenance_steer_text, maintenance_park::NativeSettlement,
    writer::WriteTx,
};
use agent_run_config::role_plan::ResolvedRolePlan;
use agent_run_domain::{
    Error, Result,
    domain::AgentId,
    error::invalid,
    worker::{
        MAINTENANCE_PAUSE_CATALOG_VERSION, MaintenancePauseRequest,
        maintenance_pause_catalog_digest,
    },
};
use agent_run_platform::fs::sha256;
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

/// Native identities are bounded before they can contribute to evidence.
fn native_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
}

/// Validate the exact live checkpoint, command, role and native settlement in
/// the caller's write transaction. `allow_parked` is only for an exact replay
/// after a committed park; cancellation or a real finish always wins.
pub(crate) fn validate_native_settlement(
    store: &Store<'_>,
    epoch: i64,
    run: &AgentId,
    attempt: &str,
    event_seq: i64,
    at: f64,
    allow_parked: bool,
) -> Result<String> {
    if epoch <= 0 || event_seq <= 0 || !at.is_finite() || at < 0.0 || !native_id(attempt) {
        return Err(invalid("invalid maintenance closing request"));
    }
    let eligible:bool=store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM maintenance_cohort c JOIN maintenance_fences f ON f.epoch=c.epoch \
         JOIN agents a ON a.id=c.agent_id JOIN attempts t ON t.agent_id=a.id \
         JOIN worker_lifecycle l ON l.attempt_id=t.id \
         WHERE c.epoch=?1 AND a.id=?2 AND t.id=?3 AND f.ended_at IS NULL \
         AND a.sequence=c.sequence AND a.status='running' AND a.finished_at IS NULL \
         AND t.ownership_active=1 AND t.finished_at IS NULL AND (t.state='running' OR (?4 AND t.state='maintenance_parked' \
         AND EXISTS(SELECT 1 FROM maintenance_parks p WHERE p.epoch=c.epoch AND p.agent_id=a.id \
         AND p.attempt_id=t.id AND p.native_event_seq=?5))) \
         AND t.number=(SELECT MAX(number) FROM attempts WHERE agent_id=a.id) \
         AND (SELECT COUNT(*) FROM attempts WHERE agent_id=a.id AND ownership_active=1)=1 \
         AND l.finish_json IS NULL AND NOT EXISTS(SELECT 1 FROM commands q WHERE q.agent_id=a.id \
         AND q.kind='cancel' AND q.state IN ('pending','claimed')))",
        params![epoch,run.as_str(),attempt,allow_parked,event_seq],|row|row.get(0),
    )?;
    if !eligible {
        return Err(invalid(
            "maintenance attempt is no longer eligible to close",
        ));
    }
    let row = store.get(run)?;
    if !row.request.explicit_finish {
        return Err(invalid("maintenance requires explicit worker completion"));
    }
    let identity = row
        .identity
        .as_ref()
        .filter(|value| value["provider_identity_version"] == 2)
        .ok_or_else(|| invalid("maintenance launch authority is unavailable"))?;
    let role = identity
        .pointer("/authority/role_payload")
        .ok_or_else(|| invalid("maintenance worker role is unavailable"))?;
    let role = ResolvedRolePlan::from_payload(role)
        .map_err(|_| invalid("maintenance worker role is invalid"))?;
    if !role.worker_mcp {
        return Err(invalid("maintenance worker role is unavailable"));
    }
    let expected_catalog = maintenance_pause_catalog_digest(role.research_tools_only());
    let catalog:bool = store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM worker_capabilities WHERE attempt_id=? AND pool_catalog_version=? AND pool_catalog_digest=? AND pool_catalog_observed_at IS NOT NULL)",
        params![attempt,MAINTENANCE_PAUSE_CATALOG_VERSION,expected_catalog],|row|row.get(0),
    )?;
    if !catalog {
        return Err(invalid("maintenance worker catalog is unavailable"));
    }
    let checkpoint:Option<(String,String,String,f64,bool,Option<i64>)> = store.conn.query_row(
        "SELECT challenge_id,checkpoint_json,checkpoint_sha256,accepted_at,receipt_observed,command_id FROM maintenance_checkpoints \
         WHERE epoch=? AND agent_id=? AND attempt_id=? AND checkpoint_json IS NOT NULL",
        params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
    ).optional()?;
    let Some((challenge, payload, digest, accepted_at, observed, Some(command_id))) = checkpoint
    else {
        return Err(invalid("maintenance checkpoint or command is unavailable"));
    };
    if !observed
        || !accepted_at.is_finite()
        || accepted_at < 0.0
        || sha256(payload.as_bytes()) != digest
    {
        return Err(Error::Integrity(
            "maintenance checkpoint evidence is invalid".into(),
        ));
    }
    let input: MaintenancePauseRequest = serde_json::from_str(&payload)
        .map_err(|_| Error::Integrity("maintenance checkpoint evidence is invalid".into()))?;
    input.validate()?;
    if input.challenge_id != challenge {
        return Err(Error::Integrity(
            "maintenance checkpoint evidence is invalid".into(),
        ));
    }
    let command: Option<(String, String, String, String, Option<String>)> = store
        .conn
        .query_row(
            "SELECT agent_id,kind,payload_json,state,result_json FROM commands WHERE id=?",
            [command_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((command_agent, kind, command_payload, state, Some(command_result))) = command else {
        return Err(invalid("maintenance command is unavailable"));
    };
    let command_payload: Value = serde_json::from_str(&command_payload)
        .map_err(|_| invalid("invalid maintenance command evidence"))?;
    let command_result: Value = serde_json::from_str(&command_result)
        .map_err(|_| invalid("invalid maintenance command evidence"))?;
    let expected_payload = json!({"text":maintenance_steer_text(&challenge),"maintenance_epoch":epoch,"maintenance_attempt":attempt,"maintenance_challenge":challenge});
    let observation = match identity["authority"]["harness"].as_str() {
        Some("codex") => "native_accepted",
        Some("claude-code") => "stdin_written",
        _ => return Err(invalid("maintenance launch authority is unavailable")),
    };
    if command_agent != run.as_str()
        || kind != "steer"
        || state != "completed"
        || command_payload != expected_payload
        || command_result != json!({"accepted":true,"observation":observation})
    {
        return Err(invalid("maintenance command evidence is invalid"));
    }
    let pending:bool=store.conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=?1 AND id<>?2 AND state='claimed') \
         OR EXISTS(SELECT 1 FROM commands WHERE agent_id=?1 AND kind='steer' AND state IN ('pending','claimed') AND json_extract(payload_json,'$.native_completion')=1)",
        params![run.as_str(),command_id],|row|row.get(0),
    )?;
    if pending {
        return Err(invalid("maintenance input is not settled"));
    }
    type SettlementEvent = (
        String,
        String,
        Option<String>,
        f64,
        Option<String>,
        Option<String>,
        String,
    );
    let event:Option<SettlementEvent> = store.conn.query_row(
        "SELECT kind,agent_id,attempt_id,at,from_status,to_status,data_json FROM events WHERE seq=?",[event_seq],
        |row|Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?)),
    ).optional()?;
    let Some((kind, event_agent, event_attempt, event_at, from_status, to_status, event_json)) =
        event
    else {
        return Err(invalid("maintenance native evidence is unavailable"));
    };
    if kind != "maintenance_native_settled_v1"
        || event_agent != run.as_str()
        || event_attempt.as_deref() != Some(attempt)
        || !event_at.is_finite()
        || event_at < accepted_at
        || event_at > at
        || event_json.len() > 16 * 1024
        || from_status.is_some()
        || to_status.is_some()
    {
        return Err(invalid("maintenance native evidence is invalid"));
    }
    let evidence: NativeSettlement = serde_json::from_str(&event_json)
        .map_err(|_| invalid("maintenance native evidence is invalid"))?;
    let harness = match evidence.protocol.as_str() {
        "codex" => "codex",
        "claude"
            if uuid::Uuid::parse_str(&evidence.turn_id)
                .is_ok_and(|id| id.hyphenated().to_string() == evidence.turn_id) =>
        {
            "claude-code"
        }
        _ => return Err(invalid("maintenance native evidence is invalid")),
    };
    if evidence.version != 1
        || evidence.coverage != "owned_foreground_v1"
        || evidence.maintenance_epoch != epoch
        || evidence.agent_id != *run
        || evidence.attempt_id != attempt
        || evidence.challenge_id != challenge
        || evidence.receipt_sha256 != digest
        || !native_id(&evidence.thread_id)
        || !native_id(&evidence.turn_id)
        || !native_id(&evidence.item_id)
        || row.runtime_session_id.as_deref() != Some(evidence.thread_id.as_str())
        || identity["authority"]["harness"] != harness
    {
        return Err(invalid("maintenance native evidence is invalid"));
    }
    // Preserve the exact stored bytes as well as every event binding field.
    // A well-formed replacement under the same sequence is not a replay.
    let event_sha = agent_run_domain::canonical::sha256_hex(
        &json!({
            "seq":event_seq,"kind":kind,"agent_id":event_agent,"attempt_id":event_attempt,
            "at":event_at,"from_status":from_status,"to_status":to_status,"data_json":event_json,
        }),
        true,
    );
    let marker:(Option<i64>,Option<String>,Option<f64>)=store.conn.query_row(
        "SELECT closing_event_seq,closing_event_sha256,closing_at FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
        params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    )?;
    match marker {
        (None, None, None) => {}
        (Some(seq), Some(hash), Some(closing_at))
            if seq == event_seq
                && hash == event_sha
                && closing_at.is_finite()
                && closing_at >= event_at => {}
        (Some(_), Some(_), Some(_)) => {
            return Err(Error::Integrity(
                "maintenance closing evidence changed".into(),
            ));
        }
        _ => {
            return Err(Error::Integrity(
                "maintenance closing evidence is incomplete".into(),
            ));
        }
    }
    Ok(event_sha)
}

impl Store<'_> {
    /// Atomically claim the pre-cleanup boundary for one exact native event.
    /// Identical replay returns false; changed evidence conflicts. Cancellation
    /// and a finish already accepted before this transaction always refuse.
    pub fn begin_maintenance_closing(
        &mut self,
        epoch: i64,
        run: &AgentId,
        attempt: &str,
        native_event_seq: i64,
        at: f64,
    ) -> Result<bool> {
        if epoch <= 0 || native_event_seq <= 0 || !at.is_finite() || at < 0.0 || !native_id(attempt)
        {
            return Err(invalid("invalid maintenance closing request"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let prior:Option<(Option<i64>,Option<String>,Option<f64>)> = tx.query_row(
            "SELECT closing_event_seq,closing_event_sha256,closing_at FROM maintenance_checkpoints WHERE epoch=? AND agent_id=? AND attempt_id=?",
            params![epoch,run.as_str(),attempt],|row|Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
        ).optional()?;
        let Some((previous, closing_sha, closing_at)) = prior else {
            return Err(invalid("maintenance checkpoint is unavailable"));
        };
        if previous.is_some_and(|old| old != native_event_seq) {
            return Err(Error::Conflict);
        }
        if previous.is_some() != closing_at.is_some() || previous.is_some() != closing_sha.is_some()
        {
            return Err(Error::Integrity(
                "maintenance closing evidence is invalid".into(),
            ));
        }
        let event_sha = validate_native_settlement(
            self,
            epoch,
            run,
            attempt,
            native_event_seq,
            at,
            previous.is_some(),
        )?;
        if previous.is_some() {
            tx.commit()?;
            return Ok(false);
        }
        let changed=tx.execute(
            "UPDATE maintenance_checkpoints SET closing_event_seq=?,closing_event_sha256=?,closing_at=? WHERE epoch=? AND agent_id=? AND attempt_id=? AND closing_event_seq IS NULL",
            params![native_event_seq,event_sha,at,epoch,run.as_str(),attempt],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(true)
    }
}
