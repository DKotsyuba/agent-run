//! Distinguish a terminal engine result from a nonterminal maintenance pause.

use crate::{Error, Result, adapters::EngineResult};
use agent_run_domain::domain::AgentId;
use serde_json::json;
use std::time::Duration;

/// Immutable allocation identity passed to the native input handoff.
#[derive(Clone)]
pub struct ResumeBoundary {
    /// Open maintenance epoch and exact source and target attempts.
    pub epoch: i64,
    pub source_attempt_id: String,
    pub target_attempt_id: String,
}

/// Exact native input correlation retained only as bounded identifiers and a
/// digest. Neither the checkpoint nor its control text belongs in diagnostics.
#[derive(Clone, Copy)]
pub struct ResumeInputEvidence<'a> {
    /// Exact confirmed native session identifier.
    pub session: &'a str,
    /// Correlated native turn id or Claude user-input UUID.
    pub turn: &'a str,
    /// Lowercase SHA-256 of the exact transmitted native input text.
    pub input_sha256: &'a str,
}

impl std::fmt::Debug for ResumeInputEvidence<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResumeInputEvidence(<private correlation>)")
    }
}

/// Bound native identities and require canonical lowercase SHA-256 spelling.
fn valid_resume_input(input: &ResumeInputEvidence<'_>) -> bool {
    let id = |value: &str| {
        !value.is_empty() && value.len() <= 1024 && !value.chars().any(char::is_control)
    };
    id(input.session)
        && id(input.turn)
        && input.input_sha256.len() == 64
        && input
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// An allocated attempt remains nonterminal until its exact native handoff is
/// durably confirmed. Database read failure is not proof of confirmation.
pub fn resume_unconfirmed(conn: &rusqlite::Connection, id: &AgentId) -> Result<bool> {
    Ok(conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN agents a ON a.id=r.agent_id \
         LEFT JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=r.agent_id \
         WHERE r.agent_id=? AND a.status IN ('created','starting','running','cancelling') \
         AND (t.id IS NULL OR NOT EXISTS(SELECT 1 FROM events e WHERE e.agent_id=r.agent_id AND e.attempt_id=r.target_attempt_id \
           AND e.kind='maintenance_resume_confirmed_v1' AND json_extract(e.data_json,'$.epoch')=r.epoch \
           AND json_extract(e.data_json,'$.source_attempt_id')=r.source_attempt_id \
           AND json_extract(e.data_json,'$.target_attempt_id')=r.target_attempt_id \
           AND json_type(e.data_json,'$.session')='text' AND json_extract(e.data_json,'$.session')=a.runtime_session_id \
           AND json_type(e.data_json,'$.turn')='text' AND length(CAST(json_extract(e.data_json,'$.turn') AS BLOB)) BETWEEN 1 AND 1024 \
           AND json_type(e.data_json,'$.input_sha256')='text' AND length(json_extract(e.data_json,'$.input_sha256'))=64 \
           AND json_extract(e.data_json,'$.input_sha256') NOT GLOB '*[^0-9a-f]*')))",
        [id.as_str()], |row| row.get(0),
    )?)
}

/// Record one native continuation after the caller has verified the exact
/// resumed session and input acknowledgement: a correlated Codex turn/start
/// reply, or Claude's matching session and user replay. An uncertain writer
/// reply is resolved by observation, never a second input or a newly submitted
/// event. A cancellation can interrupt the wait for cleanup.
pub async fn confirm_resume(
    process: &mut crate::adapters::io::Process,
    store: &mut crate::persist::Persist,
    id: &AgentId,
    boundary: &ResumeBoundary,
    input: &ResumeInputEvidence<'_>,
) -> bool {
    if !valid_resume_input(input) || store.bound_attempt() != Some(boundary.target_attempt_id.as_str())
        || !matches!(store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM maintenance_resumes r JOIN maintenance_fences f ON f.epoch=r.epoch \
             JOIN attempts t ON t.id=r.target_attempt_id AND t.agent_id=r.agent_id JOIN agents a ON a.id=r.agent_id \
             WHERE r.epoch=? AND r.agent_id=? AND r.source_attempt_id=? AND r.target_attempt_id=? \
             AND f.ended_at IS NULL AND t.ownership_active=1 AND t.finished_at IS NULL \
             AND a.status='running' AND a.finished_at IS NULL AND a.runtime_session_id=?)",
            rusqlite::params![boundary.epoch,id.as_str(),boundary.source_attempt_id,boundary.target_attempt_id,input.session],
            |row| row.get::<_,bool>(0),
        ), Ok(true))
    {
        return false;
    }
    let data = json!({
        "epoch": boundary.epoch,
        "source_attempt_id": boundary.source_attempt_id,
        "target_attempt_id": boundary.target_attempt_id,
        "session": input.session,
        "turn": input.turn,
        "input_sha256": input.input_sha256,
    });
    if process
        .observing(store.event_durable_seq(id, "maintenance_resume_confirmed_v1", &data))
        .await
        .is_ok()
    {
        return true;
    }
    loop {
        let observed = store.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND attempt_id=? \
             AND kind='maintenance_resume_confirmed_v1' AND json_extract(data_json,'$.epoch')=? \
             AND json_extract(data_json,'$.source_attempt_id')=? AND json_extract(data_json,'$.target_attempt_id')=? \
             AND json_extract(data_json,'$.session')=? AND json_extract(data_json,'$.turn')=? AND json_extract(data_json,'$.input_sha256')=?)",
            rusqlite::params![id.as_str(),boundary.target_attempt_id,boundary.epoch,boundary.source_attempt_id,boundary.target_attempt_id,input.session,input.turn,input.input_sha256],
            |row| row.get::<_, bool>(0),
        );
        if matches!(observed, Ok(true)) {
            return true;
        }
        if matches!(store.cancel_pending(id), Ok(true)) {
            return false;
        }
        process
            .observing(tokio::time::sleep(Duration::from_secs(1)))
            .await;
    }
}

/// The result of running one native attempt; a pause is not a task outcome.
pub enum EngineExit {
    /// Native execution ended with its ordinary result.
    Finished(EngineResult),
    /// The exact pre-cleanup maintenance boundary was committed.
    MaintenancePause {
        /// Active maintenance epoch containing this attempt.
        epoch: i64,
        /// Exact attempt whose native execution became idle.
        attempt_id: String,
        /// Durable settlement event referenced by the closing boundary.
        native_event_seq: i64,
    },
    /// A continuation input may have been sent, but its durable confirmation
    /// did not complete. The supervisor must retain ownership and not retry.
    MaintenanceUnconfirmed,
}

impl std::fmt::Debug for EngineExit {
    /// Keep native result and maintenance identities out of diagnostics.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Finished(_) => f.write_str("EngineExit::Finished(..)"),
            Self::MaintenancePause { .. } => f.write_str("EngineExit::MaintenancePause(..)"),
            Self::MaintenanceUnconfirmed => f.write_str("EngineExit::MaintenanceUnconfirmed"),
        }
    }
}

impl EngineExit {
    /// Extract an ordinary result for callers which do not support pausing.
    /// A maintenance boundary is never converted into success or failure.
    pub fn require_finished(self) -> Result<EngineResult> {
        match self {
            Self::Finished(result) => Ok(result),
            Self::MaintenancePause { .. } => Err(Error::Runtime(
                "maintenance pause requires a maintenance-aware supervisor".into(),
            )),
            Self::MaintenanceUnconfirmed => Err(Error::Runtime(
                "maintenance continuation requires a maintenance-aware supervisor".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Legacy callers cannot silently publish a pause as a task result.
    #[test]
    fn maintenance_pause_is_not_a_terminal_result() {
        let exit = EngineExit::MaintenancePause {
            epoch: 1,
            attempt_id: "fixture".into(),
            native_event_seq: 2,
        };
        assert!(exit.require_finished().is_err());
    }

    /// Only bounded native correlation and a canonical digest can be recorded.
    #[test]
    fn resume_input_evidence_is_bounded_and_redacted() {
        let digest = "a".repeat(64);
        let valid = ResumeInputEvidence {
            session: "private-session",
            turn: "private-turn",
            input_sha256: &digest,
        };
        assert!(valid_resume_input(&valid));
        let debug = format!("{valid:?}");
        assert!(
            !debug.contains("private-session")
                && !debug.contains("private-turn")
                && !debug.contains(&digest)
        );
        for invalid in [
            ResumeInputEvidence {
                session: "",
                ..valid
            },
            ResumeInputEvidence {
                turn: "bad\nturn",
                ..valid
            },
            ResumeInputEvidence {
                input_sha256: "ABC",
                ..valid
            },
            ResumeInputEvidence {
                input_sha256: "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                ..valid
            },
        ] {
            assert!(!valid_resume_input(&invalid));
        }
    }
}
