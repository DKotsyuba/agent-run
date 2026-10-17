//! Durable process membership shared by harness attempts and broker service generations.

use crate::Store;
use agent_run_domain::{
    Error, Result,
    domain::now,
    error::{PersistenceOperation, invalid},
};
use agent_run_platform::process::{Identity, OwnedProcess, OwnershipSnapshot};
use rusqlite::{OptionalExtension, params};

/// Validates attempt, service and temporary readiness-probe ownership; identifiers are SQL parameters.
fn validate_owner(kind: &str, id: &str) -> Result<()> {
    if !matches!(kind, "attempt" | "service" | "probe") || id.is_empty() || id.len() > 256 {
        return Err(invalid("invalid process ownership key"));
    }
    Ok(())
}

impl Store<'_> {
    /// Adds captured members atomically without changing an existing root or stealing another owner's PID/token.
    ///
    /// `kind` is attempt, service or probe, `id` names an existing durable owner, and
    /// The snapshot is a root-bearing delta; total history has no size cap.
    /// Repeated identities are no-ops. Remote operations retain their per-batch transport bound.
    /// Closed terminal attempts reject new writes so delayed producers cannot resurrect compacted details.
    /// Environment, command arguments and credentials are never accepted here.
    /// SQL refusals identify acquire, write or commit with elapsed milliseconds.
    /// Only a refused BEGIN is safe to replay; write/commit are never replayed by
    /// this method. Identity/validation errors retain their original typed contract.
    pub fn remember_processes(
        &mut self,
        kind: &str,
        id: &str,
        snapshot: &OwnershipSnapshot,
    ) -> Result<()> {
        validate_owner(kind, id)?;
        OwnedProcess::restore(snapshot.clone())?;
        let started = std::time::Instant::now();
        let tx = crate::writer::WriteTx::begin(&self.conn).map_err(|error| {
            Error::persistence(
                PersistenceOperation::OwnershipAcquire,
                started.elapsed(),
                error,
            )
        })?;
        // Statements remain one atomic mutation; any error drops/rolls back the
        // transaction before an async caller can observe it. Only acquire retries.
        let written = (|| -> Result<()> {
            let exists: bool = tx.query_row(
                match kind {
                    "attempt" => "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=?)",
                    "service" => {
                        "SELECT EXISTS(SELECT 1 FROM managed_service_generations WHERE id=?)"
                    }
                    _ => "SELECT EXISTS(SELECT 1 FROM managed_service_probes WHERE id=?)",
                },
                [id],
                |row| row.get(0),
            )?;
            if !exists {
                return Err(invalid("process owner does not exist"));
            }
            if kind == "attempt" && tx.query_row(
                "SELECT EXISTS(SELECT 1 FROM attempts t JOIN agents a ON a.id=t.agent_id WHERE t.id=?
                 AND t.ownership_active=0 AND a.status IN ('succeeded','failed','cancelled','lost','timed_out'))",
                [id], |row|row.get::<_,bool>(0))? {
                return Err(Error::Transition("terminal process ownership is closed".into()));
            }
            let leader = serde_json::to_string(&snapshot.leader)?;
            let previous: Option<String> = tx
                .query_row(
                    "SELECT leader_json FROM process_ownership WHERE owner_kind=? AND owner_id=?",
                    params![kind, id],
                    |row| row.get(0),
                )
                .optional()?;
            if previous
                .as_ref()
                .is_some_and(|previous| previous != &leader)
            {
                return Err(Error::Integrity(
                    "process owner cannot replace its root identity".into(),
                ));
            }
            let mut changed = tx.execute("INSERT INTO process_ownership(owner_kind,owner_id,leader_json,descendants_observed,updated_at) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(owner_kind,owner_id) DO NOTHING", params![kind,id,leader,snapshot.descendants_observed,now()])?;
            for member in &snapshot.members {
                changed += tx.execute("INSERT INTO process_members(owner_kind,owner_id,pid,token,identity_json) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(pid,token) DO NOTHING", params![kind,id,member.pid,member.token,serde_json::to_string(member)?])?;
                let owner: (String, String) = tx.query_row(
                    "SELECT owner_kind,owner_id FROM process_members WHERE pid=? AND token=?",
                    params![member.pid, member.token],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                if owner.0 != kind || owner.1 != id {
                    return Err(Error::Integrity(
                        "process identity has conflicting owners".into(),
                    ));
                }
            }
            tx.execute("UPDATE process_ownership SET descendants_observed=MAX(descendants_observed,?3),updated_at=?4 WHERE owner_kind=?1 AND owner_id=?2 AND (?5 OR descendants_observed < ?3)", params![kind,id,snapshot.descendants_observed,now(),changed > 0])?;
            Ok(())
        })();
        written.map_err(|error| match error {
            error @ Error::Sql(_) => Error::persistence(
                PersistenceOperation::OwnershipWrite,
                started.elapsed(),
                error,
            ),
            other => other,
        })?;
        tx.commit().map_err(|error| {
            Error::persistence(
                PersistenceOperation::OwnershipCommit,
                started.elapsed(),
                error,
            )
        })?;
        Ok(())
    }

    /// Restores only persisted evidence; historical owners without snapshots return None, never fabricated proof.
    /// Signals remain gated by native PID/token/birth observations in OwnedProcess.
    pub fn remembered_processes(&self, kind: &str, id: &str) -> Result<Option<OwnedProcess>> {
        validate_owner(kind, id)?;
        let root: Option<(String,bool)> = self.conn.query_row("SELECT leader_json,descendants_observed FROM process_ownership WHERE owner_kind=? AND owner_id=?", params![kind,id], |row| Ok((row.get(0)?,row.get(1)?))).optional()?;
        let Some((leader, observed)) = root else {
            return Ok(None);
        };
        let mut statement = self.conn.prepare("SELECT identity_json FROM process_members WHERE owner_kind=? AND owner_id=? ORDER BY pid,token")?;
        let members = statement
            .query_map(params![kind, id], |row| row.get::<_, String>(0))?
            .map(|value| -> Result<Identity> { Ok(serde_json::from_str(&value?)?) })
            .collect::<Result<Vec<_>>>()?;
        Ok(Some(OwnedProcess::restore(OwnershipSnapshot {
            leader: serde_json::from_str(&leader)?,
            members,
            descendants_observed: observed,
        })?))
    }
}

/// Deduplicates all acknowledged main/discovery identities of one execution before summarizing.
/// The agent is bound as ?1; old full discovery snapshots and new deltas count each PID generation once.
const PROCESS_OBSERVATIONS: &str = "WITH observations AS (
    SELECT pid,token,identity_json FROM process_members WHERE owner_kind='attempt'
      AND owner_id IN (SELECT id FROM attempts WHERE agent_id=?1)
    UNION ALL
    SELECT json_extract(j.value,'$.pid'),json_extract(j.value,'$.token'),j.value
      FROM events e,json_each(e.data_json,'$.members') j
      WHERE e.agent_id=?1 AND e.kind='mcp_discovery_ownership'
), processes AS (
    SELECT pid,token,MAX(json_extract(identity_json,'$.program')) AS program
      FROM observations GROUP BY pid,token
) ";

/// Replaces detailed process evidence with one bounded event after terminal, fully proven cleanup.
/// Runs inside the caller's transaction: summary insertion and deletions roll back together.
/// Active/uncertain attempts retain every identity. Repetition after compaction is a no-op.
/// Counts observed process starts, not exec transitions or unobserved short-lived children.
pub fn compact_terminal_processes(conn: &rusqlite::Connection, id: &str) -> Result<bool> {
    if conn.is_autocommit() {
        return Err(invalid("process compaction requires a transaction"));
    }
    let eligible: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agents a WHERE a.id=?1 AND a.status IN ('succeeded','failed','cancelled','lost','timed_out')
         AND NOT EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=a.id AND (t.ownership_active=1
         OR t.phase IS NOT 'cleanup_complete' OR (COALESCE(json_extract(t.cleanup_proof_json,'$.confirmed'),0) != 1 AND NOT (COALESCE(json_extract(t.cleanup_proof_json,'$.never_spawned'),0)=1 AND t.process_identity IS NULL AND NOT EXISTS(SELECT 1 FROM process_ownership x WHERE x.owner_kind='attempt' AND x.owner_id=t.id))))))",
        [id], |row| row.get(0))?;
    if !eligible {
        return Ok(false);
    }
    let total: i64 = conn.query_row(
        &format!("{PROCESS_OBSERVATIONS} SELECT COUNT(*) FROM processes"),
        [id],
        |row| row.get(0),
    )?;
    if total == 0 {
        return Ok(false);
    }
    let mut stmt = conn.prepare(&format!(
        "{PROCESS_OBSERVATIONS} SELECT program,COUNT(*) FROM processes
        WHERE program IS NOT NULL GROUP BY program ORDER BY COUNT(*) DESC,program LIMIT 16"
    ))?;
    let rows = stmt
        .query_map([id], |row| {
            Ok((row.get::<_, Option<String>>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut programs = Vec::new();
    let mut named = 0_i64;
    for (name, count) in rows {
        if let Some(name) = name {
            named += count;
            programs.push(serde_json::json!({"name":name,"count":count}));
        }
    }
    let unknown: i64 = conn.query_row(
        &format!("{PROCESS_OBSERVATIONS} SELECT COUNT(*) FROM processes WHERE program IS NULL"),
        [id],
        |row| row.get(0),
    )?;
    let (status, started_at, finished_at, failure_kind): (
        String,
        Option<f64>,
        Option<f64>,
        Option<String>,
    ) = conn.query_row(
        "SELECT status,started_at,finished_at,failure_kind FROM agents WHERE id=?",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    )?;
    let summary = serde_json::json!({"status":status,"started_at":started_at,"finished_at":finished_at,"failure_kind":failure_kind,"version":1,"observed_processes":total,"programs":programs,
        "unknown_program":unknown,"other_programs":total-named-unknown,"cleanup_confirmed":true});
    conn.execute(
        "INSERT INTO events(agent_id,at,kind,data_json,attempt_id) VALUES(?1,?2,'process_summary_v1',?3,(SELECT id FROM attempts WHERE agent_id=?1 ORDER BY number DESC LIMIT 1))",
        params![id, now(), summary.to_string()],
    )?;
    conn.execute("DELETE FROM process_members WHERE owner_kind='attempt' AND owner_id IN (SELECT id FROM attempts WHERE agent_id=?)",[id])?;
    conn.execute("DELETE FROM process_ownership WHERE owner_kind='attempt' AND owner_id IN (SELECT id FROM attempts WHERE agent_id=?)",[id])?;
    conn.execute(
        "DELETE FROM events WHERE agent_id=? AND kind='mcp_discovery_ownership'",
        [id],
    )?;
    Ok(true)
}
