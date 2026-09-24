//! Durable process membership shared by harness attempts and broker service generations.

use crate::Store;
use agent_run_domain::{domain::now, error::invalid, Error, Result};
use agent_run_platform::process::{Identity, OwnedProcess, OwnershipSnapshot};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

/// Validates attempt, service and temporary readiness-probe ownership; identifiers are SQL parameters.
fn validate_owner(kind: &str, id: &str) -> Result<()> {
    if !matches!(kind, "attempt" | "service" | "probe") || id.is_empty() || id.len() > 256 {
        return Err(invalid("invalid process ownership key"));
    }
    Ok(())
}

impl Store {
    /// Adds captured members atomically without changing an existing root or stealing another owner's PID/token.
    ///
    /// `kind` is attempt, service or probe, `id` names an existing durable owner, and
    /// `snapshot` comes from its OwnedProcess. Repeated observations are no-ops.
    /// Environment, command arguments and credentials are never accepted here.
    pub fn remember_processes(
        &mut self,
        kind: &str,
        id: &str,
        snapshot: &OwnershipSnapshot,
    ) -> Result<()> {
        validate_owner(kind, id)?;
        OwnedProcess::restore(snapshot.clone())?;
        let bytes = serde_json::to_vec(snapshot)?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err(invalid("process ownership snapshot exceeds two MiB"));
        }
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = tx.query_row(
            match kind {
                "attempt" => "SELECT EXISTS(SELECT 1 FROM attempts WHERE id=?)",
                "service" => "SELECT EXISTS(SELECT 1 FROM managed_service_generations WHERE id=?)",
                _ => "SELECT EXISTS(SELECT 1 FROM managed_service_probes WHERE id=?)",
            },
            [id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(invalid("process owner does not exist"));
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
        tx.commit()?;
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
