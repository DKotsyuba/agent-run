//! Supervisor bootstrap, ownership and service-executor identity units.
//!
//! Pure SQL units for the writer inventory that previously ran as raw SQL in
//! the supervisor, launcher and service executor, plus the launch capability
//! records. Each runs inside the caller's write scope ([`WriteTx`] nests) and
//! never touches the filesystem or processes.

use crate::{Store, writer::WriteTx};
use agent_run_domain::{Error, Result, domain::AgentId, error::invalid};
use rusqlite::params;
use serde_json::json;

/// Validates a 64-character lowercase hex digest.
fn hex64(label: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid(format!("{label} must be a lowercase SHA-256")));
    }
    Ok(())
}

impl Store<'_> {
    /// Records the fresh supervisor launch capability hash before spawn.
    /// Allowed only while the execution is starting or cancelling and no
    /// supervisor is recorded; repeating the same hash is a no-op and a
    /// different hash while one is set conflicts (use a handoff instead).
    pub fn record_supervisor_launch(
        &mut self,
        id: &AgentId,
        capability_sha256: &str,
    ) -> Result<()> {
        hex64("capability hash", capability_sha256)?;
        let tx = WriteTx::begin(&self.conn)?;
        let current: Option<(String, Option<i32>, Option<String>)> = tx
            .query_row(
                "SELECT status,supervisor_pid,supervisor_capability_sha256 FROM agents WHERE id=?",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map(Some)
            .or_else(|error| match error {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;
        let Some((status, supervisor, existing)) = current else {
            return Err(Error::NotFound(id.to_string()));
        };
        if !matches!(status.as_str(), "starting" | "cancelling") || supervisor.is_some() {
            return Err(Error::Transition(
                "supervisor launch requires an unowned starting agent".into(),
            ));
        }
        match existing {
            Some(existing) if existing == capability_sha256 => {}
            Some(_) => return Err(Error::Conflict),
            None => {
                tx.execute(
                    "UPDATE agents SET supervisor_capability_sha256=? WHERE id=?",
                    params![capability_sha256, id.as_str()],
                )?;
            }
        }
        tx.commit()
    }

    /// Records the spawned startup owner's inspected identity evidence while
    /// no supervisor is recorded (replaces the launcher's raw update).
    pub fn record_startup_owner(
        &mut self,
        id: &AgentId,
        identity: &serde_json::Value,
    ) -> Result<()> {
        if !identity.is_object() || identity["pid"].as_i64().is_none() {
            return Err(invalid("startup owner evidence must name a pid"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        tx.execute(
            "UPDATE agents SET startup_owner_pid_identity=? WHERE id=? AND supervisor_pid IS NULL",
            params![identity.to_string(), id.as_str()],
        )?;
        tx.commit()
    }

    /// Renews a startup handoff and, when renewed, installs the new launch
    /// capability hash so the previous launch's authority becomes stale.
    pub fn begin_supervisor_handoff_with(
        &mut self,
        op: &crate::ops::BeginSupervisorHandoff,
    ) -> Result<bool> {
        hex64("capability hash", &op.capability_sha256)?;
        let tx = WriteTx::begin(&self.conn)?;
        let renewed = self.begin_supervisor_handoff(
            &op.execution,
            &op.owner_identity,
            op.at,
            op.deadline_seconds,
        )?;
        if renewed {
            tx.execute(
                "UPDATE agents SET supervisor_capability_sha256=? WHERE id=?",
                params![op.capability_sha256, op.execution.as_str()],
            )?;
        }
        tx.commit()?;
        Ok(renewed)
    }

    /// Records the supervisor as owner of a starting or cancelling execution
    /// with its heartbeat and a `supervisor_ready` event (the short owner form
    /// formerly run by the supervisor). A recorded owner conflicts.
    pub fn record_owner(
        &mut self,
        id: &AgentId,
        pid: i32,
        token: &str,
        birth: Option<f64>,
        at: f64,
    ) -> Result<()> {
        if !at.is_finite() || at < 0.0 || pid <= 1 || token.is_empty() {
            return Err(invalid("invalid supervisor owner"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let changed = tx.execute(
            "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=?,heartbeat_at=? \
             WHERE id=? AND status IN ('starting','cancelling') AND supervisor_pid IS NULL",
            params![pid, token, birth, at, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.execute(
            "INSERT INTO events(agent_id,attempt_id,at,kind,data_json) \
             VALUES(?,(SELECT id FROM attempts WHERE agent_id=? AND ownership_active=1),?,?,?)",
            params![
                id.as_str(),
                id.as_str(),
                at,
                "supervisor_ready",
                json!({"pid": pid}).to_string()
            ],
        )?;
        tx.commit()
    }

    /// Sealing-only identity patch with revision compare-and-swap: sets
    /// `runtime_home`, `snapshot_sha256` and optionally
    /// `authority.assets_sha256` in the stored identity JSON and moves the
    /// config revision, only while starting and while the current revision is
    /// the expected one. Every other frozen identity byte stays unchanged; a
    /// stale revision or missing identity conflicts.
    pub fn seal_identity(&mut self, op: &crate::ops::UpdateIdentity) -> Result<()> {
        hex64("snapshot digest", &op.snapshot_sha256)?;
        if let Some(assets) = &op.assets_sha256 {
            hex64("assets digest", assets)?;
        }
        if op.runtime_home.is_empty()
            || op.new_revision.is_empty()
            || op.expected_revision.is_empty()
        {
            return Err(invalid("identity seal fields must be nonblank"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let changed = tx.execute(
            "UPDATE agents SET identity_json=CASE WHEN ?4 IS NULL \
               THEN json_set(identity_json,'$.runtime_home',?2,'$.snapshot_sha256',?3) \
               ELSE json_set(identity_json,'$.runtime_home',?2,'$.snapshot_sha256',?3,'$.authority.assets_sha256',?4) END, \
             config_revision=?5 WHERE id=?1 AND status='starting' AND config_revision=?6 AND identity_json IS NOT NULL",
            params![
                op.execution.as_str(),
                op.runtime_home,
                op.snapshot_sha256,
                op.assets_sha256,
                op.new_revision,
                op.expected_revision
            ],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()
    }

    /// Marks the legacy single attempt owned and spawning with its leader
    /// identity (the raw update formerly run by the legacy supervisor path).
    pub fn anchor_legacy_attempt(&mut self, op: &crate::ops::RetainCleanupEvidence) -> Result<()> {
        if op.leader_token.is_empty() || !op.leader_birth.is_finite() {
            return Err(invalid("invalid legacy leader identity"));
        }
        let tx = WriteTx::begin(&self.conn)?;
        let changed = tx.execute(
            "UPDATE attempts SET ownership_active=1,phase='spawning',process_identity=?,process_birth_time=? WHERE id=? AND agent_id=?",
            params![op.leader_token, op.leader_birth, op.attempt, op.execution.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()
    }

    /// Claims a starting service generation's process identity exactly once.
    /// Returns false when the generation is no longer starting or already
    /// claimed (the executor must then refuse to exec).
    pub fn claim_service_identity(
        &mut self,
        generation: &str,
        identity: &agent_run_platform::process::Identity,
    ) -> Result<bool> {
        let tx = WriteTx::begin(&self.conn)?;
        let claimed = tx.execute(
            "UPDATE managed_service_generations SET process_identity_json=? WHERE id=? AND state='starting' AND process_identity_json IS NULL",
            params![serde_json::to_string(identity)?, generation],
        )?;
        tx.commit()?;
        Ok(claimed == 1)
    }
}
