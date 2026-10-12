//! Managed service generation, lease, gate and probe units of the sole
//! writer (rows 63-73).
//!
//! The broker's service manager spawns, probes and cleans processes outside
//! any transaction and prepares definitions, identities and cleanup proofs;
//! these units apply only the guarded durable transitions. Every transition
//! is a compare-and-set on the generation's current state: a missing
//! generation is a durable `NotFound` and a generation in another state a
//! durable `Conflict`, never a storage fault.

use crate::Store;
use agent_run_domain::{Error, Result, error::invalid};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

/// Live states: the generation may still own processes and serve leases.
const LIVE: [&str; 3] = ["starting", "ready", "unhealthy"];

/// Validates a closed failure reason label.
fn reason(label: &str) -> Result<&str> {
    if label.is_empty()
        || label.len() > 64
        || !label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(invalid("service failure reason must be a closed label"));
    }
    Ok(label)
}

/// Validates a JSON text field.
fn json_text(text: &str, what: &str) -> Result<()> {
    if text.len() > 1_048_576 || serde_json::from_str::<serde_json::Value>(text).is_err() {
        return Err(invalid(format!("{what} must be bounded JSON")));
    }
    Ok(())
}

/// Validates a finite time.
fn finite(at: f64) -> Result<f64> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("service time must be finite and nonnegative"));
    }
    Ok(at)
}

/// The current state of `generation`; a missing generation is `NotFound`.
fn state(conn: &Connection, generation: &str) -> Result<String> {
    conn.query_row(
        "SELECT state FROM managed_service_generations WHERE id=?",
        [generation],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::NotFound(generation.to_owned()))
}

/// Requires `generation` to be in one of `allowed` states; another state is
/// a durable `Conflict`. Returns the current state.
fn expect_state(conn: &Connection, generation: &str, allowed: &[&str]) -> Result<String> {
    let current = state(conn, generation)?;
    if !allowed.contains(&current.as_str()) {
        return Err(Error::Conflict);
    }
    Ok(current)
}

impl Store<'_> {
    /// Releases leases of terminal executions that own no attempt and marks
    /// generations without live leases idle (row 63).
    pub fn release_finished_service_leases(&self, at: f64) -> Result<()> {
        let at = finite(at)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        tx.execute(
            "UPDATE managed_service_leases SET released_at=?1 WHERE released_at IS NULL \
             AND EXISTS(SELECT 1 FROM agents a WHERE a.id=managed_service_leases.agent_id AND a.status IN ('succeeded','failed','cancelled','lost','timed_out')) \
             AND NOT EXISTS(SELECT 1 FROM attempts t WHERE t.agent_id=managed_service_leases.agent_id AND t.ownership_active=1)",
            [at],
        )?;
        tx.execute(
            "UPDATE managed_service_generations SET idle_since=COALESCE((SELECT MAX(released_at) FROM managed_service_leases WHERE generation_id=managed_service_generations.id),?1) \
             WHERE state!='stopped' AND idle_since IS NULL \
             AND NOT EXISTS(SELECT 1 FROM managed_service_leases l WHERE l.generation_id=managed_service_generations.id AND l.released_at IS NULL)",
            [at],
        )?;
        tx.commit()
    }

    /// One step of a pending service gate (row 64): take a lease on one
    /// starting or ready generation (clearing its idle mark) while the gate
    /// is still pending and its execution live, or publish
    /// readiness when exactly the expected leases are held on ready
    /// generations. Returns whether the gate became ready.
    pub fn prepare_service_gate(&self, op: &crate::ops::PrepareServiceGate) -> Result<bool> {
        let at = finite(op.at)?;
        let id = op.execution.as_str();
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let ready = match &op.step {
            crate::ops::GateStep::Lease { generation } => {
                // Only a still-pending gate of a live execution leases: a
                // delayed step after the gate failed, became ready or the
                // execution ended is a durable conflict and writes nothing.
                let gated: bool = tx.query_row(
                    &format!(
                        "SELECT EXISTS(SELECT 1 FROM agent_service_gates g JOIN agents a ON a.id=g.agent_id \
                         WHERE g.agent_id=? AND g.state='pending' AND a.status IN {})",
                        crate::ACTIVE_SQL
                    ),
                    [id],
                    |row| row.get(0),
                )?;
                if !gated {
                    return Err(Error::Conflict);
                }
                expect_state(&tx, generation, &["starting", "ready"])?;
                tx.execute(
                    "INSERT INTO managed_service_leases(generation_id,agent_id,acquired_at) VALUES (?,?,?) ON CONFLICT DO NOTHING",
                    params![generation, id, at],
                )?;
                tx.execute(
                    "UPDATE managed_service_generations SET idle_since=NULL WHERE id=? AND idle_since IS NOT NULL",
                    [generation],
                )?;
                false
            }
            crate::ops::GateStep::Ready { expected_leases } => {
                tx.execute(
                    "UPDATE agent_service_gates SET state='ready' WHERE agent_id=?1 AND state='pending' \
                     AND (SELECT COUNT(*) FROM managed_service_leases WHERE agent_id=?1 AND released_at IS NULL)=?2 \
                     AND NOT EXISTS(SELECT 1 FROM managed_service_leases l JOIN managed_service_generations g ON g.id=l.generation_id \
                     WHERE l.agent_id=?1 AND l.released_at IS NULL AND g.state!='ready')",
                    params![id, i64::from(*expected_leases)],
                )? == 1
            }
        };
        tx.commit()?;
        Ok(ready)
    }

    /// Retires a live generation of a changed revision that no live lease
    /// holds, or creates a new starting generation when the service has no
    /// generation that is not stopped (row 65). Returns the created id.
    pub fn ensure_service_generation(
        &self,
        op: &crate::ops::EnsureServiceGeneration,
    ) -> Result<Option<String>> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let created = match &op.action {
            crate::ops::GenerationAction::Retire { generation } => {
                expect_state(&tx, generation, &LIVE)?;
                let held: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)",
                    [generation],
                    |row| row.get(0),
                )?;
                if held {
                    return Err(Error::Conflict);
                }
                tx.execute(
                    "UPDATE managed_service_generations SET state='stopping' WHERE id=? AND state IN ('starting','ready','unhealthy')",
                    [generation],
                )?;
                None
            }
            crate::ops::GenerationAction::Create {
                generation,
                service,
                revision,
                definition_json,
                broker_identity_json,
                ownership,
                exec_capability_sha256,
                at,
            } => {
                json_text(definition_json, "service definition")?;
                json_text(broker_identity_json, "broker identity")?;
                if !matches!(ownership.as_str(), "managed" | "external") {
                    return Err(invalid("service ownership must be managed or external"));
                }
                let taken: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM managed_service_generations WHERE id=?1 OR (service_id=?2 AND state!='stopped'))",
                    params![generation, service],
                    |row| row.get(0),
                )?;
                if taken {
                    return Err(Error::Conflict);
                }
                tx.execute(
                    "INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at,ownership,exec_capability_sha256) \
                     VALUES (?,?,?,?,'starting',?,?,?,?)",
                    params![
                        generation,
                        service,
                        revision,
                        definition_json,
                        broker_identity_json,
                        finite(*at)?,
                        ownership,
                        exec_capability_sha256
                    ],
                )?;
                Some(generation.clone())
            }
        };
        tx.commit()?;
        Ok(created)
    }

    /// Retires a starting generation whose bootstrap could not spawn and
    /// fails the pending gates of its lease holders (row 66).
    pub fn fail_service_spawn(&self, generation: &str) -> Result<()> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        expect_state(&tx, generation, &["starting"])?;
        tx.execute(
            "UPDATE managed_service_generations SET state='stopped',failure_kind='service_spawn_failed',\
             cleanup_json='{\"never_spawned\":true,\"confirmed\":true}' WHERE id=? AND state='starting'",
            [generation],
        )?;
        tx.execute(
            "UPDATE agent_service_gates SET state='failed',failure_kind='service_spawn_failed' WHERE state='pending' \
             AND agent_id IN (SELECT agent_id FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)",
            [generation],
        )?;
        tx.commit()
    }

    /// Fails one execution's pending service gate (row 67).
    pub fn fail_service_gate(&self, op: &crate::ops::FailServiceGate) -> Result<()> {
        self.conn.execute(
            "UPDATE agent_service_gates SET state='failed',failure_kind=? WHERE agent_id=? AND state='pending'",
            params![reason(&op.reason)?, op.execution.as_str()],
        )?;
        Ok(())
    }

    /// Marks a live generation unhealthy and fails the pending gates of its
    /// lease holders (row 68).
    pub fn mark_service_unhealthy(&self, op: &crate::ops::MarkServiceUnhealthy) -> Result<()> {
        let label = reason(&op.reason)?;
        let at = finite(op.at)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        expect_state(&tx, &op.generation, &LIVE)?;
        tx.execute(
            "UPDATE managed_service_generations SET state='unhealthy',failure_kind=?,checked_at=? \
             WHERE id=? AND state IN ('starting','ready','unhealthy')",
            params![label, at, op.generation],
        )?;
        tx.execute(
            "UPDATE agent_service_gates SET state='failed',failure_kind=? WHERE state='pending' \
             AND agent_id IN (SELECT agent_id FROM managed_service_leases WHERE generation_id=? AND released_at IS NULL)",
            params![label, op.generation],
        )?;
        tx.commit()
    }

    /// Records one prepared generation observation (row 69): a cleanup proof
    /// on any existing generation, external adoption of a starting one, or a
    /// health check of a live one.
    pub fn observe_service_generation(
        &self,
        op: &crate::ops::ObserveServiceGeneration,
    ) -> Result<()> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        match &op.observation {
            crate::ops::GenerationObservation::Cleanup { proof } => {
                if !proof.is_object() {
                    return Err(invalid("service cleanup proof must be an object"));
                }
                state(&tx, &op.generation)?;
                tx.execute(
                    "UPDATE managed_service_generations SET cleanup_json=? WHERE id=?",
                    params![proof.to_string(), op.generation],
                )?;
            }
            crate::ops::GenerationObservation::AdoptExternal => {
                expect_state(&tx, &op.generation, &["starting"])?;
                tx.execute(
                    "UPDATE managed_service_generations SET ownership='external' WHERE id=? AND state='starting'",
                    [&op.generation],
                )?;
            }
            crate::ops::GenerationObservation::Checked { at } => {
                expect_state(&tx, &op.generation, &LIVE)?;
                tx.execute(
                    "UPDATE managed_service_generations SET checked_at=? WHERE id=?",
                    params![finite(*at)?, op.generation],
                )?;
            }
        }
        tx.commit()
    }

    /// Marks a starting or ready generation ready (row 70).
    pub fn mark_service_ready(&self, op: &crate::ops::MarkServiceReady) -> Result<()> {
        let at = finite(op.at)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        expect_state(&tx, &op.generation, &["starting", "ready"])?;
        tx.execute(
            "UPDATE managed_service_generations SET state='ready',ready_at=COALESCE(ready_at,?1),checked_at=?1,failure_kind=NULL \
             WHERE id=?2 AND state IN ('starting','ready')",
            params![at, op.generation],
        )?;
        tx.commit()
    }

    /// Persists owned mode for a starting generation before its bootstrap
    /// runs (row 71).
    pub fn observe_external_service(&self, op: &crate::ops::ObserveExternalService) -> Result<()> {
        json_text(&op.broker_identity_json, "broker identity")?;
        if op.exec_capability_sha256.as_deref().is_some_and(|digest| {
            digest.len() != 64
                || !digest
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }) {
            return Err(invalid("invalid service executor capability hash"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        expect_state(&tx, &op.generation, &["starting"])?;
        tx.execute(
            "UPDATE managed_service_generations SET ownership='managed',broker_identity_json=?1,exec_capability_sha256=COALESCE(?2,exec_capability_sha256) WHERE id=?3 AND state='starting' AND process_identity_json IS NULL",
            params![op.broker_identity_json, op.exec_capability_sha256, op.generation],
        )?;
        tx.commit()
    }

    /// Stops a generation (row 72): without a proof a live (or already
    /// stopping) generation is marked stopping; with a proof a stopping or
    /// unknown generation is retired `stopped` only when the proof is
    /// confirmed and no probe remains (then its leases are released), else
    /// `unknown`. Returns the resulting state.
    pub fn stop_service_generation(
        &self,
        op: &crate::ops::StopServiceGeneration,
    ) -> Result<String> {
        let at = finite(op.at)?;
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let Some(proof) = &op.finalize else {
            expect_state(
                &tx,
                &op.generation,
                &["starting", "ready", "unhealthy", "stopping"],
            )?;
            tx.execute(
                "UPDATE managed_service_generations SET state='stopping' WHERE id=?",
                [&op.generation],
            )?;
            tx.commit()?;
            return Ok("stopping".into());
        };
        if !proof.is_object() {
            return Err(invalid("service cleanup proof must be an object"));
        }
        expect_state(&tx, &op.generation, &["stopping", "unknown"])?;
        let mut proof = proof.clone();
        let probes: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM managed_service_probes WHERE generation_id=?)",
            [&op.generation],
            |row| row.get(0),
        )?;
        if probes {
            proof["confirmed"] = json!(false);
        }
        proof["probes_gone"] = json!(!probes);
        let confirmed = proof["confirmed"] == true;
        let state = if confirmed { "stopped" } else { "unknown" };
        tx.execute(
            "UPDATE managed_service_generations SET state=?,cleanup_json=? WHERE id=? AND state IN ('stopping','unknown')",
            params![state, proof.to_string(), op.generation],
        )?;
        if confirmed {
            tx.execute(
                "UPDATE managed_service_leases SET released_at=? WHERE generation_id=? AND released_at IS NULL",
                params![at, op.generation],
            )?;
        }
        tx.commit()?;
        Ok(state.into())
    }

    /// Registers a readiness probe of a live generation with its ownership
    /// snapshot, or forgets a probe after its confirmed cleanup (row 73).
    pub fn record_service_probe(&mut self, op: &crate::ops::RecoverServiceProbes) -> Result<()> {
        match &op.action {
            crate::ops::ServiceProbeAction::Record {
                probe,
                generation,
                snapshot,
                at,
            } => {
                let at = finite(*at)?;
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
                expect_state(&tx, generation, &LIVE)?;
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM managed_service_probes WHERE id=?)",
                    [probe],
                    |row| row.get(0),
                )?;
                if exists {
                    return Err(Error::Conflict);
                }
                tx.execute(
                    "INSERT INTO managed_service_probes(id,generation_id,created_at) VALUES (?,?,?)",
                    params![probe, generation, at],
                )?;
                tx.commit()?;
                self.remember_processes("probe", probe, snapshot)?;
            }
            crate::ops::ServiceProbeAction::Forget { probe } => {
                let tx = crate::writer::WriteTx::begin(&self.conn)?;
                tx.execute(
                    "DELETE FROM process_members WHERE owner_kind='probe' AND owner_id=?",
                    [probe],
                )?;
                tx.execute(
                    "DELETE FROM process_ownership WHERE owner_kind='probe' AND owner_id=?",
                    [probe],
                )?;
                tx.execute("DELETE FROM managed_service_probes WHERE id=?", [probe])?;
                tx.commit()?;
            }
        }
        Ok(())
    }
}
