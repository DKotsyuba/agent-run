//! Atomic admission of a whole cooperative pool.

use crate::{
    Store,
    provider_admission::{AdmissionInputs, admit_in_tx},
};
use agent_run_domain::{
    Error, Result,
    catalog::ProviderCatalog,
    domain::{AgentId, now},
    error::invalid,
    pool::{AcceptanceCriterion, PoolId},
};
use rusqlite::{OptionalExtension, TransactionBehavior, params};

/// One member of a pool admission, with its pre-minted stable identity.
pub struct PoolMemberAdmission<'a> {
    /// Stable identity reserved before any row exists.
    pub id: AgentId,
    /// One-based slot.
    pub slot: u8,
    /// Member name, already unique among the pool's members.
    pub name: String,
    /// Descriptive role label.
    pub role: String,
    /// The member's own task before the pool preamble was composed in.
    pub personal_task: String,
    /// Trusted inputs whose request, effective task and identity already carry
    /// the composed task.
    pub source: PoolAdmissionSource<'a>,
}

/// Either new reservation inputs or an exact existing-worker preflight pin.
pub enum PoolAdmissionSource<'a> {
    /// Ordinary admission; only this branch reserves capacity and creates a run.
    New(AdmissionInputs<'a>),
    /// Existing independent RUNNING worker; all facts are atomically rechecked.
    Existing(crate::pool_enrollment::ExistingMember),
}

/// Everything one pool admission writes, decided outside the transaction.
pub struct PoolAdmissionInput<'a> {
    /// Pre-minted pool identity.
    pub pool_id: &'a PoolId,
    /// Replay scope: `global`, or a digest of the orchestrator binding.
    pub request_namespace: &'a str,
    /// The client's idempotency key.
    pub request_id: &'a str,
    /// Digest of the normalized outer client request, never of composed text.
    pub request_sha256: &'a str,
    /// The shared goal.
    pub goal: &'a str,
    /// The acceptance criteria.
    pub acceptance: &'a [AcceptanceCriterion],
    /// Resolved catalog shared by every member.
    pub catalog: &'a ProviderCatalog,
    /// Members in slot order.
    pub members: Vec<PoolMemberAdmission<'a>>,
    /// Effective shared binding, inferred from existing workers when outer input omitted it.
    pub orchestrator: Option<&'a agent_run_domain::domain::OrchestratorRef>,
}

/// A member as recorded, without any internal run or attempt detail.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolMemberRecord {
    /// Stable identity.
    pub agent_id: AgentId,
    /// One-based slot.
    pub slot: u8,
    /// Member name.
    pub name: String,
    /// Role label.
    pub role: String,
}

/// A committed pool, newly created or replayed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolAdmission {
    /// Pool identity.
    pub pool_id: PoolId,
    /// False for a replay of the same original request.
    pub created: bool,
    /// The original admission roster in slot order (revision one), stable
    /// across later replacements.
    pub members: Vec<PoolMemberRecord>,
}

/// Reads the original admission roster of `pool_id` in slot order: the
/// members that joined at roster revision one, retained even after later
/// replacements. A replay therefore always answers with the identities the
/// original request minted; the current roster is read from pool status.
fn members_of(conn: &rusqlite::Connection, pool_id: &str) -> Result<Vec<PoolMemberRecord>> {
    let mut statement = conn.prepare(
        "SELECT agent_id,slot,name,role FROM pool_members \
         WHERE pool_id=? AND joined_roster_revision=1 ORDER BY slot",
    )?;
    let rows = statement.query_map([pool_id], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get(1)?,
            row.get(2)?,
            row.get(3)?,
        ))
    })?;
    rows.map(|row| {
        let (id, slot, name, role) = row?;
        Ok(PoolMemberRecord {
            agent_id: id.parse()?,
            slot,
            name,
            role,
        })
    })
    .collect()
}

impl Store {
    /// Returns an existing pool for the exact original client request, `None`
    /// when the key is unused, or `Conflict` when another request used it.
    pub fn replay_pool(
        &self,
        namespace: &str,
        request_id: &str,
        request_sha256: &str,
    ) -> Result<Option<PoolAdmission>> {
        replay_pool(&self.conn, namespace, request_id, request_sha256)
    }

    /// Admits every member, the pool and its roster in one immediate
    /// transaction, or nothing.
    ///
    /// Replay of the same outer request returns the original pool and
    /// identities before any other check. A new pool runs the ordinary
    /// per-member admission once per member in slot order against the same
    /// transaction, so caps, account reservations and physical keys see the
    /// earlier members, and a failure of any member rolls back every row. The
    /// quota capacity revision advances once. Nothing is launched.
    pub fn admit_pool(&mut self, input: PoolAdmissionInput<'_>) -> Result<PoolAdmission> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(found) = replay_pool(
            &tx,
            input.request_namespace,
            input.request_id,
            input.request_sha256,
        )? {
            tx.commit()?;
            return Ok(found);
        }
        if input.members.is_empty() {
            return Err(invalid("pool members must share one orchestrator binding"));
        }
        let mut names = std::collections::BTreeSet::new();
        for member in &input.members {
            if !names.insert(member.name.to_lowercase()) {
                return Err(invalid("member names must be unique"));
            }
            match &member.source {
                PoolAdmissionSource::New(inputs) => {
                    if inputs.effective.orchestrator.as_ref() != input.orchestrator {
                        return Err(invalid("pool members must share one orchestrator binding"));
                    }
                    admit_in_tx(&tx, input.catalog, member.id.clone(), inputs, None)?;
                }
                PoolAdmissionSource::Existing(pin) => {
                    let actual = crate::pool_enrollment::existing_member(&tx, &member.id, now())?;
                    if actual.run_id != pin.run_id
                        || actual.attempt_id != pin.attempt_id
                        || actual.fingerprint != pin.fingerprint
                        || actual.deadline != pin.deadline
                        || !crate::pool_enrollment::same_binding(
                            actual.orchestrator.as_ref(),
                            pin.orchestrator.as_ref(),
                        )?
                    {
                        return Err(invalid("existing worker changed during pool admission"));
                    }
                    if actual.orchestrator.is_some()
                        && !crate::pool_enrollment::same_binding(
                            actual.orchestrator.as_ref(),
                            input.orchestrator,
                        )?
                    {
                        return Err(invalid(
                            "existing worker is bound to a different orchestrator",
                        ));
                    }
                }
            }
        }
        let session = input
            .orchestrator
            .map(|reference| crate::session_for_reference(&tx, reference, now()))
            .transpose()?;
        tx.execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,orchestrator_session_id,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES(?,?,?,?,?,?,?,'open',1,?)",
            params![
                input.pool_id.as_str(),
                input.request_namespace,
                input.request_id,
                input.request_sha256,
                session,
                input.goal,
                serde_json::to_string(input.acceptance)?,
                now()
            ],
        )?;
        for member in &input.members {
            tx.execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
                 VALUES(?,?,?,?,?,?,1)",
                params![
                    member.id.as_str(),
                    input.pool_id.as_str(),
                    member.slot,
                    member.name,
                    member.role,
                    member.personal_task
                ],
            )?;
        }
        for member in &input.members {
            if let PoolAdmissionSource::Existing(pin) = &member.source {
                if let Some(session) = session.as_ref() {
                    tx.execute("UPDATE agents SET orchestrator_session_id=? WHERE id=? AND orchestrator_session_id IS NULL",params![session,pin.run_id.as_str()])?;
                }
                let challenge = format!("pool-join-v1-{}", uuid::Uuid::new_v4().simple());
                tx.execute("INSERT INTO pool_enrollments(agent_id,run_id,attempt_id,challenge,deadline,created_at) VALUES(?,?,?,?,?,?)",
                    params![member.id.as_str(),pin.run_id.as_str(),pin.attempt_id,challenge,pin.deadline,now()])?;
                let goal_brief = input.goal.chars().take(96).collect::<String>();
                let roster = input
                    .members
                    .iter()
                    .map(|m| format!("{} ({}, {})", m.name, m.role, m.id))
                    .collect::<Vec<_>>()
                    .join("; ");
                let intro = format!(
                    "Pool {} enrollment pending. Seat: {} ({}). Goal brief: {}. Roster: {}. Your current work, permissions, session and original deadline remain unchanged. Read pool_read for the authoritative full goal and acceptance criteria; then acknowledge awareness with pool_post request_id={} and a brief current-work summary. Transport receipt is not acknowledgement.",
                    input.pool_id, member.name, member.role, goal_brief, roster, challenge
                );
                if intro.len() > 4096 {
                    return Err(invalid(
                        "pool enrollment introduction exceeds bounded message size",
                    ));
                }
                tx.execute("INSERT INTO commands(agent_id,kind,payload_json,state,created_at) VALUES(?,'steer',?,'pending',?)",
                    params![pin.run_id.as_str(),serde_json::to_string(&serde_json::json!({"text":intro,"pool_enrollment":member.id}))?,now()])?;
            }
        }
        if input
            .members
            .iter()
            .any(|m| matches!(m.source, PoolAdmissionSource::New(_)))
        {
            Store::advance_quota_capacity_revision(&tx)?;
        }
        let members = members_of(&tx, input.pool_id.as_str())?;
        tx.commit()?;
        Ok(PoolAdmission {
            pool_id: input.pool_id.clone(),
            created: true,
            members,
        })
    }
}

/// Shared replay lookup for the store handle and an open transaction.
fn replay_pool(
    conn: &rusqlite::Connection,
    namespace: &str,
    request_id: &str,
    request_sha256: &str,
) -> Result<Option<PoolAdmission>> {
    let found: Option<(String, String)> = conn
        .query_row(
            "SELECT id,request_sha256 FROM pools WHERE request_namespace=? AND request_id=?",
            params![namespace, request_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((id, stored)) = found else {
        return Ok(None);
    };
    if stored != request_sha256 {
        return Err(Error::Conflict);
    }
    Ok(Some(PoolAdmission {
        members: members_of(conn, &id)?,
        pool_id: id.parse()?,
        created: false,
    }))
}
