//! Atomic admission of a whole cooperative pool.

use crate::{
    provider_admission::{admit_in_tx, AdmissionInputs},
    Store,
};
use agent_run_domain::{
    catalog::ProviderCatalog,
    domain::{now, AgentId},
    error::invalid,
    pool::{AcceptanceCriterion, PoolId},
    Error, Result,
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

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
    pub inputs: AdmissionInputs<'a>,
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
        let binding = input
            .members
            .first()
            .map(|m| &m.inputs.effective.orchestrator);
        if input.members.is_empty()
            || input
                .members
                .iter()
                .any(|m| Some(&m.inputs.effective.orchestrator) != binding)
        {
            return Err(invalid("pool members must share one orchestrator binding"));
        }
        let mut first_agent = None;
        for member in &input.members {
            admit_in_tx(&tx, input.catalog, member.id.clone(), &member.inputs, None)?;
            first_agent.get_or_insert(member.id.as_str());
        }
        let session: Option<String> = tx.query_row(
            "SELECT orchestrator_session_id FROM agents WHERE id=?",
            [first_agent],
            |row| row.get(0),
        )?;
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
        Store::advance_quota_capacity_revision(&tx)?;
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
