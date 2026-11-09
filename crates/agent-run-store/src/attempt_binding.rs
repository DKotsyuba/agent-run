//! Writer-side attempt admissions and permanent abort/retirement fences.
//! Peer provenance and Bridge/Pool authenticity are verified by the caller;
//! arbitrary request-supplied observations or outcomes are never authority.
use crate::{Store, writer::WriteTx};
use agent_run_domain::{
    Error, Result, StoreId,
    attempt_binding::{
        self as wire, Actor, Admission, AdmissionRequest, AttemptRef, ConsumeReceipt,
        CurrentReceipt, EgressCurrent, Incarnation, IngressCurrent, LineageStep, Observation,
        RoleQualification, Route, SuccessorProof,
    },
    domain::AgentId,
    error::invalid,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

/// Hard refusal before creating another durable v1 protocol key. Compaction
/// requires a separately specified permanent namespace-retirement scheme.
const MAX_RECORDS: i64 = 100_000;
const ACTIVE_CURRENT_SQL: &str = "SELECT count(*) FROM attempt_pending_current p LEFT JOIN attempt_current_receipts r ON r.pool_route_json=p.pool_route_json AND r.request_id=p.request_id JOIN attempt_launches l ON l.launch_id=COALESCE(p.launch_id,json_extract(r.receipt_json,'$.attempt.launch_id')) JOIN attempts t ON t.id=l.attempt_id JOIN agents a ON a.id=t.agent_id WHERE t.ownership_active=1 AND a.status IN ('starting','running')";

/// Content-free headroom, not a grant or a guarantee of future availability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptCapacityHeadroom {
    pub active_current_used: u64,
    pub active_current_limit: u64,
    pub admission_keys_used: u64,
    pub admission_keys_limit: u64,
    pub retirements_used: u64,
    pub retirements_limit: u64,
    pub qualifications_used: u64,
    pub qualifications_limit: u64,
    pub owner_effects_used: u64,
    pub owner_effects_limit: u64,
}

/// The frozen launch candidate or an immutable exact-key replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressBegin {
    pub candidate: Option<EgressCandidate>,
    pub receipt: Option<CurrentReceipt>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EgressCandidate {
    pub attempt: AttemptRef,
    pub incarnation: Incarnation,
}

fn capacity(conn: &Connection, table: &'static str) -> Result<()> {
    let query = match table {
        "admissions" => {
            "SELECT count(*) FROM (SELECT intent_id FROM attempt_pending_checks UNION SELECT intent_id FROM attempt_admissions)"
        }
        "receipts" => ACTIVE_CURRENT_SQL,
        "retirements" => "SELECT count(*) FROM attempt_namespace_retirements",
        "qualifications" => "SELECT count(*) FROM attempt_role_qualifications",
        _ => return Err(invalid("unknown attempt capacity")),
    };
    if conn.query_row(query, [], |r| r.get::<_, i64>(0))? >= MAX_RECORDS {
        return Err(invalid("attempt capacity exhausted"));
    }
    Ok(())
}

/// Permanent admission lookup; `None` is only a snapshot, never a fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdmissionRecord {
    pub state: String,
    pub admission: Option<Admission>,
    pub outcome_digest: Option<String>,
}

/// Evidence fetched by the core from the pinned authenticated Bridge reader.
/// The caller must verify the returned store, route, validity and aborted flag.
pub struct CurrentConsume<'a> {
    pub receipt: &'a ConsumeReceipt,
    /// Fresh direct kernel inspection of the bound leader, made after lookup.
    pub inspected: &'a Incarnation,
}

/// Authenticated terminal Pool outcome already fetched outside the writer.
pub struct TerminalOutcome<'a> {
    pub pool_route: &'a Route,
    pub intent_id: &'a str,
    pub security_digest: &'a str,
    pub state: &'a str,
    pub outcome_digest: &'a str,
}

/// Frozen namespace and both permanent operation keys.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRequest {
    pub expected_store_id: StoreId,
    pub request_id: String,
    pub pool_route: Route,
    pub bridge_route: Route,
    pub producer: String,
    pub issuer_epoch: String,
    pub retirement_key: String,
    pub pool_seal_request_id: String,
}
impl NamespaceRequest {
    pub fn validate(&self) -> Result<()> {
        self.pool_route.validate()?;
        self.bridge_route.validate()?;
        wire::validate_uuid(&self.bridge_route.component_store_id)?;
        for value in [
            &self.request_id,
            &self.producer,
            &self.issuer_epoch,
            &self.retirement_key,
            &self.pool_seal_request_id,
        ] {
            wire::validate_id(value)?;
        }
        if self.retirement_key == self.pool_seal_request_id
            || self.pool_route.worker_uid != self.bridge_route.worker_uid
        {
            return Err(invalid("invalid namespace request"));
        }
        Ok(())
    }
}

/// Authenticated Pool namespace seal, fetched directly on the pinned route.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSeal {
    pub version: u32,
    pub store_id: String,
    pub request_id: String,
    pub seal_id: String,
    pub bridge_route: Route,
    pub producer: String,
    pub issuer_epoch: String,
    pub retirement_key: String,
    pub state: String,
}
impl PoolSeal {
    /// Checks every authenticated response field against its frozen namespace.
    pub fn matches_request(&self, request: &NamespaceRequest) -> bool {
        self.version == 1
            && self.store_id == request.pool_route.component_store_id
            && self.request_id == request.pool_seal_request_id
            && wire::validate_id(&self.seal_id).is_ok()
            && self.bridge_route == request.bridge_route
            && self.producer == request.producer
            && self.issuer_epoch == request.issuer_epoch
            && self.retirement_key == request.retirement_key
            && matches!(self.state.as_str(), "draining" | "sealed")
    }
}

/// Durable retirement phase, with a final Bridge count when known.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceRecord {
    pub state: String,
    pub pool_seal_id: Option<String>,
    pub retired_count: Option<u64>,
}

/// Fresh consensus classification relative to a previously verified ballot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsensusCurrent {
    pub state: String,
    pub last_attempt: Option<AttemptRef>,
    pub last_incarnation: Option<Incarnation>,
    pub successor_proof: Option<SuccessorProof>,
}
fn unavailable() -> ConsensusCurrent {
    ConsensusCurrent {
        state: "unavailable".into(),
        last_attempt: None,
        last_incarnation: None,
        successor_proof: None,
    }
}

type AttemptRow = (String, u32, String, String, u64, u32, u32, u64, i64);
type NamespaceRow = (
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    String,
    Option<String>,
    Option<u64>,
);
type AdmissionRow = (String, String, String, Option<String>, Option<String>);
type CurrentRow = (
    String,
    i64,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<i64>,
    Option<String>,
    Option<String>,
    String,
);
type FrozenRoleRow = (
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);
type BoundRoleRow = (String, String, String, i64, i64, i64, i64, String);
type PriorRoleRow = (String, String, String, String, String, i64, i64, i64, i64);
fn bound_attempt(
    conn: &Connection,
    execution: &AgentId,
    selected: Option<&str>,
) -> Result<Option<AttemptRow>> {
    Ok(conn.query_row(
        "SELECT t.id,t.number,l.launch_id,l.boot_id,l.pid_namespace_inode,l.uid,l.pid,l.start_ticks,t.ownership_active \
         FROM attempts t JOIN attempt_launches l ON l.attempt_id=t.id AND l.state='bound' \
         WHERE t.agent_id=? AND (?2 IS NULL OR t.id=?2) ORDER BY t.number DESC LIMIT 1",
        params![execution.as_str(),selected], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?)),
    ).optional()?)
}

fn namespace_record(
    conn: &Connection,
    request: &NamespaceRequest,
) -> Result<Option<NamespaceRecord>> {
    let row: Option<NamespaceRow> = conn.query_row(
        "SELECT request_id,pool_seal_request_id,pool_route_json,bridge_route_json,producer,issuer_epoch,retirement_key,state,pool_seal_id,retired_count FROM attempt_namespace_retirements WHERE retirement_key=? OR request_id=? OR pool_seal_request_id=? OR (bridge_route_json=? AND producer=? AND issuer_epoch=?) LIMIT 1",
        params![request.retirement_key,request.request_id,request.pool_seal_request_id,route_json(&request.bridge_route)?,request.producer,request.issuer_epoch],
        |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?)),
    ).optional()?;
    let Some((
        request_id,
        seal_request,
        pool,
        bridge,
        producer,
        issuer,
        key,
        state,
        pool_seal_id,
        retired_count,
    )) = row
    else {
        return Ok(None);
    };
    if request_id != request.request_id
        || seal_request != request.pool_seal_request_id
        || pool != route_json(&request.pool_route)?
        || bridge != route_json(&request.bridge_route)?
        || producer != request.producer
        || issuer != request.issuer_epoch
        || key != request.retirement_key
    {
        return Err(Error::Conflict);
    }
    Ok(Some(NamespaceRecord {
        state,
        pool_seal_id,
        retired_count,
    }))
}

fn route_json(route: &Route) -> Result<String> {
    route.validate()?;
    Ok(serde_json::to_string(route)?)
}

fn read_record(
    conn: &Connection,
    intent: &str,
    digest: &str,
    route: Option<&Route>,
) -> Result<Option<AdmissionRecord>> {
    wire::validate_id(intent)?;
    wire::validate_digest(digest)?;
    let row: Option<AdmissionRow> = conn.query_row(
        "SELECT security_digest,pool_route_json,state,admission_json,outcome_digest FROM attempt_admissions WHERE intent_id=?",
        [intent], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)),
    ).optional()?;
    let Some((stored_digest, stored_route, state, raw, outcome_digest)) = row else {
        return Ok(None);
    };
    if stored_digest != digest
        || route
            .map(route_json)
            .transpose()?
            .is_some_and(|expected| expected != stored_route)
    {
        return Err(Error::Conflict);
    }
    let admission = raw.map(|value| serde_json::from_str(&value)).transpose()?;
    Ok(Some(AdmissionRecord {
        state,
        admission,
        outcome_digest,
    }))
}

/// Proves root-to-tail continuity and returns the requested suffix. Gaps,
/// branches, rollback, unknown rows and incomplete predecessor state refuse.
fn lineage(conn: &Connection, root: &AgentId, bound: &AgentId) -> Result<SuccessorProof> {
    let mut stmt = conn.prepare("SELECT id,parent_agent_id,root_agent_id,sequence,status FROM agents WHERE root_agent_id=?1 OR id=?1 ORDER BY sequence,id LIMIT 20002")?;
    let mut rows = stmt.query([root.as_str()])?;
    let mut chain = Vec::new();
    let mut previous_status: Option<String> = None;
    while let Some(row) = rows.next()? {
        if chain.len() >= 20_000 {
            return Err(invalid("attempt lineage unavailable"));
        }
        let raw: String = row.get(0)?;
        let parent: Option<String> = row.get(1)?;
        let stored_root: String = row.get(2)?;
        let sequence: u32 = row.get(3)?;
        let status: String = row.get(4)?;
        let execution_id: AgentId = raw.parse()?;
        if stored_root != root.as_str()
            || sequence != chain.len() as u32 + 1
            || (chain.is_empty() && (execution_id != *root || parent.is_some()))
            || (!chain.is_empty()
                && (parent.as_deref()
                    != chain
                        .last()
                        .map(|step: &LineageStep| step.execution_id.as_str())
                    || !previous_status.as_deref().is_some_and(|s| {
                        matches!(
                            s,
                            "succeeded" | "failed" | "cancelled" | "timed_out" | "lost"
                        )
                    })))
        {
            return Err(invalid("attempt lineage unavailable"));
        }
        let parent_execution_id = parent.map(|value| value.parse()).transpose()?;
        chain.push(LineageStep {
            execution_id,
            parent_execution_id,
            sequence,
        });
        previous_status = Some(status);
    }
    let Some(tip) = chain.last() else {
        return Err(invalid("attempt lineage unavailable"));
    };
    let current_execution_id = tip.execution_id.clone();
    let start = chain
        .iter()
        .position(|step| step.execution_id == *bound)
        .ok_or_else(|| invalid("attempt lineage unavailable"))?;
    Ok(SuccessorProof {
        lineage_root_id: root.clone(),
        bound_execution_id: bound.clone(),
        current_execution_id,
        chain: chain[start..].to_vec(),
    })
}

/// Proves that the supplied immutable binding is still the last owned attempt.
/// `inspected` is captured immediately before submission and re-matched under the
/// writer transaction. It is an observation, not a promise of commit-time life.
fn current_exact(
    conn: &Connection,
    expected: &AttemptRef,
    actor: &Actor,
    worker_uid: u32,
    bound: &AgentId,
    inspected: &Incarnation,
) -> Result<SuccessorProof> {
    inspected.validate()?;
    let proof = lineage(conn, &expected.lineage_root_id, bound)?;
    if proof.current_execution_id != expected.execution_id {
        return Err(invalid("binding stale"));
    }
    let row: Option<CurrentRow> = conn.query_row(
        "SELECT t.id,t.number,l.launch_id,l.state,l.boot_id,l.pid_namespace_inode,l.uid,l.pid,l.start_ticks,t.process_identity,a.runtime_session_id,a.status \
         FROM attempts t JOIN agents a ON a.id=t.agent_id LEFT JOIN attempt_launches l ON l.attempt_id=t.id \
         WHERE t.agent_id=? ORDER BY t.number DESC LIMIT 1",
        [expected.execution_id.as_str()], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?,r.get(9)?,r.get(10)?,r.get(11)?))
    ).optional()?;
    let Some((
        attempt,
        number,
        launch,
        state,
        boot,
        namespace,
        uid,
        pid,
        ticks,
        token,
        session,
        status,
    )) = row
    else {
        return Err(invalid("attempt not ready"));
    };
    if attempt != expected.attempt_id
        || launch.as_deref().is_some_and(|id| id != expected.launch_id)
        || expected.sequence != proof.chain.last().map_or(0, |s| s.sequence)
    {
        return Err(invalid("attempt mismatch"));
    }
    if state.as_deref() != Some("bound")
        || boot.is_none()
        || namespace.is_none()
        || uid.is_none()
        || pid.is_none()
        || ticks.is_none()
        || token.is_none()
    {
        return Err(invalid("attempt not ready"));
    }
    if session.is_none() {
        return Err(invalid("attempt not ready"));
    }
    if session.as_deref() != Some(actor.native_id.as_str()) {
        return Err(invalid("actor mismatch"));
    }
    if Some(inspected.boot_id.as_str()) != boot.as_deref()
        || i64::try_from(inspected.pid_namespace_inode).ok() != namespace
        || Some(i64::from(inspected.uid)) != uid
        || Some(i64::from(inspected.pid)) != pid
        || i64::try_from(inspected.start_ticks).ok() != ticks
        || token.as_deref()
            != Some(format!("linux:{}:{}", inspected.boot_id, inspected.start_ticks).as_str())
        || inspected.uid != worker_uid
    {
        return Err(invalid("attempt mismatch"));
    }
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM attempts WHERE agent_id=?",
        [expected.execution_id.as_str()],
        |r| r.get(0),
    )?;
    let active: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM attempts t JOIN attempt_launches l ON l.attempt_id=t.id WHERE t.id=? AND t.agent_id=? AND t.ownership_active=1 AND l.launch_id=? AND l.state='bound')", params![expected.attempt_id,expected.execution_id.as_str(),expected.launch_id], |r| r.get(0))?;
    let qualified: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM attempt_role_qualifications WHERE launch_id=? AND attempt_id=? AND boot_id=? AND pid_namespace_inode=? AND uid=? AND pid=? AND start_ticks=?)",params![expected.launch_id,expected.attempt_id,inspected.boot_id,inspected.pid_namespace_inode,inspected.uid,inspected.pid,inspected.start_ticks],|r|r.get(0))?;
    if !qualified {
        return Err(invalid("attempt not ready"));
    }
    let phase: Option<String> = conn
        .query_row(
            "SELECT phase FROM worker_lifecycle WHERE attempt_id=?",
            [&expected.attempt_id],
            |r| r.get(0),
        )
        .optional()?;
    let cancelled:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='cancel' AND state IN ('pending','claimed'))",[expected.execution_id.as_str()],|r|r.get(0))?;
    if cancelled
        || phase.as_deref() == Some("closing")
        || !matches!(status.as_str(), "starting" | "running")
    {
        return Err(invalid("binding stale"));
    }
    if !matches!(phase.as_deref(), Some("running" | "idle")) {
        return Err(invalid("attempt not ready"));
    }
    if !active {
        return Err(invalid("binding stale"));
    }
    if count != number || state.as_deref() != Some("bound") {
        return Err(invalid("attempt mismatch"));
    }
    Ok(proof)
}

impl Store<'_> {
    /// Report only bounded counts; no request IDs, actor or process data.
    pub fn attempt_capacity_headroom(&self) -> Result<AttemptCapacityHeadroom> {
        let current: i64 = self.conn.query_row(ACTIVE_CURRENT_SQL, [], |r| r.get(0))?;
        let admission:i64=self.conn.query_row("SELECT count(*) FROM (SELECT intent_id FROM attempt_pending_checks UNION SELECT intent_id FROM attempt_admissions)",[],|r|r.get(0))?;
        let (retirements,qualifications,effects):(i64,i64,i64)=self.conn.query_row("SELECT (SELECT count(*) FROM attempt_namespace_retirements),(SELECT count(*) FROM attempt_role_qualifications),(SELECT count(*) FROM owner_effects)",[],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)))?;
        Ok(AttemptCapacityHeadroom {
            active_current_used: current.max(0) as u64,
            active_current_limit: MAX_RECORDS as u64,
            admission_keys_used: admission.max(0) as u64,
            admission_keys_limit: MAX_RECORDS as u64,
            retirements_used: retirements.max(0) as u64,
            retirements_limit: MAX_RECORDS as u64,
            qualifications_used: qualifications.max(0) as u64,
            qualifications_limit: MAX_RECORDS as u64,
            owner_effects_used: effects.max(0) as u64,
            owner_effects_limit: crate::owner_v2::MAX_EFFECTS as u64,
        })
    }
    /// Freeze the authenticated Bridge launch; only the request digest persists.
    pub fn attempt_begin_current(&self, request: &IngressCurrent, launch_id: &str) -> Result<()> {
        self.attempt_begin_current_gated(request, launch_id, true)
    }
    /// Writer-bound switch fence: pending-only work is not authority when disabled.
    pub fn attempt_begin_current_gated(
        &self,
        request: &IngressCurrent,
        launch_id: &str,
        enabled: bool,
    ) -> Result<()> {
        request.validate()?;
        wire::validate_uuid(launch_id)?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let pool = route_json(&request.pool_route)?;
        let bridge = route_json(&request.bridge_route)?;
        let fingerprint = wire::current_fingerprint(request)?;
        let old:Option<(String,String,String,Option<String>)> = tx.query_row("SELECT fingerprint,bridge_route_json,producer,launch_id FROM attempt_pending_current WHERE pool_route_json=? AND request_id=?",params![pool,request.request_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?))).optional()?;
        if let Some((prior, old_bridge, producer, original_launch)) = old {
            if prior != fingerprint
                || old_bridge != bridge
                || producer != request.producer
                || original_launch.as_deref() != Some(launch_id)
            {
                return Err(Error::Conflict);
            }
            if !enabled {
                let completed:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_current_receipts WHERE pool_route_json=? AND request_id=? AND fingerprint=?)",params![pool,request.request_id,fingerprint],|r|r.get(0))?;
                if !completed {
                    return Err(Error::Unsupported(
                        "managed attempt binding disabled".into(),
                    ));
                }
            }
            tx.commit()?;
            return Ok(());
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let known:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_launches l JOIN attempts t ON t.id=l.attempt_id AND t.agent_id=l.execution_id JOIN agents a ON a.id=t.agent_id WHERE l.launch_id=? AND t.ownership_active=1 AND a.status IN ('starting','running') AND l.state IN ('prepared','spawning','bound'))",[launch_id],|r|r.get(0))?;
        if !known {
            return Err(invalid("attempt not ready"));
        }
        capacity(&tx, "receipts")?;
        tx.execute("INSERT INTO attempt_pending_current(request_id,pool_route_json,bridge_route_json,producer,fingerprint,state,launch_id) VALUES(?,?,?,?,?,'pending',?)",params![request.request_id,pool,bridge,request.producer,fingerprint,launch_id])?;
        tx.commit()
    }

    /// Freeze the original launch before asking Bridge for the root actor.
    pub fn attempt_begin_egress(
        &self,
        request: &EgressCurrent,
        enabled: bool,
    ) -> Result<EgressBegin> {
        request.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        let store = crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let pool = route_json(&request.pool_route)?;
        let bridge = route_json(&request.bridge_route)?;
        let fingerprint = wire::egress_current_fingerprint(request)?;
        let receipt: Option<(String,String)> = tx.query_row("SELECT fingerprint,receipt_json FROM attempt_current_receipts WHERE pool_route_json=? AND request_id=?",params![pool,request.request_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((prior, raw)) = receipt {
            if prior != fingerprint {
                return Err(Error::Conflict);
            }
            let receipt = serde_json::from_str(&raw)?;
            tx.commit()?;
            return Ok(EgressBegin {
                candidate: None,
                receipt: Some(receipt),
            });
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let pending: Option<(String,String,String,Option<String>,String)> = tx.query_row("SELECT fingerprint,bridge_route_json,producer,launch_id,state FROM attempt_pending_current WHERE pool_route_json=? AND request_id=?",params![pool,request.request_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        if pending
            .as_ref()
            .is_some_and(|(prior, old_bridge, producer, launch, state)| {
                prior != &fingerprint
                    || old_bridge != &bridge
                    || producer != &request.producer
                    || launch.is_none()
                    || state != "pending"
            })
        {
            return Err(Error::Conflict);
        }
        let proof = lineage(&tx, &request.lineage_root_id, &request.bound_execution_id)?;
        let execution = proof.current_execution_id.clone();
        let selected = pending
            .as_ref()
            .and_then(|(_, _, _, launch, _)| launch.as_deref());
        let row =
            bound_attempt(&tx, &execution, None)?.ok_or_else(|| invalid("attempt not ready"))?;
        let (attempt_id, _, launch_id, boot_id, pid_namespace_inode, uid, pid, start_ticks, _) =
            row;
        if selected.is_some_and(|id| id != launch_id) {
            return Err(invalid("binding stale"));
        }
        let candidate = EgressCandidate {
            attempt: AttemptRef {
                run_store_id: store,
                lineage_root_id: request.lineage_root_id.clone(),
                execution_id: execution.clone(),
                sequence: proof.chain.last().map_or(0, |step| step.sequence),
                attempt_id,
                launch_id,
            },
            incarnation: Incarnation {
                host_id: request.host_id.clone(),
                boot_id,
                pid_namespace_inode,
                uid,
                pid,
                start_ticks,
            },
        };
        candidate.incarnation.validate()?;
        let session: Option<String> = tx.query_row(
            "SELECT runtime_session_id FROM agents WHERE id=?",
            [execution.as_str()],
            |r| r.get(0),
        )?;
        let actor = Actor {
            address: "candidate".into(),
            role: "root".into(),
            native_id: session.ok_or_else(|| invalid("attempt not ready"))?,
            epoch: 1,
        };
        current_exact(
            &tx,
            &candidate.attempt,
            &actor,
            request.run_route.worker_uid,
            &request.bound_execution_id,
            &candidate.incarnation,
        )?;
        if pending.is_none() {
            capacity(&tx, "receipts")?;
            tx.execute("INSERT INTO attempt_pending_current(request_id,pool_route_json,bridge_route_json,producer,fingerprint,state,launch_id) VALUES(?,?,?,?,?,'pending',?)",params![request.request_id,pool,bridge,request.producer,fingerprint,candidate.attempt.launch_id])?;
        }
        tx.commit()?;
        Ok(EgressBegin {
            candidate: Some(candidate),
            receipt: None,
        })
    }

    /// Commit only the actor returned by authenticated Bridge, after the same
    /// writer rechecks the frozen launch, role, lifecycle and current lineage.
    pub fn attempt_finish_egress(
        &self,
        request: &EgressCurrent,
        candidate: &EgressCandidate,
        actor: &Actor,
        inspected: &Incarnation,
        enabled: bool,
    ) -> Result<CurrentReceipt> {
        request.validate()?;
        actor.validate()?;
        inspected.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let fingerprint = wire::egress_current_fingerprint(request)?;
        let pool = route_json(&request.pool_route)?;
        let prior: Option<(String,String)> = tx.query_row("SELECT fingerprint,receipt_json FROM attempt_current_receipts WHERE pool_route_json=? AND request_id=?",params![pool,request.request_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((old, raw)) = prior {
            if old != fingerprint {
                return Err(Error::Conflict);
            }
            let receipt = serde_json::from_str(&raw)?;
            tx.commit()?;
            return Ok(receipt);
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let pending: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_pending_current WHERE pool_route_json=? AND request_id=? AND fingerprint=? AND launch_id=? AND state='pending')",params![pool,request.request_id,fingerprint,candidate.attempt.launch_id],|r|r.get(0))?;
        if !pending {
            return Err(Error::Conflict);
        }
        if &candidate.incarnation != inspected
            || candidate.attempt.run_store_id != request.expected_store_id
            || candidate.attempt.lineage_root_id != request.lineage_root_id
            || inspected.host_id != request.host_id
        {
            return Err(invalid("attempt mismatch"));
        }
        let proof = current_exact(
            &tx,
            &candidate.attempt,
            actor,
            request.run_route.worker_uid,
            &request.bound_execution_id,
            inspected,
        )?;
        let receipt = CurrentReceipt {
            receipt_id: uuid::Uuid::new_v4().to_string(),
            purpose: "egress".into(),
            lineage_root_id: request.lineage_root_id.clone(),
            bound_execution_id: request.bound_execution_id.clone(),
            attempt: candidate.attempt.clone(),
            incarnation: inspected.clone(),
            actor: actor.clone(),
            successor_proof: proof,
        };
        tx.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",params![request.request_id,pool,fingerprint,receipt.receipt_id,serde_json::to_string(&receipt)?])?;
        if tx.execute("UPDATE attempt_pending_current SET state='resolved' WHERE pool_route_json=? AND request_id=? AND state='pending'",params![pool,request.request_id])? != 1 { return Err(Error::Conflict); }
        tx.commit()?;
        Ok(receipt)
    }

    /// Bind supervisor-verified managed role and native inventory/config to the
    /// exact durable launch and leader; historical attempts cannot be upgraded.
    pub fn attempt_qualify_role(&self, execution: &AgentId, q: &RoleQualification) -> Result<()> {
        q.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        let frozen: Option<FrozenRoleRow> = tx.query_row(
            "SELECT json_extract(identity_json,'$.provider_config.managed_attempt_binding.enabled'),json_extract(identity_json,'$.provider_config.managed_attempt_binding.role_name'),json_extract(identity_json,'$.authority.profile'),json_extract(identity_json,'$.authority.harness'),json_extract(identity_json,'$.authority.role_payload.config_revision'),json_extract(identity_json,'$.provider_config.managed_attempt_binding.host_id') FROM agents WHERE id=? AND json_extract(identity_json,'$.provider_identity_version')=2",
            [execution.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        if !frozen.is_some_and(|(enabled, configured, profile, harness, revision, host)| {
            enabled == Some(1)
                && configured.is_some()
                && configured == profile
                && harness.as_deref() == Some(q.harness.as_str())
                && revision.as_deref() == Some(q.role_digest.as_str())
                && host.as_deref() == Some(q.incarnation.host_id.as_str())
        }) {
            return Err(Error::Conflict);
        }
        let row:Option<BoundRoleRow> = tx.query_row(
            "SELECT l.launch_id,l.state,l.boot_id,l.pid_namespace_inode,l.uid,l.pid,l.start_ticks,t.process_identity FROM attempt_launches l JOIN attempts t ON t.id=l.attempt_id WHERE l.attempt_id=? AND t.agent_id=? AND t.ownership_active=1",
            params![q.attempt_id,execution.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?))
        ).optional()?;
        let Some((launch, state, boot, namespace, uid, pid, ticks, token)) = row else {
            return Err(Error::Conflict);
        };
        if launch != q.launch_id
            || state != "bound"
            || boot != q.incarnation.boot_id
            || Some(namespace) != i64::try_from(q.incarnation.pid_namespace_inode).ok()
            || uid != i64::from(q.incarnation.uid)
            || pid != i64::from(q.incarnation.pid)
            || Some(ticks) != i64::try_from(q.incarnation.start_ticks).ok()
            || token
                != format!(
                    "linux:{}:{}",
                    q.incarnation.boot_id, q.incarnation.start_ticks
                )
        {
            return Err(Error::Conflict);
        }
        let prior:Option<PriorRoleRow>=tx.query_row("SELECT attempt_id,harness,role_digest,config_digest,inventory_digest,pid_namespace_inode,uid,pid,start_ticks FROM attempt_role_qualifications WHERE launch_id=?",[&q.launch_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
        if let Some((attempt, harness, role, config, inventory, ns, old_uid, old_pid, old_ticks)) =
            prior
        {
            if attempt != q.attempt_id
                || harness != q.harness
                || role != q.role_digest
                || config != q.config_digest
                || inventory != q.inventory_digest
                || ns != namespace
                || old_uid != uid
                || old_pid != pid
                || old_ticks != ticks
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(());
        }
        capacity(&tx, "qualifications")?;
        tx.execute("INSERT INTO attempt_role_qualifications(launch_id,attempt_id,harness,role_digest,config_digest,inventory_digest,boot_id,pid_namespace_inode,uid,pid,start_ticks) VALUES(?,?,?,?,?,?,?,?,?,?,?)",params![q.launch_id,q.attempt_id,q.harness,q.role_digest,q.config_digest,q.inventory_digest,q.incarnation.boot_id,q.incarnation.pid_namespace_inode,q.incarnation.uid,q.incarnation.pid,q.incarnation.start_ticks])?;
        tx.commit()
    }
    /// Observe an exact retirement key without performing remote I/O.
    pub fn attempt_retirement_lookup(
        &self,
        request: &NamespaceRequest,
    ) -> Result<Option<NamespaceRecord>> {
        request.validate()?;
        crate::store_identity::require(&self.conn, Some(&request.expected_store_id))?;
        namespace_record(&self.conn, request)
    }
    /// Read exact request fingerprint and original receipt before a retry
    /// performs any Bridge I/O. This is not a fresh currentness observation.
    pub fn attempt_current_by_request(
        &self,
        store: &StoreId,
        pool_route: &Route,
        request_id: &str,
    ) -> Result<Option<(String, CurrentReceipt)>> {
        wire::validate_id(request_id)?;
        crate::store_identity::require(&self.conn, Some(store))?;
        let row:Option<(String,String)> = self.conn.query_row("SELECT fingerprint,receipt_json FROM attempt_current_receipts WHERE pool_route_json=? AND request_id=?",params![route_json(pool_route)?,request_id],|r|Ok((r.get(0)?,r.get(1)?))).optional()?;
        row.map(|(fingerprint, raw)| Ok((fingerprint, serde_json::from_str(&raw)?)))
            .transpose()
    }

    /// Classify a previously verified ballot against one database snapshot and
    /// a separately captured fresh kernel observation. Unknown is never death.
    #[allow(clippy::too_many_arguments)]
    pub fn attempt_consensus_current(
        &self,
        store: &StoreId,
        root: &AgentId,
        bound: &AgentId,
        expected: &AttemptRef,
        expected_incarnation: &Incarnation,
        inspected: Option<&Incarnation>,
        host_id: &str,
    ) -> Result<ConsensusCurrent> {
        crate::store_identity::require(&self.conn, Some(store))?;
        if expected.validate().is_err()
            || expected_incarnation.validate().is_err()
            || wire::validate_id(host_id).is_err()
            || expected.run_store_id != *store
            || expected.lineage_root_id != *root
            || expected.execution_id != *bound
            || expected_incarnation.host_id != host_id
        {
            return Ok(unavailable());
        }
        let tx = self.conn.unchecked_transaction()?;
        let proof = match lineage(&tx, root, bound) {
            Ok(proof) => proof,
            Err(_) => return Ok(unavailable()),
        };
        let Some(original) = bound_attempt(&tx, bound, Some(&expected.attempt_id))? else {
            return Ok(unavailable());
        };
        let (old_id, old_number, old_launch, old_boot, old_ns, old_uid, old_pid, old_ticks, _) =
            original;
        if old_id != expected.attempt_id
            || old_launch != expected.launch_id
            || old_boot != expected_incarnation.boot_id
            || old_ns != expected_incarnation.pid_namespace_inode
            || old_uid != expected_incarnation.uid
            || old_pid != expected_incarnation.pid
            || old_ticks != expected_incarnation.start_ticks
            || expected.sequence != proof.chain.first().map_or(0, |step| step.sequence)
        {
            return Ok(unavailable());
        }
        let qualified:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_role_qualifications WHERE launch_id=? AND attempt_id=? AND boot_id=? AND pid_namespace_inode=? AND uid=? AND pid=? AND start_ticks=?)",params![expected.launch_id,expected.attempt_id,expected_incarnation.boot_id,expected_incarnation.pid_namespace_inode,expected_incarnation.uid,expected_incarnation.pid,expected_incarnation.start_ticks],|r|r.get(0))?;
        if !qualified {
            return Ok(unavailable());
        }
        let tip = &proof.current_execution_id;
        let Some((id, number, launch, boot, namespace, uid, pid, ticks, active)) =
            bound_attempt(&tx, tip, None)?
        else {
            return Ok(unavailable());
        };
        let total: i64 = tx.query_row(
            "SELECT count(*) FROM attempts WHERE agent_id=?",
            [tip.as_str()],
            |r| r.get(0),
        )?;
        if i64::from(number) != total {
            return Ok(unavailable());
        }
        let status: String = tx.query_row(
            "SELECT status FROM agents WHERE id=?",
            [tip.as_str()],
            |r| r.get(0),
        )?;
        let last = AttemptRef {
            run_store_id: store.clone(),
            lineage_root_id: root.clone(),
            execution_id: tip.clone(),
            sequence: proof.chain.last().map_or(0, |s| s.sequence),
            attempt_id: id,
            launch_id: launch,
        };
        let incarnation = Incarnation {
            host_id: host_id.into(),
            boot_id: boot,
            pid_namespace_inode: namespace,
            uid,
            pid,
            start_ticks: ticks,
        };
        if last.validate().is_err() || incarnation.validate().is_err() {
            return Ok(unavailable());
        }
        let later = tip != bound || number > old_number;
        let state = if later {
            "superseded"
        } else if last != *expected || incarnation != *expected_incarnation {
            return Ok(unavailable());
        } else if matches!(
            status.as_str(),
            "succeeded" | "failed" | "cancelled" | "timed_out" | "lost"
        ) {
            "terminal_no_successor"
        } else if matches!(status.as_str(), "starting" | "running")
            && active == 1
            && inspected == Some(&incarnation)
        {
            "live"
        } else {
            return Ok(unavailable());
        };
        tx.commit()?;
        Ok(ConsensusCurrent {
            state: state.into(),
            last_attempt: Some(last),
            last_incarnation: Some(incarnation),
            successor_proof: Some(proof),
        })
    }

    /// Lookup a stored immutable successful ingress receipt, never renewing it.
    pub fn attempt_current_receipt(
        &self,
        store: &StoreId,
        pool_route: &Route,
        receipt_id: &str,
    ) -> Result<Option<CurrentReceipt>> {
        wire::validate_id(receipt_id)?;
        crate::store_identity::require(&self.conn, Some(store))?;
        let raw: Option<String> = self.conn.query_row("SELECT receipt_json FROM attempt_current_receipts WHERE receipt_id=? AND pool_route_json=?", params![receipt_id,route_json(pool_route)?], |r| r.get(0)).optional()?;
        raw.map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    /// Persist an ingress receipt after the caller has authenticated Bridge's
    /// original current context and inspected its exact process incarnation.
    /// The writer independently rechecks the durable current binding/lineage.
    pub fn attempt_current_ingress(
        &self,
        request: &IngressCurrent,
        observation: &Observation,
        inspected: &Incarnation,
    ) -> Result<CurrentReceipt> {
        self.attempt_current_ingress_gated(request, observation, inspected, true)
    }
    /// Check the enabled snapshot inside the same writer transaction that
    /// might create a current receipt; an existing exact receipt may replay.
    pub fn attempt_current_ingress_gated(
        &self,
        request: &IngressCurrent,
        observation: &Observation,
        inspected: &Incarnation,
        enabled: bool,
    ) -> Result<CurrentReceipt> {
        request.validate()?;
        observation.validate()?;
        inspected.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        let store = crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let fingerprint = wire::current_fingerprint(request)?;
        let pool = route_json(&request.pool_route)?;
        let prior: Option<(String,String)> = tx.query_row("SELECT fingerprint,receipt_json FROM attempt_current_receipts WHERE pool_route_json=? AND request_id=?",params![pool,request.request_id],|r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        if let Some((old, raw)) = prior {
            if old != fingerprint {
                return Err(Error::Conflict);
            }
            let receipt = serde_json::from_str(&raw)?;
            tx.commit()?;
            return Ok(receipt);
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let pending:bool=tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_pending_current WHERE pool_route_json=? AND request_id=? AND fingerprint=? AND launch_id=? AND state='pending')",params![pool,request.request_id,fingerprint,observation.launch_id],|r|r.get(0))?;
        if !pending {
            return Err(Error::Conflict);
        }
        if &observation.incarnation != inspected {
            return Err(invalid("attempt mismatch"));
        }
        let found: Option<(String,u32,String,String,String)> = tx.query_row("SELECT l.execution_id,l.sequence,l.attempt_id,l.state,a.root_agent_id FROM attempt_launches l JOIN attempts t ON t.id=l.attempt_id AND t.agent_id=l.execution_id JOIN agents a ON a.id=t.agent_id WHERE l.launch_id=?", [&observation.launch_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).optional()?;
        let Some((execution, sequence, attempt_id, state, root)) = found else {
            return Err(invalid("attempt not ready"));
        };
        if state != "bound" {
            return Err(invalid("attempt not ready"));
        }
        let root: AgentId = root.parse()?;
        let attempt = AttemptRef {
            run_store_id: store,
            lineage_root_id: root.clone(),
            execution_id: execution.parse()?,
            sequence,
            attempt_id,
            launch_id: observation.launch_id.clone(),
        };
        let proof = current_exact(
            &tx,
            &attempt,
            &observation.actor,
            request.run_route.worker_uid,
            &root,
            inspected,
        )?;
        let receipt = CurrentReceipt {
            receipt_id: uuid::Uuid::new_v4().to_string(),
            purpose: "ingress".into(),
            lineage_root_id: root.clone(),
            bound_execution_id: root,
            attempt,
            incarnation: inspected.clone(),
            actor: observation.actor.clone(),
            successor_proof: proof,
        };
        tx.execute("INSERT INTO attempt_current_receipts(request_id,pool_route_json,fingerprint,receipt_id,receipt_json) VALUES(?,?,?,?,?)",params![request.request_id,pool,fingerprint,receipt.receipt_id,serde_json::to_string(&receipt)?])?;
        if tx.execute("UPDATE attempt_pending_current SET state='resolved' WHERE pool_route_json=? AND request_id=? AND state='pending'",params![pool,request.request_id])?!=1 {return Err(Error::Conflict);}
        tx.commit()?;
        Ok(receipt)
    }

    /// Persist original endpoint identities and operation keys before discovery
    /// or any other remote I/O; preparation alone does not fence admissions.
    pub fn attempt_retirement_prepare(
        &self,
        request: &NamespaceRequest,
    ) -> Result<NamespaceRecord> {
        request.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        if let Some(existing) = namespace_record(&tx, request)? {
            tx.commit()?;
            return Ok(existing);
        }
        capacity(&tx, "retirements")?;
        tx.execute("INSERT INTO attempt_namespace_retirements(retirement_key,request_id,pool_seal_request_id,bridge_route_json,pool_route_json,producer,issuer_epoch,state) VALUES(?,?,?,?,?,?,?,'prepared')",params![request.retirement_key,request.request_id,request.pool_seal_request_id,route_json(&request.bridge_route)?,route_json(&request.pool_route)?,request.producer,request.issuer_epoch])?;
        tx.commit()?;
        Ok(NamespaceRecord {
            state: "prepared".into(),
            pool_seal_id: None,
            retired_count: None,
        })
    }

    /// Fence a retired issuer before contacting Pool or Bridge retirement.
    /// The current issuer comes only from fresh authenticated Bridge discovery.
    pub fn attempt_retirement_fence(
        &self,
        request: &NamespaceRequest,
        current_issuer: &str,
    ) -> Result<NamespaceRecord> {
        request.validate()?;
        wire::validate_id(current_issuer)?;
        if current_issuer == request.issuer_epoch {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let existing = namespace_record(&tx, request)?.ok_or(Error::Conflict)?;
        if existing.state != "prepared" {
            tx.commit()?;
            return Ok(existing);
        }
        if tx.execute("UPDATE attempt_namespace_retirements SET state='fenced' WHERE retirement_key=? AND state='prepared'",[&request.retirement_key])? != 1 {return Err(Error::Conflict);}
        tx.commit()?;
        Ok(NamespaceRecord {
            state: "fenced".into(),
            pool_seal_id: None,
            retired_count: None,
        })
    }

    /// Pin the same authenticated Pool seal ID across draining and sealed
    /// replies, before returning either state to the caller.
    pub fn attempt_retirement_observe_seal(
        &self,
        request: &NamespaceRequest,
        seal: &PoolSeal,
    ) -> Result<NamespaceRecord> {
        request.validate()?;
        if !seal.matches_request(request) {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let mut record = namespace_record(&tx, request)?.ok_or(Error::Conflict)?;
        if record.state == "prepared" {
            return Err(Error::Conflict);
        }
        if let Some(prior) = &record.pool_seal_id {
            if prior != &seal.seal_id {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(record);
        }
        if record.state!="fenced" || tx.execute("UPDATE attempt_namespace_retirements SET pool_seal_id=? WHERE retirement_key=? AND state='fenced' AND pool_seal_id IS NULL",params![seal.seal_id,request.retirement_key])?!=1 {return Err(Error::Conflict);}
        record.pool_seal_id = Some(seal.seal_id.clone());
        tx.commit()?;
        Ok(record)
    }

    /// Validate freshly authenticated Pool sealing and atomically verify all
    /// original Run admission work is settled before permitting Bridge I/O.
    pub fn attempt_retirement_invoke(
        &self,
        request: &NamespaceRequest,
        seal: &PoolSeal,
    ) -> Result<NamespaceRecord> {
        request.validate()?;
        wire::validate_id(&seal.seal_id)?;
        if !seal.matches_request(request) || seal.state != "sealed" {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let record = namespace_record(&tx, request)?.ok_or(Error::Conflict)?;
        if record.pool_seal_id.as_deref() != Some(&seal.seal_id) {
            return Err(Error::Conflict);
        }
        if record.state != "fenced" {
            tx.commit()?;
            return Ok(record);
        }
        let route = route_json(&request.bridge_route)?;
        let pending: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_pending_checks WHERE bridge_route_json=? AND producer=? AND issuer_epoch=? AND state='pending')", params![route,request.producer,request.issuer_epoch], |r| r.get(0))?;
        let unsettled: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_admissions a JOIN attempt_pending_checks p ON p.intent_id=a.intent_id WHERE p.bridge_route_json=? AND p.producer=? AND p.issuer_epoch=? AND a.state='admitted')", params![route,request.producer,request.issuer_epoch], |r| r.get(0))?;
        if pending || unsettled {
            return Err(invalid("namespace recovery incomplete"));
        }
        if tx.execute("UPDATE attempt_namespace_retirements SET state='invoking' WHERE retirement_key=? AND state='fenced' AND pool_seal_id=?",params![request.retirement_key,seal.seal_id])? != 1 { return Err(Error::Conflict); }
        tx.commit()?;
        Ok(NamespaceRecord {
            state: "invoking".into(),
            pool_seal_id: Some(seal.seal_id.clone()),
            retired_count: None,
        })
    }

    /// Record an exact verified Bridge retirement receipt. Unknown ACK keeps
    /// invoking durable and can only be retried with the same original key.
    pub fn attempt_retirement_complete(
        &self,
        request: &NamespaceRequest,
        pool_seal_id: &str,
        retired_count: u64,
    ) -> Result<NamespaceRecord> {
        request.validate()?;
        wire::validate_id(pool_seal_id)?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let record = namespace_record(&tx, request)?.ok_or(Error::Conflict)?;
        if record.pool_seal_id.as_deref() != Some(pool_seal_id) {
            return Err(Error::Conflict);
        }
        if record.state == "retired" {
            if record.retired_count != Some(retired_count) {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(record);
        }
        if record.state != "invoking" || tx.execute("UPDATE attempt_namespace_retirements SET state='retired',retired_count=? WHERE retirement_key=? AND state='invoking'",params![retired_count,request.retirement_key])? != 1 { return Err(Error::Conflict); }
        tx.commit()?;
        Ok(NamespaceRecord {
            state: "retired".into(),
            pool_seal_id: Some(pool_seal_id.into()),
            retired_count: Some(retired_count),
        })
    }

    /// Journal the exact admission intent before the first Bridge lookup. An
    /// exact pending replay is recovery and remains allowed behind a fence;
    /// a new intent under a fenced namespace is rejected before any I/O.
    pub fn attempt_begin_admission(&self, request: &AdmissionRequest) -> Result<()> {
        self.attempt_begin_admission_gated(request, true)
    }
    /// Writer-bound switch fence: an unadmitted pending key may not gain
    /// authority when the effective switch is disabled.
    pub fn attempt_begin_admission_gated(
        &self,
        request: &AdmissionRequest,
        enabled: bool,
    ) -> Result<()> {
        request.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let fingerprint = wire::admission_fingerprint(request)?;
        if let Some(record) = read_record(
            &tx,
            &request.intent_id,
            &request.security_digest,
            Some(&request.pool_route),
        )? {
            if record.state == "aborted_before_admit" {
                return Err(Error::Conflict);
            }
            let stored: Option<String> = tx.query_row(
                "SELECT request_fingerprint FROM attempt_admissions WHERE intent_id=?",
                [&request.intent_id],
                |r| r.get(0),
            )?;
            if stored.as_deref() != Some(&fingerprint) {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(());
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let source = &request.consumed_ref;
        let pool = route_json(&request.pool_route)?;
        let bridge = route_json(&source.bridge_route)?;
        let existing: Option<(String,String,String,String,String,String)> = tx.query_row(
            "SELECT security_digest,request_fingerprint,pool_route_json,bridge_route_json,producer,issuer_epoch FROM attempt_pending_checks WHERE intent_id=?",
            [&request.intent_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)),
        ).optional()?;
        if let Some((digest, old_fingerprint, old_pool, old_bridge, producer, epoch)) = existing {
            if digest != request.security_digest
                || old_fingerprint != fingerprint
                || old_pool != pool
                || old_bridge != bridge
                || producer != source.producer
                || epoch != source.issuer_epoch
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(());
        }
        capacity(&tx, "admissions")?;
        let fenced: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_namespace_retirements WHERE bridge_route_json=? AND producer=? AND issuer_epoch=? AND state!='prepared')", params![bridge,source.producer,source.issuer_epoch], |r| r.get(0))?;
        if fenced {
            return Err(Error::Conflict);
        }
        tx.execute("INSERT INTO attempt_pending_checks(intent_id,security_digest,request_fingerprint,pool_route_json,bridge_route_json,producer,issuer_epoch,state) VALUES(?,?,?,?,?,?,?,'pending')", params![request.intent_id,request.security_digest,fingerprint,pool,bridge,source.producer,source.issuer_epoch])?;
        tx.commit()
    }

    /// Observe an existing durable admission or permanent abort tombstone.
    pub fn attempt_admission_lookup(
        &self,
        store: &StoreId,
        pool_route: &Route,
        intent: &str,
        digest: &str,
    ) -> Result<Option<AdmissionRecord>> {
        crate::store_identity::require(&self.conn, Some(store))?;
        read_record(&self.conn, intent, digest, Some(pool_route))
    }

    /// Commit one authenticated current observation with a same-transaction
    /// durable binding recheck. Historical exact replay is served before any
    /// new Bridge lookup by `attempt_admission_lookup` in the caller.
    pub fn attempt_admit(
        &self,
        request: &AdmissionRequest,
        evidence: &CurrentConsume<'_>,
    ) -> Result<AdmissionRecord> {
        self.attempt_admit_gated(request, evidence, true)
    }
    /// Apply the switch inside the transaction before a pending request can
    /// become a durable admission. Existing admissions still replay.
    pub fn attempt_admit_gated(
        &self,
        request: &AdmissionRequest,
        evidence: &CurrentConsume<'_>,
        enabled: bool,
    ) -> Result<AdmissionRecord> {
        request.validate()?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(&request.expected_store_id))?;
        let fingerprint = wire::admission_fingerprint(request)?;
        if let Some(record) = read_record(
            &tx,
            &request.intent_id,
            &request.security_digest,
            Some(&request.pool_route),
        )? {
            let stored: Option<String> = tx.query_row(
                "SELECT request_fingerprint FROM attempt_admissions WHERE intent_id=?",
                [&request.intent_id],
                |r| r.get(0),
            )?;
            if record.state == "aborted_before_admit" {
                return Err(Error::Conflict);
            }
            if stored.as_deref() != Some(&fingerprint) {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(record);
        }
        if !enabled {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        let r = evidence.receipt;
        r.observation.validate()?;
        let source = &request.consumed_ref;
        if r.observation.actor != request.binding.actor {
            return Err(invalid("actor mismatch"));
        }
        if r.producer != source.producer
            || r.consume_request_id != source.consume_request_id
            || r.invocation_key != source.invocation_key
            || r.security_digest != source.security_digest
            || r.issuer_epoch != source.issuer_epoch
            || r.observation.launch_id != request.binding.attempt.launch_id
            || &r.observation.incarnation != evidence.inspected
        {
            return Err(invalid("attempt mismatch"));
        }
        let pending: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM attempt_pending_checks WHERE intent_id=? AND security_digest=? AND request_fingerprint=? AND state='pending')", params![request.intent_id,request.security_digest,fingerprint], |r| r.get(0))?;
        if !pending {
            return Err(Error::Conflict);
        }
        // An operation that began before a retirement fence may finish its
        // original-key recovery. Pool's namespace fence prevents a new effect.
        let proof = current_exact(
            &tx,
            &request.binding.attempt,
            &request.binding.actor,
            request.binding.run_route.worker_uid,
            &request.binding.attempt.execution_id,
            evidence.inspected,
        )?;
        let admission = Admission {
            admission_id: uuid::Uuid::new_v4().to_string(),
            intent_id: request.intent_id.clone(),
            security_digest: request.security_digest.clone(),
            policy: request.policy.clone(),
            binding: request.binding.clone(),
            incarnation: evidence.inspected.clone(),
            successor_proof: proof,
            consumed_ref: source.clone(),
        };
        tx.execute("INSERT INTO attempt_admissions(intent_id,security_digest,pool_route_json,request_fingerprint,admission_id,admission_json,state) VALUES(?,?,?,?,?,?,'admitted')", params![request.intent_id,request.security_digest,route_json(&request.pool_route)?,fingerprint,admission.admission_id,serde_json::to_string(&admission)?])?;
        if tx.execute("UPDATE attempt_pending_checks SET state='resolved' WHERE intent_id=? AND state='pending'", [&request.intent_id])? != 1 { return Err(Error::Conflict); }
        tx.commit()?;
        Ok(AdmissionRecord {
            state: "admitted".into(),
            admission: Some(admission),
            outcome_digest: None,
        })
    }

    /// Atomically fence a not-yet-admitted intent. Existing admissions require
    /// authenticated terminal Pool outcome via `attempt_settle` instead.
    pub fn attempt_abort_absent(
        &self,
        store: &StoreId,
        intent: &str,
        digest: &str,
        pool_route: &Route,
        requested_outcome_digest: Option<&str>,
    ) -> Result<AdmissionRecord> {
        wire::validate_id(intent)?;
        wire::validate_digest(digest)?;
        if let Some(value) = requested_outcome_digest {
            wire::validate_digest(value)?;
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        if let Some(record) = read_record(&tx, intent, digest, Some(pool_route))? {
            if record.state == "aborted_before_admit" {
                let stored: Option<String> = tx.query_row(
                    "SELECT abort_request_digest FROM attempt_admissions WHERE intent_id=?",
                    [intent],
                    |r| r.get(0),
                )?;
                if stored.as_deref() != requested_outcome_digest {
                    return Err(Error::Conflict);
                }
            }
            tx.commit()?;
            return Ok(record);
        }
        let pending: Option<(String,String)> = tx.query_row("SELECT security_digest,pool_route_json FROM attempt_pending_checks WHERE intent_id=?", [intent], |r| Ok((r.get(0)?,r.get(1)?))).optional()?;
        if pending.is_some_and(|(prior_digest, prior_route)| {
            prior_digest != digest
                || route_json(pool_route).ok().as_deref() != Some(prior_route.as_str())
        }) {
            return Err(Error::Conflict);
        }
        capacity(&tx, "admissions")?;
        tx.execute("INSERT INTO attempt_admissions(intent_id,security_digest,pool_route_json,abort_request_digest,state) VALUES(?,?,?,?,'aborted_before_admit')",params![intent,digest,route_json(pool_route)?,requested_outcome_digest])?;
        tx.execute("UPDATE attempt_pending_checks SET state='resolved' WHERE intent_id=? AND security_digest=? AND pool_route_json=? AND state='pending'",params![intent,digest,route_json(pool_route)?])?;
        tx.commit()?;
        Ok(AdmissionRecord {
            state: "aborted_before_admit".into(),
            admission: None,
            outcome_digest: None,
        })
    }

    /// Record only an authenticated Pool terminal result; no network I/O occurs
    /// while this same-writer transaction orders settlement against admission.
    pub fn attempt_settle(
        &self,
        store: &StoreId,
        admission_id: &str,
        outcome: &TerminalOutcome<'_>,
    ) -> Result<AdmissionRecord> {
        wire::validate_id(admission_id)?;
        wire::validate_digest(outcome.outcome_digest)?;
        let terminal = match outcome.state {
            "committed" => "settled_committed",
            "aborted" => "settled_aborted",
            _ => return Err(invalid("Pool outcome is not terminal")),
        };
        if wire::settlement_digest(
            &outcome.pool_route.component_store_id,
            outcome.intent_id,
            outcome.security_digest,
            outcome.state,
        )? != outcome.outcome_digest
        {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        let record = read_record(
            &tx,
            outcome.intent_id,
            outcome.security_digest,
            Some(outcome.pool_route),
        )?
        .ok_or(Error::Conflict)?;
        if record
            .admission
            .as_ref()
            .is_none_or(|a| a.admission_id != admission_id)
        {
            return Err(Error::Conflict);
        }
        if record.state != "admitted" {
            if record.state != terminal
                || record.outcome_digest.as_deref() != Some(outcome.outcome_digest)
            {
                return Err(Error::Conflict);
            }
            tx.commit()?;
            return Ok(record);
        }
        if tx.execute("UPDATE attempt_admissions SET state=?,outcome_digest=? WHERE intent_id=? AND state='admitted'",params![terminal,outcome.outcome_digest,outcome.intent_id])? != 1 { return Err(Error::Conflict); }
        tx.commit()?;
        Ok(AdmissionRecord {
            state: terminal.into(),
            admission: record.admission,
            outcome_digest: Some(outcome.outcome_digest.into()),
        })
    }
}
