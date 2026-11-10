//! Durable Pool enrollment keys and atomic external ownership of new roots.
use crate::{Store, batch_admission::BatchAdmission, writer::WriteTx};
use agent_run_domain::{
    Error, Result,
    attempt_binding::{self, Route},
    enrollment::{Admission, Key, Receipt, Start},
    error::invalid,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// A hard bound on permanent keys, including pre-admission tombstones.
const MAX_ENROLLMENTS: i64 = 16_384;

type EnrollmentRow = (
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);
type JobStateRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// All route facts originate in authenticated local configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Grant {
    pub pool_route: Route,
    pub run_route: Route,
    pub bridge_route: Route,
    pub producer: String,
    pub host_id: String,
    /// Digest of the exact configured transport and both resident/frontend pins.
    pub transport_pin_sha256: String,
}
impl Grant {
    /// Resolve only out-of-band local configuration; never a request field.
    pub fn from_config(config: &agent_run_config::provider_config::ProviderConfig) -> Result<Self> {
        let settings = config
            .managed_attempt_binding
            .as_ref()
            .ok_or_else(|| Error::Unsupported("managed enrollment unavailable".into()))?;
        let transport = settings
            .transport
            .as_ref()
            .ok_or_else(|| Error::Unsupported("managed enrollment unavailable".into()))?;
        let pin = json!({"pool_server":settings.pool_server,"pool_frontend_sha256":settings.pool_executable_sha256,
            "bridge_hook_sha256":settings.bridge_hook_sha256,"pool_resident_executable":settings.pool_resident_executable,
            "pool_resident_sha256":settings.pool_resident_sha256,"host_id":settings.host_id,"transport":transport});
        Ok(Self {
            pool_route: transport.pool_route.clone(),
            run_route: transport.run_route.clone(),
            bridge_route: transport.bridge_route.clone(),
            producer: transport.producer.clone(),
            host_id: settings.host_id.clone(),
            transport_pin_sha256: agent_run_domain::canonical::sha256_hex(&pin, true),
        })
    }
    fn validate(&self, key: &Key) -> Result<()> {
        key.validate(&self.pool_route)?;
        self.run_route.validate()?;
        self.bridge_route.validate()?;
        attempt_binding::validate_id(&self.producer)?;
        attempt_binding::validate_id(&self.host_id)?;
        attempt_binding::validate_digest(&self.transport_pin_sha256)?;
        if self.run_route.component_store_id != key.expected_store_id.to_string()
            || self.pool_route.worker_uid != self.run_route.worker_uid
            || self.bridge_route.worker_uid != self.run_route.worker_uid
        {
            return Err(invalid("route mismatch"));
        }
        Ok(())
    }
}

/// Exact stored state. Prepared is a pending operation, not absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub state: String,
    pub receipt: Option<Receipt>,
    pub grant: Option<Grant>,
}

/// Immutable result returned by the admission writer, including replay status.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Admitted {
    pub receipt: Receipt,
    pub replayed: bool,
}

fn route_json(route: &Route) -> Result<String> {
    Ok(serde_json::to_string(route)?)
}

fn record(conn: &Connection, key: &Key, pool_route: &Route) -> Result<Option<Record>> {
    key.validate(pool_route)?;
    crate::store_identity::require(conn, Some(&key.expected_store_id))?;
    let row: Option<EnrollmentRow> = conn.query_row(
        "SELECT pool_route_json,fingerprint,fingerprint_version,state,receipt_json,receipt_id,grant_json FROM managed_enrollments WHERE pool_store_id=? AND request_id=?",
        params![key.pool_store_id, key.request_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?)),
    ).optional()?;
    let Some((route, fingerprint, version, state, raw, receipt_id, raw_grant)) = row else {
        return Ok(None);
    };
    if route != route_json(pool_route)? || fingerprint != key.fingerprint {
        return Err(Error::Conflict);
    }
    if version != 1 {
        return Err(Error::Unsupported(
            "unknown enrollment fingerprint version".into(),
        ));
    }
    let receipt: Option<Receipt> = raw.map(|raw| serde_json::from_str(&raw)).transpose()?;
    let grant: Option<Grant> = raw_grant
        .map(|raw| serde_json::from_str(&raw))
        .transpose()?;
    if !matches!(state.as_str(), "prepared" | "admitted" | "aborted")
        || (state == "admitted") != receipt.is_some()
        || (matches!(state.as_str(), "prepared" | "admitted") && grant.is_none())
        || grant
            .as_ref()
            .is_some_and(|grant| grant.pool_route != *pool_route || grant.validate(key).is_err())
        || receipt.as_ref().is_some_and(|receipt| {
            receipt.version != 1
                || receipt.store_id != key.expected_store_id
                || receipt.request_id != key.request_id
                || receipt.fingerprint != key.fingerprint
                || receipt.state != "admitted"
                || receipt_id.as_deref() != Some(receipt.receipt_id.as_str())
                || attempt_binding::validate_uuid(&receipt.receipt_id).is_err()
                || !(1..=20).contains(&receipt.jobs.len())
                || receipt.jobs.iter().enumerate().any(|(index, job)| {
                    job.index != index
                        || job.sequence != 1
                        || job.lineage_root_id != job.execution_id
                        || attempt_binding::validate_id(&job.member_id).is_err()
                        || attempt_binding::validate_id(&job.managed_role_name).is_err()
                        || attempt_binding::validate_digest(&job.role_digest).is_err()
                })
                || receipt
                    .jobs
                    .iter()
                    .map(|job| &job.member_id)
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != receipt.jobs.len()
                || receipt
                    .jobs
                    .iter()
                    .map(|job| &job.lineage_root_id)
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != receipt.jobs.len()
        })
    {
        return Err(Error::Integrity("invalid enrollment receipt".into()));
    }
    Ok(Some(Record {
        state,
        receipt,
        grant,
    }))
}

fn capacity(conn: &Connection) -> Result<()> {
    let count: i64 = conn.query_row("SELECT count(*) FROM managed_enrollments", [], |row| {
        row.get(0)
    })?;
    if count >= MAX_ENROLLMENTS {
        return Err(invalid("attempt capacity exhausted"));
    }
    Ok(())
}

impl Store<'_> {
    /// Verify the live switch and original route while the writer is fenced.
    fn require_enrollment_config(
        &self,
        grant: &Grant,
        revision: Option<&str>,
        role: &str,
    ) -> Result<()> {
        let (config, actual) = agent_run_config::provider_config::ProviderConfig::load(&self.home)
            .map_err(|_| Error::Unsupported("managed enrollment unavailable".into()))?;
        let selected = config
            .managed_attempt_binding
            .as_ref()
            .filter(|settings| settings.enabled)
            .ok_or_else(|| Error::Unsupported("managed attempt binding disabled".into()))?;
        let current = Grant::from_config(&config)?;
        if &current != grant {
            return Err(invalid("enrollment transport changed"));
        }
        if revision.is_some_and(|expected| expected != actual) || selected.role_name != role {
            return Err(invalid("enrollment role mismatch"));
        }
        Ok(())
    }

    /// Read only; a negative result is not a fence against a later submit.
    pub fn managed_enrollment_lookup(&self, key: &Key, route: &Route) -> Result<Option<Record>> {
        record(&self.conn, key, route)
    }

    /// Prepare exact bytes before quota I/O; disabled mode may replay only a receipt.
    pub fn managed_enrollment_prepare(
        &self,
        key: &Key,
        grant: &Grant,
        request: &Value,
    ) -> Result<Record> {
        grant.validate(key)?;
        let (start, digest) = Start::parse(request)?;
        if start.expected_store_id != key.expected_store_id
            || start.pool_store_id != key.pool_store_id
            || start.request_id != key.request_id
            || digest != key.fingerprint
        {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        if let Some(existing) = record(&tx, key, &grant.pool_route)? {
            if existing
                .grant
                .as_ref()
                .is_some_and(|original| original != grant)
            {
                return Err(invalid("enrollment transport changed"));
            }
            if existing.state != "prepared" {
                tx.commit()?;
                return Ok(existing);
            }
            self.require_enrollment_config(grant, None, &start.jobs[0].selector.role_name)?;
            tx.commit()?;
            return Ok(existing);
        }
        self.require_enrollment_config(grant, None, &start.jobs[0].selector.role_name)?;
        if start
            .jobs
            .iter()
            .any(|job| job.selector.role_name != start.jobs[0].selector.role_name)
        {
            return Err(invalid("enrollment role mismatch"));
        }
        capacity(&tx)?;
        tx.execute(
            "INSERT INTO managed_enrollments(pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state,request_json,grant_json) VALUES(?,?,?,1,?,'prepared',?,?)",
            params![route_json(&grant.pool_route)?, key.pool_store_id, key.request_id, key.fingerprint, serde_json::to_string(request)?, serde_json::to_string(grant)?],
        )?;
        tx.commit()?;
        Ok(Record {
            state: "prepared".into(),
            receipt: None,
            grant: Some(grant.clone()),
        })
    }

    /// Permanently fence a missing or prepared key, or replay the admitted receipt.
    pub fn managed_enrollment_abort(&self, key: &Key, route: &Route) -> Result<Record> {
        key.validate(route)?;
        let tx = WriteTx::begin(&self.conn)?;
        match record(&tx, key, route)? {
            Some(existing) if existing.state != "prepared" => {
                tx.commit()?;
                return Ok(existing);
            }
            Some(_) => {
                tx.execute("UPDATE managed_enrollments SET state='aborted' WHERE pool_route_json=? AND pool_store_id=? AND request_id=? AND state='prepared'",params![route_json(route)?,key.pool_store_id,key.request_id])?;
            }
            None => {
                capacity(&tx)?;
                tx.execute("INSERT INTO managed_enrollments(pool_route_json,pool_store_id,request_id,fingerprint_version,fingerprint,state) VALUES(?,?,?,1,?,'aborted')",params![route_json(route)?,key.pool_store_id,key.request_id,key.fingerprint])?;
            }
        }
        tx.commit()?;
        Ok(Record {
            state: "aborted".into(),
            receipt: None,
            grant: None,
        })
    }

    /// Admit the existing provider batch and its ownership fence in one transaction.
    pub fn managed_enrollment_admit(
        &self,
        key: &Key,
        grant: &Grant,
        config_revision: &str,
        role_digests: &[String],
        batch: BatchAdmission<'_>,
    ) -> Result<Admitted> {
        grant.validate(key)?;
        let tx = WriteTx::begin(&self.conn)?;
        let existing = record(&tx, key, &grant.pool_route)?.ok_or(Error::Conflict)?;
        if existing
            .grant
            .as_ref()
            .is_some_and(|original| original != grant)
        {
            return Err(invalid("enrollment transport changed"));
        }
        if let Some(receipt) = existing.receipt {
            tx.commit()?;
            return Ok(Admitted {
                receipt,
                replayed: true,
            });
        }
        if existing.state == "aborted" {
            return Err(invalid("enrollment aborted"));
        }
        if existing.state != "prepared" {
            return Err(Error::Conflict);
        }
        let raw: String = tx.query_row(
            "SELECT request_json FROM managed_enrollments WHERE pool_store_id=? AND request_id=?",
            params![key.pool_store_id, key.request_id],
            |row| row.get(0),
        )?;
        let (start, digest) = Start::parse(&serde_json::from_str(&raw)?)?;
        if digest != key.fingerprint
            || !batch.retain_history
            || batch.expected_store_id != Some(&key.expected_store_id)
            || start.jobs.len() != batch.jobs.len()
            || start.jobs.len() != role_digests.len()
            || batch.preconditions.unwrap_or(&[]) != start.preconditions
        {
            return Err(Error::Conflict);
        }
        self.require_enrollment_config(
            grant,
            Some(config_revision),
            &start.jobs[0].selector.role_name,
        )?;
        for ((job, selected), digest) in start.jobs.iter().zip(&batch.jobs).zip(role_digests) {
            attempt_binding::validate_digest(digest)?;
            let input = &selected.inputs;
            let mut original: agent_run_domain::ProviderStartRequest =
                serde_json::from_value(job.spec.clone())?;
            original.validate()?;
            if job.selector.role_name != start.jobs[0].selector.role_name
                || &original != input.request
                || input.request.profile != job.selector.role_name
                || input.request.write
                || !input.request.explicit_finish
                || input.effective.write
                || input.authority.profile != job.selector.role_name
                || !matches!(
                    input.authority.harness,
                    agent_run_domain::HarnessId::Codex | agent_run_domain::HarnessId::ClaudeCode
                )
                || input.authority.role_payload["config_revision"] != *digest
                || input.identity["provider_config_sha256"] != config_revision
            {
                return Err(invalid("enrollment role mismatch"));
            }
        }
        let result = self.admit_batch(batch)?;
        // A public ordinary batch collision is never an externally owned new root.
        if result.replayed {
            return Err(Error::Conflict);
        }
        let mut jobs = Vec::with_capacity(start.jobs.len());
        for ((member, admitted), digest) in start
            .jobs
            .iter()
            .zip(&result.receipt.jobs)
            .zip(role_digests)
        {
            let pin = admitted
                .pin_id
                .clone()
                .ok_or_else(|| Error::Integrity("enrollment pin missing".into()))?;
            if tx.execute("UPDATE agents SET pool_membership_ever=1 WHERE id=? AND root_agent_id=id AND pool_membership_ever=0",[admitted.agent_id.as_str()])? != 1 {
                return Err(Error::Conflict);
            }
            jobs.push(Admission {
                index: admitted.index,
                member_id: member.member_id.clone(),
                lineage_root_id: admitted.agent_id.clone(),
                execution_id: admitted.agent_id.clone(),
                sequence: admitted.sequence,
                execution_ref: admitted.execution_ref.clone(),
                pin_id: pin,
                managed_role_name: member.selector.role_name.clone(),
                role_digest: digest.clone(),
            });
        }
        let receipt = Receipt {
            version: 1,
            store_id: key.expected_store_id.clone(),
            request_id: key.request_id.clone(),
            receipt_id: uuid::Uuid::new_v4().to_string(),
            fingerprint: key.fingerprint.clone(),
            state: "admitted".into(),
            jobs,
        };
        if tx.execute("UPDATE managed_enrollments SET state='admitted',receipt_id=?,receipt_json=? WHERE pool_route_json=? AND pool_store_id=? AND request_id=? AND state='prepared'",params![receipt.receipt_id,serde_json::to_string(&receipt)?,route_json(&grant.pool_route)?,key.pool_store_id,key.request_id])? != 1 { return Err(Error::Conflict); }
        for job in &receipt.jobs {
            tx.execute("INSERT INTO managed_enrollment_members(lineage_root_id,pool_route_json,pool_store_id,pool_id,request_id,member_id,member_index) VALUES(?,?,?,?,?,?,?)",params![job.lineage_root_id.as_str(),route_json(&grant.pool_route)?,key.pool_store_id,start.pool_id,key.request_id,job.member_id,job.index])?;
        }
        tx.commit()?;
        Ok(Admitted {
            receipt,
            replayed: false,
        })
    }

    /// Bounded non-authoritative job diagnostics for an immutable admission.
    pub fn managed_enrollment_jobs_state(&self, receipt: &Receipt) -> Result<Vec<Value>> {
        let mut states = Vec::with_capacity(receipt.jobs.len());
        for job in &receipt.jobs {
            let row: Option<JobStateRow> = self.conn.query_row(
                "SELECT a.status,a.failure_kind,q.launch_id,q.role_digest,q.config_digest,q.inventory_digest FROM agents a LEFT JOIN attempts t ON t.id=(SELECT id FROM attempts WHERE agent_id=a.id ORDER BY number DESC LIMIT 1) LEFT JOIN attempt_role_qualifications q ON q.attempt_id=t.id WHERE a.root_agent_id=? ORDER BY a.sequence DESC LIMIT 1",
                [job.lineage_root_id.as_str()],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))
            ).optional()?;
            let (state, failure, qualification) = match row {
                None => ("unknown", None, None),
                Some((status, failure, launch, role, config, inventory)) => {
                    let qualification = launch.zip(role).zip(config.zip(inventory)).map(|((launch_id,role_digest),(config_digest,inventory_digest))| json!({"launch_id":launch_id,"role_digest":role_digest,"config_digest":config_digest,"inventory_digest":inventory_digest}));
                    let state = match status.as_str() {
                        "created" => "not_started",
                        "starting" => "starting",
                        "running" if qualification.is_some() => "qualified",
                        "running" => "qualification_pending",
                        "succeeded" | "failed" | "cancelled" | "lost" | "timed_out" => "terminal",
                        _ => "unknown",
                    };
                    let failure = failure.map(|kind| match kind.as_str() {
                        "preparation_rejected" => "preparation_rejected",
                        "supervisor_spawn_failed" => "supervisor_spawn_failed",
                        _ => "execution_failed",
                    });
                    (state, failure, qualification)
                }
            };
            states.push(json!({"index":job.index,"member_id":job.member_id,"state":state,"failure_code":failure,"last_attempt":null,"incarnation":null,"qualification":qualification}));
        }
        Ok(states)
    }

    /// Resolve a member only through its original durable enrollment and fence.
    pub fn managed_enrollment_member(
        &self,
        key: &Key,
        route: &Route,
        member_id: &str,
    ) -> Result<(Receipt, Admission, Grant)> {
        attempt_binding::validate_id(member_id)?;
        let record = self
            .managed_enrollment_lookup(key, route)?
            .ok_or_else(|| invalid("attempt not ready"))?;
        if record.state == "aborted" {
            return Err(invalid("enrollment aborted"));
        }
        let grant = record
            .grant
            .ok_or_else(|| Error::Integrity("enrollment transport pin is missing".into()))?;
        let receipt = record.receipt.ok_or_else(|| invalid("attempt not ready"))?;
        let member = receipt
            .jobs
            .iter()
            .find(|job| job.member_id == member_id)
            .cloned()
            .ok_or_else(|| invalid("attempt mismatch"))?;
        let matches: bool = self.conn.query_row("SELECT EXISTS(SELECT 1 FROM managed_enrollment_members WHERE lineage_root_id=? AND pool_route_json=? AND pool_store_id=? AND request_id=? AND member_id=? AND member_index=?)",params![member.lineage_root_id.as_str(),route_json(route)?,key.pool_store_id,key.request_id,member_id,member.index],|r|r.get(0))?;
        if !matches {
            return Err(Error::Integrity("enrollment ownership is missing".into()));
        }
        Ok((receipt, member, grant))
    }

    /// Project the original supervisor qualification for one exact receipt.
    pub fn managed_enrollment_qualification(
        &self,
        receipt: &agent_run_domain::attempt_binding::CurrentReceipt,
    ) -> Result<Value> {
        let incarnation = &receipt.incarnation;
        let row: Option<(String, String, String)> = self.conn.query_row(
            "SELECT role_digest,config_digest,inventory_digest FROM attempt_role_qualifications WHERE launch_id=? AND attempt_id=? AND boot_id=? AND pid_namespace_inode=? AND uid=? AND pid=? AND start_ticks=?",
            params![receipt.attempt.launch_id,receipt.attempt.attempt_id,incarnation.boot_id,incarnation.pid_namespace_inode,incarnation.uid,incarnation.pid,incarnation.start_ticks],
            |r|Ok((r.get(0)?,r.get(1)?,r.get(2)?)),
        ).optional()?;
        let (role_digest, config_digest, inventory_digest) =
            row.ok_or_else(|| invalid("attempt not ready"))?;
        Ok(
            json!({"launch_id":receipt.attempt.launch_id,"role_digest":role_digest,"config_digest":config_digest,"inventory_digest":inventory_digest}),
        )
    }
}
