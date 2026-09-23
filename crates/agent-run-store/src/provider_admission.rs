//! Atomic first-attempt admission from trusted provider quota candidates.

use crate::{admission, lineage, tx_event, Record, Store, ACTIVE_SQL};
use agent_run_domain::{
    canonical,
    catalog::{
        AccountId, AttemptCredentials, ProviderCatalog, QuotaAdmissionError, QuotaCandidateSet,
        ResolvedLaunchAuthority, SelectionIntent,
    },
    domain::{now, AgentId, StartRequest, Status},
    error::invalid,
    Error, ProviderStartRequest, Result,
};
use agent_run_platform::process;
use agent_run_platform::process::{Cleanup, Identity};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};

/// One durable first-attempt selection, including an idempotent replay.
#[derive(Debug, Clone)]
pub struct ProviderAdmission {
    /// One logical agent id retained across future attempts.
    pub agent_id: AgentId,
    /// Exact reserved first-attempt id.
    pub attempt_id: String,
    /// Global physical account selected in the admission transaction.
    pub account_id: AccountId,
    /// False only for a byte-identical request-id replay.
    pub created: bool,
}

/// The continued parent of one explicit provider resume.
#[derive(Debug, Clone, Copy)]
pub struct ProviderResume<'a> {
    /// Terminal parent run whose native session the child continues.
    pub parent: &'a AgentId,
    /// Account the parent's last attempt used; an automatic resume keeps it
    /// while it is still a valid candidate and switches only when it is not.
    pub prefer: &'a AccountId,
}

/// Returns an existing v2 admission before consulting mutable configuration,
/// or a conflict for a request id previously used by another identity.
pub fn replay(store: &Store, request: &ProviderStartRequest) -> Result<Option<ProviderAdmission>> {
    let projection = request.storage_projection();
    admission::replay_request(store, &projection)?
        .map(|record| replayed(&store.conn, &record, request))
        .transpose()
}

/// Verifies one row's explicit v2 request and returns its immutable selection.
fn replayed(
    conn: &rusqlite::Connection,
    record: &Record,
    request: &ProviderStartRequest,
) -> Result<ProviderAdmission> {
    let fingerprint = canonical::sha256_hex(&serde_json::to_value(request)?, true);
    let identity = record.identity.as_ref().ok_or(Error::Conflict)?;
    if identity["provider_identity_version"] != 2
        || identity["replay_request_sha256"] != fingerprint
    {
        return Err(Error::Conflict);
    }
    let (attempt_id, selected): (String, String) = conn.query_row(
        "SELECT id,selected_account_id FROM attempts WHERE agent_id=? AND number=1",
        [record.id.as_str()],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(ProviderAdmission {
        agent_id: record.id.clone(),
        attempt_id,
        account_id: selected.parse()?,
        created: false,
    })
}

impl Store {
    /// Returns the one owned first attempt and selected account for an
    /// admitted v2 agent; no account label or runtime spelling is inferred.
    pub fn provider_attempt(&self, id: &AgentId) -> Result<(String, AccountId)> {
        let (attempt, account): (String, String) = self.conn.query_row(
            "SELECT id,selected_account_id FROM attempts WHERE agent_id=? AND ownership_active=1",
            [id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((attempt, account.parse()?))
    }

    /// Marks the owned attempt as entering spawn before any child is created.
    pub fn provider_spawning(&self, id: &AgentId, attempt: &str) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE attempts SET phase='spawning',state='starting' \
             WHERE id=? AND agent_id=? AND ownership_active=1 AND phase='prepared'",
            params![attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Returns a pre-child OS spawn failure to the prepared phase so the
    /// supervisor can certify never-spawned cleanup and close ownership.
    pub fn provider_spawn_failed(&self, id: &AgentId, attempt: &str) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE attempts SET phase='prepared' \
             WHERE id=? AND agent_id=? AND ownership_active=1 AND phase='spawning' AND process_identity IS NULL",
            params![attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Persists the inspected child leader identity immediately after spawn,
    /// before the supervisor can mark the agent running.
    pub fn provider_process(&self, id: &AgentId, attempt: &str, leader: &Identity) -> Result<()> {
        let changed = self.conn.execute(
            "UPDATE attempts SET process_identity=?,process_birth_time=? \
             WHERE id=? AND agent_id=? AND ownership_active=1 AND phase='spawning' AND process_identity IS NULL",
            params![leader.token, leader.birth, attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Records verified process-group and descendant cleanup for the exact
    /// owned attempt. An unknown cleanup cannot release its reservation.
    pub fn provider_cleanup(&self, id: &AgentId, attempt: &str, proof: &Cleanup) -> Result<()> {
        if !proof.confirmed {
            return Err(invalid("provider process cleanup is unverified"));
        }
        let changed = self.conn.execute(
            "UPDATE attempts SET cleanup_proof_json=?,phase='cleanup_complete' \
             WHERE id=? AND agent_id=? AND ownership_active=1 AND process_identity IS NOT NULL",
            params![serde_json::to_string(proof)?, attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Certifies an attempt that remained prepared never spawned a child.
    /// This is only for cancellation or preparation failure before spawn.
    pub fn provider_never_spawned(&self, id: &AgentId) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE attempts SET cleanup_proof_json='{\"never_spawned\":true}',phase='cleanup_complete' \
             WHERE agent_id=? AND ownership_active=1 AND phase='prepared' AND process_identity IS NULL",
            [id.as_str()],
        )?;
        Ok(changed == 1)
    }

    /// Looks up one provider request-id replay without loading mutable config.
    pub fn replay_provider_request(
        &self,
        request: &ProviderStartRequest,
    ) -> Result<Option<ProviderAdmission>> {
        replay(self, request)
    }

    /// Atomically validates the committed candidate rank, current registry,
    /// account scope, global/harness caps and reservations, then creates one
    /// logical agent, owned attempt, exact physical keys, and initial events.
    ///
    /// The caller supplies trusted quota candidates, never wire input.
    /// Replay wins before revision/capacity checks. A stale revision returns
    /// `selection_stale` and commits no row; scoring stays outside the store.
    #[allow(clippy::too_many_arguments)]
    pub fn admit_provider(
        &mut self,
        request: &ProviderStartRequest,
        effective: &StartRequest,
        catalog: &ProviderCatalog,
        authority: &ResolvedLaunchAuthority,
        candidates: &QuotaCandidateSet,
        identity: &Value,
        global_cap: usize,
        harness_cap: Option<usize>,
        pinned: Option<&AccountId>,
    ) -> Result<ProviderAdmission> {
        self.admit_provider_lineage(
            request,
            effective,
            catalog,
            authority,
            candidates,
            identity,
            global_cap,
            harness_cap,
            pinned,
            None,
        )
    }

    /// [`Self::admit_provider`] for an explicit resume: in the same immediate
    /// transaction it also proves the parent terminal, process-quiescent and
    /// fully cleaned up (every attempt carries cleanup proof), takes the
    /// parent's lineage (root, next sequence, native session id) and keeps
    /// the parent's account while it is still a valid candidate. The partial
    /// unique parent index still forbids a second child; a replayed request id
    /// must name the same parent.
    #[allow(clippy::too_many_arguments)]
    pub fn admit_provider_resume(
        &mut self,
        request: &ProviderStartRequest,
        effective: &StartRequest,
        catalog: &ProviderCatalog,
        authority: &ResolvedLaunchAuthority,
        candidates: &QuotaCandidateSet,
        identity: &Value,
        global_cap: usize,
        harness_cap: Option<usize>,
        pinned: Option<&AccountId>,
        resume: ProviderResume<'_>,
    ) -> Result<ProviderAdmission> {
        self.admit_provider_lineage(
            request,
            effective,
            catalog,
            authority,
            candidates,
            identity,
            global_cap,
            harness_cap,
            pinned,
            Some(resume),
        )
    }

    /// Shared body of [`Self::admit_provider`] and
    /// [`Self::admit_provider_resume`]; `resume` is `None` for a new root run.
    #[allow(clippy::too_many_arguments)]
    fn admit_provider_lineage(
        &mut self,
        request: &ProviderStartRequest,
        effective: &StartRequest,
        catalog: &ProviderCatalog,
        authority: &ResolvedLaunchAuthority,
        candidates: &QuotaCandidateSet,
        identity: &Value,
        global_cap: usize,
        harness_cap: Option<usize>,
        pinned: Option<&AccountId>,
        resume: Option<ProviderResume<'_>>,
    ) -> Result<ProviderAdmission> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let session = admission::replay_session(&tx, effective)?;
        if effective.orchestrator.is_none() || session.is_some() {
            if let Some(found) =
                admission::replay_in_transaction(&tx, effective, session.as_deref())?
            {
                if found.parent_agent_id.as_ref() != resume.map(|resume| resume.parent) {
                    return Err(Error::Conflict);
                }
                let replay = replayed(&tx, &found, request)?;
                tx.commit()?;
                return Ok(replay);
            }
        }
        let lineage = match resume {
            Some(resume) => {
                let lineage = lineage::resume_parent(&tx, resume.parent)?;
                let uncleaned: i64 = tx.query_row(
                    "SELECT COUNT(*) FROM attempts WHERE agent_id=? \
                     AND (cleanup_proof_json IS NULL OR phase!='cleanup_complete')",
                    [resume.parent.as_str()],
                    |row| row.get(0),
                )?;
                if uncleaned != 0 {
                    return Err(Error::Unsupported(format!(
                        "continuation_unavailable: agent {} has an attempt without cleanup proof",
                        resume.parent
                    )));
                }
                Some(lineage)
            }
            None => None,
        };
        candidates.validate()?;
        authority.validate()?;
        let definition = catalog
            .provider(&request.provider)
            .ok_or_else(|| invalid("provider is not configured"))?;
        if candidates.provider != request.provider
            || candidates.model != request.model
            || authority.provider != request.provider
            || authority.model != request.model
            || authority.harness != definition.harness
            || authority.connection != definition.connection
            || authority.profile != request.profile
            || authority.workdir != effective.workdir
            || effective.runtime != request.provider.as_str()
            || effective.model != request.model
            || effective.task != request.task
            || global_cap == 0
            || harness_cap == Some(0)
            || identity["provider_identity_version"] != 2
            || identity["provider_request"] != serde_json::to_value(request)?
            || identity["authority"] != serde_json::to_value(authority)?
            || identity["replay_request_sha256"]
                != canonical::sha256_hex(&serde_json::to_value(request)?, true)
            || !matches!(
                (&candidates.intent, pinned),
                (SelectionIntent::Auto, None) | (SelectionIntent::Pinned(_), Some(_))
            )
            || pinned.is_some_and(|account| !matches!(&candidates.intent, SelectionIntent::Pinned(id) if id == account))
        {
            return Err(invalid("provider admission inputs disagree"));
        }
        let revision = Store::quota_capacity_revision_in(&tx)?;
        if revision != candidates.capacity_revision {
            return Err(QuotaAdmissionError::SelectionStale {
                committed_capacity_revision: candidates.capacity_revision,
                current_capacity_revision: revision,
            }
            .into());
        }
        let global: i64 = tx.query_row(
            &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL}"),
            [],
            |row| row.get(0),
        )?;
        let harness: i64 = tx.query_row(
            &format!("SELECT COUNT(*) FROM agents WHERE status IN {ACTIVE_SQL} AND json_extract(identity_json,'$.authority.harness')=?"),
            [authority.harness.as_str()], |row| row.get(0),
        )?;
        if global >= global_cap as i64 || harness_cap.is_some_and(|cap| harness >= cap as i64) {
            return Err(Error::Capacity);
        }
        let mut chosen = None;
        for candidate in &candidates.candidates {
            if pinned.is_some_and(|account| &candidate.account != account)
                || !authority.eligible_accounts.contains(&candidate.account)
                || AttemptCredentials::from_selected(
                    catalog,
                    &request.provider,
                    &request.model,
                    &candidate.account,
                )
                .is_err()
            {
                continue;
            }
            let current: Option<(String, String, String)> = tx.query_row(
                "SELECT auth_family,secret_ref,status FROM provider_accounts WHERE account_id=?",
                [candidate.account.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            ).optional()?;
            let Some((family, reference, status)) = current else {
                continue;
            };
            let Some(registered) = catalog.account(&candidate.account) else {
                continue;
            };
            if status != "enabled"
                || family != registered.auth_family.as_str()
                || reference != registered.secret_ref.as_str()
            {
                continue;
            }
            let active = Store::active_attempt_counts_in(&tx, &[candidate.account.clone()])?
                [candidate.account.as_str()];
            let reserved = Store::active_reservation_counts_in(&tx, &candidate.physical_keys)?;
            let load = active.max(reserved.values().copied().max().unwrap_or(0));
            // A resume keeps its parent's still-valid account ahead of any
            // rank; it moves only when that account is no longer a candidate.
            let kept = resume.is_none_or(|resume| &candidate.account != resume.prefer);
            let key = (kept, candidate.rank, load, candidate.account.as_str());
            if chosen.as_ref().is_none_or(|(best, _)| key < *best) {
                chosen = Some((key, candidate));
            }
        }
        let Some((_, candidate)) = chosen else {
            return Err(QuotaAdmissionError::NoEligibleAccount {
                provider: request.provider.clone(),
                model: request.model.clone(),
            }
            .into());
        };
        let at = now();
        let session = admission::upsert_session(&tx, effective, at)?;
        let id = AgentId::new();
        let attempt_id = format!("att_{}", uuid::Uuid::new_v4().simple());
        let summary: String = request
            .task
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .chars()
            .take(160)
            .collect();
        let (root, sequence, resumed) = match &lineage {
            Some(lineage) => (
                lineage.root_agent_id.clone(),
                lineage.sequence,
                Some(lineage.runtime_session_id.as_str()),
            ),
            None => (id.clone(), 1, None),
        };
        let inserted = tx.execute(
            "INSERT INTO agents(id,request_id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,parent_agent_id,root_agent_id,sequence,resume_of_runtime_session_id,identity_json,selection_intent,requested_account_id) \
             VALUES(?,?,?,?,?,?,?,?,?,?,'starting',?,?,'pending:provider-v2',?,?,?,?,?,?,?)",
            params![id.as_str(), request.request_id, session, request.provider.as_str(),
                request.model, request.profile, request.task, summary,
                effective.workdir.to_string_lossy(), serde_json::to_string(effective)?,
                at, effective.timeout_seconds.unwrap_or(480.0),
                resume.map(|resume| resume.parent.as_str()), root.as_str(), sequence, resumed,
                serde_json::to_string(identity)?,
                if pinned.is_some() { "pinned" } else { "auto" },
                pinned.map(AccountId::as_str)],
        );
        match inserted {
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation && resume.is_some() =>
            {
                return Err(invalid(format!(
                    "agent {} has already been resumed",
                    resume
                        .map(|resume| resume.parent.as_str())
                        .unwrap_or_default()
                )));
            }
            other => {
                other?;
            }
        }
        if let Ok(owner) = process::inspect(std::process::id() as i32) {
            tx.execute(
                "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
                params![serde_json::to_string(&owner)?, owner.birth, id.as_str()],
            )?;
        }
        tx.execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id,phase,ownership_active) \
             VALUES(?,?,1,'prepared','{}',?,?,'prepared',1)",
            params![attempt_id, id.as_str(), at, candidate.account.as_str()],
        )?;
        for key in &candidate.physical_keys {
            tx.execute(
                "INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,?)",
                params![attempt_id, key.as_str()],
            )?;
        }
        Store::advance_quota_capacity_revision(&tx)?;
        tx_event(&tx, &id, "created", None, Some(Status::Created), &json!({}))?;
        tx_event(
            &tx,
            &id,
            "start_accepted",
            Some(Status::Created),
            Some(Status::Starting),
            &match resume {
                Some(resume) => json!({"durable_admission":true,"resume_of":resume.parent}),
                None => json!({"durable_admission":true}),
            },
        )?;
        tx.commit()?;
        Ok(ProviderAdmission {
            agent_id: id,
            attempt_id,
            account_id: candidate.account.clone(),
            created: true,
        })
    }
}
