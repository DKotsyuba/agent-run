//! Atomic first-attempt admission from trusted provider quota candidates.

use crate::{ACTIVE_SQL, Record, Store, admission, lineage, tx_event};
use agent_run_domain::{
    Error, ProviderStartRequest, Result, canonical,
    catalog::{
        AccountId, AttemptCredentials, BindingCooldownReason, ProviderBinding, ProviderCatalog,
        ProviderConnection, ProviderDefinition, QuotaAdmissionError, QuotaCandidateSet,
        ResolvedLaunchAuthority, SelectionIntent,
    },
    domain::{AgentId, StartRequest, Status, now},
    error::invalid,
};
use agent_run_platform::process;
use agent_run_platform::process::{Cleanup, Identity};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};

#[cfg(feature = "test-fixtures")]
thread_local! {
    /// Fail one owner inspection after a specified number of successful probes.
    static OWNER_FAILURE_AFTER: std::cell::Cell<Option<usize>> = const { std::cell::Cell::new(None) };
}

/// Arms a single synthetic owner-inspection failure on the calling test thread.
#[cfg(feature = "test-fixtures")]
pub(crate) fn fixture_fail_owner_after(successful: usize) {
    OWNER_FAILURE_AFTER.with(|state| state.set(Some(successful)));
}

/// Captures the admitting process's native identity or refuses the whole admission.
fn admission_owner() -> Result<Identity> {
    #[cfg(feature = "test-fixtures")]
    if OWNER_FAILURE_AFTER.with(|state| match state.get() {
        Some(0) => {
            state.set(None);
            true
        }
        Some(remaining) => {
            state.set(Some(remaining - 1));
            false
        }
        None => false,
    }) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "synthetic owner inspection failure",
        )
        .into());
    }
    let pid = i32::try_from(std::process::id())
        .map_err(|_| invalid("admitting process identity is unavailable"))?;
    let owner = process::inspect(pid)?;
    if pid <= 1
        || owner.pid != pid
        || owner.zombie
        || !owner.birth.is_finite()
        || owner.birth < 0.0
        || owner.token.len() > 256
        || !(owner.token.starts_with("linux:") || owner.token.starts_with("darwin:"))
    {
        return Err(invalid("admitting process identity is invalid"));
    }
    Ok(owner)
}

/// SQL predicate on an `attempts` row `t` that provably never spawned a
/// child: its execution's only owned attempt, still `prepared`, with no
/// recorded engine identity or birth, no cleanup proof yet and no process
/// ownership (leader/group) or member rows. A `spawning` phase, any process
/// evidence or an existing (even unconfirmed) cleanup proof is never
/// treated as never-spawned.
pub(crate) const NEVER_SPAWNED: &str = "t.ownership_active=1 AND t.phase='prepared' \
     AND t.process_identity IS NULL AND t.process_birth_time IS NULL AND t.cleanup_proof_json IS NULL \
     AND (SELECT COUNT(*) FROM attempts u WHERE u.agent_id=t.agent_id AND u.ownership_active=1)=1 \
     AND NOT EXISTS(SELECT 1 FROM process_ownership o WHERE o.owner_kind='attempt' AND o.owner_id=t.id) \
     AND NOT EXISTS(SELECT 1 FROM process_members m WHERE m.owner_kind='attempt' AND m.owner_id=t.id)";

/// Duration of a confirmed ordered-binding failure, in seconds.
pub const BINDING_COOLDOWN_SECONDS: f64 = 3600.0;

/// One durable first-attempt selection, including an idempotent replay.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// Read-only system-worker proof of the unchanged parent and last attempt.
    pub preflight_digest: Option<&'a str>,
    /// Broker-owned restart permitting this exact captured continuation.
    pub restart_id: Option<i64>,
}

/// Fingerprints the exact parent and last-attempt evidence used by a system
/// resume preflight, without exposing the stored request, paths or history.
pub fn resume_preflight_digest(conn: &rusqlite::Connection, parent: &AgentId) -> Result<String> {
    type ParentAttempt = (
        String,
        Option<String>,
        String,
        Option<String>,
        String,
        String,
        Option<String>,
        String,
        i64,
    );
    let row: ParentAttempt = conn.query_row(
        "SELECT a.request_json,a.identity_json,a.status,a.runtime_session_id,\
         t.id,t.adapter_state_json,t.selected_account_id,t.phase,t.ownership_active \
         FROM agents a JOIN attempts t ON t.agent_id=a.id \
         WHERE a.id=?1 ORDER BY t.number DESC LIMIT 1",
        [parent.as_str()],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ))
        },
    )?;
    Ok(canonical::sha256_hex(&serde_json::to_value(row)?, true))
}

/// Returns an existing v2 admission before consulting mutable configuration,
/// or a conflict for a request id previously used by another identity.
pub fn replay(store: &Store, request: &ProviderStartRequest) -> Result<Option<ProviderAdmission>> {
    let projection = request.storage_projection();
    admission::replay_request(store, &projection)?
        .map(|record| replayed(&store.conn, &record, request))
        .transpose()
}

/// Verifies the original immutable v2 replay proof, then compares current
/// intent after projecting retired run controls. Known legacy transport aliases
/// remain supported; no historical JSON or selection is rewritten.
fn replayed(
    conn: &rusqlite::Connection,
    record: &Record,
    request: &ProviderStartRequest,
) -> Result<ProviderAdmission> {
    let request_value = serde_json::to_value(request)?;
    let mut fingerprints = vec![canonical::sha256_hex(&request_value, true)];
    if let Some(reference) = &request.orchestrator {
        let alias = reference.legacy_transport()?;
        let mut legacy = request_value;
        legacy["orchestrator"]["transport"] = json!(alias);
        fingerprints.push(canonical::sha256_hex(&legacy, true));
    }
    let identity = record.identity.as_ref().ok_or(Error::Conflict)?;
    let raw = identity["provider_request"].clone();
    let mut original_digests = vec![canonical::sha256_hex(&raw, true)];
    let historical = ProviderStartRequest::from_history(raw.clone())?;
    if let Some(reference) = &historical.orchestrator {
        let canonical = reference.canonical_transport()?;
        let legacy = reference.legacy_transport()?;
        for transport in [canonical, legacy] {
            let mut spelling = raw.clone();
            spelling["orchestrator"]["transport"] = json!(transport);
            original_digests.push(canonical::sha256_hex(&spelling, true));
        }
    }
    let projected_digest = canonical::sha256_hex(&serde_json::to_value(historical)?, true);
    if identity["provider_identity_version"] != 2
        || !identity["replay_request_sha256"]
            .as_str()
            .is_some_and(|previous| original_digests.iter().any(|digest| digest == previous))
        || !fingerprints.iter().any(|value| value == &projected_digest)
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

impl Store<'_> {
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

    /// Returns the account the agent's most recent attempt was admitted with.
    ///
    /// The recorded selection is authoritative for offline work on a sealed
    /// home: it never consults the current configuration or a provider
    /// default. An agent with no recorded account selection — a legacy or
    /// never-admitted row — is a validation error, not a guess.
    pub fn recorded_account(&self, id: &AgentId) -> Result<AccountId> {
        let account: Option<String> = self
            .conn
            .query_row(
                "SELECT selected_account_id FROM attempts WHERE agent_id=? \
                 ORDER BY number DESC, rowid DESC LIMIT 1",
                [id.as_str()],
                |row| row.get(0),
            )
            .optional()?
            .flatten();
        account
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                invalid(format!(
                    "agent {} has no recorded account selection",
                    id.as_str()
                ))
            })?
            .parse()
    }

    /// Claims the owned prepared attempt for spawning before any child is
    /// created. Returns `false`, changing nothing, when a cancel is pending or
    /// claimed: the claim and the cancel check are one statement, so an
    /// accepted cancel can never race past this boundary.
    /// Configured service gates and leased generations must still be ready in
    /// the same statement; a readiness race returns Conflict without spawning.
    pub fn provider_spawning(&self, id: &AgentId, attempt: &str) -> Result<bool> {
        let changed = self.conn.execute(
            "UPDATE attempts SET phase='spawning',state='starting' \
             WHERE id=?1 AND agent_id=?2 AND ownership_active=1 AND phase='prepared' \
             AND NOT EXISTS (SELECT 1 FROM commands WHERE agent_id=?2 AND kind='cancel' AND state IN ('pending','claimed')) \
             AND NOT EXISTS (SELECT 1 FROM agent_service_gates WHERE agent_id=?2 AND state!='ready') \
             AND NOT EXISTS (SELECT 1 FROM managed_service_leases l JOIN managed_service_generations g ON g.id=l.generation_id WHERE l.agent_id=?2 AND l.released_at IS NULL AND g.state!='ready')",
            params![attempt, id.as_str()],
        )?;
        if changed == 1 {
            return Ok(true);
        }
        let cancelled: bool = self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='cancel' AND state IN ('pending','claimed'))",
            [id.as_str()],
            |row| row.get(0),
        )?;
        if cancelled {
            Ok(false)
        } else {
            Err(Error::Conflict)
        }
    }

    /// Returns a pre-child validation or OS spawn refusal to the prepared phase
    /// so the supervisor can certify never-spawned cleanup and close ownership.
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
        self.provider_process_evidence(id, attempt, leader, None)
    }

    /// Atomically anchors the leader and optional actual native store before running.
    /// Only supervisor operations supply evidence; session is observed separately.
    /// An existing identity, foreign attempt or prepopulated session refuses.
    pub fn provider_process_evidence(
        &self,
        id: &AgentId,
        attempt: &str,
        leader: &Identity,
        integration: Option<&agent_run_domain::integration::NativeIntegration>,
    ) -> Result<()> {
        if let Some(evidence) = integration {
            evidence.validate()?;
            if evidence.session.is_some() {
                return Err(invalid("native session must be observed by the adapter"));
            }
        }
        let changed = self.conn.execute(
            "UPDATE attempts SET process_identity=?,process_birth_time=?,session_facts_json=? \
             WHERE id=? AND agent_id=? AND ownership_active=1 AND phase='spawning' AND process_identity IS NULL",
            params![leader.token, leader.birth, integration.map(serde_json::to_string).transpose()?, attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Records verified process-group and descendant cleanup for the exact
    /// owned attempt. An unknown cleanup cannot release its reservation.
    /// One autocommit statement uses the existing busy allowance and reports
    /// CleanupWrite/timing/count=1 on SQL refusal. Neither signals nor writes
    /// are replayed; callers preserve observed proof outside SQLite on outage.
    pub fn provider_cleanup(&self, id: &AgentId, attempt: &str, proof: &Cleanup) -> Result<()> {
        crate::persistence(
            agent_run_domain::error::PersistenceOperation::CleanupWrite,
            || {
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
            },
        )
    }

    /// Records the attempt's continuation evidence (`native_history` seal or
    /// `native_history_unavailable` reason) in its adapter state, only after
    /// its cleanup proof; the value is metadata, never credential material.
    pub fn provider_history(&self, id: &AgentId, attempt: &str, state: &Value) -> Result<()> {
        self.provider_history_with_cooldown(id, attempt, state, None, now())
    }

    /// Records cleanup-bound history and, for a verified ordered-binding failure,
    /// atomically records its one-hour cooldown and advances the capacity revision.
    /// `at` is the writer's observation time, in Unix seconds. Identical attempt
    /// evidence is idempotent and cannot extend a previously recorded cooldown.
    pub fn provider_history_with_cooldown(
        &self,
        id: &AgentId,
        attempt: &str,
        state: &Value,
        cooldown: Option<BindingCooldownReason>,
        at: f64,
    ) -> Result<()> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let (identity, account, previous): (Option<String>, Option<String>, String) = tx
            .query_row(
                "SELECT a.identity_json,t.selected_account_id,t.adapter_state_json \
             FROM agents a JOIN attempts t ON t.agent_id=a.id \
             WHERE a.id=?1 AND t.id=?2 AND t.phase='cleanup_complete'",
                params![id.as_str(), attempt],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        let previous: Value = serde_json::from_str(&previous)
            .map_err(|_| Error::Integrity("attempt history is malformed".into()))?;
        let old_reason = previous.get("binding_cooldown_recorded");
        if state.get("binding_cooldown_recorded").is_some() != cooldown.is_some() {
            return Err(invalid("binding cooldown evidence is malformed"));
        }
        if old_reason.is_some() && old_reason != state.get("binding_cooldown_recorded") {
            return Err(Error::Conflict);
        }
        if let Some(reason) = cooldown {
            if !at.is_finite() || at < 0.0 || !(at + BINDING_COOLDOWN_SECONDS).is_finite() {
                return Err(invalid("invalid binding cooldown time"));
            }
            if state["binding_cooldown_recorded"] != json!(reason) {
                return Err(invalid("binding cooldown evidence is malformed"));
            }
            let identity =
                identity.ok_or_else(|| Error::Integrity("provider identity is missing".into()))?;
            let identity: Value = serde_json::from_str(&identity)
                .map_err(|_| Error::Integrity("provider identity is malformed".into()))?;
            let authority: ResolvedLaunchAuthority =
                serde_json::from_value(identity["authority"].clone())
                    .map_err(|_| Error::Integrity("provider authority is malformed".into()))?;
            let account: AccountId = account
                .ok_or_else(|| invalid("attempt account is missing"))?
                .parse()?;
            let provider = &identity["provider_config"]["providers"][authority.provider.as_str()];
            let connection: ProviderConnection =
                serde_json::from_value(provider["connection"].clone())
                    .map_err(|_| Error::Integrity("provider connection is malformed".into()))?;
            let bindings: Vec<ProviderBinding> =
                serde_json::from_value(provider["bindings"].clone())
                    .map_err(|_| Error::Integrity("provider bindings are malformed".into()))?;
            let mut selected = bindings.iter().filter(|binding| {
                binding.account == account
                    && binding
                        .models
                        .as_ref()
                        .is_none_or(|models| models.contains(&authority.model))
            });
            let binding = selected
                .next()
                .ok_or_else(|| invalid("attempt binding is unavailable"))?;
            let native = &state["native_failure"];
            if selected.next().is_some()
                || identity["provider_identity_version"] != 2
                || provider["ordered_accounts"] != true
                || authority.harness != agent_run_domain::catalog::HarnessId::ClaudeCode
                || provider["harness"] != authority.harness.as_str()
                || connection != authority.connection
                || !authority.eligible_accounts.contains(&account)
                || native["class"] != reason.as_str()
                || native["attempt"] != attempt
                || native["account"] != account.as_str()
                || native["provider"] != authority.provider.as_str()
                || native["model"] != authority.model
                || (reason == BindingCooldownReason::BillingUnavailable
                    && !matches!(
                        binding.connection.as_ref().unwrap_or(&connection),
                        ProviderConnection::Custom { .. }
                    ))
                || (reason == BindingCooldownReason::QuotaExhausted
                    && (native["signal"] != "claude.rate_limit_event.rejected"
                        || !matches!(
                            native["window"].as_str(),
                            Some(
                                "five_hour"
                                    | "seven_day"
                                    | "seven_day_opus"
                                    | "seven_day_sonnet"
                                    | "seven_day_overage_included"
                                    | "overage"
                            )
                        )))
            {
                return Err(invalid(
                    "binding cooldown does not match the frozen attempt",
                ));
            }
            if old_reason.is_none() {
                let changed = tx.execute(
                    "INSERT INTO binding_cooldowns(provider_id,account_id,reason,observed_at,blocked_until,attempt_id) \
                     VALUES(?1,?2,?3,?4,?5,?6) ON CONFLICT(account_id,provider_id) DO UPDATE SET \
                     reason=excluded.reason,observed_at=excluded.observed_at,blocked_until=excluded.blocked_until,attempt_id=excluded.attempt_id \
                     WHERE excluded.blocked_until > binding_cooldowns.blocked_until",
                    params![authority.provider.as_str(), account.as_str(), reason.as_str(), at, at + BINDING_COOLDOWN_SECONDS, attempt],
                )?;
                if changed != 0 {
                    Store::advance_quota_capacity_revision(&tx)?;
                }
            }
        }
        let changed = tx.execute(
            "UPDATE attempts SET adapter_state_json=? \
             WHERE id=? AND agent_id=? AND phase='cleanup_complete'",
            params![serde_json::to_string(state)?, attempt, id.as_str()],
        )?;
        if changed != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(())
    }

    /// Marks a switched attempt's spawned child as running on an agent that is
    /// already running: records the new process group and the attempt state,
    /// without a status transition and without touching `started_at`, so the
    /// logical run's start (and any deadline derived from it) is preserved.
    pub fn provider_rerunning(&mut self, id: &AgentId, attempt: &str, pgid: i32) -> Result<()> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let agent = tx.execute(
            "UPDATE agents SET process_group_id=? WHERE id=? AND status='running'",
            params![pgid, id.as_str()],
        )?;
        let owned = tx.execute(
            "UPDATE attempts SET state='running' WHERE id=? AND agent_id=? AND ownership_active=1 \
             AND phase='spawning' AND process_identity IS NOT NULL",
            params![attempt, id.as_str()],
        )?;
        if agent != 1 || owned != 1 {
            return Err(Error::Conflict);
        }
        tx.commit()?;
        Ok(())
    }

    /// Returns the adapter state of an agent's latest attempt (`{}` when it
    /// recorded nothing).
    pub fn latest_attempt_state(&self, id: &AgentId) -> Result<String> {
        Ok(self.conn.query_row(
            "SELECT adapter_state_json FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [id.as_str()],
            |row| row.get(0),
        )?)
    }

    /// Certifies an attempt that remained prepared never spawned a child
    /// (see the internal `NEVER_SPAWNED` proof). This is only for cancellation or preparation
    /// failure before spawn; the sole writer synthesizes the same proof inside
    /// the terminal transaction instead (row 42).
    pub fn provider_never_spawned(&self, id: &AgentId) -> Result<bool> {
        let changed = self.conn.execute(
            &format!(
                "UPDATE attempts SET cleanup_proof_json='{{\"never_spawned\":true}}',phase='cleanup_complete' \
                 WHERE id IN (SELECT t.id FROM attempts t WHERE t.agent_id=? AND {NEVER_SPAWNED})"
            ),
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
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let session = admission::replay_session(&tx, effective)?;
        if (effective.orchestrator.is_none() || session.is_some())
            && let Some(found) =
                admission::replay_in_transaction(&tx, effective, session.as_deref())?
        {
            if found.parent_agent_id.as_ref() != resume.map(|resume| resume.parent) {
                return Err(Error::Conflict);
            }
            let replay = replayed(&tx, &found, request)?;
            tx.commit()?;
            return Ok(replay);
        }
        if let Some(restart) = resume.and_then(|resume| resume.restart_id) {
            crate::service_restart::require_restart_resume(
                &tx,
                restart,
                resume.expect("checked").parent,
            )?;
        } else {
            crate::service_restart::require_admissions_open(&tx)?;
        }
        let lineage = match resume {
            Some(resume) => {
                let lineage = lineage::resume_parent(&tx, resume.parent)?;
                if let Some(expected) = resume.preflight_digest
                    && resume_preflight_digest(&tx, resume.parent)? != expected
                {
                    return Err(Error::ExecutionChanged);
                }
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
                // Same rule as the legacy path: a home whose storage layout is
                // still prepared cannot gain a child, decided inside this
                // transaction from the parent's frozen identity.
                let parent_identity: Option<String> = tx
                    .query_row(
                        "SELECT identity_json FROM agents WHERE id=?1",
                        [resume.parent.as_str()],
                        |row| row.get(0),
                    )
                    .optional()?;
                if let Some(raw) = parent_identity {
                    let identity: Value = serde_json::from_str(&raw)
                        .map_err(|_| invalid("resume parent identity is malformed"))?;
                    crate::runtime_storage::refuse_prepared_resume_home(&tx, &identity)?;
                }
                Some(lineage)
            }
            None => None,
        };
        let inputs = AdmissionInputs {
            request,
            effective,
            authority,
            candidates,
            identity,
            global_cap,
            harness_cap,
            pinned,
        };
        let admission = admit_in_tx(
            &tx,
            catalog,
            AgentId::new(),
            &inputs,
            resume.zip(lineage.as_ref()),
        )?;
        if let Some(restart) = resume.and_then(|resume| resume.restart_id) {
            let changed = tx.execute(
                "UPDATE service_restart_agents SET resumed_execution_id=? WHERE restart_id=? AND agent_id=? AND resumed_execution_id IS NULL",
                params![admission.agent_id.as_str(), restart, resume.expect("checked").parent.as_str()],
            )?;
            if changed != 1 {
                return Err(Error::Conflict);
            }
        }
        Store::advance_quota_capacity_revision(&tx)?;
        check_quota_decision(candidates)?;
        tx.commit()?;
        Ok(admission)
    }
}

/// Borrowed, trusted inputs of one provider admission.
pub struct AdmissionInputs<'a> {
    /// Strict provider request exactly as frozen into the identity.
    pub request: &'a ProviderStartRequest,
    /// Effective storage request derived from it.
    pub effective: &'a StartRequest,
    /// Resolved launch authority.
    pub authority: &'a ResolvedLaunchAuthority,
    /// Trusted ranked candidates for the committed capacity revision.
    pub candidates: &'a QuotaCandidateSet,
    /// Frozen non-secret launch identity.
    pub identity: &'a Value,
    /// Global active-run cap.
    pub global_cap: usize,
    /// Optional per-harness active-run cap.
    pub harness_cap: Option<usize>,
    /// Account pinned by the request label, if any.
    pub pinned: Option<&'a AccountId>,
}

/// Ensures an ordered provider's priority and bindings came from the durable
/// configuration snapshot, not a different caller-supplied catalog. Historical
/// unordered admissions keep their existing acceptance path.
fn check_ordered_snapshot(identity: &Value, definition: &ProviderDefinition) -> Result<()> {
    let document = identity.get("provider_config");
    let frozen = document
        .and_then(|document| document.get("providers"))
        .and_then(|providers| providers.get(definition.id.as_str()));
    let ordered = match frozen.and_then(|provider| provider.get("ordered_accounts")) {
        Some(Value::Bool(ordered)) => *ordered,
        None => false,
        Some(_) => {
            return Err(Error::Integrity(
                "frozen account ordering is malformed".into(),
            ));
        }
    };
    if !ordered && !definition.ordered_accounts {
        return Ok(());
    }
    let document =
        document.ok_or_else(|| Error::Integrity("frozen provider config is missing".into()))?;
    if !ordered
        || !definition.ordered_accounts
        || agent_run_config::provider_config::ProviderConfig::snapshot_document(document)
            .map_err(|_| Error::Integrity("frozen provider config is malformed".into()))?
            != identity["provider_config_snapshot"]
    {
        return Err(Error::Integrity("frozen provider ordering changed".into()));
    }
    let mut offered = serde_json::to_value(definition)?;
    offered
        .as_object_mut()
        .ok_or_else(|| Error::Integrity("provider definition is malformed".into()))?
        .remove("id");
    if frozen != Some(&offered) {
        return Err(Error::Integrity(
            "provider differs from frozen ordering".into(),
        ));
    }
    Ok(())
}

/// Writes one agent admission inside the caller's immediate transaction.
///
/// `id` is injected so a batch can reserve every stable identity before any
/// row exists. The inputs are re-verified against each other, the candidate
/// revision against the committed one, and the global and harness caps and
/// reservations against rows already written in this transaction, so earlier
/// batch members count. The caller commits and advances the quota capacity
/// revision exactly once; this function does neither. External quota evidence
/// is clock-fenced after the local revision check and before any selection/write.
pub(crate) fn admit_in_tx(
    tx: &rusqlite::Connection,
    catalog: &ProviderCatalog,
    id: AgentId,
    inputs: &AdmissionInputs<'_>,
    lineage: Option<(ProviderResume<'_>, &lineage::ResumeLineage)>,
) -> Result<ProviderAdmission> {
    let AdmissionInputs {
        request,
        effective,
        authority,
        candidates,
        identity,
        global_cap,
        harness_cap,
        pinned,
    } = *inputs;
    let resume = lineage.map(|(resume, _)| resume);
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
        || effective.display_name != request.display_name
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
        || pinned.is_some_and(
            |account| !matches!(&candidates.intent, SelectionIntent::Pinned(id) if id == account),
        )
    {
        return Err(invalid("provider admission inputs disagree"));
    }
    check_ordered_snapshot(identity, definition)?;
    let revision = Store::quota_capacity_revision_in(tx)?;
    if revision != candidates.capacity_revision {
        return Err(QuotaAdmissionError::SelectionStale {
            committed_capacity_revision: candidates.capacity_revision,
            current_capacity_revision: revision,
        }
        .into());
    }
    check_quota_decision(candidates)?;
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
    let candidate = choose(
        tx,
        catalog,
        &request.provider,
        &request.model,
        authority,
        candidates,
        pinned,
        resume.map(|resume| resume.prefer),
        &std::collections::BTreeSet::new(),
    )?;
    let at = now();
    candidates.check_quota_decision(at)?;
    let session = admission::upsert_session(tx, effective, at)?;
    let attempt_id = format!("att_{}", uuid::Uuid::new_v4().simple());
    let summary: String = request
        .task
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .chars()
        .take(160)
        .collect();
    let (root, sequence, resumed) = match lineage {
        Some((_, lineage)) => (
            lineage.root_agent_id.clone(),
            lineage.sequence,
            Some(lineage.runtime_session_id.as_str()),
        ),
        None => (id.clone(), 1, None),
    };
    let inserted = tx.execute(
            "INSERT INTO agents(id,request_id,orchestrator_session_id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,parent_agent_id,root_agent_id,sequence,resume_of_runtime_session_id,identity_json,selection_intent,requested_account_id,display_name) \
             VALUES(?,?,?,?,?,?,?,?,?,?,'starting',?,'pending:provider-v2',?,?,?,?,?,?,?,?)",
            params![id.as_str(), request.request_id, session, request.provider.as_str(),
                request.model, request.profile, request.task, summary,
                effective.workdir.to_string_lossy(), serde_json::to_string(effective)?,
                at,
                resume.map(|resume| resume.parent.as_str()), root.as_str(), sequence, resumed,
                serde_json::to_string(identity)?,
                if pinned.is_some() { "pinned" } else { "auto" },
                pinned.map(AccountId::as_str),
                effective.display_name],
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
    let owner = admission_owner()?;
    tx.execute(
        "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
        params![serde_json::to_string(&owner)?, owner.birth, id.as_str()],
    )?;
    tx.execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id,phase,ownership_active) \
             VALUES(?,?,1,'prepared','{}',?,?,'prepared',1)",
            params![attempt_id, id.as_str(), at, candidate.account.as_str()],
        )?;
    if identity
        .pointer("/provider_config/services")
        .and_then(Value::as_object)
        .is_some_and(|services| !services.is_empty())
    {
        tx.execute(
            "INSERT INTO agent_service_gates(agent_id,state) VALUES (?,'pending')",
            [id.as_str()],
        )?;
    }
    for key in &candidate.physical_keys {
        tx.execute(
            "INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,?)",
            params![attempt_id, key.as_str()],
        )?;
    }
    tx_event(tx, &id, "created", None, Some(Status::Created), &json!({}))?;
    tx_event(
        tx,
        &id,
        "start_accepted",
        Some(Status::Created),
        Some(Status::Starting),
        &quota_decision_metadata(
            match resume {
                Some(resume) => json!({"durable_admission":true,"resume_of":resume.parent}),
                None => json!({"durable_admission":true}),
            },
            candidates,
        ),
    )?;
    Ok(ProviderAdmission {
        agent_id: id,
        attempt_id,
        account_id: candidate.account.clone(),
        created: true,
    })
}

/// Rechecks optional external evidence at a transaction's current wall-clock
/// boundary; callers must roll back staged mutations when this refuses.
pub(crate) fn check_quota_decision(candidates: &QuotaCandidateSet) -> Result<()> {
    candidates.check_quota_decision(now())
}

/// Adds only the bounded source reference to an existing admission event.
/// The validated deadline and clock policy stay transient, not stored quota state.
fn quota_decision_metadata(mut metadata: Value, candidates: &QuotaCandidateSet) -> Value {
    if let Some(decision) = &candidates.quota_decision {
        metadata["quota_decision"] = json!({
            "source_epoch": decision.source_epoch,
            "revision": decision.revision,
            "config_revision": decision.config_revision,
            "evaluated_at": decision.evaluated_at,
        });
    }
    metadata
}

/// Chooses one candidate inside an admission or allocation transaction.
///
/// A candidate qualifies only when it matches `pinned` (if any), lies in the
/// frozen `authority` scope, is not in `exclude` (accounts already tried by
/// this logical agent), can lease `provider`/`model` from `catalog`, and its
/// committed registry row is enabled with the catalog's family and storage
/// reference. `prefer` keeps a resume's still-valid parent account. Next,
/// ordered providers use frozen binding order; other providers use candidate
/// rank, active load and account id.
#[allow(clippy::too_many_arguments)]
fn choose<'a>(
    tx: &rusqlite::Connection,
    catalog: &ProviderCatalog,
    provider: &agent_run_domain::catalog::ProviderId,
    model: &str,
    authority: &ResolvedLaunchAuthority,
    candidates: &'a QuotaCandidateSet,
    pinned: Option<&AccountId>,
    prefer: Option<&AccountId>,
    exclude: &std::collections::BTreeSet<AccountId>,
) -> Result<&'a agent_run_domain::catalog::QuotaCandidate> {
    let definition =
        catalog
            .provider(provider)
            .ok_or_else(|| QuotaAdmissionError::NoEligibleAccount {
                provider: provider.clone(),
                model: model.to_owned(),
            })?;
    let ordered = definition.ordered_accounts.then(|| {
        definition
            .ordered_bindings(model)
            .iter()
            .enumerate()
            .map(|(position, binding)| (binding.account.clone(), position))
            .collect::<std::collections::BTreeMap<_, _>>()
    });
    let mut chosen = None;
    let mut blocked = None;
    let at = now();
    for candidate in &candidates.candidates {
        if pinned.is_some_and(|account| &candidate.account != account)
            || exclude.contains(&candidate.account)
            || !authority.eligible_accounts.contains(&candidate.account)
            || AttemptCredentials::from_selected(catalog, provider, model, &candidate.account)
                .is_err()
        {
            continue;
        }
        let current: Option<(String, String, String)> = tx
            .query_row(
                "SELECT auth_family,secret_ref,status FROM provider_accounts WHERE account_id=?",
                [candidate.account.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
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
        if let Some((reason, retry_after_seconds)) =
            binding_cooldown(tx, provider, &candidate.account, at)?
        {
            if blocked
                .as_ref()
                .is_none_or(|(_, _, seconds)| retry_after_seconds < *seconds)
            {
                blocked = Some((candidate.account.clone(), reason, retry_after_seconds));
            }
            continue;
        }
        let active = Store::active_attempt_counts_in(tx, std::slice::from_ref(&candidate.account))?
            [candidate.account.as_str()];
        let reserved = Store::active_reservation_counts_in(tx, &candidate.physical_keys)?;
        let load = active.max(reserved.values().copied().max().unwrap_or(0));
        let kept = prefer.is_none_or(|prefer| &candidate.account != prefer);
        let rank = match &ordered {
            Some(order) => match order.get(&candidate.account) {
                Some(rank) => *rank,
                None => continue,
            },
            None => usize::try_from(candidate.rank)
                .map_err(|_| invalid("candidate rank exceeds platform size"))?,
        };
        let key = (kept, rank, load, candidate.account.as_str());
        if chosen.as_ref().is_none_or(|(best, _)| key < *best) {
            chosen = Some((key, candidate));
        }
    }
    chosen
        .map(|(_, candidate)| candidate)
        .ok_or_else(|| match blocked {
            Some((account, reason, retry_after_seconds)) => QuotaAdmissionError::BindingCooldown {
                provider: provider.clone(),
                model: model.to_owned(),
                account,
                reason,
                retry_after_seconds,
            }
            .into(),
            None => QuotaAdmissionError::NoEligibleAccount {
                provider: provider.clone(),
                model: model.to_owned(),
            }
            .into(),
        })
}

/// Reads an active provider/account cooldown from the caller's transaction.
pub fn binding_cooldown(
    conn: &rusqlite::Connection,
    provider: &agent_run_domain::catalog::ProviderId,
    account: &AccountId,
    at: f64,
) -> Result<Option<(BindingCooldownReason, u64)>> {
    if !at.is_finite() || at < 0.0 {
        return Err(invalid("invalid binding cooldown time"));
    }
    let row: Option<(String, f64)> = conn.query_row(
        "SELECT reason,blocked_until FROM binding_cooldowns WHERE provider_id=?1 AND account_id=?2 AND blocked_until>?3",
        params![provider.as_str(), account.as_str(), at], |row| Ok((row.get(0)?, row.get(1)?)),
    ).optional()?;
    row.map(|(value, until)| {
        if !until.is_finite() {
            return Err(Error::Integrity("binding cooldown is malformed".into()));
        }
        let reason = serde_json::from_value(json!(value))
            .map_err(|_| Error::Integrity("binding cooldown is malformed".into()))?;
        Ok((reason, ((until - at).ceil() as u64).max(1)))
    })
    .transpose()
}

/// The next attempt allocated on an existing logical agent.
#[derive(Debug, Clone)]
pub struct NextAttempt {
    /// New owned attempt id.
    pub attempt_id: String,
    /// Its one-based number within the logical agent.
    pub number: u32,
    /// Global physical account selected for it.
    pub account_id: AccountId,
    /// The previous attempt whose ownership and reservations were released.
    pub released: String,
}

impl Store<'_> {
    /// Atomically replaces a logical agent's cleaned-up attempt with the next
    /// one, in one immediate transaction; explicit public resume stays a
    /// separate new logical child.
    ///
    /// Requires: the agent is a nonterminal provider run with no pending or
    /// claimed cancel; its latest attempt is the only owned one, has verified
    /// cleanup proof, an authoritative quota failure (or billing failure on an
    /// ordered Claude custom binding), and a native history seal whose `task_proven` shows
    /// this logical request's admitted turn reached native history (the
    /// continuation evidence); the selection intent is automatic (a pinned run never
    /// switches); `candidates` are for the agent's frozen provider and model
    /// at the current capacity revision. The chosen account must be in the
    /// frozen scope, enabled now, and not tried by any earlier attempt of the
    /// agent. The agent keeps its already-owned global and harness slot, so
    /// no cap is re-checked. The previous attempt's ownership (and with it
    /// its physical-key reservations) is released and the new attempt, its
    /// keys and an `attempt_allocated` event are written exactly once; the
    /// unique owned-attempt invariant still holds under concurrent callers,
    /// and a losing, held or cancelled caller gets an error with nothing written.
    /// `candidates` must come from trusted internal ranking, never a caller.
    /// External quota evidence is clock-fenced independently of local revision
    /// contention before changing the previous or next attempt.
    pub fn allocate_next_attempt(
        &mut self,
        id: &AgentId,
        catalog: &ProviderCatalog,
        candidates: &QuotaCandidateSet,
    ) -> Result<NextAttempt> {
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        crate::lineage_holds::require_unheld(&tx, id)?;
        let record = tx.query_row(
            "SELECT * FROM agents WHERE id=?",
            [id.as_str()],
            Record::read,
        )?;
        if record.status.terminal() {
            return Err(invalid("a terminal agent cannot take another attempt"));
        }
        let cancelled: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM commands WHERE agent_id=? AND kind='cancel' AND state IN ('pending','claimed'))",
            [id.as_str()],
            |row| row.get(0),
        )?;
        if cancelled {
            return Err(invalid("a cancelled agent cannot take another attempt"));
        }
        let identity = record
            .identity
            .as_ref()
            .filter(|identity| identity["provider_identity_version"] == 2)
            .ok_or_else(|| invalid("only provider runs take another attempt"))?;
        let authority: ResolvedLaunchAuthority =
            serde_json::from_value(identity["authority"].clone())
                .map_err(|_| Error::Integrity("stored provider authority is malformed".into()))?;
        let intent: String = tx.query_row(
            "SELECT selection_intent FROM agents WHERE id=?",
            [id.as_str()],
            |row| row.get(0),
        )?;
        if intent != "auto" {
            return Err(QuotaAdmissionError::NoEligibleAccount {
                provider: authority.provider.clone(),
                model: authority.model.clone(),
            }
            .into());
        }
        let owned: i64 = tx.query_row(
            "SELECT COUNT(*) FROM attempts WHERE agent_id=? AND ownership_active=1",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let (previous, number, phase, proof, state, active, previous_account): (
            String,
            u32,
            Option<String>,
            Option<String>,
            String,
            i64,
            Option<String>,
        ) = tx.query_row(
            "SELECT id,number,phase,cleanup_proof_json,adapter_state_json,ownership_active,selected_account_id \
                 FROM attempts WHERE agent_id=? ORDER BY number DESC LIMIT 1",
            [id.as_str()],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )?;
        if owned != 1 || active != 1 {
            return Err(Error::Conflict);
        }
        if phase.as_deref() != Some("cleanup_complete") || proof.is_none() {
            return Err(invalid("the previous attempt has no verified cleanup"));
        }
        let evidence: Value = serde_json::from_str(&state).unwrap_or(Value::Null);
        let previous_state = match evidence["native_failure"]["class"].as_str() {
            Some("quota_exhausted") => "exhausted",
            Some("billing_unavailable") => {
                let definition = catalog
                    .provider(&authority.provider)
                    .ok_or_else(|| invalid("ordered billing fallback has no frozen provider"))?;
                check_ordered_snapshot(identity, definition)?;
                let previous_account: AccountId = previous_account
                    .as_deref()
                    .ok_or_else(|| Error::Integrity("attempt account is missing".into()))?
                    .parse()?;
                if !definition.ordered_accounts
                    || authority.harness != agent_run_domain::catalog::HarnessId::ClaudeCode
                    || definition.harness != authority.harness
                    || definition.connection != authority.connection
                    || !authority.eligible_accounts.contains(&previous_account)
                    || !matches!(
                        definition.connection_for(&previous_account, &authority.model),
                        Some(agent_run_domain::catalog::ProviderConnection::Custom { .. })
                    )
                    || evidence["native_failure"]["attempt"].as_str() != Some(previous.as_str())
                    || evidence["native_failure"]["account"].as_str()
                        != Some(previous_account.as_str())
                    || evidence["native_failure"]["provider"].as_str()
                        != Some(authority.provider.as_str())
                    || evidence["native_failure"]["model"].as_str()
                        != Some(authority.model.as_str())
                {
                    return Err(invalid(
                        "billing fallback does not match the frozen attempt",
                    ));
                }
                "failed"
            }
            _ => {
                return Err(invalid(
                    "only a recorded quota or eligible billing failure allows another attempt",
                ));
            }
        };
        if evidence["native_history"]["seal"].is_null() {
            return Err(Error::Unsupported(
                "continuation_unavailable: the previous attempt recorded no native history seal"
                    .into(),
            ));
        }
        // The sealed history must also prove this logical request's admitted
        // task reached the native session; a meta-only or early-rejected
        // history would otherwise continue a conversation that never began.
        if evidence["native_history"]["task_proven"] != true {
            return Err(Error::Unsupported(
                "continuation_unavailable: the admitted task never reached native history".into(),
            ));
        }
        candidates.validate()?;
        if candidates.provider != authority.provider
            || candidates.model != authority.model
            || candidates.intent != SelectionIntent::Auto
        {
            return Err(invalid(
                "next-attempt inputs disagree with the frozen authority",
            ));
        }
        let revision = Store::quota_capacity_revision_in(&tx)?;
        if revision != candidates.capacity_revision {
            return Err(QuotaAdmissionError::SelectionStale {
                committed_capacity_revision: candidates.capacity_revision,
                current_capacity_revision: revision,
            }
            .into());
        }
        check_quota_decision(candidates)?;
        let definition = catalog.provider(&authority.provider).ok_or_else(|| {
            QuotaAdmissionError::NoEligibleAccount {
                provider: authority.provider.clone(),
                model: authority.model.clone(),
            }
        })?;
        check_ordered_snapshot(identity, definition)?;
        if definition.ordered_accounts
            && (authority.harness != definition.harness
                || authority.connection != definition.connection
                || !definition
                    .models
                    .iter()
                    .any(|model| model.id == authority.model))
        {
            return Err(Error::Integrity(
                "ordered provider authority changed".into(),
            ));
        }
        let tried: std::collections::BTreeSet<AccountId> = tx
            .prepare("SELECT selected_account_id FROM attempts WHERE agent_id=? AND selected_account_id IS NOT NULL")?
            .query_map([id.as_str()], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .into_iter()
            .map(|account| account.parse())
            .collect::<Result<_>>()?;
        let candidate = choose(
            &tx,
            catalog,
            &authority.provider,
            &authority.model,
            &authority,
            candidates,
            None,
            None,
            &tried,
        )?;
        check_quota_decision(candidates)?;
        // Close the failed attempt durably and release its ownership (and
        // with it its physical-key reservations) in the same transaction.
        let released = tx.execute(
            "UPDATE attempts SET ownership_active=0,state=?,finished_at=? \
             WHERE id=? AND agent_id=? AND ownership_active=1",
            params![previous_state, now(), previous, id.as_str()],
        )?;
        if released != 1 {
            return Err(Error::Conflict);
        }
        let attempt_id = format!("att_{}", uuid::Uuid::new_v4().simple());
        let number = number + 1;
        tx.execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,selected_account_id,phase,ownership_active) \
             VALUES(?,?,?,'prepared','{}',?,?,'prepared',1)",
            params![attempt_id, id.as_str(), number, now(), candidate.account.as_str()],
        )?;
        for key in &candidate.physical_keys {
            tx.execute(
                "INSERT INTO attempt_quota_keys(attempt_id,quota_key) VALUES(?,?)",
                params![attempt_id, key.as_str()],
            )?;
        }
        Store::advance_quota_capacity_revision(&tx)?;
        tx_event(
            &tx,
            id,
            "attempt_allocated",
            None,
            None,
            &quota_decision_metadata(
                json!({"previous":previous,"number":number,"account":candidate.account}),
                candidates,
            ),
        )?;
        check_quota_decision(candidates)?;
        tx.commit()?;
        Ok(NextAttempt {
            attempt_id,
            number,
            account_id: candidate.account.clone(),
            released: previous,
        })
    }
}
