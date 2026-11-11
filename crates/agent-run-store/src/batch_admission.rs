//! One atomic admission and permanent replay receipt for independent jobs.

use crate::{
    Store,
    provider_admission::{AdmissionInputs, admit_in_tx, check_quota_decision},
};
use agent_run_domain::{
    Error, Result, StoreId,
    batch::{
        BatchJobReceipt, BatchPrecondition, ExecutionRef, FINGERPRINT_VERSION, MAX_BATCH_JOBS,
        PinId, StartBatchReceipt, StartBatchResult, validate_preconditions,
    },
    catalog::ProviderCatalog,
    domain::{AgentId, external_id},
    error::invalid,
    storage::{MAX_RESULT_BYTES, encoded_len},
};
use rusqlite::{Connection, OptionalExtension, params};

/// Trusted prepared inputs for one independent batch member.
pub struct BatchMember<'a> {
    /// Pre-minted public identity.
    pub id: AgentId,
    /// The same trusted inputs used by a single provider admission.
    pub inputs: AdmissionInputs<'a>,
}

/// Broker-prepared intent and candidates, never supplied by wire clients.
pub struct BatchAdmission<'a> {
    /// Globally scoped batch key in this Run home.
    pub request_id: &'a str,
    /// Versioned normalized intent digest.
    pub fingerprint: &'a str,
    /// Whether each newly admitted lineage receives a permanent pin.
    pub retain_history: bool,
    /// Optional scope rechecked under the admission writer transaction.
    pub expected_store_id: Option<&'a StoreId>,
    /// Exact ordered lineage prerequisites, checked under this transaction.
    pub preconditions: Option<&'a [BatchPrecondition]>,
    /// One frozen provider catalog shared by all jobs.
    pub catalog: &'a ProviderCatalog,
    /// Ordered, pre-minted jobs.
    pub jobs: Vec<BatchMember<'a>>,
}

/// Checks all prerequisite lineages against the caller's writer snapshot.
fn require_preconditions(
    conn: &Connection,
    store_id: &StoreId,
    conditions: &[BatchPrecondition],
) -> Result<()> {
    use agent_run_domain::{
        PreconditionFailureReason,
        status::{
            CleanupState, Coverage, PinState, RestartResume, StatusMatch, StatusRequest,
            StatusSelector,
        },
    };
    validate_preconditions(conditions)?;
    let request = StatusRequest {
        version: 1,
        expected_store_id: Some(store_id.clone()),
        targets: conditions
            .iter()
            .map(|condition| StatusSelector {
                agent_id: condition.agent_id.clone(),
                sequence: condition.sequence,
                execution_ref: Some(condition.execution_ref.clone()),
                pin_id: Some(condition.pin_id.clone()),
            })
            .collect(),
        include_view: false,
    };
    let snapshot = crate::status::read_in_snapshot(conn, &request)?;
    let mut roots = std::collections::HashSet::new();
    for item in &snapshot.response.items {
        if let Some(root) = &item.canonical_agent_id
            && !roots.insert(root)
        {
            return Err(invalid("duplicate canonical batch precondition"));
        }
    }
    let mut unknown = false;
    let mut not_met = false;
    for item in &snapshot.response.items {
        let same_execution = matches!((&item.selected, &item.retained_latest),
            (Some(selected), Some(latest))
                if selected.sequence == latest.sequence
                    && selected.execution_ref.is_some()
                    && selected.execution_ref == latest.execution_ref);
        let satisfied = item.r#match == StatusMatch::Verified
            && item.pin_state == PinState::Active
            && item.coverage == Coverage::Complete
            && same_execution
            && item.restart_resume == RestartResume::None
            && item.lineage_terminal == Some(true)
            && item.cleanup.scope == "lineage"
            && item.cleanup.state == CleanupState::Confirmed;
        if satisfied {
            continue;
        }
        let proven_false = item.r#match == StatusMatch::Mismatch
            || matches!(item.pin_state, PinState::Released | PinState::Mismatch)
            || item.restart_resume == RestartResume::Pending
            || item.lineage_terminal == Some(false)
            || item.cleanup.state == CleanupState::Pending
            || (item.r#match == StatusMatch::Verified
                && item.coverage == Coverage::Complete
                && item.selected.is_some()
                && item.retained_latest.is_some()
                && !same_execution);
        not_met |= proven_false;
        unknown |= !proven_false;
    }
    if not_met || unknown {
        return Err(Error::PreconditionFailed {
            reason: if not_met {
                PreconditionFailureReason::NotMet
            } else {
                PreconditionFailureReason::Unknown
            },
        });
    }
    Ok(())
}

/// Reads and validates a permanent receipt, refusing conflicts and unknown codecs.
fn replay(
    conn: &Connection,
    request_id: &str,
    fingerprint: &str,
    expected: Option<&StoreId>,
) -> Result<(StoreId, Option<StartBatchResult>)> {
    // One SQL statement binds the identity and receipt to the same read snapshot.
    type ScopedReceiptRow = (String, Option<u32>, Option<String>, Option<String>);
    let row: Option<ScopedReceiptRow> = conn.query_row(
        "SELECT s.store_id,r.fingerprint_version,r.fingerprint,r.receipt_json \
         FROM store_identity s LEFT JOIN start_batch_receipts r ON r.request_id=? WHERE s.singleton=1",
        [request_id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
    ).optional()?;
    let (raw_id, version, stored, raw) =
        row.ok_or_else(|| Error::Integrity("store identity is missing".into()))?;
    let store_id: StoreId = raw_id
        .parse()
        .map_err(|_| Error::Integrity("store identity is invalid".into()))?;
    if expected.is_some_and(|expected| expected != &store_id) {
        return Err(Error::StoreIdentityMismatch);
    }
    let (version, stored, raw) = match (version, stored, raw) {
        (None, None, None) => return Ok((store_id, None)),
        (Some(version), Some(stored), Some(raw)) => (version, stored, raw),
        _ => return Err(Error::Integrity("invalid batch receipt".into())),
    };
    if version != FINGERPRINT_VERSION {
        return Err(Error::Unsupported(
            "unknown batch fingerprint version".into(),
        ));
    }
    if stored != fingerprint {
        return Err(Error::Conflict);
    }
    let receipt: StartBatchReceipt =
        serde_json::from_str(&raw).map_err(|_| Error::Integrity("invalid batch receipt".into()))?;
    if receipt.validate().is_err()
        || receipt.request_id != request_id
        || receipt.store_id != store_id
    {
        return Err(Error::Integrity("invalid batch receipt".into()));
    }
    Ok((
        store_id,
        Some(StartBatchResult {
            receipt,
            replayed: true,
        }),
    ))
}

impl Store<'_> {
    /// Resolves an exact batch replay without mutable configuration or agent rows.
    pub fn replay_batch(
        &self,
        request_id: &str,
        fingerprint: &str,
    ) -> Result<Option<StartBatchResult>> {
        self.replay_batch_scoped(request_id, fingerprint, None)
    }

    /// Resolves replay and optional database scope in one read snapshot.
    pub fn replay_batch_scoped(
        &self,
        request_id: &str,
        fingerprint: &str,
        expected: Option<&StoreId>,
    ) -> Result<Option<StartBatchResult>> {
        replay(&self.conn, request_id, fingerprint, expected).map(|(_, result)| result)
    }

    /// Commits every ordinary admission and its permanent receipt, or none.
    /// Replays run before restart/capacity checks and never authorize a launch.
    /// Every new member's quota clock fence is rechecked before the shared commit.
    pub fn admit_batch(&self, batch: BatchAdmission<'_>) -> Result<StartBatchResult> {
        external_id("request_id", batch.request_id)?;
        if !(1..=MAX_BATCH_JOBS).contains(&batch.jobs.len())
            || batch
                .jobs
                .iter()
                .any(|job| job.inputs.request.request_id.is_some())
            || batch.fingerprint.len() != 64
            || !batch
                .fingerprint
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(invalid("invalid batch admission"));
        }
        if batch.preconditions.is_some() && batch.expected_store_id.is_none() {
            return Err(invalid("batch preconditions require expected_store_id"));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let (store_id, replayed) = replay(
            &tx,
            batch.request_id,
            batch.fingerprint,
            batch.expected_store_id,
        )?;
        if let Some(result) = replayed {
            if result.receipt.preconditions.as_deref() != batch.preconditions {
                return Err(Error::Integrity(
                    "invalid batch receipt preconditions".into(),
                ));
            }
            tx.commit()?;
            return Ok(result);
        }
        if let Some(conditions) = batch.preconditions {
            require_preconditions(&tx, &store_id, conditions)?;
        }
        crate::service_restart::require_admissions_open(&tx)?;
        let mut jobs = Vec::with_capacity(batch.jobs.len());
        for (index, job) in batch.jobs.iter().enumerate() {
            // This admission creates the exact execution row and its number-one
            // provider attempt together; never derive identity from a later attempt.
            let admitted = admit_in_tx(&tx, batch.catalog, job.id.clone(), &job.inputs, None)?;
            if !admitted.created || admitted.agent_id != job.id {
                return Err(Error::Integrity("invalid batch admission identity".into()));
            }
            let execution_ref =
                ExecutionRef::from_first_provider_attempt(&admitted.agent_id, &admitted.attempt_id)
                    .map_err(|_| {
                        Error::Integrity("invalid first provider attempt identity".into())
                    })?;
            let pin_id = if batch.retain_history {
                let pin_id = PinId::new();
                tx.execute(
                    "INSERT INTO history_pins(pin_id,root_agent_id,released) VALUES(?,?,0)",
                    params![pin_id.as_str(), admitted.agent_id.as_str()],
                )?;
                Some(pin_id)
            } else {
                None
            };
            jobs.push(BatchJobReceipt {
                index,
                agent_id: admitted.agent_id,
                sequence: 1,
                execution_ref,
                pin_id,
            });
        }
        let output = StartBatchResult {
            receipt: StartBatchReceipt {
                version: 1,
                store_id,
                request_id: batch.request_id.to_owned(),
                jobs,
                preconditions: batch.preconditions.map(<[_]>::to_vec),
            },
            replayed: false,
        };
        output
            .receipt
            .validate()
            .map_err(|_| Error::Integrity("invalid batch receipt".into()))?;
        if encoded_len(&serde_json::to_value(&output)?) > MAX_RESULT_BYTES {
            return Err(invalid("batch receipt exceeds writer result limit"));
        }
        Store::advance_quota_capacity_revision(&tx)?;
        tx.execute(
            "INSERT INTO start_batch_receipts(request_id,fingerprint_version,fingerprint,receipt_json) VALUES(?,?,?,?)",
            params![batch.request_id, FINGERPRINT_VERSION, batch.fingerprint, serde_json::to_string(&output.receipt)?],
        )?;
        for job in &batch.jobs {
            check_quota_decision(job.inputs.candidates)?;
        }
        tx.commit()?;
        Ok(output)
    }
}
