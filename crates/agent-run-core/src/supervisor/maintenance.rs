//! Nonterminal finalization of an already-closed maintenance attempt.

use super::*;
use agent_run_platform::process::Cleanup;

/// The exact attempt and immutable launch inputs needed after native closing.
pub(super) struct PauseContext<'a> {
    pub id: &'a AgentId,
    pub attempt: &'a str,
    pub epoch: i64,
    pub native_event_seq: i64,
    pub agent_dir: &'a Path,
    pub identity: &'a ProviderLaunchIdentity,
    pub environment: &'a BTreeMap<String, String>,
    pub continuation_root: Option<&'a Path>,
}

/// Only fully observed process cleanup can make a pause resumable.
fn confirmed(proof: &Cleanup) -> bool {
    proof.confirmed
        && proof.group_gone
        && proof.descendants_gone == Some(true)
        && proof.scope == "verified_descendants"
}

/// Check cleanup against the same stored attempt and the supervisor's owned
/// process generation, not only booleans from a possibly stale receipt.
fn cleanup_matches(
    context: &PauseContext<'_>,
    store: &Persist,
    process: &Process,
    proof: &Cleanup,
) -> bool {
    if !confirmed(proof) {
        return false;
    }
    let Some(leader) = process.owner.leader.as_ref() else {
        return false;
    };
    let Ok(row) = store.get(context.id) else {
        return false;
    };
    let Some(group) = row.process_group_id.filter(|group| *group > 1) else {
        return false;
    };
    let attempt: Result<(Option<String>, Option<f64>)> = store.conn.query_row(
        "SELECT process_identity,process_birth_time FROM attempts WHERE id=? AND agent_id=? AND ownership_active=1",
        rusqlite::params![context.attempt, context.id.as_str()], |row| Ok((row.get(0)?,row.get(1)?)),
    ).map_err(Into::into);
    attempt.is_ok_and(|(token, birth)| {
        token.as_deref() == Some(leader.token.as_str())
            && birth == Some(leader.birth)
            && leader.group == group
            && proof.process_group_id == Some(group)
    })
}

/// Retain a content-free recovery receipt even when cleanup cannot be proven.
fn retain_pause_evidence(
    context: &PauseContext<'_>,
    process: &Process,
    cleanup: &Result<Cleanup>,
    exit: Option<i32>,
    cancellation: bool,
) -> Result<()> {
    let cleanup = match cleanup {
        Ok(proof) => serde_json::to_value(proof)?,
        Err(error) => json!({"confirmed":false,"diagnostic":execution_error_metadata(error)}),
    };
    let data = json!({
        "version":1,"attempt":context.attempt,"recorded_at":domain::now(),
        "terminal_commit":"not_proven_by_receipt","ownership":process.owner.snapshot(),
        "cleanup":cleanup,"engine_result_available":false,"native_status":null,
        "candidate":null,"candidate_diagnostic":null,"exit_code":exit,"diagnostic":null,
        "maintenance":{"epoch":context.epoch,"native_event_seq":context.native_event_seq}
    });
    let directory = fs::Dir::open(context.agent_dir)?;
    let path = PathBuf::from(if cancellation {
        format!("recovery.{}.maintenance-cancel.json", context.attempt)
    } else {
        format!("recovery.{}.json", context.attempt)
    });
    directory.write(
        &path,
        &fs::canonical_json(&data)?,
        if context.identity.system_route.is_some() {
            0o660
        } else {
            0o600
        },
    )?;
    Ok(())
}

/// Whether cancellation can safely submit a new cleanup proof, or only observe
/// an earlier write whose outcome may be unknown.
enum CleanupRecovery {
    ObserveOnly,
    RetryOnCancel,
    ProvenNotSubmitted { proof: Cleanup, exit: Option<i32> },
}

struct HoldState {
    reason: &'static str,
    cleanup: CleanupRecovery,
    cancellation_submitted: bool,
}

impl HoldState {
    fn observe(reason: &'static str) -> Self {
        Self {
            reason,
            cleanup: CleanupRecovery::ObserveOnly,
            cancellation_submitted: false,
        }
    }
    fn unverified(reason: &'static str) -> Self {
        Self {
            reason,
            cleanup: CleanupRecovery::RetryOnCancel,
            cancellation_submitted: false,
        }
    }
    fn proven(reason: &'static str, proof: Cleanup, exit: Option<i32>) -> Self {
        Self {
            reason,
            cleanup: CleanupRecovery::ProvenNotSubmitted { proof, exit },
            cancellation_submitted: false,
        }
    }
    fn cancellation_unknown() -> Self {
        Self {
            reason: "cancellation_unconfirmed",
            cleanup: CleanupRecovery::ObserveOnly,
            cancellation_submitted: true,
        }
    }
}

/// Retain the owned process and the nonterminal task after an uncertain
/// finalization. A later explicit terminal resolution may release the hold;
/// no native operation, signal or uncertain storage write is retried here.
pub(super) async fn hold_closed(
    context: &PauseContext<'_>,
    store: &mut Persist,
    process: &mut Process,
    reason: &'static str,
) -> Result<()> {
    hold_with(context, store, process, HoldState::observe(reason)).await
}

/// Keep an uncertain continuation owned until it is resolved or explicitly
/// cancelled. Only that cancellation authorizes one cleanup attempt.
pub(super) async fn hold_unconfirmed(
    context: &PauseContext<'_>,
    store: &mut Persist,
    process: &mut Process,
) -> Result<()> {
    hold_with(
        context,
        store,
        process,
        HoldState::unverified("continuation_unconfirmed"),
    )
    .await
}

/// Retain a failed, not-yet-confirmed continuation without publishing a
/// terminal failure. An explicit cancel may close a provably never-spawned
/// target once; an uncertain cleanup or terminal write is only observed.
pub(super) async fn hold_unstarted(id: &AgentId, store: &mut Persist) -> Result<()> {
    let _ = store
        .event_durable(
            id,
            "maintenance_held_v1",
            &json!({"reason":"preparation_unconfirmed"}),
        )
        .await;
    let mut cancellation_attempted = false;
    loop {
        if let Ok(row) = store.get(id) {
            if row.status.terminal() {
                return Ok(());
            }
            if !cancellation_attempted
                && matches!(store.cancel_pending(id), Ok(true))
                && matches!(persist::attempt_never_spawned(&store.conn, id), Ok(true))
                && let Ok((attempt, _)) = store.provider_attempt(id)
            {
                if store.bound_attempt() != Some(attempt.as_str())
                    && store.repin(&attempt).await.is_err()
                {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                if !matches!(
                    agent_run_store::maintenance_target_cancel::preparation_is_settled(
                        &store.conn,
                        id,
                        &attempt,
                    ),
                    Ok(true)
                ) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
                cancellation_attempted = true;
                if matches!(
                    crate::lifecycle::reconcile::cleanup_mcp_discovery(store, &attempt),
                    Ok(true)
                ) {
                    let outcome = Outcome {
                        status: Status::Cancelled,
                        exit_code: None,
                        failure_kind: None,
                        failure_text: None,
                        runtime_session_id: row.runtime_session_id,
                    };
                    if store.finish(id, &outcome, None, None).await.is_ok() {
                        let _ = commands::complete_terminal_persisted(store, id).await;
                        return Ok(());
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Preserve the disposition of every already-submitted effect across a hold.
async fn hold_with(
    context: &PauseContext<'_>,
    store: &mut Persist,
    process: &mut Process,
    mut state: HoldState,
) -> Result<()> {
    let reason = state.reason;
    if let Some(logger) = crate::logging::configured() {
        logger.log(
            crate::logging::Level::Warning,
            &format!("maintenance held: {reason}"),
        );
    }
    let _ = process
        .observing(store.event_durable(
            context.id,
            "maintenance_held_v1",
            &json!({"reason":reason}),
        ))
        .await;
    loop {
        if let Ok(row) = store.get(context.id) {
            if row.status.terminal() {
                return Ok(());
            }
            let cancellation = store.cancel_pending(context.id);
            if reason == "park_unconfirmed" && matches!(cancellation, Ok(false)) {
                let parked = store
                    .maintenance_park(context.epoch, context.id)
                    .ok()
                    .flatten()
                    .is_some_and(|park| {
                        park.attempt_id == context.attempt
                            && park.native_event_seq == context.native_event_seq
                    })
                    && store
                        .parked_maintenance_checkpoint(context.epoch, context.id, context.attempt)
                        .ok()
                        .flatten()
                        .is_some();
                if parked {
                    return Ok(());
                }
            }
            if !state.cancellation_submitted && matches!(cancellation, Ok(true)) {
                let stored: Option<String> = store.conn.query_row(
                    "SELECT cleanup_proof_json FROM attempts WHERE id=? AND agent_id=? AND phase='cleanup_complete' AND ownership_active=1",
                    rusqlite::params![context.attempt, context.id.as_str()], |row| row.get::<_,Option<String>>(0),
                ).optional().ok().flatten().flatten();
                let cleaned = stored
                    .as_deref()
                    .and_then(|raw| serde_json::from_str::<Cleanup>(raw).ok())
                    .is_some_and(|proof| cleanup_matches(context, store, process, &proof));
                if cleaned {
                    // The cancellation is sent once. If its acknowledgement
                    // is unknown, later iterations only observe durable state.
                    state.cancellation_submitted = true;
                    let outcome = Outcome {
                        status: Status::Cancelled,
                        exit_code: None,
                        failure_kind: None,
                        failure_text: None,
                        runtime_session_id: row.runtime_session_id,
                    };
                    if store.finish(context.id, &outcome, None, None).await.is_ok() {
                        let _ = commands::complete_terminal_persisted(store, context.id).await;
                        return Ok(());
                    }
                } else {
                    let recovery =
                        std::mem::replace(&mut state.cleanup, CleanupRecovery::ObserveOnly);
                    let recovered = match recovery {
                        CleanupRecovery::RetryOnCancel => {
                            // A later explicit cancellation authorizes one new stop.
                            match process.owner.cleanup(Duration::from_secs(2)).await {
                                Ok(proof) if confirmed(&proof) => {
                                    Some((proof, process.reap().await))
                                }
                                _ => None,
                            }
                        }
                        CleanupRecovery::ProvenNotSubmitted { proof, exit } => Some((proof, exit)),
                        CleanupRecovery::ObserveOnly => None,
                    };
                    if let Some((proof, exit)) = recovered {
                        if cleanup_matches(context, store, process, &proof)
                            && retain_pause_evidence(
                                context,
                                process,
                                &Ok(proof.clone()),
                                exit,
                                true,
                            )
                            .is_ok()
                            && process
                                .checkpoint_ownership_ready(agent_run_domain::OwnershipStage::Final)
                                .await
                                .is_ok()
                        {
                            // Unknown acknowledgement is observation-only thereafter.
                            let _ = store
                                .provider_cleanup(context.id, context.attempt, &proof)
                                .await;
                        } else {
                            // No cleanup write was submitted. Keep the same proof
                            // while a local receipt or ownership check is unavailable.
                            state.cleanup = CleanupRecovery::ProvenNotSubmitted { proof, exit };
                        }
                    }
                }
            }
        }
        process
            .observing(tokio::time::sleep(Duration::from_secs(1)))
            .await;
    }
}

/// Finish an already-closed native attempt without creating a task answer or
/// terminal notice. Every failure retains the process/task in a nonterminal
/// hold; the ordinary failed-execution path is never used for a pause.
pub(super) async fn finalize_pause(
    context: PauseContext<'_>,
    store: &mut Persist,
    process: &mut Process,
) -> Result<()> {
    if store.bound_attempt() != Some(context.attempt) {
        return hold_closed(&context, store, process, "attempt_unavailable").await;
    }
    let cleanup = process.owner.cleanup(Duration::from_secs(2)).await;
    if !cleanup.as_ref().is_ok_and(confirmed) {
        let _ = retain_pause_evidence(&context, process, &cleanup, None, false);
        return hold_with(
            &context,
            store,
            process,
            HoldState::unverified("cleanup_unverified"),
        )
        .await;
    }
    let proof = cleanup.as_ref().expect("confirmed above").clone();
    let exit = process.reap().await;
    if retain_pause_evidence(&context, process, &cleanup, exit, false).is_err() {
        return hold_with(
            &context,
            store,
            process,
            HoldState::proven("cleanup_receipt_unavailable", proof, exit),
        )
        .await;
    }
    if process
        .checkpoint_ownership_ready(agent_run_domain::OwnershipStage::Final)
        .await
        .is_err()
    {
        return hold_with(
            &context,
            store,
            process,
            HoldState::proven("ownership_unverified", proof, exit),
        )
        .await;
    }
    if !cleanup_matches(&context, store, process, &proof) {
        return hold_with(
            &context,
            store,
            process,
            HoldState::proven("cleanup_identity_unverified", proof, exit),
        )
        .await;
    }
    if store
        .provider_cleanup(context.id, context.attempt, &proof)
        .await
        .is_err()
    {
        return hold_closed(&context, store, process, "cleanup_record_unconfirmed").await;
    }
    let cleanup_event = match serde_json::to_value(&proof) {
        Ok(value) => value,
        Err(_) => return hold_closed(&context, store, process, "cleanup_event_unavailable").await,
    };
    if store
        .event_durable(context.id, "process_cleanup", &cleanup_event)
        .await
        .is_err()
    {
        return hold_closed(&context, store, process, "cleanup_event_unconfirmed").await;
    }
    match store.cancel_pending(context.id) {
        Ok(true) => {
            let session = match store.get(context.id) {
                Ok(row) => row.runtime_session_id,
                Err(_) => {
                    return hold_closed(&context, store, process, "cancellation_unavailable").await;
                }
            };
            let outcome = Outcome {
                status: Status::Cancelled,
                exit_code: exit,
                failure_kind: None,
                failure_text: None,
                runtime_session_id: session,
            };
            if store.finish(context.id, &outcome, None, None).await.is_ok() {
                let _ = commands::complete_terminal_persisted(store, context.id).await;
                return Ok(());
            }
            return hold_with(&context, store, process, HoldState::cancellation_unknown()).await;
        }
        Ok(false) => {}
        Err(_) => return hold_closed(&context, store, process, "cancellation_unavailable").await,
    }
    let state = history_evidence(
        store,
        context.id,
        context.attempt,
        context.identity,
        context.environment,
        context.continuation_root,
    );
    let session = match store.get(context.id) {
        Ok(row) => row.runtime_session_id,
        Err(_) => return hold_closed(&context, store, process, "history_unavailable").await,
    };
    let Some(session) = session else {
        return hold_closed(&context, store, process, "history_unavailable").await;
    };
    let root = context
        .continuation_root
        .map(Path::to_path_buf)
        .or_else(|| history_root(context.identity.authority.harness, context.environment));
    if state["native_history"]["task_proven"] != true
        || root.is_none()
        || crate::service::verify_recorded_history(
            &state.to_string(),
            context.identity,
            root.as_deref(),
            &session,
        )
        .is_err()
    {
        return hold_closed(&context, store, process, "history_unverified").await;
    }
    if store
        .provider_history(context.id, context.attempt, history_metadata(&state, None))
        .await
        .is_err()
    {
        return hold_closed(&context, store, process, "history_record_unconfirmed").await;
    }
    if store
        .park_maintenance_attempt(
            context.id,
            context.epoch,
            context.attempt,
            context.native_event_seq,
            domain::now(),
        )
        .await
        .is_err()
    {
        // A lost acknowledgement may have committed; resolve the exact park
        // without repeating the mutation or starting another process.
        let committed = store
            .maintenance_park(context.epoch, context.id)
            .ok()
            .flatten()
            .is_some_and(|park| {
                park.attempt_id == context.attempt
                    && park.native_event_seq == context.native_event_seq
            })
            && store
                .parked_maintenance_checkpoint(context.epoch, context.id, context.attempt)
                .ok()
                .flatten()
                .is_some();
        if !committed {
            return hold_closed(&context, store, process, "park_unconfirmed").await;
        }
    }
    Ok(())
}
