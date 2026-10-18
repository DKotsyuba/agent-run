//! Mapping of decoded operations to their pure store units.
//!
//! [`remote_facts`]/[`local_facts`] extract the execution, attempt and
//! service generation a payload targets (used by the receipt scope and the
//! authority predicates). [`run_remote`]/[`run_local`] execute the payload on
//! the writer's store inside the already-open top-level transaction and
//! return its typed output as JSON. Rows not implemented yet report
//! [`implemented`] = false and are refused before any transaction, so they
//! never leave a receipt.

use super::apply::{Checks, Claimed};
use super::*;
use crate::Store;
use agent_run_domain::{Result, domain::AgentId, error::invalid};
use serde_json::{Value, json};

/// Target facts of one payload: execution, attempt and service generation.
pub(crate) type Facts = (Option<AgentId>, Option<String>, Option<String>);

/// Whether a row has a store implementation in this revision.
pub fn implemented(kind: OpKindRef) -> bool {
    use OpKindRef as K;
    matches!(
        kind,
        K::ClaimStartup
            | K::RecordSupervisorOwner
            | K::SetOwner
            | K::UpdateIdentity
            | K::Running
            | K::RuntimeSession
            | K::RememberProcesses
            | K::RetainCleanupEvidence
            | K::ProviderSpawning
            | K::ProviderSpawnFailed
            | K::ProviderProcess
            | K::ProviderCleanup
            | K::ProviderRerunning
            | K::CreateAttempt
            | K::FinishAttempt
            | K::IssueWorkerCapability
            | K::BeginWorkerLifecycle
            | K::RecordResumeUsageBaseline
            | K::AppendJournal
            | K::EnqueueCommand
            | K::ClaimCommand
            | K::CompleteCommand
            | K::ObservedCommandResult
            | K::EnqueueNativeWake
            | K::WorkerTurn
            | K::ObserveWorkerFinishReceipt
            | K::BindOrchestrator
            | K::RecordContextComponents
            | K::RecordContextReceipt
            | K::RegisterAccount
            | K::DisableAccount
            | K::CancelDelivery
            | K::BindPool
            | K::BootstrapServiceProcess
            | K::ClaimServiceProcessIdentity
            | K::RegisterWorkerCatalog
            | K::AcceptWorkerFinish
            | K::HealthTick
            | K::AcceptHealthReply
            | K::NotifyOrchestrator
            | K::RecordSupervisorLaunch
            | K::RecordStartupOwner
            | K::BeginSupervisorHandoff
            | K::LatchNativeExhaustion
            | K::PrepareRuntimeLayout
            | K::CommitRuntimeLayout
            | K::RemoveRuntimeLayout
            | K::ProviderHistory
            | K::CompleteTerminalCommands
            | K::AllocateNextAttempt
            | K::FinishTerminal
            | K::PersistRunStats
            | K::CaptureIncident
            | K::AdmitStart
            | K::AdmitProvider
            | K::AdmitProviderResume
            | K::AdmitPool
            | K::ReplacePoolMember
            | K::ReconcilePoolEnrollments
            | K::PersistRecoveryObservation
            | K::PoolWrite
            | K::PoolOperatorPost
            | K::SettlePool
            | K::ExpireUnboundDeliveries
            | K::PruneIncidentLedger
            | K::PruneReceipts
            | K::PersistCapacitySamples
            | K::PruneCapacitySamples
            | K::RecordQuotaSnapshot
            | K::ClaimDelivery
            | K::CompleteDelivery
            | K::PrepareBridgeDelivery
            | K::ReconcileReapedAgent
            | K::ReleaseOrphanedAttempt
            | K::MarkLost
            | K::RecoverFinish
            | K::ReleaseFinishedServiceLeases
            | K::PrepareServiceGate
            | K::EnsureServiceGeneration
            | K::FailServiceSpawn
            | K::FailServiceGate
            | K::MarkServiceUnhealthy
            | K::ObserveServiceGeneration
            | K::MarkServiceReady
            | K::ObserveExternalService
            | K::StopServiceGeneration
            | K::RecoverServiceProbes
            | K::PruneHistoryBatch
    )
}

/// Maximum deliveries one expiry batch (row 57) expires; the ids stay well
/// inside the 64 KiB result bound.
const EXPIRY_BATCH: usize = 1000;

/// Serializes a typed output.
fn out<T: serde::Serialize>(value: T) -> Result<Value> {
    Ok(serde_json::to_value(value)?)
}

/// Target facts of a remote payload.
pub(crate) fn remote_facts(op: &RemoteOp) -> Facts {
    use RemoteOp as R;
    let exec = |id: &AgentId| (Some(id.clone()), None, None);
    let attempt = |id: &AgentId, attempt: &str| (Some(id.clone()), Some(attempt.to_owned()), None);
    match op {
        R::ClaimDelivery(_)
        | R::CompleteDelivery(_)
        | R::PrepareBridgeDelivery(_)
        | R::PruneCapacitySamples(_) => (None, None, None),
        R::ClaimStartup(op) => exec(&op.execution),
        R::RecordSupervisorOwner(op) => exec(&op.execution),
        R::UpdateIdentity(op) => exec(&op.execution),
        R::Running(op) => exec(&op.execution),
        R::RuntimeSession(op) => (Some(op.execution.clone()), op.attempt.clone(), None),
        // Resolved inside the transaction from the owner row (see `remote_checks`).
        R::RememberProcesses(_) => (None, None, None),
        R::RetainCleanupEvidence(op) => attempt(&op.execution, &op.attempt),
        R::ProviderSpawning(op) => attempt(&op.target.execution, &op.target.attempt),
        R::ProviderSpawnFailed(op) => attempt(&op.target.execution, &op.target.attempt),
        R::ProviderProcess(op) => attempt(&op.target.execution, &op.target.attempt),
        R::ProviderCleanup(op) => attempt(&op.target.execution, &op.target.attempt),
        R::ProviderHistory(op) => attempt(&op.target.execution, &op.target.attempt),
        R::ProviderRerunning(op) => attempt(&op.target.execution, &op.target.attempt),
        R::AllocateNextAttempt(op) => exec(&op.execution),
        R::CreateAttempt(op) => exec(&op.execution),
        R::FinishAttempt(op) => attempt(&op.execution, &op.attempt),
        R::IssueWorkerCapability(op) => attempt(&op.execution, &op.attempt),
        R::BeginWorkerLifecycle(op) => attempt(&op.execution, &op.attempt),
        R::RecordResumeUsageBaseline(op) => exec(&op.execution),
        R::LatchNativeExhaustion(op) => exec(&op.execution),
        R::AppendJournal(op) => exec(&op.execution),
        R::EnqueueCommand(op) => exec(&op.execution),
        R::ClaimCommand(op) => exec(&op.execution),
        R::CompleteCommand(op) => exec(&op.execution),
        R::ObservedCommandResult(op) => exec(&op.execution),
        R::CompleteTerminalCommands(op) => exec(&op.execution),
        R::EnqueueNativeWake(op) => exec(&op.execution),
        R::WorkerTurn(op) => exec(&op.execution),
        R::ObserveWorkerFinishReceipt(op) => exec(&op.execution),
        R::PersistRecoveryObservation(op) => attempt(&op.execution, &op.attempt),
        R::FinishTerminal(op) => exec(&op.execution),
        R::PersistRunStats(op) => exec(&op.row.agent_id),
        R::BindOrchestrator(op) => exec(&op.execution),
        R::BootstrapServiceProcess(op) => (None, None, Some(op.generation.clone())),
        R::ClaimServiceProcessIdentity(op) => (None, None, Some(op.generation.clone())),
        R::BindPool(_)
        | R::CancelDelivery(_)
        | R::PersistCapacitySamples(_)
        | R::RecordQuotaSnapshot(_)
        | R::RegisterAccount(_)
        | R::DisableAccount(_)
        | R::RecordContextComponents(_)
        | R::RecordContextReceipt(_)
        | R::CommitRuntimeLayout(_) => (None, None, None),
        // The layout's owner execution is the authorization target; a
        // supervisor may only prepare its own (row 88).
        R::PrepareRuntimeLayout(op) => (op.owner.clone(), None, None),
    }
}

/// Payload facts checked inside the transaction besides the target: the
/// process owner whose scope is resolved from its row, the process identity
/// a payload proposes to record (a bootstrap snapshot's leader included),
/// the account a native latch names and the runtime home whose recorded
/// layout owner a commit is authorized against.
pub(crate) fn remote_checks(op: &RemoteOp) -> Checks {
    match op {
        RemoteOp::CommitRuntimeLayout(op) => Checks {
            layout: Some(op.runtime_home.clone()),
            ..Checks::default()
        },
        RemoteOp::RememberProcesses(op) => Checks {
            owner: Some((op.kind.clone(), op.owner.clone())),
            ..Checks::default()
        },
        RemoteOp::RecordSupervisorOwner(op) => Checks {
            claimed: Some(Claimed {
                pid: op.pid,
                token: op.identity.clone(),
                birth: op.birth_time,
            }),
            ..Checks::default()
        },
        RemoteOp::BootstrapServiceProcess(op) => Checks {
            claimed: Some(Claimed {
                pid: op.snapshot.leader.pid,
                token: op.snapshot.leader.token.clone(),
                birth: Some(op.snapshot.leader.birth),
            }),
            ..Checks::default()
        },
        RemoteOp::ClaimServiceProcessIdentity(op) => Checks {
            claimed: Some(Claimed {
                pid: op.identity.pid,
                token: op.identity.token.clone(),
                birth: Some(op.identity.birth),
            }),
            ..Checks::default()
        },
        RemoteOp::LatchNativeExhaustion(op) => Checks {
            account: Some(op.account.as_str().to_owned()),
            ..Checks::default()
        },
        _ => Checks::default(),
    }
}

/// Target facts of a broker-local payload.
pub(crate) fn local_facts(op: &LocalOp) -> Facts {
    use LocalOp as L;
    let exec = |id: &AgentId| (Some(id.clone()), None, None);
    match op {
        L::RecordSupervisorLaunch(op) => exec(&op.execution),
        L::RecordStartupOwner(op) => exec(&op.execution),
        L::BeginSupervisorHandoff(op) => exec(&op.execution),
        L::SetOwner(op) => exec(&op.execution),
        L::RegisterWorkerCatalog(op) => (
            Some(op.proof.run_id.clone()),
            Some(op.proof.attempt_id.clone()),
            None,
        ),
        L::AcceptWorkerFinish(op) => (Some(op.execution.clone()), Some(op.attempt.clone()), None),
        L::AcceptHealthReply(op) => (Some(op.execution.clone()), Some(op.attempt.clone()), None),
        L::HealthTick(op) => (Some(op.execution.clone()), None, None),
        L::NotifyOrchestrator(op) => (Some(op.execution.clone()), Some(op.attempt.clone()), None),
        L::PoolWrite(op) => (Some(op.execution.clone()), Some(op.attempt.clone()), None),
        L::ReconcileReapedAgent(op) => exec(&op.execution),
        L::ReleaseOrphanedAttempt(op) => {
            (Some(op.execution.clone()), Some(op.attempt.clone()), None)
        }
        L::MarkLost(op) => exec(&op.expected.execution),
        L::RecoverFinish(op) => exec(&op.expected.execution),
        L::PrepareServiceGate(op) => exec(&op.execution),
        L::FailServiceGate(op) => exec(&op.execution),
        L::CaptureIncident(op) => exec(&op.projection.execution),
        _ => (None, None, None),
    }
}

/// Executes a remote payload inside the open transaction.
pub(crate) fn run_remote(store: &mut Store, op: RemoteOp) -> Result<Value> {
    use RemoteOp as R;
    match op {
        R::ClaimStartup(op) => out(store.claim_startup(
            &op.execution,
            &op.owner_identity,
            op.owner_birth_time,
            op.at,
            op.deadline_seconds,
        )?),
        R::RecordSupervisorOwner(op) => {
            match op.process_group_id {
                Some(group) => store.record_supervisor(
                    &op.execution,
                    op.pid,
                    &op.identity,
                    group,
                    op.birth_time,
                    op.at,
                )?,
                None => {
                    store.record_owner(&op.execution, op.pid, &op.identity, op.birth_time, op.at)?
                }
            }
            out(())
        }
        R::UpdateIdentity(op) => out(store.seal_identity(&op)?),
        R::Running(op) => out(store.running(&op.execution, op.pgid)?),
        R::RuntimeSession(op) => {
            let bound = store.attempt.clone();
            store.attempt = op.attempt.clone();
            let result = store.runtime_session(&op.execution, &op.session);
            store.attempt = bound;
            out(result?)
        }
        R::RememberProcesses(op) => {
            out(store.remember_processes(&op.kind, &op.owner, &op.snapshot)?)
        }
        R::RetainCleanupEvidence(op) => out(store.anchor_legacy_attempt(&op)?),
        R::ProviderSpawning(op) => {
            out(store.provider_spawning(&op.target.execution, &op.target.attempt)?)
        }
        R::ProviderSpawnFailed(op) => {
            out(store.provider_spawn_failed(&op.target.execution, &op.target.attempt)?)
        }
        R::ProviderProcess(op) => {
            out(store.provider_process(&op.target.execution, &op.target.attempt, &op.leader)?)
        }
        R::ProviderCleanup(op) => {
            out(store.provider_cleanup(&op.target.execution, &op.target.attempt, &op.proof)?)
        }
        R::ProviderRerunning(op) => {
            out(store.provider_rerunning(&op.target.execution, &op.target.attempt, op.pgid)?)
        }
        R::CreateAttempt(op) => {
            out(store.create_attempt(&op.execution, &op.state, &op.adapter_state)?)
        }
        R::FinishAttempt(op) => out(store.finish_attempt(&op.execution, &op.attempt, &op.state)?),
        R::IssueWorkerCapability(op) => {
            out(store.issue_worker_capability(&op.execution, &op.attempt, &op.token, op.at)?)
        }
        R::BeginWorkerLifecycle(op) => {
            out(store.begin_worker_lifecycle(&op.execution, &op.attempt, op.at)?)
        }
        R::RecordResumeUsageBaseline(op) => out(store.record_resume_usage_baseline(&op.execution)?),
        R::AppendJournal(op) => {
            if op.fragments.len() > 32 {
                return Err(invalid("journal batch exceeds 32 fragments"));
            }
            let mut seqs = Vec::with_capacity(op.fragments.len());
            for fragment in &op.fragments {
                seqs.push(store.append_fragment(&op.execution, fragment)?);
            }
            out(seqs)
        }
        R::EnqueueCommand(op) => out(store.enqueue(&op.execution, &op.kind, &op.payload)?),
        R::ClaimCommand(op) => out(store
            .claim_command_for_turn(&op.execution, op.allow_native_wake)?
            .map(|(command_id, kind, _)| ClaimedCommand { command_id, kind })),
        R::CompleteCommand(op) => {
            out(store.complete_command(&op.execution, op.command_id, &op.result)?)
        }
        R::ObservedCommandResult(op) => {
            if !matches!(op.strength.as_str(), "stdin_written" | "native_accepted") {
                return Err(invalid("unknown observation strength"));
            }
            if op.fragments.len() > 32 {
                return Err(invalid("journal batch exceeds 32 fragments"));
            }
            let mut result = op.result.clone();
            if let Some(object) = result.as_object_mut() {
                object.insert("observation".into(), json!(op.strength));
            }
            store.complete_command(&op.execution, op.command_id, &result)?;
            if let Some(idle) = op.worker_turn {
                store.worker_turn(&op.execution, idle, agent_run_domain::domain::now())?;
            }
            let mut seqs = Vec::with_capacity(op.fragments.len());
            for fragment in &op.fragments {
                seqs.push(store.append_fragment(&op.execution, fragment)?);
            }
            out(seqs)
        }
        R::EnqueueNativeWake(op) => {
            out(store.enqueue_native_wake(&op.execution, &op.digest, &op.text, op.at)?)
        }
        R::WorkerTurn(op) => out(store.worker_turn(&op.execution, op.idle, op.at)?),
        R::ObserveWorkerFinishReceipt(op) => {
            out(store.observe_worker_finish_receipt(&op.execution)?)
        }
        R::BindOrchestrator(op) => {
            out(store.bind_orchestrator(&op.execution, &op.reference, op.at)?)
        }
        R::RecordContextComponents(op) => {
            out(store.record_context_components_for_ref(&op.reference, &op.components, op.at)?)
        }
        R::RecordContextReceipt(op) => out(match &op.target {
            SessionTarget::Id { id } => (
                id.clone(),
                store.record_context_receipt(id, &op.context_key, op.at)?,
            ),
            SessionTarget::Reference { reference } => {
                store.record_context_receipt_for_ref(reference, &op.context_key, op.at)?
            }
        }),
        R::RegisterAccount(op) => out(store.register_account(&op.record)?),
        R::DisableAccount(op) => out(store.disable_account(&op.account)?),
        R::ClaimDelivery(op) => out(store.claim_delivery_lease(&op)?),
        R::CompleteDelivery(op) => out(store.complete_delivery_attempt(&op.completion)?),
        R::PrepareBridgeDelivery(op) => out(store.prepare_bridge_delivery(&op)?),
        R::PruneCapacitySamples(op) => out(store.prune_capacity_samples(op.retention)?),

        R::CancelDelivery(op) => out(store.cancel_delivery(&op.delivery_id)?),
        R::BindPool(op) => out(store.bind_pool(&op.pool_id, &op.reference, op.at)?),
        R::BootstrapServiceProcess(op) => {
            out(store.remember_processes("service", &op.generation, &op.snapshot)?)
        }
        R::ClaimServiceProcessIdentity(op) => {
            out(store.claim_service_identity(&op.generation, &op.identity)?)
        }
        R::LatchNativeExhaustion(op) => out(store.latch_native_exhaustion(
            &op.account,
            &op.runtime,
            &op.lane,
            &op.window,
            &op.source,
            &op.models,
            op.observed_at,
            op.reset_at,
        )?),
        R::PrepareRuntimeLayout(op) => out(layout_receipt(
            store.prepare_runtime_storage_layout(&op.layout_json, op.owner.as_ref())?,
        )),
        R::CommitRuntimeLayout(op) => out(layout_receipt(store.commit_runtime_storage_layout(
            &op.runtime_home,
            &op.operation_token,
            &op.layout_sha256,
        )?)),
        R::ProviderHistory(op) => {
            let metadata = history_value(&op.metadata)?;
            out(store.provider_history(&op.target.execution, &op.target.attempt, &metadata)?)
        }
        R::CompleteTerminalCommands(op) => {
            out(store.complete_terminal_commands(&op.execution, op.at)?)
        }
        R::FinishTerminal(op) => {
            crate::terminal::commit_terminal(
                store,
                &op.execution,
                &op.outcome,
                op.proof.as_ref(),
                op.usage.as_ref(),
            )?;
            out(store.get(&op.execution)?.status.as_str())
        }
        R::PersistRunStats(op) => out(store.persist_run_stats(&op.row)?),
        R::AllocateNextAttempt(op) => {
            let catalog = catalog_from(&op.catalog)?;
            let next = store.allocate_next_attempt(&op.execution, &catalog, &op.candidates)?;
            out(NextAttemptOutput {
                attempt_id: next.attempt_id,
                number: next.number,
                account_id: next.account_id,
                released: next.released,
            })
        }
        R::PersistRecoveryObservation(op) => out(super::apply::record_recovery(&store.conn, &op)?),
        R::PersistCapacitySamples(op) => out(store.persist_capacity_samples(&op)?),
        R::RecordQuotaSnapshot(op) => {
            out(store.record_quota_snapshot(&op.runtime, &op.snapshot, op.retention, op.at)?)
        }
    }
}

/// Executes a broker-local payload inside the open transaction.
pub(crate) fn run_local(store: &mut Store, op: LocalOp) -> Result<Value> {
    use LocalOp as L;
    match op {
        L::RecordSupervisorLaunch(op) => {
            out(store.record_supervisor_launch(&op.execution, &op.capability_sha256)?)
        }
        L::RecordStartupOwner(op) => out(store.record_startup_owner(&op.execution, &op.identity)?),
        L::BeginSupervisorHandoff(op) => out(store.begin_supervisor_handoff_with(&op)?),
        L::SetOwner(op) => out(store.set_owner(&op.execution, op.pid, &op.identity, op.birth)?),
        L::RegisterWorkerCatalog(op) => out(store.register_worker_catalog(&op.proof, op.at)?),
        L::AcceptWorkerFinish(op) => out(store.accept_worker_finish(
            &op.execution,
            &op.attempt,
            &op.token,
            &op.input,
            op.at,
            op.guard,
        )?),
        L::HealthTick(op) => out(store.health_tick_prepared(&op.execution, op.at, &op.facts)?),
        L::AcceptHealthReply(op) => out(store.accept_health_reply(
            &op.execution,
            &op.attempt,
            &op.token,
            &op.input,
            op.at,
        )?),
        L::NotifyOrchestrator(op) => out(store.notify_orchestrator(
            &op.execution,
            &op.attempt,
            &op.token,
            &op.input,
            op.at,
        )?),
        L::RemoveRuntimeLayout(op) => out(store.remove_runtime_storage_layout(&op.runtime_home)?),
        L::CaptureIncident(op) => out(store.capture_incident(&op.projection)?),
        L::AdmitStart(op) => {
            let parent = op.parent.as_ref().map(|id| store.get(id)).transpose()?;
            let (agent_id, created) = store.admit_with_config_revision(
                &op.request,
                &op.config,
                &op.config_revision,
                &op.identity,
                parent.as_ref(),
            )?;
            out(AdmitStartOutput {
                agent_id,
                replayed: !created,
            })
        }
        L::AdmitProvider(op) => {
            let spec = &op.inputs;
            out(store.admit_provider(
                &spec.request,
                &spec.effective,
                &op.catalog,
                &spec.authority,
                &spec.candidates,
                &spec.identity,
                spec.global_cap,
                spec.harness_cap,
                spec.pinned.as_ref(),
            )?)
        }
        L::AdmitProviderResume(op) => {
            let spec = &op.inputs;
            out(store.admit_provider_resume(
                &spec.request,
                &spec.effective,
                &op.catalog,
                &spec.authority,
                &spec.candidates,
                &spec.identity,
                spec.global_cap,
                spec.harness_cap,
                spec.pinned.as_ref(),
                crate::provider_admission::ProviderResume {
                    parent: &op.parent,
                    prefer: &op.prefer,
                },
            )?)
        }
        L::AdmitPool(op) => {
            let members = op
                .members
                .iter()
                .map(|member| crate::pool_admission::PoolMemberAdmission {
                    id: member.id.clone(),
                    slot: member.slot,
                    name: member.name.clone(),
                    role: member.role.clone(),
                    personal_task: member.personal_task.clone(),
                    source: match &member.source {
                        PoolMemberSource::New(spec) => {
                            crate::pool_admission::PoolAdmissionSource::New(inputs(spec))
                        }
                        PoolMemberSource::Existing(existing) => {
                            crate::pool_admission::PoolAdmissionSource::Existing(
                                (**existing).clone(),
                            )
                        }
                    },
                })
                .collect();
            out(store.admit_pool(crate::pool_admission::PoolAdmissionInput {
                pool_id: &op.pool_id,
                request_namespace: &op.request_namespace,
                request_id: &op.request_id,
                request_sha256: &op.request_sha256,
                goal: &op.goal,
                acceptance: &op.acceptance,
                catalog: &op.catalog,
                members,
                orchestrator: op.orchestrator.as_ref(),
            })?)
        }
        L::ReplacePoolMember(op) => {
            let replaced = store.replace_pool_member(crate::pool_replace::PoolReplaceInput {
                pool_id: &op.pool_id,
                old: &op.old,
                request_id: &op.request_id,
                request_sha256: &op.request_sha256,
                expected_roster_revision: op.expected_roster_revision,
                new_id: op.new_id.clone(),
                name: &op.name,
                personal_task: &op.personal_task,
                catalog: &op.catalog,
                inputs: inputs(&op.inputs),
            })?;
            out(match replaced {
                Ok(replacement) => PoolOutcome::Done(replacement),
                Err(denial) => PoolOutcome::Denied(denial),
            })
        }
        L::ReconcilePoolEnrollments(op) => out(store.reconcile_pool_enrollments(&op.pool)?),
        L::PoolWrite(op) => {
            let write = match op.write {
                PoolWriteSpec::Message(message) => crate::pool_log::PoolWrite::Message(message),
                PoolWriteSpec::Proposal(proposal) => crate::pool_log::PoolWrite::Proposal(proposal),
                PoolWriteSpec::Vote(vote) => crate::pool_log::PoolWrite::Vote(vote),
            };
            out(pool_outcome(store.pool_write(
                &op.execution,
                &op.attempt,
                &op.token,
                write,
            )?))
        }
        L::PoolOperatorPost(op) => out(pool_outcome(store.pool_operator_post(
            &op.pool_id,
            &op.request_id,
            &op.message,
        )?)),
        L::SettlePool(op) => out(match op.target {
            SettleTarget::Pool { pool_id } => json!({ "settled": store.settle_pool(&pool_id)? }),
            SettleTarget::Of { agent_id } => json!({ "settled": store.settle_pool_of(&agent_id)? }),
            SettleTarget::Open { limit, seed } => {
                if limit == 0 || limit > 64 {
                    return Err(invalid("pool settlement batch must be 1..=64"));
                }
                json!({ "settled": store.settle_open_pools(limit, seed)? })
            }
        }),
        L::ExpireUnboundDeliveries(op) => {
            out(store.expire_unbound_batch(op.at, Some(EXPIRY_BATCH))?)
        }
        L::PruneIncidentLedger(op) => out(crate::incidents::prune(&store.conn, op.at)?),
        L::PruneReceipts(op) => out(super::apply::prune_receipts(&store.conn, &op)?),

        L::ReconcileReapedAgent(op) => out(store.advance_reconcile_cursor(&op)?),
        L::ReleaseOrphanedAttempt(op) => out(store.release_orphaned_attempt(&op)?),
        L::MarkLost(op) => out(store.mark_lost(&op)?),
        L::RecoverFinish(op) => out(store.recover_finish(&op)?),
        L::ReleaseFinishedServiceLeases(op) => out(store.release_finished_service_leases(op.at)?),
        L::PrepareServiceGate(op) => out(store.prepare_service_gate(&op)?),
        L::EnsureServiceGeneration(op) => out(store.ensure_service_generation(&op)?),
        L::FailServiceSpawn(op) => out(store.fail_service_spawn(&op.generation)?),
        L::FailServiceGate(op) => out(store.fail_service_gate(&op)?),
        L::MarkServiceUnhealthy(op) => out(store.mark_service_unhealthy(&op)?),
        L::ObserveServiceGeneration(op) => out(store.observe_service_generation(&op)?),
        L::MarkServiceReady(op) => out(store.mark_service_ready(&op)?),
        L::ObserveExternalService(op) => out(store.observe_external_service(&op)?),
        L::StopServiceGeneration(op) => out(store.stop_service_generation(&op)?),
        L::RecoverServiceProbes(op) => out(store.record_service_probe(&op)?),
        L::PruneHistoryBatch(op) => out(store.prune_history_prepared(&op)?),
        other => Err(agent_run_domain::Error::Unsupported(format!(
            "operation {} is not implemented",
            other.kind().name()
        ))),
    }
}

/// Borrowed admission inputs of an owned specification.
fn inputs(spec: &AdmissionSpec) -> crate::provider_admission::AdmissionInputs<'_> {
    crate::provider_admission::AdmissionInputs {
        request: &spec.request,
        effective: &spec.effective,
        authority: &spec.authority,
        candidates: &spec.candidates,
        identity: &spec.identity,
        global_cap: spec.global_cap,
        harness_cap: spec.harness_cap,
        pinned: spec.pinned.as_ref(),
    }
}

/// A pool append outcome as its typed receipt result.
fn pool_outcome(
    result: std::result::Result<
        crate::pool_log::PoolWriteReceipt,
        agent_run_domain::pool::PoolDenial,
    >,
) -> PoolOutcome<PoolWriteOutput> {
    match result {
        Ok(receipt) => PoolOutcome::Done(PoolWriteOutput {
            pool_id: receipt.pool_id,
            seq: receipt.seq,
            duplicate: receipt.duplicate,
        }),
        Err(denial) => PoolOutcome::Denied(denial),
    }
}

/// Projects a layout record onto its receipt result.
fn layout_receipt(record: crate::runtime_storage::RuntimeStorageLayoutRecord) -> LayoutReceipt {
    LayoutReceipt {
        runtime_home: record.runtime_home,
        layout_sha256: record.layout_sha256,
        state: record.state.as_str().to_owned(),
        operation_token: record.operation_token,
    }
}

/// Validates continuation metadata and renders the stored attempt history
/// object: exactly one of sealed history or a bounded unavailable reason
/// (<= 512 bytes), plus optional closed native-failure facts.
fn history_value(metadata: &ProviderHistoryMetadata) -> Result<Value> {
    if metadata.native_history.is_some() == metadata.native_history_unavailable.is_some() {
        return Err(invalid(
            "history metadata must be sealed history or an unavailable reason",
        ));
    }
    if metadata
        .native_history_unavailable
        .as_ref()
        .is_some_and(|reason| reason.is_empty() || reason.len() > 512)
    {
        return Err(invalid("history unavailable reason must be 1..512 bytes"));
    }
    if metadata
        .native_history
        .as_ref()
        .is_some_and(|history| !history.is_object())
    {
        return Err(invalid("sealed history must be an object"));
    }
    let mut value = serde_json::Map::new();
    if let Some(history) = &metadata.native_history {
        value.insert("native_history".into(), history.clone());
    }
    if let Some(reason) = &metadata.native_history_unavailable {
        value.insert("native_history_unavailable".into(), json!(reason));
    }
    if let Some(failure) = &metadata.native_failure {
        value.insert("native_failure".into(), failure.clone());
    }
    Ok(Value::Object(value))
}

/// Rebuilds and revalidates a frozen catalog from its serialized form.
fn catalog_from(value: &Value) -> Result<agent_run_domain::catalog::ProviderCatalog> {
    #[derive(serde::Deserialize)]
    /// Serialized catalog fields.
    struct Wire {
        /// Accounts.
        accounts: Vec<agent_run_domain::catalog::AccountRecord>,
        /// Providers.
        providers: Vec<agent_run_domain::catalog::ProviderDefinition>,
    }
    let wire: Wire = serde_json::from_value(value.clone())?;
    agent_run_domain::catalog::ProviderCatalog::new(wire.accounts, wire.providers)
}
