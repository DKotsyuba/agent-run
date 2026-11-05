//! Broker-owned preparation and recovery ticks for a planned restart.

use crate::{Error, Result, broker_writer::BROKER_BUDGET, service::Service};
use agent_run_domain::domain::AgentId;
use agent_run_store::{
    maintenance::{MaintenanceReadyMember, PREPARATION_LIMIT_SECONDS},
    ops::{
        CommitMaintenanceStop, DeferMaintenanceFence, EndMaintenanceFence,
        ObserveMaintenanceRestart, ReadyMaintenanceFence,
    },
};

/// Content-free current state of one captured execution.
struct Member {
    id: AgentId,
    status: String,
    attempt: Option<String>,
    attempt_state: Option<String>,
    pause_requested: bool,
    pause_catalog: bool,
    pre_notice_ready: bool,
}

/// Read current cohort members in one snapshot. Missing members are not ready.
async fn cohort(service: &Service, epoch: i64) -> Result<Vec<Member>> {
    service.read(move |store| {
        let mut statement = store.conn().prepare(
            "SELECT c.agent_id,a.status,t.id,t.state, \
             EXISTS(SELECT 1 FROM maintenance_checkpoints m WHERE m.epoch=c.epoch AND m.agent_id=c.agent_id AND m.attempt_id=t.id AND m.command_id IS NOT NULL), \
             EXISTS(SELECT 1 FROM worker_capabilities w WHERE w.attempt_id=t.id AND w.pool_catalog_version=5 AND w.pool_catalog_observed_at IS NOT NULL), \
             (c.orchestrator_session_id IS NULL OR EXISTS(SELECT 1 FROM maintenance_recipients r JOIN bridge_delivery_intents b ON b.delivery_id=r.before_delivery_id WHERE r.epoch=c.epoch AND r.orchestrator_session_id=c.orchestrator_session_id AND b.custody_state='custody_accepted' AND b.intent_json IS NOT NULL)) \
             FROM maintenance_cohort c \
             LEFT JOIN agents a ON a.id=c.agent_id AND a.sequence=c.sequence \
             LEFT JOIN attempts t ON t.agent_id=a.id AND t.ownership_active=1 AND t.finished_at IS NULL \
             WHERE c.epoch=? ORDER BY c.agent_id",
        )?;
        let rows = statement.query_map([epoch], |row| {
            Ok((row.get::<_,String>(0)?,row.get::<_,Option<String>>(1)?,row.get::<_,Option<String>>(2)?,row.get::<_,Option<String>>(3)?,row.get::<_,bool>(4)?,row.get::<_,bool>(5)?,row.get::<_,bool>(6)?))
        })?.collect::<rusqlite::Result<Vec<_>>>()?;
        rows.into_iter().map(|(id,status,attempt,attempt_state,pause_requested,pause_catalog,pre_notice_ready)| {
            let id: AgentId = id.parse()?;
            let eligible = pause_catalog && attempt.as_ref().is_some_and(|attempt| {
                store.maintenance_pause_eligible(epoch, &id, attempt).unwrap_or(false)
            });
            Ok(Member {
                id,
                status:status.ok_or_else(||Error::Integrity("maintenance cohort member is unavailable".into()))?,
                attempt, attempt_state, pause_requested, pause_catalog:eligible, pre_notice_ready,
            })
        }).collect()
    }).await
}

/// Send eligible pause requests, then atomically commit readiness after each
/// active member's original worker proves history and supervisor termination.
/// Returns false while a member, pre-notice or external proof is still pending.
pub async fn prepare_tick(service: &Service, epoch: i64) -> Result<bool> {
    let fence = service
        .read(|store| store.active_maintenance_fence())
        .await?;
    let Some(fence) = fence.filter(|fence| fence.epoch == epoch && fence.phase == "preparing")
    else {
        return Ok(false);
    };
    let deadline = fence.created_at + PREPARATION_LIMIT_SECONDS;
    let current = cohort(service, epoch).await?;
    for member in &current {
        if crate::domain::now() >= deadline {
            defer(service, epoch).await?;
            return Ok(false);
        }
        if member.status == "running"
            && member.attempt_state.as_deref() == Some("running")
            && !member.pause_requested
            && member.pause_catalog
            && member.pre_notice_ready
            && let Some(attempt) = &member.attempt
        {
            // A legacy worker, pending pre-notice or an already-changing
            // attempt is not a reason to terminate its work.
            let _ = service
                .request_maintenance_pause(epoch, &member.id, attempt)
                .await;
        }
    }
    let mut ready = Vec::new();
    for member in cohort(service, epoch).await? {
        if crate::domain::now() >= deadline {
            defer(service, epoch).await?;
            return Ok(false);
        }
        if matches!(
            member.status.as_str(),
            "created" | "starting" | "running" | "cancelling"
        ) {
            if member.attempt_state.as_deref() != Some("maintenance_parked") {
                return Ok(false);
            }
            let Ok(preflight) = service.prepare_maintenance_resume(epoch, &member.id).await else {
                return Ok(false);
            };
            ready.push(MaintenanceReadyMember {
                execution: member.id,
                preflight,
            });
        }
    }
    if crate::domain::now() >= deadline {
        defer(service, epoch).await?;
        return Ok(false);
    }
    match service
        .lane("maintenance")?
        .local(
            ReadyMaintenanceFence {
                epoch,
                members: ready,
            },
            BROKER_BUDGET,
        )
        .await
    {
        Ok(_) => Ok(true),
        Err(Error::Transition(_) | Error::Conflict) => Ok(false),
        Err(error) => Err(error),
    }
}

/// Defer a known-not-restarted preparation. This never opens admission while
/// a claimed pause or its continuation remains unresolved.
pub async fn defer(service: &Service, epoch: i64) -> Result<()> {
    service
        .lane("maintenance")?
        .local(DeferMaintenanceFence { epoch }, BROKER_BUDGET)
        .await?;
    Ok(())
}

/// Whether an OS stop can proceed, must wait for maintenance, or has no fence.
pub enum StopDisposition {
    /// No planned maintenance is active.
    NoFence,
    /// The active cohort cannot yet be stopped safely.
    Waiting,
    /// The ready boundary is durably sealed against a concurrent deferral.
    Committed,
}

/// Seal the verified ready boundary only in response to an actual stop signal.
pub async fn commit_stop(service: &Service) -> Result<StopDisposition> {
    let Some(fence) = service
        .read(|store| store.active_maintenance_fence())
        .await?
    else {
        return Ok(StopDisposition::NoFence);
    };
    if !matches!(fence.phase.as_str(), "ready" | "stopping") {
        return Ok(StopDisposition::Waiting);
    }
    let committed = service
        .lane("maintenance")?
        .local(CommitMaintenanceStop { epoch: fence.epoch }, BROKER_BUDGET)
        .await?;
    Ok(if committed {
        StopDisposition::Committed
    } else {
        StopDisposition::Waiting
    })
}

/// Reconcile one interrupted or committed restart. A successor is launched
/// only by the single owned future that received a fresh allocation receipt;
/// replay, unknown outcomes and broker crashes never authorize another launch.
pub async fn recovery_tick(service: &Service) -> Result<()> {
    let fence = service
        .read(|store| store.active_maintenance_fence())
        .await?;
    let Some(mut fence) = fence else {
        return Ok(());
    };
    if matches!(fence.phase.as_str(), "preparing" | "ready" | "stopping") {
        let prior = if fence.phase == "preparing" {
            fence.origin_writer_epoch
        } else {
            fence.ready_writer_epoch
        };
        let Some(prior) = prior else {
            return Err(Error::Integrity(
                "maintenance writer identity is unavailable".into(),
            ));
        };
        let current: i64 = service
            .read(|store| {
                Ok(store.conn().query_row(
                    "SELECT epoch FROM writer_authority WHERE id=1",
                    [],
                    |row| row.get(0),
                )?)
            })
            .await?;
        if current <= prior {
            if matches!(fence.phase.as_str(), "preparing" | "ready")
                && crate::domain::now() >= fence.created_at + PREPARATION_LIMIT_SECONDS
            {
                defer(service, fence.epoch).await?;
                fence.phase = "deferring".into();
            } else if fence.phase == "preparing" {
                let _ = prepare_tick(service, fence.epoch).await?;
                return Ok(());
            } else {
                return Ok(());
            }
        }
        if matches!(fence.phase.as_str(), "preparing" | "ready" | "stopping") {
            match service
                .lane("maintenance")?
                .local(
                    ObserveMaintenanceRestart { epoch: fence.epoch },
                    BROKER_BUDGET,
                )
                .await
            {
                Ok(observed) => fence = observed,
                Err(Error::Transition(_)) => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }
    if !matches!(fence.phase.as_str(), "recovering" | "deferring") {
        return Ok(());
    }
    for member in cohort(service, fence.epoch).await? {
        let prepared = member.attempt_state.as_deref() == Some("prepared");
        if !prepared && member.attempt_state.as_deref() != Some("maintenance_parked") {
            continue;
        }
        let service = service.clone();
        let epoch = fence.epoch;
        let owned = tokio::spawn(async move {
            let authorized = if prepared {
                service
                    .claim_maintenance_prepared(epoch, &member.id)
                    .await?
                    .is_some_and(|claim| claim.launch_authorized)
            } else {
                let request_id = format!("maintenance-resume:{epoch}:{}", member.id);
                service
                    .claim_maintenance_resume(epoch, &member.id, request_id)
                    .await?
                    .launch_authorized
            };
            if authorized {
                let lane = service.lane("maintenance-launch")?;
                if let Err(error) = service.launch_provider_supervisor(&member.id, &lane).await {
                    let lookup = member.id.clone();
                    let never_spawned = matches!(&error, Error::Io(_))
                        && service
                            .read(move |store| {
                                crate::persist::attempt_never_spawned(&store.conn, &lookup)
                            })
                            .await?;
                    if !never_spawned {
                        return Err(error);
                    }
                    let mut outcome =
                        agent_run_domain::domain::Outcome::failure("supervisor_spawn_failed");
                    outcome.failure_text =
                        Some("the run supervisor could not be started after maintenance".into());
                    lane.finish_terminal(&member.id, &outcome).await?;
                }
            }
            Ok::<(), Error>(())
        });
        match owned.await {
            Ok(Ok(())) => {}
            Ok(Err(
                error @ (Error::BrokerUnavailable
                | Error::Persistence { .. }
                | Error::WriterFailure(_)),
            )) => return Err(error),
            // A blocked member stays held; it must not starve other members.
            Ok(Err(_)) | Err(_) => continue,
        }
    }
    match service
        .lane("maintenance")?
        .local(
            EndMaintenanceFence {
                epoch: fence.epoch,
                at: crate::domain::now(),
            },
            BROKER_BUDGET,
        )
        .await
    {
        Ok(_) | Err(Error::Transition(_)) => Ok(()),
        Err(error) => Err(error),
    }
}
