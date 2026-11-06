//! Broker loop for the planned service restart; no public MCP tools are added.

use crate::{Result, domain::AgentId, service::Service};
use rusqlite::OptionalExtension;
use std::{collections::HashSet, time::Duration};
use tokio::task::JoinSet;

/// Advance preparation and own one resume attempt per captured agent.
pub async fn run(service: Service) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut attempted: HashSet<(i64, AgentId)> = HashSet::new();
    let mut resumes = JoinSet::new();
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if let Err(error) = advance(&service, &mut attempted, &mut resumes).await {
                    eprintln!("service restart: {}", error.public().kind);
                }
            }
            result = resumes.join_next(), if !resumes.is_empty() => {
                if result.is_some_and(|result| result.is_err()) {
                    eprintln!("service restart: resume task ended unexpectedly");
                }
            }
        }
    }
}

/// Read the active phase and dispatch pending resumes without blocking peers.
async fn advance(
    service: &Service,
    attempted: &mut HashSet<(i64, AgentId)>,
    resumes: &mut JoinSet<()>,
) -> Result<()> {
    let snapshot = service.read(|store| {
        let restart: Option<(i64,String)> = store.conn().query_row(
            "SELECT id,phase FROM service_restarts WHERE finished_at IS NULL",
            [], |row|Ok((row.get(0)?,row.get(1)?)),
        ).optional()?;
        let Some((id, phase)) = restart else { return Ok(None); };
        let pending = if phase == "recovering" {
            let mut statement = store.conn().prepare(
                "SELECT agent_id FROM service_restart_agents WHERE restart_id=? \
                 AND resume_status='pending' AND resumed_execution_id IS NULL AND cancelled_before_restart=0 ORDER BY agent_id",
            )?;
            let rows = statement.query_map([id], |row|row.get::<_,String>(0))?;
            rows.map(|row| row?.parse::<AgentId>()).collect::<Result<Vec<_>>>()?
        } else { Vec::new() };
        Ok(Some((id, phase, pending)))
    }).await?;
    let Some((id, phase, pending)) = snapshot else {
        attempted.clear();
        return Ok(());
    };
    match phase.as_str() {
        "preparing" => {
            service.advance_service_restart(id).await?;
        }
        "recovering" => {
            for agent in pending {
                if attempted.insert((id, agent.clone())) {
                    if let Err(error) = service.request_service_restart_resume(id, &agent).await {
                        attempted.remove(&(id, agent));
                        return Err(error);
                    }
                    let service = service.clone();
                    resumes.spawn(async move {
                        let _ = service.resume_service_restart(id, &agent).await;
                    });
                }
            }
            service.refresh_service_restart(id).await?;
        }
        _ => {}
    }
    Ok(())
}
