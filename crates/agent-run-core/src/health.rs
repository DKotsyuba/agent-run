//! Supervisor-owned control probes and existing broker maintenance integration.
use crate::{Result, domain::AgentId, persist::Persist, process, service::Service};
use agent_run_adapters::io::Process;
use serde_json::{Value, json};

/// Consume a non-model ping or refuse stale health/pool corrections before input.
/// Pool-loop corrections may enter only the same still-active inference turn;
/// unlike opted-in health probes, they never wake an idle model.
/// The loop's exact native owner, attempt, idle boundary and current task work
/// are verified without invoking the model. Returns true only when this command
/// is completed here; ordinary commands and valid model probes remain with the
/// adapter's existing single native writer and delivery receipt logic.
pub async fn command(
    process: &mut Process,
    store: &mut Persist,
    run: &AgentId,
    command: (i64, &str, &Value),
    idle: bool,
) -> Result<bool> {
    let (id, kind, payload) = command;
    if kind == "steer" && payload.get("pool_read_guard").is_some() {
        if idle || !store.pool_poll_guard_current(run, payload)? {
            process
                .observing(store.complete_command(
                    run,
                    id,
                    &json!({"accepted":false,"reason":"stale_pool_read_guard"}),
                ))
                .await?;
            return Ok(true);
        }
        return Ok(false);
    }
    let model = kind == "steer" && payload.get("health_ping").is_some();
    if kind != "ping" && !model {
        return Ok(false);
    }
    let current = store.health_command_current(run, payload, model, crate::domain::now())?
        && (!model || idle);
    if kind == "ping" {
        let alive = process.owner.leader.as_ref().is_some_and(|owner| {
            process::observe(Some(owner.pid), Some(&owner.token), Some(owner.birth))
                == process::ProcessState::Alive
        });
        process.observing(store.complete_command(run,id,&json!({"accepted":current,"native_alive":current&&alive,"phase":if idle{"idle"}else{"running"}}))).await?;
        return Ok(true);
    }
    if !current {
        process
            .observing(store.complete_command(
                run,
                id,
                &json!({"accepted":false,"reason":"stale_health_probe"}),
            ))
            .await?;
        return Ok(true);
    }
    Ok(false)
}

impl Service {
    /// Inspect one fair due page through the existing writer lane. Historical
    /// attempts and disabled probes are excluded by the reader; every decision
    /// is revalidated in the writer. All model effects require frozen recover
    /// admission. The broker owns this awaited sweep; no detached model jobs.
    pub async fn health_tick(&self) -> Result<usize> {
        let at = crate::domain::now();
        let due = self.read(move |store| store.health_due(at, 100)).await?;
        let lane = self.lane("health")?;
        let mut checked = 0;
        for execution in due {
            let selected = execution.clone();
            let facts = match self
                .read(move |store| store.prepare_health(&selected))
                .await
            {
                Ok(facts) => facts,
                Err(error) => {
                    eprintln!("health activity read unavailable: {}", error.public().kind);
                    let selected = execution.clone();
                    match self
                        .read(move |store| store.prepare_health_control(&selected))
                        .await
                    {
                        Ok(facts) => facts,
                        Err(error) => {
                            eprintln!("health control read unavailable: {}", error.public().kind);
                            continue;
                        }
                    }
                }
            };
            let Some(facts) = facts else { continue };

            if lane
                .local(
                    agent_run_store::ops::HealthTick {
                        execution,
                        at,
                        facts,
                    },
                    crate::broker_writer::BROKER_BUDGET,
                )
                .await?
            {
                checked += 1;
            }
        }
        Ok(checked)
    }
}
