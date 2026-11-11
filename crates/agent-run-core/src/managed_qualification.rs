//! Supervisor-only durable qualification after managed native checks.
#[cfg(target_os = "linux")]
use crate::error::invalid;
use crate::{Result, persist::Persist, state::Record};
use agent_run_adapters::{io::Process, managed_role::Selection};
#[cfg(target_os = "linux")]
use agent_run_domain::attempt_binding::{Incarnation, RoleQualification};

/// Re-inspect the leader and bind digests of the verified config and exact
/// inventory to the already durable R1 launch before the first model input.
pub(crate) async fn record(
    process: &mut Process,
    store: &mut Persist,
    record: &Record,
    selection: &Selection,
    harness: &str,
    config_digest: String,
    inventory_digest: String,
) -> Result<()> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (
            process,
            store,
            record,
            selection,
            harness,
            config_digest,
            inventory_digest,
        );
        Err(agent_run_domain::Error::Unsupported(
            "managed qualification requires Linux".into(),
        ))
    }
    #[cfg(target_os = "linux")]
    {
        let attempt = store
            .bound_attempt()
            .ok_or_else(|| invalid("managed attempt unavailable"))?
            .to_owned();
        let launch:String=store.conn.query_row("SELECT launch_id FROM attempt_launches WHERE execution_id=? AND attempt_id=? AND state='bound'",rusqlite::params![record.id.as_str(),attempt],|r|r.get(0))?;
        let leader = process
            .owner
            .leader
            .as_ref()
            .ok_or_else(|| invalid("managed leader unavailable"))?;
        let kernel = agent_run_platform::process::kernel_incarnation(leader)?;
        let qualification = RoleQualification {
            launch_id: launch,
            attempt_id: attempt.clone(),
            harness: harness.into(),
            role_digest: selection.role_digest.clone(),
            config_digest,
            inventory_digest,
            incarnation: Incarnation {
                host_id: selection.host_id.clone(),
                boot_id: kernel.boot_id,
                pid_namespace_inode: kernel.pid_namespace_inode,
                uid: kernel.uid,
                pid: kernel.pid,
                start_ticks: kernel.start_ticks,
            },
        };
        process
            .observing(store.qualify_attempt_role(&record.id, &attempt, qualification))
            .await?;
        if let Some(owner) = &process.native_owner_v2 {
            owner.enable();
        }
        Ok(())
    }
}
