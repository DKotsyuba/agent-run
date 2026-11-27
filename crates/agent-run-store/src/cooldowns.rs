//! Transactional release and independent audit of provider/account cooldowns.

use crate::{Result, Store, invalid, now};
use agent_run_domain::{
    ProviderId,
    catalog::{AccountId, BindingCooldownReason},
};
use rusqlite::{Connection, params};
use serde::Serialize;

/// An active provider/account cooldown as reported to an operator.
#[derive(Serialize)]
pub struct Cooldown {
    provider: String,
    account: String,
    reason: String,
    observed_at: f64,
    blocked_until: f64,
}

impl Store<'_> {
    /// Lists active cooldowns in provider/account order at the current clock.
    pub(crate) fn list_cooldowns(&self) -> Result<Vec<Cooldown>> {
        let mut query = self.conn.prepare(
            "SELECT provider_id,account_id,reason,observed_at,blocked_until
             FROM binding_cooldowns WHERE blocked_until>? ORDER BY provider_id,account_id",
        )?;
        Ok(query
            .query_map([now()], |row| {
                Ok(Cooldown {
                    provider: row.get(0)?,
                    account: row.get(1)?,
                    reason: row.get(2)?,
                    observed_at: row.get(3)?,
                    blocked_until: row.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?)
    }
}

impl Store<'_> {
    /// Clears the recorded binding and audits the authenticated operator in one transaction.
    pub(crate) fn clear_cooldown(
        &mut self,
        provider: &ProviderId,
        account: &AccountId,
        reason: &str,
        uid: u32,
    ) -> Result<bool> {
        let reason = reason.trim();
        if reason.is_empty() || reason.len() > 1024 || reason.chars().any(char::is_control) {
            return Err(invalid(
                "cooldown clear reason must be 1 to 1024 bytes without control characters",
            ));
        }
        let tx = crate::writer::WriteTx::begin(&self.conn)?;
        let cleared = release(&tx, provider, account, now(), reason, Some(uid), None)?;
        if cleared {
            Self::advance_quota_capacity_revision(&tx)?;
        }
        tx.commit()?;
        Ok(cleared)
    }
}

/// Releases only an active quota cooldown older than the already-fenced model observation.
/// Admission advances the capacity revision with its reservation changes.
pub(crate) fn recover(
    tx: &Connection,
    provider: &ProviderId,
    account: &AccountId,
    at: f64,
    observed: f64,
) -> Result<bool> {
    release(
        tx,
        provider,
        account,
        at,
        "quota_recovered",
        None,
        Some(observed),
    )
}

/// Copies the precise row into durable audit before deleting it on the caller's transaction.
fn release(
    tx: &Connection,
    provider: &ProviderId,
    account: &AccountId,
    at: f64,
    reason: &str,
    uid: Option<u32>,
    evidence: Option<f64>,
) -> Result<bool> {
    let changed = tx.execute(
        "INSERT INTO binding_cooldown_releases
           (provider_id,account_id,cooldown_reason,cooldown_observed_at,blocked_until,at,
            reason,operator_uid,evidence_observed_at)
         SELECT provider_id,account_id,reason,observed_at,blocked_until,?3,?4,?5,?6
         FROM binding_cooldowns WHERE provider_id=?1 AND account_id=?2
           AND (?6 IS NULL OR (reason=?7 AND observed_at<?6 AND blocked_until>?3))",
        params![
            provider.as_str(),
            account.as_str(),
            at,
            reason,
            uid,
            evidence,
            BindingCooldownReason::QuotaExhausted.as_str()
        ],
    )?;
    if changed == 0 {
        return Ok(false);
    }
    tx.execute(
        "DELETE FROM binding_cooldowns WHERE provider_id=?1 AND account_id=?2",
        params![provider.as_str(), account.as_str()],
    )?;
    Ok(true)
}
