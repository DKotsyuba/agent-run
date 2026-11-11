//! Durable effect identity, reserve/write fence and recovery for owner-v2.
//! Transport authentication and live-target checks are the caller's duty.
use crate::{Store, writer::WriteTx};
use agent_run_domain::{
    Error, Result, StoreId,
    attempt_binding::{Route, validate_digest, validate_id, validate_uuid},
    error::invalid,
    owner_v2::{OutboxIntent, outbox_digest},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_EFFECTS: i64 = 100_000;
/// Content-free effect projection. `reserved` and `invoking` read as unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectView {
    pub producer_scope: String,
    pub effect_id: String,
    pub effect_digest: String,
    pub state: String,
    pub protocol: Option<String>,
    pub native_id: Option<String>,
    pub reason: Option<String>,
}
/// A fresh durable token permits exactly one attempt to cross the invoking fence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reservation {
    pub view: EffectView,
    pub dispatch_token: Option<String>,
}

/// Operator-pinned original routes; never taken from a custody request frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectRoute {
    pub run_route: Route,
    pub bridge_route: Route,
}
impl EffectRoute {
    fn validate(&self, store: &StoreId) -> Result<()> {
        self.run_route.validate()?;
        self.bridge_route.validate()?;
        validate_uuid(&self.bridge_route.component_store_id)?;
        if self.run_route.component_store_id != store.to_string()
            || self.run_route.worker_uid != self.bridge_route.worker_uid
        {
            return Err(Error::Conflict);
        }
        Ok(())
    }
    fn from_outbox(outbox: &OutboxIntent) -> Self {
        Self {
            run_route: outbox.target_binding.run_route.clone(),
            bridge_route: outbox.bridge_route.clone(),
        }
    }
}

type Row = (
    String,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    String,
);
fn row(conn: &Connection, scope: &str, id: &str, digest: &str) -> Result<Option<Row>> {
    validate_id(scope)?;
    validate_id(id)?;
    validate_digest(digest)?;
    let row:Option<Row>=conn.query_row("SELECT effect_digest,outbox_json,dispatch_token,state,protocol,native_id,reason,run_route_json,bridge_route_json FROM owner_effects WHERE producer_scope=? AND effect_id=?",params![scope,id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
    if row.as_ref().is_some_and(|r| r.0 != digest) {
        return Err(Error::Conflict);
    }
    Ok(row)
}
fn check_route(row: &Row, route: &EffectRoute) -> Result<()> {
    if row.7 != serde_json::to_string(&route.run_route)?
        || row.8 != serde_json::to_string(&route.bridge_route)?
    {
        return Err(Error::Conflict);
    }
    Ok(())
}
fn view(
    scope: &str,
    id: &str,
    digest: &str,
    state: &str,
    protocol: Option<String>,
    native_id: Option<String>,
    reason: Option<String>,
) -> EffectView {
    let (state, reason) = match state {
        "reserved" | "invoking" => ("unknown", Some("native_uncertain".into())),
        other => (other, reason),
    };
    EffectView {
        producer_scope: scope.into(),
        effect_id: id.into(),
        effect_digest: digest.into(),
        state: state.into(),
        protocol,
        native_id,
        reason,
    }
}
fn result(scope: &str, id: &str, digest: &str, row: &Row) -> EffectView {
    view(
        scope,
        id,
        digest,
        &row.3,
        row.4.clone(),
        row.5.clone(),
        row.6.clone(),
    )
}
fn capacity(conn: &Connection) -> Result<()> {
    if conn.query_row("SELECT count(*) FROM owner_effects", [], |r| {
        r.get::<_, i64>(0)
    })? >= MAX_EFFECTS
    {
        return Err(Error::Capacity);
    }
    Ok(())
}
fn reason(value: &str) -> bool {
    matches!(
        value,
        "actor_mismatch"
            | "not_idle"
            | "awaiting_input"
            | "finished"
            | "budget"
            | "cancelled"
            | "unavailable"
            | "invalid_request"
            | "capacity"
            | "conflict"
            | "attempt_not_ready"
            | "attempt_mismatch"
            | "qualification_pending"
            | "native_uncertain"
            | "channel_failed"
    )
}

impl Store<'_> {
    /// Check that a supervisor operation remains scoped to the frozen target.
    pub fn owner_effect_matches_target(
        &self,
        scope: &str,
        id: &str,
        digest: &str,
        execution: &agent_run_domain::domain::AgentId,
        attempt: &str,
    ) -> Result<bool> {
        validate_id(scope)?;
        validate_id(id)?;
        validate_digest(digest)?;
        validate_id(attempt)?;
        Ok(self.conn.query_row("SELECT EXISTS(SELECT 1 FROM owner_effects WHERE producer_scope=? AND effect_id=? AND effect_digest=? AND json_extract(outbox_json,'$.target_binding.attempt.execution_id')=? AND json_extract(outbox_json,'$.target_binding.attempt.attempt_id')=?)",params![scope,id,digest,execution.as_str(),attempt],|r|r.get(0))?)
    }
    /// Read one pinned effect. Absence is not an abort or a no-effect proof.
    pub fn owner_effect_lookup(
        &self,
        store: &StoreId,
        scope: &str,
        id: &str,
        digest: &str,
        route: &EffectRoute,
    ) -> Result<EffectView> {
        route.validate(store)?;
        crate::store_identity::require(&self.conn, Some(store))?;
        Ok(match row(&self.conn, scope, id, digest)? {
            Some(found) => {
                check_route(&found, route)?;
                result(scope, id, digest, &found)
            }
            None => view(scope, id, digest, "absent", None, None, None),
        })
    }
    /// Reserve a fresh dispatch or atomically re-reserve a fenced known-unsent
    /// identical effect. Existing reserved/invoking/unknown/accepted/aborted
    /// never return a dispatch token and therefore never authorize another write.
    pub fn owner_effect_reserve(
        &self,
        store: &StoreId,
        outbox: &OutboxIntent,
        digest: &str,
    ) -> Result<Reservation> {
        self.owner_effect_reserve_gated(store, outbox, digest, true)
    }
    /// Disabled mode returns a prior terminal/uncertain result but cannot
    /// reserve an absent effect or reopen a known-unsent effect.
    pub fn owner_effect_reserve_gated(
        &self,
        store: &StoreId,
        outbox: &OutboxIntent,
        digest: &str,
        enabled: bool,
    ) -> Result<Reservation> {
        self.owner_effect_reserve_inner(store, outbox, digest, enabled, false)
    }
    /// Recheck bounded local configuration after the writer transaction starts.
    pub fn owner_effect_reserve_managed(
        &self,
        store: &StoreId,
        outbox: &OutboxIntent,
        digest: &str,
        enabled: bool,
    ) -> Result<Reservation> {
        self.owner_effect_reserve_inner(store, outbox, digest, enabled, true)
    }
    fn owner_effect_reserve_inner(
        &self,
        store: &StoreId,
        outbox: &OutboxIntent,
        digest: &str,
        enabled: bool,
        revalidate: bool,
    ) -> Result<Reservation> {
        outbox.validate()?;
        validate_digest(digest)?;
        if outbox_digest(outbox)? != digest || &outbox.target_binding.attempt.run_store_id != store
        {
            return Err(Error::Conflict);
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        let (scope, id) = (&outbox.producer_scope, &outbox.effect_id);
        let encoded = serde_json::to_string(outbox)?;
        let route = EffectRoute::from_outbox(outbox);
        route.validate(store)?;
        let currently_enabled = || {
            enabled
                && (!revalidate
                    || agent_run_config::provider_config::ProviderConfig::load(&self.home)
                        .ok()
                        .and_then(|(config, _)| config.managed_attempt_binding)
                        .is_some_and(|settings| settings.enabled))
        };
        if let Some(prior) = row(&tx, scope, id, digest)? {
            check_route(&prior, &route)?;
            if prior.1.as_deref().is_some_and(|s| s != encoded) {
                return Err(Error::Conflict);
            }
            if prior.3 != "known_unsent" {
                let result = Reservation {
                    view: result(scope, id, digest, &prior),
                    dispatch_token: None,
                };
                tx.commit()?;
                return Ok(result);
            }
            if !currently_enabled() {
                return Err(Error::Unsupported(
                    "managed attempt binding disabled".into(),
                ));
            }
            let token = uuid::Uuid::new_v4().to_string();
            if tx.execute("UPDATE owner_effects SET state='reserved',dispatch_token=?,reason=NULL WHERE producer_scope=? AND effect_id=? AND effect_digest=? AND state='known_unsent'",params![token,scope,id,digest])?!=1{return Err(Error::Conflict)}
            tx.commit()?;
            return Ok(Reservation {
                view: view(
                    scope,
                    id,
                    digest,
                    "unknown",
                    None,
                    None,
                    Some("native_uncertain".into()),
                ),
                dispatch_token: Some(token),
            });
        }
        if !currently_enabled() {
            return Err(Error::Unsupported(
                "managed attempt binding disabled".into(),
            ));
        }
        capacity(&tx)?;
        let token = uuid::Uuid::new_v4().to_string();
        tx.execute("INSERT INTO owner_effects(producer_scope,effect_id,effect_digest,run_route_json,bridge_route_json,outbox_json,dispatch_token,state) VALUES(?,?,?,?,?,?,?,'reserved')",params![scope,id,digest,serde_json::to_string(&route.run_route)?,serde_json::to_string(&route.bridge_route)?,encoded,token])?;
        tx.commit()?;
        Ok(Reservation {
            view: view(
                scope,
                id,
                digest,
                "unknown",
                None,
                None,
                Some("native_uncertain".into()),
            ),
            dispatch_token: Some(token),
        })
    }
    /// Commit invoking BEFORE a possible native write. Abort that won first
    /// makes this CAS fail; an expired or cancelled task is never assumed absent.
    pub fn owner_effect_invoke(
        &self,
        store: &StoreId,
        scope: &str,
        id: &str,
        digest: &str,
        token: &str,
    ) -> Result<()> {
        validate_id(token)?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        let prior = row(&tx, scope, id, digest)?.ok_or(Error::Conflict)?;
        if prior.3 != "reserved" || prior.2.as_deref() != Some(token) {
            return Err(Error::Conflict);
        }
        if tx.execute("UPDATE owner_effects SET state='invoking' WHERE producer_scope=? AND effect_id=? AND effect_digest=? AND dispatch_token=? AND state='reserved'",params![scope,id,digest,token])?!=1{return Err(Error::Conflict)}
        tx.commit()
    }
    /// Record one original lane outcome. `known_unsent` is permitted only when
    /// the sole lane has fenced all late writers; caller must have that proof.
    #[allow(clippy::too_many_arguments)]
    pub fn owner_effect_finish(
        &self,
        store: &StoreId,
        scope: &str,
        id: &str,
        digest: &str,
        token: &str,
        state: &str,
        protocol: Option<&str>,
        native_id: Option<&str>,
        why: Option<&str>,
    ) -> Result<EffectView> {
        validate_id(token)?;
        match state {
            "accepted"
                if matches!(protocol, Some("codex_turn" | "claude_input"))
                    && native_id.is_some_and(|v| validate_id(v).is_ok())
                    && why.is_none() => {}
            "known_unsent"
                if protocol.is_none()
                    && native_id.is_none()
                    && why.is_some_and(|v| {
                        reason(v) && !matches!(v, "native_uncertain" | "channel_failed")
                    }) => {}
            "unknown"
                if protocol.is_none()
                    && native_id.is_none()
                    && why.is_some_and(|v| matches!(v, "native_uncertain" | "channel_failed")) => {}
            _ => return Err(invalid("invalid owner outcome")),
        }
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        let prior = row(&tx, scope, id, digest)?.ok_or(Error::Conflict)?;
        if prior.2.as_deref() != Some(token) {
            return Err(Error::Conflict);
        }
        if prior.3 == state
            && prior.4.as_deref() == protocol
            && prior.5.as_deref() == native_id
            && prior.6.as_deref() == why
        {
            let result = result(scope, id, digest, &prior);
            tx.commit()?;
            return Ok(result);
        }
        let allowed = matches!(
            (prior.3.as_str(), state),
            ("reserved", "known_unsent")
                | ("invoking", "known_unsent" | "unknown" | "accepted")
                | ("unknown", "accepted")
        );
        if !allowed {
            return Err(Error::Conflict);
        }
        if tx.execute("UPDATE owner_effects SET state=?,protocol=?,native_id=?,reason=? WHERE producer_scope=? AND effect_id=? AND effect_digest=? AND dispatch_token=? AND state=?",params![state,protocol,native_id,why,scope,id,digest,token,prior.3])?!=1{return Err(Error::Conflict)}
        tx.commit()?;
        Ok(view(
            scope,
            id,
            digest,
            state,
            protocol.map(str::to_owned),
            native_id.map(str::to_owned),
            why.map(str::to_owned),
        ))
    }
    /// Atomically fence an absent or not-yet-invoking effect. If invoking won,
    /// return unknown without pretending that the native write was cancelled.
    pub fn owner_effect_abort(
        &self,
        store: &StoreId,
        scope: &str,
        id: &str,
        digest: &str,
        route: &EffectRoute,
    ) -> Result<EffectView> {
        route.validate(store)?;
        let tx = WriteTx::begin(&self.conn)?;
        crate::store_identity::require(&tx, Some(store))?;
        let Some(prior) = row(&tx, scope, id, digest)? else {
            capacity(&tx)?;
            tx.execute("INSERT INTO owner_effects(producer_scope,effect_id,effect_digest,run_route_json,bridge_route_json,state) VALUES(?,?,?,?,?,'aborted')",params![scope,id,digest,serde_json::to_string(&route.run_route)?,serde_json::to_string(&route.bridge_route)?])?;
            tx.commit()?;
            return Ok(view(scope, id, digest, "aborted", None, None, None));
        };
        check_route(&prior, route)?;
        if matches!(prior.3.as_str(), "reserved" | "known_unsent") {
            if tx.execute("UPDATE owner_effects SET state='aborted',reason=NULL WHERE producer_scope=? AND effect_id=? AND effect_digest=? AND state=?",params![scope,id,digest,prior.3])?!=1{return Err(Error::Conflict)}
            tx.commit()?;
            return Ok(view(scope, id, digest, "aborted", None, None, None));
        }
        let result = result(scope, id, digest, &prior);
        tx.commit()?;
        Ok(result)
    }
}
