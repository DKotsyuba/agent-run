//! Writer-backed lineage holds; transport authorization remains with callers.

use crate::{Error, Result, broker_writer::BROKER_BUDGET, service::Service};
use agent_run_domain::{
    StoreId,
    error::invalid,
    lineage_hold::{HoldId, LineageHoldAcquire},
    lineage_hold_wire::{
        LineageHoldAcquireRequest, LineageHoldAcquireResponse, LineageHoldListRequest,
        LineageHoldListResponse, LineageHoldListState, LineageHoldLookupRequest,
        LineageHoldLookupResponse, LineageHoldReleaseRequest, LineageHoldReleaseResponse,
        LineageHoldWireObservation, LineageHoldWireReceipt, LineageHoldWireState,
    },
};
use agent_run_store::{
    lineage_holds::{
        LineageHoldAcquireResult, LineageHoldFilter, LineageHoldObservation, LineageHoldReceipt,
        LineageHoldRelease, LineageHoldState, LineageHoldsPage,
    },
    ops::{AcquireLineageHold, ReleaseLineageHold},
};
use serde::Serialize;
use serde_json::Value;

impl Service {
    /// Atomically proves and holds ordered successful lineages in the named store.
    ///
    /// A transport failure is not evidence that admission did not commit; recover
    /// with the original request ID and fingerprint. Exact Store replay preserves
    /// its original receipt and never reactivates a released hold.
    pub async fn acquire_lineage_hold(
        &self,
        acquire: LineageHoldAcquire,
    ) -> Result<LineageHoldAcquireResult> {
        self.lane("lineage-holds")?
            .local(AcquireLineageHold { acquire }, BROKER_BUDGET)
            .await
    }

    /// Verifies one release proof against the named store and permanent hold.
    ///
    /// Each new invocation gets a new writer envelope and must authenticate even
    /// when the hold is already released. Signature bytes are never returned.
    pub async fn release_lineage_hold(
        &self,
        store_id: StoreId,
        hold_id: HoldId,
        signature: [u8; 64],
    ) -> Result<Option<LineageHoldRelease>> {
        self.lane("lineage-holds")?
            .local(
                ReleaseLineageHold::new(hold_id, store_id, signature),
                BROKER_BUDGET,
            )
            .await
    }

    /// Reads committed hold history in one store-scoped snapshot without mutation.
    ///
    /// Absence does not rule out an in-flight acquire committing afterward.
    pub async fn lookup_lineage_hold(
        &self,
        store_id: StoreId,
        request_id: String,
        fingerprint: [u8; 32],
    ) -> Result<Option<LineageHoldObservation>> {
        self.read(move |store| store.lookup_lineage_hold(&store_id, &request_id, &fingerprint))
            .await
    }

    /// Lists one bounded store-scoped recovery page; pages are separate snapshots.
    pub async fn list_lineage_holds(
        &self,
        store_id: StoreId,
        filter: LineageHoldFilter,
        limit: usize,
        after: Option<HoldId>,
    ) -> Result<LineageHoldsPage> {
        self.read(move |store| store.list_lineage_holds(&store_id, filter, limit, after.as_ref()))
            .await
    }
}

/// Converts durable state without exposing the stored key or intent digest.
fn wire_state(state: LineageHoldState) -> LineageHoldWireState {
    match state {
        LineageHoldState::Active => LineageHoldWireState::Active,
        LineageHoldState::Released => LineageHoldWireState::Released,
    }
}

/// Converts one complete receipt to the bounded operator representation.
fn wire_receipt(receipt: LineageHoldReceipt) -> LineageHoldWireReceipt {
    LineageHoldWireReceipt {
        version: receipt.version,
        store_id: receipt.store_id,
        hold_id: receipt.hold_id,
        fingerprint: receipt.fingerprint.into(),
        targets: receipt.targets,
        canonical_roots: receipt.canonical_roots,
    }
}

/// Converts one store-scoped snapshot observation without changing its state.
fn wire_observation(observation: LineageHoldObservation) -> LineageHoldWireObservation {
    LineageHoldWireObservation {
        receipt: wire_receipt(observation.receipt),
        state: wire_state(observation.state),
    }
}

/// Rejects an oversized complete result without publishing a truncated receipt.
fn bounded<T: Serialize>(value: T) -> Result<Value> {
    let value = serde_json::to_value(value)?;
    if serde_json::to_vec(&value)?.len()
        > agent_run_domain::observation::SOCKET_MAX_FRAME_BYTES - 8192
    {
        return Err(Error::Runtime(
            "lineage hold response exceeds frame budget".into(),
        ));
    }
    Ok(value)
}

/// Parses only already raw-frame-checked operator requests with static errors.
fn args<T: serde::de::DeserializeOwned>(raw: Value) -> Result<T> {
    if !raw.is_object() {
        return Err(invalid("lineage hold arguments must be an object"));
    }
    serde_json::from_value(raw).map_err(|_| invalid("invalid lineage hold arguments"))
}

/// Dispatches an operator-only hold request; callers must authorize and validate
/// duplicate fields against the original raw frame before calling this helper.
pub(crate) async fn call(service: &Service, method: &str, raw: Value) -> Result<Value> {
    match method {
        "_lineage_hold_acquire" => {
            let request: LineageHoldAcquireRequest = args(raw)?;
            let result = service.acquire_lineage_hold(request.try_into()?).await?;
            bounded(LineageHoldAcquireResponse {
                receipt: wire_receipt(result.receipt),
                state: wire_state(result.state),
                replayed: result.replayed,
            })
        }
        "_lineage_hold_lookup" => {
            let request: LineageHoldLookupRequest = args(raw)?;
            request.validate()?;
            let store_id = request.expected_store_id;
            let result = service
                .lookup_lineage_hold(
                    store_id.clone(),
                    request.request_id,
                    request.fingerprint.into_bytes(),
                )
                .await?;
            bounded(LineageHoldLookupResponse::new(
                store_id,
                result.map(wire_observation),
            ))
        }
        "_lineage_hold_list" => {
            let request: LineageHoldListRequest = args(raw)?;
            request.validate()?;
            let store_id = request.expected_store_id;
            let filter = match request.state {
                LineageHoldListState::All => LineageHoldFilter::All,
                LineageHoldListState::Active => LineageHoldFilter::Active,
                LineageHoldListState::Released => LineageHoldFilter::Released,
            };
            let result = service
                .list_lineage_holds(
                    store_id.clone(),
                    filter,
                    request.limit,
                    request.after_hold_id,
                )
                .await?;
            bounded(LineageHoldListResponse {
                store_id,
                holds: result.holds.into_iter().map(wire_observation).collect(),
                next_after_hold_id: result.next_after_hold_id,
            })
        }
        "_lineage_hold_release" => {
            let request: LineageHoldReleaseRequest = args(raw)?;
            request.validate()?;
            let signature = request.signature_bytes();
            let store_id = request.expected_store_id;
            let hold_id = request.hold_id;
            let result = service
                .release_lineage_hold(store_id.clone(), hold_id.clone(), signature)
                .await?;
            if result.as_ref().is_some_and(|release| {
                release.hold_id != hold_id || release.state != LineageHoldState::Released
            }) {
                return Err(Error::Integrity(
                    "invalid lineage hold release result".into(),
                ));
            }
            bounded(LineageHoldReleaseResponse::new(
                store_id,
                result.map(|release| (release.hold_id, release.already_released)),
            ))
        }
        _ => Err(invalid("unknown lineage hold operation")),
    }
}
