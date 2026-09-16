//! Durable lifecycle recovery and observer-facing waiting primitives.

/// Evidence-gated recovery of rows abandoned by a dead broker or supervisor.
pub mod reconcile;
