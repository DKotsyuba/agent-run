//! Raw store access for tests only (feature `test-fixtures`).
//!
//! Production code never enables this feature: online writes go through the
//! writer service and offline writes through
//! [`crate::writer::OfflineWriterGuard`]. Tests that seed or inspect a
//! disposable private home with raw SQL use these helpers so their
//! connections carry the same compatibility marker as the authorized writer.
//! Direct SQL access is trusted and is not authorized by that marker.

use crate::Store;
use agent_run_domain::Result;
use rusqlite::Connection;
use std::path::Path;

/// Causes one provider owner inspection to fail after `successful` probes on this thread.
pub fn fail_owner_inspection_after(successful: usize) {
    crate::provider_admission::fixture_fail_owner_after(successful);
}

/// Registers the writer's compatibility marker on a raw test connection.
pub fn fenced(conn: &Connection) -> Result<()> {
    crate::writer::register_fence(conn)
}

/// Opens the existing store of a test home with a fenced connection.
pub fn open(home: &Path) -> Result<Store<'static>> {
    let store = Store::connect(home, false)?;
    fenced(&store.conn)?;
    Ok(store)
}

/// Initializes (creating or migrating) the store of a test home with a
/// fenced connection.
pub fn initialize(home: &Path) -> Result<Store<'static>> {
    let store = Store::initialize(home)?;
    fenced(&store.conn)?;
    Ok(store)
}
