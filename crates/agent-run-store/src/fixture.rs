//! Raw store access for tests only (feature `test-fixtures`).
//!
//! Production code never enables this feature: online writes go through the
//! writer service and offline writes through
//! [`crate::writer::OfflineWriterGuard`]. Tests that seed or inspect a
//! disposable private home with raw SQL use these helpers so their
//! connections carry the writer fence function and pass the mutation-fence
//! triggers exactly like the authorized writer connection.

use crate::Store;
use agent_run_domain::Result;
use rusqlite::Connection;
use std::path::Path;

/// Registers the writer fence function on a raw test connection, so its DML
/// passes the mutation-fence triggers.
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
