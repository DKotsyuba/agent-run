//! Database-bound public identity; never a secret or access credential.

use agent_run_domain::{Error, Result, StoreId};
use rusqlite::{Connection, OptionalExtension};

/// Reads the unique identity from an existing connection or transaction.
pub fn read(conn: &Connection) -> Result<StoreId> {
    let raw: Option<String> = conn
        .query_row(
            "SELECT store_id FROM store_identity WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .optional()?;
    raw.ok_or_else(|| Error::Integrity("store identity is missing".into()))?
        .parse()
        .map_err(|_| Error::Integrity("store identity is invalid".into()))
}

/// Rejects a scope mismatch without disclosing paths or asserting prior outcome.
pub fn require(conn: &Connection, expected: Option<&StoreId>) -> Result<StoreId> {
    let actual = read(conn)?;
    if expected.is_some_and(|expected| expected != &actual) {
        return Err(Error::StoreIdentityMismatch);
    }
    Ok(actual)
}
