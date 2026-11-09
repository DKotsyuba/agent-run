//! Synthetic database identity, scope and backup invariants.

mod common;

use agent_run_domain::{
    Error, StoreId,
    storage::{RejectStage, StorageRejection},
};
use agent_run_store::store_identity;
use rusqlite::{Connection, MAIN_DB};

/// New databases differ; reopening and SQLite backup preserve the same identity.
#[test]
fn database_identity_is_canonical_stable_and_cloned_with_backup() {
    let first = common::Home::new();
    let second = common::Home::new();
    let store = first.store();
    let id = store_identity::read(&store.conn).unwrap();
    let other = store_identity::read(&second.store().conn).unwrap();
    assert_ne!(id, other);
    assert_eq!(id.as_str().len(), 38);
    assert_eq!(id.as_str().parse::<StoreId>().unwrap(), id);
    for invalid in [
        "",
        "store_bad",
        "store_0123456789ABCDEF0123456789abcdef",
        "store_0123456789abcdef0123456789abcdef0",
    ] {
        assert!(invalid.parse::<StoreId>().is_err());
    }
    assert!(
        store
            .conn
            .execute("UPDATE store_identity SET store_id=?", [other.as_str()])
            .is_err()
    );
    assert!(
        store
            .conn
            .execute("DELETE FROM store_identity", [])
            .is_err()
    );
    let backup = first.path.join("synthetic-backup.sqlite");
    store.conn.backup(MAIN_DB, &backup, None).unwrap();
    let copy = Connection::open(&backup).unwrap();
    assert_eq!(store_identity::read(&copy).unwrap(), id);
    drop(copy);
    drop(store);
    assert_eq!(store_identity::read(&first.store().conn).unwrap(), id);
}

/// A mismatch keeps its stable class through the sole-writer rejection boundary.
#[test]
fn identity_mismatch_survives_writer_error_roundtrip() {
    let rejection =
        StorageRejection::from_error(&Error::StoreIdentityMismatch, RejectStage::Statement);
    let error = rejection.into_error();
    assert!(matches!(error, Error::StoreIdentityMismatch));
    assert_eq!(error.machine_code().as_str(), "StoreIdentityMismatch");
    assert!(!error.public().message.contains('/'));
}
