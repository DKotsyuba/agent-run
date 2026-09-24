//! Durable ownership and conflict checks using inert identities rather than spawned processes.

use agent_run_platform::process::{Identity, OwnedProcess, OwnershipSnapshot};
use agent_run_store::Store;

/// Constructs inert process evidence; tests never signal these identities.
fn identity(pid: i32, token: &str) -> Identity {
    Identity {
        pid,
        ppid: 2,
        group: 10,
        birth: 123.0,
        token: token.into(),
        zombie: false,
    }
}

/// Creates a durable service owner without starting any command.
fn generation(store: &Store, id: &str) {
    store.conn.execute("INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at) VALUES (?1,?1,?2,'{}','starting','{}',1)", rusqlite::params![id,"0".repeat(64)]).unwrap();
}

/// Captured members survive reopening, retain PID reuse generations and cannot acquire a second owner.
#[test]
fn restored_members_are_durable_and_owner_conflicts_roll_back() {
    let home = tempfile::tempdir().unwrap();
    Store::initialize(home.path()).unwrap();
    let mut store = Store::open(home.path()).unwrap();
    generation(&store, "first");
    generation(&store, "second");
    let root = identity(10, "root");
    let old_child = identity(11, "child-old");
    let new_child = identity(11, "child-new");
    let snapshot = OwnershipSnapshot {
        leader: root.clone(),
        members: vec![root, old_child, new_child],
        descendants_observed: true,
    };
    store
        .remember_processes("service", "first", &snapshot)
        .unwrap();
    store
        .remember_processes("service", "first", &snapshot)
        .unwrap();
    assert!(store
        .remember_processes("service", "second", &snapshot)
        .is_err());
    assert!(store
        .remembered_processes("service", "second")
        .unwrap()
        .is_none());
    drop(store);
    let store = Store::open(home.path()).unwrap();
    let snapshot = store
        .remembered_processes("service", "first")
        .unwrap()
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(snapshot.members.len(), 3);
    assert!(snapshot.descendants_observed);
    assert!(store
        .remembered_processes("attempt", "historical-with-no-snapshot")
        .unwrap()
        .is_none());
    let mut broken = snapshot.clone();
    broken.members.clear();
    assert!(OwnedProcess::restore(broken).is_err());
    let mut broken = snapshot;
    broken.members.push(identity(1, "init"));
    assert!(OwnedProcess::restore(broken).is_err());
}
