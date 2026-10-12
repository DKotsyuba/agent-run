//! Offline and online writer fences on disposable private homes: a broker
//! exclusion binds the canonical home and both real bind locks, recorded
//! process identities are parsed strictly and proven gone before offline
//! authority is granted, and the online writer never follows a symlinked
//! state database into another home.
mod common;

use agent_run_store::writer::{BrokerExclusion, StoreWriter, WriterLease};
use fs2::FileExt;
use rusqlite::params;
use serde_json::json;

/// Holds an exclusive flock on `name` in `home`, like a serving broker.
fn hold(home: &std::path::Path, name: &str) -> std::fs::File {
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(home.join(name))
        .unwrap();
    file.lock_exclusive().unwrap();
    file
}

/// Takes the broker exclusion of `home` with its two legitimate sockets.
fn exclude(home: &std::path::Path) -> agent_run_domain::Result<BrokerExclusion> {
    BrokerExclusion::acquire(home, &[&home.join("api.sock"), &home.join("writer.sock")])
}

/// The socket list must be exactly the home's api.sock and writer.sock:
/// empty, incomplete, duplicate, foreign-name and cross-home lists are refused.
#[test]
fn broker_exclusion_requires_the_complete_legitimate_socket_set() {
    let a = common::Home::new();
    let b = common::Home::new();
    let api = a.path.join("api.sock");
    let writer = a.path.join("writer.sock");
    for sockets in [
        vec![],
        vec![api.as_path()],
        vec![api.as_path(), api.as_path()],
        vec![api.as_path(), writer.as_path(), writer.as_path()],
    ] {
        assert!(BrokerExclusion::acquire(&a.path, &sockets).is_err());
    }
    let other = a.path.join("other.sock");
    assert!(BrokerExclusion::acquire(&a.path, &[api.as_path(), other.as_path()]).is_err());
    let foreign = b.path.join("writer.sock");
    assert!(BrokerExclusion::acquire(&a.path, &[api.as_path(), foreign.as_path()]).is_err());
    assert!(BrokerExclusion::acquire(&a.path, &[writer.as_path(), api.as_path()]).is_ok());
}

/// An exclusion is refused while either real bind lock is held, and an
/// exclusion of one home cannot authorize another home; a symlinked path to
/// the same home is the same canonical home.
#[test]
fn broker_exclusion_binds_canonical_home_and_both_locks() {
    let a = common::Home::new();
    let b = common::Home::new();
    for name in [".api.sock.lock", ".writer.sock.lock"] {
        let held = hold(&a.path, name);
        assert!(matches!(
            exclude(&a.path),
            Err(agent_run_domain::Error::Conflict)
        ));
        drop(held);
    }
    let exclusion = exclude(&a.path).unwrap();
    assert!(WriterLease::acquire_exclusive(&b.path, exclusion).is_err());
    let link = a
        .temp
        .path()
        .parent()
        .unwrap()
        .join(format!("link-{}", uuid::Uuid::new_v4().simple()));
    std::os::unix::fs::symlink(&a.path, &link).unwrap();
    let exclusion = exclude(&a.path).unwrap();
    let guard = WriterLease::acquire_exclusive(&link, exclusion);
    std::fs::remove_file(&link).unwrap();
    assert!(guard.is_ok());
}

/// A forked child inherits open file descriptions, but parent guard drop must explicitly release exclusion.
/// The live parent guard still excludes contenders; the finite child is always reaped before outcome assertions.
#[cfg(unix)]
#[test]
fn broker_exclusion_drop_unlocks_while_inherited_child_is_alive() {
    let home = common::Home::new();
    let guard = exclude(&home.path).unwrap();
    let delay = libc::timespec {
        tv_sec: 2,
        tv_nsec: 0,
    };
    // SAFETY: after fork the child executes only async-signal-safe nanosleep/_exit
    // on preexisting stack data; it never enters Rust allocation, I/O or destructors.
    let pid = unsafe {
        let pid = libc::fork();
        if pid == 0 {
            libc::nanosleep(&delay, std::ptr::null_mut());
            libc::_exit(0);
        }
        pid
    };
    assert!(pid > 0, "finite fixture child must fork");
    let held = matches!(exclude(&home.path), Err(agent_run_domain::Error::Conflict));
    drop(guard);
    let reacquired = exclude(&home.path);
    let mut status = 0;
    // SAFETY: this is exactly the positive child PID returned by this test's fork.
    let observed = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) };
    let child_was_alive = observed == 0;
    let mut reaped = observed == pid;
    if !reaped {
        loop {
            // SAFETY: wait/reap only the same owned child, never a process group.
            let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
            if waited == pid {
                reaped = true;
                break;
            }
            if waited < 0 && std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                break;
            }
        }
    }
    assert!(
        reaped,
        "owned finite child must be reaped before assertions"
    );
    assert!(held, "parent's live exclusion must remain exclusive");
    assert!(
        child_was_alive,
        "reacquisition must be observed before inherited child exits"
    );
    assert!(
        reacquired.is_ok(),
        "parent guard drop must unlock inherited bind descriptions"
    );
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
}

/// Admits one active agent without a recorded startup owner (admission
/// records the admitting test process) and returns its id.
fn active_agent(home: &common::Home) -> String {
    let mut store = home.store();
    let (id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=NULL,startup_owner_birth_time=NULL WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    id.to_string()
}

/// Attempts offline authority on `home`.
fn offline(home: &common::Home) -> agent_run_domain::Result<()> {
    let exclusion = exclude(&home.path)?;
    WriterLease::acquire_exclusive(&home.path, exclusion).map(drop)
}

/// The identity of this test process.
fn me() -> agent_run_platform::process::Identity {
    agent_run_platform::process::inspect(std::process::id() as i32).unwrap()
}

/// Incomplete or malformed recorded identities refuse offline authority.
#[test]
fn incomplete_or_malformed_identities_fail_closed() {
    let home = common::Home::new();
    let id = active_agent(&home);
    home.store()
        .conn
        .execute(
            "UPDATE agents SET supervisor_pid=? WHERE id=?",
            params![12345, id],
        )
        .unwrap();
    assert!(offline(&home).is_err());
    home.store()
        .conn
        .execute(
            "UPDATE agents SET supervisor_pid=NULL,startup_owner_pid_identity='not json' WHERE id=?",
            [&id],
        )
        .unwrap();
    assert!(offline(&home).is_err());
    home.store()
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=? WHERE id=?",
            params![json!({"pid": 12345}).to_string(), id],
        )
        .unwrap();
    assert!(offline(&home).is_err());
    home.store()
        .conn
        .execute(
            "UPDATE agents SET startup_owner_pid_identity=NULL WHERE id=?",
            [&id],
        )
        .unwrap();
    assert!(offline(&home).is_ok());
    home.store()
        .conn
        .execute(
            "INSERT INTO managed_service_generations(id,service_id,revision,definition_json,state,broker_identity_json,created_at,process_identity_json) \
             VALUES('g1','svc',?1,'{}','ready','{}',1,?2)",
            params!["a".repeat(64), json!({"pid": 12345}).to_string()],
        )
        .unwrap();
    assert!(offline(&home).is_err());
}

/// Records `pid`/`token`/`birth` as the supervisor of agent `id`.
fn supervisor(home: &common::Home, id: &str, pid: i32, token: Option<&str>, birth: Option<f64>) {
    home.store()
        .conn
        .execute(
            "UPDATE agents SET supervisor_pid=?,supervisor_identity=?,supervisor_birth_time=? WHERE id=?",
            params![pid, token, birth, id],
        )
        .unwrap();
}

/// An exited child's identity: its pid and Rust token (`exited` when the
/// child was already gone at inspection). The child runs with a cleared
/// environment so no home or worker flag reaches it.
fn exited_child() -> (i32, String) {
    let mut child = std::process::Command::new("/usr/bin/true")
        .env_clear()
        .spawn()
        .unwrap();
    let pid = child.id() as i32;
    let token = agent_run_platform::process::inspect(pid)
        .map(|identity| identity.token)
        .unwrap_or_else(|_| "darwin:0:0".into());
    child.wait().unwrap();
    (pid, token)
}

/// A live recorded process refuses; a different Rust token (reuse), a
/// different legacy birth and an exited process are proven gone; an
/// arbitrary non-Rust token without a birth is unknown evidence and refuses.
#[test]
fn live_identities_refuse_and_reused_or_exited_are_gone() {
    let home = common::Home::new();
    let id = active_agent(&home);
    let me = me();
    supervisor(&home, &id, me.pid, Some(&me.token), None);
    assert!(offline(&home).is_err());
    supervisor(&home, &id, me.pid, Some("other-token"), None);
    assert!(offline(&home).is_err());
    supervisor(&home, &id, me.pid, None, None);
    assert!(offline(&home).is_err());
    supervisor(&home, &id, me.pid, Some("legacy-token"), Some(me.birth));
    assert!(offline(&home).is_err());
    supervisor(
        &home,
        &id,
        me.pid,
        Some("legacy-token"),
        Some(me.birth + 1.0),
    );
    assert!(offline(&home).is_ok());
    let reused = format!("{}:reused", me.token);
    let reused = if me.token.starts_with("darwin:") {
        "darwin:1:1"
    } else {
        reused.as_str()
    };
    supervisor(&home, &id, me.pid, Some(reused), None);
    assert!(offline(&home).is_ok());
    let (pid, token) = exited_child();
    supervisor(&home, &id, pid, Some(&token), None);
    assert!(offline(&home).is_ok());
}

/// The startup owner is checked independently of the supervisor: a live
/// owner refuses even when the recorded supervisor is exited or reused,
/// and a legacy text owner needs its birth column.
#[test]
fn startup_owner_is_checked_even_with_a_gone_supervisor() {
    let home = common::Home::new();
    let id = active_agent(&home);
    let me = me();
    let (pid, token) = exited_child();
    supervisor(&home, &id, pid, Some(&token), None);
    let owner = |raw: Option<String>, birth: Option<f64>| {
        home.store()
            .conn
            .execute(
                "UPDATE agents SET startup_owner_pid_identity=?,startup_owner_birth_time=? WHERE id=?",
                params![raw, birth, id],
            )
            .unwrap();
    };
    owner(Some(serde_json::to_string(&me).unwrap()), None);
    assert!(offline(&home).is_err());
    supervisor(&home, &id, me.pid, Some("darwin:1:1"), None);
    assert!(offline(&home).is_err());
    supervisor(&home, &id, pid, Some(&token), None);
    owner(Some(format!("{} legacy", me.pid)), Some(me.birth));
    assert!(offline(&home).is_err());
    owner(Some(format!("{} legacy", me.pid)), None);
    assert!(offline(&home).is_err());
    owner(Some(format!("{} legacy", me.pid)), Some(me.birth + 1.0));
    assert!(offline(&home).is_ok());
}

/// Replaces the home's database with `build` applied to a fresh file.
fn reshape(home: &common::Home, build: &str) {
    let path = home.path.join("state.db");
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
    }
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch(build)
        .unwrap();
}

/// Unknown database shapes refuse instead of skipping the proof; an empty
/// database and a supported historical shape without attempts or service
/// tables are proven from the columns they have.
#[test]
fn quiescence_reads_supported_shapes_and_refuses_unknown_ones() {
    let home = common::Home::new();
    reshape(&home, "CREATE TABLE foo(x)");
    assert!(offline(&home).is_err());
    reshape(
        &home,
        "CREATE TABLE agents(id TEXT, status TEXT, supervisor_pid INTEGER)",
    );
    assert!(offline(&home).is_err());
    reshape(
        &home,
        "CREATE TABLE agents(id TEXT, status TEXT, supervisor_pid INTEGER, supervisor_identity TEXT);\
         CREATE TABLE managed_service_generations(id TEXT, state TEXT)",
    );
    assert!(offline(&home).is_err());
    reshape(&home, "");
    assert!(offline(&home).is_ok());
    let me = me();
    reshape(
        &home,
        "CREATE TABLE agents(id TEXT, status TEXT, supervisor_pid INTEGER, supervisor_identity TEXT)",
    );
    rusqlite::Connection::open(home.path.join("state.db"))
        .unwrap()
        .execute(
            "INSERT INTO agents VALUES('a','running',?,?)",
            params![me.pid, me.token],
        )
        .unwrap();
    assert!(offline(&home).is_err());
    let fixture = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/fixtures/baseline/db/historical-v2.sqlite");
    reshape(&home, "");
    std::fs::copy(fixture, home.path.join("state.db")).unwrap();
    assert!(offline(&home).is_ok());
}

/// A state.db symlink into another home is refused before opening; the other
/// home's writer epoch stays unchanged while its own lease is held.
#[test]
fn writer_refuses_symlinked_state_database() {
    let a = tempfile::tempdir().unwrap();
    let b = common::Home::new();
    std::os::unix::fs::symlink(b.path.join("state.db"), a.path().join("state.db")).unwrap();
    let lease_b = WriterLease::acquire_online(&b.path).unwrap();
    let lease_a = WriterLease::acquire_online(a.path()).unwrap();
    assert!(StoreWriter::open(lease_a).is_err());
    let epochs: i64 = b
        .store()
        .conn
        .query_row("SELECT COUNT(*) FROM writer_authority", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(epochs, 0);
    drop(lease_b);
}

/// The test-only fenced helper registers the writer fence function on a raw
/// connection; an unfenced raw connection has no such function.
#[test]
fn fixture_fenced_registers_the_fence_function() {
    let home = common::Home::new();
    let raw = rusqlite::Connection::open(home.path.join("state.db")).unwrap();
    let probe = "SELECT agent_run_writer_fence()";
    assert!(raw.query_row(probe, [], |_| Ok(())).is_err());
    agent_run_store::fixture::fenced(&raw).unwrap();
    raw.query_row(probe, [], |_| Ok(())).unwrap();
    let store = agent_run_store::fixture::open(&home.path).unwrap();
    store.conn.query_row(probe, [], |_| Ok(())).unwrap();
}
