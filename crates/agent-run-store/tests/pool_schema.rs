//! Schema-25 contract: pool tables enforce membership history, author-at-send
//! immutability and purge ordering, and the committed fixture agrees with its manifest.
use rusqlite::{Connection, params};
use std::path::Path;

const DB_DIR: &str = "../../tests/fixtures/baseline/db";
const POOL: &str = "pool-20261003-120000-0123456789";
const A1: &str = "ag-20260101-000001-0000000001";
const A2: &str = "ag-20260101-000002-0000000002";
const A3: &str = "ag-20260101-000003-0000000003";

/// A writable copy of the committed v25 fixture with foreign keys enforced, holding one open pool.
fn pool_db(dir: &Path) -> Connection {
    let path = dir.join("state.db");
    std::fs::copy(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(DB_DIR)
            .join("current-v25.sqlite"),
        &path,
    )
    .unwrap();
    let conn = Connection::open(path).unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    conn.execute(
        "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,\
         state,roster_revision,created_at) VALUES (?1,'ns','req',?2,'goal','[]','open',1,1.0)",
        params![POOL, "0".repeat(64)],
    )
    .unwrap();
    conn
}

/// Inserts a current member in `slot`.
fn member(conn: &Connection, agent: &str, slot: i64, name: &str) -> rusqlite::Result<usize> {
    conn.execute(
        "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision)\
         VALUES (?1,?2,?3,?4,'reviewer','t',1)",
        params![agent, POOL, slot, name],
    )
}

/// Inserts an entry from column values given as SQL fragments after the pool id.
fn entry(conn: &Connection, columns: &str, values: &str) -> rusqlite::Result<usize> {
    conn.execute(
        &format!(
            "INSERT INTO pool_entries(pool_id,roster_revision,body,idem_scope,request_id,created_at,{columns}) \
             VALUES ('{POOL}',1,'b','s',lower(hex(randomblob(4))),1.0,{values})"
        ),
        [],
    )
}

/// Historical members stay, only one row per slot and name is current, identity is frozen.
#[test]
fn membership_history_keeps_one_current_member_per_slot() {
    let dir = tempfile::tempdir().unwrap();
    let mut conn = pool_db(dir.path());
    member(&conn, A1, 1, "Ada").unwrap();
    assert!(member(&conn, A2, 1, "Bob").is_err(), "slot already current");
    assert!(member(&conn, A2, 2, "Ada").is_err(), "name already current");
    assert!(
        conn.execute(
            "UPDATE pool_members SET replaced_by=?2 WHERE agent_id=?1",
            [A1, A2]
        )
        .is_err(),
        "a successor must exist by commit"
    );
    // Replacement: retire the old row and insert its successor in one transaction.
    let tx = conn.transaction().unwrap();
    tx.execute(
        "UPDATE pool_members SET replaced_by=?2 WHERE agent_id=?1",
        [A1, A2],
    )
    .unwrap();
    member(&tx, A2, 1, "Bob").unwrap();
    tx.commit().unwrap();
    assert!(
        conn.execute(
            "UPDATE pool_members SET replaced_by=?2 WHERE agent_id=?1",
            [A1, A1]
        )
        .is_err()
    );
    assert!(
        conn.execute(
            "UPDATE pool_members SET replaced_by=?2 WHERE agent_id=?1",
            [A1, A3]
        )
        .is_err()
    );
    assert!(
        conn.execute("UPDATE pool_members SET name='X' WHERE agent_id=?1", [A1])
            .is_err()
    );
    assert!(
        conn.execute("UPDATE pool_members SET slot=2 WHERE agent_id=?1", [A2])
            .is_err()
    );
    let all: i64 = conn
        .query_row("SELECT COUNT(*) FROM pool_members", [], |r| r.get(0))
        .unwrap();
    assert_eq!(all, 2, "replaced member row retained");
    let current: String = conn
        .query_row(
            "SELECT agent_id FROM pool_members WHERE slot=1 AND replaced_by IS NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(current, A2);
}

/// Entry checks reject inconsistent authors, links and duplicates; rows are immutable.
#[test]
fn entries_enforce_author_shape_links_and_immutability() {
    let dir = tempfile::tempdir().unwrap();
    let conn = pool_db(dir.path());
    member(&conn, A3, 1, "Ada").unwrap();
    let cols = "author_kind,author_agent_id,author_name,author_role,sender_run_id,sender_attempt_id,\
                direction,kind,severity,delivery_id";
    let ok = |extra: &str| format!("'member','{A3}','Ada','reviewer','{A3}','att_03',{extra}");
    entry(
        &conn,
        cols,
        &ok("'orchestrator_copy','report','risk','delivery-succeeded'"),
    )
    .unwrap();
    assert!(
        entry(
            &conn,
            cols,
            &ok("'orchestrator_copy','report','risk','delivery-succeeded'")
        )
        .is_err(),
        "one entry per linked delivery"
    );
    entry(&conn, cols, &ok("'team','message',NULL,NULL")).unwrap();
    for (label, values) in [
        (
            "member without sender",
            format!("'member','{A3}','Ada','reviewer',NULL,NULL,'team','message',NULL,NULL"),
        ),
        (
            "operator with author",
            format!("'operator','{A3}','Ada','reviewer',NULL,NULL,'team','message',NULL,NULL"),
        ),
        (
            "operator vote",
            "'operator',NULL,NULL,NULL,NULL,NULL,'team','roster',NULL,NULL".into(),
        ),
        (
            "team copy of a message",
            ok("'orchestrator_copy','message',NULL,NULL"),
        ),
        ("report without severity", ok("'team','report',NULL,NULL")),
        ("vote without decision", ok("'team','vote',NULL,NULL")),
        (
            "author outside the pool",
            format!("'member','{A1}','Ada','reviewer','{A1}','att_03','team','message',NULL,NULL"),
        ),
    ] {
        assert!(entry(&conn, cols, &values).is_err(), "{label}");
    }
    entry(
        &conn,
        "author_kind,direction,kind",
        "'operator','team','message'",
    )
    .unwrap();
    entry(
        &conn,
        "author_kind,direction,kind",
        "'broker','team','roster'",
    )
    .unwrap();
    assert!(
        conn.execute("UPDATE pool_entries SET body='x'", [])
            .is_err()
    );
    conn.execute(
        "INSERT INTO pool_entries(pool_id,roster_revision,body,idem_scope,request_id,created_at,author_kind,direction,kind)\
         VALUES (?1,1,'b','op','same',1.0,'operator','team','message')",
        [POOL],
    )
    .unwrap();
    assert!(conn
        .execute(
            "INSERT INTO pool_entries(pool_id,roster_revision,body,idem_scope,request_id,created_at,author_kind,direction,kind)\
             VALUES (?1,1,'b','op','same',1.0,'operator','team','message')",
            [POOL],
        )
        .is_err());
}

/// Agents cannot be purged while pool rows reference them; pool rows go first, and indexes exist.
#[test]
fn purge_order_and_indexes_support_reference_aware_retention() {
    let dir = tempfile::tempdir().unwrap();
    let conn = pool_db(dir.path());
    member(&conn, A3, 1, "Ada").unwrap();
    conn.execute("UPDATE agents SET status=status WHERE id=?1", [A3])
        .unwrap();
    assert!(
        conn.execute("DELETE FROM agents WHERE id=?1", [A3])
            .is_err()
    );
    conn.execute("DELETE FROM pool_members WHERE pool_id=?1", [POOL])
        .unwrap();
    conn.execute("DELETE FROM pools WHERE id=?1", [POOL])
        .unwrap();
    for index in [
        "idx_pool_entries_pool_seq",
        "idx_pool_members_current_slot",
        "idx_pool_members_current_name",
    ] {
        let found: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?1",
                [index],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(found, 1, "{index}");
    }
}

/// The manifest entry for the v25 fixture agrees with the database on version and every row count.
#[test]
fn v25_manifest_entry_matches_fixture() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(DB_DIR);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("manifest.json")).unwrap()).unwrap();
    assert_eq!(manifest["schema_version"], 25);
    let entry = manifest["files"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["file"] == "current-v25.sqlite")
        .expect("v25 entry");
    let conn = Connection::open_with_flags(
        root.join("current-v25.sqlite"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |r| r.get(0))
        .unwrap();
    assert_eq!((version, entry["user_version"].as_i64()), (25, Some(25)));
    let rows = entry["rows"].as_object().unwrap();
    let tables: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        rows.len() as i64,
        tables,
        "every table has a recorded count"
    );
    for (table, count) in rows {
        let actual: i64 = conn
            .query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(Some(actual), count.as_i64(), "{table}");
    }
}
