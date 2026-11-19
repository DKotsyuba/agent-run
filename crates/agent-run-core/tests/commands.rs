//! Core ports of Python supervisor command terminal-result behavior.
//!
//! These tests use SQLite temporary homes only; none require Unix sockets.

use crate::common;

use agent_run_core::{commands, domain::Outcome};
use serde_json::json;

/// Mirrors Python `test_supervisor.py::_drain_terminal_commands`.
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_final_drain_completes_late_cancel_steer_and_unknown`.
/// Mirrors Python `tests/test_supervisor.py::SupervisorTests::test_a_steer_without_the_capability_is_refused_not_dropped`.
#[test]
fn python_test_supervisor_terminal_commands_get_one_durable_result() {
    let home = common::Home::new();
    let mut store = home.store();
    let (agent_id, _) = store
        .admit(&home.request(), &home.config, &json!({}), None)
        .unwrap();
    store
        .enqueue(&agent_id, "steer", &json!({"text":"too late"}))
        .unwrap();
    store
        .finish(&agent_id, &Outcome::failure("fixture"), None, None)
        .unwrap();
    commands::complete_terminal(&mut store, &agent_id).unwrap();
    let (state, result): (String, String) = store
        .conn
        .query_row(
            "SELECT state,result_json FROM commands WHERE agent_id=?",
            [agent_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(state, "completed");
    assert_eq!(result, r#"{"accepted":false,"reason":"agent_terminal"}"#);
}

/// Legacy Run pools are never settled any more: a historical open pool whose
/// members all end with ready votes stays open and creates no common notice.
#[test]
fn complete_terminal_never_settles_a_legacy_pool() {
    use agent_run_core::commands;
    let home = common::Home::new();
    let config = agent_run_config::config::Config::load(&home.path).unwrap();
    let mut request = home.request();
    request.workdir = home.path.clone();
    request.validate().unwrap();
    let mut store = agent_run_store::fixture::open(&home.path).unwrap();
    let ids: Vec<_> = (0..2)
        .map(|_| {
            store
                .admit(&request, &config, &serde_json::json!({}), None)
                .unwrap()
                .0
        })
        .collect();
    let pool = "pool-20260101-000000-0123456789";
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES(?,'ns','r',lower(hex(zeroblob(32))),'goal','[{\"id\":\"done\",\"text\":\"ok\"}]','open',1,1.0)",
            [pool],
        )
        .unwrap();
    for (slot, id) in ids.iter().enumerate() {
        store
            .conn
            .execute(
                "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) VALUES(?,?,?,?,'r','t',1)",
                rusqlite::params![id.as_str(), pool, slot + 1, format!("m{slot}")],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active,phase,cleanup_proof_json) VALUES(?,?,1,'finished','{}',1.0,0,'cleanup_complete','{}')",
                rusqlite::params![format!("att_{}", format!("{slot}").repeat(24)), id.as_str()],
            )
            .unwrap();
    }
    let first_attempt = format!("att_{}", "0".repeat(24));
    store
        .conn
        .execute(
            "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,direction,kind,roster_revision,snapshot,body,sender_run_id,sender_attempt_id,idem_scope,request_id,created_at) \
             VALUES(?,'member',?,'m','r','team','proposal',1,'result','p',?,?,'s','p',1.0)",
            rusqlite::params![pool, ids[0].as_str(), ids[0].as_str(), first_attempt],
        )
        .unwrap();
    for (slot, id) in ids.iter().enumerate() {
        store
            .conn
            .execute(
                "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,direction,kind,roster_revision,proposal_seq,decision,checks_json,body,sender_run_id,sender_attempt_id,idem_scope,request_id,created_at) \
                 VALUES(?,'member',?,'m','r','team','vote',1,1,'ready','[{\"criterion_id\":\"done\",\"status\":\"met\",\"evidence\":\"x\"}]','v',?,?,?,'v',2.0)",
                rusqlite::params![pool, id.as_str(), id.as_str(), format!("att_{}", format!("{slot}").repeat(24)), id.as_str()],
            )
            .unwrap();
    }
    // Both members end; the historical pool is inert either way.
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?",
            [ids[0].as_str()],
        )
        .unwrap();
    commands::complete_terminal(&mut store, &ids[0]).unwrap();
    let state = |store: &agent_run_core::state::Store| -> String {
        store
            .conn
            .query_row("SELECT state FROM pools", [], |row| row.get(0))
            .unwrap()
    };
    assert_eq!(state(&store), "open");
    store
        .conn
        .execute(
            "UPDATE agents SET status='succeeded' WHERE id=?",
            [ids[1].as_str()],
        )
        .unwrap();
    commands::complete_terminal(&mut store, &ids[1]).unwrap();
    assert_eq!(state(&store), "open");
    let deliveries: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM deliveries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(deliveries, 0);
}
