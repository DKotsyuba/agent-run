//! Core ports of Python supervisor command terminal-result behavior.
//!
//! These tests use SQLite temporary homes only; none require Unix sockets.

mod common;

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

/// The shared pool delivery loader refuses honestly for replaced members and
/// foreign entries, and renders current-member entries verbatim.
#[test]
fn pool_entry_text_validates_membership_and_renders() {
    use agent_run_core::commands;
    let home = common::Home::new();
    let config = agent_run_config::config::Config::load(&home.path).unwrap();
    let mut request = home.request();
    request.workdir = home.path.clone();
    request.validate().unwrap();
    let mut store = agent_run_core::state::Store::open(&home.path).unwrap();
    let (member, _) = store
        .admit(&request, &config, &serde_json::json!({}), None)
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES('pool-20260101-000000-0123456789','ns','r1','0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef','goal','[]','open',1,1.0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision) \
             VALUES(?,'pool-20260101-000000-0123456789',1,'alice','doer','task',1)",
            [member.as_str()],
        )
        .unwrap();
    let attempt = format!("att_{}", "e".repeat(24));
    store
        .conn
        .execute(
            "INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active)              VALUES(?,?,1,'running','{}',1.0,1)",
            rusqlite::params![attempt, member.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_entries(pool_id,author_kind,author_agent_id,author_name,author_role,direction,kind,roster_revision,body,sender_run_id,sender_attempt_id,idem_scope,request_id,created_at) \
             VALUES('pool-20260101-000000-0123456789','member',?,'alice','doer','team','message',1,'hello',?,?, 'scope-1','r-1',1.0)",
            rusqlite::params![member.as_str(), member.as_str(), attempt],
        )
        .unwrap();
    let seq: i64 = store
        .conn
        .query_row("SELECT seq FROM pool_entries", [], |row| row.get(0))
        .unwrap();
    let text = commands::pool_entry_text(&store, &member, seq)
        .unwrap()
        .expect("current member renders");
    assert!(text.contains("alice (doer,"), "{text}");
    assert!(text.contains("hello"), "{text}");
    // A replaced member refuses honestly and the log keeps the entry.
    let replacement: agent_run_core::domain::AgentId =
        "ag-20260101-000000-0000000009".parse().unwrap();
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision)              VALUES(?,?,?,?,?,?,?,?, 'succeeded', 2.0, 10.0, 'fixture')",
            rusqlite::params![
                replacement.as_str(), "mock", "fixture", "review", "task", "summary",
                home.path.display().to_string(), "{}"
            ],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_members(agent_id,pool_id,slot,name,role,personal_task,joined_roster_revision)              VALUES(?,'pool-20260101-000000-0123456789',2,'alice-2','doer','task',2)",
            [replacement.as_str()],
        )
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE pool_members SET replaced_by=? WHERE agent_id=?",
            rusqlite::params![replacement.as_str(), member.as_str()],
        )
        .unwrap();
    assert_eq!(
        commands::pool_entry_text(&store, &member, seq).unwrap(),
        Err("not_pool_member")
    );
    // An unknown sequence is not_found, never a silent success.
    assert_eq!(
        commands::pool_entry_text(&store, &member, seq + 100).unwrap(),
        Err("entry_not_found")
    );
    // The current member renders; an entry of a pool it does not belong to
    // refuses as foreign.
    assert!(
        commands::pool_entry_text(&store, &replacement, seq)
            .unwrap()
            .is_ok()
    );
    store
        .conn
        .execute(
            "INSERT INTO pools(id,request_namespace,request_id,request_sha256,goal,acceptance_json,state,roster_revision,created_at) \
             VALUES('pool-20260101-000000-0000000002','ns','r2','0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef','goal','[]','open',1,1.0)",
            [],
        )
        .unwrap();
    store
        .conn
        .execute(
            "INSERT INTO pool_entries(pool_id,author_kind,direction,kind,roster_revision,body,idem_scope,request_id,created_at) \
             VALUES('pool-20260101-000000-0000000002','operator','team','message',1,'elsewhere','op','r-2',1.0)",
            [],
        )
        .unwrap();
    let foreign: i64 = store
        .conn
        .query_row("SELECT MAX(seq) FROM pool_entries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        commands::pool_entry_text(&store, &replacement, foreign).unwrap(),
        Err("not_pool_member")
    );
    // A recipient superseded by a resumed child is a stale tip.
    store
        .conn
        .execute(
            "INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,timeout_seconds,config_revision,root_agent_id,sequence)              VALUES('ag-20260101-000000-0000000010','mock','fixture','review','task','summary',?,'{}','running',3.0,10.0,'fixture',?,2)",
            rusqlite::params![home.path.display().to_string(), replacement.as_str()],
        )
        .unwrap();
    assert_eq!(
        commands::pool_entry_text(&store, &replacement, seq).unwrap(),
        Err("stale_tip")
    );
    // Push refusals are finite safe codes: no payload, session or raw error.
    let refused = |payload: serde_json::Value, run: &agent_run_core::domain::AgentId| {
        commands::pool_push_text(&store, run, &payload).unwrap_err()
    };
    let malformed = refused(serde_json::json!({"seq": 0, "extra": "secret"}), &member);
    assert_eq!(malformed["push"], "refused");
    assert_eq!(malformed["reason"], "malformed_pool_command");
    assert!(!malformed.to_string().contains("secret"));
    let unknown: agent_run_core::domain::AgentId = "ag-20260101-000000-0000000099".parse().unwrap();
    assert_eq!(
        refused(serde_json::json!({"seq": seq}), &unknown)["reason"],
        "pool_lookup_failed"
    );
    assert_eq!(
        refused(serde_json::json!({"seq": seq}), &member)["reason"],
        "not_pool_member"
    );
}

/// The terminal hook every supervisor path calls completes a fully proven
/// pool exactly once and leaves a still-open one alone.
#[test]
fn complete_terminal_settles_a_fully_proven_pool() {
    use agent_run_core::commands;
    let home = common::Home::new();
    let config = agent_run_config::config::Config::load(&home.path).unwrap();
    let mut request = home.request();
    request.workdir = home.path.clone();
    request.validate().unwrap();
    let mut store = agent_run_core::state::Store::open(&home.path).unwrap();
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
    // The first member ends; the other still runs, so nothing completes.
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
    assert_eq!(state(&store), "completed");
    commands::complete_terminal(&mut store, &ids[1]).unwrap();
    let deliveries: i64 = store
        .conn
        .query_row("SELECT COUNT(*) FROM deliveries", [], |row| row.get(0))
        .unwrap();
    assert_eq!(deliveries, 1);
}
