//! Synthetic launch journal fault boundaries; no provider processes are used.
use agent_run_domain::domain::AgentId;
use rusqlite::params;

fn seeded() -> (
    tempfile::TempDir,
    agent_run_store::Store<'static>,
    AgentId,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = agent_run_store::fixture::initialize(dir.path()).unwrap();
    let id = AgentId::new();
    let attempt = format!("{}:1", id.as_str());
    store.conn.execute("INSERT INTO agents(id,runtime,model,profile,task,task_summary,workdir,request_json,status,created_at,config_revision,sequence) VALUES(?,'fixture','model','role','task','task','/fixture','{}','starting',1,'cfg',1)", [id.as_str()]).unwrap();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,phase,ownership_active) VALUES(?,?,1,'starting','{}',1,'prepared',1)", params![attempt,id.as_str()]).unwrap();
    (dir, store, id, attempt)
}

#[test]
fn launch_prepare_replay_and_crash_before_bind_never_duplicate_spawn() {
    let (_dir, mut store, id, attempt) = seeded();
    let launch = store.prepare_attempt_launch(&id, &attempt).unwrap();
    assert_eq!(uuid::Uuid::parse_str(&launch).unwrap().to_string(), launch);
    assert_eq!(store.prepare_attempt_launch(&id, &attempt).unwrap(), launch);
    assert!(store.provider_spawning(&id, &attempt).unwrap());
    // A crash or lost reply leaves spawning unresolved. Replaying prepare
    // returns the same identifier, and the spawn claim cannot be repeated.
    assert_eq!(store.prepare_attempt_launch(&id, &attempt).unwrap(), launch);
    assert!(store.provider_spawning(&id, &attempt).is_err());
    assert!(
        store
            .prepare_attempt_launch(&AgentId::new(), &attempt)
            .is_err()
    );
    assert_eq!(
        store
            .conn
            .query_row("SELECT count(*) FROM attempt_launches", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    let lost = agent_run_store::ops::MarkLost {
        expected: agent_run_store::ops::ExpectedOwnership {
            execution: id.clone(),
            supervisor_pid: None,
            supervisor_identity: None,
            supervisor_birth_time: None,
            process_group_id: None,
            startup_owner: None,
            startup_birth_time: None,
        },
        failure_kind: "supervisor_dead".into(),
        failure_text: "fixture".into(),
        evidence: serde_json::json!({"verdict":"dead"}),
        checked_at: 2.0,
    };
    assert!(store.mark_lost(&lost).unwrap());
    assert_eq!(
        store
            .conn
            .query_row("SELECT state FROM attempt_launches", [], |r| r
                .get::<_, String>(0))
            .unwrap(),
        "unknown"
    );
    assert_eq!(store.prepare_attempt_launch(&id, &attempt).unwrap(), launch);
    assert!(store.provider_spawning(&id, &attempt).is_err());
}

#[cfg(target_os = "linux")]
#[test]
fn launch_bind_is_atomic_with_exact_leader_and_failed_spawn_is_terminal() {
    let (_dir, store, id, attempt) = seeded();
    store.prepare_attempt_launch(&id, &attempt).unwrap();
    assert!(store.provider_spawning(&id, &attempt).unwrap());
    let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    let mut facts = agent_run_platform::process::kernel_incarnation(&leader).unwrap();
    facts.start_ticks += 1;
    assert!(
        store
            .provider_process_launch_evidence(&id, &attempt, &leader, None, Some(&facts))
            .is_err()
    );
    let state: String = store
        .conn
        .query_row("SELECT state FROM attempt_launches", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "spawning");
    facts.start_ticks -= 1;
    store
        .provider_process_launch_evidence(&id, &attempt, &leader, None, Some(&facts))
        .unwrap();
    let (state, pid, ticks): (String, u32, u64) = store
        .conn
        .query_row(
            "SELECT state,pid,start_ticks FROM attempt_launches",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(
        (state.as_str(), pid, ticks),
        ("bound", facts.pid, facts.start_ticks)
    );
    assert!(store.provider_spawn_failed(&id, &attempt).is_err());

    let (_dir, store, id, attempt) = seeded();
    let launch = store.prepare_attempt_launch(&id, &attempt).unwrap();
    assert!(store.provider_spawning(&id, &attempt).unwrap());
    store.provider_spawn_failed(&id, &attempt).unwrap();
    assert_eq!(store.prepare_attempt_launch(&id, &attempt).unwrap(), launch);
    assert!(store.provider_spawning(&id, &attempt).is_err());
    let state: String = store
        .conn
        .query_row("SELECT state FROM attempt_launches", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "failed");
}

#[cfg(target_os = "linux")]
#[test]
fn missing_kernel_incarnation_keeps_ordinary_leader_but_refuses_binding() {
    let (_dir, store, id, attempt) = seeded();
    store.prepare_attempt_launch(&id, &attempt).unwrap();
    assert!(store.provider_spawning(&id, &attempt).unwrap());
    let leader = agent_run_platform::process::inspect(std::process::id() as i32).unwrap();
    store
        .provider_process_launch_evidence(&id, &attempt, &leader, None, None)
        .unwrap();
    let (state, pid, token): (String, Option<u32>, String) = store.conn.query_row(
        "SELECT l.state,l.pid,t.process_identity FROM attempt_launches l JOIN attempts t ON t.id=l.attempt_id", [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    ).unwrap();
    assert_eq!(state, "unknown");
    assert_eq!(pid, None);
    assert_eq!(token, leader.token);
}
