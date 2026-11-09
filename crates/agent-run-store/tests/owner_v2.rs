//! Synthetic owner-v2 effect writer crash/replay and abort fences.
use agent_run_domain::StoreId;
use agent_run_domain::{
    attempt_binding::{Actor, AttemptRef, Binding, Incarnation, Route},
    domain::AgentId,
    owner_v2::{OutboxIntent, outbox_digest},
};
use agent_run_store::{Store, owner_v2::EffectRoute};

fn fixture() -> (
    tempfile::TempDir,
    Store<'static>,
    StoreId,
    OutboxIntent,
    String,
) {
    let dir = tempfile::tempdir().unwrap();
    let store = agent_run_store::fixture::initialize(dir.path()).unwrap();
    let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
    let id = AgentId::new();
    let route = |store: String, id: &str| Route {
        component_store_id: store,
        route_id: id.into(),
        worker_uid: 1000,
    };
    let bridge = route("01234567-89ab-4cde-8fab-0123456789ab".into(), "bridge");
    let outbox = OutboxIntent {
        effect_id: "effect".into(),
        producer_scope: "scope".into(),
        event_key: "event".into(),
        payload_sha256: agent_run_platform::fs::sha256(b"text"),
        payload_length: 4,
        target_binding: Binding {
            pool_store_id: "pool".into(),
            pool_id: "pool".into(),
            member_id: "member".into(),
            binding_generation: 1,
            attempt: AttemptRef {
                run_store_id: sid.clone(),
                lineage_root_id: id.clone(),
                execution_id: id,
                sequence: 1,
                attempt_id: "attempt".into(),
                launch_id: "01234567-89ab-4cde-8fab-0123456789ab".into(),
            },
            actor: Actor {
                address: "host/codex/s".into(),
                role: "root".into(),
                native_id: "s".into(),
                epoch: 1,
            },
            run_route: route(sid.to_string(), "run"),
            bridge_route: bridge.clone(),
        },
        expected_incarnation: Incarnation {
            host_id: "host".into(),
            boot_id: "01234567-89ab-4cde-8fab-0123456789ab".into(),
            pid_namespace_inode: 2,
            uid: 1000,
            pid: 42,
            start_ticks: 7,
        },
        bridge_route: bridge,
        mode: "message".into(),
        retention: "keep_all".into(),
    };
    let digest = outbox_digest(&outbox).unwrap();
    (dir, store, sid, outbox, digest)
}

#[test]
fn dispatch_effect_identity_and_unknown_never_resends() {
    let (dir, store, sid, outbox, digest) = fixture();
    let route = EffectRoute {
        run_route: outbox.target_binding.run_route.clone(),
        bridge_route: outbox.bridge_route.clone(),
    };
    assert!(matches!(
        store.owner_effect_reserve_gated(&sid, &outbox, &digest, false),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    assert_eq!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "absent"
    );
    let first = store.owner_effect_reserve(&sid, &outbox, &digest).unwrap();
    let old = first.dispatch_token.unwrap();
    assert_eq!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "unknown"
    );
    store
        .owner_effect_finish(
            &sid,
            "scope",
            "effect",
            &digest,
            &old,
            "known_unsent",
            None,
            None,
            Some("not_idle"),
        )
        .unwrap();
    assert!(matches!(
        store.owner_effect_reserve_gated(&sid, &outbox, &digest, false),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    let second = store.owner_effect_reserve(&sid, &outbox, &digest).unwrap();
    let token = second.dispatch_token.unwrap();
    assert_ne!(old, token);
    assert!(
        store
            .owner_effect_invoke(&sid, "scope", "effect", &digest, &old)
            .is_err()
    );
    store
        .owner_effect_invoke(&sid, "scope", "effect", &digest, &token)
        .unwrap();
    assert_eq!(
        store
            .owner_effect_abort(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "unknown"
    );
    store
        .owner_effect_finish(
            &sid,
            "scope",
            "effect",
            &digest,
            &token,
            "unknown",
            None,
            None,
            Some("native_uncertain"),
        )
        .unwrap();
    assert!(
        store
            .owner_effect_reserve(&sid, &outbox, &digest)
            .unwrap()
            .dispatch_token
            .is_none()
    );
    drop(store);
    let store = agent_run_store::fixture::open(dir.path()).unwrap();
    assert_eq!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "unknown"
    );
    assert!(
        store
            .owner_effect_reserve(&sid, &outbox, &digest)
            .unwrap()
            .dispatch_token
            .is_none()
    );
    let accepted = store
        .owner_effect_finish(
            &sid,
            "scope",
            "effect",
            &digest,
            &token,
            "accepted",
            Some("codex_turn"),
            Some("native-turn"),
            None,
        )
        .unwrap();
    assert_eq!(accepted.state, "accepted");
    let replay = store
        .owner_effect_reserve_gated(&sid, &outbox, &digest, false)
        .unwrap();
    assert!(replay.dispatch_token.is_none());
    assert_eq!(replay.view, accepted);
    assert_eq!(
        store
            .owner_effect_abort(&sid, "scope", "effect", &digest, &route)
            .unwrap(),
        accepted
    );
    assert!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &"f".repeat(64), &route)
            .is_err()
    );
}

#[test]
fn queued_enabled_snapshot_rechecks_config_inside_writer_transaction() {
    let (dir, store, sid, outbox, digest) = fixture();
    let config = format!(
        "schema_version=2\n[managed_attempt_binding]\nenabled=true\nrole_name='managed'\npool_server='pool'\npool_executable_sha256='{}'\nbridge_hook_sha256='{}'\nhost_id='host'\n",
        "a".repeat(64),
        "b".repeat(64)
    );
    std::fs::write(
        dir.path().join("config.toml"),
        config.replace("enabled=true", "enabled=false"),
    )
    .unwrap();
    assert!(matches!(
        store.owner_effect_reserve_managed(&sid, &outbox, &digest, true),
        Err(agent_run_domain::Error::Unsupported(_))
    ));
    let route = EffectRoute {
        run_route: outbox.target_binding.run_route.clone(),
        bridge_route: outbox.bridge_route.clone(),
    };
    assert_eq!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "absent"
    );
    std::fs::write(dir.path().join("config.toml"), config).unwrap();
    assert!(
        store
            .owner_effect_reserve_managed(&sid, &outbox, &digest, true)
            .unwrap()
            .dispatch_token
            .is_some()
    );
}

#[test]
fn lost_reservation_ack_never_issues_a_second_dispatch_token() {
    let (_dir, store, sid, outbox, digest) = fixture();
    let first = store.owner_effect_reserve(&sid, &outbox, &digest).unwrap();
    assert!(first.dispatch_token.is_some());
    // The response is lost after commit; a fresh dispatch cannot regain it.
    let retry = store.owner_effect_reserve(&sid, &outbox, &digest).unwrap();
    assert!(retry.dispatch_token.is_none());
    assert_eq!(retry.view.state, "unknown");
}

#[test]
fn abort_fences_delayed_reserved_writer_and_absent_lookup_is_not_fence() {
    let (_dir, store, sid, outbox, digest) = fixture();
    let route = EffectRoute {
        run_route: outbox.target_binding.run_route.clone(),
        bridge_route: outbox.bridge_route.clone(),
    };
    assert_eq!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "absent"
    );
    let reserved = store.owner_effect_reserve(&sid, &outbox, &digest).unwrap();
    let token = reserved.dispatch_token.unwrap();
    assert_eq!(
        store
            .owner_effect_abort(&sid, "scope", "effect", &digest, &route)
            .unwrap()
            .state,
        "aborted"
    );
    assert!(
        store
            .owner_effect_invoke(&sid, "scope", "effect", &digest, &token)
            .is_err()
    );
    assert!(
        store
            .owner_effect_reserve(&sid, &outbox, &digest)
            .unwrap()
            .dispatch_token
            .is_none()
    );
    assert_eq!(
        store
            .owner_effect_abort(&sid, "other", "new", &digest, &route)
            .unwrap()
            .state,
        "aborted"
    );
    assert_eq!(
        store
            .owner_effect_abort(&sid, "other", "new", &digest, &route)
            .unwrap()
            .state,
        "aborted"
    );
    assert!(
        store
            .owner_effect_abort(&sid, "other", "new", &"e".repeat(64), &route)
            .is_err()
    );
    let mut foreign_route = route.clone();
    foreign_route.bridge_route.route_id = "foreign-route".into();
    assert!(
        store
            .owner_effect_lookup(&sid, "scope", "effect", &digest, &foreign_route)
            .is_err()
    );
    assert!(
        store
            .owner_effect_abort(&sid, "other", "new", &digest, &foreign_route)
            .is_err()
    );
}

#[test]
fn supervisor_crash_during_owner_write_remains_unknown_no_resend() {
    let (dir, store, sid, outbox, digest) = fixture();
    let route = EffectRoute {
        run_route: outbox.target_binding.run_route.clone(),
        bridge_route: outbox.bridge_route.clone(),
    };
    let token = store
        .owner_effect_reserve(&sid, &outbox, &digest)
        .unwrap()
        .dispatch_token
        .unwrap();
    drop(store);
    let mut supervisor = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crashing_supervisor_fixture", "--nocapture"])
        .env_clear()
        .env("RUN_R3_FIXTURE_HOME", dir.path())
        .env("TMPDIR", dir.path())
        .env("RUN_R3_FIXTURE_TOKEN", &token)
        .env("RUN_R3_FIXTURE_DIGEST", &digest)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let barrier = dir.path().join("native-received");
    let started = std::time::Instant::now();
    while !barrier.exists() {
        if started.elapsed() >= std::time::Duration::from_secs(15) {
            let _ = supervisor.kill();
            let _ = supervisor.wait();
            panic!("synthetic supervisor did not reach the crash point");
        }
        assert!(
            supervisor.try_wait().unwrap().is_none(),
            "synthetic supervisor exited early"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    supervisor.kill().unwrap();
    supervisor.wait().unwrap();
    let store = agent_run_store::fixture::open(dir.path()).unwrap();
    assert_eq!(
        store
            .owner_effect_lookup(
                &sid,
                &outbox.producer_scope,
                &outbox.effect_id,
                &digest,
                &route
            )
            .unwrap()
            .state,
        "unknown"
    );
    assert!(
        store
            .owner_effect_reserve(&sid, &outbox, &digest)
            .unwrap()
            .dispatch_token
            .is_none()
    );
    assert_eq!(
        store
            .owner_effect_abort(
                &sid,
                &outbox.producer_scope,
                &outbox.effect_id,
                &digest,
                &route
            )
            .unwrap()
            .state,
        "unknown"
    );
    let mut unrelated = outbox.clone();
    unrelated.effect_id = "unrelated".into();
    let unrelated_digest = outbox_digest(&unrelated).unwrap();
    assert!(
        store
            .owner_effect_reserve(&sid, &unrelated, &unrelated_digest)
            .unwrap()
            .dispatch_token
            .is_some()
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("native-input")).unwrap(),
        "received\n"
    );
}

/// Child-only synthetic supervisor: persist invoking, deliver one native
/// input, then stop at the crash point before writing any owner outcome.
#[test]
fn crashing_supervisor_fixture() {
    use std::io::Write;
    use std::process::{Command, Stdio};
    let Some(home) = std::env::var_os("RUN_R3_FIXTURE_HOME").map(std::path::PathBuf::from) else {
        return;
    };
    let token = std::env::var("RUN_R3_FIXTURE_TOKEN").unwrap();
    let digest = std::env::var("RUN_R3_FIXTURE_DIGEST").unwrap();
    let store = agent_run_store::fixture::open(&home).unwrap();
    let sid = agent_run_store::store_identity::read(&store.conn).unwrap();
    store
        .owner_effect_invoke(&sid, "scope", "effect", &digest, &token)
        .unwrap();
    let mut native = Command::new("/bin/sh")
        .args([
            "-c",
            "IFS= read -r input; printf 'received\\n' >> \"$1\"",
            "fixture",
        ])
        .arg(home.join("native-input"))
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    writeln!(native.stdin.take().unwrap(), "synthetic input").unwrap();
    assert!(native.wait().unwrap().success());
    std::fs::write(home.join("native-received"), b"ok").unwrap();
    std::thread::park();
}
