//! Real native-loop, private-writer and kernel owner controls without models or production configuration.
mod common;
use agent_run_adapters::{
    LaunchPlan,
    io::Process,
    native_owner::{self, Request},
};
use agent_run_core::{native_owner::refusal, stream};
use agent_run_domain::{
    domain::Status,
    worker::{FinishRequest, FinishStatus},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
};
/// Private fixture-only worker capability; no installed credential is read.
const TOKEN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
/// Initial successful inference is deliberately not a logical finish.
const TURN: &str = r#"{"type":"result","subtype":"success","is_error":false,"session_id":"s","result":"NOT_A_FINISH","usage":{}}"#;
/// Qualify the test's actual executable/current kernel peer, keeping the channel alive until the verifier closes it.
fn owner_environment(home: &Path) -> (BTreeMap<String, String>, tokio::task::JoinHandle<()>) {
    use std::os::unix::fs::PermissionsExt;
    let channel = home.join("channel.sock");
    let listener = UnixListener::bind(&channel).unwrap();
    std::fs::set_permissions(&channel, std::fs::Permissions::from_mode(0o600)).unwrap();
    let server = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let _ = stream.read(&mut [0]).await;
        }
    });
    let exe = std::env::current_exe().unwrap().canonicalize().unwrap();
    (
        BTreeMap::from([
            (
                native_owner::SETTINGS[0].into(),
                channel.to_str().unwrap().into(),
            ),
            (
                native_owner::SETTINGS[1].into(),
                exe.to_str().unwrap().into(),
            ),
            (
                native_owner::SETTINGS[2].into(),
                agent_run_platform::fs::sha256(&std::fs::read(exe).unwrap()),
            ),
            (
                native_owner::SETTINGS[3].into(),
                home.to_str().unwrap().into(),
            ),
        ]),
        server,
    )
}
/// Write one exact owner frame; no native input or public runner command is used by this driver.
async fn send(socket: &Path, request: &Request) -> UnixStream {
    let mut stream = UnixStream::connect(socket).await.unwrap();
    let bytes = serde_json::to_vec(request).unwrap();
    stream.write_u32_le(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream
}
/// Read one bounded finite owner receipt, refusing a missing response instead of assuming acceptance.
async fn reply(stream: &mut UnixStream) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        let length = stream.read_u32_le().await.unwrap() as usize;
        assert!(length <= native_owner::FRAME_BYTES);
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    })
    .await
    .unwrap()
}
/// Wait only inside a bounded provider-free fixture for the actual durable worker phase.
async fn phase(
    store: &agent_run_store::Store<'_>,
    id: &agent_run_domain::domain::AgentId,
    expected: &str,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store
                .worker_lifecycle_view(id)
                .unwrap()
                .is_some_and(|x| x["phase"] == expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
/// The existing Claude writer accepts only after a real correlated replay, preserves one effect on retry and finishes/cleans the same owned actor.
#[tokio::test]
async fn claude_owner_requires_replay_and_finishes_same_actor() {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
    let (id, _) = home
        .store()
        .admit(&req, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&id, "attempt", TOKEN, 100.)
        .unwrap();
    store.begin_worker_lifecycle(&id, "attempt", 100.).unwrap();
    let row = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let (environment, channel) = owner_environment(&home.path);
    let script = format!(
        r#"read initial
printf '%s\n' '{TURN}'
read wake
sleep 1
printf '%s\n' "$wake" | /usr/bin/sed 's/{{/{{"isReplay":true,"session_id":"s",/'
printf '%s\n' '{{"type":"assistant","session_id":"s","message":{{"role":"assistant","content":[{{"type":"tool_use","id":"finish-call","name":"mcp__agent_run_worker__finish","input":{{"summary":"FINAL"}}}}]}}}}'
for i in $(seq 1 500); do test -f receipt && break; sleep 0.01; done
read digest < receipt
printf '{{"type":"user","session_id":"s","message":{{"role":"user","content":[{{"type":"tool_result","tool_use_id":"finish-call","content":"%s"}}]}}}}\n' "$digest"
sleep 1
"#
    );
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment,
        initial_input: None,
    })
    .unwrap();
    let leader = process.owner.leader.clone().unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET process_identity=?,process_birth_time=? WHERE id='attempt'",
            rusqlite::params![leader.token, leader.birth],
        )
        .unwrap();
    let socket = home
        .path
        .read_dir()
        .unwrap()
        .filter_map(|x| x.ok())
        .map(|x| x.path().join("o.sock"))
        .find(|x| x.exists())
        .unwrap();
    let mut control = home.store();
    let driver = async {
        phase(&control, &id, "idle").await;
        let mut request = Request {
            thread_id: "s".into(),
            call_id: "original-call".into(),
            text: "frozen-manifest".into(),
            native_pid: leader.pid as u32,
            native_start_time: leader.birth.floor() as u64,
            wake_only: true,
        };
        let mut mismatch = request.clone();
        mismatch.call_id = "wrong-actor".into();
        mismatch.thread_id = "other".into();
        let mut connection = send(&socket, &mismatch).await;
        assert_eq!(reply(&mut connection).await["reason"], "actor_mismatch");
        let mut connection = send(&socket, &request).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), connection.readable())
                .await
                .is_err(),
            "pipe write is not native acknowledgement"
        );
        let accepted = reply(&mut connection).await;
        assert_eq!(accepted["state"], "accepted");
        assert_eq!(accepted["protocol"], "claude_input");
        assert!(uuid::Uuid::parse_str(accepted["native_id"].as_str().unwrap()).is_ok());
        let mut retry = send(&socket, &request).await;
        assert_eq!(reply(&mut retry).await, accepted);
        request.text.push('!');
        let mut conflict = send(&socket, &request).await;
        assert_eq!(reply(&mut conflict).await["reason"], "conflict");
        let receipt = control
            .accept_worker_finish(
                &id,
                "attempt",
                TOKEN,
                &FinishRequest {
                    summary: "FINAL".into(),
                    status: FinishStatus::Done,
                },
                agent_run_domain::domain::now(),
            )
            .unwrap();
        std::fs::write(home.path.join("receipt"), format!("{}\n", receipt.sha256)).unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let (result, ()) = tokio::join!(
            stream::run(&mut process, &mut persist, &row, Some("initial\n")),
            driver
        );
        persist.flush().await.unwrap();
        result.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result.outcome.status, Status::Succeeded);
    assert_eq!(result.answer.as_deref(), Some("FINAL"));
    assert_eq!(process.owner.pid, leader.pid);
    assert_eq!(
        store.worker_lifecycle_view(&id).unwrap().unwrap()["turn_count"],
        2
    );
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    assert!(proof.confirmed && proof.group_gone && proof.descendants_gone == Some(true));
    process.reap().await;
    assert!(!socket.exists());
    channel.abort();
}
/// Real process/store admission refuses stale, mismatched, active, cancelled, closing, terminal and dead actors before any native effect.
#[tokio::test]
async fn owner_lifetime_and_durable_phase_refuse_before_effect() {
    let home = common::Home::new();
    let mut req = home.request();
    req.explicit_finish = true;
    let (id, _) = home
        .store()
        .admit(&req, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)",[id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&id, "attempt", TOKEN, 100.)
        .unwrap();
    store.begin_worker_lifecycle(&id, "attempt", 100.).unwrap();
    store.worker_turn(&id, true, 100.).unwrap();
    let row = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let persist = harness.supervisor(&home.path, prep, &id).await;
    let (environment, channel) = owner_environment(&home.path);
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), "read unused".into()],
        cwd: home.path.clone(),
        environment: environment.clone(),
        initial_input: None,
    })
    .unwrap();
    let leader = process.owner.leader.clone().unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET process_identity=?,process_birth_time=? WHERE id='attempt'",
            rusqlite::params![leader.token, leader.birth],
        )
        .unwrap();
    let socket = home
        .path
        .read_dir()
        .unwrap()
        .filter_map(|x| x.ok())
        .map(|x| x.path().join("o.sock"))
        .find(|x| x.exists())
        .unwrap();
    let mut owner = process.native_owner.take();
    let request = Request {
        thread_id: "s".into(),
        call_id: "guard".into(),
        text: "manifest".into(),
        native_pid: leader.pid as u32,
        native_start_time: leader.birth.floor() as u64,
        wake_only: true,
    };
    let _stream = send(&socket, &request).await;
    let mut call = native_owner::Owner::receive(&mut owner).await;
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        None
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=0 WHERE id='attempt'",
            [],
        )
        .unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("actor_mismatch")
    );
    store
        .conn
        .execute(
            "UPDATE attempts SET ownership_active=1 WHERE id='attempt'",
            [],
        )
        .unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("other"), true, false).unwrap(),
        Some("actor_mismatch")
    );
    call.request.native_start_time += 1;
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("actor_mismatch")
    );
    call.request.native_start_time -= 1;
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), false, false).unwrap(),
        Some("not_idle")
    );
    store.worker_turn(&id, false, 101.).unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("not_idle")
    );
    store.worker_turn(&id, true, 102.).unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, true).unwrap(),
        Some("finished")
    );
    store.enqueue(&id, "cancel", &json!({})).unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("cancelled")
    );
    store
        .conn
        .execute("DELETE FROM commands WHERE agent_id=?", [id.as_str()])
        .unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET sequence=sequence+1 WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("actor_mismatch")
    );
    store
        .conn
        .execute(
            "UPDATE agents SET sequence=sequence-1,status='failed' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("finished")
    );
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    call.deadline = tokio::time::Instant::now() - Duration::from_millis(1);
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("unavailable")
    );
    call.deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    assert!(proof.confirmed);
    process.reap().await;
    assert_eq!(
        refusal(&process, &persist, &row, &call, Some("s"), true, false).unwrap(),
        Some("unavailable")
    );
    drop(owner);
    assert!(!socket.exists());
    channel.abort();
}

/// Explicit opt-in never relaxes research/IPC constraints or enables one-shot/unsupported harnesses.
#[test]
fn owner_configuration_preserves_restricted_and_ordinary_launches() {
    use agent_run_config::profiles::Profile;
    use agent_run_domain::domain::Constraint;
    use std::collections::BTreeSet;
    let home = common::Home::new();
    let mut request = home.request();
    request.explicit_finish = true;
    let (id, _) = home
        .store()
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut row = home.store().get(&id).unwrap();
    let mut role = Profile {
        name: "fixture".into(),
        body: "fixture".into(),
        write: false,
        network: false,
        revision: "fixture".into(),
        canonical: false,
        allow_external_read_roots: false,
        read_roots: vec![],
        skills: vec![],
        mcp: vec![],
        mcp_tools: BTreeMap::new(),
        required_constraints: BTreeSet::new(),
    };
    let settings = BTreeMap::from_iter(
        native_owner::SETTINGS
            .into_iter()
            .chain([native_owner::LOCATOR])
            .map(|key| (key.into(), "fixture".into())),
    );
    let mut enabled = settings.clone();
    agent_run_core::native_owner::configure(&mut enabled, &row, &role, true);
    assert_eq!(enabled, settings);
    for constraint in [
        Constraint::ResearchToolsOnly,
        Constraint::UnixIpcIsolation,
        Constraint::McpIpcIsolation,
    ] {
        role.required_constraints = BTreeSet::from([constraint]);
        let mut env = settings.clone();
        agent_run_core::native_owner::configure(&mut env, &row, &role, true);
        assert!(env.is_empty());
    }
    role.required_constraints.clear();
    let mut unsupported = settings.clone();
    agent_run_core::native_owner::configure(&mut unsupported, &row, &role, false);
    assert!(unsupported.is_empty());
    row.request.explicit_finish = false;
    let mut ordinary = settings;
    agent_run_core::native_owner::configure(&mut ordinary, &row, &role, true);
    assert!(ordinary.is_empty());
}

/// One actual Claude input without its native replay stays uncertain, retires the endpoint and never writes a second body.
#[tokio::test]
async fn claude_owner_missing_replay_retires_one_effect() {
    let home = common::Home::new();
    let mut request = home.request();
    request.explicit_finish = true;
    let (id, _) = home
        .store()
        .admit(&request, &home.config, &json!({}), None)
        .unwrap();
    let mut store = home.store();
    store.conn.execute("INSERT INTO attempts(id,agent_id,number,state,adapter_state_json,created_at,ownership_active) VALUES('attempt',?,1,'running','{}',100,1)", [id.as_str()]).unwrap();
    store
        .conn
        .execute(
            "UPDATE agents SET status='running' WHERE id=?",
            [id.as_str()],
        )
        .unwrap();
    store
        .issue_worker_capability(&id, "attempt", TOKEN, 100.)
        .unwrap();
    store.begin_worker_lifecycle(&id, "attempt", 100.).unwrap();
    let row = store.get(&id).unwrap();
    let prep = common::writer::prepare_supervisor(&mut store, &id);
    let harness = common::writer::Harness::start(&home.path);
    let mut persist = harness.supervisor(&home.path, prep, &id).await;
    let (environment, channel) = owner_environment(&home.path);
    let script = format!(
        "read initial; printf '%s\\n' '{TURN}'; read wake; printf 'one\\n' >> effect; if read unexpected; then printf 'duplicate\\n' >> effect; fi"
    );
    let mut process = Process::spawn(&LaunchPlan {
        binary: "/bin/sh".into(),
        args: vec!["-c".into(), script],
        cwd: home.path.clone(),
        environment,
        initial_input: None,
    })
    .unwrap();
    let leader = process.owner.leader.clone().unwrap();
    store
        .conn
        .execute(
            "UPDATE attempts SET process_identity=?,process_birth_time=? WHERE id='attempt'",
            rusqlite::params![leader.token, leader.birth],
        )
        .unwrap();
    let socket = home
        .path
        .read_dir()
        .unwrap()
        .filter_map(|x| x.ok())
        .map(|x| x.path().join("o.sock"))
        .find(|x| x.exists())
        .unwrap();
    let control = home.store();
    let driver = async {
        phase(&control, &id, "idle").await;
        let call = Request {
            thread_id: "s".into(),
            call_id: "uncertain".into(),
            text: "manifest".into(),
            native_pid: leader.pid as u32,
            native_start_time: leader.birth.floor() as u64,
            wake_only: true,
        };
        let mut stream = send(&socket, &call).await;
        let mut byte = [0];
        match tokio::time::timeout(Duration::from_secs(33), stream.read(&mut byte)).await {
            Ok(Ok(0)) | Ok(Err(_)) => {}
            Ok(Ok(1)) => {
                let mut header = [0; 4];
                header[0] = byte[0];
                stream.read_exact(&mut header[1..]).await.unwrap();
                let length = u32::from_le_bytes(header) as usize;
                assert!(length <= native_owner::FRAME_BYTES);
                let mut bytes = vec![0; length];
                stream.read_exact(&mut bytes).await.unwrap();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["state"], "unknown");
                assert_eq!(body["call_id"], "uncertain");
                assert!(body["protocol"].is_null() && body["native_id"].is_null());
            }
            _ => panic!("original no-replay deadline did not close or return uncertainty"),
        }
    };
    let result = tokio::time::timeout(Duration::from_secs(36), async {
        let (result, ()) = tokio::join!(
            stream::run(&mut process, &mut persist, &row, Some("initial\n")),
            driver
        );
        result
    })
    .await
    .unwrap();
    assert!(
        result
            .err()
            .expect("missing replay must fail closed")
            .to_string()
            .contains("acknowledgement is uncertain")
    );
    assert!(!socket.exists());
    assert_eq!(
        std::fs::read_to_string(home.path.join("effect")).unwrap(),
        "one\n"
    );
    let proof = process.owner.cleanup(Duration::from_secs(2)).await.unwrap();
    assert!(proof.confirmed && proof.group_gone && proof.descendants_gone == Some(true));
    process.reap().await;
    channel.abort();
}
