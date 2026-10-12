//! Real isolated supervisors and finite native fixtures competing for one WAL writer.
use super::*;

/// Wait up to six seconds for a fixture's marker and any required native result
/// row; reads never acquire a writer. The path belongs only to this test home.
async fn ready(home: &Path, work: &Path, id: &AgentId, result: bool) {
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let observed = !result || agent_run_store::fixture::open(home).unwrap().conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM events WHERE agent_id=? AND kind='native_result_frame_v1')",
                [id.as_str()], |r| r.get::<_, bool>(0)).unwrap();
            if work.join("sqlite-ready").exists() && observed { break; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("finite native barrier");
}

/// Hold only BEGIN IMMEDIATE on an independent connection. The writer thread
/// releases after `hold`; native work is never repeated. Returns its join handle
/// after proving the lock acquired, so there is no scheduling race in the test.
fn writer(home: &Path, hold: Duration) -> std::thread::JoinHandle<()> {
    let home = home.to_path_buf();
    let (send, receive) = std::sync::mpsc::sync_channel(0);
    let held = std::thread::spawn(move || {
        let store = agent_run_store::fixture::open(&home).unwrap();
        store.conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        send.send(()).unwrap();
        std::thread::sleep(hold);
        store.conn.execute_batch("ROLLBACK").unwrap();
    });
    receive.recv_timeout(Duration::from_secs(2)).unwrap();
    held
}

/// Three real supervisors share a writer outage during streaming. A statement
/// refusal targets only the third; its verified failure cannot damage the two
/// neighbours, which preserve each
/// native identity, transcript, result and cleanup, and emit one notice each.
/// Resuming the first retains its logical root and sequence without native replay.
#[tokio::test]
async fn concurrent_streaming_busy_preserves_results_and_resume_lineage() {
    let (_temp, home) = home();
    let _cleanup = ServiceProcesses(home.clone());
    let service = fixture_service(home.clone()).await;
    let mut runs = Vec::new();
    fs::write(home.join("fixture-pause-terminal-1"), b"pause").unwrap();
    for n in 0..3 {
        let work = home.join(format!("work-{n}"));
        fs::create_dir(&work).unwrap();
        let mut req = request(&home);
        req.workdir = work.clone();
        req.task = if n == 2 {
            "fixture:sqlite-stream-fail"
        } else {
            "fixture:sqlite-stream"
        }
        .into();
        req.request_id = Some(format!("sqlite-stream-{n}"));
        let admitted = service
            .admit_provider_trusted(req, candidates(committed(&home)))
            .await
            .unwrap();
        let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        let child = supervisor(&home, &id).await;
        ready(&home, &work, &id, false).await;
        runs.push((
            id,
            work,
            child,
            admitted["attempt_id"].as_str().unwrap().to_owned(),
        ));
    }
    agent_run_store::fixture::open(&home).unwrap().conn.execute_batch(
        "CREATE TRIGGER fixture_one_owner_refusal BEFORE INSERT ON process_members \
         WHEN NEW.owner_id=(SELECT id FROM attempts WHERE agent_id=(SELECT id FROM agents WHERE task='fixture:sqlite-stream-fail')) \
         AND NEW.pid<>(SELECT process_group_id FROM agents WHERE task='fixture:sqlite-stream-fail') \
         BEGIN SELECT RAISE(ABORT,'SOURCE_SECRET'); END"
    ).unwrap();
    let held = writer(&home, Duration::from_millis(900));
    for (_, work, _, _) in &runs {
        fs::write(work.join("sqlite-release"), b"go").unwrap();
    }
    // The statement refusal stops only that native engine. Before the distinct
    // final snapshot operation, remove the fixture fault behind the existing
    // test barrier, without replaying native input or relaxing any ownership gate.
    let failed_receipt = home
        .join("agents")
        .join(runs[2].0.as_str())
        .join(format!("recovery.{}.json", runs[2].3));
    tokio::time::timeout(Duration::from_secs(6), async {
        while !failed_receipt.exists() || !home.join("fixture-paused-terminal-1").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    agent_run_store::fixture::open(&home)
        .unwrap()
        .conn
        .execute_batch("DROP TRIGGER fixture_one_owner_refusal")
        .unwrap();
    fs::write(home.join("fixture-resume-terminal-1"), b"go").unwrap();
    for (n, (_, _, child, _)) in runs.iter_mut().enumerate() {
        let exit = tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(
            exit.success(),
            "supervisor commits even a native failure as a verified terminal result: {n}"
        );
    }
    held.join().unwrap();
    let store = agent_run_store::fixture::open(&home).unwrap();
    for (id, _, _, attempt) in runs.iter().take(2) {
        let row = store.get(id).unwrap();
        assert_eq!(
            row.status,
            Status::Succeeded,
            "execution {id}: failure_kind={:?}, failure_text={:?}",
            row.failure_kind,
            row.failure_text
        );
        assert!(row.runtime_session_id.is_some());
        assert_eq!(
            service.answer(id).unwrap()["content"],
            "fixture final answer\n"
        );
        for kind in [
            "runtime_session",
            "native_result_frame_v1",
            "process_cleanup",
        ] {
            assert_eq!(
                store
                    .conn
                    .query_row(
                        "SELECT count(*) FROM events WHERE agent_id=? AND kind=?",
                        rusqlite::params![id.as_str(), kind],
                        |r| r.get::<_, i64>(0)
                    )
                    .unwrap(),
                1,
                "duplicate {kind}"
            );
        }
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT count(*) FROM deliveries WHERE agent_id=?",
                    [id.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        let captured = store
            .remembered_processes("attempt", attempt)
            .unwrap()
            .unwrap()
            .snapshot()
            .unwrap();
        assert!(captured.members.len() >= 2, "new descendant not persisted");
        let proof: String = store
            .conn
            .query_row(
                "SELECT cleanup_proof_json FROM attempts WHERE id=? AND ownership_active=0",
                [attempt],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&proof).unwrap()["confirmed"],
            true
        );
        assert!(store.conn.query_row("SELECT EXISTS(SELECT 1 FROM messages WHERE agent_id=? AND role='assistant' AND content LIKE '%fixture partial%')",[id.as_str()],|r|r.get::<_,bool>(0)).unwrap());
    }
    let failed = store.get(&runs[2].0).unwrap();
    assert_eq!(failed.status, Status::Failed);
    assert_eq!(
        failed.failure_kind.as_deref(),
        Some("runtime_ownership_checkpoint_failed")
    );
    assert!(!failed.failure_text.unwrap().contains("SOURCE_SECRET"));
    assert_eq!(service.answer(&runs[2].0).unwrap()["available"], false);
    let (active, proof): (i64, String) = store
        .conn
        .query_row(
            "SELECT ownership_active,cleanup_proof_json FROM attempts WHERE id=?",
            [&runs[2].3],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(active, 0);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&proof).unwrap()["confirmed"],
        true
    );
    assert_eq!(
        store
            .conn
            .query_row(
                "SELECT count(*) FROM deliveries WHERE agent_id=?",
                [runs[2].0.as_str()],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    let failure: String = store
        .conn
        .query_row(
            "SELECT data_json FROM events WHERE agent_id=? AND kind='execution_failure_v1'",
            [runs[2].0.as_str()],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&failure).unwrap()["operation"],
        "ownership_write"
    );
    let parent = store.get(&runs[0].0).unwrap();
    let resumed = service
        .admit_provider_resume(
            &parent,
            "fixture:answer".into(),
            Some("sqlite-resume".into()),
            None,
            None,
        )
        .await
        .unwrap();
    let child: AgentId = serde_json::from_value(resumed["agent_id"].clone()).unwrap();
    run_to_end(&home, &child).await;
    let row = store.get(&child).unwrap();
    assert_eq!(row.root_agent_id, parent.root_agent_id);
    assert_eq!(row.sequence, parent.sequence + 1);
    assert_eq!(row.runtime_session_id, parent.runtime_session_id);
    assert_eq!(row.status, Status::Succeeded);
    assert_eq!(
        store
            .conn
            .query_row("PRAGMA quick_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
}

/// A final writer contention, short or longer than any former per-process busy
/// allowance, commits exactly once through the resident writer; private
/// no-follow recovery evidence still retains cleanup and the candidate answer.
#[tokio::test]
async fn terminal_busy_commits_exactly_once_through_the_writer() {
    for hold in [Duration::from_millis(450), Duration::from_secs(8)] {
        let (_temp, home) = home();
        let _cleanup = ServiceProcesses(home.clone());
        let service = fixture_service(home.clone()).await;
        let mut req = request(&home);
        req.task = "fixture:answer".into();
        let admitted = service
            .admit_provider_trusted(req, candidates(committed(&home)))
            .await
            .unwrap();
        let id: AgentId = serde_json::from_value(admitted["agent_id"].clone()).unwrap();
        let attempt = admitted["attempt_id"].as_str().unwrap();
        fs::write(home.join("fixture-pause-terminal-1"), b"pause").unwrap();
        let mut child = supervisor(&home, &id).await;
        tokio::time::timeout(Duration::from_secs(6), async {
            while !home.join("fixture-paused-terminal-1").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let held = writer(&home, hold);
        fs::write(home.join("fixture-resume-terminal-1"), b"go").unwrap();
        let before = std::time::Instant::now();
        let exit = tokio::time::timeout(Duration::from_secs(18), child.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(before.elapsed() < Duration::from_secs(12));
        held.join().unwrap();
        let store = agent_run_store::fixture::open(&home).unwrap();
        let row = store.get(&id).unwrap();
        let dir = home.join("agents").join(id.as_str());
        let receipt: serde_json::Value = serde_json::from_slice(
            &fs::read(dir.join(format!("recovery.{attempt}.json"))).unwrap(),
        )
        .unwrap();
        assert_eq!(receipt["cleanup"]["confirmed"], true);
        assert_eq!(receipt["terminal_commit"], "not_proven_by_receipt");
        assert_eq!(receipt["engine_result_available"], true);
        let candidate: agent_run_platform::verify::Proof =
            serde_json::from_value(receipt["candidate"].clone()).unwrap();
        assert_eq!(
            agent_run_platform::verify::read(&dir, &candidate, 2048)
                .unwrap()
                .1
                .unwrap(),
            "fixture final answer\n"
        );
        assert!(!receipt.to_string().contains("synthetic-token"));
        // The single resident writer waits out a foreign lock holder inside the
        // supervisor's original deadline: whatever the outage length, the final
        // commit lands exactly once (no per-process busy allowance remains).
        assert!(
            exit.success(),
            "hold {hold:?}: failure_kind={:?}, failure_text={:?}",
            row.failure_kind,
            row.failure_text
        );
        assert_eq!(row.status, Status::Succeeded);
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT count(*) FROM deliveries WHERE agent_id=?",
                    [id.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT count(*) FROM events WHERE agent_id=? AND to_status='succeeded'",
                    [id.as_str()],
                    |r| r.get::<_, i64>(0)
                )
                .unwrap(),
            1,
            "one terminal transition"
        );
    }
}

/// Child construction removes all actually inherited worker bindings without
/// mutating the genuine parent. Fresh synthetic tokens added later remain usable.
#[tokio::test]
async fn test_children_strip_real_context_and_preserve_explicit_fixture_bindings() {
    let before: Vec<_> = std::env::vars_os()
        .filter(|(key, _)| {
            key == "AGENT_RUN_HOME" || key.to_string_lossy().starts_with("AGENT_RUN_WORKER_")
        })
        .collect();
    let command = isolated_sync_command("/usr/bin/true");
    for (key, _) in &before {
        assert!(
            command
                .get_envs()
                .any(|(name, value)| name == key && value.is_none())
        );
    }
    assert!(
        command
            .get_envs()
            .any(|(name, value)| name == "AGENT_RUN_HOME" && value.is_none())
    );
    assert!(
        before
            == std::env::vars_os()
                .filter(|(key, _)| key == "AGENT_RUN_HOME"
                    || key.to_string_lossy().starts_with("AGENT_RUN_WORKER_"))
                .collect::<Vec<_>>()
    );
    let mut fresh = isolated_sync_command("/usr/bin/true");
    fresh.env("AGENT_RUN_WORKER_TOKEN", "synthetic-token");
    assert!(
        fresh
            .get_envs()
            .any(|(name, value)| name == "AGENT_RUN_WORKER_TOKEN"
                && value == Some(std::ffi::OsStr::new("synthetic-token")))
    );
}
